// Live lifecycle acceptance runs inside an owned PostgreSQL container only.
// Unlike the independent behavioral oracle, it intentionally exercises SMT's
// production renderer, introspector and drift/sync APIs together.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/johndauphine/smt/internal/dbconfig"
	"github.com/johndauphine/smt/internal/ddl"
	"github.com/johndauphine/smt/internal/driver"
	"github.com/johndauphine/smt/internal/driver/postgres"
	"github.com/johndauphine/smt/internal/fidelity"
	"github.com/johndauphine/smt/internal/schemadiff"
)

type assertion struct {
	ID           string           `json:"id"`
	Pass         bool             `json:"pass"`
	Catalog      []driver.Table   `json:"catalog"`
	Drift        schemadiff.Drift `json:"drift"`
	Diff         schemadiff.Diff  `json:"diff"`
	DriftError   string           `json:"drift_error,omitempty"`
	SyncError    string           `json:"sync_error,omitempty"`
	TypedRefusal bool             `json:"typed_refusal"`
}
type report struct {
	Outcome    string           `json:"outcome"`
	Error      string           `json:"error,omitempty"`
	SQL        string           `json:"rendered_sql"`
	Checks     []fidelity.Check `json:"expected_checks"`
	Desired    []driver.Table   `json:"desired_source_metadata"`
	Assertions []assertion      `json:"assertions"`
}

func execute(r *report) error {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	reader, err := postgres.NewReader(&dbconfig.SourceConfig{Type: "postgres", Host: "127.0.0.1", Port: 5432, Database: "smt_behavior", User: "postgres", Schema: "public", SSLMode: "disable"}, 2)
	if err != nil {
		return err
	}
	defer reader.Close()
	table := driver.Table{Schema: "public", Name: "lifecycle_limits", Columns: []driver.Column{
		{Name: "tiny", DataType: "tinyint", IsNullable: true, DefaultExpression: "(7)", HasDefault: true},
		{Name: "payload", DataType: "varbinary", MaxLength: 50, IsNullable: true, DefaultExpression: "(0xAB)", HasDefault: true},
	}}
	r.Desired = []driver.Table{table}
	renderer, err := ddl.NewRenderer("postgres", "public", "fail")
	if err != nil {
		return err
	}
	renderer = renderer.WithSource("mssql")
	r.SQL, _, err = renderer.CreateTableWithFidelity(&table, fidelity.Checked)
	if err != nil {
		return err
	}
	r.Checks, _, err = renderer.FidelityChecks(&table, fidelity.Checked)
	if err != nil {
		return err
	}
	if len(r.Checks) != 2 {
		return fmt.Errorf("expected exactly two restrictions, got %d", len(r.Checks))
	}
	dialect := &postgres.Dialect{}
	qualified := dialect.QualifyTable("public", "lifecycle_limits")
	exec := func(sql string) error { _, err := reader.DB().ExecContext(ctx, sql); return err }
	reset := func(populated bool) error {
		if err := exec("DROP TABLE IF EXISTS " + qualified); err != nil {
			return err
		}
		if err := exec(r.SQL); err != nil {
			return err
		}
		if populated {
			return exec("INSERT INTO " + qualified + "(tiny) VALUES(255)")
		}
		return nil
	}
	catalog := func() ([]driver.Table, error) {
		tables, err := reader.ExtractSchema(ctx, "public")
		if err != nil {
			return nil, err
		}
		if len(tables) != 1 || tables[0].Name != table.Name {
			return nil, fmt.Errorf("unexpected table inventory")
		}
		for i := range tables {
			if err := reader.LoadIndexes(ctx, &tables[i]); err != nil {
				return nil, err
			}
			if err := reader.LoadForeignKeys(ctx, &tables[i]); err != nil {
				return nil, err
			}
			if err := reader.LoadCheckConstraints(ctx, &tables[i]); err != nil {
				return nil, err
			}
		}
		return tables, nil
	}
	assert := func(id string, noOp bool) error {
		actual, err := catalog()
		if err != nil {
			return err
		}
		if noOp {
			if len(actual[0].CheckConstraints) != 2 {
				return fmt.Errorf("%s: expected two catalog checks", id)
			}
			for _, c := range actual[0].CheckConstraints {
				if c.Validated == nil || !*c.Validated {
					return fmt.Errorf("%s: actual check %s lacks true validation", id, c.Name)
				}
			}
		}
		for _, option := range []struct {
			name string
			opts schemadiff.DriftOptions
		}{{"managed", schemadiff.DefaultDriftOptions()}, {"source_side_objects_disabled", schemadiff.DriftOptions{}}} {
			drift, _, driftErr := schemadiff.ComputeDriftWithFidelity(r.Desired, actual, "mssql", "postgres", option.opts, fidelity.Checked)
			diff, _, syncErr := schemadiff.ComputeLiveDiffWithFidelity(r.Desired, actual, "mssql", "postgres", option.opts, fidelity.Checked)
			item := assertion{ID: id + "/" + option.name, Catalog: actual, Drift: drift, Diff: diff}
			if driftErr != nil {
				item.DriftError = driftErr.Error()
			}
			if syncErr != nil {
				item.SyncError = syncErr.Error()
			}
			var refusal *schemadiff.FidelityEvolutionError
			item.TypedRefusal = errors.As(syncErr, &refusal)
			if noOp {
				item.Pass = driftErr == nil && drift.IsEmpty() && syncErr == nil && diff.IsEmpty()
			} else {
				item.Pass = driftErr == nil && !drift.IsEmpty() && item.TypedRefusal && diff.IsEmpty()
			}
			r.Assertions = append(r.Assertions, item)
		}
		return nil
	}
	if err := reset(false); err != nil {
		return err
	}
	if err := assert("create_introspect_noop", true); err != nil {
		return err
	}
	if err := exec("INSERT INTO " + qualified + "(tiny) VALUES(255)"); err != nil {
		return err
	}
	for n := 1; n <= 2; n++ {
		if err := assert(fmt.Sprintf("populated_sync_noop_%d", n), true); err != nil {
			return err
		}
	}
	for _, check := range r.Checks {
		name := dialect.QuoteIdentifier(check.Name)
		for _, mutation := range []struct{ name, sql string }{
			{"missing", "ALTER TABLE " + qualified + " DROP CONSTRAINT " + name},
			{"unvalidated", "ALTER TABLE " + qualified + " DROP CONSTRAINT " + name + "; ALTER TABLE " + qualified + " ADD CONSTRAINT " + name + " CHECK (" + check.Expression + ") NOT VALID"},
			{"tampered", "ALTER TABLE " + qualified + " DROP CONSTRAINT " + name + "; ALTER TABLE " + qualified + " ADD CONSTRAINT " + name + " CHECK (true)"},
		} {
			if err := reset(true); err != nil {
				return err
			}
			if err := exec(mutation.sql); err != nil {
				return err
			}
			if err := assert(mutation.name+"/"+check.Name, false); err != nil {
				return err
			}
		}
	}
	for _, mutation := range []struct{ name, sql string }{
		{"binary_default_mismatch", "ALTER TABLE " + qualified + " ALTER COLUMN payload SET DEFAULT decode('ac','hex')"},
		{"binary_default_missing", "ALTER TABLE " + qualified + " ALTER COLUMN payload DROP DEFAULT"},
		{"unexplained_prefix_check", "ALTER TABLE " + qualified + " ADD CONSTRAINT smt_fidelity_user_rule CHECK (tiny >= 8)"},
	} {
		if err := reset(true); err != nil {
			return err
		}
		if err := exec(mutation.sql); err != nil {
			return err
		}
		if err := assert(mutation.name, false); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	r := report{Outcome: "started"}
	if err := execute(&r); err != nil {
		r.Outcome = "harness_failure"
		r.Error = err.Error()
	} else {
		r.Outcome = "passed"
		for _, a := range r.Assertions {
			if !a.Pass {
				r.Outcome = "failed_assertions"
			}
		}
	}
	if err := json.NewEncoder(os.Stdout).Encode(r); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}
