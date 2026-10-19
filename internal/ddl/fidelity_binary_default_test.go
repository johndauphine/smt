package ddl

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/smt/internal/driver"
	"github.com/johndauphine/smt/internal/fidelity"
)

func TestFidelityBinaryDefaultIsTypedAndDoesNotMutateSource(t *testing.T) {
	renderer, err := NewRenderer("postgres", "public", "fail")
	if err != nil {
		t.Fatal(err)
	}
	renderer = renderer.WithSource("mssql")
	table := &driver.Table{Name: "limits", Columns: []driver.Column{{Name: "payload", DataType: "varbinary", MaxLength: 50, IsNullable: true, HasDefault: true, DefaultExpression: "((0xAB))"}}}
	before := table.Columns[0]
	legacy, _, err := renderer.CreateTableDDL(table)
	if err != nil {
		t.Fatal(err)
	}
	legacyProfile, _, err := renderer.CreateTableWithFidelity(table, fidelity.Legacy)
	if err != nil || legacyProfile != legacy {
		t.Fatalf("legacy SQL changed: %q, %v", legacyProfile, err)
	}
	checked, _, err := renderer.CreateTableWithFidelity(table, fidelity.Checked)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(checked, "DEFAULT decode('ab', 'hex')") {
		t.Fatalf("missing typed binary default: %s", checked)
	}
	if strings.Contains(checked, "DEFAULT 0x") {
		t.Fatalf("source hex SQL leaked: %s", checked)
	}
	if !reflect.DeepEqual(table.Columns[0], before) {
		t.Fatalf("source column mutated: %#v -> %#v", before, table.Columns[0])
	}
}

func TestFidelityUnsupportedBinaryDefaultReturnsNoSQL(t *testing.T) {
	renderer, err := NewRenderer("postgres", "public", "fail")
	if err != nil {
		t.Fatal(err)
	}
	renderer = renderer.WithSource("mssql")
	table := &driver.Table{Name: "limits", Columns: []driver.Column{{Name: "payload", DataType: "varbinary", MaxLength: 50, HasDefault: true, DefaultExpression: "CONVERT(varbinary(50), 'AB', 2)"}}}
	sql, _, err := renderer.CreateTableWithFidelity(table, fidelity.Checked)
	var unsupported *fidelity.UnsupportedError
	if !errors.As(err, &unsupported) || sql != "" {
		t.Fatalf("unsupported default emitted SQL: %q, %v", sql, err)
	}
	sql, assessment, err := renderer.CreateTableWithFidelity(table, fidelity.Strict)
	var violation *fidelity.ViolationError
	if !errors.As(err, &violation) || sql != "" {
		t.Fatalf("strict default emitted SQL: %q, %v", sql, err)
	}
	found := false
	for _, finding := range assessment.Findings {
		if finding.Code == "binary_default_semantics" {
			found = true
		}
	}
	if !found {
		t.Fatalf("unsupported default not assessed: %#v", assessment)
	}
}
