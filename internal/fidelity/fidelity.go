// Package fidelity assesses source semantics alongside the legacy canonical
// mapping. It never changes canonical types or the default rendering path.
package fidelity

import (
	"fmt"
	"sort"
	"strings"

	"github.com/johndauphine/smt/internal/driver"
	"github.com/johndauphine/smt/internal/expr"
	"github.com/johndauphine/smt/schema/canonical"
)

// SemanticsVersion identifies the assessment rules and generated restrictions.
const SemanticsVersion = "1"

type Profile string

const (
	Legacy  Profile = "legacy"
	Checked Profile = "checked"
	Strict  Profile = "strict"
)

func ParseProfile(value string) (Profile, error) {
	switch p := Profile(strings.ToLower(strings.TrimSpace(value))); p {
	case "", Legacy:
		return Legacy, nil
	case Checked, Strict:
		return p, nil
	default:
		return "", fmt.Errorf("unknown fidelity profile %q", value)
	}
}

// Stable codes are persisted in review artifacts. Requirements describe
// restrictions enforced by the checked renderer; findings remain unresolved.
const (
	TinyIntRange            = "mssql_tinyint_range"
	VarBinaryLength         = "mssql_varbinary_length"
	SourceContextMissing    = "source_context_missing"
	SourceMetadataMissing   = "source_metadata_missing"
	TemporalPrecision       = "temporal_precision_loss"
	TemporalRange           = "temporal_range_semantics"
	OriginalOffset          = "original_offset_loss"
	FixedBinaryPadding      = "fixed_binary_padding"
	LegacyDateTime          = "legacy_datetime_semantics"
	UTF16Length             = "utf16_length_semantics"
	TimeInputConversion     = "time_input_conversion"
	TextComparison          = "text_comparison_semantics"
	BinaryInputConversion   = "binary_input_conversion"
	BinaryDefaultSemantics  = "binary_default_semantics"
	UnassessedType          = "unassessed_source_type"
	RowVersionSemantics     = "rowversion_generation_semantics"
	IdentitySemantics       = "identity_assignment_semantics"
	ComputedSemantics       = "computed_column_semantics"
	NullableUniqueSemantics = "nullable_unique_semantics"
)

type Object struct {
	Schema string `json:"schema,omitempty"`
	Table  string `json:"table,omitempty"`
	Column string `json:"column,omitempty"`
}

type Requirement struct {
	Code          string `json:"code"`
	Object        Object `json:"object"`
	SourceDialect string `json:"source_dialect"`
	TargetDialect string `json:"target_dialect"`
	Min           int64  `json:"min,omitempty"`
	Max           int64  `json:"max,omitempty"`
	MaxBytes      int    `json:"max_bytes,omitempty"`
}

type Finding struct {
	Code          string `json:"code"`
	Object        Object `json:"object"`
	SourceDialect string `json:"source_dialect,omitempty"`
	TargetDialect string `json:"target_dialect"`
	Reason        string `json:"reason"`
}

type Assessment struct {
	Requirements []Requirement `json:"requirements,omitempty"`
	Findings     []Finding     `json:"findings,omitempty"`
}

// UnsupportedError rejects an unimplemented assessment or enforcement request.
type UnsupportedError struct {
	SourceDialect string
	TargetDialect string
	Reason        string
}

func (e *UnsupportedError) Error() string {
	return fmt.Sprintf("unsupported fidelity assessment %q -> %q: %s", e.SourceDialect, e.TargetDialect, e.Reason)
}

// ViolationError is the strict profile's gate over the assessed semantics; it
// does not claim equivalence for every possible database behavior.
type ViolationError struct{ Findings []Finding }

func (e *ViolationError) Error() string {
	if len(e.Findings) == 0 {
		return "strict fidelity assessment failed"
	}
	f := e.Findings[0]
	return fmt.Sprintf("strict fidelity assessment rejected %s.%s.%s: %s (%d unresolved findings)", f.Object.Schema, f.Object.Table, f.Object.Column, f.Code, len(e.Findings))
}

// Gate must run before any statements from an assessed plan are executed.
func Gate(profile Profile, assessment Assessment) error {
	p, err := ParseProfile(string(profile))
	if err != nil {
		return err
	}
	if p == Strict && len(assessment.Findings) != 0 {
		return &ViolationError{Findings: append([]Finding(nil), assessment.Findings...)}
	}
	return nil
}

func dialect(value string) string {
	switch value = strings.ToLower(strings.TrimSpace(value)); value {
	case "postgres", "postgresql", "pg":
		return "postgres"
	case "mssql", "sqlserver", "sql-server", "sql_server":
		return "mssql"
	case "mysql", "maria", "mariadb":
		return "mysql"
	default:
		return strings.ToLower(driver.Canonicalize(value))
	}
}

func validatePair(source, target string) error {
	if target == "postgres" && (source == "mssql" || source == "") {
		return nil
	}
	return &UnsupportedError{SourceDialect: source, TargetDialect: target, Reason: "the assessed profile currently supports SQL Server to PostgreSQL only"}
}

func AssessTable(table *driver.Table, source, target string) (Assessment, error) {
	source, target = dialect(source), dialect(target)
	if err := validatePair(source, target); err != nil {
		return Assessment{}, err
	}
	if table == nil {
		return Assessment{}, fmt.Errorf("fidelity assessment requires table metadata")
	}
	var result Assessment
	if source == "" && len(table.Columns) == 0 {
		result.Findings = append(result.Findings, Finding{Code: SourceContextMissing,
			Object: Object{Schema: table.Schema, Table: table.Name}, TargetDialect: target,
			Reason: "original source dialect is required to assess source semantics"})
	}
	for _, column := range table.Columns {
		a, err := AssessColumn(column, Object{Schema: table.Schema, Table: table.Name, Column: column.Name}, source, target)
		if err != nil {
			return Assessment{}, err
		}
		result.Requirements = append(result.Requirements, a.Requirements...)
		result.Findings = append(result.Findings, a.Findings...)
	}
	if source == "mssql" {
		result.Findings = append(result.Findings, assessUniqueIndexes(table, source, target)...)
	}
	sortAssessment(&result)
	return result, nil
}

func assessUniqueIndexes(table *driver.Table, source, target string) []Finding {
	columns := make(map[string]driver.Column, len(table.Columns))
	for _, column := range table.Columns {
		columns[strings.ToLower(column.Name)] = column
	}
	var findings []Finding
	for _, index := range table.Indexes {
		if !index.IsUnique {
			continue
		}
		nullable, unknown := false, len(index.Columns) == 0
		for i, name := range index.Columns {
			if i < len(index.ColumnExpressions) && index.ColumnExpressions[i] {
				unknown = true
				continue
			}
			column, exists := columns[strings.ToLower(name)]
			if !exists {
				unknown = true
				continue
			}
			nullable = nullable || column.IsNullable
		}
		object := Object{Schema: table.Schema, Table: table.Name}
		if nullable {
			reason := fmt.Sprintf("SQL Server unique index/constraint %q treats NULL key values as equal for uniqueness while PostgreSQL's generated unique index uses NULLS DISTINCT; source uniqueness is not preserved", index.Name)
			if strings.TrimSpace(index.Filter) != "" {
				reason = fmt.Sprintf("unique index %q has nullable keys and a filter; equivalent NULL exclusion and source uniqueness have not been established for the translated predicate", index.Name)
			}
			findings = append(findings, Finding{Code: NullableUniqueSemantics, Object: object, SourceDialect: source, TargetDialect: target, Reason: reason})
		}
		if unknown {
			findings = append(findings, Finding{Code: SourceMetadataMissing, Object: object, SourceDialect: source, TargetDialect: target,
				Reason: fmt.Sprintf("unique index/constraint %q has missing or expression key metadata; its key nullability and NULL uniqueness cannot be assessed", index.Name)})
		}
	}
	return findings
}

// AssessColumn reads original source metadata, before dialect distinctions are
// collapsed by canonicalization. No source metadata is mutated.
func AssessColumn(column driver.Column, object Object, source, target string) (Assessment, error) {
	source, target = dialect(source), dialect(target)
	if err := validatePair(source, target); err != nil {
		return Assessment{}, err
	}
	object.Column = column.Name
	var result Assessment
	finding := func(code, reason string) {
		result.Findings = append(result.Findings, Finding{Code: code, Object: object, SourceDialect: source, TargetDialect: target, Reason: reason})
	}
	requirement := func(code string, min, max int64, maxBytes int) {
		result.Requirements = append(result.Requirements, Requirement{Code: code, Object: object, SourceDialect: source, TargetDialect: target, Min: min, Max: max, MaxBytes: maxBytes})
	}
	if source == "" {
		finding(SourceContextMissing, "original source dialect is required to assess signedness, length, temporal and comparison semantics")
		return result, nil
	}
	dt := strings.ToLower(strings.TrimSpace(column.DataType))
	switch dt {
	case "smallint", "int", "bigint":
		// These native scalar storage domains match PostgreSQL integer types.
	case "tinyint":
		// SQL Server tinyint is unsigned even when IsUnsigned is false. MySQL
		// tinyint signedness and tinyint(1) are distinct and never enter here.
		requirement(TinyIntRange, 0, 255, 0)
	case "varbinary":
		switch {
		case column.MaxLength > 0 && column.MaxLength <= 8000:
			requirement(VarBinaryLength, 0, 0, column.MaxLength)
		case column.MaxLength == -1:
			// SQL Server MAX remains unbounded in the target mapping.
		default:
			finding(SourceMetadataMissing, "varbinary requires a catalog byte bound in 1..8000 or -1 for MAX; zero does not prove an unbounded source")
		}
		finding(BinaryInputConversion, "the byte-length restriction covers stored values; explicit casts and parameter conversions can truncate before source assignment and are not emulated")
	case "binary":
		finding(FixedBinaryPadding, "SQL Server fixed binary padding and conversion depend on input type; an exact-length CHECK would reject valid source inputs and is not emitted")
	case "datetime", "smalldatetime":
		finding(LegacyDateTime, "SQL Server legacy timestamp range, tick/minute rounding and rollover are not preserved by the target timestamp type")
	case "datetime2", "datetimeoffset", "time":
		if dt != "time" {
			finding(TemporalRange, "PostgreSQL native timestamps accept values outside the SQL Server calendar range, including infinities; the source range is not enforced")
		}
		if column.DatetimePrecision == nil {
			finding(SourceMetadataMissing, "source fractional-seconds precision is missing; older metadata does not establish precision equivalence")
		} else if *column.DatetimePrecision < 0 || *column.DatetimePrecision > 7 {
			finding(SourceMetadataMissing, "SQL Server fractional-seconds precision must be in 0..7")
		} else if *column.DatetimePrecision == 7 {
			finding(TemporalPrecision, "SQL Server precision 7 stores 100-nanosecond fractions; PostgreSQL native temporal types retain at most microseconds")
		}
		if dt == "datetimeoffset" {
			finding(OriginalOffset, "PostgreSQL timestamptz preserves the instant but not the original per-value SQL Server offset or its native offset interface")
		}
		if dt == "time" {
			finding(TimeInputConversion, "source input conversion and rounding can clamp valid near-midnight values while PostgreSQL accepts explicit 24:00; a post-conversion midnight CHECK cannot distinguish these inputs")
		}
	case "date":
		finding(TemporalRange, "PostgreSQL date accepts values outside the SQL Server calendar range, including infinities; the source range is not enforced")
	case "rowversion", "timestamp":
		finding(RowVersionSemantics, "SQL Server rowversion is generated and updated by the database; a mapped binary or timestamp target does not preserve its generation or native interface")
	case "char", "nchar", "varchar", "nvarchar", "text", "ntext":
		// Character-specific findings follow the canonical category below.
	default:
		finding(UnassessedType, "the source type's value domain, conversions and native operations are outside the explicitly assessed integer, bounded binary, character and temporal feature scope")
	}
	if column.IsIdentity {
		finding(IdentitySemantics, "PostgreSQL GENERATED BY DEFAULT identity permits explicit assignment differently from SQL Server IDENTITY; override and sequence behavior are not proven equivalent")
	}
	if column.IsComputed {
		finding(ComputedSemantics, "cross-dialect computed expression evaluation and native update behavior have not been independently established for this source column")
	}
	if (dt == "binary" || dt == "varbinary") && driver.ColumnHasDefault(column) {
		if _, err := expr.ParseBinaryLiteral(column.DefaultExpression, source); err != nil {
			finding(BinaryDefaultSemantics, "binary default is outside supported source byte literals and NULL; source conversion, casting and function semantics are not established")
		}
	}

	ct := canonical.ToCanonical(column.DataType, driver.MetaOf(column), source)
	if ct.Kind == canonical.Char || ct.Kind == canonical.Varchar || ct.Kind == canonical.Text {
		finding(TextComparison, "source and target collation, encoding, trailing-space comparison and uniqueness semantics are not proven equivalent; collation context is unavailable")
		if dt == "nvarchar" || dt == "nchar" {
			if column.MaxLength > 0 {
				finding(UTF16Length, "SQL Server national character length counts UTF-16 units while PostgreSQL character length counts characters, including supplementary-character boundaries")
			} else if column.MaxLength != -1 {
				finding(SourceMetadataMissing, "national character source length is missing")
			}
		}
	}
	sortAssessment(&result)
	return result, nil
}

func objectKey(o Object) string { return o.Schema + "\x00" + o.Table + "\x00" + o.Column }

func sortAssessment(result *Assessment) {
	sort.SliceStable(result.Requirements, func(i, j int) bool {
		a, b := result.Requirements[i], result.Requirements[j]
		return objectKey(a.Object)+"\x00"+a.Code < objectKey(b.Object)+"\x00"+b.Code
	})
	sort.SliceStable(result.Findings, func(i, j int) bool {
		a, b := result.Findings[i], result.Findings[j]
		return objectKey(a.Object)+"\x00"+a.Code+"\x00"+a.Reason < objectKey(b.Object)+"\x00"+b.Code+"\x00"+b.Reason
	})
}
