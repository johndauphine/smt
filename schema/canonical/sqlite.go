package canonical

import (
	"fmt"
	"strconv"
	"strings"
)

// SQLite has type affinity rather than strict column types. This mapper keeps
// standard declared type names where doing so preserves intent, while treating
// SQLite's native INTEGER affinity as a signed 64-bit value. Unrecognized
// declarations remain Raw instead of guessing a portable semantic type.
func sqliteToCanonical(typeName string, m TypeMeta) CanonicalType {
	base, params := sqliteDeclaration(typeName)
	if base == "" {
		// SQLite permits an omitted type. Although its affinity is BLOB, such
		// columns commonly hold mixed application values; TEXT is the safer
		// portable representation and matches DMT's documented policy.
		return CanonicalType{Kind: Text}
	}

	length := sqliteLength(m, params)
	precision, scale := sqlitePrecisionScale(m, params)
	fsp := sqliteFsp(m, params)

	switch base {
	case "bool", "boolean":
		return CanonicalType{Kind: Boolean}
	case "tinyint":
		return CanonicalType{Kind: TinyInt}
	case "smallint", "int2":
		return CanonicalType{Kind: SmallInt}
	case "mediumint":
		return CanonicalType{Kind: MediumInt}
	case "int4":
		return CanonicalType{Kind: Integer}
	case "integer", "int", "bigint", "int8", "rowid":
		// SQLite INTEGER values are signed 64-bit regardless of the
		// declaration spelling.
		return CanonicalType{Kind: BigInt}
	case "real", "double", "double precision", "float":
		// SQLite's REAL storage class is an IEEE 754 64-bit float.
		return CanonicalType{Kind: Double}
	case "decimal", "numeric":
		return CanonicalType{Kind: Decimal, Precision: precision, Scale: scale}
	case "varchar", "nvarchar", "character varying", "varying character":
		return CanonicalType{Kind: Varchar, Length: length}
	case "char", "nchar", "character":
		return CanonicalType{Kind: Char, Length: length}
	case "text", "clob":
		return CanonicalType{Kind: Text}
	case "blob":
		return CanonicalType{Kind: Blob}
	case "date":
		return CanonicalType{Kind: Date}
	case "time":
		return CanonicalType{Kind: Time, Fsp: fsp}
	case "datetime", "timestamp":
		return CanonicalType{Kind: Timestamp, Fsp: fsp}
	case "uuid":
		return CanonicalType{Kind: Uuid}
	case "json":
		return CanonicalType{Kind: Json}
	}

	// Preserve SQLite's documented affinity precedence for custom type names
	// whose meaning is still reliably inferable. In particular, "FLOATING
	// POINT" has INTEGER affinity because "POINT" contains "INT".
	switch {
	case strings.Contains(base, "int"):
		return CanonicalType{Kind: BigInt}
	case strings.Contains(base, "char"), strings.Contains(base, "clob"), strings.Contains(base, "text"):
		return CanonicalType{Kind: Text}
	case strings.Contains(base, "blob"):
		return CanonicalType{Kind: Blob}
	case strings.Contains(base, "real"), strings.Contains(base, "floa"), strings.Contains(base, "doub"):
		return CanonicalType{Kind: Double}
	default:
		return CanonicalType{Kind: Raw, Raw: base}
	}
}

func isSQLite(dialect string) bool {
	switch strings.ToLower(strings.TrimSpace(dialect)) {
	case "sqlite", "sqlite3":
		return true
	default:
		return false
	}
}

func sqliteDeclaration(typeName string) (string, []int) {
	decl := strings.ToLower(strings.TrimSpace(typeName))
	i := strings.IndexByte(decl, '(')
	if i < 0 {
		return decl, nil
	}
	base := strings.TrimSpace(decl[:i])
	end := strings.LastIndexByte(decl, ')')
	if end <= i || strings.TrimSpace(decl[end+1:]) != "" {
		return base, nil
	}

	parts := strings.Split(decl[i+1:end], ",")
	params := make([]int, 0, len(parts))
	for _, part := range parts {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || n < 0 {
			return base, nil
		}
		params = append(params, n)
	}
	return base, params
}

func sqliteLength(m TypeMeta, params []int) int {
	if m.MaxLength != 0 {
		return m.MaxLength
	}
	if len(params) > 0 && params[0] > 0 {
		return params[0]
	}
	return 0
}

func sqlitePrecisionScale(m TypeMeta, params []int) (int, int) {
	if m.Precision != 0 || m.Scale != 0 {
		return m.Precision, m.Scale
	}
	if len(params) == 2 && params[0] > 0 {
		return params[0], params[1]
	}
	return 0, 0
}

func sqliteFsp(m TypeMeta, params []int) *int {
	if m.DatetimePrecision != nil {
		return m.DatetimePrecision
	}
	if len(params) == 1 {
		fsp := params[0]
		return &fsp
	}
	return nil
}

func fromCanonicalSQLite(ct CanonicalType, opts RenderOpts) (string, error) {
	switch ct.Kind {
	case Boolean:
		return "BOOLEAN", nil
	case TinyInt:
		return "TINYINT", nil
	case SmallInt:
		return "SMALLINT", nil
	case MediumInt:
		return "MEDIUMINT", nil
	case Integer:
		return "INTEGER", nil
	case BigInt:
		return "BIGINT", nil
	case Decimal:
		return decimalDDL("NUMERIC", ct), nil
	case Real, Double:
		return "REAL", nil
	case Varchar:
		if ct.Length > 0 {
			return fmt.Sprintf("VARCHAR(%d)", ct.Length), nil
		}
		return "TEXT", nil
	case Char:
		return sized("CHAR", ct.Length, "1"), nil
	case Text:
		return "TEXT", nil
	case Binary, VarBinary, Blob, RowVersion:
		return "BLOB", nil
	case Date:
		return "DATE", nil
	case Time:
		return "TIME", nil
	case Timestamp:
		return "DATETIME", nil
	case Uuid, Json, Xml, Enum, Set, Array:
		return "TEXT", nil
	case Spatial:
		return "BLOB", nil
	case Raw:
		return "", fmt.Errorf("%w: %s", ErrUnknownType, ct.Raw)
	default:
		return "", fmt.Errorf("%w", ErrUnknownType)
	}
}

func sqliteMappingWarnings(ct CanonicalType, rendered string) []MappingWarning {
	var out []MappingWarning
	add := func(reason string) {
		out = append(out, MappingWarning{
			Kind:          kindName(ct.Kind),
			TargetDialect: "sqlite",
			Reason:        reason,
		})
	}

	switch ct.Kind {
	case TinyInt, SmallInt, MediumInt, Integer, BigInt:
		if ct.Unsigned {
			add("SQLite has no unsigned integer type; rendered as " + rendered)
		}
		if ct.Kind == TinyInt || ct.Kind == MediumInt {
			add("SQLite type affinity does not enforce integer width; rendered as " + rendered)
		}
	case Varchar, Char:
		if ct.Length > 0 {
			add("SQLite type affinity does not enforce declared length; rendered as " + rendered)
		}
	case Decimal:
		if ct.Precision > 0 {
			add("SQLite NUMERIC affinity does not enforce declared precision or scale; rendered as " + rendered)
		}
	case Real:
		add("SQLite REAL storage is 64-bit; rendered as " + rendered)
	case Binary, VarBinary:
		if ct.Length > 0 {
			add("SQLite BLOB does not enforce declared byte length; rendered as " + rendered)
		}
	case RowVersion:
		add("SQLite has no rowversion type; rendered as " + rendered)
	case Time, Timestamp:
		if ct.WithTZ {
			add("SQLite has no time-zone-aware temporal type; rendered as " + rendered)
		}
		if _, ok := ct.Fspv(); ok {
			add("SQLite does not enforce fractional-seconds precision; rendered as " + rendered)
		}
	case Uuid:
		add("SQLite has no native UUID type; rendered as " + rendered)
	case Json:
		add("SQLite JSON values are stored as TEXT; validation requires JSON functions or constraints")
	case Xml:
		add("SQLite has no native XML type; rendered as " + rendered)
	case Enum, Set:
		add("SQLite has no native enum/set type; rendered as " + rendered)
	case Array:
		add("SQLite has no native array type; rendered as " + rendered)
	case Spatial:
		add("SQLite spatial storage requires an extension; rendered as " + rendered)
	}
	return out
}
