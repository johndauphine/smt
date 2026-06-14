package canonical

import (
	"errors"
	"fmt"
	"strings"
)

// ErrUnknownType is returned by FromCanonical for a Kind: Raw (non-portable)
// type. The caller applies its unknown-type policy (fail / warn / text_fallback).
var ErrUnknownType = errors.New("non-portable source type")

// RenderOpts carries the few column-level facts that affect the rendered TYPE
// (as opposed to separate column clauses). IsIdentity matters because some
// targets pick a different physical type for an identity column (pg keeps an
// unsigned bigint as bigint under IDENTITY rather than widening to numeric).
type RenderOpts struct {
	IsIdentity bool
}

// FromCanonical renders a CanonicalType as a target dialect's DDL type string
// (without NOT NULL / DEFAULT / IDENTITY clauses — those are the renderer's).
// dialect is the canonical target driver name.
func FromCanonical(ct CanonicalType, dialect string, opts RenderOpts) (string, error) {
	switch canonDialect(dialect) {
	case "postgres":
		return fromCanonicalPG(ct, opts)
	case "mssql":
		return fromCanonicalMSSQL(ct, opts)
	case "mysql":
		return fromCanonicalMySQL(ct, opts)
	default:
		return "", fmt.Errorf("FromCanonical: unsupported target dialect %q", dialect)
	}
}

func canonDialect(d string) string {
	switch strings.ToLower(strings.TrimSpace(d)) {
	case "postgres", "postgresql", "pg":
		return "postgres"
	case "mysql", "mariadb", "maria":
		return "mysql"
	case "mssql", "sqlserver", "sql-server", "sql_server":
		return "mssql"
	default:
		return strings.ToLower(strings.TrimSpace(d))
	}
}

func fromCanonicalPG(ct CanonicalType, opts RenderOpts) (string, error) {
	switch ct.Kind {
	case Boolean:
		return "boolean", nil
	case TinyInt:
		return "smallint", nil // pg has no 8-bit int
	case SmallInt:
		if ct.Unsigned {
			return "integer", nil
		}
		return "smallint", nil
	case MediumInt:
		return "integer", nil // 24-bit (and unsigned) fits 32-bit
	case Integer:
		if ct.Unsigned {
			return "bigint", nil
		}
		return "integer", nil
	case BigInt:
		// Unsigned 64-bit overflows signed bigint, so it widens to numeric —
		// except under IDENTITY, where pg requires an integer-family type.
		if ct.Unsigned && !opts.IsIdentity {
			return "numeric(20,0)", nil
		}
		return "bigint", nil
	case Decimal:
		if ct.Precision > 0 {
			return fmt.Sprintf("numeric(%d,%d)", ct.Precision, ct.Scale), nil
		}
		return "numeric", nil
	case Real:
		return "real", nil
	case Double:
		return "double precision", nil
	case Varchar:
		if ct.Length <= 0 {
			return "text", nil
		}
		return fmt.Sprintf("character varying(%d)", ct.Length), nil
	case Char:
		if ct.Length <= 0 {
			return "text", nil
		}
		return fmt.Sprintf("character(%d)", ct.Length), nil
	case Text:
		return "text", nil
	case Binary, VarBinary, Blob, RowVersion:
		return "bytea", nil
	case Date:
		return "date", nil
	case Time:
		return pgTemporal("time", ct), nil
	case Timestamp:
		return pgTemporal("timestamp", ct), nil
	case Uuid:
		return "uuid", nil
	case Json:
		return "jsonb", nil
	case Xml:
		return "xml", nil
	case Enum, Set:
		return "text", nil
	case Array:
		elem := Text
		if ct.Element != nil {
			elem = ct.Element.Kind
		}
		switch elem {
		case Integer, SmallInt, BigInt:
			return "integer[]", nil
		case Uuid:
			return "uuid[]", nil
		default:
			return "text[]", nil
		}
	case Raw:
		return "", fmt.Errorf("%w: %s", ErrUnknownType, ct.Raw)
	default:
		return "", fmt.Errorf("%w", ErrUnknownType)
	}
}

// pgTemporal renders a time/timestamp with its fractional-seconds precision
// (clamped to pg's max of 6) and tz suffix.
func pgTemporal(base string, ct CanonicalType) string {
	out := base
	if fsp, ok := ct.Fspv(); ok && fsp >= 0 {
		if fsp > 6 {
			fsp = 6
		}
		out = fmt.Sprintf("%s(%d)", base, fsp)
	}
	if ct.WithTZ {
		return out + " with time zone"
	}
	if base == "timestamp" {
		return out + " without time zone"
	}
	return out
}

// ---- MSSQL target ----------------------------------------------------------

func fromCanonicalMSSQL(ct CanonicalType, opts RenderOpts) (string, error) {
	switch ct.Kind {
	case Boolean:
		return "BIT", nil
	case TinyInt:
		return "TINYINT", nil
	case SmallInt:
		if ct.Unsigned {
			return "INT", nil
		}
		return "SMALLINT", nil
	case MediumInt:
		return "INT", nil
	case Integer:
		if ct.Unsigned {
			return "BIGINT", nil
		}
		return "INT", nil
	case BigInt:
		if ct.Unsigned {
			return "DECIMAL(20,0)", nil
		}
		return "BIGINT", nil
	case Decimal:
		return decimalDDL("DECIMAL", ct), nil
	case Real:
		return "REAL", nil
	case Double:
		return "FLOAT", nil
	case Varchar:
		if ct.National {
			return sizedCapped("NVARCHAR", ct.Length, 4000), nil
		}
		return sizedCapped("VARCHAR", ct.Length, 8000), nil
	case Char:
		if ct.National {
			if ct.Length > 4000 {
				return "NVARCHAR(MAX)", nil
			}
			return sized("NCHAR", ct.Length, "1"), nil
		}
		if ct.Length > 8000 {
			return "VARCHAR(MAX)", nil
		}
		return sized("CHAR", ct.Length, "1"), nil
	case Text, Json, Xml, Array:
		return "NVARCHAR(MAX)", nil
	case Binary, VarBinary, Blob:
		return sizedCapped("VARBINARY", ct.Length, 8000), nil
	case RowVersion:
		return "ROWVERSION", nil
	case Date:
		return "DATE", nil
	case Time:
		return fspDDL("TIME", ct, 7), nil
	case Timestamp:
		if ct.WithTZ {
			return fspDDL("DATETIMEOFFSET", ct, 7), nil
		}
		return fspDDL("DATETIME2", ct, 7), nil
	case Uuid:
		return "UNIQUEIDENTIFIER", nil
	case Enum:
		return sized("NVARCHAR", enumStringLen(ct), "255"), nil
	case Set:
		return sized("NVARCHAR", setStringLen(ct), "255"), nil
	case Raw:
		return "", fmt.Errorf("%w: %s", ErrUnknownType, ct.Raw)
	default:
		return "", fmt.Errorf("%w", ErrUnknownType)
	}
}

// ---- MySQL target ----------------------------------------------------------

func fromCanonicalMySQL(ct CanonicalType, opts RenderOpts) (string, error) {
	u := func(base string) string {
		if ct.Unsigned {
			return base + " UNSIGNED"
		}
		return base
	}
	switch ct.Kind {
	case Boolean:
		return "TINYINT(1)", nil
	case TinyInt:
		return u("TINYINT"), nil
	case SmallInt:
		return u("SMALLINT"), nil
	case MediumInt:
		return u("MEDIUMINT"), nil
	case Integer:
		return u("INT"), nil
	case BigInt:
		return u("BIGINT"), nil
	case Decimal:
		return u(decimalDDL("DECIMAL", ct)), nil
	case Real:
		return "FLOAT", nil
	case Double:
		return "DOUBLE", nil
	case Varchar:
		return mysqlVarchar(ct.Length), nil
	case Char:
		if ct.Length > 255 {
			return mysqlVarchar(ct.Length), nil
		}
		return sized("CHAR", ct.Length, "1"), nil
	case Text:
		return mysqlTextTier(ct.Length), nil
	case Json, Array:
		return "JSON", nil
	case Xml:
		return "LONGTEXT", nil // mysql has no XML type; XML as text
	case Binary, VarBinary:
		if ct.Length <= 0 {
			return "LONGBLOB", nil
		}
		if ct.Length > 65535 {
			return "MEDIUMBLOB", nil
		}
		return fmt.Sprintf("VARBINARY(%d)", ct.Length), nil
	case Blob:
		return mysqlBlobTier(ct.Length), nil
	case RowVersion:
		return "BINARY(8)", nil
	case Date:
		return "DATE", nil
	case Time:
		return mysqlFspDDL("TIME", ct, 0), nil
	case Timestamp:
		if ct.UTCNormalized {
			return mysqlFspDDL("TIMESTAMP", ct, 6), nil
		}
		return mysqlFspDDL("DATETIME", ct, 6), nil
	case Uuid:
		return "CHAR(36)", nil
	case Enum:
		return enumDDL("ENUM", ct)
	case Set:
		return enumDDL("SET", ct)
	case Raw:
		return "", fmt.Errorf("%w: %s", ErrUnknownType, ct.Raw)
	default:
		return "", fmt.Errorf("%w", ErrUnknownType)
	}
}

// ---- shared sizing helpers (mirror internal/ddl/renderer.go) ----------------

func sized(name string, length int, unbounded string) string {
	if length <= 0 || length == -1 {
		return fmt.Sprintf("%s(%s)", name, unbounded)
	}
	return fmt.Sprintf("%s(%d)", name, length)
}

func sizedCapped(name string, length, max int) string {
	if length > max {
		return name + "(MAX)"
	}
	return sized(name, length, "MAX")
}

func decimalDDL(name string, ct CanonicalType) string {
	if ct.Precision > 0 {
		return fmt.Sprintf("%s(%d,%d)", name, ct.Precision, ct.Scale)
	}
	return name
}

func fspDDL(name string, ct CanonicalType, max int) string {
	fsp, ok := ct.Fspv()
	if !ok || fsp < 0 {
		return name
	}
	if fsp > max {
		fsp = max
	}
	return fmt.Sprintf("%s(%d)", name, fsp)
}

func mysqlFspDDL(name string, ct CanonicalType, def int) string {
	p := def
	if fsp, ok := ct.Fspv(); ok && fsp >= 0 {
		p = fsp
		if p > 6 {
			p = 6
		}
	}
	if p == 0 {
		return name
	}
	return fmt.Sprintf("%s(%d)", name, p)
}

func mysqlVarchar(length int) string {
	if length <= 0 {
		return "LONGTEXT"
	}
	if length > mediumCap/4 {
		return "LONGTEXT"
	}
	if length > 16383 {
		return "MEDIUMTEXT"
	}
	return fmt.Sprintf("VARCHAR(%d)", length)
}

func mysqlTextTier(capacity int) string {
	switch {
	case capacity <= 0:
		return "LONGTEXT" // unbounded foreign LOB
	case capacity <= tinyCap:
		return "TINYTEXT"
	case capacity <= baseCap:
		return "TEXT"
	case capacity <= mediumCap:
		return "MEDIUMTEXT"
	default:
		return "LONGTEXT"
	}
}

func mysqlBlobTier(capacity int) string {
	switch {
	case capacity <= 0:
		return "LONGBLOB"
	case capacity <= tinyCap:
		return "TINYBLOB"
	case capacity <= baseCap:
		return "BLOB"
	case capacity <= mediumCap:
		return "MEDIUMBLOB"
	default:
		return "LONGBLOB"
	}
}

func enumStringLen(ct CanonicalType) int {
	length := ct.Length
	for _, v := range ct.EnumValues {
		if len(v) > length {
			length = len(v)
		}
	}
	return length
}

func setStringLen(ct CanonicalType) int {
	if ct.Length > 0 {
		return ct.Length
	}
	if len(ct.EnumValues) == 0 {
		return 0
	}
	length := 0
	for i, v := range ct.EnumValues {
		if i > 0 {
			length++
		}
		length += len(v)
	}
	return length
}

func enumDDL(name string, ct CanonicalType) (string, error) {
	if len(ct.EnumValues) == 0 {
		return "", fmt.Errorf("%s is missing allowed values", strings.ToLower(name))
	}
	quoted := make([]string, len(ct.EnumValues))
	for i, v := range ct.EnumValues {
		quoted[i] = "'" + strings.ReplaceAll(strings.ReplaceAll(v, `\`, `\\`), "'", "''") + "'"
	}
	return name + "(" + strings.Join(quoted, ",") + ")", nil
}
