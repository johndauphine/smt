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
