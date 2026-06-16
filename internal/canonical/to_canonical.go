package canonical

import "strings"

// MySQL LOB tier byte capacities, used so a MySQL→MySQL round-trip preserves
// the exact tier (TINYTEXT/TEXT/MEDIUMTEXT/LONGTEXT, and the BLOB tiers) while
// a non-MySQL source's unbounded LOB still picks the largest tier on a MySQL
// target. FromCanonical selects the tier from CanonicalType.Length.
const (
	tinyCap   = 255
	baseCap   = 65535
	mediumCap = 16777215
	longCap   = 4294967295
)

// ToCanonical normalizes a source column's dialect-specific type into the
// dialect-neutral CanonicalType. dialect is the canonical source driver name
// ("postgres", "mysql", "mariadb", "mssql"); unknown dialects are treated
// permissively. An unrecognized type name becomes Kind: Raw carrying the
// original name, so the caller's unknown-type policy decides what to do.
func ToCanonical(typeName string, m TypeMeta, dialect string) CanonicalType {
	dt := strings.ToLower(strings.TrimSpace(typeName))
	mysql := isMySQL(dialect)

	if ct, ok := toSpatial(dt, m, dialect); ok {
		return ct
	}

	switch dt {
	// ---- integers / booleans -------------------------------------------
	case "tinyint":
		// MySQL's tinyint(1) is the boolean convention (the reader captures
		// DisplayWidth==1 only for it). Plain tinyint is an 8-bit integer.
		// tinyint(1) is MySQL's boolean (BOOL/BOOLEAN are aliases for it). The
		// boolean class has no sign, so a tinyint(1) UNSIGNED canonicalizes to
		// Boolean too — the (meaningless) UNSIGNED is dropped on round-trip.
		if mysql && m.DisplayWidth == 1 {
			return CanonicalType{Kind: Boolean}
		}
		return CanonicalType{Kind: TinyInt, Unsigned: m.IsUnsigned}
	case "smallint", "int2", "smallserial":
		return CanonicalType{Kind: SmallInt, Unsigned: m.IsUnsigned}
	case "mediumint":
		return CanonicalType{Kind: MediumInt, Unsigned: m.IsUnsigned}
	case "int", "integer", "int4", "serial":
		return CanonicalType{Kind: Integer, Unsigned: m.IsUnsigned}
	case "bigint", "int8", "bigserial":
		return CanonicalType{Kind: BigInt, Unsigned: m.IsUnsigned}
	case "bit", "bool", "boolean":
		return CanonicalType{Kind: Boolean}

	// ---- exact / approximate numeric -----------------------------------
	case "decimal", "numeric", "number":
		return CanonicalType{Kind: Decimal, Precision: m.Precision, Scale: m.Scale}
	case "money":
		return CanonicalType{Kind: Decimal, Precision: 19, Scale: 4}
	case "smallmoney":
		return CanonicalType{Kind: Decimal, Precision: 10, Scale: 4}
	case "float":
		// Dialect-dependent precision: MySQL FLOAT is 32-bit single; MSSQL
		// FLOAT (no precision) is 64-bit double. (PostgreSQL never reports a
		// bare "float" — it uses real / double precision / float4 / float8.)
		if mysql {
			return CanonicalType{Kind: Real}
		}
		return CanonicalType{Kind: Double}
	case "double", "double precision", "float8":
		return CanonicalType{Kind: Double}
	case "real":
		// MySQL REAL is a synonym for DOUBLE (8-byte); MSSQL/PG REAL is 4-byte
		// single. (MySQL's REAL_AS_FLOAT sql_mode is non-default and ignored.)
		if mysql {
			return CanonicalType{Kind: Double}
		}
		return CanonicalType{Kind: Real}
	case "float4":
		return CanonicalType{Kind: Real}

	// ---- character ------------------------------------------------------
	case "varchar", "character varying":
		return CanonicalType{Kind: Varchar, Length: m.MaxLength}
	case "nvarchar":
		return CanonicalType{Kind: Varchar, Length: m.MaxLength, National: true}
	case "char", "character", "bpchar":
		return CanonicalType{Kind: Char, Length: m.MaxLength}
	case "nchar":
		return CanonicalType{Kind: Char, Length: m.MaxLength, National: true}
	case "text":
		// Dialect-ambiguous: MySQL's 64KiB tier vs the unbounded LOB of pg
		// (~1GB) / legacy MSSQL (~2GB). Carry the tier capacity so a MySQL
		// round-trip stays TEXT while a foreign unbounded source lands LONGTEXT.
		if mysql {
			return CanonicalType{Kind: Text, Length: baseCap}
		}
		return CanonicalType{Kind: Text} // unbounded
	case "tinytext":
		return CanonicalType{Kind: Text, Length: tinyCap}
	case "mediumtext":
		return CanonicalType{Kind: Text, Length: mediumCap}
	case "longtext":
		return CanonicalType{Kind: Text, Length: longCap}
	case "ntext":
		return CanonicalType{Kind: Text} // MSSQL national text — unbounded

	// ---- binary ---------------------------------------------------------
	case "binary":
		return CanonicalType{Kind: Binary, Length: m.MaxLength}
	case "varbinary":
		return CanonicalType{Kind: VarBinary, Length: m.MaxLength}
	case "bytea":
		return CanonicalType{Kind: Blob} // pg unbounded bytes
	case "image":
		return CanonicalType{Kind: Blob} // MSSQL ~2GB
	case "blob":
		// "blob" is a MySQL-only type name — inherently the 64KiB tier
		// regardless of how the source dialect is labeled.
		return CanonicalType{Kind: Blob, Length: baseCap}
	case "tinyblob":
		return CanonicalType{Kind: Blob, Length: tinyCap}
	case "mediumblob":
		return CanonicalType{Kind: Blob, Length: mediumCap}
	case "longblob":
		return CanonicalType{Kind: Blob, Length: longCap}
	case "rowversion":
		return CanonicalType{Kind: RowVersion}

	// ---- temporal -------------------------------------------------------
	case "date":
		return CanonicalType{Kind: Date}
	case "time", "time without time zone":
		return CanonicalType{Kind: Time, Fsp: m.DatetimePrecision}
	case "time with time zone", "timetz":
		return CanonicalType{Kind: Time, WithTZ: true, Fsp: m.DatetimePrecision}
	case "datetime", "datetime2", "smalldatetime", "timestamp without time zone":
		return CanonicalType{Kind: Timestamp, Fsp: m.DatetimePrecision}
	case "timestamp":
		// MySQL TIMESTAMP is UTC-normalized + range-limited — a real semantic
		// distinct from pg's naive timestamp; preserve it for a MySQL target.
		return CanonicalType{Kind: Timestamp, Fsp: m.DatetimePrecision, UTCNormalized: mysql}
	case "datetimeoffset", "timestamptz", "timestamp with time zone":
		return CanonicalType{Kind: Timestamp, WithTZ: true, Fsp: m.DatetimePrecision}

	// ---- structured / special ------------------------------------------
	case "uniqueidentifier", "uuid":
		return CanonicalType{Kind: Uuid}
	case "json", "jsonb":
		return CanonicalType{Kind: Json}
	case "xml":
		return CanonicalType{Kind: Xml}
	case "enum":
		return CanonicalType{Kind: Enum, Length: m.MaxLength, EnumValues: m.EnumValues}
	case "set":
		return CanonicalType{Kind: Set, Length: m.MaxLength, EnumValues: m.EnumValues}

	// ---- pg arrays ------------------------------------------------------
	case "_text", "text[]", "_varchar", "varchar[]", "_bpchar", "bpchar[]":
		return CanonicalType{Kind: Array, Element: &CanonicalType{Kind: Text}}
	case "_int2", "int2[]", "_int4", "int4[]", "_int8", "int8[]":
		return CanonicalType{Kind: Array, Element: &CanonicalType{Kind: Integer}}
	case "_uuid", "uuid[]":
		return CanonicalType{Kind: Array, Element: &CanonicalType{Kind: Uuid}}
	case "array":
		return CanonicalType{Kind: Array, Element: &CanonicalType{Kind: Text}}

	default:
		return CanonicalType{Kind: Raw, Raw: dt}
	}
}

func toSpatial(dt string, m TypeMeta, dialect string) (CanonicalType, bool) {
	base, subtype, srid := parseSpatialType(dt, m)
	if base == "" {
		return CanonicalType{}, false
	}
	switch base {
	case "geography":
		return CanonicalType{Kind: Spatial, SpatialType: "geography", SpatialSubType: subtype, SRID: srid}, true
	case "geometry":
		return CanonicalType{Kind: Spatial, SpatialType: "geometry", SpatialSubType: subtype, SRID: srid}, true
	case "point", "linestring", "polygon", "multipoint", "multilinestring", "multipolygon", "geometrycollection":
		if !isMySQL(dialect) {
			return CanonicalType{}, false
		}
		return CanonicalType{Kind: Spatial, SpatialType: "geometry", SpatialSubType: base, SRID: srid}, true
	default:
		return CanonicalType{}, false
	}
}

func parseSpatialType(dt string, m TypeMeta) (base, subtype string, srid int) {
	srid = m.SRID
	base = strings.ToLower(strings.TrimSpace(dt))
	if i := strings.Index(base, "("); i >= 0 && strings.HasSuffix(base, ")") {
		rawBase := strings.TrimSpace(base[:i])
		inner := strings.TrimSpace(base[i+1 : len(base)-1])
		parts := strings.Split(inner, ",")
		if len(parts) > 0 {
			subtype = normalizeSpatialSubType(parts[0])
		}
		if len(parts) > 1 {
			if parsed, ok := atoiPositive(strings.TrimSpace(parts[1])); ok {
				srid = parsed
			}
		}
		base = rawBase
	}
	if subtype == "" {
		subtype = normalizeSpatialSubType(m.SpatialSubType)
	}
	return base, subtype, srid
}

func normalizeSpatialSubType(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.TrimPrefix(s, "st_")
	s = strings.ReplaceAll(s, " ", "")
	switch s {
	case "geometry", "geography":
		return ""
	default:
		return s
	}
}

func atoiPositive(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	return n, true
}

// IsSpatialTypeName reports whether a dialect type name belongs to a supported
// spatial family. It accepts bare names and PostGIS-style geometry(...).
func IsSpatialTypeName(typeName string) bool {
	_, ok := toSpatial(strings.ToLower(strings.TrimSpace(typeName)), TypeMeta{}, "mysql")
	return ok
}

func isMySQL(dialect string) bool {
	switch strings.ToLower(strings.TrimSpace(dialect)) {
	case "mysql", "mariadb":
		return true
	default:
		return false
	}
}
