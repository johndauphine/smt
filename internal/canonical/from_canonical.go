package canonical

import (
	"errors"
	"fmt"
	"strings"
)

// ErrUnknownType is returned by FromCanonical for a Kind: Raw (non-portable)
// type. The caller applies its unknown-type policy (fail / warn / text_fallback).
var ErrUnknownType = errors.New("non-portable source type")

// ErrMissingEnumValues is returned by FromCanonical when an Enum/Set canonical
// type reaches a target that renders a native member list (MySQL ENUM/SET) but
// carries no values. The caller applies its unknown-type policy the same way it
// would for an unmappable type (warn / text_fallback degrade to a sized VARCHAR;
// fail surfaces the error). Targets that render enum/set as a plain sized string
// (MSSQL NVARCHAR) never hit this.
var ErrMissingEnumValues = errors.New("enum/set is missing allowed values")

// RenderOpts carries the few column-level facts that affect the rendered TYPE
// (as opposed to separate column clauses). IsIdentity matters because some
// targets pick a different physical type for an identity column (pg keeps an
// unsigned bigint as bigint under IDENTITY rather than widening to numeric).
type RenderOpts struct {
	IsIdentity bool
}

// MappingWarning describes a mappable-but-lossy canonical type conversion.
// These warnings are advisory: the rendered DDL is valid, but it does not carry
// every source-side semantic fact.
type MappingWarning struct {
	Kind          string `json:"kind"`
	TargetDialect string `json:"target_dialect"`
	Reason        string `json:"reason"`
}

// FromCanonical renders a CanonicalType as a target dialect's DDL type string
// (without NOT NULL / DEFAULT / IDENTITY clauses — those are the renderer's).
// dialect is the canonical target driver name.
func FromCanonical(ct CanonicalType, dialect string, opts RenderOpts) (string, error) {
	typ, _, err := FromCanonicalWithWarnings(ct, dialect, opts)
	return typ, err
}

// FromCanonicalWithWarnings renders the type and returns advisory warnings for
// conversions that are supported but approximate/lossy.
func FromCanonicalWithWarnings(ct CanonicalType, dialect string, opts RenderOpts) (string, []MappingWarning, error) {
	target := canonDialect(dialect)
	var (
		typ string
		err error
	)
	switch target {
	case "postgres":
		typ, err = fromCanonicalPG(ct, opts)
	case "mssql":
		typ, err = fromCanonicalMSSQL(ct, opts)
	case "mysql":
		typ, err = fromCanonicalMySQL(ct, opts)
	default:
		return "", nil, fmt.Errorf("FromCanonical: unsupported target dialect %q", dialect)
	}
	if err != nil {
		return "", nil, err
	}
	return typ, mappingWarnings(ct, target, typ, opts), nil
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
	case Spatial:
		return pgSpatialDDL(ct), nil
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
	case Spatial:
		if ct.SpatialType == "geography" {
			return "GEOGRAPHY", nil
		}
		return "GEOMETRY", nil
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
	case Spatial:
		return mysqlSpatialDDL(ct), nil
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
		return "", ErrMissingEnumValues
	}
	quoted := make([]string, len(ct.EnumValues))
	for i, v := range ct.EnumValues {
		quoted[i] = "'" + strings.ReplaceAll(strings.ReplaceAll(v, `\`, `\\`), "'", "''") + "'"
	}
	return name + "(" + strings.Join(quoted, ",") + ")", nil
}

func pgSpatialDDL(ct CanonicalType) string {
	family := ct.SpatialType
	if family != "geography" {
		family = "geometry"
	}
	subtype := pgSpatialSubType(ct.SpatialSubType)
	if subtype == "" && ct.SRID <= 0 {
		return family
	}
	if subtype == "" {
		subtype = "Geometry"
	}
	if ct.SRID > 0 {
		return fmt.Sprintf("%s(%s,%d)", family, subtype, ct.SRID)
	}
	return fmt.Sprintf("%s(%s)", family, subtype)
}

func mysqlSpatialDDL(ct CanonicalType) string {
	base := strings.ToUpper(mysqlSpatialBase(ct.SpatialSubType))
	if base == "" {
		base = "GEOMETRY"
	}
	if ct.SRID > 0 {
		return fmt.Sprintf("%s SRID %d", base, ct.SRID)
	}
	return base
}

func mysqlSpatialBase(subtype string) string {
	switch strings.ToLower(strings.TrimSpace(subtype)) {
	case "point":
		return "POINT"
	case "linestring":
		return "LINESTRING"
	case "polygon":
		return "POLYGON"
	case "multipoint":
		return "MULTIPOINT"
	case "multilinestring":
		return "MULTILINESTRING"
	case "multipolygon":
		return "MULTIPOLYGON"
	case "geometrycollection":
		return "GEOMETRYCOLLECTION"
	default:
		return "GEOMETRY"
	}
}

func pgSpatialSubType(subtype string) string {
	switch strings.ToLower(strings.TrimSpace(subtype)) {
	case "point":
		return "Point"
	case "linestring":
		return "LineString"
	case "polygon":
		return "Polygon"
	case "multipoint":
		return "MultiPoint"
	case "multilinestring":
		return "MultiLineString"
	case "multipolygon":
		return "MultiPolygon"
	case "geometrycollection":
		return "GeometryCollection"
	default:
		return ""
	}
}

func mappingWarnings(ct CanonicalType, target, rendered string, opts RenderOpts) []MappingWarning {
	var out []MappingWarning
	add := func(reason string) {
		out = append(out, MappingWarning{
			Kind:          kindName(ct.Kind),
			TargetDialect: target,
			Reason:        reason,
		})
	}

	switch ct.Kind {
	case TinyInt:
		if target == "postgres" {
			add("target has no 8-bit integer; rendered as " + rendered)
		}
	case MediumInt:
		if target != "mysql" {
			add("target has no 24-bit integer; rendered as " + rendered)
		}
	case SmallInt, Integer:
		if ct.Unsigned && target != "mysql" {
			add("target has no unsigned integer flag; widened to " + rendered)
		}
	case BigInt:
		if ct.Unsigned && target != "mysql" && !opts.IsIdentity {
			add("target has no unsigned 64-bit integer; rendered as " + rendered)
		}
	case Time, Timestamp:
		if ct.WithTZ && target == "mysql" {
			add("target has no equivalent time-zone-aware type; rendered as " + rendered)
		}
		if ct.UTCNormalized && target != "mysql" {
			add("MySQL TIMESTAMP UTC-normalization is not represented on target; rendered as " + rendered)
		}
		if max := temporalFSPMax(target); max >= 0 {
			if fsp, ok := ct.Fspv(); ok && fsp > max {
				add(fmt.Sprintf("fractional-seconds precision %d clamped to target max %d", fsp, max))
			}
		}
	case Text, Blob:
		if target != "mysql" && mysqlLOBCapacity(ct.Length) {
			add("MySQL LOB capacity tier is not represented on target; rendered as " + rendered)
		}
	case Spatial:
		if target == "postgres" {
			add("PostgreSQL spatial rendering requires PostGIS types to be installed")
		}
		if target == "mssql" && (ct.SRID > 0 || ct.SpatialSubType != "") {
			add("SQL Server column DDL does not encode spatial subtype or SRID; rendered as " + rendered)
		}
		if target == "mysql" && ct.SpatialType == "geography" {
			add("MySQL has no native geography type; rendered as " + rendered)
		}
	}
	return out
}

func temporalFSPMax(target string) int {
	switch target {
	case "postgres", "mysql":
		return 6
	case "mssql":
		return 7
	default:
		return -1
	}
}

func mysqlLOBCapacity(length int) bool {
	switch length {
	case tinyCap, baseCap, mediumCap, longCap:
		return true
	default:
		return false
	}
}

func kindName(k Kind) string {
	switch k {
	case Boolean:
		return "boolean"
	case TinyInt:
		return "tinyint"
	case SmallInt:
		return "smallint"
	case MediumInt:
		return "mediumint"
	case Integer:
		return "integer"
	case BigInt:
		return "bigint"
	case Decimal:
		return "decimal"
	case Real:
		return "real"
	case Double:
		return "double"
	case Varchar:
		return "varchar"
	case Char:
		return "char"
	case Text:
		return "text"
	case Binary:
		return "binary"
	case VarBinary:
		return "varbinary"
	case Blob:
		return "blob"
	case Date:
		return "date"
	case Time:
		return "time"
	case Timestamp:
		return "timestamp"
	case Uuid:
		return "uuid"
	case Json:
		return "json"
	case Xml:
		return "xml"
	case RowVersion:
		return "rowversion"
	case Enum:
		return "enum"
	case Set:
		return "set"
	case Array:
		return "array"
	case Spatial:
		return "spatial"
	case Raw:
		return "raw"
	default:
		return "unknown"
	}
}
