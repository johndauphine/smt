// Package canonical is SMT's dialect-neutral type intermediate representation
// (#62), modeled on UVG's src/ddl_typemap. A source column's dialect-specific
// type string is normalized into a CanonicalType via ToCanonical, and a
// CanonicalType is rendered into a target dialect's DDL type via FromCanonical.
// The composition FromCanonical(ToCanonical(...)) is the source→canonical→
// target mapping that both the deterministic renderer and the drift/AI-review
// comparator route through, so the two can never disagree about types.
//
// The package takes primitives (TypeMeta), not driver.Column, so it remains a
// leaf with no internal dependencies — the comparator lives in package driver,
// which therefore cannot import a package that imports driver.
package canonical

// Kind is the canonical type category. Each category carries only the
// parameters that are semantically meaningful across dialects (length,
// precision/scale, fractional-seconds precision, tz-awareness, enum members).
type Kind int

const (
	Unknown Kind = iota // not yet classified / sentinel

	Boolean
	TinyInt   // 8-bit  (MySQL/MSSQL TINYINT; pg widens to smallint)
	SmallInt  // 16-bit
	MediumInt // 24-bit (MySQL only; others widen to 32-bit)
	Integer   // 32-bit
	BigInt    // 64-bit
	Decimal   // exact numeric, Precision/Scale
	Real      // 32-bit float
	Double    // 64-bit float

	Varchar // variable-length char, Length (0/-1 = unbounded)
	Char    // fixed-length char, Length
	Text    // unbounded character LOB

	Binary    // fixed-length bytes, Length
	VarBinary // variable-length bytes, Length (0/-1 = unbounded)
	Blob      // unbounded byte LOB

	Date
	Time      // WithTZ, Fsp
	Timestamp // WithTZ, Fsp

	Uuid
	Json
	Xml
	RowVersion // opaque row-change token (MSSQL rowversion/timestamp)

	Enum // EnumValues
	Set  // EnumValues (MySQL multi-value)

	Array // Element is the element kind (best-effort; pg arrays)

	// Raw is a non-portable passthrough: the original type name is kept in
	// Raw and emitted verbatim / handled by the unknown-type policy.
	Raw
)

// CanonicalType is a dialect-neutral type value. Zero value is {Kind: Unknown}.
type CanonicalType struct {
	Kind Kind

	// Length is the declared character or byte length for Varchar/Char/
	// Binary/VarBinary. <= 0 (and the MSSQL -1 MAX sentinel) means unbounded.
	Length int

	// Precision/Scale apply to Decimal. Precision <= 0 means unspecified
	// (bare NUMERIC / DECIMAL).
	Precision int
	Scale     int

	// Fsp is fractional-seconds precision (digits) for Time/Timestamp.
	// nil = unspecified (emit without a precision suffix).
	Fsp *int

	// WithTZ is set for tz-aware Time/Timestamp.
	WithTZ bool

	// UTCNormalized marks MySQL's native TIMESTAMP — a naive-looking but
	// UTC-normalized, 1970–2038-range, session-tz-converted type. It is a
	// real semantic distinct from a generic naive Timestamp, so it survives a
	// MySQL→MySQL round-trip (#101); a non-MySQL target renders it as an
	// ordinary naive Timestamp.
	UTCNormalized bool

	// Unsigned marks an unsigned integer (MySQL). Preserved so a target that
	// can't represent it (everything except MySQL) can widen deterministically.
	Unsigned bool

	// EnumValues holds the member list for Enum/Set.
	EnumValues []string

	// Element is the element type for Array.
	Element *CanonicalType

	// Raw holds the original source type name for Kind == Raw.
	Raw string
}

// Fspv returns the fractional-seconds precision and whether it was specified.
func (c CanonicalType) Fspv() (int, bool) {
	if c.Fsp == nil {
		return 0, false
	}
	return *c.Fsp, true
}

// TypeMeta is the per-column metadata ToCanonical needs beyond the type name —
// the structured fields the readers populate on driver.Column. It is a plain
// value type so callers in any package can build it without importing driver.
type TypeMeta struct {
	MaxLength         int
	Precision         int
	Scale             int
	DatetimePrecision *int
	IsUnsigned        bool
	DisplayWidth      int      // MySQL: captured only for tinyint(1)
	EnumValues        []string // MySQL ENUM/SET members
}
