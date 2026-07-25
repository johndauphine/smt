// Package canonical preserves SMT's former internal import path.
//
// Deprecated: new code should import smt/schema/canonical. The implementation
// lives there so it can be consumed as a public, dependency-free leaf package.
package canonical

import public "smt/schema/canonical"

type (
	Kind           = public.Kind
	CanonicalType  = public.CanonicalType
	TypeMeta       = public.TypeMeta
	RenderOpts     = public.RenderOpts
	MappingWarning = public.MappingWarning
)

const (
	Unknown      = public.Unknown
	Boolean      = public.Boolean
	BitString    = public.BitString
	VarBitString = public.VarBitString
	TinyInt      = public.TinyInt
	SmallInt     = public.SmallInt
	MediumInt    = public.MediumInt
	Integer      = public.Integer
	BigInt       = public.BigInt
	Decimal      = public.Decimal
	Real         = public.Real
	Double       = public.Double
	Varchar      = public.Varchar
	Char         = public.Char
	Text         = public.Text
	Binary       = public.Binary
	VarBinary    = public.VarBinary
	Blob         = public.Blob
	Date         = public.Date
	Time         = public.Time
	Timestamp    = public.Timestamp
	Uuid         = public.Uuid
	Json         = public.Json
	Xml          = public.Xml
	RowVersion   = public.RowVersion
	Enum         = public.Enum
	Set          = public.Set
	Array        = public.Array
	Spatial      = public.Spatial
	Raw          = public.Raw
)

var (
	ErrUnknownType       = public.ErrUnknownType
	ErrMissingEnumValues = public.ErrMissingEnumValues
)

func ToCanonical(typeName string, m TypeMeta, dialect string) CanonicalType {
	return public.ToCanonical(typeName, m, dialect)
}

func FromCanonical(ct CanonicalType, dialect string, opts RenderOpts) (string, error) {
	return public.FromCanonical(ct, dialect, opts)
}

func FromCanonicalWithWarnings(ct CanonicalType, dialect string, opts RenderOpts) (string, []MappingWarning, error) {
	return public.FromCanonicalWithWarnings(ct, dialect, opts)
}

func IsSpatialTypeName(typeName string) bool {
	return public.IsSpatialTypeName(typeName)
}
