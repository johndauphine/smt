package driver

import "context"

// TypeMapper handles data type conversions between databases.
type TypeMapper interface {
	// MapType converts a source type to the target type.
	// Returns the mapped type string (e.g., "varchar(255)", "numeric(10,2)").
	MapType(info TypeInfo) string

	// CanMap returns true if this mapper can handle the given conversion.
	CanMap(sourceDBType, targetDBType string) bool

	// SupportedTargets returns the list of target database types this mapper supports.
	// Returns ["*"] if it can map to any target (e.g., AI mapper).
	SupportedTargets() []string
}

// TableDDLReviewer audits deterministic CREATE TABLE DDL. It is intentionally
// review-only so production DDL paths do not depend on AI generation
// interfaces.
type TableDDLReviewer interface {
	VerifyTableDDL(ctx context.Context, req VerifyTableDDLRequest) (*VerifyResult, error)
}

// DatabaseContext contains metadata about a database for AI context.
type DatabaseContext struct {
	// Version is the full database version string (e.g., "PostgreSQL 15.4", "Oracle 23ai").
	Version string

	// MajorVersion is the parsed major version number (e.g., 15 for PostgreSQL 15.4).
	MajorVersion int

	// Charset is the database character set (e.g., "UTF8", "AL32UTF8", "utf8mb4").
	Charset string

	// NationalCharset is the national character set (Oracle: AL16UTF16, etc.).
	NationalCharset string

	// Collation is the default collation (e.g., "en_US.UTF-8", "utf8mb4_unicode_ci").
	Collation string

	// CodePage is the Windows code page number (e.g., 1252 for Latin1, 65001 for UTF-8).
	CodePage int

	// Encoding is the encoding name (e.g., "UTF-8", "LATIN1", "CP1252").
	Encoding string

	// CaseSensitiveIdentifiers indicates if unquoted identifiers are case-sensitive.
	// false = case-insensitive (Oracle uppercase, SQL Server, MySQL default)
	// true = case-sensitive (PostgreSQL lowercase preserves case)
	CaseSensitiveIdentifiers bool

	// IdentifierCase is how unquoted identifiers are stored:
	// "upper" (Oracle), "lower" (PostgreSQL), "preserve" (MySQL), "insensitive" (SQL Server)
	IdentifierCase string

	// CaseSensitiveData indicates if string comparisons are case-sensitive by default.
	CaseSensitiveData bool

	// MaxIdentifierLength is the maximum length for table/column names.
	MaxIdentifierLength int

	// MaxVarcharLength is the maximum varchar length in characters or bytes.
	MaxVarcharLength int

	// MaxNVarcharLength is the max NVARCHAR length in characters (MSSQL: 4000).
	// Set only for databases that have a separate national character type.
	MaxNVarcharLength int

	// VarcharSemantics indicates if varchar uses "byte" or "char" semantics.
	VarcharSemantics string

	// BytesPerChar is the maximum bytes per character (1 for Latin1, 4 for UTF-8).
	BytesPerChar int

	// Features lists available features (e.g., "InnoDB", "CLOB", "JSON", "GEOGRAPHY").
	Features []string

	// StorageEngine is the storage engine (MySQL: InnoDB, Oracle: tablespace, etc.).
	StorageEngine string

	// DatabaseName is the specific database/schema name being used.
	DatabaseName string

	// ServerName is the database server hostname.
	ServerName string

	// Notes contains any additional context about the database.
	Notes string
}

// TypeInfo contains metadata about a column type.
type TypeInfo struct {
	// SourceDBType is the source database type (e.g., "mssql", "postgres").
	SourceDBType string

	// TargetDBType is the target database type.
	TargetDBType string

	// DataType is the source column's data type.
	DataType string

	// MaxLength is the maximum length for string types (-1 for MAX).
	MaxLength int

	// Precision is the numeric precision.
	Precision int

	// Scale is the numeric scale.
	Scale int

	// SampleValues contains sample data values from the source column.
	// Used by AI mapper to provide context for better type mapping decisions.
	SampleValues []string
}

// FinalizationDDLReviewer audits deterministic index/FK/CHECK DDL.
type FinalizationDDLReviewer interface {
	VerifyFinalizationDDL(ctx context.Context, req VerifyFinalizationDDLRequest) (*VerifyResult, error)
}

// DDLType specifies the type of DDL to generate.
type DDLType string

const (
	DDLTypeIndex           DDLType = "index"
	DDLTypeForeignKey      DDLType = "foreign_key"
	DDLTypeCheckConstraint DDLType = "check_constraint"
)

// VerifyTableDDLRequest is the input to VerifyTableDDL. Carries the source
// metadata plus the proposed target DDL so the auditor can compare
// attribute-by-attribute.
type VerifyTableDDLRequest struct {
	SourceDBType  string
	TargetDBType  string
	SourceTable   *Table
	TargetSchema  string
	SourceContext *DatabaseContext
	TargetContext *DatabaseContext

	// ProposedDDL is the just-generated CREATE TABLE statement under audit.
	ProposedDDL string
}

// VerifyFinalizationDDLRequest is the input to VerifyFinalizationDDL.
type VerifyFinalizationDDLRequest struct {
	Type            DDLType
	SourceDBType    string
	TargetDBType    string
	Table           *Table
	TargetSchema    string
	TargetContext   *DatabaseContext
	Index           *Index
	ForeignKey      *ForeignKey
	CheckConstraint *CheckConstraint
	ProposedDDL     string
}

// VerifyResult is the auditor's verdict.
//
// OK=true means the proposed DDL preserves all six audit criteria
// (max_length / precision / scale, nullability, identity, timezone-
// awareness, default-class, type semantics) for every column.
//
// OK=false means at least one criterion failed; Issues carries one human-
// readable line per failure ("column_name: criterion — expected vs
// emitted"). The writer feeds Issues back into the next generation
// attempt as PreviousAttempt.Error.
//
// On a malformed AI response that's neither parseable as OK nor as ISSUES,
// the parser returns OK=false with a single synthetic issue containing the
// raw response (truncated). Fail-closed by design.
type VerifyResult struct {
	OK     bool
	Issues []string
}

// GetAITypeMapper returns the global AI type mapper loaded from secrets.
// This is the only type mapper available - all type mapping is done via AI.
func GetAITypeMapper() (TypeMapper, error) {
	return NewAITypeMapperFromSecrets()
}
