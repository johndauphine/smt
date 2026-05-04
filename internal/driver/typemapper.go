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

// TableTypeMapper handles table-level DDL generation using AI.
// Unlike TypeMapper which maps individual columns, TableTypeMapper receives
// complete source table metadata and generates full target DDL.
// This provides better context for AI to make smart decisions about:
// - Character vs byte semantics (e.g., VARCHAR2 CHAR for Oracle)
// - Appropriate type sizing based on column semantics
// - Constraint handling across database platforms
type TableTypeMapper interface {
	// GenerateTableDDL generates complete CREATE TABLE DDL for the target database.
	// It takes the full source table metadata and produces target-specific DDL.
	// Returns an error if the AI fails - no fallback, caller must handle failure.
	GenerateTableDDL(ctx context.Context, req TableDDLRequest) (*TableDDLResponse, error)

	// CanMap returns true if this mapper can handle the given conversion.
	CanMap(sourceDBType, targetDBType string) bool

	// CacheTableDDL stores a known-good DDL for the request, replacing any prior
	// cached value. Called by the writer after a CREATE TABLE has successfully
	// executed against the target database — turns a runtime-validated DDL into
	// the entry future GenerateTableDDL calls (without PreviousAttempt) will hit.
	//
	// Without this method, a first-try call that produces bad DDL caches the bad
	// DDL; a subsequent retry with PreviousAttempt produces good DDL but bypasses
	// the cache for both lookup and store, so the bad cached DDL remains and a
	// future migration of the same table-shape would fail again. CacheTableDDL
	// closes that loop by letting the writer re-prime the cache once the database
	// has confirmed the DDL is correct.
	CacheTableDDL(req TableDDLRequest, ddl string)
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

// TableDDLRequest contains all information needed to generate target table DDL.
type TableDDLRequest struct {
	// SourceDBType is the source database type (e.g., "postgres", "mssql").
	SourceDBType string

	// TargetDBType is the target database type (e.g., "oracle", "mysql").
	TargetDBType string

	// SourceTable contains complete table metadata from the source database.
	SourceTable *Table

	// TargetSchema is the schema name in the target database.
	TargetSchema string

	// SourceContext contains metadata about the source database.
	SourceContext *DatabaseContext

	// TargetContext contains metadata about the target database.
	TargetContext *DatabaseContext

	// PreviousAttempt is set on retry calls to feed the AI the prior attempt's
	// DDL plus the database error it produced. Lets the AI see exactly what was
	// rejected and why so it can correct on the next try. When non-nil:
	//   - the prompt builder appends a "PRIOR ATTEMPT FAILED" section
	//   - the cache lookup is skipped (we need a fresh AI call, not the cached
	//     bad answer from the prior attempt)
	//   - the cache store is skipped (the writer is responsible for re-priming
	//     the cache via CacheTableDDL after the retry's DDL successfully executes)
	// Nil on first-try calls — the original cache-and-call path is unchanged.
	PreviousAttempt *TableDDLAttempt

	// Note: Indexes and CHECK constraints are always created separately in Finalize,
	// not included in the initial CREATE TABLE DDL.
}

// TableDDLAttempt records a previous CREATE TABLE attempt that the database
// rejected. Used to give the AI the exact text of its prior wrong answer plus
// the database's complaint about it, in the hope that the next attempt corrects
// the specific defect rather than rerolling at random.
type TableDDLAttempt struct {
	// DDL is the verbatim CREATE TABLE the AI emitted on the prior attempt.
	DDL string

	// Error is the verbatim text of the database error that rejected the DDL.
	Error string
}

// TableDDLResponse contains the generated DDL and metadata.
type TableDDLResponse struct {
	// CreateTableDDL is the complete CREATE TABLE statement for the target database.
	CreateTableDDL string

	// ColumnTypes maps column names to their target types (for reference/logging).
	ColumnTypes map[string]string

	// Notes contains any AI-generated notes about the mapping decisions.
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

// FinalizationDDLMapper handles AI-driven DDL generation for finalization phase.
type FinalizationDDLMapper interface {
	// GenerateFinalizationDDL generates DDL for indexes, foreign keys, or check constraints.
	GenerateFinalizationDDL(ctx context.Context, req FinalizationDDLRequest) (string, error)
}

// DDLType specifies the type of DDL to generate.
type DDLType string

const (
	DDLTypeIndex           DDLType = "index"
	DDLTypeForeignKey      DDLType = "foreign_key"
	DDLTypeCheckConstraint DDLType = "check_constraint"
	DDLTypeDropTable       DDLType = "drop_table"
)

// FinalizationDDLRequest contains information needed to generate finalization DDL.
type FinalizationDDLRequest struct {
	// Type specifies what kind of DDL to generate.
	Type DDLType

	// SourceDBType is the source database type (e.g., "postgres", "mssql").
	SourceDBType string

	// TargetDBType is the target database type (e.g., "oracle", "mysql").
	TargetDBType string

	// Table contains the target table metadata.
	Table *Table

	// TargetSchema is the schema name in the target database.
	TargetSchema string

	// TargetContext contains metadata about the target database.
	TargetContext *DatabaseContext

	// Index contains index metadata (when Type is DDLTypeIndex).
	Index *Index

	// ForeignKey contains FK metadata (when Type is DDLTypeForeignKey).
	ForeignKey *ForeignKey

	// CheckConstraint contains check constraint metadata (when Type is DDLTypeCheckConstraint).
	CheckConstraint *CheckConstraint

	// TargetTableDDL is the CREATE TABLE DDL for the target table.
	// This helps AI understand the actual table structure when generating indexes/FKs.
	TargetTableDDL string

	// PreviousAttempt is set on retry calls (see #29 PR B) and carries the
	// prior failed DDL plus the database error that rejected it. When non-nil,
	// the per-DDL-type prompt builders append a "PRIOR ATTEMPT FAILED" section
	// at the end of the prompt so the AI can correct the specific defect.
	// The finalization mapper has no cache, so unlike TableDDLRequest there's
	// no cache-skip / cache-reprime concern — retries just produce a fresh AI
	// call with the corrective context.
	PreviousAttempt *FinalizationDDLAttempt
}

// FinalizationDDLAttempt records a previous CREATE INDEX / FOREIGN KEY /
// CHECK CONSTRAINT attempt that the database rejected. Mirrors
// TableDDLAttempt's role for the table-creation phase. See #29.
type FinalizationDDLAttempt struct {
	// DDL is the verbatim DDL the AI emitted on the prior attempt.
	DDL string
	// Error is the verbatim text of the database error that rejected it.
	Error string
}

// TableDropDDLMapper handles AI-driven DDL generation for dropping tables.
type TableDropDDLMapper interface {
	// GenerateDropTableDDL generates DDL statement(s) for dropping a table.
	// The AI will generate database-specific syntax that properly handles
	// foreign key constraints and other database-specific requirements.
	GenerateDropTableDDL(ctx context.Context, req DropTableDDLRequest) (string, error)
}

// DropTableDDLRequest contains information needed to generate DROP TABLE DDL.
type DropTableDDLRequest struct {
	// TargetDBType is the target database type (e.g., "mysql", "postgres").
	TargetDBType string

	// TargetSchema is the schema name in the target database.
	TargetSchema string

	// TableName is the name of the table to drop.
	TableName string

	// TargetContext contains metadata about the target database.
	TargetContext *DatabaseContext
}

// GetAITypeMapper returns the global AI type mapper loaded from secrets.
// This is the only type mapper available - all type mapping is done via AI.
func GetAITypeMapper() (TypeMapper, error) {
	return NewAITypeMapperFromSecrets()
}
