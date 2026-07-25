package postgres

import (
	pgddl "github.com/johndauphine/smt/internal/ddl/postgres"
	"github.com/johndauphine/smt/internal/driver"
)

// The deterministic DDL implementation lives in internal/ddl/postgres so it
// can be used by the public schema API without loading this database driver.
// These wrappers preserve the historic driver-package entry points for
// existing callers; the CLI reaches the same renderer through internal/ddl.

func RenderCreateTableDDLWithSource(t *driver.Table, targetSchema string, unlogged bool, unknownTypePolicy, sourceDialect string) (string, map[string]string, error) {
	return pgddl.RenderCreateTableDDLWithSource(t, targetSchema, unlogged, unknownTypePolicy, sourceDialect)
}

func RenderColumnDefinitionWithSource(col driver.Column, tableColumns []driver.Column, unknownTypePolicy, sourceDialect string) (string, error) {
	return pgddl.RenderColumnDefinitionWithSource(col, tableColumns, unknownTypePolicy, sourceDialect)
}

func RenderColumnTypeWithPolicy(col driver.Column, unknownTypePolicy string) (string, error) {
	return pgddl.RenderColumnTypeWithPolicy(col, unknownTypePolicy)
}

func RenderColumnTypeWithSource(col driver.Column, unknownTypePolicy, sourceDialect string) (string, error) {
	return pgddl.RenderColumnTypeWithSource(col, unknownTypePolicy, sourceDialect)
}

func RenderColumnDefaultDDLWithSource(col driver.Column, unknownTypePolicy, sourceDialect string) (string, error) {
	return pgddl.RenderColumnDefaultDDLWithSource(col, unknownTypePolicy, sourceDialect)
}

func RenderCreateIndexDDLWithSource(t *driver.Table, idx *driver.Index, targetSchema, sourceDialect string) (string, error) {
	return pgddl.RenderCreateIndexDDLWithSource(t, idx, targetSchema, sourceDialect)
}

func RenderCreateForeignKeyDDL(t *driver.Table, fk *driver.ForeignKey, targetSchema string) (string, error) {
	return pgddl.RenderCreateForeignKeyDDL(t, fk, targetSchema)
}

func RenderCreateCheckConstraintDDL(t *driver.Table, chk *driver.CheckConstraint, targetSchema string) (string, error) {
	return pgddl.RenderCreateCheckConstraintDDL(t, chk, targetSchema)
}

func RenderCreateCheckConstraintDDLWithSource(t *driver.Table, chk *driver.CheckConstraint, targetSchema, sourceDialect string) (string, error) {
	return pgddl.RenderCreateCheckConstraintDDLWithSource(t, chk, targetSchema, sourceDialect)
}
