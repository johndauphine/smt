// These positional literals pin every exported value-struct layout in the
// released public API. Registry and Renderer intentionally have private fields.
package legacystructliterals

import (
	"github.com/johndauphine/smt/schema"
	"github.com/johndauphine/smt/schema/canonical"
)

var (
	_ = schema.Request{"public", "mssql", schema.UnknownTypeFail}
	_ = schema.Options{"postgres", "public", "mssql", schema.UnknownTypeFail}
	_ = schema.Result{"SQL", nil}
	_ = schema.Plan{nil}
	_ = schema.Warning{"kind", "reason", "mssql", "postgres", "table", "column"}
	_ = schema.Column{"id", "int", 0, 0, 0, nil, false, false, false, 0, "", false, "", false, "", false, nil, 0, ""}
	_ = schema.Table{"table", nil, nil}
	_ = schema.TableRef{"table", nil}
	_ = schema.Index{"index", nil, nil, nil, false, nil, ""}
	_ = schema.PrimaryKey{"pk", nil}
	_ = schema.ForeignKey{"fk", nil, "public", "parent", nil, schema.ReferentialActionCascade, schema.ReferentialActionNoAction}
	_ = schema.UniqueConstraint{"uq", nil}
	_ = schema.CheckConstraint{"ck", "id > 0"}
	_ = schema.UnsupportedFeatureError{"postgres", "feature"}
	_ = schema.Batch{nil, nil, false, nil}
	_ = schema.DropOptions{false}
	_ = schema.TruncateOptions{false}
	_ = schema.ConstraintRef{"ck", schema.ConstraintCheck}
	_ = schema.Evolution{schema.EvolutionAddColumn, schema.TableRef{}, schema.Column{}, "", schema.ConstraintRef{}, schema.DropOptions{}, schema.TruncateOptions{}}
	_ = canonical.CanonicalType{canonical.Unknown, 0, 0, 0, nil, false, false, false, false, nil, nil, "", "", 0, ""}
	_ = canonical.TypeMeta{0, 0, 0, nil, false, 0, nil, 0, ""}
	_ = canonical.RenderOpts{false}
	_ = canonical.MappingWarning{"kind", "postgres", "reason"}
)

// No optional interfaces are required of an existing external dialect.
type oldDialect struct{}

var _ schema.Dialect = oldDialect{}

func (oldDialect) Name() string      { return "external-legacy" }
func (oldDialect) Aliases() []string { return nil }
func (oldDialect) Capabilities() schema.Capabilities {
	return schema.Capabilities{CreateSchema: true, CreateTable: true, CreateColumn: true}
}
func (oldDialect) RenderSchema(schema.Request) (schema.Result, error) {
	return schema.Result{"SCHEMA", nil}, nil
}
func (oldDialect) RenderTable(schema.Request, schema.Table) (schema.Result, error) {
	return schema.Result{"TABLE", nil}, nil
}
func (oldDialect) RenderColumn(schema.Request, schema.Column) (schema.Result, error) {
	return schema.Result{"COLUMN", nil}, nil
}
