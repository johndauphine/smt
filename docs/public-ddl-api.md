# Public schema DDL API

`github.com/johndauphine/smt/schema` is the supported library surface for
deterministic schema, table, column, index, constraint, and foreign-key DDL. It uses SMT's
existing renderer and canonical type mapper; its input and result types do not
expose `internal/*` packages or database handles. It does not load a database
driver or require a PostgreSQL client dependency.

## Module consumption

Require the module as `github.com/johndauphine/smt`; the public canonical type
API is `github.com/johndauphine/smt/schema/canonical`. SMT declares Go 1.25.7.
Downstream DMT currently declares Go 1.25.0, so its module directive must be
raised to Go 1.25.7 when it adds this dependency. No `replace` directive is
needed after an SMT release is available.

```go
renderer, err := schema.NewRenderer(schema.Options{
    TargetDialect: "clickhouse",
    Schema:        "analytics",
    SourceDialect: "postgres",
})
if err != nil {
    return err
}

result, err := renderer.CreateTable(schema.Table{
    Name: "events",
    Columns: []schema.Column{
        {Name: "event_id", DataType: "bigint"},
        {Name: "attributes", DataType: "json", IsNullable: true},
    },
    PrimaryKey: []string{"event_id"},
})
if err != nil {
    return err
}
fmt.Println(result.SQL)
for _, warning := range result.Warnings {
    fmt.Println(warning.Reason)
}
```

For a complete create path, use `PlanCreate`. It returns ordered public
`schema.Statement` values (`create_schema`, then `create_table`) whose SQL can
be executed verbatim by the caller. SMT does not open a connection or choose
retry/concurrency policy for a plan.

```go
plan, err := renderer.PlanCreate([]schema.Table{tableA, tableB})
if err != nil {
    return err
}
for _, statement := range plan.Statements {
    if err := target.ExecRaw(ctx, statement.SQL); err != nil {
        return err
    }
}
```

The create plan deliberately contains only schema/database and table artifacts
(including inline primary keys). Use the standalone side-object methods after
the table statements in the execution order your application chooses:

```go
orders := schema.TableRef{
    Name: "orders",
    Columns: []schema.Column{
        {Name: "id", DataType: "bigint"},
        {Name: "email", DataType: "varchar", MaxLength: 255},
    },
}

// This is a unique index, not a UNIQUE constraint.
index, err := renderer.CreateIndex(orders, schema.Index{
    Name: "ix_orders_email", Columns: []string{"email"}, IsUnique: true,
})
if err != nil {
    return err
}

// A named UNIQUE constraint is a distinct database object.
unique, err := renderer.CreateUniqueConstraint(orders, schema.UniqueConstraint{
    Name: "uq_orders_email", Columns: []string{"email"},
})
if err != nil {
    return err
}

check, err := renderer.CreateCheckConstraint(orders, schema.CheckConstraint{
    Name: "ck_orders_id", Expression: "id > 0",
})
if err != nil {
    return err
}

foreignKey, err := renderer.CreateForeignKey(orders, schema.ForeignKey{
    Name:       "fk_orders_accounts",
    Columns:    []string{"account_id"},
    RefSchema:  "identity",
    RefTable:   "accounts",
    RefColumns: []string{"id"},
    OnDelete:   schema.ReferentialActionCascade,
    OnUpdate:   schema.ReferentialActionNoAction,
})
if err != nil {
    return err
}
for _, result := range []schema.Result{index, unique, check, foreignKey} {
    if err := target.ExecRaw(ctx, result.SQL); err != nil {
        return err
    }
}
```

`TableRef.Columns` is optional for named primary/unique constraints. Provide it
for a filtered-index predicate or check expression when source column types are
needed for deterministic translation (for example boolean conventions).
`CreatePrimaryKey` uses an explicit name when supplied, otherwise it derives
`pk_<table>` using the same deterministic naming convention as `CreateTable`.

`CreateForeignKey` adds one named standalone foreign key. `Columns` and
`RefColumns` are positional and must have the same non-zero length, so composite
keys are supported directly. The local table uses the renderer's configured
schema; set `RefSchema` to qualify the referenced table, or leave it empty to
use that same schema. `OnDelete` and `OnUpdate` use `ReferentialAction` values.
The empty value omits the clause, while `ReferentialActionNoAction` emits an
explicit `NO ACTION` clause. SQL Server rejects `RESTRICT`, and MySQL rejects
`SET DEFAULT`, with `*schema.UnsupportedFeatureError` rather than emitting
invalid target SQL.

Alter, drop, and scheduling remain outside this public milestone.

`SourceDialect` is optional, but callers should set it whenever it is known:
some names have source-specific meanings, such as MySQL `TINYINT(1)` and
`TIMESTAMP`. `Column` includes the source type metadata needed by the public
canonical mapper: length, precision/scale, fractional-seconds precision,
unsignedness, enum values, and spatial metadata. Set `HasDefault` when an
empty `DefaultExpression` still represents a source `DEFAULT` clause.

## Dialects and capability checks

A new `schema.Registry` starts with these dialects and aliases:

| Dialect | Aliases | Schema/table/column | Secondary indexes | Standalone PK / named UNIQUE / CHECK / FK |
| --- | --- | --- | --- | --- |
| `postgres` | `postgresql`, `pg` | yes | yes | yes |
| `mssql` | `sqlserver`, `sql-server`, `sql_server` | yes | yes | yes |
| `mysql` | `mariadb`, `maria` | yes | yes | yes |
| `sqlite` | `sqlite3` | no schema / yes table-column | yes | no |
| `clickhouse` | `click-house` | database / yes table-column | no | no |

Inspect `renderer.Capabilities()` before using identities, defaults, computed
columns, a named schema, or a side-object feature. The side-object fields are
`SecondaryIndexes`, `StandalonePrimaryKeys`, `NamedUniqueConstraints`,
`CheckConstraints`, `StandaloneForeignKeys`, `IndexExpressionKeys`,
`IndexPrefixLengths`, `IndexIncludeColumns`, and `FilteredIndexes`. If input
requests an unsupported feature, rendering returns
`*schema.UnsupportedFeatureError`; SMT never silently drops it.

PostgreSQL supports expression-key, included-column, and filtered indexes.
SQL Server supports included-column and filtered indexes. MySQL supports
expression-key and prefix-length index parts. SQLite supports ordinary/unique
and filtered indexes only. ClickHouse side objects are explicitly unsupported:
its table primary key is a MergeTree sorting key rather than a standalone
relational constraint, and its secondary-index model needs parameters that are
outside this API.

SQLite's named-schema creation is deliberately unsupported when called through
`CreateSchema`. `PlanCreate` follows DMT's established SQLite create behavior:
it treats a configured schema as connection selection and emits unqualified
table DDL. SQLite identities are supported through `CreateTable` only when the
identity is the sole primary-key column, using `INTEGER PRIMARY KEY
AUTOINCREMENT`; a standalone identity `CreateColumn` returns an explicit
unsupported-feature error. SQLite cannot add a standalone foreign-key
constraint through `ALTER TABLE`, so `CreateForeignKey` returns an explicit
unsupported-feature error. ClickHouse supports
the practical create-table subset: it emits `MergeTree` with `ORDER BY` set to
the primary-key columns (or `tuple()` when there is no primary key). ClickHouse
nullability is rendered as `Nullable(T)`, where `T` comes from the canonical
type mapper. The canonical package never represents nullability itself.

ClickHouse `PRIMARY KEY` is a sparse sorting index, not a uniqueness constraint,
so `CreateTable` reports a `primary-key-not-unique` warning whenever a primary
key is supplied. `CreatePrimaryKey` is unsupported for ClickHouse rather than
pretending that table behavior can be added as a relational constraint.

ClickHouse does not allow `Nullable(Array(...))`, `Nullable(Map(...))`, or a
nullable column in its `PRIMARY KEY` / `ORDER BY` expression. The public API
rejects each of those inputs with `*schema.UnsupportedFeatureError`; it never
rewrites an array to `Array(Nullable(...))`, because that changes semantics.

## Warnings and unknown types

`Result.Warnings` returns canonical mapping warnings with table/column context,
plus target-semantics warnings such as the ClickHouse primary-key caveat. The
rendered SQL contains no implicit fallback comments.

The default `UnknownTypeFail` policy rejects unmappable types. Set
`UnknownTypeWarn` or `UnknownTypeTextFallback` to use the dialect's conservative
text fallback; the result then contains an `unknown-type-fallback` warning.

## Custom dialects

Applications can register a `schema.Dialect` implementation into an isolated
registry and select it by its name or aliases:

```go
registry := schema.NewRegistry()
if err := registry.Register(myDialect{}); err != nil {
    return err
}
renderer, err := registry.NewRenderer(schema.Options{TargetDialect: "my-db"})
```

The dialect interface receives only public `schema.Request`, `schema.Table`,
and `schema.Column` values and returns `schema.Result`; it is safe to implement
without importing SMT internals. A custom dialect can additionally implement
`schema.SideObjectDialect` to render indexes and constraints, and
`schema.ForeignKeyDialect` to render standalone foreign keys. These optional
extensions are separate so existing custom side-object dialects remain source
compatible. Registries do not share mutable global state.
