# Public schema DDL API

`github.com/johndauphine/smt/schema` is the supported library surface for
deterministic schema, table, and column DDL. It uses SMT's existing renderer
and canonical type mapper; its input and result types do not expose `internal/*`
packages or database handles. It does not load a database driver or require a
PostgreSQL client dependency.

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

`SourceDialect` is optional, but callers should set it whenever it is known:
some names have source-specific meanings, such as MySQL `TINYINT(1)` and
`TIMESTAMP`. `Column` includes the source type metadata needed by the public
canonical mapper: length, precision/scale, fractional-seconds precision,
unsignedness, enum values, and spatial metadata. Set `HasDefault` when an
empty `DefaultExpression` still represents a source `DEFAULT` clause.

## Dialects and capability checks

A new `schema.Registry` starts with these dialects and aliases:

| Dialect | Aliases | Schema DDL | Table/column DDL |
| --- | --- | --- | --- |
| `postgres` | `postgresql`, `pg` | yes | yes |
| `mssql` | `sqlserver`, `sql-server`, `sql_server` | yes | yes |
| `mysql` | `mariadb`, `maria` | yes | yes |
| `sqlite` | `sqlite3` | no | yes |
| `clickhouse` | `click-house` | database | yes |

Inspect `renderer.Capabilities()` before using identities, defaults, computed
columns, or a named schema. If input requests an unsupported feature, rendering
returns `*schema.UnsupportedFeatureError`; SMT never silently drops it.

SQLite's named-schema creation is deliberately unsupported. ClickHouse supports
the practical create-table subset: it emits `MergeTree` with `ORDER BY` set to
the primary-key columns (or `tuple()` when there is no primary key). ClickHouse
nullability is rendered as `Nullable(T)`, where `T` comes from the canonical
type mapper. The canonical package never represents nullability itself.

ClickHouse `PRIMARY KEY` is a sparse sorting index, not a uniqueness constraint,
so `CreateTable` reports a `primary-key-not-unique` warning whenever a primary
key is supplied.

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
without importing SMT internals. Registries do not share mutable global state.
