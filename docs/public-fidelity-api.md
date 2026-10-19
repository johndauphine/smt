# Opt-in public fidelity rendering

`schema.NewRenderer(schema.Options{...})`, its methods, and direct
`schema/canonical` calls retain their existing behavior. Existing public struct
layouts and custom `schema.Dialect` implementations remain compatible. Fidelity
does not require a database connection, execution policy, or model provider.

Use the separate constructor to request the initial assessed create path:

```go
renderer, err := schema.NewFidelityRenderer(schema.Options{
    TargetDialect: "postgres",
    Schema:        "public",
    SourceDialect: "mssql",
}, schema.FidelityOptions{
    Profile: schema.FidelityChecked,
})
if err != nil {
    return err
}
result, err := renderer.CreateTable(schema.Table{
    Name: "limits",
    Columns: []schema.Column{
        {Name: "level", DataType: "tinyint", IsNullable: true},
        {Name: "payload", DataType: "varbinary", MaxLength: 8, IsNullable: true},
    },
})
if err != nil {
    return err
}
// result.Artifact.SQL is one CREATE TABLE statement.
// Present result.Artifact.Warnings and result.Findings to the caller.
```

The caller owns execution, retries, transactions, and scheduling. No row movement
or data cleanup is performed. Preview and execution should consume the same
returned artifact.

## Profiles and source context

| Profile | Behavior |
| --- | --- |
| Empty or `FidelityLegacy` | Delegate to existing renderer behavior with no new findings. |
| `FidelityChecked` | Enforce supported restrictions and report known unresolved semantics. |
| `FidelityStrict` | Enforce the same restrictions and reject unresolved findings with `*schema.FidelityViolationError`. |

The initial built-in checked/strict pair is SQL Server to PostgreSQL. Known
unsupported source/target pairs return `*schema.UnsupportedFeatureError` at
construction. An omitted source dialect produces `source_context_missing` when
columns are assessed; checked mode reports it, while strict rejects it. A
schema-only request has no column semantics to assess. Unknown profile values
return an error. Strict is a gate over the assessed column semantics; success is
not a proof of equivalence for every database operation or conversion path.

`FidelityOptions.SourceContext` is a new companion value. Its
`ReservedConstraintNames` map uses exact input table names as keys. Supply all
existing or planned constraint and index names that share each table's target
namespace. The renderer reserves these names when naming its generated checks;
it does not render the supplied names as source constraints. Omitted entries
declare no reserved names. The constructor copies the map and its slices, so
subsequent caller changes cannot change a renderer's output.

`UniqueIndexes`, also keyed by exact input table name, supplies source unique
index/constraint metadata for assessment; every entry must set `IsUnique: true`.
Its names also reserve generated-check names. It never emits supplied indexes
or UNIQUE constraints. SQL Server source uniqueness over a nullable key can
accept a different number of NULL-bearing rows than PostgreSQL's ordinary
unique indexes, so checked reports `nullable_unique_semantics` and strict
rejects it. Filters do not establish NULL exclusion without proven expression
semantics. Missing/expression key metadata is unresolved; nullable INCLUDE
columns do not affect uniqueness. Nonnullable unique keys add no such finding.
The constructor copies the metadata and all nested slices.

The public renderer assesses the table and companion facts actually supplied.
Omitted unique-index metadata is not evidence of equivalent source uniqueness
outside that supplied surface. Callers must supply original source facts they
want assessed; `PlanCreate` continues to leave side-object scheduling to them.

```go
options := schema.FidelityOptions{
    Profile: schema.FidelityChecked,
    SourceContext: schema.FidelitySourceContext{
        ReservedConstraintNames: map[string][]string{
            "limits": {"ck_limits_existing", "pk_limits"},
        },
        UniqueIndexes: map[string][]schema.Index{
            "limits": {{Name: "uq_source_level", Columns: []string{"level"}, IsUnique: true}},
        },
    },
}
```

Collation and input-conversion context are not assumed equivalent. Missing
metadata and unresolved behaviors produce deterministic findings with stable
codes, object names, source/target dialects, and reasons. Existing advisory type
mapping warnings remain separate from these findings.

## Initial supported scope

Checked/strict whole-table rendering adds named inline PostgreSQL CHECKs for SQL
Server `tinyint` values in `0..255` and bounded `varbinary` stored byte length.
NULLs retain SQL CHECK's ordinary NULL behavior; `varbinary(MAX)` remains
unbounded. Explicit source casts and parameter conversions may truncate before
assignment and are not emulated; binary conversion therefore remains a finding,
including when a stored-byte bound can be enforced.

Opt-in binary/varbinary literal defaults are parsed into bytes and rendered as
PostgreSQL `decode(..., 'hex')`, including SQL Server's empty `0x` value and
leading-zero treatment of odd hexadecimal lengths. Literal marker/digit case
does not change the bytes. Balanced enclosing catalog parentheses and NULL are
accepted. Unsupported binary default expressions, including casts/functions,
are reported by assessment and refused by the opt-in renderer; they are never
passed through as plausible PostgreSQL SQL. Legacy default output is unchanged.

Other known gaps remain findings: fixed binary padding, legacy datetime rounding
and ranges, UTF-16 length, text comparison/collation, original offsets, precision
7 timestamps, and time input conversion. An exact-length binary CHECK or a time
midnight CHECK is not presented as a substitute for those semantics.

`CreateSchema`, `CreateTable`, and `PlanCreate` form the initial opt-in create
surface. `PlanCreate` preserves input table order and contains only an optional
schema statement and table statements. It never adds side-object statements.
Errors return no partial executable artifact or plan. Opt-in normalized table or
column identifier collisions are rejected before returning SQL.

Standalone column definitions, indexes, constraints, and all schema-evolution
methods on `FidelityRenderer` currently return `*schema.UnsupportedFeatureError`
in checked/strict mode. They delegate to their existing methods in legacy mode.
Use this refusal to detect unsupported lifecycle work before executing a plan;
falling back to the legacy renderer would discard the requested fidelity policy.

## Custom dialects and consumer gate

`Registry.NewFidelityRenderer` accepts existing custom dialects in legacy mode.
Checked/strict custom support is optional through `schema.FidelityDialect`, which
advertises profiles and implements whole-table rendering with public values.
Strict results containing findings are rejected by the wrapper, and custom
errors discard any accompanying SQL. Required methods on `schema.Dialect`,
`SideObjectDialect`, and `EvolutionDialect` are unchanged.

The DMT consumer check is repeatable on Linux or WSL with the required Go
toolchain on PATH:

```sh
bash testdata/check-dmt-consumer.sh /path/to/DMT
```

It archives DMT's HEAD into a disposable directory, records revisions and
candidate-file hashes, and runs its adapter, driver, and writer package tests
with `-short`, plus its CLI build. It compares the pinned dependency baseline
with a frozen local SMT replacement using temporary module files. DMT's source,
`go.mod`, and `go.sum` remain unchanged. Logs and binaries are written only to the
ignored `.acceptance-artifacts/dmt-consumer/` directory by default.
