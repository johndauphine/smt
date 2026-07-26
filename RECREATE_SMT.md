# Reconstruction Prompt: SMT’s Deterministic Schema-Migration and DDL Subsystem

Use this document as the complete build specification for recreating SMT’s
schema-migration and deterministic DDL capability inside a new data migration
tool. Assume you do **not** have access to the original repository. The result
may be implemented in any language. Preserve the observable contracts below;
do not imitate an implementation language or directory layout merely for
historical similarity.

## 1. How to interpret this specification

The words **MUST**, **MUST NOT**, **SHOULD**, and **MAY** are normative.

- Sections labeled **Required contract** define behavior that a faithful
  reconstruction must expose.
- Sections labeled **Implementation guidance** are suggestions. You may choose
  different abstractions if all observable behavior and acceptance criteria
  remain true.
- The final appendix records identifiers and tooling from the current reference
  implementation. It is non-normative and exists only to help compatibility
  work.

You are explicitly invited to improve the internal design. Data structures,
module boundaries, registries, parsing strategy, rendering strategy, algorithms,
concurrency model, and implementation language are free choices unless a
specific mechanism is required for file/API interoperability, deterministic
compatibility, or safety. A faithful reconstruction is judged by inputs,
outputs, errors, artifacts, and acceptance tests—not by internal resemblance.

If two requirements appear to conflict, use this priority order:

1. Never emit or apply plausible but semantically unsupported SQL.
2. Preserve schema fidelity and deterministic output.
3. Preview and apply the same immutable rendered plan.
4. Preserve stable public, CLI, and persisted-artifact contracts.
5. Prefer operator-visible refusal over silent degradation.

## 2. Product mission and decision rationale

### Required contract

Build a schema-only subsystem that:

1. Introspects a source database schema.
2. Preserves source type and expression meaning through one consistent
   cross-dialect semantic contract.
3. Renders deterministic target-dialect DDL.
4. Reports drift between the source-derived desired schema and a live target.
5. Captures source-schema snapshots and plans incremental schema evolution.
6. Writes reviewable SQL and provenance artifacts before any apply.
7. Applies DDL only after an explicit operator action.
8. Supports PostgreSQL, SQL Server, and MySQL as both CLI sources and CLI
   targets.
9. Exposes a connection-free public DDL surface for PostgreSQL, SQL Server,
   MySQL, SQLite, and ClickHouse, with exact capability reporting.

The central product decision is **deterministic first**. Executable DDL is
derived from catalog metadata and deterministic rules. An AI provider is not
required for ordinary create, drift, snapshot, or sync work. Optional AI may
parse already-rendered DDL for review or diagnose failures, but it must not
silently generate, patch, or replace the executable plan.

This decision exists because schema fidelity is mechanical: exact lengths,
precision, scale, nullability, identity, timezone class, default semantics,
computed-column semantics, constraint structure, and index shape can and must
be tested. A probabilistic author is unsuitable as the source of executable
DDL.

## 3. Boundary with the surrounding data migration tool

### Required contract

Treat the reconstructed component as the schema/DDL half of a larger data
migration product.

The schema component owns:

- schema catalog introspection;
- the abstract schema model;
- canonical type and expression translation;
- identifier policy;
- create-plan rendering;
- structural diff, drift, risk classification, and evolution rendering;
- SQL artifact and manifest production;
- schema snapshot serialization;
- DDL execution contracts, including ordering, refusal gates, and recovery
  records;
- optional inspection of deterministic DDL.

The surrounding data migration tool owns:

- reading and writing table rows;
- chunking, pagination, worker pools, bulk-load protocols, and buffer sizing;
- row-transfer retry and resume;
- data validation and row-count validation;
- runtime transfer tuning;
- scheduling schema work relative to data work;
- connection lifecycle when it consumes the connection-free public DDL API.

The schema component MUST NOT introduce row transfer, chunk-level progress,
worker-pool tuning, read-ahead/write-ahead behavior, or data-copy SQL into its
own architecture.

The host tool MAY schedule table creation before row loading and secondary
indexes/foreign keys/checks afterward. The renderer must therefore expose
tables and standalone side objects independently. When the standalone SMT CLI
builds a complete create plan, its fixed order is:

1. schema or database;
2. tables, with columns and inline primary keys;
3. secondary indexes;
4. foreign keys;
5. check constraints.

The host must never ask an AI to regenerate SQL that the deterministic
renderer has already produced.

## 4. Explicit non-goals

The reconstruction MUST NOT:

- move rows;
- infer table or column renames; an apparent rename is a drop plus an add;
- silently drop unsupported type, column, index, or constraint facts;
- invent a table rebuild when an in-place evolution operation is unsupported;
- infer `CASCADE`;
- claim all-or-nothing cross-dialect DDL transactions;
- pass an unknown cross-dialect function through unchanged;
- use count-only checks as proof of schema fidelity;
- make a live database connection part of the public DDL rendering API;
- expose SQLite or ClickHouse as full CLI source/target drivers unless those
  drivers are separately implemented and accepted;
- treat ClickHouse primary-key syntax as a relational uniqueness constraint;
- make optional AI availability a prerequisite for deterministic operation.

A general SQL parser, query planner, and full expression AST are also
non-goals. The expression grammar should stay deliberately small and
fail-closed.

## 5. Architectural invariants

### Required contract

1. **Render once.** Preview and apply MUST consume the same ordered plan object.
   Do not render one script for review and independently render another for
   execution.
2. **Immutable plan semantics.** A plan is an ordered list of statements plus
   metadata. Execution may skip an already-existing create object by catalog
   check, but it must not rewrite statement SQL.
3. **Stable ordering.** The same schema model, configuration, renderer version,
   and dialects MUST yield byte-identical SQL, warnings, and fingerprints.
   Parallel rendering is allowed only if output slots preserve deterministic
   order.
4. **Independently testable behavior.** Structural comparison, dialect
   rendering, catalog access, and risk classification MUST be testable without
   relying on an AI provider. Their exact code boundaries are not prescribed.
5. **Source context survives.** Type and expression translation MUST carry the
   canonical source dialect through to the target renderer.
6. **Capability-gated behavior.** Unsupported public operations MUST return a
   machine-detectable unsupported-feature error before emitting SQL.
7. **Fail closed.** Unknown type and expression behavior follows explicit
   policy; it is never guessed.
8. **One identifier policy.** Create, sync, drift, existence checks, and public
   rendering MUST share the same target identifier normalization.
9. **Artifacts first.** Apply paths MUST persist SQL and manifest artifacts
   before the first DDL statement executes.
10. **Stop on failure.** Required statements execute sequentially and stop at
    the first failure. The error identifies the 1-based statement number,
    description, database error, and SQL text.

### Implementation guidance

A practical—but optional—decomposition is:

- database driver registry and schema reader/writer interfaces;
- abstract schema model;
- canonical type mapper;
- expression intermediate representation;
- deterministic create/evolution renderer;
- structural snapshot diff;
- live-target drift and live sync diff;
- orchestrator and artifact writer;
- state/history/profile layer;
- CLI and optional UI;
- optional AI review/diagnostic adapters.

These are logical responsibilities, not required modules or packages. Combine,
split, or replace them when an alternative design is clearer and still
satisfies every contract.

## 6. Database driver and dialect conventions

### Required contract

The CLI’s externally visible driver selection MUST:

- resolve driver names and aliases case-insensitively;
- expose only canonical names when listing available drivers;
- support PostgreSQL aliases `postgresql` and `pg`;
- support SQL Server aliases `sqlserver`, `sql-server`, and `sql_server`;
- support MySQL aliases `mariadb` and `maria`;
- expose driver defaults such as port, default schema, connection options, and
  canonical dialect name.

Driver discovery SHOULD be extensible without edits throughout orchestration
and configuration code. A registry, dependency injection, generated dispatch,
or another mechanism is acceptable.

The database-access layer MUST be able to:

- perform full table/column/primary-key extraction for one schema;
- separate loading of secondary indexes, foreign keys, and check constraints;
- row counts only as catalog metadata, not as a transfer feature;
- database identity/context metadata for diagnostics or optional review.

The target-access layer MUST be able to:

- ping and raw statement execution;
- table existence checks;
- named index, foreign-key, and check-constraint existence checks;
- raw scalar queries needed by catalog checks;
- optional retrieval of an existing table’s DDL for diagnostics.

The selected dialect behavior MUST provide identifier quoting, schema/table
qualification, parameter placeholders, and connection-string construction.
Executable DDL behavior must remain deterministic regardless of how this logic
is internally grouped.

For a new CLI database engine, it MUST become discoverable before configuration
validation and provide both source-introspection and target-execution behavior.

## 7. Abstract schema model

### Required contract

Whatever internal representation is chosen, the component MUST accept,
preserve, compare, and serialize at least these facts. The groupings below are
semantic records, not prescribed classes or structs.

### Table

- source schema name;
- table name;
- ordered columns;
- ordered primary-key columns;
- secondary indexes;
- foreign keys;
- check constraints;
- optional non-DDL statistics such as row count and estimated row size.

Statistics MUST be excluded from schema fingerprints and structural equality.

### Column

- name and source catalog type name;
- maximum length;
- numeric precision and scale;
- optional fractional-seconds precision, where “unspecified” differs from zero;
- nullability;
- identity/auto-increment status;
- unsignedness;
- MySQL display width, retained only where semantically needed for the
  `tinyint(1)` boolean convention;
- ordinal position;
- default presence separately from default expression text, so an explicitly
  empty default is distinguishable from no default;
- raw `ON UPDATE` expression;
- computed/generated status, expression, and persisted/stored versus virtual
  class;
- ordered enum/set members;
- spatial reference identifier and spatial subtype.

Sample values MAY exist for optional diagnostics, but MUST NOT affect
deterministic DDL, equality, or fingerprints.

### Index

- name;
- ordered key parts;
- a positional marker for expression key parts;
- positional prefix lengths;
- uniqueness;
- clustered status if introspected;
- ordered included columns;
- optional filter/predicate.

### Foreign key

- name;
- ordered local columns;
- referenced schema and table;
- ordered referenced columns;
- `ON DELETE` and `ON UPDATE` actions.

Local and referenced column lists are positional and must have the same
non-zero length.

### Check constraint

- name;
- predicate definition.

Model transformations such as identifier normalization, target-schema
retargeting, and management-scope filtering MUST NOT mutate stored snapshots or
caller input, whether immutability is achieved by copying, persistent data
structures, ownership rules, or another technique.

## 8. Canonical type system

### Required contract

Type translation MUST behave as though it passes through a single
dialect-neutral semantic contract:

`source catalog type + structured metadata + source dialect`
→ `canonical type`
→ `target DDL type + warnings`.

The implementation MAY use tables, pattern matching, generated code, algebraic
types, classes, or another design. However, different source-target paths MUST
not drift in their meaning or fidelity rules; all paths must satisfy the same
canonical semantics and acceptance corpus.

The public canonical-mapping behavior and internal translation logic MUST be
able to distinguish:

- boolean;
- fixed and variable bit strings with bit length;
- tiny, small, medium, regular, and big integers;
- exact decimal with precision and scale;
- real and double floating point;
- bounded variable character, fixed character, and unbounded text;
- fixed binary, variable binary, and binary large object;
- date;
- time and timestamp with optional fractional-seconds precision and timezone
  awareness;
- MySQL’s UTC-normalized native `TIMESTAMP` semantic as distinct from a generic
  naive timestamp, at least for same-dialect round trips;
- UUID, JSON, XML, and SQL Server rowversion;
- enum and set with ordered member values;
- array with a canonical element type;
- spatial family, subtype, and SRID;
- raw/unclassified source type.

Canonical nullability is not part of the type itself. It is a column property.

The mapper MUST preserve, or explicitly warn about loss of:

- exact bounded lengths;
- exact decimal precision and scale;
- fractional-seconds precision;
- timezone awareness;
- unsigned range;
- national/Unicode character semantics;
- bit-string width;
- enum/set values;
- array element type;
- spatial family/subtype/SRID;
- identity-compatible physical type constraints.

Important mapping behavior includes:

- MySQL `tinyint(1)` maps as boolean when source metadata confirms display
  width 1; multi-bit strings do not collapse to boolean.
- MySQL native `TIMESTAMP` remains distinguishable on MySQL-to-MySQL work.
- PostgreSQL arrays retain their element type and element parameters.
- SQL Server `datetime2` remains timezone-naive; SQL Server
  `datetimeoffset` and PostgreSQL `timestamptz` remain timezone-aware.
- A timezone-aware source maps to MySQL `TIMESTAMP`, while a naive timestamp
  maps to `DATETIME`.
- Unsigned integers widen deterministically when the target lacks an unsigned
  equivalent. Unsigned 64-bit values must not be narrowed to signed 64-bit.
- PostgreSQL has no 8-bit integer, so a tiny integer widens.
- PostgreSQL character and binary “unbounded” sentinels are handled
  semantically, not as literal negative lengths.
- MySQL enum/set targets require member values. Missing values follow the
  unknown-type policy; they are not fabricated.
- Same-dialect MySQL work preserves native text/blob capacity tiers where a
  generic cross-dialect mapping would flatten them.
- Identity columns use the target’s identity form without accidentally
  rendering identity syntax inside an `ALTER COLUMN TYPE`.

### Unknown-type policies

Support exactly these policy values:

- `fail` — default; return an error and emit no fallback SQL;
- `warn` — render a conservative target text type and emit an
  `unknown-type-fallback` warning;
- `text_fallback` — same conservative text fallback and warning, with the
  policy name making the operator’s intent explicit.

Warnings MUST carry reason, source dialect, target dialect, and table/column
context where available. A raw SQL fragment MUST NOT hide fallback decisions in
embedded comments; an outer plan serializer may render structured warnings as
review comments.

## 9. Expression translation and equivalence

### Required contract

Defaults and checks MUST have one consistent semantic translation and
equivalence contract across every CLI target. A small expression IR is the
proven reference design, but an AST, parser-combinator pipeline, normalized
token model, or another deterministic approach is acceptable.

The observable translation/comparison behavior MUST cover:

- string, number, boolean, and null literals;
- identifiers;
- semantic function categories for local current date/time, UTC current time,
  current date, current time, UUID generation, coalesce, concatenation, and a
  small pass-through set;
- unary `NOT`, numeric negation, `IS NULL`, and `IS NOT NULL`;
- comparison, boolean, and basic arithmetic binary operators;
- `IN` and `NOT IN`;
- `LIKE`, `NOT LIKE`, and supported regex forms;
- date/time casts relevant to default equivalence;
- `AT TIME ZONE`;
- explicit grouping;
- a deterministic unsupported/raw outcome for unrecognized input.

At minimum, normalize these equivalence families:

- `GETDATE`, `SYSDATETIME`, `NOW`, and `CURRENT_TIMESTAMP`;
- `GETUTCDATE`, `SYSUTCDATETIME`, and `UTC_TIMESTAMP`;
- `CURRENT_DATE` and `CURDATE`;
- `CURRENT_TIME`, `CURTIME`, and `LOCALTIME`;
- `NEWID`, `NEWSEQUENTIALID`, `UUID`, `gen_random_uuid`, and
  `uuid_generate_v4`;
- `ISNULL`, `COALESCE`, and `IFNULL`;
- `CONCAT(...)` and string `||`;
- boolean `0/1` and `false/true` when column context establishes a boolean;
- PostgreSQL `x = ANY(ARRAY[...])` and portable `x IN (...)`;
- class-relevant `CONVERT(date, x)`, `CAST(x AS date)`, `::date`, and
  corresponding time forms.

Rendering preserves the distinction between local-now and UTC-now so target
SQL keeps UTC intent. For historical drift/review compatibility, semantic
default comparison collapses both families into the same `current_dt` class.
Current date and current timestamp remain distinct.

Rendering MUST use column context for boolean, text, timezone-aware, JSON,
array, and fractional-seconds behavior. Check rendering MUST use table-column
context for identifier and boolean rewriting.

Forms outside the supported grammar MUST fail unless an additional
deterministic translator handles them. Every cross-dialect path MUST reject
unknown function calls. Function-looking text inside string literals MUST
remain untouched.

Same-dialect expressions MAY be preserved verbatim when they are valid in the
same dialect. Cross-dialect vendor functions such as an unimplemented
`DATEADD(...)` MUST never pass through as a guess.

MySQL string literals MUST correctly escape backslashes and control characters.
Avoid output that turns a double unary minus into a SQL line-comment prefix.

Computed-column and filtered-index expressions MAY use narrower deterministic
rewrites than defaults/checks, but retain the same fail-closed unknown-function
invariant.

## 10. Identifier normalization and quoting

### Required contract

All create, sync, drift, public rendering, and existence checks MUST use one
identifier policy.

For PostgreSQL:

- lowercase the identifier;
- replace characters other than Unicode letters, digits, and underscore with
  underscore;
- prefix an identifier that starts with a digit with `col_`;
- use `col_` for an empty normalized identifier;
- limit to 63 UTF-8 bytes;
- when truncating, retain a valid UTF-8 prefix and append `_` plus eight
  lowercase hexadecimal digits derived from a stable CRC-32 of the full
  normalized name;
- fail before rendering if two distinct source tables, or two distinct columns
  in one table, normalize to the same target name.

SQL Server and MySQL preserve identifier case/name by default.

Quote identifiers using double quotes for PostgreSQL and SQLite, brackets for
SQL Server, and backticks for MySQL and ClickHouse. Always qualify with the
configured target schema/database when that dialect and operation support it.

## 11. Deterministic create rendering

### Required contract

A create plan statement contains:

- target-normalized table/object name where applicable;
- human description;
- executable SQL without a trailing semicolon;
- risk;
- optional risk notes and mapping warnings;
- object kind and object name for idempotent apply checks.

The serialized script adds:

```text
-- [<risk>] <description>
-- note: <optional note>
-- warning: <optional warning>
<SQL>;
```

Create-schema behavior:

- empty configured schema is a no-op;
- PostgreSQL uses idempotent schema creation;
- SQL Server uses a catalog guard and dynamic schema creation;
- MySQL and ClickHouse create a database idempotently;
- SQLite named-schema creation is unsupported.

Create-table behavior:

- preserve input column order;
- render a deterministic named primary-key constraint for relational targets;
- default that name to `pk_<target-normalized-table>`;
- PostgreSQL identities use generated identity semantics;
- SQL Server identities use `IDENTITY(1,1)`;
- MySQL identities use `AUTO_INCREMENT`;
- MySQL tables use InnoDB and UTF-8 (`utf8mb4`) defaults;
- defaults are omitted for identity columns;
- computed-column storage class and nullability are preserved where the target
  supports them.

Side objects MUST be rendered only after all tables:

- indexes preserve key order, expression markers, prefix lengths, uniqueness,
  included-column order, and filter presence;
- foreign keys preserve positional composite columns, referenced schema/table,
  and explicit actions;
- named unique constraints remain distinct from unique indexes;
- checks use deterministic expression translation.

Create rendering MAY run concurrently per table, but serialized output MUST
remain stable and dependency ordered.

## 12. Connection-free public DDL API

### Required contract

Expose a library-level renderer that:

- is constructed from target dialect, optional target schema, optional source
  dialect, and unknown-type policy;
- exposes the selected canonical dialect;
- returns explicit create and evolution capabilities;
- accepts only public value objects, not internal database-driver objects;
- performs no database I/O;
- returns SQL plus structured warnings;
- is safe for concurrent read-only use after construction;
- allows isolated custom dialect registries without shared mutable global
  state.

The create surface MUST support these operations:

- create schema/database;
- create table with inline primary key;
- create standalone column definition;
- create secondary index;
- create standalone primary key;
- create standalone foreign key;
- create named unique constraint;
- create named check constraint;
- create an ordered plan containing schema/database and tables only.

The plan operation intentionally excludes standalone side objects so a data
migration host can schedule them independently.

### Built-in create capabilities

| Feature | PostgreSQL | SQL Server | MySQL | SQLite | ClickHouse |
|---|---:|---:|---:|---:|---:|
| Named schema/database creation | yes | yes | yes | no | yes |
| Table and column creation | yes | yes | yes | yes | yes |
| Table primary key input | yes | yes | yes | yes | yes, with caveat |
| Identity columns | yes | yes | yes | table-context only | no |
| Defaults | yes | yes | yes | yes | no |
| Computed columns | yes | yes | yes | no | no |
| Secondary indexes | yes | yes | yes | yes | no |
| Standalone PK / named UNIQUE / CHECK / FK | yes | yes | yes | no | no |
| Expression index keys | yes | no | yes | no | no |
| Prefix-length index parts | no | no | yes | no | no |
| Included columns | yes | yes | no | no | no |
| Filtered indexes | yes | yes | no | yes | no |

Dialect aliases for the public renderer additionally include `sqlite3` for
SQLite and `click-house` for ClickHouse.

Capability violations MUST produce a typed unsupported-feature error. Invalid
input, such as an empty table name, missing key columns, misaligned positional
index metadata, or invalid referential action, produces a validation error
before rendering.

Foreign-key action rules:

- an empty action omits the clause;
- explicit `NO ACTION` emits the clause;
- SQL Server rejects `RESTRICT`;
- MySQL rejects `SET DEFAULT`;
- composite keys are supported.

SQLite special cases:

- a configured schema in a create plan is treated as connection selection and
  table SQL remains unqualified;
- exact auto-increment is supported only when the identity is the sole primary
  key, rendered as `INTEGER PRIMARY KEY AUTOINCREMENT`;
- standalone identity-column rendering is unsupported;
- standalone foreign-key addition is unsupported.

ClickHouse special cases:

- create tables with `MergeTree`;
- use primary-key columns as `ORDER BY`, or `tuple()` when there is no key;
- report a `primary-key-not-unique` warning because this key is a sparse sorting
  index, not a uniqueness constraint;
- render nullable primitives as `Nullable(T)`;
- reject `Nullable(Array(...))` and `Nullable(Map(...))`;
- reject nullable columns in `PRIMARY KEY`/`ORDER BY`;
- never rewrite a composite’s nullability to a different semantic shape;
- reject standalone relational side objects.

## 13. Public schema-evolution API

### Required contract

Expose connection-free, typed operations for:

- drop schema;
- drop table;
- drop index;
- drop named primary-key, unique, foreign-key, or check constraint;
- add column;
- drop column;
- alter column type;
- alter column nullability;
- set column default;
- drop column default;
- truncate table.

Each operation returns an ordered batch, not an execution result. A batch MUST
carry:

- ordered statements;
- cleanup statements to run after required failure where relevant;
- a `requires single physical connection` flag;
- indexes of best-effort statements whose errors are advisory.

The caller owns transactions, retries, live-state checks, and error policy.
Statements within a batch MUST execute in order.

`CASCADE` is never inferred. It is accepted only when explicitly requested and
advertised.

### Built-in evolution capabilities

| Operation | PostgreSQL | SQL Server | MySQL | SQLite | ClickHouse |
|---|---:|---:|---:|---:|---:|
| Drop schema | yes | yes | yes | no | yes |
| Drop schema/table with cascade | yes | no | no | no | no |
| Drop table | yes | yes | yes | yes | yes |
| Drop index | yes | yes | yes | yes | no |
| Drop constraint | yes | yes | yes | no | no |
| Add/drop column | yes | yes | yes | yes | yes |
| Alter type/nullability | yes | yes | yes | no | no |
| Set/drop default | yes | yes | yes | no | no |
| Truncate | yes | yes | yes | emulated | yes |
| Truncate cascade | yes | no | no | no | no |

Required multi-statement contracts:

- MySQL destructive table drop and truncate disable and restore
  `FOREIGN_KEY_CHECKS` on the same physical connection. Cleanup restores the
  setting if the required statement fails.
- SQLite table drop disables and restores `PRAGMA foreign_keys` on the same
  physical connection.
- SQL Server default replacement first drops the catalog-named existing default
  constraint, then adds the deterministic replacement, on one connection.
- SQLite truncation is `DELETE` followed by advisory cleanup of the matching
  `sqlite_sequence` row; sequence cleanup is best effort.

Unsupported in-place operations return the typed unsupported-feature error;
they do not propose a rebuild.

## 14. Snapshot and structural diff

### Required contract

A snapshot is serialized JSON containing:

- `version`;
- `schema`;
- `source_type`;
- `captured_at` in UTC;
- `tables` containing the full table models.

Write snapshot format version `4` for drop-in compatibility. Readers MUST
accept:

- unversioned and versions 0–1;
- version 2, which added unsignedness, enum values, and `ON UPDATE`;
- version 3, which added fractional-seconds precision;
- version 4, which added display width.

When comparing an older snapshot with a current extraction, backfill fields
that did not exist in the old format from the current model before diffing.
This avoids spurious changes. Do not mutate either input.

Stored snapshot lookup is scoped by source dialect/type, source identity, and
source schema. For compatibility, source identity has the exact form
`host=<lowercased-host>;port=<port>;database=<database>`. When reading state
created before source identity was stored, an empty-identity row may be used as
a fallback after an exact-identity lookup.

Structural snapshot comparison MUST:

- match tables, columns, indexes, foreign keys, and checks by name;
- produce added, removed, and changed tables;
- report per-table added/removed/changed columns;
- report primary-key changes;
- report added/removed indexes, foreign keys, and checks;
- preserve stable order;
- treat index/FK/check shape changes as remove plus add;
- ignore row count, estimated row size, sample values, and ordinal position;
- not detect identity changes in snapshot mode;
- compare default presence separately from default text.

Before rendering, apply in this order:

1. include/exclude table scope;
2. structural diff;
3. filter unmanaged object-kind deltas;
4. collision check;
5. target identifier normalization;
6. retarget all table and foreign-key schema references to the configured
   target schema;
7. deterministic evolution rendering.

## 15. Live target drift and live sync diff

### Required contract

Live comparison treats the source schema as desired target shape and
introspects the target as existing shape.

Before comparison:

- apply the same include/exclude scope used by create and sync;
- exclude target tables corresponding to deliberately out-of-scope source
  tables, while retaining genuine target-only tables;
- load only managed side-object kinds;
- normalize desired identifiers for the target;
- retarget desired schema references to the configured target schema.

Tables and columns are matched case-insensitively after normalization.
Column comparison MUST use semantic cross-dialect equivalence, not raw type
strings. Compare at least:

- canonical type;
- exact bounded length;
- exact precision and scale;
- fractional-seconds precision;
- nullability;
- identity;
- timezone class;
- default presence and semantic expression class;
- unsignedness;
- computed-column presence and, where representable, storage class.

Index comparison uses ordered key parts, expression flags, prefix lengths,
uniqueness, included columns, and filter presence. Foreign-key comparison uses
ordered local/reference pairs, referenced table and relative schema, and
normalized actions. Empty action, `NO ACTION`, and `RESTRICT` compare as the
same no-op class for drift. Check predicates are compared by count
cross-dialect; same-dialect predicate changes may be detected but are not
automatically reconciled.

Read-only drift output classifies:

- missing tables/columns;
- extra tables/columns;
- changed columns;
- missing/extra indexes;
- missing/extra foreign keys;
- check-count drift;
- primary-key drift.

“Destructive drift” means at least an extra target table or extra target
column. Drift never modifies either database.

## 16. Deterministic sync plan and risk semantics

### Required contract

Support two baseline modes:

- `target` — default; compare source-derived desired shape with the live target;
- `snapshot` — compare current source with the latest matching stored snapshot;
  planning is offline and opens a target connection only for apply.

Snapshot mode and snapshot saving require the full SQLite state backend; they
are unavailable with the single-run YAML state-file backend.

Snapshot mode cannot see direct target drift. It detects primary-key changes
and refuses them, does not detect identity changes between snapshots, and fails
rendering a computed-column change rather than placing it in a partial
executable plan.

Supported sync changes:

- added tables with managed side objects;
- removed tables;
- added and removed columns;
- supported type/length/precision/scale/nullability/default changes;
- added and removed non-primary indexes;
- added and removed foreign keys;
- named check additions/removals when the exact operation is safely known.

Detected but unsupported:

- primary-key add, drop, or re-key;
- identity/auto-increment changes;
- computed-column presence, expression, or storage-class changes;
- `ON UPDATE` changes;
- same-dialect enum/set member changes;
- spatial SRID changes;
- display-width changes;
- unsafe check reconciliation.

Unsupported changes appear as `-- [unsupported]` entries with table,
description, and reason. Apply MUST refuse the entire plan before executing any
statement if even one unsupported change exists.

Every executable statement has one risk:

- `safe` — expected to be non-destructive;
- `blocking` — may lock, scan, validate, or fail on existing rows;
- `rebuild` — may rewrite a table or fail casts;
- `data-loss-risk` — drops data;
- `unknown` — treated at least as strictly as data loss.

Required examples:

- create table: `safe`;
- drop FK/index/check: `safe`;
- add nullable/defaulted/identity column: normally `safe`;
- add non-null column with no default or identity: `blocking`;
- create index/FK/check: `blocking`;
- set `NOT NULL`: `blocking`;
- type change: `rebuild`;
- drop column/table: `data-loss-risk`.

Apply MUST refuse `data-loss-risk` and `unknown` statements unless the operator
explicitly allows data loss.

Required statement ordering:

1. create all added table definitions;
2. drop removed foreign keys on changed tables;
3. per changed table: drop checks/indexes, add columns, alter columns, drop
   columns, add indexes/checks;
4. create non-FK side objects for added tables;
5. add foreign keys on changed tables;
6. add foreign keys for added tables only after all referenced tables/columns
   exist;
7. drop removed tables children-first.

If removed tables form an FK cycle, drop only cycle-member FKs needed to break
the cycle, then continue children-first. Self-references do not block a table
drop.

When a SQL Server type/default change requires replacing a default, drop the
existing catalog-named default before the type/default operation. When a type
and default both change, do not leave the old default attached during the type
alteration.

## 17. Apply, rerun, and recovery behavior

### Required contract

Preview is the default for create and sync. Apply requires an explicit flag.

For create apply:

- persist the complete SQL and manifest first;
- execute in dependency order;
- check catalog existence for tables, indexes, foreign keys, and checks;
- skip already-existing named objects as shape-unverified, not as proven equal;
- stop at the first required failure;
- mark the run failed and retain artifacts;
- advise drift after recovery because an existence skip does not validate
  shape.

For sync apply:

- create a run record;
- persist migration SQL and manifest first;
- refuse unsupported changes;
- enforce the data-loss gate;
- execute sequentially;
- stop and record the current phase on the first failure;
- save a new baseline only after every required statement succeeds.

Do not wrap an entire create or sync plan in a claimed cross-dialect
transaction. Operators may manually apply the generated SQL inside a
dialect-specific transaction.

After a partial `snapshot`-mode apply failure, leave the baseline unchanged.
Refuse a blind replay against that stale baseline. Recovery is either:

- inspect the failed run artifact, correct the target, and run target-mode
  sync; or
- capture a new source baseline only after the target is known correct.

## 18. CLI contract

### Required contract

The standalone command name is `smt`. With no subcommand, it MAY launch an
interactive UI. Unknown commands return a configuration/usage error.

Stable global flags:

- `--config`, `-c`;
- `--profile`;
- `--state-file`;
- `--log-format` (`text` or `json`);
- `--verbosity` (`debug`, `info`, `warn`, `error`);
- `--shutdown-timeout`;
- `--help`, `-h`;
- `--version`, `-v`.

Stable commands:

- `init`;
- `create`;
- `sync`;
- `drift`;
- `snapshot`;
- `snapshot list` and alias `snapshot ls`;
- `health-check`;
- `profile save`, `profile list`, `profile delete`, `profile export`;
- `init-secrets`;
- `history`.

Command behavior and stable flags:

- `init`: guided or non-interactive configuration creation; supports output,
  force overwrite, stdout printing, optional health check/profile save, source
  and target connection fields, unknown-type policy, AI-review fields,
  migration scope/object flags, notification fields, and profile metadata.
- `create`: `--apply`, `--out`/`-o` (default `schema.sql`),
  `--source-schema`, and `--target-schema`.
- `create --apply-suggested`: experimental, not stable. If implemented, it may
  apply one explicitly requested, validated, AI-translated expression splice;
  the resulting artifact must be clearly labeled as containing AI-authored
  content. It must never silently alter deterministic `schema.sql`.
- `sync`: `--against` (`target` default or `snapshot`), `--apply`,
  `--out`/`-o` (default `migration.sql`), `--allow-data-loss`, and
  `--save-snapshot`.
- `drift`: `--fail-on-destructive-only`.
- `snapshot`: `--out`/`-o` for an additional JSON copy.
- `snapshot list`: `--limit`/`-n`, default 50.
- profile save/delete/export: `--name`/`-n`; export also has `--out`/`-o`.
- `init-secrets`: `--force`/`-f`.
- `history`: `--run`.

For drop-in CLI compatibility, `init` exposes these exact stable flags:

- `--out`/`-o`, `--force`/`-f`, `--print`, `--non-interactive`/`-y`,
  `--health-check`, and `--save-profile`;
- `--source.type`, `--source.host`, `--source.port`, `--source.database`,
  `--source.user`, `--source.password_mode`, `--source.password`, and
  `--source.schema`;
- `--target.type`, `--target.schema`, `--target.configure`, `--target.host`,
  `--target.port`, `--target.database`, `--target.user`,
  `--target.password_mode`, and `--target.password`;
- `--unknown_type_policy`;
- `--ai_review`, `--ai_review.mode`, `--ai_review.model`,
  `--ai_review.diagnose_failures`, and `--ai_review.suggest_fixes`;
- `--migration`, `--migration.include_tables`,
  `--migration.exclude_tables`, `--migration.create_indexes`,
  `--migration.create_foreign_keys`, and
  `--migration.create_check_constraints`;
- `--slack`, `--slack.webhook_var`, `--slack.channel`, and
  `--slack.username`;
- `--profile.name` and `--profile.description`.

Exit codes:

| Code | Meaning |
|---:|---|
| 0 | success |
| 1 | configuration, serialization, or CLI contract error |
| 2 | connection, authentication, pool, DNS, or network error |
| 3 | schema operation failed |
| 4 | validation error |
| 5 | cancellation |
| 6 | state, checkpoint, profile, or history error |
| 7 | file I/O error |
| 8 | drift detected |

Drift is the only stable domain-result exit: no drift is 0; detected drift is
8. With `--fail-on-destructive-only`, additive-only drift returns 0.

## 19. Configuration, secrets, and security

### Required contract

Per-migration configuration contains:

- source and target connection/dialect/schema information;
- deterministic schema-generation mode;
- unknown-type policy;
- include/exclude table globs;
- booleans controlling indexes, foreign keys, and checks;
- artifact/state directory;
- optional AI-review settings;
- optional profile metadata and notifications.

Defaults:

- omitted source dialect defaults to SQL Server for compatibility;
- SQL Server’s default schema is `dbo`;
- omitted target dialect defaults to PostgreSQL, whose default schema is
  `public`; MySQL uses its database name as the schema when no separate schema
  is provided;
- schema generation mode is `deterministic`;
- unknown-type policy is `fail`;
- AI review is disabled;
- AI review mode is `warn`;
- indexes, foreign keys, and check constraints are managed unless explicitly
  disabled.

Exclude patterns win over include patterns. Include patterns, when present,
form an allowlist. Matching is case-insensitive and uses shell-style table-name
globs.

Configuration parsing MUST reject unknown fields. It must reject
`migration.ai_verify` with guidance to use `ai_review.enabled`, and reject
`migration.ai_verifier_model` with guidance to use `ai_review.model`.

Global migration defaults support only maximum source/target connections,
managed indexes/foreign keys/checks, and data directory. Legacy row-transfer
tuning keys in the global secrets file are ignored with one warning that names
the removed keys; they do not reactivate transfer behavior.

Passwords support:

- `${env:VARIABLE}`;
- `${file:path}`;
- the legacy `${VARIABLE}` shorthand;
- literal values.

Real credentials MUST NOT be written into manifests, logs, generated SQL, test
fixtures, or source control.

Global secrets store AI provider credentials, profile-encryption key, and
notification webhook. `SMT_SECRETS_FILE` may override the secrets-file
location; `SMT_MASTER_KEY` is the compatibility fallback for the profile key.
Restrict secrets, state, SQL artifacts, and manifests to owner access where the
platform supports file permissions.

## 20. Optional AI boundary

### Required contract

AI review is off by default and has `warn` and `fail` modes.

For table DDL review:

1. give the model already-rendered DDL;
2. ask it only to parse structured column metadata;
3. compare parsed columns with the source model using deterministic code;
4. record issues and the method label in the manifest.

For indexes, foreign keys, and checks, prefer narrow deterministic DDL parsers
and structural comparators. Check/filter predicates use expression structural
comparison where supported and normalized text only as a fallback.

Review may warn or stop before apply. It MUST NOT rewrite executable SQL.
Parser failure in fail mode must not be treated as approval.

Optional failure diagnosis may return cause, suggestions, confidence, and
category. It is advisory and cannot change the failed result or retry DDL.

An optional suggestion path may write a separately named, clearly labeled SQL
artifact containing exactly one validated expression substitution. It is never
the deterministic artifact and is never applied without the experimental
explicit opt-in described above.

Provider adapters MAY support cloud and local OpenAI-compatible, Anthropic,
Google, Ollama, or LM Studio endpoints. Provider breadth is secondary to the
deterministic core.

## 21. Persisted artifacts and compatibility

### Required contract

The full state backend is a SQLite database named `migrate.db` below the
configured data directory. Its active logical tables and compatibility columns
are:

- `runs`: `id`, `kind`, `started_at`, `completed_at`, `status`, `phase`,
  `source_schema`, `target_schema`, sanitized `config`, `config_hash`,
  `profile_name`, `config_path`, and `error`;
- `profiles`: `name`, `description`, `config_enc`, `created_at`, and
  `updated_at`;
- `schema_snapshots`: `id`, `source_type`, `source_identity`,
  `source_schema`, `captured_at`, and opaque JSON `payload`.

For profile compatibility, encrypt with AES-256-GCM using a 32-byte key. The
stored payload is version byte `1`, then a 12-byte GCM nonce, then ciphertext;
use the profile name as authenticated associated data.

The optional `--state-file` backend stores only the current run as a private
YAML file with `run_id`, `started_at`, optional `completed_at`, `status`,
`phase`, optional `error`, `source_schema`, `target_schema`, optional
`config_hash`, `profile_name`, and `config_path`. It does not provide encrypted
profiles or schema snapshots.

Fresh databases MUST NOT create data-transfer task/progress tables. When
opening an older database that still contains such tables, leave them intact as
harmless legacy data.

Within major version 1:

- open state databases written by 1.0 and later 1.x;
- use idempotent additive migrations for missing active tables/columns/indexes;
- allow new nullable columns, new tables, and new indexes;
- do not require old binaries to read newer minor/patch state.

Every create run writes `schema.sql` and `manifest.json`. Every applied sync
writes `migration.sql` and `manifest.json` under
`runs/<run-id>/ddl/`. Preview also writes the requested review file.

The create manifest reader contract contains:

- `smt_version`;
- `renderer_version`;
- `source_dialect`;
- `target_dialect`;
- `target_schema`;
- `unknown_type_policy`;
- `ai_review_enabled`;
- `ai_review_mode`;
- `ai_review_warnings`, with `label`, `method`, and `issues`;
- `mapping_warnings`;
- `table_count`;
- `source_schema_fingerprint`;
- `plan_fingerprint`.

The sync manifest contract is:

- `smt_version`;
- `renderer_version`;
- `source_dialect`;
- `target_dialect`;
- `target_schema`;
- `unknown_type_policy`;
- `sync_mode`;
- `statement_count`;
- `unsupported_count`;
- `source_snapshot_fingerprint`;
- `plan_fingerprint`.

Create-manifest mapping-warning entries contain `table`, `column`,
`source_dialect`, `target_dialect`, `source_type`, `target_type`, `kind`, and
`reason`.

Fingerprints are `sha256:<lowercase hex>`. Source fingerprints exclude row
counts, row-size estimates, and samples. Plan fingerprints cover the exact
serialized SQL.

Readers MUST ignore unknown manifest fields and treat missing optional arrays
as empty. Current findings use method `deterministic_comparator`; accept the
historical `free_text_auditor` value when reading old v1 manifests.

The renderer version identifies the combined deterministic type-mapping and
DDL-output contract. Use value `20` for byte-compatible reconstruction of the
current release. Increment it whenever the same input/configuration can produce
different DDL because of a mapping, expression, identifier, default, side
object, or evolution change. A version change invalidates generated SQL but
does not automatically invalidate a source snapshot.

## 22. Staged implementation plan

This sequence is guidance, but each stage has required observable exit
criteria.

### Stage 1 — schema model, capabilities, and golden harness

Suggested focus: establish the abstract model, dialect aliases, typed
unsupported errors, stable ordering behavior, and a golden-output harness.

Exit criteria:

- models serialize stably;
- alias resolution is case-insensitive;
- capability matrices match this specification;
- unsupported calls emit no SQL.

### Stage 2 — canonical types

Suggested focus: establish the canonical type behavior independently of column
clauses.

Exit criteria:

- all canonical kinds and metadata are round-tripped in unit tests;
- the three CLI dialects pass a source-by-target type corpus;
- SQLite and ClickHouse mapping goldens pass;
- unknown policies and lossy warnings are pinned.

### Stage 3 — expression semantics

Suggested focus: establish deterministic translation, equivalence, and class
labels. An expression IR is one suitable design, not a required one.

Exit criteria:

- each supported source expression renders correctly to all three CLI targets;
- time, UTC-time, UUID, boolean, coalesce, concatenation, `IN`, LIKE/regex, and
  date/time casts pass cross-dialect tables;
- unsupported/unknown forms fail on every target;
- string literals containing function names remain unchanged.

### Stage 4 — create renderers and public API

Suggested focus: add schema/table/column/side-object rendering and the
connection-free API.

Exit criteria:

- representative exact SQL goldens pass for all five public dialects;
- create plans contain only schema and table statements;
- standalone side-object goldens and validations pass;
- SQLite and ClickHouse special cases fail or warn exactly as specified.

### Stage 5 — evolution renderers

Suggested focus: add every typed evolution operation and batch contract.

Exit criteria:

- capability matrix tests pass;
- every supported dialect has exact SQL goldens;
- same-connection, cleanup, and best-effort contracts are exercised by a fake
  executor;
- unsupported operations never suggest rebuild SQL.

### Stage 6 — CLI drivers and introspection

Suggested focus: add PostgreSQL, SQL Server, and MySQL catalog/execution
adapters and driver discovery.

Exit criteria:

- each driver introspects all required column and side-object facts;
- reader conformance tests pass against disposable databases;
- writer existence checks and raw execution are covered;
- source/target aliases work in configuration and identifier normalization.

### Stage 7 — create orchestration and artifacts

Suggested focus: add scope filtering, collision checks, ordered plan rendering,
preview, apply, history, manifests, and first-failure handling.

Exit criteria:

- create preview needs no target connection;
- preview SQL equals apply SQL byte-for-byte;
- apply stops at first failure;
- rerun skips existing named create objects;
- artifacts exist before execution begins.

### Stage 8 — snapshots, drift, and sync

Suggested focus: add versioned snapshots, compatibility backfill, pure
structural comparison, live semantic comparison, risk rendering, and both sync
modes.

Exit criteria:

- old snapshot fixtures deserialize without false drift;
- plans are stable across repeated runs;
- unsupported and data-loss gates execute before DDL;
- FK dependency and cycle tests pass;
- failed snapshot-mode apply cannot be blindly replayed.

### Stage 9 — optional operational features

Suggested focus: add encrypted profiles, file-backed headless state,
notifications, interactive configuration, and AI review/diagnostics only after
deterministic acceptance passes.

Exit criteria:

- core tests never require network access or host secrets;
- optional live tests are explicitly enabled;
- no optional subsystem mutates deterministic SQL.

## 23. Acceptance criteria

The reconstruction is complete only when all criteria below are demonstrably
met.

### A. Determinism

1. Render the same non-trivial schema 100 times per target; all SQL, warning
   order, manifest content except run/time fields, and plan fingerprints match.
2. Run with rendering concurrency 1 and greater than 1; output is identical.
3. Preview and apply consume the same statement bytes.
4. A renderer behavior change fails a test unless the renderer version and
   affected goldens are intentionally updated.

### B. Type and column fidelity

For every PostgreSQL/SQL Server/MySQL source-target pair:

1. exact bounded character/binary lengths match or produce a documented,
   asserted warning;
2. precision and scale match;
3. nullability matches;
4. timezone-aware versus naive class matches;
5. fractional-seconds precision matches within documented target limits;
6. identity/auto-increment semantics match;
7. default presence and semantic class match;
8. computed-column presence is retained in the model and rendered DDL;
   expression and supported storage-class behavior are pinned by deterministic
   unit/golden tests;
9. unsignedness, enum/set values, bit widths, array elements, and spatial
   metadata are preserved or explicitly refused/warned.

Do not accept table, FK, or check counts alone.

### C. Expression conformance

Pin exact outputs for examples including:

- SQL Server current time → target-local current-time forms;
- SQL Server UTC current time → PostgreSQL UTC expression, SQL Server UTC
  function, and MySQL UTC function;
- UUID generators across all three targets;
- SQL Server `ISNULL` and MySQL `IFNULL` mapped by semantic class;
- boolean 0/1 on boolean columns;
- PostgreSQL `ANY(ARRAY[...])` to `IN`;
- quoted text containing `GETDATE()` unchanged;
- unknown `DATEADD` and custom functions rejected cross-dialect;
- MySQL backslash/control-character literal escaping.

### D. Public API conformance

1. Every capability bit is independently tested.
2. Every unsupported operation returns the typed error and empty SQL/batch.
3. Input lists preserve order.
4. Custom registries are isolated.
5. Legacy custom create-only dialects still work after side-object/evolution
   extension interfaces are introduced.
6. Create plan, side objects, and evolution batches contain no connection,
   retry, or transaction policy.

### E. Sync safety

1. Live-target mode detects and refuses primary-key, identity, computed,
   `ON UPDATE`, enum/set member, SRID, display-width, and ambiguous-check
   changes. Snapshot mode detects/refuses primary-key changes but, by
   compatibility contract, does not detect identity changes.
2. One unsupported item prevents all apply statements.
3. Column/table drops require explicit data-loss authorization.
4. Added non-null column without default is labeled blocking.
5. Type change is labeled rebuild.
6. Removed tables drop children-first.
7. FK cycles are broken narrowly and deterministically.
8. A required execution failure stops later statements and records SQL.
9. Snapshot is saved only after full success.

### F. Drift

1. Semantically equivalent cross-dialect types do not drift.
2. Halved/bucketed varchar lengths do drift.
3. `datetime2` versus timezone-aware timestamp drifts; `datetimeoffset` versus
   timezone-aware timestamp does not.
4. Index key order, prefix lengths, expressions, includes, uniqueness, and
   filter presence participate.
5. FK column order, referenced columns/schema/table, and actions participate.
6. Scope and managed-object flags match create/sync exactly.
7. Drift is read-only and returns exit 8 unless destructive-only mode suppresses
   additive drift.

### G. Integration matrix

Use native CRM-style fixtures for all nine source-target pairs:

| Source | Target |
|---|---|
| SQL Server | SQL Server |
| SQL Server | PostgreSQL |
| SQL Server | MySQL |
| PostgreSQL | SQL Server |
| PostgreSQL | PostgreSQL |
| PostgreSQL | MySQL |
| MySQL | SQL Server |
| MySQL | PostgreSQL |
| MySQL | MySQL |

Each fixture should contain at least:

- 14 related tables plus a type-smoke table;
- composite and single-column keys;
- indexes with dialect-specific options;
- foreign keys with actions;
- checks;
- identities;
- timezone-aware and naive time types;
- exact numeric boundaries;
- bounded and legacy LOB strings/binaries;
- UUID, JSON, enum/set, arrays where native, spatial metadata where available;
- stored and virtual computed columns;
- explicit fractional-seconds precision.

Verify table presence, column lengths, precision/scale, nullability, identity,
timezone class, default-expression class, primary keys, indexes, foreign keys,
and check presence structurally after apply. Archive a machine-readable matrix
report. Computed-column expression/storage equivalence is not part of the
current v1 live-matrix compatibility claim; it SHOULD be added as a stricter
future gate without weakening the deterministic rendering requirement.

### H. Hermetic and release gates

The default suite MUST:

- require no live databases, AI providers, network, or host secrets;
- include unit, golden, compatibility, race/thread-safety, CLI, and build
  checks.

Explicit live gates SHOULD include:

- a historical SQL Server-to-PostgreSQL no-AI schema;
- the nine-pair CRM matrix;
- optional live AI parser/review/diagnostic smoke.

Release artifacts SHOULD cover mainstream Linux, macOS, and Windows targets
with SHA-256 checksums. Follow semantic versioning. A stable 1.x CLI item is not
removed or renamed without deprecation; experimental flags may change.

### I. Representative exact SQL goldens

These fragment examples pin whitespace, quoting, qualification, and naming for
drop-in deterministic compatibility. They are shown before the plan serializer
adds risk comments and semicolons.

For target PostgreSQL schema `public`, table `Accounts`, columns
`id bigint identity` and `name varchar(80) not null`, primary key `id`:

```sql
CREATE TABLE "public"."accounts" (
    "id" bigint GENERATED BY DEFAULT AS IDENTITY,
    "name" character varying(80) NOT NULL,
    CONSTRAINT "pk_accounts" PRIMARY KEY ("id")
)
```

For target SQL Server schema `dbo`, table `Accounts`, columns
`ID int identity not null` and `Name varchar(80) not null`, primary key `ID`:

```sql
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'dbo') EXEC(N'CREATE SCHEMA [dbo]')
```

```sql
CREATE TABLE [dbo].[Accounts] (
    [ID] INT IDENTITY(1,1) NOT NULL,
    [Name] VARCHAR(80) NOT NULL,
    CONSTRAINT [pk_Accounts] PRIMARY KEY ([ID])
)
```

For the same logical table on MySQL database `crm`:

```sql
CREATE DATABASE IF NOT EXISTS `crm`
```

```sql
CREATE TABLE `crm`.`Accounts` (
    `ID` INT AUTO_INCREMENT NOT NULL,
    `Name` VARCHAR(80) NOT NULL,
    CONSTRAINT `pk_Accounts` PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
```

For ClickHouse database `analytics`, table `events` with `event_id Int64`,
nullable string `attributes`, nullable UTC millisecond timestamp
`occurred_at`, and key `event_id`:

```sql
CREATE TABLE `analytics`.`events` (
    `event_id` Int64,
    `attributes` Nullable(String),
    `occurred_at` Nullable(DateTime64(3, 'UTC')),
    PRIMARY KEY (`event_id`)
) ENGINE = MergeTree ORDER BY (`event_id`)
```

## 24. Definition of done

Do not declare completion until:

- the deterministic core works with AI disabled and no target connection for
  create preview or snapshot-mode sync preview; target-mode sync preview still
  introspects the target by definition;
- every supported/unsupported capability is test-pinned;
- exact SQL goldens cover all public dialects and operations;
- all nine CLI source-target pairs pass structural fidelity checks;
- preview/apply identity, risk gates, failure recovery, and artifact
  fingerprints are demonstrated;
- old snapshots/state/manifests satisfy the v1 read contract;
- no secret or machine-specific path appears in source, fixtures, artifacts, or
  documentation;
- the schema subsystem can be embedded in a data migration host without taking
  ownership of row transfer.

---

## Appendix A — Non-normative reference identifiers from the current implementation

This appendix is informational. Do not treat its language, package layout, or
toolchain as a requirement.

The current reference implementation is Go module
`github.com/johndauphine/smt`, declares Go `1.25.7`, and reports SMT version
`1.4.0`. Its deterministic renderer version is `20`.

Notable current public identifiers:

- `schema.NewRenderer`, `schema.Registry`, `schema.Renderer`;
- `schema.Options`, `schema.Result`, `schema.Warning`;
- `schema.Table`, `schema.TableRef`, `schema.Column`, `schema.Index`;
- `schema.PrimaryKey`, `schema.ForeignKey`, `schema.UniqueConstraint`,
  `schema.CheckConstraint`;
- `schema.Plan`, `schema.Statement`, `schema.Batch`;
- `Renderer.CreateSchema`, `CreateTable`, `CreateColumn`, `CreateIndex`,
  `CreatePrimaryKey`, `CreateForeignKey`, `CreateUniqueConstraint`,
  `CreateCheckConstraint`, and `PlanCreate`;
- `Renderer.DropSchema`, `DropTable`, `DropIndex`, `DropConstraint`,
  `AddColumn`, `DropColumn`, `AlterColumnType`,
  `AlterColumnNullability`, `SetColumnDefault`, `DropColumnDefault`, and
  `TruncateTable`;
- `schema.UnsupportedFeatureError`;
- `schema/canonical.ToCanonical`,
  `schema/canonical.FromCanonicalWithWarnings`.

Current internal responsibility names include `internal/driver`,
`internal/ddl`, `internal/expr`, `internal/schemadiff`,
`internal/orchestrator`, and `internal/checkpoint`. These names describe the
same logical boundaries specified above, but another implementation should use
idioms natural to its language.
