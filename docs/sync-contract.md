# SMT v1 Sync Support Contract

`smt sync` computes a deterministic schema plan from the current source schema
and the live target schema. The default mode writes `migration.sql` for review.
`smt sync --apply` executes the plan statement by statement.

This contract describes the default `--against target` mode (live-target
introspection). `smt sync --against snapshot` diffs the source against the
latest stored snapshot instead — same renderer, risk labels, and apply
gating — with these deltas from the target-mode contract:

- the baseline is the stored snapshot, so drift introduced directly on the
  target is invisible to it (use `--against target` or `smt drift` for that);
- primary-key and identity changes between snapshots are not detected (the
  snapshot diff compares columns, indexes, FKs, and checks by name);
- a computed-column change between snapshots fails the render with an error
  instead of an `-- [unsupported]` plan entry.

## Supported Changes

SMT v1 can render these sync changes:

- added tables, including managed indexes, foreign keys, and check constraints;
- removed tables, ordered children-first, including FK-cycle break statements;
- added columns;
- removed columns, marked `data-loss-risk`;
- supported column type, length, precision, scale, nullability, and default
  changes;
- added and removed non-PK indexes;
- added and removed foreign keys;
- added and removed check constraints when SMT can identify them by name.

Risk labels in `migration.sql` have operational meaning:

- `safe`: expected to be online and non-destructive;
- `blocking`: may scan, validate, lock, or fail on existing rows;
- `rebuild`: may require table rebuild behavior;
- `data-loss-risk`: drops data and is never applied without
  `--allow-data-loss`.

## Unsupported Changes

SMT v1 detects but refuses to render these classes:

- primary-key add, drop, or re-key;
- identity or auto-increment changes;
- computed-column presence, expression, or storage-class changes;
- `ON UPDATE` clause changes;
- same-dialect `ENUM` or `SET` value-list changes;
- spatial SRID changes;
- display-width changes;
- check-constraint reconciliation cases where SMT cannot safely identify the
  exact add/drop/change operation.

Unsupported changes are reported in the plan as `-- [unsupported]` comments and
in CLI output with the affected table and reason. `smt sync --apply` refuses to
run any plan that contains unsupported changes, so SMT does not apply a partial
DDL plan while silently skipping manual work.

## Apply Behavior

`smt sync --apply` first refuses unsupported changes. It then refuses
`data-loss-risk` statements unless `--allow-data-loss` is set. When a plan is
allowed to run, statements execute sequentially and stop at the first failure.
The error includes the failed statement number, description, database error, and
SQL text so operators can inspect the target and decide whether to adjust the
schema manually or rerun after fixing the cause.

`--save-snapshot` writes a new source-schema baseline only after a successful
apply.
