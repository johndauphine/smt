# SMT fidelity improvement plan

Prepared 2026-10-19. This plan strengthens deterministic schema migration while
protecting applications such as DMT that consume SMT as a library. The first
release should preserve existing public APIs and default output, offer explicit
opt-in enforcement for supported source restrictions, and report remaining
semantic differences. Full SQL Server emulation is not the first milestone.

Implementation of the first milestone is now in progress on
`codex/fidelity-opt-in`, using three Sol agents for consumer compatibility,
independent validation, and fidelity/lifecycle rules. All local builds, tests and
database execution use native Linux under WSL. Windows security settings remain
unchanged. The current implementation and evidence are summarized in
[the milestone report](fidelity-milestone-results.md). The completed work is saved
on that feature branch; release gates remain outstanding.

## Evidence and limits

The [independent CRM comparison](../../SMTX/docs/smt-crm-independent-results.md)
ran unchanged SMT against the full 15-table, 176-column source fixture. Generation
took about 3.9 seconds in one run, and the SQL executed successfully. With table
references adjusted to SMT's naming convention, it passed 20/23 original behavior
cases and 31/69 accumulated cases. The existing release's nine-pair acceptance
matrix measures a different contract and remains a separate result.

Live Azure SQL Edge confirmed missing tiny-integer and binary-length restrictions,
binary padding differences, legacy date rounding differences, and a Unicode length
boundary difference. It also contradicted eight historical time expectations;
six previously credited Astra passes were not faithful to that source. Preserve
those historical files and scores. Establish a new, source-validated baseline
before using the CRM cases as an implementation gate.

These observations concern the pinned Edge build and PostgreSQL target in the
report. Record server versions, collation, session settings, input conversion
paths and result normalization. A supported SQL Server release must also run the
relevant cases before advertising SQL Server compatibility beyond that evidence.

## Compatibility is a release gate

Compilation alone is insufficient: DMT could still break if statement boundaries,
order, type mappings, warning handling or accepted data change. Preserve all of
these for existing callers in the default mode:

- Existing exported function signatures, interfaces, struct layouts and zero-value
  behavior in `schema` and `schema/canonical`. Do not append fields to existing
  public structs: positional literals can break. Do not add required methods to
  third-party dialect implementations.
- Existing default SQL, identifier normalization, capability results, warning and
  error behavior, statement boundaries, plan ordering and side-object ownership.
  Keep `PlanCreate`'s documented scope and single-statement artifact contract.
- Library independence from database handles, model credentials, network calls and
  execution policy. DMT retains row movement, retries and scheduling.
- Existing state and snapshot readers, manifest fields and renderer-version rules
  described in [v1 compatibility](v1-compatibility.md).

Introduce fidelity controls through new types, new entry points or optional
capability interfaces. Keep `schema.NewRenderer(schema.Options{...})` and direct
canonical mapping calls unchanged. A proposed new renderer constructor takes the
existing options plus a separate fidelity-options value; the compatibility owner
finalizes its exact API before implementation. New source-semantics metadata uses
a companion result/context rather than changing public canonical struct layouts.

The proposed profiles are:

| Profile | Contract |
| --- | --- |
| Legacy, the existing default | Preserve current output and behavior for existing CLI and library consumers. |
| Checked, explicit opt-in | Add proven source restrictions and structured findings for known remaining differences. Preserve valid source inputs and native application interfaces within the declared supported scope. |
| Strict, explicit opt-in | Apply the same supported enforcement and reject plans with known unresolved fidelity losses or missing information required to assess the covered semantics. |

Strict is a gate over assessed features, not a claim that every possible behavior
has been proved equivalent. An unsupported third-party dialect must return a typed
capability error when fidelity is requested, rather than silently ignoring it.
Changing defaults is outside this work; any later proposal needs its own consumer
migration and versioning decision.

DMT was subsequently cloned from `https://github.com/johndauphine/dmt.git` into
Ubuntu WSL at `/home/johnd/repos/DMT`, on `main` at revision `080983dd`. Its working
tree is clean and resides on the Linux ext4 filesystem. Its `go.mod` requires
SMT v1.4.0 and Go 1.25.7. Real integration points include
`internal/smtddl/renderer.go` and `internal/driver/deterministic_mapper.go`;
existing tests cover the adapter, DDL parity and writer statement sequencing.

The pinned dependency and local candidate have both passed DMT's adapter,
driver, and generic writer tests and CLI build in disposable checkouts with
temporary module replacements. DMT's original source and dependency files remain
unchanged. Repeat this consumer gate after the final candidate freeze; its script
records both revisions and the uncommitted SMT candidate's file hashes. These
focused consumer tests do not claim exhaustive DMT application compatibility.

## Implementation work packages

### A. Pin the consumer contract

Owner: compatibility agent. Start first; it may run alongside B.

Extend the external-consumer fixtures around
[`schema/ddl_compatibility_test.go`](../schema/ddl_compatibility_test.go),
[`schema/ddl_public_test.go`](../schema/ddl_public_test.go), and the public
canonical tests. Cover the older positional literals already pinned, other
exported structs, custom dialect implementations, capabilities, error types,
warnings, and representative byte-for-byte default SQL and statement sequences.
Include create, side objects and evolution methods used by DMT once inspected.

Deliver a small API contract for the new opt-in entry point, source context,
fidelity findings and optional dialect support. Pin behavior for omitted source
metadata and profiles. Existing custom dialects must still build and run.
Audit all advertised public dialects, including SQLite and ClickHouse, even though
the first new enforcement rules target SQL Server to PostgreSQL.

Acceptance: legacy consumer fixtures pass without edits, the API change is
additive without altering public layouts, and DMT's real consumer gate is either
passing with recorded evidence or explicitly blocked pending its checkout.

### B. Establish an independent behavioral baseline

Owner: validation agent. Expected results must not be computed by SMT's mapper
or comparator. Use source execution and retain observations separately from target
execution. Reuse the experimental evidence as input, not as unquestioned truth.

Create a versioned suite under `testdata/behavior/` with an explicit live acceptance
runner and ignored output directory. Archive the original 69-case experiment
unchanged. Validate each retained case against the live source, correct the eight
time expectations in the new version, and freeze the new manifest before renderer
work. Cases without established source behavior are marked unverified and excluded
from correctness claims, never silently treated as passes.

Cover successful inserts and updates as well as rejections, NULLs, defaults,
boundary lengths, explicitly cast inputs and bound parameters. Compare stored
values, equality, ordering, uniqueness and native operations where relevant.
Separate source failures, target failures and harness failures. Classify rejection
categories explicitly; do not require identical vendor error numbers or accept
any database error as a successful rejection.

Maintain a simple-schema tier for ordinary tables, nullability, defaults, decimal
precision, keys and indexes, plus an edge-case tier. Include small new schemas
whose source expectations are frozen before implementation. Identifier binding
must be explicit and reversible, and naming compatibility gets its own result.

Acceptance: reproducible pre-change results with hashes, engine/settings metadata
and case-level observations. No change to previous SMTX runs, frozen expectations
or candidate SQL. No database connection to a user's existing application data.

### C. Preserve source semantics and expose findings

Owner: semantics agent, after A defines the extension contract.

Current [`schema/canonical/to_canonical.go`](../schema/canonical/to_canonical.go)
combines legacy datetime, smalldatetime and datetime2 into the same timestamp
category; timezone awareness does not record preservation of an original offset.
The new path needs enough companion metadata to distinguish these facts and to
assess signedness, byte limits, UTF-16 length, padding and collation-dependent
comparisons. Keep existing canonical entry points and their outputs compatible.

Build one deterministic assessment layer beside the canonical pipeline. It
returns enforceable requirements and structured unresolved findings with stable
codes, affected objects, source/target context and reasons. Missing collation or
conversion context is uncertainty, not permission to assume equivalence.

Reuse this assessment in the opt-in public API, create manifests and sync plans.
Keep findings about type widening separate from demonstrated loss after generated
constraints are applied. Add optional manifest fields without changing existing
reader contracts; preserve warning ordering deterministically. Wire profile
selection through configuration, preview and apply, with strict rejection before
execution. Do not require a model to discover these known differences.

Acceptance: the observed gap categories have deterministic diagnostics; legacy
mode output is unchanged; strict gates are covered by hermetic tests; old snapshot
and manifest fixtures still load. Source semantics survive every new path that
needs them, with conservative handling of older snapshots.

### D. Enforce the first low-risk restrictions

Owner: renderer agent, after A, B and C's shared contract.

Start with SQL Server tinyint range and bounded varbinary length in the checked
and strict PostgreSQL paths. Generate deterministic named CHECKs for the validated
value ranges and byte limits. Preserve NULL behavior, valid boundary values,
defaults and updates. Keep SQL Server unsigned tinyint distinct from MySQL signed
and unsigned tinyint and its boolean convention. MAX/unbounded binary must stay
unbounded. Probe explicit casts separately because source truncation can occur
before assignment.

Use [`internal/ddl/postgres/renderer.go`](../internal/ddl/postgres/renderer.go),
the shared renderer and the source-semantics assessment. Expressions belong in
`internal/expr` or the typed deterministic requirement renderer, not CLI string
rewrites. Existing identifier normalization remains the single naming authority.
Generated constraint names need deterministic collision handling within dialect
limits, including collisions with user constraints.

Do not add an exact-length CHECK for fixed binary as a substitute for padding;
that rejects valid source values. Do not add a less-than-midnight CHECK as a
complete time fix; target coercion may already have lost the distinction between
a valid near-midnight input and an invalid explicit end-of-day input.

Acceptance: the three original tinyint/binary failures pass on the validated
baseline, every previously correct case remains correct, and legacy default SQL
remains unchanged. Test SQL source/target combinations and public entry points
affected by shared code, not only the CRM fixture.

### E. Make generated restrictions work through the whole lifecycle

Owner: lifecycle agent, after C; integrate with D before accepting its changes.

Extend desired-schema construction, create, sync, drift and public evolution
rendering consistently. Principal locations are `internal/schemadiff`,
`internal/orchestrator`, `internal/ddl`, and the public `schema` adapters.
Generated restrictions must appear as intended target objects, not perpetual
extra constraints or repeated add/drop work. Preserve source constraints and do
not recognize ownership by a name prefix alone.

Prove create/apply/introspect produces no unintended drift, a second sync is empty,
and add/type-change/drop-column operations create, update or remove only the exact
managed restrictions. Profile transitions must be explicit. A transition to
stronger constraints can fail on existing rows and must be classified as blocking;
it must never silently clean up, truncate or transfer data. If an evolution case
cannot be supported safely, report it as unsupported before applying any part of
that plan. Public single-statement methods retain that contract; work requiring
multiple statements needs an opt-in batch capability or an explicit refusal.

Acceptance: lifecycle tests cover new tables and populated targets, user constraint
collisions, older snapshots and both sync baselines. Preview and apply use the same
rendered plan. Unsupported work cannot result in a partially applied plan.

### F. Resolve advanced value semantics deliberately

Owner: semantics agent, after B. Separate follow-up from the first restrictions.

| Area | Required result before claiming support |
| --- | --- |
| Fixed binary | Prove padding, overflow and conversion behavior for literals, typed values, parameters and updates. |
| Legacy dates | Prove source tick/minute rounding, date ranges, boundary ties and day rollover without double rounding. |
| Unicode length | Measure UTF-16 units where required, including supplementary characters and trailing-space boundaries; do not use character count as a substitute. |
| Time precision and offsets | Separate stored precision, original-offset preservation and native query interfaces. Keep explicit-invalid input tests alongside valid near-midnight tests. |
| Text comparison and uniqueness | Use captured collation context and compare equality, ordering, trailing spaces and duplicate keys. An index-only workaround is not general comparison equivalence. |
| Identity and unique objects | Test explicit identity inserts, generation and uniqueness behavior against source. Do not blindly replace BY DEFAULT with ALWAYS or unique indexes with constraints merely to satisfy the old catalog oracle. |

The first milestone may deliver warnings and strict refusal for these features.
Implement emulation only when it preserves the declared application interface and
can be managed by the schema lifecycle. Any generated helper functions, triggers
or domains require a separate capability and lifecycle design. Do not silently
switch native columns to text, add external extensions, or change DMT's row path.
Preserve native extraction, arithmetic, sorting and parameter behavior in tests.

Acceptance: each feature is either covered by source-backed behavior tests and
supported lifecycle operations, or explicitly reported as unresolved. A higher
aggregate score cannot conceal a newly rejected valid input or lost native operation.

### G. Evaluate broader optional AI review later

Owner: review agent, after the deterministic findings and baseline are stable.

Retain the existing parse-plus-Go-comparator review in
[`internal/driver/ai_verify.go`](../internal/driver/ai_verify.go). Its architecture
was chosen after problems with a free-text semantic auditor. Any broader semantic
review is an additional opt-in experiment, not a replacement acceptance oracle.

Give the reviewer source metadata, database context, generated plan and known
deterministic findings. Return structured, attributable findings only; no SQL
authoring, edits or automatic application. Test with recorded/fake responses first.
Measure useful new findings and false positives against the independent suite.
Keep unsupported claims and uncertainty separate from confirmed database evidence.
No new live model requests are part of this planning task.

Acceptance: enabling review cannot change executable SQL or default behavior;
provider failures follow an explicit review policy; known deterministic issues
remain detectable with AI disabled. Live review acceptance is required only if a
release advertises this new review capability.

## Agent sequence and ownership

Use up to three Sol implementation agents plus a coordinating reviewer. These are
assignments for the implementation stage, not agents launched by this document.

| Wave | Agent assignment | Dependency and ownership |
| --- | --- | --- |
| 1 | Compatibility agent: A | Own public consumer fixtures and the extension API contract. |
| 1 | Validation agent: B | Own independent fixtures, source observations and freeze; no renderer edits. |
| 1 | Semantics agent: C design and F investigation | Read implementation and propose source facts; wait for the API contract before shared-code edits. |
| 2 | Semantics agent: C implementation | Land shared assessment and profile interfaces first. |
| 3 | Renderer agent: D | Own constraint generation; coordinate any public adapter edits with the compatibility owner. |
| 3 | Lifecycle agent: E | Own desired state, drift and sync; consume the agreed requirement representation. |
| 3 | Validation/compatibility agent: integration | Run frozen cases and consumer checks independently; do not change expectations to match a candidate. |
| 4 | Semantics and review agents: F, then G | Separate bounded proposals and opt-in implementations after the first milestone passes. |

The coordinator owns integration, shared-file arbitration, renderer-version
updates, final acceptance and the compatibility decision. No two agents edit the
same API or canonical files concurrently. Use isolated worktrees for overlapping
packages when implementation starts; follow repository rules and preserve local
user work. Agents report exact fixed/regressed cases, tests and unresolved limits.
Follow the standing branch and local-commit rules in [AGENTS.md](../AGENTS.md).
Pushing, merging and releasing require the user's explicit authorization.

## Verification and completion

All local Go builds and tests run in Ubuntu WSL with the version declared by
`go.mod`; custom Windows test executables are not part of this workflow. Disposable
database containers must be owned by the test run, with pinned images, recorded
settings and cleanup verification. Keep credentials and acceptance outputs ignored.
Do not run the existing fixture-reload targets against unrelated containers.

During development, run the smallest affected package tests. At integration, run
the full hermetic suite, public compatibility fixtures, build, formatting, lint
and vet, plus race checks in WSL or Linux CI with the required native compiler.
Run the complete existing CRM matrix on supported CI infrastructure, the new
source-backed behavioral suite, and the real DMT consumer gate. ARM Edge local
results complement rather than substitute for the SQL Server release gate.

For each opt-in candidate, report case-by-case fixed, regressed, unchanged and
unverified outcomes against the new frozen baseline. Require zero unexplained
regressions in previously source-correct behavior, plus preserved native interfaces
and default-mode consumer output. Never improve a score by replacing valid-input
tests with rejection tests or weakening the source expectations.

Assess `internal/ddl.RendererVersion` for every changed output contract, including
the new opt-in profiles; the coordinator owns the coordinated bump. Include profile
and source-semantics version in cache/fingerprint inputs where they affect output.
Golden changes must be intentional and reviewed. Test old snapshots/manifests and
document regeneration without requiring an unnecessary source re-baseline.

The first milestone is complete when A through E pass, known advanced differences
from F are surfaced, legacy/default consumers remain compatible, and release docs
state the exact supported profile and behaviors. It does not claim universal
production equivalence. G is optional follow-up and is not required for the
deterministic improvements to be useful.

Relevant contracts: [public API](public-ddl-api.md), [sync](sync-contract.md),
[live acceptance](live-acceptance.md), [recovery](apply-recovery.md), and
[persisted compatibility](v1-compatibility.md).
