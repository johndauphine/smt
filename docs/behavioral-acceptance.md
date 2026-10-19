# Source-backed behavioral acceptance

The versioned suite in [`testdata/behavior`](../testdata/behavior/README.md)
compares stored values and classified rejections with observations from a live
source. Expected values never come from SMT's type mapper or comparator. Keep
this gate separate from the existing CRM metadata matrix and the historical
69-case experimental diagnostic.

Run all commands below in Ubuntu WSL from the SMT repository. Python needs only
its standard library. Go uses the version in `go.mod`. The harness uses WSLC's
CLI as a transport into its Linux ARM64 VM, and all custom binaries and database
execution are native Linux. Images must already exist locally; there are no
implicit pulls. The pinned image digests are in `containers.py`.

```bash
export PATH=/home/johnd/.local/opt/go1.25.7/bin:$PATH
mkdir -p .acceptance-artifacts/behavior
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build \
  -o .acceptance-artifacts/behavior/client ./testdata/behavior/client
python3 -m unittest discover -s testdata/behavior -v
python3 testdata/behavior/run.py verify-freeze
```

The client accepts no host or connection string. Every live command creates its
own unique labeled container with no networking, host bind mounts or published
ports, and connects only inside that container. It validates ownership before
removing the container and its volumes, then checks the container inventory.
Never point this suite at an application database or reuse unrelated containers.
If interrupted by an external process kill, inspect the recorded owned container
ID before explicitly removing it; an interrupted run is incomplete evidence.

To reproduce source observations without changing the frozen oracle:

```bash
python3 testdata/behavior/run.py reference \
  --out .acceptance-artifacts/behavior/source-repeat-1
python3 testdata/behavior/run.py reference \
  --out .acceptance-artifacts/behavior/source-repeat-2
```

Each output directory must be new. Reports include all raw source results,
semantic observations, source guards, engine versions, image IDs/digests,
collation, session settings, client hash, timestamps and cleanup status. A source
guard mismatch is a source-baseline failure, never a target pass. Only a new,
unfrozen suite version may run `--suite DIR freeze --reference DIR1 --reference DIR2`; freezing
requires two complete, agreeing, validated runs with matching fixture hashes and
confirmed cleanup.

Build an unmodified CLI and ask it to render the small live source fixture:

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build \
  -o .acceptance-artifacts/behavior/smt ./cmd/smt
python3 testdata/behavior/run.py generate \
  --binary .acceptance-artifacts/behavior/smt --profile legacy \
  --out .acceptance-artifacts/behavior/legacy-generation
python3 testdata/behavior/run.py generate \
  --binary .acceptance-artifacts/behavior/smt --profile checked \
  --out .acceptance-artifacts/behavior/checked-generation
```

Generation invokes `smt create` with AI disabled inside a fresh owned Edge
container and copies its `schema.sql` unchanged. `checked`/`strict` pass the
explicit `--fidelity` option. A strict refusal is recorded as a generation
failure/refusal; it is not a behavioral success. Public API callers may instead
render the two source tables into any local SQL file and pass that exact file
to `target`; include all standalone side objects returned by the API in plan
order. Do not patch the SQL to satisfy tests.

```bash
python3 testdata/behavior/run.py target \
  --sql .acceptance-artifacts/behavior/legacy-generation/candidate.sql \
  --out .acceptance-artifacts/behavior/legacy-target
python3 testdata/behavior/run.py target \
  --sql .acceptance-artifacts/behavior/checked-generation/candidate.sql \
  --out .acceptance-artifacts/behavior/checked-target
python3 testdata/behavior/run.py compare \
  --baseline .acceptance-artifacts/behavior/legacy-target \
  --candidate .acceptance-artifacts/behavior/checked-target \
  --out .acceptance-artifacts/behavior/checked-comparison.json
```

Candidate reports preserve the exact SQL bytes/hash, frozen manifest hash,
binding map, engine settings and per-case results. A failing `target` exit code
can mean expected known differences; read its `outcome` and case observations.
`compare` lists exact fixed, regressed, unchanged-correct, unchanged-different and
unverified IDs. Any prior correct case becoming incorrect is a regression,
including source-valid inputs rejected by a new constraint.

Identifiers are bound only through `{{simple}}`/`{{edge}}` tokens. Default target
bindings are `public.behavior_simple` and `public.behavior_edge`. For another
renderer naming convention, supply `--bindings FILE` containing the explicit
JSON object `{"simple":["schema","table"],"edge":["schema","table"]}`. The
binding must be unique and reversible. Candidate SQL is never rewritten. Table
naming is reported separately. The named ordinary-index probe intentionally
checks the fixture's declared `ix_behavior_tiny` identifier and default table
names; another naming policy requires its own naming assessment.

Each probe runs in a fresh connection and rolled-back transaction; successful
multi-step inserts and updates read the stored result before rollback. Integer
and binary parameters use the native database drivers, including PostgreSQL's
extended protocol, rather than SQL text substitution. Dates and binary values
are normalized by explicit dialect queries; Unicode storage is read as the exact
string. Rejection categories are range, length, datetime, uniqueness and
nullability. Unrelated SQLSTATEs/server errors and transport errors never pass
an expected rejection. A PostgreSQL CHECK violation is eligible only for the
specific frozen range/length/datetime rejection category under test. Successful
boundary cases guard against over-restrictive checks.

The explicit width-conversion binary case intentionally models truncation before
assignment: SQL Server `CAST(... AS varbinary(50))` is paired with PostgreSQL's
explicit substring conversion. This proves that the declared conversion path
accepts the resulting valid value. It does not claim that PostgreSQL `bytea`
implements a length-bearing cast or that application parameter conversions are
equivalent. Unbounded binary casts and raw bound bytes have separate rejection
cases.

Passing these finite probes never sets a universal `verified=true` claim. Edge is
a retired source build and does not replace the supported SQL Server release
acceptance gate. General collation equivalence, identity overrides, offsets,
evolution, concurrency and the full historical CRM corpus remain outside this
version. Native fixed binary padding, legacy date rounding, time coercion, unique
NULLs and UTF-16 limits are deliberately visible differences until implemented
and supported through the schema lifecycle.

The initial 2026-10-19 local acceptance froze 66 agreeing source observations
from two fresh Edge databases. The checked CLI candidate with SQL SHA-256
`4414e7240852d62bd9d6df4c0fffcca4c54746035fe5c983354a69f8ed98898c`
executed unchanged in PostgreSQL and matched 47/66 cases: 42/43 simple and 5/23
edge cases. Every tinyint and bounded-varbinary case passed, including successful
boundaries, updates, NULLs, defaults, explicit conversions and bound parameters.
The 19 differences were fixed binary padding/overflow (2), time input/rounding
(12), legacy timestamps (3), UTF-16 overflow (1), and nullable UNIQUE semantics
(1). Table naming passed and no probe had a harness failure. Source and target
containers were removed. The ignored detailed evidence is under
`.acceptance-artifacts/behavior/checked-generation-final/` and
`checked-target-final/`. A second fresh PostgreSQL run of the same candidate
reproduced all 47 correct and 19 different results, with zero changed cases and
confirmed cleanup (`checked-target-repeat/`, `repeat-comparison.json`).

The legacy candidate was also preserved unchanged. It could not apply because
it emitted `payload bytea DEFAULT 0xAB`, which PostgreSQL rejected with SQLSTATE
`42804`. Its behavior cases were therefore not evaluated. The checked path's
deterministic binary-default translation resolved that setup failure. This is
not a claim of 47 fixes or zero legacy regressions: all 66 baseline comparisons
remain unverified because the legacy fixture did not apply. Independent legacy
consumer and CRM regression gates remain necessary.

## Live checked lifecycle integration

This separate integration test deliberately exercises SMT's production renderer,
PostgreSQL introspector and fidelity drift/sync APIs together. It does not supply
or change the independent behavioral oracle. Its small source metadata table has
nullable SQL Server tinyint and varbinary(50), with defaults `(7)` and `(0xAB)`.

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build \
  -o .acceptance-artifacts/behavior/lifecycle ./testdata/behavior/lifecycle
python3 testdata/behavior/lifecycle.py \
  --binary .acceptance-artifacts/behavior/lifecycle \
  --out .acceptance-artifacts/behavior/lifecycle-new-run
```

The runner creates a fresh owned PostgreSQL container with `--network=none` and
no published ports or host mounts. Its PostgreSQL listener permits container
loopback so the production reader can use its ordinary connection path. It
executes the checked renderer's exact CREATE TABLE SQL, loads real columns,
indexes, foreign keys and checks through `postgres.Reader`, and passes those
catalog results to `ComputeDriftWithFidelity` and `ComputeLiveDiffWithFidelity`.
Catalog check validation must be explicitly true. Empty and populated targets
must produce empty drift and repeated empty syncs.

For each of the two required checks, the harness independently tests a missing
check, a same-name `CHECK(true)` replacement and a same-predicate `NOT VALID`
replacement. Changed/deleted binary defaults and an unexplained check with an
SMT-like name prefix also require drift and typed `FidelityEvolutionError`
refusal with an empty diff. Every case runs with both normal side-object
comparison and disabled source side-object comparison. These deliberate
mutations affect only the owned temporary database; no migration fix is applied.

The initial local run passed all 24 assertions. Evidence in
`.acceptance-artifacts/behavior/lifecycle-final-2/report.json` includes the actual
catalog metadata, desired source metadata, exact rendered SQL, generated check
definitions, drift/refusal results, executable hashes, engine metadata and
confirmed container removal. Renderer logs are preserved separately as
`integration-stdout.txt`.
