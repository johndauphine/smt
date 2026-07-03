# Code-review handoff: issues #194–#221

Twenty-eight findings from a multi-agent code review of SMT (2026-07-03, against commit
`d2b3567`), each filed as a GitHub issue with a **🤖 AI implementation brief** in the body
(symptom, root cause with `file:line`, proposed fix, primary files, test pointers, verify
command, acceptance criteria). This file is the index and coordination plan.

`go vet` and `go test ./... -short` were both clean when these were filed — every finding is
a **behavioral** bug the `testdata/crm/` fixture matrix cannot see, because the fixtures lack
the triggering shapes (covering/filtered/expression indexes, arrays, `BIT(N)`, oversized
varchar, function defaults). Counts-and-matrix-green is exactly the blind spot these cover.

**How to work an issue:** open the GitHub issue, follow its brief. Branch `fix/<slug>` off
`main` (never commit to `main`), Conventional Commit ending `(#<issue>)`, include a regression
test that fails before and passes after. A few findings are marked **PLAUSIBLE** rather than
CONFIRMED (they rest on documented engine semantics not executed live) — **re-verify the
mechanism before writing the fix**, don't assume.

**Build / test / lint:** `make build` · `make test` (full) · `make test-short` (skips
integration) · `make lint` · `make check` (fmt + test). Integration tests need live DBs:
`make test-dbs-up` (postgres + mssql), `make mysql-test-up`. End-to-end column fidelity:
`testdata/crm/verify_columns.sh` (criteria 1–6) — **do not trust count-only checks**, and
re-run it after any renderer change.

---

## ⚠️ File-conflict clusters (do not parallelize within a cluster)

Issues in the same cluster edit the same file. Assign each cluster to **one** worker, or
sequence and rebase. Verified against the filed issue bodies.

| Cluster | Shared file | Issues |
|---|---|---|
| Deterministic renderer | `internal/ddl/renderer.go` | #194, #197, #202, #203, #204, #205, #218 |
| Canonical type IR | `internal/canonical/{to,from}_canonical.go` | #198, #199, #200, #218 |
| Snapshot diff/render | `internal/schemadiff/{diff,render_deterministic}.go` | #204, #206, #207 |
| Deterministic comparator | `internal/driver/verify_compare.go` | #199, #217, #219 |
| MSSQL reader | `internal/driver/mssql/reader.go` | #195, #210 |
| MySQL reader | `internal/driver/mysql/reader.go` | #197, #209 |
| Config/secrets | `internal/config/config.go`, `internal/secrets/secrets.go` | #215, #216, #220 |
| CLI exit/apply paths | `cmd/smt/{sync,drift,run}.go` | #212, #214 |

Note the two-way overlaps: **#197** is in both the renderer and MySQL-reader clusters;
**#218** spans the renderer and canonical clusters; **#199** spans canonical and the
comparator; **#204** spans the renderer and snapshot-diff clusters. Whoever takes one of these
holds both surfaces for that change.

## ⚠️ RendererVersion is a shared serialization point

Ten issues bump `RendererVersion` (in `internal/ddl`): **#198, #199, #200, #201, #202, #203,
#204, #205, #218, #221**. It is a single integer plus the golden fixtures
(`internal/schemadiff/golden_test.go`, regen with `UPDATE_GOLDEN=1`). Independent branches
will collide on the bump and churn goldens. **Land these serially**, each rebasing on the
prior and taking the next version number, and regenerate goldens once per landing with the
diff justified in the commit. Do not fan them out.

---

## Suggested order

1. **Readers first — independent per engine, no `RendererVersion`, high impact.** The index
   introspection bugs (#194 reader half, #195, #196, #197 reader half) plus #209, #210. These
   are the matrix-invisible correctness core and mostly don't touch shared render code.
2. **Security / data-integrity that stand alone:** #201 (MySQL DDL-injection via unescaped
   backslash), #208 (snapshot baseline collision), #211 (DSN creds + `verify-ca` downgrade),
   #216 (webhook at rest + disable override), #215 (Windows secrets gate).
3. **Renderer + canonical clusters, serialized** (share `RendererVersion`): sequence
   #198 → #199 → #200 (canonical), then #202 → #203 → #204 → #205 → #194-render-gate →
   #197-render → #218 (renderer), each rebasing and bumping the version once.
4. **Snapshot-sync correctness:** #206, #207 (one worker — same files), then #212/#213/#214
   (apply recovery + exit codes — coordinate the recovery story across them).
5. **Comparator:** #217 then #219 (with #199 already landed, since all three touch
   `verify_compare.go`).
6. **Roll-ups last, one worker each (multi-item checklists):** #218 (folds into wave 3),
   #219, #220, #221.

---

## All issues

### Correctness — index introspection & rendering
| # | Title | Primary files |
|---|---|---|
| [194](../../issues/194) | Filtered/partial index predicates never captured; invalid `WHERE` on MySQL | mssql/pg `reader.go`, `ddl/renderer.go`, pg `deterministic.go` |
| [195](../../issues/195) | MSSQL covering-index INCLUDE columns leak into the key column list | `mssql/reader.go` |
| [196](../../issues/196) | PostgreSQL expression indexes vanish; INCLUDE columns flatten into keys/PK | `postgres/reader.go` |
| [197](../../issues/197) | MySQL functional indexes break the load; prefix lengths dropped | `mysql/reader.go`, `ddl/renderer.go` |

### Correctness — canonical type mapping (`RendererVersion`)
| # | Title | Primary files |
|---|---|---|
| [198](../../issues/198) | pg array element types collapse (`bigint[]`→`integer[]`, `varchar[]` loses length) | `canonical/{to,from}_canonical.go` |
| [199](../../issues/199) | `bit`/`bit(N)`→boolean regardless of dialect/width; comparator blesses the loss | `canonical/*`, `verify_compare.go` |
| [200](../../issues/200) | Unconstrained `numeric`→`DECIMAL(18,0)`, UNSIGNED dropped, precision not validated | `canonical/{to,from}_canonical.go` |

### Correctness — expression IR & renderer (`RendererVersion`)
| # | Title | Primary files |
|---|---|---|
| [201](../../issues/201) | MySQL string render never escapes backslashes (corruption + DDL injection) | `expr/render.go` |
| [202](../../issues/202) | pg identity column ALTER TYPE emits invalid SQL (breaks int→bigint PK widening) | `ddl/renderer.go`, pg `deterministic.go` |
| [203](../../issues/203) | Computed columns lose NOT NULL on MSSQL/MySQL targets | `ddl/renderer.go` |
| [204](../../issues/204) | MySQL `ALTER … SET DEFAULT <function>` emits invalid syntax | `ddl/renderer.go`, `schemadiff/render_deterministic.go` |
| [205](../../issues/205) | MSSQL→MySQL computed string concat without a literal operand coerces to numbers | `ddl/renderer.go` |

### Correctness — snapshot sync
| # | Title | Primary files |
|---|---|---|
| [206](../../issues/206) | Snapshot mode blind to side-object definition changes and PK changes | `schemadiff/diff.go`, `render_deterministic.go` |
| [207](../../issues/207) | No cross-table dependency ordering in rendered plans | `schemadiff/render_deterministic.go` |
| [208](../../issues/208) | Snapshot baseline keyed only on (source_type, schema) — different DBs share a baseline | `checkpoint/snapshots.go` |

### Correctness / robustness — readers & connection
| # | Title | Primary files |
|---|---|---|
| [209](../../issues/209) | MySQL `DEFAULT ''` conflated with no-default; MariaDB per-table CHECK cross-contamination | `mysql/reader.go` |
| [210](../../issues/210) | MSSQL identity probe lacks QUOTENAME (aborts on quoted names); missing `rows.Err()` | `mssql/reader.go` |
| [211](../../issues/211) | DSN creds double-escaped; `verify-ca`→`skip-verify`; pg/mssql space→`+` in userinfo | mysql/pg/mssql `dialect.go` |

### Correctness — orchestrator & CLI
| # | Title | Primary files |
|---|---|---|
| [212](../../issues/212) | `sync --apply` writes no artifact/run record; snapshot-mode rerun wedges | `cmd/smt/sync.go` |
| [213](../../issues/213) | `executePlan` idempotent skip is shape-blind yet records `success` | `orchestrator/phases.go` |
| [214](../../issues/214) | drift `3=drift` collides with classifier fallback; apply errors misclassify (6/15) | `exitcodes/`, `cmd/smt/{drift,sync,run}.go` |

### Security / config / state
| # | Title | Primary files |
|---|---|---|
| [215](../../issues/215) | Secrets 0600 gate has no Windows branch — `secrets.Load()` never succeeds on Windows | `secrets/secrets.go`, `checkpoint/profiles.go` |
| [216](../../issues/216) | `slack.enabled:false` can't disable; webhook stored unredacted in state DB | `config/config.go`, `orchestrator/orchestrator.go` |
| [217](../../issues/217) | Comparator flags correct oversized-varchar→MEDIUMTEXT as permanent drift (CI exit 3) | `driver/verify_compare.go` |

### Design / hardening roll-ups (checklists — one worker each)
| # | Title | Primary files |
|---|---|---|
| [218](../../issues/218) | Type-IR minor fidelity gaps (binary→varbinary, unicode varchar→mssql, timetz, warn policy) | `canonical/from_canonical.go`, `ddl/renderer.go` |
| [219](../../issues/219) | AI-review & shared-driver hardening (OnUpdate, dispatch dup, registry rlock, cache race, …) | `driver/{ai_typemapper,ai_verify,verify_*,registry}.go` |
| [220](../../issues/220) | Config/state-DB/CLI hygiene (strict YAML, glob errors, timestamps, perms, phantom runs, …) | `config/`, `checkpoint/`, `cmd/smt/profile.go` |
| [221](../../issues/221) | expr: IN-list order-sensitive Equal; double-unary-minus renders `--` line comment | `expr/{equal,parse}.go` |
