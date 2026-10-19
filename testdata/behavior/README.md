# Independent behavioral acceptance

`v1` is a source-backed finite behavioral oracle for the pinned Azure SQL Edge
15.0.2000.1574 ARM64 -> PostgreSQL 17.11 pair. It is independent of SMT's mapper
and comparator. These are explicit live acceptance tests, excluded from `go test`.

`v1/source.sql` defines two small independent tables. `cases.json` describes
source and target operations, input conversion paths, and human-reviewed source
guards. `source-observations.json` records live source results; `manifest.json`
freezes their hashes after two agreeing runs in separate disposable databases.
Do not edit a frozen version to improve a candidate's score. Add a new version
and independently revalidate it instead.

The simple tier covers ordinary tables, tinyint, bounded varbinary, decimal
precision, nullability, defaults, keys and native operations. The edge tier
exercise fixed binary padding, legacy date rounding, time rounding and UTF-16
boundaries. Use each case's `tier` field to select a result group.

`legacy69` preserves the seven original experimental JSON files byte-for-byte.
Its manifest records the old 31/69 SMT and 57/69 Astra scores without endorsing
their expectations. Eight old near-midnight expectations contradict the live
source; six of those had been credited as Astra passes. The new version measures
the actual source result, including near-midnight clamps and explicit `24:00:00`
rejection. The remaining CRM cases have not all been converted into a live-source
oracle, so the old 69 are excluded from source-backed correctness claims.

See [behavioral acceptance](../../docs/behavioral-acceptance.md) for commands,
isolation, interpretation, and limits.

`lifecycle.py` and `lifecycle/main.go` are a separate production integration
test. They render checked DDL, introspect a real owned PostgreSQL database and
verify no-op drift/sync and refusal of altered restrictions/defaults. They never
change or compute the `v1` behavioral expectations.
