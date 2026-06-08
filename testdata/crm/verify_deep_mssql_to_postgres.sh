#!/usr/bin/env bash
# Deep CRM schema verification for MSSQL source -> PostgreSQL target migrations.
#
# Usage:
#   verify_deep_mssql_to_postgres.sh <mssql_db> <mssql_schema> <pg_db> <pg_schema>
#
# Defaults assume the WSL CRM fixture containers:
#   MSSQL_CONTAINER=dmt-wsl-mssql
#   PG_CONTAINER=dmt-wsl-postgres

set -euo pipefail

usage() {
  cat >&2 <<'USAGE'
Usage:
  verify_deep_mssql_to_postgres.sh <mssql_db> <mssql_schema> <pg_db> <pg_schema>

Environment overrides:
  MSSQL_CONTAINER   default: dmt-wsl-mssql
  MSSQL_SQLCMD      default: /opt/mssql-tools18/bin/sqlcmd
  MSSQL_HOST        default: localhost
  MSSQL_USER        default: sa
  MSSQL_PASS        default: TestPass2024
  PG_CONTAINER      default: dmt-wsl-postgres
  PG_HOST           default: localhost
  PG_USER           default: postgres
  PG_PASS           default: TestPass2024

Set MSSQL_CONTAINER='' or PG_CONTAINER='' to run sqlcmd/psql on the host
instead of through docker exec.
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -ne 4 ]]; then
  usage
  exit 2
fi

MSSQL_DB="$1"
MSSQL_SCHEMA="$2"
PG_DB="$3"
PG_SCHEMA="$4"

MSSQL_CONTAINER="${MSSQL_CONTAINER-dmt-wsl-mssql}"
MSSQL_SQLCMD="${MSSQL_SQLCMD:-/opt/mssql-tools18/bin/sqlcmd}"
MSSQL_HOST="${MSSQL_HOST:-localhost}"
MSSQL_USER="${MSSQL_USER:-sa}"
MSSQL_PASS="${MSSQL_PASS:-TestPass2024}"

PG_CONTAINER="${PG_CONTAINER-dmt-wsl-postgres}"
PG_HOST="${PG_HOST:-localhost}"
PG_USER="${PG_USER:-postgres}"
PG_PASS="${PG_PASS:-TestPass2024}"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
failures=0

mssql_query() {
  local query="$1"

  if [[ -n "$MSSQL_CONTAINER" ]]; then
    docker exec "$MSSQL_CONTAINER" "$MSSQL_SQLCMD" \
      -S "$MSSQL_HOST" -U "$MSSQL_USER" -P "$MSSQL_PASS" -C -d "$MSSQL_DB" \
      -h-1 -W -s '|' -Q "$query" 2>/dev/null
  else
    "$MSSQL_SQLCMD" \
      -S "$MSSQL_HOST" -U "$MSSQL_USER" -P "$MSSQL_PASS" -C -d "$MSSQL_DB" \
      -h-1 -W -s '|' -Q "$query" 2>/dev/null
  fi | sed -e '/^$/d' -e '/^-/d' -e '/rows affected/d' -e '/Changed database/d'
}

pg_query() {
  local query="$1"

  if [[ -n "$PG_CONTAINER" ]]; then
    docker exec -e PGPASSWORD="$PG_PASS" "$PG_CONTAINER" psql \
      -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" -At -F '|' -v ON_ERROR_STOP=1 \
      -c "$query" 2>/dev/null
  else
    PGPASSWORD="$PG_PASS" psql \
      -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" -At -F '|' -v ON_ERROR_STOP=1 \
      -c "$query" 2>/dev/null
  fi
}

compare_files() {
  local label="$1"
  local src="$2"
  local tgt="$3"

  sort "$src" > "$src.sorted"
  sort "$tgt" > "$tgt.sorted"

  if diff -u "$src.sorted" "$tgt.sorted"; then
    echo "PASS $label"
  else
    echo "FAIL $label"
    failures=$((failures + 1))
  fi
}

echo "== Deep verification: $MSSQL_DB.$MSSQL_SCHEMA -> $PG_DB.$PG_SCHEMA =="

mssql_query "
SET NOCOUNT ON;
SELECT 'tables|' + CAST(COUNT(*) AS varchar(20))
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = '$MSSQL_SCHEMA' AND TABLE_TYPE = 'BASE TABLE'
UNION ALL
SELECT 'columns|' + CAST(COUNT(*) AS varchar(20))
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = '$MSSQL_SCHEMA'
UNION ALL
SELECT 'primary_keys|' + CAST(COUNT(*) AS varchar(20))
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
WHERE TABLE_SCHEMA = '$MSSQL_SCHEMA' AND CONSTRAINT_TYPE = 'PRIMARY KEY'
UNION ALL
SELECT 'foreign_keys|' + CAST(COUNT(*) AS varchar(20))
FROM sys.foreign_keys fk
JOIN sys.schemas s ON s.schema_id = fk.schema_id
WHERE s.name = '$MSSQL_SCHEMA'
UNION ALL
SELECT 'computed_columns|' + CAST(COUNT(*) AS varchar(20))
FROM sys.computed_columns cc
JOIN sys.tables t ON t.object_id = cc.object_id
JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE s.name = '$MSSQL_SCHEMA';
" > "$work/source_counts.tsv"

pg_query "
SELECT 'tables|' || COUNT(*)::text
FROM information_schema.tables
WHERE table_schema = '$PG_SCHEMA' AND table_type = 'BASE TABLE'
UNION ALL
SELECT 'columns|' || COUNT(*)::text
FROM information_schema.columns
WHERE table_schema = '$PG_SCHEMA'
UNION ALL
SELECT 'primary_keys|' || COUNT(*)::text
FROM information_schema.table_constraints
WHERE table_schema = '$PG_SCHEMA' AND constraint_type = 'PRIMARY KEY'
UNION ALL
SELECT 'foreign_keys|' || COUNT(*)::text
FROM information_schema.table_constraints
WHERE table_schema = '$PG_SCHEMA' AND constraint_type = 'FOREIGN KEY'
UNION ALL
SELECT 'computed_columns|' || COUNT(*)::text
FROM information_schema.columns
WHERE table_schema = '$PG_SCHEMA' AND is_generated = 'ALWAYS';
" > "$work/target_counts.tsv"

compare_files "aggregate object counts" "$work/source_counts.tsv" "$work/target_counts.tsv"

mssql_query "
SET NOCOUNT ON;
SELECT
  LOWER(ku.TABLE_NAME) + '|' +
  STRING_AGG(LOWER(ku.COLUMN_NAME), ',') WITHIN GROUP (ORDER BY ku.ORDINAL_POSITION)
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
  ON ku.CONSTRAINT_CATALOG = tc.CONSTRAINT_CATALOG
 AND ku.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
 AND ku.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
WHERE tc.TABLE_SCHEMA = '$MSSQL_SCHEMA'
  AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
GROUP BY ku.TABLE_NAME, ku.CONSTRAINT_NAME;
" > "$work/source_pk.tsv"

pg_query "
SELECT
  lower(tc.table_name) || '|' ||
  string_agg(lower(kcu.column_name), ',' ORDER BY kcu.ordinal_position)
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON kcu.constraint_schema = tc.constraint_schema
 AND kcu.constraint_name = tc.constraint_name
WHERE tc.table_schema = '$PG_SCHEMA'
  AND tc.constraint_type = 'PRIMARY KEY'
GROUP BY tc.table_name, tc.constraint_name;
" > "$work/target_pk.tsv"

compare_files "primary key signatures" "$work/source_pk.tsv" "$work/target_pk.tsv"

mssql_query "
SET NOCOUNT ON;
SELECT
  LOWER(t.name) + '|' +
  LOWER(i.name) + '|' +
  STRING_AGG(LOWER(c.name), ',') WITHIN GROUP (ORDER BY ic.key_ordinal)
FROM sys.indexes i
JOIN sys.tables t ON t.object_id = i.object_id
JOIN sys.schemas s ON s.schema_id = t.schema_id
JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
JOIN sys.columns c ON c.object_id = t.object_id AND c.column_id = ic.column_id
WHERE s.name = '$MSSQL_SCHEMA'
  AND i.is_unique = 1
  AND i.is_primary_key = 0
  AND ic.is_included_column = 0
GROUP BY t.name, i.name;
" > "$work/source_unique.tsv"

pg_query "
SELECT
  lower(t.relname) || '|' ||
  lower(i.relname) || '|' ||
  string_agg(lower(a.attname), ',' ORDER BY k.ord)
FROM pg_index ix
JOIN pg_class t ON t.oid = ix.indrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
JOIN pg_class i ON i.oid = ix.indexrelid
JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ord) ON k.ord <= ix.indnkeyatts
JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
WHERE n.nspname = '$PG_SCHEMA'
  AND ix.indisunique
  AND NOT ix.indisprimary
GROUP BY t.relname, i.relname;
" > "$work/target_unique.tsv"

compare_files "unique index signatures" "$work/source_unique.tsv" "$work/target_unique.tsv"

mssql_query "
SET NOCOUNT ON;
SELECT
  LOWER(pt.name) + '|' +
  LOWER(fk.name) + '|' +
  STRING_AGG(LOWER(pc.name), ',') WITHIN GROUP (ORDER BY fkc.constraint_column_id) + '|' +
  LOWER(rt.name) + '|' +
  STRING_AGG(LOWER(rc.name), ',') WITHIN GROUP (ORDER BY fkc.constraint_column_id) + '|' +
  LOWER(REPLACE(fk.delete_referential_action_desc COLLATE DATABASE_DEFAULT, '_', ' '))
FROM sys.foreign_keys fk
JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
JOIN sys.tables pt ON pt.object_id = fk.parent_object_id
JOIN sys.schemas ps ON ps.schema_id = pt.schema_id
JOIN sys.columns pc ON pc.object_id = pt.object_id AND pc.column_id = fkc.parent_column_id
JOIN sys.tables rt ON rt.object_id = fk.referenced_object_id
JOIN sys.columns rc ON rc.object_id = rt.object_id AND rc.column_id = fkc.referenced_column_id
WHERE ps.name = '$MSSQL_SCHEMA'
GROUP BY pt.name, fk.name, rt.name, fk.delete_referential_action_desc;
" > "$work/source_fk.tsv"

pg_query "
SELECT
  lower(pt.relname) || '|' ||
  lower(con.conname) || '|' ||
  string_agg(lower(pa.attname), ',' ORDER BY k.ord) || '|' ||
  lower(rt.relname) || '|' ||
  string_agg(lower(ra.attname), ',' ORDER BY k.ord) || '|' ||
  CASE con.confdeltype
    WHEN 'a' THEN 'no action'
    WHEN 'r' THEN 'restrict'
    WHEN 'c' THEN 'cascade'
    WHEN 'n' THEN 'set null'
    WHEN 'd' THEN 'set default'
  END
FROM pg_constraint con
JOIN pg_class pt ON pt.oid = con.conrelid
JOIN pg_namespace pn ON pn.oid = pt.relnamespace
JOIN pg_class rt ON rt.oid = con.confrelid
JOIN LATERAL unnest(con.conkey, con.confkey) WITH ORDINALITY AS k(attnum, refattnum, ord) ON true
JOIN pg_attribute pa ON pa.attrelid = pt.oid AND pa.attnum = k.attnum
JOIN pg_attribute ra ON ra.attrelid = rt.oid AND ra.attnum = k.refattnum
WHERE con.contype = 'f'
  AND pn.nspname = '$PG_SCHEMA'
GROUP BY pt.relname, con.conname, rt.relname, con.confdeltype;
" > "$work/target_fk.tsv"

compare_files "foreign key signatures" "$work/source_fk.tsv" "$work/target_fk.tsv"

mssql_query "
SET NOCOUNT ON;
SELECT
  LOWER(t.name) + '|' +
  LOWER(cc.name) + '|' +
  CASE WHEN cc.is_persisted = 1 THEN 'stored' ELSE 'virtual' END
FROM sys.computed_columns cc
JOIN sys.tables t ON t.object_id = cc.object_id
JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE s.name = '$MSSQL_SCHEMA';
" > "$work/source_generated.tsv"

pg_query "
SELECT
  lower(table_name) || '|' ||
  lower(column_name) || '|stored'
FROM information_schema.columns
WHERE table_schema = '$PG_SCHEMA'
  AND is_generated = 'ALWAYS';
" > "$work/target_generated.tsv"

compare_files "generated column signatures" "$work/source_generated.tsv" "$work/target_generated.tsv"

echo "== Constraint behavior smoke test: Tags.color =="
if pg_query "
DO \$\$
DECLARE
  company_id integer;
BEGIN
  INSERT INTO ${PG_SCHEMA}.companies (code, name)
  VALUES ('SMTVERIFY', 'SMT verifier company')
  RETURNING id INTO company_id;

  INSERT INTO ${PG_SCHEMA}.tags (company_id, name, color)
  VALUES (company_id, 'valid color', '#A1b2C3');

  BEGIN
    INSERT INTO ${PG_SCHEMA}.tags (company_id, name, color)
    VALUES (company_id, 'invalid color', 'nothex');
    RAISE EXCEPTION 'invalid Tags.color value was accepted';
  EXCEPTION
    WHEN check_violation THEN
      NULL;
  END;

  RAISE EXCEPTION 'rollback verifier data';
EXCEPTION
  WHEN raise_exception THEN
    IF SQLERRM = 'rollback verifier data' THEN
      NULL;
    ELSE
      RAISE;
    END IF;
END
\$\$;
" >/dev/null; then
  echo "PASS Tags.color check constraint behavior"
else
  echo "FAIL Tags.color check constraint behavior"
  failures=$((failures + 1))
fi

if [[ "$failures" -gt 0 ]]; then
  echo "== Deep verification FAILED: $failures failure group(s) =="
  exit 1
fi

echo "== Deep verification PASS =="
