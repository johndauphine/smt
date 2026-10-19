#!/usr/bin/env python3
"""Independent source-backed SQL Server -> PostgreSQL behavioral acceptance.

No SMT mapper/comparator imports and no model requests. Source reference and
freeze are separate from candidate application; a frozen suite never self-edits.
"""
import argparse
import datetime
import json
from pathlib import Path
import re
import subprocess
import sys

from containers import DatabaseSetupFailure, Engine, SOURCE_SETTINGS, TARGET_SETTINGS, digest

HERE = Path(__file__).resolve().parent
SUITE = HERE / 'v1'
REPO = HERE.parent.parent
DEFAULT_CLIENT = REPO / '.acceptance-artifacts/behavior/client'
DEFAULT_BINDINGS = {'simple': ['public', 'behavior_simple'], 'edge': ['public', 'behavior_edge']}
SOURCE_BINDINGS = {'simple': ['dbo', 'behavior_simple'], 'edge': ['dbo', 'behavior_edge']}
ERRORS = {
    'mssql': {'range': {220, 8115}, 'length': {8152, 2628}, 'datetime': {241, 242}, 'unique': {2601, 2627}, 'nullability': {515}},
    'postgres': {'range': {'22003', '23514'}, 'length': {'22001', '23514'}, 'datetime': {'22007', '22008', '23514'}, 'unique': {'23505'}, 'nullability': {'23502'}},
}


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def suite_name():
    return 'behavior-' + SUITE.name


def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def save(path, value):
    Path(path).write_text(json.dumps(value, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')


def validate_bindings(bindings):
    if set(bindings) != set(DEFAULT_BINDINGS):
        raise ValueError('bindings must name exactly simple and edge')
    for parts in bindings.values():
        if not isinstance(parts, list) or len(parts) != 2 or any(not isinstance(p, str) or not re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]{0,62}', p) for p in parts):
            raise ValueError('bindings need two explicit portable schema/table identifier components')
    if len({tuple(v) for v in bindings.values()}) != len(bindings):
        raise ValueError('binding collision')
    return bindings


def bind(steps, bindings, engine):
    validate_bindings(bindings)
    result = []
    for step in steps:
        sql = step['sql']
        for token, parts in bindings.items():
            quoted = '.'.join(('[' + p + ']') if engine == 'mssql' else ('"' + p + '"') for p in parts)
            sql = sql.replace('{{' + token + '}}', quoted)
        if '{{' in sql or '}}' in sql:
            raise ValueError('unbound identifier token')
        result.append(dict(step, sql=sql))
    return result


def observe(result, engine, expected_category=None):
    if result.get('ok'):
        rows = result['rows']
        if len(rows) != 1 or set(rows[0]) != {'value'} or not isinstance(rows[0]['value'], str):
            return {'kind': 'harness_failure', 'detail': 'probe must return one string value', 'raw': result}
        return {'kind': 'value', 'value': rows[0]['value']}
    key = 'number' if engine == 'mssql' else 'sqlstate'
    if key not in result:
        return {'kind': 'harness_failure', 'detail': result.get('error', 'unknown transport failure')}
    code = result[key]
    # A target CHECK can represent a range, length, or time restriction. It is
    # accepted only for a frozen rejection in that declared semantic category.
    categories = [c for c, codes in ERRORS[engine].items() if code in codes]
    category = expected_category if expected_category in categories else (categories[0] if len(categories) == 1 else None)
    if category is None:
        return {'kind': 'unexpected_database_error', key: code, 'detail': result['error']}
    return {'kind': 'rejection', 'category': category}


def suite_hashes():
    return {p.name: digest(p.read_bytes()) for p in (SUITE / 'source.sql', SUITE / 'cases.json')}


def check_freeze():
    manifest = read(SUITE / 'manifest.json')
    if manifest['files'] != suite_hashes():
        raise ValueError('frozen source/cases changed; make a new version instead')
    if digest((SUITE / 'source-observations.json').read_bytes()) != manifest['source_observations_sha256']:
        raise ValueError('frozen source observations changed')
    return manifest


def reference(args):
    args.out.mkdir(parents=True, exist_ok=False)
    cases = read(SUITE / 'cases.json')
    report = {'format_version': 1, 'suite': suite_name(), 'started_at': now(), 'files': suite_hashes(),
              'settings': SOURCE_SETTINGS, 'cases': [], 'outcome': 'started'}
    db = Engine('mssql', args.client)
    try:
        with db:
            report['engine'] = db.metadata
            db.require((SUITE / 'source.sql').read_text(encoding='utf-8'))
            for case in cases:
                raw = db.query(bind(case['source'], SOURCE_BINDINGS, 'mssql'))
                observation = observe(raw, 'mssql', case['guard'].get('category'))
                item = {'id': case['id'], 'observation': observation, 'guard_pass': observation == case['guard'], 'raw': raw}
                report['cases'].append(item)
                save(args.out / 'reference.json', report)
                print(case['id'] + ': ' + ('ok' if item['guard_pass'] else 'SOURCE GUARD MISMATCH'), flush=True)
            report['outcome'] = 'validated' if all(x['guard_pass'] for x in report['cases']) else 'source_guard_mismatch'
    except DatabaseSetupFailure as exc:
        report.update(outcome='source_setup_failure', error=str(exc), database_error=exc.result)
    except Exception as exc:
        report.update(outcome='harness_failure', error=str(exc))
    finally:
        report.update(container_removed=db.removed, finished_at=now())
        save(args.out / 'reference.json', report)
    return 0 if report['outcome'] == 'validated' and db.removed else 1


def freeze(args):
    if (SUITE / 'manifest.json').exists():
        raise ValueError('v1 already frozen: never replace expectations to match candidates')
    reports = [read(p / 'reference.json') for p in args.reference]
    cases = read(SUITE / 'cases.json')
    wanted = [c['id'] for c in cases]
    if len(reports) < 2:
        raise ValueError('two independent fresh source runs are required')
    if len({r.get('engine', {}).get('container_id') for r in reports}) != len(reports):
        raise ValueError('source references must use distinct fresh containers')
    for r in reports:
        if r['outcome'] != 'validated' or not r['container_removed'] or r['files'] != suite_hashes() or [c['id'] for c in r['cases']] != wanted:
            raise ValueError('source reference incomplete, changed, unvalidated, or not cleaned up')
    canonical = [{'id': c['id'], 'observation': c['observation']} for c in reports[0]['cases']]
    if any([{'id': c['id'], 'observation': c['observation']} for c in r['cases']] != canonical for r in reports[1:]):
        raise ValueError('source runs disagree')
    observations = {'suite': suite_name(), 'reference_engine': reports[0]['engine'], 'settings': SOURCE_SETTINGS,
                    'cases': canonical, 'fresh_source_runs': len(reports)}
    save(SUITE / 'source-observations.json', observations)
    manifest = {'format_version': 1, 'suite': suite_name(), 'frozen_at': now(), 'files': suite_hashes(),
                'source_observations_sha256': digest((SUITE / 'source-observations.json').read_bytes()),
                'source_run_sha256': [digest((p / 'reference.json').read_bytes()) for p in args.reference],
                'case_count': len(cases), 'verified_source_case_ids': wanted,
                'legacy69_status': 'Archived separately, not a source-backed correctness gate',
                'limitations': ['Retired Azure SQL Edge ARM64 15.0.2000.1574, not a supported SQL Server release',
                                'Finite probes do not establish universal equivalence',
                                'Explicit width conversion case models documented source-side conversion before assignment; target SQL truncates explicitly for this path only',
                                'No CRM 69-case source conversion claim; collation, identity, offset, evolution and concurrency require further gates']}
    save(SUITE / 'manifest.json', manifest)
    return 0


def target(args):
    manifest = check_freeze()
    cases = read(SUITE / 'cases.json')
    expected = {c['id']: c['observation'] for c in read(SUITE / 'source-observations.json')['cases']}
    bindings = validate_bindings(read(args.bindings) if args.bindings else DEFAULT_BINDINGS)
    args.out.mkdir(parents=True, exist_ok=False)
    sql = args.sql.read_bytes()
    (args.out / 'candidate.sql').write_bytes(sql)
    report = {'format_version': 1, 'suite': suite_name(), 'started_at': now(), 'candidate_sha256': digest(sql),
              'frozen_manifest_sha256': digest((SUITE / 'manifest.json').read_bytes()), 'verified': False,
              'settings': TARGET_SETTINGS, 'bindings': bindings, 'cases': [], 'outcome': 'started',
              'harness_sha256': {p.name: digest(p.read_bytes()) for p in [HERE / 'run.py', HERE / 'containers.py', HERE / 'client/main.go']}}
    db = Engine('postgres', args.client)
    try:
        with db:
            report['engine'] = db.metadata
            db.require(sql.decode('utf-8'))
            names = db.query([{'sql': "SELECT string_agg(table_schema || '.' || table_name, ',' ORDER BY table_schema, table_name) AS value FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema NOT IN ('pg_catalog','information_schema')"}])
            name_observation = observe(names, 'postgres')
            report['naming'] = {'actual': name_observation, 'pass': name_observation == {'kind': 'value', 'value': ','.join(sorted('.'.join(v) for v in bindings.values()))}}
            for case in cases:
                oracle = expected[case['id']]
                raw = db.query(bind(case['target'], bindings, 'postgres'))
                observation = observe(raw, 'postgres', oracle.get('category'))
                report['cases'].append({'id': case['id'], 'tier': case['tier'], 'input_path': case['input_path'],
                                        'expected': oracle, 'observation': observation, 'pass': observation == oracle, 'raw': raw})
                save(args.out / 'report.json', report)
            report['passed'] = sum(c['pass'] for c in report['cases'])
            report['tiers'] = {tier: {'passed': sum(c['pass'] for c in report['cases'] if c['tier'] == tier),
                                      'total': sum(c['tier'] == tier for c in report['cases'])}
                               for tier in sorted({c['tier'] for c in report['cases']})}
            report['outcome'] = 'passed_covered_cases_only' if report['passed'] == manifest['case_count'] and report['naming']['pass'] else 'target_behavior_differences'
    except DatabaseSetupFailure as exc:
        report.update(outcome='candidate_setup_failure', error=str(exc), database_error=exc.result)
    except Exception as exc:
        report.update(outcome='harness_failure', error=str(exc))
    finally:
        report.update(container_removed=db.removed, finished_at=now())
        save(args.out / 'report.json', report)
    score = str(report['passed']) + '/' + str(len(cases)) if 'passed' in report else 'behavior not evaluated'
    print(report['outcome'] + ': ' + score + '; ' + str(args.out / 'report.json'))
    return 0 if report['outcome'] == 'passed_covered_cases_only' and db.removed else 1


def generate(args):
    check_freeze()
    args.out.mkdir(parents=True, exist_ok=False)
    # The exact native Linux CLI bytes supplied by the caller execute in Edge.
    config = '''source:
  type: mssql
  host: 127.0.0.1
  port: 1433
  database: smt_behavior
  user: sa
  password: ${env:MSSQL_SA_PASSWORD}
  schema: dbo
  encrypt: false
  trust_server_cert: true
target:
  type: postgres
  schema: public
schema_generation:
  mode: deterministic
  unknown_type_policy: fail
ai_review:
  enabled: false
  diagnose_failures: false
  suggest_fixes: false
migration:
  data_dir: /tmp/smt-results
  create_indexes: true
  create_foreign_keys: true
  create_check_constraints: true
slack:
  enabled: false
'''
    report = {'started_at': now(), 'binary_sha256': digest(args.binary.read_bytes()), 'profile': args.profile,
              'frozen_manifest_sha256': digest((SUITE / 'manifest.json').read_bytes()),
              'source_sha256': suite_hashes()['source.sql'], 'ai_calls': 0, 'outcome': 'started'}
    db = Engine('mssql', args.client)
    try:
        with db:
            report['engine'] = db.metadata
            db.require((SUITE / 'source.sql').read_text(encoding='utf-8'))
            db.upload(args.binary.read_bytes(), '/tmp/smt')
            db.upload(config.encode('utf-8'), '/tmp/smt-config.yaml')
            cmd = ['exec', '--user=mssql', db.container_id, 'env', 'SMT_SECRETS_FILE=/tmp/smt-no-secrets.yaml',
                   '/tmp/smt', '--config', '/tmp/smt-config.yaml', '--state-file', '/tmp/smt-state.yaml',
                   'create', '--out', '/tmp/smt-candidate.sql']
            if args.profile != 'legacy':
                cmd += ['--fidelity', args.profile]
            report['stdout'] = db.command(*cmd)
            candidate = db.command('exec', '--user=mssql', db.container_id, 'cat', '/tmp/smt-candidate.sql')
            (args.out / 'candidate.sql').write_text(candidate, encoding='utf-8')
            remote_hash = db.command('exec', '--user=mssql', db.container_id, 'sha256sum', '/tmp/smt-candidate.sql').split()[0]
            if digest((args.out / 'candidate.sql').read_bytes()) != remote_hash:
                raise ValueError('generated SQL bytes changed during transport')
            paths = db.command('exec', '--user=mssql', db.container_id, 'find', '/tmp/smt-results', '-name', 'manifest.json').splitlines()
            if len(paths) != 1 or not paths[0].startswith('/tmp/smt-results/'):
                raise ValueError('expected exactly one CLI manifest')
            cli_manifest = json.loads(db.command('exec', '--user=mssql', db.container_id, 'cat', paths[0]))
            if cli_manifest['ai_review_enabled'] is not False or cli_manifest['table_count'] != 2:
                raise ValueError('CLI did not generate both tables with AI disabled')
            save(args.out / 'smt-manifest.json', cli_manifest)
            report.update(outcome='generated', candidate_sha256=digest((args.out / 'candidate.sql').read_bytes()))
    except Exception as exc:
        report.update(outcome='generation_failure', error=str(exc))
    finally:
        report.update(container_removed=db.removed, finished_at=now())
        save(args.out / 'generation.json', report)
    print(report['outcome'] + ': ' + str(args.out))
    return 0 if report['outcome'] == 'generated' and db.removed else 1


def compare(args):
    baseline, candidate = read(args.baseline / 'report.json'), read(args.candidate / 'report.json')
    if baseline['frozen_manifest_sha256'] != candidate['frozen_manifest_sha256'] or baseline['bindings'] != candidate['bindings']:
        raise ValueError('comparison requires the same frozen oracle and identifier bindings')
    before = {c['id']: c for c in baseline['cases']}
    after = {c['id']: c for c in candidate['cases']}
    report = {'suite': suite_name(), 'baseline_sha256': digest((args.baseline / 'report.json').read_bytes()),
              'candidate_sha256': digest((args.candidate / 'report.json').read_bytes()),
              'fixed': [], 'regressed': [], 'unchanged_correct': [], 'unchanged_difference': [], 'unverified': []}
    for case in read(SUITE / 'cases.json'):
        name = case['id']
        a, b = before.get(name), after.get(name)
        if a is None or b is None or a['observation']['kind'] == 'harness_failure' or b['observation']['kind'] == 'harness_failure':
            report['unverified'].append(name)
        elif a['pass'] and not b['pass']:
            report['regressed'].append(name)
        elif not a['pass'] and b['pass']:
            report['fixed'].append(name)
        else:
            report['unchanged_correct' if b['pass'] else 'unchanged_difference'].append(name)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    if args.out.exists():
        raise ValueError('comparison output already exists')
    save(args.out, report)
    print(json.dumps({key: len(report[key]) for key in ['fixed', 'regressed', 'unchanged_correct', 'unchanged_difference', 'unverified']}))
    return 1 if report['regressed'] or report['unverified'] else 0


def main():
    global SUITE
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--client', type=Path, default=DEFAULT_CLIENT)
    p.add_argument('--suite', type=Path, default=SUITE, help='versioned fixture directory (default: v1)')
    sub = p.add_subparsers(dest='command', required=True)
    ref = sub.add_parser('reference'); ref.add_argument('--out', type=Path, required=True)
    fr = sub.add_parser('freeze'); fr.add_argument('--reference', type=Path, action='append', required=True)
    tar = sub.add_parser('target'); tar.add_argument('--sql', type=Path, required=True); tar.add_argument('--bindings', type=Path); tar.add_argument('--out', type=Path, required=True)
    gen = sub.add_parser('generate'); gen.add_argument('--binary', type=Path, required=True); gen.add_argument('--out', type=Path, required=True); gen.add_argument('--profile', choices=['legacy', 'checked', 'strict'], default='legacy')
    cmp = sub.add_parser('compare'); cmp.add_argument('--baseline', type=Path, required=True); cmp.add_argument('--candidate', type=Path, required=True); cmp.add_argument('--out', type=Path, required=True)
    sub.add_parser('verify-freeze')
    args = p.parse_args()
    SUITE = args.suite.resolve()
    try:
        if args.command == 'verify-freeze': check_freeze(); print('frozen suite hashes valid'); return 0
        return {'reference': reference, 'freeze': freeze, 'target': target, 'generate': generate, 'compare': compare}[args.command](args)
    except Exception as exc:
        print('harness failure: ' + str(exc), file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main())
