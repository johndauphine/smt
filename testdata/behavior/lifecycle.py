#!/usr/bin/env python3
"""Separate live production lifecycle integration; never changes the frozen oracle."""
import argparse
import json
from pathlib import Path
import sys

from containers import Engine, digest
from run import DEFAULT_CLIENT, HERE, now, save


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--client', type=Path, default=DEFAULT_CLIENT)
    p.add_argument('--binary', type=Path, required=True)
    p.add_argument('--out', type=Path, required=True)
    args = p.parse_args()
    args.out.mkdir(parents=True, exist_ok=False)
    report = {'format_version': 1, 'kind': 'checked_lifecycle_integration', 'started_at': now(),
              'binary_sha256': digest(args.binary.read_bytes()),
              'harness_sha256': digest(Path(__file__).read_bytes()),
              'client_source_sha256': digest((HERE / 'lifecycle/main.go').read_bytes()), 'outcome': 'started'}
    db = Engine('postgres', args.client, postgres_loopback=True)
    try:
        with db:
            report['engine'] = db.metadata
            db.upload(args.binary.read_bytes(), '/tmp/smt-behavior-lifecycle')
            output = db.command('exec', '--user=postgres', db.container_id, '/tmp/smt-behavior-lifecycle')
            (args.out / 'integration-stdout.txt').write_text(output, encoding='utf-8')
            # Production rendering logs mapping warnings before the final JSON.
            result = json.loads(output.strip().splitlines()[-1])
            report['integration'] = result
            report['outcome'] = result['outcome']
    except Exception as exc:
        report.update(outcome='harness_failure', error=str(exc))
    finally:
        report.update(container_removed=db.removed, finished_at=now())
        save(args.out / 'report.json', report)
    assertions = report.get('integration', {}).get('assertions', [])
    print(report['outcome'] + ': ' + str(sum(a['pass'] for a in assertions)) + '/' + str(len(assertions)) + '; ' + str(args.out / 'report.json'))
    return 0 if report['outcome'] == 'passed' and db.removed else 1


if __name__ == '__main__':
    sys.exit(main())
