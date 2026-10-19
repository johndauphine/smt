"""Hermetic harness contract checks; never start an engine."""
import copy
import unittest

import run


class RunnerTests(unittest.TestCase):
    def test_unrelated_errors_never_satisfy_expected_rejection(self):
        for code in ('42P01', '42601', '42501', '23503'):
            observation = run.observe({'ok': False, 'sqlstate': code, 'error': 'unrelated failure'}, 'postgres', 'range')
            self.assertEqual(observation['kind'], 'unexpected_database_error')
        self.assertNotEqual(run.observe({'ok': False, 'sqlstate': '22001', 'error': 'length'}, 'postgres', 'range'),
                            {'kind': 'rejection', 'category': 'range'})

    def test_transport_failure_is_not_source_or_target_rejection(self):
        self.assertEqual(run.observe({'ok': False, 'error': 'connection refused'}, 'mssql')['kind'], 'harness_failure')

    def test_one_string_value_contract(self):
        for rows in ([], [{'value': 1}], [{'value': 'ok', 'extra': 'bad'}], [{'value': 'a'}, {'value': 'b'}]):
            self.assertEqual(run.observe({'ok': True, 'rows': rows}, 'postgres')['kind'], 'harness_failure')
        self.assertEqual(run.observe({'ok': True, 'rows': [{'value': '00:00:00'}]}, 'postgres'), {'kind': 'value', 'value': '00:00:00'})

    def test_binding_is_explicit_reversible_and_preserves_parameters(self):
        steps = [{'sql': 'INSERT INTO {{simple}}(tiny) VALUES($1)', 'params': [{'name': 'v', 'kind': 'int', 'value': 255}]}]
        original = copy.deepcopy(steps)
        actual = run.bind(steps, run.DEFAULT_BINDINGS, 'postgres')
        self.assertEqual(actual[0]['sql'], 'INSERT INTO "public"."behavior_simple"(tiny) VALUES($1)')
        self.assertEqual(actual[0]['params'], steps[0]['params'])
        self.assertEqual(steps, original)
        self.assertEqual(actual[0]['sql'].replace('"public"."behavior_simple"', '{{simple}}'), steps[0]['sql'])
        for bad in ({'simple': ['public', 'x']}, {'simple': ['public', 'x'], 'edge': ['public', 'x']},
                    {'simple': ['public', 'x; DROP'], 'edge': ['public', 'y']}):
            with self.assertRaises(ValueError):
                run.validate_bindings(bad)
        with self.assertRaises(ValueError):
            run.bind([{'sql': 'SELECT * FROM {{unknown}}'}], run.DEFAULT_BINDINGS, 'postgres')

    def test_frozen_guards_agree_with_observations(self):
        if not (run.SUITE / 'manifest.json').exists():
            self.skipTest('suite not yet frozen')
        manifest = run.check_freeze()
        cases = run.read(run.SUITE / 'cases.json')
        observations = run.read(run.SUITE / 'source-observations.json')['cases']
        self.assertEqual(len(cases), manifest['case_count'])
        self.assertEqual([c['id'] for c in cases], [c['id'] for c in observations])
        self.assertEqual([c['guard'] for c in cases], [c['observation'] for c in observations])

    def test_legacy69_archive_bytes_unchanged(self):
        folder = run.HERE / 'legacy69'
        manifest = run.read(folder / 'manifest.json')
        count = 0
        for name, expected in manifest['files'].items():
            self.assertEqual(run.digest((folder / name).read_bytes()), expected)
            count += len(run.read(folder / name))
        self.assertEqual(count, 69)
        self.assertFalse(manifest['source_verified'])


if __name__ == '__main__':
    unittest.main()
