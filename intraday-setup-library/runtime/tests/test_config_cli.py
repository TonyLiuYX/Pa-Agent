"""Public source controls keep strict configuration and truthful provenance."""
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from runtime.config import RuntimeConfig
from runtime.__main__ import main, provider_for


class SourceConfigurationTests(unittest.TestCase):
    def test_session_counts_reject_booleans_fractional_and_out_of_bounds(self):
        for value in (True, False, 0, -1, 1.5, float('nan'), float('inf')):
            with self.subTest(demo_sessions=value), self.assertRaises(ValueError):
                RuntimeConfig(demo_sessions=value).validate()
        for value in (True, False, -1, 31, 1.5, float('nan'), float('inf')):
            with self.subTest(history_sessions=value), self.assertRaises(ValueError):
                RuntimeConfig(alpaca_history_sessions=value).validate()
        RuntimeConfig(demo_sessions=3, alpaca_history_sessions=0).validate()
        RuntimeConfig(alpaca_history_sessions=30).validate()

    def test_multi_session_demo_has_distinct_calendar_and_event_identities(self):
        provider = provider_for(RuntimeConfig(symbols=('SPY',), demo_minutes=1,
                                              demo_sessions=3, pace_seconds=0))
        rows = list(provider.events())
        sessions = [event for event in rows if event.kind == 'session']
        self.assertEqual(len(sessions), 3)
        self.assertEqual(len({event.event_id for event in rows}), len(rows))
        self.assertEqual(len({event.payload['open_time'] for event in sessions}), 3)
        self.assertEqual(provider.describe()['source_scope'], 'synthetic_not_market_data')
        self.assertEqual(provider.describe()['sessions'], 3)

    def test_source_controls_belong_to_frozen_observation_configuration(self):
        default = RuntimeConfig()
        self.assertNotEqual(default.fingerprint(), RuntimeConfig(demo_sessions=2).fingerprint())
        self.assertNotEqual(default.fingerprint(), RuntimeConfig(alpaca_history_sessions=0).fingerprint())
        self.assertEqual(default.fingerprint(), RuntimeConfig(port=9000, pace_seconds=.1).fingerprint())

    def test_live_source_uses_requested_history_count_without_network(self):
        with patch.dict(os.environ, {}, clear=True):
            provider = provider_for(RuntimeConfig(mode='live', alpaca_history_sessions=0))
        self.assertEqual(provider.history_sessions, 0)
        self.assertEqual(provider.describe()['feed'], 'iex')

    def test_doctor_reads_source_configuration_without_exposing_credentials(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'runtime.json'
            path.write_text(json.dumps({'demo_sessions': 4, 'alpaca_history_sessions': 0}))
            output = io.StringIO()
            with patch.object(sys, 'argv', ['runtime', 'doctor', '--config', str(path)]), \
                 patch.dict(os.environ, {'ALPACA_API_KEY_ID': 'fixture-key',
                                         'ALPACA_API_SECRET_KEY': 'fixture-secret'}, clear=True), \
                 redirect_stdout(output):
                main()
            row = json.loads(output.getvalue())
            self.assertEqual(row['demo_sessions'], 4)
            self.assertEqual(row['alpaca_history_sessions'], 0)
            self.assertTrue(row['alpaca_configured'])
            self.assertNotIn('fixture-key', output.getvalue())
            self.assertNotIn('fixture-secret', output.getvalue())


if __name__ == '__main__':
    unittest.main()
