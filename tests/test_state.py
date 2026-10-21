import tempfile
import unittest
from pathlib import Path

from grid_bot.risk import RiskState
from grid_bot.state import RunnerState, StateStore, load_runner_state, save_runner_state


class StateStoreTests(unittest.TestCase):
    def test_round_trips_peak_roi(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = StateStore(str(Path(directory) / "state.json"))
            store.save_risk_state(RiskState(peak_roi=0.42))
            self.assertEqual(store.load_risk_state().peak_roi, 0.42)

    def test_rejects_corrupted_runner_state_without_resetting_it(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            path.write_text("{")
            store = StateStore(str(path))

            with self.assertRaisesRegex(RuntimeError, "reconcile the account"):
                load_runner_state(store)
            self.assertEqual(path.read_text(), "{")

    def test_round_trips_grid_baseline_timestamp(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = StateStore(str(Path(directory) / "state.json"))
            save_runner_state(store, RunnerState(
                baseline_account_equity=1000.0,
                baseline_started_at="2026-10-20T01:52:34.236+00:00",
                phase="RUNNING",
            ))

            state = load_runner_state(store)

        self.assertEqual(state.baseline_started_at, "2026-10-20T01:52:34.236+00:00")
