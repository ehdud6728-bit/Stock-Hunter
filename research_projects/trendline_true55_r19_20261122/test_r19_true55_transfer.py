"""Offline, deterministic R19 as-of and source-contract regression tests."""
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd

import r19_true55_transfer as r19


def synthetic_frame(n=131):
    dates = pd.date_range("2021-01-01", periods=n, freq="B").strftime("%Y-%m-%d")
    rows = pd.DataFrame({"date": dates, "code": ["TEST01"] * n,
                         "open": [99.] * n, "high": [100.] * n,
                         "low": [98.] * n, "close": [99.] * n,
                         "volume": [1000.] * n, "amount": [99000.] * n})
    rows.loc[60, "close"] = 102.
    rows.loc[60, "high"] = 150.  # Same-day high must NOT be included in prior 55 highs.
    rows.loc[61, "open"] = 101.
    return rows


class FrozenR17Stub:
    @staticmethod
    def exit_close_next_open(data, t, days, kind):
        return {"net_pct": -3.0, "exit_day": 3, "exit_reason": kind}

    @staticmethod
    def make_features(data, t):
        return {f: 1.0 for f in r19.FEATURES}


class True55Tests(unittest.TestCase):
    def test_only_prior_55_high_not_today_high(self):
        rows = synthetic_frame()
        candidates, high = r19.prior55_candidates(rows)
        self.assertEqual(high.iloc[60], 100.)
        self.assertIn(60, candidates)
        self.assertEqual(high.iloc[61], 150.)
        self.assertNotIn(61, candidates)

    def test_strict_close_above_not_equal(self):
        rows = synthetic_frame()
        rows.loc[60, "close"] = 100.
        candidates, _ = r19.prior55_candidates(rows)
        self.assertNotIn(60, candidates)

    def test_dplus1_open_gate_is_strict(self):
        rows = synthetic_frame()
        rows.loc[61, "open"] = 100.
        signals, audit = r19.collect_signals(rows, FrozenR17Stub)
        self.assertEqual(len(signals), 0)
        self.assertEqual(audit["dplus1_open_below_or_equal_breakout"], 1)

    def test_complete_horizon_gate_and_no_lookahead(self):
        rows = synthetic_frame()
        signals, audit = r19.collect_signals(rows, FrozenR17Stub)
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0]["date"], str(rows.date.iloc[60]))
        self.assertEqual(audit["raw_d0_breakouts"], 1)
        self.assertEqual(signals[0]["outcomes"]["D0_LOW_NEXT_OPEN"]["exit_day"], 3)
        rows.iloc[62:100, rows.columns.get_loc("high")] = 10000.
        changed, _ = r19.collect_signals(rows, FrozenR17Stub)
        self.assertEqual(changed[0]["date"], signals[0]["date"])
        self.assertEqual(changed[0]["features"], signals[0]["features"])
        self.assertNotEqual(changed[0]["expost_not_tradable"],
                            signals[0]["expost_not_tradable"])
        short = synthetic_frame(120)
        incomplete, short_audit = r19.collect_signals(short, FrozenR17Stub)
        self.assertEqual(len(incomplete), 0)
        self.assertEqual(short_audit["incomplete_d60"], 1)

    def test_sixty_bar_embargo_applies_before_future_gate(self):
        rows = synthetic_frame(205)
        rows.loc[70, ["close", "high"]] = [155., 156.]
        rows.loc[140, ["close", "high"]] = [160., 161.]
        rows.loc[141, "open"] = 157.
        signals, audit = r19.collect_signals(rows, FrozenR17Stub)
        self.assertEqual(audit["after_60bar_embargo"], 2)
        self.assertEqual(len(signals), 2)
        self.assertEqual(signals[1]["date"], str(rows.date.iloc[140]))

    def test_training_cutoff_and_quantile_only_from_past(self):
        def mk(i, year, ret):
            return {"date": f"{year}-06-{i%28+1:02d}",
                    "features": {f: 0.1 * i + j for j, f in enumerate(r19.FEATURES)},
                    "outcomes": {r19.EXITS[0]: {"net_pct": ret}}}
        early = [mk(i, 2021, float(i-30)) for i in range(30)]
        late = [mk(i, 2024, 999.0) for i in range(6)]
        _, q10, q75, n = r19.train_warning(early + late, r19.EXITS[0])
        self.assertEqual(n, 30)
        self.assertAlmostEqual(q10, float(np.quantile([i-30 for i in range(30)], .1)))
        _, early_only_q, early_only_p, _ = r19.train_warning(early, r19.EXITS[0])
        self.assertEqual(q10, early_only_q)
        self.assertEqual(q75, early_only_p)

    def test_invalid_provenance_rejected_even_with_valid_price_file(self):
        with TemporaryDirectory() as d:
            base = Path(d)
            (base / "R16_SOURCE_MANIFEST.json").write_text('{"kis_run_id":"wrong"}')
            with self.assertRaises(ValueError):
                r19.read_frozen_sources(base, base)

    def test_no_fixed_percentage_stops_or_credential_paths(self):
        code = Path(r19.__file__).read_text()
        self.assertNotIn("-5%", code)
        self.assertNotIn("+10%", code)
        self.assertNotIn("KIS_APP_SECRET", code)
        self.assertEqual(r19.EXITS, ("D0_LOW_NEXT_OPEN", "PREV5_LOW_NEXT_OPEN",
                                      "PREV10_LOW_NEXT_OPEN"))


if __name__ == "__main__":
    unittest.main()
