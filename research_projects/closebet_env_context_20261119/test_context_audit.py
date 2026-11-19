import copy
import unittest

import pandas as pd

from context_audit import SOURCE_SHA256, assemble, describe, wave_proxy


def example_bars():
    items = []
    for day in range(-60, 5):
        price = 100 + (day + 60) * .1
        items.append({
            "D": day, "trade_date": f"2025-01-{(day + 61):02d}",
            "open": price, "high": price + 2, "low": price - 2, "close": price + .5,
            "valid_price": 1, "volume": 200 if day == 0 else 100,
            "amount": 20000 if day == 0 else 10000,
            "MA20": 103, "MA60": 103, "MA112": 103, "MA224": 110,
        })
    return pd.DataFrame(items)


EVENT = {"code": "005930", "date": "2025-03-31", "year": 2025, "signal_group": "NO_TOUCH"}


class ContextAuditTests(unittest.TestCase):
    def test_twenty_day_flow_excludes_d0(self):
        x = describe(EVENT, example_bars())
        self.assertEqual(x["context_status"], "KNOWN_PRE20_UNCERTIFIED_SOURCE")
        self.assertEqual(x["volume_ratio_d0_to_pre20"], 2)
        self.assertEqual(x["amount_ratio_d0_to_pre20"], 2)

    def test_dplus_changes_do_not_change_d0_features(self):
        bars = example_bars()
        baseline = describe(EVENT, bars)
        bars.loc[bars.D.gt(0), ["close", "volume", "amount"]] = [999, 9999999, 99999999]
        self.assertEqual(baseline, describe(EVENT, bars))

    def test_missing_prior_day_never_fills_as_zero(self):
        bars = example_bars()
        bars = bars.loc[bars.D.ne(-12)]
        x = describe(EVENT, bars)
        self.assertEqual(x["context_status"], "UNKNOWN_PRE20_PRICE_OR_FLOW")
        self.assertNotIn("volume_ratio_d0_to_pre20", x)

    def test_missing_flow_is_unknown(self):
        bars = example_bars()
        bars.loc[bars.D.eq(-2), "amount"] = float("nan")
        self.assertEqual(describe(EVENT, bars)["context_status"], "UNKNOWN_PRE20_PRICE_OR_FLOW")

    def test_60_day_incomplete_is_not_wave_candidate(self):
        bars = example_bars().loc[lambda d: d.D.ne(-50)]
        x = describe(EVENT, bars)
        self.assertEqual(x["context_status"], "KNOWN_PRE20_UNCERTIFIED_SOURCE")
        self.assertEqual(x["history60_status"], "UNKNOWN_INCOMPLETE_60_BARS")
        self.assertEqual(x["wave_proxy_status"], "UNKNOWN")

    def test_224_ma_stays_source_only(self):
        x = describe(EVENT, example_bars())
        self.assertEqual(x["ma224_basis"], "PILOT_STORED_ONLY_UNCERTIFIED")
        self.assertLess(x["d0_close_vs_ma224_pct"], 0)

    def test_wave_proxy_respects_pre_d0_trough_peak_retrace(self):
        bars = example_bars().loc[lambda d: d.D.le(-1)].copy()
        # Prior 10 -> 20 rise, then -1 falls to 16: 40% retracement.
        bars.loc[bars.D.between(-60, -20), "close"] = 10
        bars.loc[bars.D.between(-19, -3), "close"] = 20
        bars.loc[bars.D.between(-2, -1), "close"] = 16
        self.assertEqual(wave_proxy(bars)["wave_proxy_status"], "CANDIDATE")

    def test_300_frozen_and_fullmarket_gates(self):
        events = [dict(EVENT, code=str(i).zfill(6)) for i in range(300)]
        for e in events:
            e.update(context_status="UNKNOWN_PRE20_PRICE_OR_FLOW", history60_status="UNKNOWN", wave_proxy_status="UNKNOWN")
        keys = {(e["code"], e["date"]) for e in events}
        manifest = {"data_retrieved": False, "not_pit_certified": True}
        result = assemble(events, SOURCE_SHA256, keys, manifest)
        self.assertFalse(result["can_modify_live_search_watch"])
        self.assertFalse(result["can_publish_strategy_performance"])
        self.assertIsNone(result["strategy_winner"])
        with self.assertRaisesRegex(ValueError, "RECONCILIATION"):
            assemble(events, SOURCE_SHA256, keys - {next(iter(keys))}, manifest)
        changed = copy.deepcopy(manifest)
        changed["data_retrieved"] = True
        with self.assertRaisesRegex(ValueError, "FULL_MARKET"):
            assemble(events, SOURCE_SHA256, keys, changed)


if __name__ == "__main__":
    unittest.main()
