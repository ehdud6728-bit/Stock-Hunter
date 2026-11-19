"""Synthetic, research-only invariants for frozen-pilot year audit."""
import unittest
from year_fragility_audit import make_audit, path_net20, summarize, EXPECTED_SOURCE_SHA256


def event(code, date, year, group, net=None):
    return {
        "code": code, "date": date, "year": year, "signal_group": group,
        "forward_paths": {"20": (
            {"status": "PROVISIONAL_SYNTHETIC_ONLY",
             "net_return_pct_by_total_cost_bps": {"20": net}}
            if net is not None else {"status": "UNKNOWN_INVALID_PATH_BARS"}
        )}
    }


def sample_300():
    return [
        event(f"{year % 100:02d}{i:04d}", f"{year}-{i // 28 + 1:02d}-{i % 28 + 1:02d}",
              year, "ENV10_ONLY" if i < 10 else "NO_TOUCH", float(i - 25))
        for year in range(2021, 2026) for i in range(60)
    ]


class YearFragilityAuditTests(unittest.TestCase):
    def test_best_single_case_can_reverse_mean(self):
        events = [event("000001", "2021-01-01", 2021, "ENV10_ONLY", -2),
                  event("000002", "2021-01-02", 2021, "ENV10_ONLY", -1),
                  event("000003", "2021-01-03", 2021, "ENV10_ONLY", 100)]
        s = summarize(events)
        self.assertAlmostEqual(s["mean_net20bps_d20_pct"], 97 / 3, places=5)
        self.assertAlmostEqual(s["mean_without_single_best_case_pct"], -1.5)
        self.assertTrue(s["best_case_removal_changes_mean_sign"])
        self.assertEqual(s["best_case_code"], "000003")

    def test_missing_path_never_counts_as_zero(self):
        s = summarize([event("000001", "2021-01-01", 2021, "ENV20_BOTH", None)])
        self.assertEqual(s["unknown_d20_count"], 1)
        self.assertEqual(s["known_d20_count"], 0)
        self.assertIsNone(s["mean_net20bps_d20_pct"])
        self.assertIsNone(s["mean_without_single_best_case_pct"])

    def test_repeated_ticker_is_not_independent(self):
        s = summarize([event("000001", "2021-01-01", 2021, "ENV10_ONLY", 2),
                       event("000001", "2021-02-01", 2021, "ENV10_ONLY", 3)])
        self.assertEqual(s["event_count"], 2)
        self.assertEqual(s["unique_tickers"], 1)

    def test_full_300_reporting_is_descriptive_only(self):
        events = sample_300()
        keys = {(e["code"], e["date"]) for e in events}
        a = make_audit(events, EXPECTED_SOURCE_SHA256,
                       {"data_retrieved": False, "not_pit_certified": True,
                        "n_source_requests": 85576}, ["date"], keys)
        self.assertEqual(a["pilot_events"], 300)
        self.assertEqual(a["by_year"]["2024"]["groups"]["ENV10_ONLY"]["event_count"], 10)
        self.assertEqual(a["market_regime_status"], "UNKNOWN_CALENDAR_ONLY_NO_INDEX_PRICE_LEVELS")
        self.assertIsNone(a["strategy_winner"])
        self.assertFalse(a["can_modify_live_search_watch"])

    def test_unmatched_original_pool_blocks_research(self):
        events = sample_300()
        with self.assertRaisesRegex(ValueError, "PILOT_EVENT_NOT_IN_PRESELECTED_1500"):
            make_audit(events, EXPECTED_SOURCE_SHA256,
                       {"data_retrieved": False, "not_pit_certified": True},
                       ["date"], set())

    def test_fullmarket_state_change_requires_review(self):
        events = sample_300()
        keys = {(e["code"], e["date"]) for e in events}
        with self.assertRaisesRegex(ValueError, "FULLMARKET_SOURCE_READINESS_CHANGED"):
            make_audit(events, EXPECTED_SOURCE_SHA256,
                       {"data_retrieved": True, "not_pit_certified": True},
                       ["date"], keys)


if __name__ == "__main__":
    unittest.main()
