"""R28 independent ticker/new 2026 historical sample, strictly fixed policies."""
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

import r28_2026_validation as r28


def synthetic(code="005930", t=320,n=475):
    dates=pd.bdate_range("2025-01-02",periods=n).strftime("%Y-%m-%d")
    g=pd.DataFrame({"code":[code]*n,"date":dates,
                    "open":[100.]*n,"high":[110.]*n,
                    "low":[90.]*n,"close":[100.]*n,
                    "volume":[1000.]*n,"amount":[1000000.]*n})
    g.loc[t,["open","high","low","close"]]=[119.,150.,100.,145.]
    g.loc[t+1:,["open","high","low","close"]]=[145.,160.,130.,145.]
    return g


class R28DisjointValidationTests(unittest.TestCase):
    def test_30_new_codes_have_no_prior_26_overlap(self):
        self.assertEqual(len(r28.EXPECTED_CODES),30)
        self.assertEqual(len(r28.PREVIOUS_R19),26)
        self.assertFalse(set(r28.EXPECTED_CODES)&set(r28.PREVIOUS_R19))

    def test_real_sha_mandatory_and_missing_source_denied(self):
        with TemporaryDirectory() as temp:
            with self.assertRaises((ValueError,FileNotFoundError)):
                r28.load_pinned_source(Path(temp))

    def test_reuses_actual_r19_prior55_candidate_function(self):
        g=synthetic()
        indexes,prior=r28.r19.prior55_candidates(g)
        self.assertIn(320,indexes)
        self.assertEqual(float(prior.iloc[320]),110.)
        self.assertGreater(float(g.close.iloc[320]),float(prior.iloc[320]))

    def test_r19_dplus1_open_must_exceed_frozen_prior55(self):
        g=synthetic()
        g.loc[321,"open"]=109.
        events,audit=r28.frozen_true55_events(g)
        self.assertEqual(len(events),0)
        self.assertGreaterEqual(audit["invalid_next_open_below_prior55"],1)

    def test_R27_D3_policy_as_is_must_sell_at_next_open(self):
        g=synthetic()
        g.loc[323,"close"]=145.  # at entry, D3 confirmation fails
        g.loc[324,"open"]=112.
        events,audit=r28.frozen_true55_events(g)
        e=next(x for x in events if x["date"]==str(g.date.iloc[320]))
        self.assertEqual(e["outcomes"]["FIXED_R27_D3_CONFIRM"]["exit_day"],4)
        self.assertEqual(e["outcomes"]["FIXED_R27_D3_CONFIRM"]["exit_reason"],
                         "D3_CONFIRMATION_FAILED")
        self.assertEqual(e["outcomes"]["FIXED_R27_D3_CONFIRM"]["net_pct"],
                         round(100*(112/145-1)-.2,6))

    def test_future_high_not_counted_as_actual_30pct_sell(self):
        g=synthetic()
        g.loc[325,"high"]=200.
        events,_=r28.frozen_true55_events(g)
        e=next(x for x in events if x["date"]==str(g.date.iloc[320]))
        self.assertTrue(e["expost_LABELS_NEVER_DECISION"]["full_D1_to_D60_high30"])
        self.assertLess(e["outcomes"]["FIXED_R27_D3_CONFIRM"]["net_pct"],30.)
        self.assertLess(e["outcomes"]["BASE_R25_D0LOW_CLOSE30"]["net_pct"],30.)

    def test_2025_breakout_is_not_a_2026_result(self):
        g=synthetic(t=180)
        events,audit=r28.frozen_true55_events(g)
        self.assertEqual(events,[])
        self.assertGreaterEqual(audit["not_2026_eval_after_embargo"],1)

    def test_signal_after_2026_is_embargoed_by_prior_2025_candidate(self):
        g=synthetic(t=247)
        # t247 ~ 2025-12, next breakout t280 ~ 2026-02 but <60 bars later.
        g.loc[280,["high","close"]]=[220.,219.]
        events,audit=r28.frozen_true55_events(g)
        self.assertEqual(len(events),0)
        self.assertGreaterEqual(audit["not_2026_eval_after_embargo"],1)

    def test_incomplete_D60_is_not_included(self):
        g=synthetic(t=440,n=475)
        events,audit=r28.frozen_true55_events(g)
        self.assertEqual(events,[])
        self.assertGreaterEqual(audit["incomplete_d60"],1)

    def test_original_2026_event_exposure_has_one_unique_id(self):
        g=synthetic()
        events,audit=r28.frozen_true55_events(g)
        self.assertGreaterEqual(len(events),1)
        self.assertEqual(len(events),len({(e["code"],e["date"]) for e in events}))
        self.assertTrue(all(e["date"]>=r28.REVIEW_START for e in events))

    def test_pair_reports_prevented_and_new_big_losses(self):
        e={"code":"A","date":"2026-03-01",
           "outcomes":{
             "BASE_R25_D0LOW_CLOSE30":{"net_pct":-25.},
             "FIXED_R27_D3_CONFIRM":{"net_pct":-5.}},
           "expost_LABELS_NEVER_DECISION":{"full_D1_to_D60_high30":False}}
        x=r28.paired([e])
        self.assertEqual(x["R25_loss15_becomes_R27_above_minus15_n"],1)
        self.assertEqual(x["R25_above_minus15_becomes_R27_loss15_n"],0)
        self.assertAlmostEqual(x["mean_R27_minus_R25_net_percentage_points"],20.)

    def test_cluster_bootstrap_is_reproducible_and_small_sample_warned(self):
        sample=[]
        for i in range(8):
            sample.append({"code":f"{i:06d}",
                           "outcomes":{
                             "BASE_R25_D0LOW_CLOSE30":{"net_pct":-10.},
                             "FIXED_R27_D3_CONFIRM":{"net_pct":-9.+i*.1}}})
        x=r28.ticker_cluster_bootstrap_ci(sample,n=100,seed=11)
        y=r28.ticker_cluster_bootstrap_ci(sample,n=100,seed=11)
        self.assertEqual(x,y)
        self.assertIsNotNone(x["mean_delta_95pct_ci"])
        self.assertIsNone(r28.ticker_cluster_bootstrap_ci(sample[:4])["mean_delta_95pct_ci"])

    def test_zero_event_describe_does_not_invent_positive_returns(self):
        x=r28.describe([],"BASE_R25_D0LOW_CLOSE30")
        self.assertIsNone(x["mean_net_pct"])
        self.assertEqual(x["n"],0)


if __name__=="__main__":
    unittest.main()
