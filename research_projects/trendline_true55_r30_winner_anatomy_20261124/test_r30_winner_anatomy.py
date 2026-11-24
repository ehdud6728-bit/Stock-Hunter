"""R30 tests: original R23 feature meaning, as-of-only, no hindsight gates."""
from __future__ import annotations

import copy
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd

import r30_winner_anatomy as r30


def candles(t=75,n=150,code="005930"):
    g=pd.DataFrame({
      "code":[code]*n,
      "date":pd.bdate_range("2025-01-02",periods=n).strftime("%Y-%m-%d"),
      "open":[100.]*n,"high":[110.]*n,"low":[90.]*n,
      "close":[100.]*n,"volume":[1000.]*n,
      "amount":[100000.]*n})
    g.loc[t,["open","high","low","close","volume","amount"]]=[
        120.,135.,105.,125.,5000.,600000.]
    g.loc[t+1:,["open","high","low","close","volume","amount"]]=[
        120.,135.,110.,125.,1600.,200000.]
    return g


def new29_row(g,t=75,selected=True,netR25=33.):
    f=r30.r23.feature_asof_d0(g,t)
    return {
      "code":str(g.code.iloc[t]),"date":str(g.date.iloc[t]),
      "year":2025,
      "R25_D0_features_only":[
        (125.-105.)/(135.-105.),
        float(f["d0_amount_vs_prior20_ratio"]),
        float(f["d0_volume_vs_prior20_ratio"]),
        25.,100*(125./110.-1)],
      "R25_D0_selected_TRAINONLY":selected,
      "R25_frozen_cutoff":0.17839853,
      "R25_frozen_D0_cutoff":.17839853,
      "R25_D0_score_TRAINONLY":.5,
      "expost_original_full60_intraday_high30":True,
      "outcomes":{
        r30.NEW_BASE:{"net_pct":netR25,"exit_day":20,
                       "exit_reason":"CLOSE30_TARGET"},
        r30.NEW_D3:{"net_pct":-1.,"exit_day":4,
                     "exit_reason":"D3_CONFIRMATION_FAILED"}}
    }


def new28_row(g,t=75,selected=True):
    x=new29_row(g,t,selected)
    return {
      "code":str(g.code.iloc[t]),"date":str(g.date.iloc[t]),
      "year":2025,
      "entry_price":float(g.open.iloc[t+1]),
      "frozen_prior55_high":float(g.high.iloc[t-55:t].max()),
      "outcomes":x["outcomes"],
      "expost_LABELS_NEVER_DECISION":{
        "full_D1_to_D60_high30":True,
        "full_D1_to_D60_high_pct":40.}
    }


def small_row(code="A",label=True,amplitude=20.,net=35.,selected=True):
    f={field:amplitude for field in r30.NUMERIC}
    f["factors"]={k:"NO" for k in r30.FACTORS}
    f["factors"]["first_wave_then_pre_d0_pullback"]="YES" if amplitude>=20 else "NO"
    return {
      "code":code,"date":"2026-05-01",
      "features_ASOF_D0_ONLY":f,
      "expost_high30":label,"R25_net_pct":net,
      "R29_selected_frozen_D0":selected,
    }


class R30WinnerAnatomyTests(unittest.TestCase):
    def test_original_R23_original_factor_names_are_reused_unchanged(self):
        self.assertEqual(r30.FACTORS,r30.r23.FACTORS)
        self.assertEqual(len(r30.FACTORS),6)
        self.assertEqual(r30.r23.WAVE_MIN_PCT,12.)
        self.assertEqual(r30.r23.CONTRACTION_RATIO,.8)

    def test_original_R25_D0_feature_names_not_outcome_fields(self):
        self.assertEqual(tuple(r30.r25.r24.FEATURES),
             ("close_location","amount_vs_prev20","volume_vs_prev20",
              "return_prev20_pct","breakout_vs_prior55_high_pct"))
        self.assertTrue(all("EXPOST" not in n for n in r30.NUMERIC))

    def test_old_new_artifact_sha_pins_all_64hex(self):
        for pin in (*r30.OLD23.values(),r30.OLD25,r30.NEW28,*r30.NEW29.values()):
            self.assertEqual(len(pin[1]),64)
            self.assertTrue(all(c in "0123456789abcdef" for c in pin[1]))

    def test_incorrect_source_hash_fails_closed(self):
        with TemporaryDirectory() as d:
            p=Path(d)/"test.json";p.write_text("[]",encoding="utf-8")
            with self.assertRaisesRegex(ValueError,"HASH_DRIFT"):
                r30.read_pin(Path(d),("test.json","f"*64))

    def test_original_149_and_new36_frozen_identity_mandatory(self):
        with self.assertRaisesRegex(ValueError,"149_IDS"):
            r30.normalize_old([],[])
        with self.assertRaisesRegex(ValueError,"36_IDS"):
            r30.normalize_new(pd.DataFrame(),[],[])

    def test_R23_features_from_completed_D0_only(self):
        g=candles()
        f=r30.r23.feature_asof_d0(g,75)
        g.loc[76:140,["high","low","close","volume","amount"]]=[
           950.,15.,900.,900000.,90000000.]
        after=r30.r23.feature_asof_d0(g,75)
        self.assertEqual(f,after)

    def test_d0_winner_vs_other_medians_without_threshold_search(self):
        rows=[small_row("A",True,40.),
              small_row("B",True,25.),
              small_row("C",False,8.,-10.),
              small_row("D",False,12.,-12.)]
        z=r30.numeric_group(rows,"first_wave_amplitude_pct")
        self.assertEqual(z["big30_n"],2)
        self.assertEqual(z["not_big30_n"],2)
        self.assertAlmostEqual(z["median_big30"],32.5)
        self.assertAlmostEqual(z["median_not_big30"],10.)
        self.assertIsNone(z["auc"])  # not enough data: don't claim certainty

    def test_categorical_sparsity_is_explicit(self):
        rows=[small_row("A",True,50.),
              small_row("B",False,5.,-20.)]
        z=r30.categorical_group(rows,"first_wave_then_pre_d0_pullback")
        self.assertEqual(z["YES"]["n"],1)
        self.assertEqual(z["YES"]["expost_intraday_high30_n"],1)
        self.assertEqual(z["NO"]["R25_actual_loss15_n"],1)
        self.assertEqual(z["UNKNOWN"]["n"],0)
        self.assertIsNone(z["UNKNOWN"]["expost_high30_pct"])

    def test_auc_direction_is_not_auto_reversed_to_claim_predictive_power(self):
        rows=[small_row(str(i),i>=5,float(i),35. if i>=5 else -10.)
              for i in range(20)]
        out=r30.auc_ticker_bootstrap(rows,"return_prev20_pct",n=30)
        self.assertAlmostEqual(out["auc"],1.)
        rev=[{**r,"expost_high30":not r["expost_high30"]} for r in rows]
        out2=r30.auc_ticker_bootstrap(rev,"return_prev20_pct",n=30)
        self.assertAlmostEqual(out2["auc"],0.)

    def test_new_R23_feature_reuse_and_R29_outcome_copy(self):
        g=candles()
        original28=new28_row(g)
        original29=new29_row(g)
        # A single synthetic event must fail, never masquerade as 36 source events.
        with self.assertRaisesRegex(ValueError,"36_IDS"):
            r30.normalize_new(g,[original28],[original29])

    def test_real_R28_R29_36_event_outcome_keys_in_complete_synthetic_replay(self):
        """Regression: frozen R28 JSON aliases are NOT R25/R27 policy identifiers."""
        frames=[]; originals=[]; scored=[]
        for i in range(36):
            g=candles(code=f"{i:06d}")
            a=new28_row(g)
            b=new29_row(g)
            a["expost_LABELS_NEVER_DECISION"]["full_D1_to_D60_high30"]=i<20
            b["expost_original_full60_intraday_high30"]=i<20
            b["R25_D0_selected_TRAINONLY"]=i<28
            frames.append(g);originals.append(a);scored.append(b)
        raw=pd.concat(frames,ignore_index=True)
        rows=r30.normalize_new(raw,originals,scored)
        self.assertEqual(len(rows),36)
        self.assertEqual(sum(x["expost_high30"] for x in rows),20)
        self.assertEqual(sum(x["R29_selected_frozen_D0"] for x in rows),28)
        self.assertEqual(rows[0]["R25_net_pct"],33.)
        self.assertEqual(rows[0]["R27_net_pct"],-1.)
        self.assertEqual(r30.NEW_BASE,"BASE_R25_D0LOW_CLOSE30")
        self.assertEqual(r30.NEW_D3,"FIXED_R27_D3_CONFIRM")
        # A stored outcome key accidentally replaced by algorithm ID must
        # not be silently interpreted as the same exit result.
        bad=copy.deepcopy(scored)
        policy_key=r30.r28.R25
        bad[0]["outcomes"][policy_key]=bad[0]["outcomes"].pop(r30.NEW_BASE)
        with self.assertRaisesRegex(ValueError,"STORED_EXIT_ALIAS_DRIFT"):
            r30.normalize_new(raw,originals,bad)

    def test_d3_known_and_strict_d4_forward_first30(self):
        g=candles()
        g.loc[78,"close"]=119. # D3 <= entry
        g.loc[79,"open"]=117.
        g.loc[79,"high"]=165. # D4 reaches +30% above entry120
        x=new28_row(g)
        y=new29_row(g)
        z=r30.d3_false_negative_audit(g,[x],[y])
        self.assertEqual(z["R27_D3_failed_among_R25_survivors"]["n"],1)
        case=z["entire_new_36_D3_case_records"][0]
        self.assertTrue(case["expost_D4_to_D60_high30_vs_original_entry"])
        self.assertEqual(case["expost_first_D4plus_high30_day_from_D0"],4)
        self.assertLess(case["D3_close_vs_entry_pct_ASOF"],0)

    def test_early_pre_D3_high_not_counted_as_D4_future(self):
        g=candles()
        g.loc[76,"high"]=175. # D1 +30 occurred before D3
        g.loc[78,"close"]=119.
        g.loc[79:135,"high"]=145. # after D3, no +30 high
        x=new28_row(g);y=new29_row(g)
        z=r30.d3_false_negative_audit(g,[x],[y])
        self.assertFalse(z["entire_new_36_D3_case_records"][0][
            "expost_D4_to_D60_high30_vs_original_entry"])

    def test_before_d3_next_open_exit_not_in_d3_survivors(self):
        g=candles()
        x=new28_row(g);y=new29_row(g)
        x["outcomes"][r30.NEW_BASE]["exit_day"]=4
        y["outcomes"][r30.NEW_BASE]["exit_day"]=4
        z=r30.d3_false_negative_audit(g,[x],[y])
        self.assertEqual(z["R25_still_held_without_pending_D4_sell"]["n"],0)
        self.assertEqual(z["R27_D3_failed_among_R25_survivors"]["n"],0)

    def test_d3_future_outcomes_do_not_change_d3_asof_price_fact(self):
        g=candles()
        a=new28_row(g);b=new29_row(g)
        before=r30.d3_false_negative_audit(g,[a],[b])
        g.loc[79:135,["high","low","close"]]=[200.,50.,170.]
        after=r30.d3_false_negative_audit(g,[a],[b])
        self.assertEqual(before["entire_new_36_D3_case_records"][0][
                          "D3_close_vs_entry_pct_ASOF"],
                         after["entire_new_36_D3_case_records"][0][
                          "D3_close_vs_entry_pct_ASOF"])

    def test_empty_numeric_feature_and_no_replacement(self):
        rows=[small_row("A",True,3.)]
        z=r30.numeric_group(rows,"return_prev20_pct")
        self.assertEqual(z["n"],1)
        self.assertIsNone(z["auc"])

    def test_no_live_policy_side_effects(self):
        self.assertEqual(r30.NEW_BASE,r30.r29.BASE)
        self.assertEqual(r30.NEW_D3,r30.r29.D3)
        self.assertEqual(r30.FIXED_SEED,20261124)


if __name__=="__main__":
    unittest.main()
