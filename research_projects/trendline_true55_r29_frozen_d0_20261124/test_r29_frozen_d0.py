"""R29 fixed R25 model transfer checks; synthetic, offline, no secrets."""
from __future__ import annotations

import copy
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd

import r29_frozen_d0 as r29


def train84():
    train=[]
    for i in range(84):
        train.append({
            "code":f"{i:06d}","date":"2023-07-01",
            "d0_features_only":[(i%7)/9.,1+(i%11)/12.,
                                .5+(i%13)/16.,(i%9)-4.,(i%5)/3.],
            "labels_EXPOST_NOT_FEATURES":{
                "big30_full60":bool(i%5==0 or i%7==0)}})
    return train


def ohlcv(t=70,n=140):
    dates=pd.bdate_range("2025-11-03",periods=n).strftime("%Y-%m-%d")
    g=pd.DataFrame({"code":["005930"]*n,"date":dates,
           "open":[100.]*n,"high":[110.]*n,"low":[90.]*n,
           "close":[100.]*n,"volume":[1000.]*n,"amount":[1000000.]*n})
    g.loc[t,["open","high","low","close"]]=[119.,130.,100.,125.]
    g.loc[t+1:,["open","high","low","close"]]=[120.,145.,110.,125.]
    return g


def event(g,idx=70):
    return {"code":str(g.code.iloc[idx]),"date":str(g.date.iloc[idx]),
      "year":int(g.date.iloc[idx][:4]),"frozen_prior55_high":110.,
      "outcomes":{
        r29.BASE:{"net_pct":35.,"exit_day":15,"exit_reason":"CLOSE30_TARGET"},
        r29.D3:{"net_pct":-1.,"exit_day":4,"exit_reason":"D3_CONFIRMATION_FAILED"}},
      "expost_LABELS_NEVER_DECISION":{"full_D1_to_D60_high30":True}}


def scored(select=True,big=True,base=-23.,d3=-10.,code="005930"):
    return {"code":code,"date":"2026-03-10","year":2026,
      "R25_D0_score_TRAINONLY":.4 if select else .1,
      "R25_D0_selected_TRAINONLY":select,
      "expost_original_full60_intraday_high30":big,
      "outcomes":{
        r29.BASE:{"net_pct":base,"exit_day":20,"exit_reason":"D0_LOW_CLOSE_BREAK"},
        r29.D3:{"net_pct":d3,"exit_day":4,"exit_reason":"D3_CONFIRMATION_FAILED"}}}


class R29FrozenTransferTests(unittest.TestCase):
    def test_fixed_source_runs_and_sha_pins_in_code(self):
        self.assertEqual(r29.R25_RUN,"45160999953")
        self.assertEqual(r29.R28_RUN,"45229111748")
        self.assertEqual(r29.BLUE_RUN,"45221059025")
        for pairs in (r29.R25_PATHS,r29.R28_PATHS):
            for rel,sha in pairs.values():
                self.assertEqual(len(sha),64)
                self.assertTrue(all(c in "0123456789abcdef" for c in sha))

    def test_tampered_json_does_not_parse_as_valid_source(self):
        with TemporaryDirectory() as directory:
            p=Path(directory)
            (p/"a.json").write_text('{"event":"tampered"}')
            with self.assertRaisesRegex(ValueError,"SHA_MISMATCH"):
                r29.read_locked_json(p,("a.json","0"*64))

    def test_missing_source_fail_closed(self):
        with TemporaryDirectory() as directory:
            with self.assertRaises(FileNotFoundError):
                r29.train_and_replay_original(Path(directory))
            with self.assertRaises(FileNotFoundError):
                r29.load_and_replay_R28(Path(directory),Path(directory))

    def test_original_R25_train_model_has_fixed_25th_quantile(self):
        train=train84()
        clf,cut=r29.r25.train_opportunity_model(train)
        x=np.array([r["d0_features_only"] for r in train],dtype=float)
        self.assertAlmostEqual(
            cut,float(np.quantile(clf.predict_proba(x)[:,1],.25)),12)

    def test_new_data_future_label_cannot_change_predictions(self):
        train=train84()
        clf,cut=r29.r25.train_opportunity_model(train)
        g=ohlcv()
        first=r29.predict_asof_D0(clf,cut,[event(g)],g)
        mutated=copy.deepcopy(event(g))
        mutated["expost_LABELS_NEVER_DECISION"]["full_D1_to_D60_high30"]=False
        mutated["outcomes"][r29.D3]["net_pct"]=-99.
        again=r29.predict_asof_D0(clf,cut,[mutated],g)
        self.assertEqual(first[0]["R25_D0_score_TRAINONLY"],
                         again[0]["R25_D0_score_TRAINONLY"])
        self.assertEqual(first[0]["R25_D0_selected_TRAINONLY"],
                         again[0]["R25_D0_selected_TRAINONLY"])

    def test_prices_strictly_after_D0_cannot_affect_score(self):
        train=train84()
        clf,cut=r29.r25.train_opportunity_model(train)
        g=ohlcv()
        base=r29.predict_asof_D0(clf,cut,[event(g)],g)
        g.loc[71:135,["open","high","low","close","volume","amount"]]=[
            900.,1000.,800.,950.,9_000_000.,9_000_000_000.]
        later=r29.predict_asof_D0(clf,cut,[event(g)],g)
        self.assertEqual(base[0]["R25_D0_features_only"],
                         later[0]["R25_D0_features_only"])
        self.assertEqual(base[0]["R25_D0_score_TRAINONLY"],
                         later[0]["R25_D0_score_TRAINONLY"])

    def test_original_five_features_exact_order(self):
        self.assertEqual(tuple(r29.r25.r24.FEATURES),
             ("close_location","amount_vs_prev20","volume_vs_prev20",
              "return_prev20_pct","breakout_vs_prior55_high_pct"))
        clf,cut=r29.r25.train_opportunity_model(train84())
        g=ohlcv()
        out=r29.predict_asof_D0(clf,cut,[event(g)],g)[0]
        vector=out["R25_D0_features_only"]
        self.assertAlmostEqual(vector[0],(125.-100.)/(130.-100.),12)
        self.assertAlmostEqual(vector[1],1.,12)
        self.assertAlmostEqual(vector[2],1.,12)
        self.assertAlmostEqual(vector[3],25.,12)
        self.assertAlmostEqual(vector[4],100*(125./110.-1),12)

    def test_prior55_frozen_level_changed_fails_closed(self):
        clf,cut=r29.r25.train_opportunity_model(train84())
        g=ohlcv()
        e=event(g);e["frozen_prior55_high"]=180.
        with self.assertRaisesRegex(ValueError,"PRIOR55_FEATURE"):
            r29.predict_asof_D0(clf,cut,[e],g)

    def test_unknown_signal_day_fail_closed(self):
        clf,cut=r29.r25.train_opportunity_model(train84())
        g=ohlcv()
        e=event(g);e["date"]="2030-01-01"
        with self.assertRaisesRegex(ValueError,"MISSING_PRICE_AT_D0"):
            r29.predict_asof_D0(clf,cut,[e],g)

    def test_selection_recall_and_missed_winner_explicit(self):
        rows=[
          scored(True,True,base=40.,d3=4.,code="A"),
          scored(False,True,base=35.,d3=-5.,code="B"),
          scored(False,False,base=-20.,d3=-10.,code="C"),
          scored(True,False,base=-12.,d3=-10.,code="D")]
        x=r29.selection_metrics(rows)
        self.assertEqual(x["selected_n"],2)
        self.assertEqual(x["rejected_n"],2)
        self.assertEqual(x["original_full60_intraday_big30_EXPOST_n"],2)
        self.assertEqual(x["selected_original_big30_EXPOST_n"],1)
        self.assertEqual(x["rejected_big30_opportunities_EXPOST_n"],1)
        self.assertEqual(x["big30_recall_pct_EXPOST"],50.)
        self.assertEqual(x["missed_R25_actual_realized_net30_due_to_D0_rejection_EXPOST"],1)
        self.assertEqual(x["rejected_R25_actual_loss_le15_n_EXPOST"],1)

    def test_original_R25_vs_R27_exit_pair_is_reused_unchanged(self):
        rows=[scored(True,True,base=35.,d3=-1.),
              scored(True,False,base=-20.,d3=-5.,code="B")]
        r=r29.compact(rows)
        self.assertEqual(r["n"],2)
        self.assertEqual(r["by_method"][r29.BASE]["realized_net30_n"],1)
        self.assertEqual(r["by_method"][r29.D3]["realized_net30_n"],0)
        self.assertEqual(r["paired_R27_minus_R25"]
                         ["R25_loss15_becomes_R27_above_minus15_n"],1)
        self.assertEqual(r["paired_R27_minus_R25"]
                         ["R25_realized30_lost_by_R27_n"],1)

    def test_new_r29_does_not_retrain_with_2026(self):
        g=ohlcv()
        a=event(g)
        m,cut=r29.r25.train_opportunity_model(train84())
        out=r29.predict_asof_D0(m,cut,[a],g)
        self.assertEqual(out[0]["R25_frozen_cutoff"],round(cut,8))
        self.assertIn(out[0]["R25_D0_selected_TRAINONLY"],(True,False))

    def test_empty_sample_does_not_generate_fake_edge(self):
        self.assertIsNone(r29.score_metrics([])["AUC"])
        self.assertEqual(r29.selection_metrics([])["total_n"],0)
        self.assertEqual(r29.compact([])["n"],0)

    def test_score_metric_one_class_never_invents_auc(self):
        r=[scored(True,True,base=3.,d3=4.)]
        self.assertIsNone(r29.score_metrics(r)["AUC_EXPOST"])

    def test_R25_selection_not_based_on_prior_R28_D3_return(self):
        g=ohlcv()
        clf,cut=r29.r25.train_opportunity_model(train84())
        e1=event(g)
        e2=copy.deepcopy(e1)
        e1["outcomes"][r29.D3]["net_pct"]=-100.
        e2["outcomes"][r29.D3]["net_pct"]=100.
        a=r29.predict_asof_D0(clf,cut,[e1],g)[0]
        b=r29.predict_asof_D0(clf,cut,[e2],g)[0]
        self.assertEqual(a["R25_D0_score_TRAINONLY"],b["R25_D0_score_TRAINONLY"])
        self.assertEqual(a["R25_D0_selected_TRAINONLY"],b["R25_D0_selected_TRAINONLY"])


if __name__=="__main__":
    unittest.main()
