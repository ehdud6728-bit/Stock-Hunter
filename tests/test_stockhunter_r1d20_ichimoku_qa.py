import copy
import json
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd
from research_formulas import stockhunter_r1d20_ichimoku_qa as m


class IchimokuAsOfSafety(unittest.TestCase):
    def bars(self, n=224, delta=0):
        out=[]
        for i in range(n):
            close=100+(i/10)+delta
            out.append((i,close,close+3,close-3,close,1000,100000))
        return out

    def test_protocol_lock(self):
        p=m.load_protocol('R1D20_PREDECLARED_PROTOCOL.json')
        self.assertTrue(p['guards']['no_live_search_score_rank_orders_change'])
        self.assertTrue(p['ichimoku_contract']['chikou_backprojection_not_read'])

    def test_224_session_required(self):
        self.assertEqual(m.make_asof(self.bars(223),222)['asof_status'],'BLOCK_NO_224_BARS')

    def test_session_gap_rejected(self):
        b=self.bars();b[100]=(101,*b[100][1:]);self.assertEqual(m.make_asof(b,223)['asof_status'],'BLOCK_NONCONTIGUOUS_224')

    def test_nonfinite_close_blocked(self):
        b=self.bars();b[130]=(130,1,2,1,float('nan'),100,1000)
        self.assertEqual(m.make_asof(b,223)['asof_status'],'BLOCK_NONFINITE_OHLCVA')

    def test_inconsistent_ohlc_blocked(self):
        b=self.bars();j=170;b[j]=(j,100,101,99,102,1000,100000)
        self.assertEqual(m.make_asof(b,223)['asof_status'],'BLOCK_INCONSISTENT_OHLC')

    def test_raw_corporate_action_gap_quarantine(self):
        b=self.bars();j=200;b[j]=(j,300,303,297,300,1000,100000)
        self.assertEqual(m.make_asof(b,223)['asof_status'],'BLOCK_UNADJUSTED_CORPORATE_ACTION_GAP_PROXY')

    def test_today_cloud_is_lagged_by_26_sessions(self):
        b=self.bars();x=m.make_asof(b,223);self.assertEqual(x['asof_status'],'ASOF_OK')
        h=pd.Series([u[2] for u in b]);l=pd.Series([u[3] for u in b]);ten=(h.rolling(9).max()+l.rolling(9).min())/2
        kij=(h.rolling(26).max()+l.rolling(26).min())/2
        a=(ten+kij)/2;span_b=(h.rolling(52).max()+l.rolling(52).min())/2
        self.assertAlmostEqual(x['cloud_a_displayed_today'],a.iloc[-27])
        self.assertAlmostEqual(x['cloud_b_displayed_today'],span_b.iloc[-27])
        self.assertAlmostEqual(x['cloud_a_projected_t_plus26_known_today'],a.iloc[-1])
        self.assertNotEqual(x['cloud_a_projected_t_plus26_known_today'],x['cloud_a_displayed_today'])

    def test_future_candle_cannot_change_asof_signal(self):
        b=self.bars()
        x=m.make_asof(b,223)
        future=(224,100000,100005,99995,100000,1000,100000)
        self.assertEqual(x,m.make_asof((b+[future])[:-1],223))
        # Valid call is explicitly capped to signal date; invalid future-inclusive call rejects session mismatches.
        self.assertEqual(m.make_asof(b+[future],223)['asof_status'],'BLOCK_NONCONTIGUOUS_224')

    def test_last_close_cannot_retroactively_change_displayed_cloud(self):
        b=self.bars();x=m.make_asof(b,223)
        # Change ONLY current candle OHLC: displayed cloud calculated at t-26 does not change.
        v=copy.deepcopy(b);j=223;v[j]=(j,100,103,97,100,1000,100000)
        y=m.make_asof(v,223)
        self.assertEqual(y['asof_status'],'ASOF_OK')
        self.assertEqual(x['cloud_a_displayed_today'],y['cloud_a_displayed_today'])
        self.assertEqual(x['cloud_b_displayed_today'],y['cloud_b_displayed_today'])

    def test_exact_ichimoku_class_mutually_exclusive(self):
        a=m.make_asof(self.bars(),223)
        self.assertEqual(a['cloud_above']+a['cloud_inside']+a['cloud_below'],1)

    def test_bb_formula_ddof_zero(self):
        b=self.bars();r=m.make_asof(b,223);c=pd.Series([v[4] for v in b]);lower=c.rolling(30).mean()-2*c.rolling(30).std(ddof=0)
        self.assertAlmostEqual(r['bb30_lower_today'],lower.iloc[-1])

    def test_outcomes_not_in_features(self):
        x=m.make_asof(self.bars(),223)
        for key in m.OUTCOME_FORBIDDEN:self.assertNotIn(key,x)

    def test_no_asof_feature_leak_in_hypothesis_masks(self):
        b=m.make_asof(self.bars(),223)
        b.update({'signal_5MA_reclaim_today':0,'signal_20MA_reclaim_today':0,'amount3d_above_prior10':0})
        x=pd.DataFrame([b]);f=m.hypothesis_masks(x)
        self.assertEqual(set(f),set(m.load_protocol('R1D20_PREDECLARED_PROTOCOL.json')['hypotheses_locked_before_R1D20_period_labels']))

    def test_duplicate_hypothesis_rejected(self):
        p=json.loads(Path('R1D20_PREDECLARED_PROTOCOL.json').read_text())
        p['hypotheses_locked_before_R1D20_period_labels']+=p['hypotheses_locked_before_R1D20_period_labels'][:1]
        with tempfile.TemporaryDirectory() as t:
            f=Path(t)/'bad.json';f.write_text(json.dumps(p))
            with self.assertRaisesRegex(RuntimeError,'DUPLICATE_HYPOTHESIS'):m.load_protocol(f)

    def test_protocol_lock_offline_and_no_return_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            m.run(type('Args',(),{'protocol':'R1D20_PREDECLARED_PROTOCOL.json','mode':'lock','out':tmp}))
            out=json.loads((Path(tmp)/'R1D20_LOCK_MANIFEST.json').read_text())
            self.assertFalse(out['outcome_labels_read']);self.assertEqual(out['orders_placed'],0)

if __name__=='__main__':unittest.main()
