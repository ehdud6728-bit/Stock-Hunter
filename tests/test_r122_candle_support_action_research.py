import unittest
import pandas as pd
import numpy as np
from closebet_research.r122_candle_support_action_research import featurize,simulate,setup_tags,ENTRIES,STOPS


def bars_stable(price=100,n=230):
    x=[]
    for s in range(n):
        close=price+0.01*s
        x.append({'sess':s,'open':close-1,'close':close,'low':close-2,'high':close+2,'volume':100+s%4,'amount':100000})
    return pd.DataFrame(x)

class CandleSupportTests(unittest.TestCase):
    def test_gaps_fail_closed(self):
        g=bars_stable();g=g[g.sess!=45].reset_index(drop=True);k={int(s):i for i,s in enumerate(g.sess)}
        self.assertFalse(featurize(g,229,k)['feature_valid'])
    def test_only_d0_earlier_no_future(self):
        g=bars_stable(100,255);k={int(s):i for i,s in enumerate(g.sess)}
        a=featurize(g,229,k)
        g.loc[g.sess>229,['open','high','low','close','volume']]=100000
        b=featurize(g,229,k)
        self.assertEqual(a,b)
    def test_prev_low_reclaim(self):
        g=bars_stable(100,251)
        g.loc[g.sess==249,['open','high','low','close']]=[100,104,100,103]
        g.loc[g.sess==250,['open','high','low','close']]=[101,105,99,104] # not included t=250? yes
        k={int(s):i for i,s in enumerate(g.sess)}
        a=featurize(g,250,k)
        self.assertEqual(a['prev_low_relation'],'RECLAIM_PREV_LOW')
        self.assertTrue(a['feature_valid'])
    def test_body_candle_class(self):
        g=bars_stable(100,235);k={int(s):i for i,s in enumerate(g.sess)}
        a=featurize(g,234,k)
        self.assertIn(a['candle_type'],['BULL_STRONG','BULL_SMALL','BEAR_STRONG','BEAR_SMALL','DOJI'])
    def test_candle_below_prev_low(self):
        a={'feature_valid':True,'prev_low_relation':'CLOSE_BELOW_PREV_LOW','lower_wick_fraction':.5,'ma_band':'ABOVE_20_60','candle_type':'BULL_STRONG','d0_volume_prev20_ratio':1.0}
        self.assertEqual(setup_tags(a),'BROKEN_PREVIOUS_LOW')
    def test_confirm_requires_next_open(self):
        ctx={'support_level':95,'d0_high':105,'d0_close':100,'prev_low':96,'candle_type':'BULL_SMALL','d0_volume_prev20_ratio':.9}
        bars=[(100,102,94,101,90),(103,108,100,104,100)]+[(104,104,102,103,90)]*18
        a=simulate(bars,ctx,'E1_SUPPORT_RECLAIM_NEXT_OPEN','S0_FIXED5')
        self.assertEqual(a['entry_day'],2)
    def test_baseline_no_future_leak(self):
        ctx={'support_level':95,'d0_high':105,'d0_close':100,'prev_low':96,'candle_type':'BULL_SMALL','d0_volume_prev20_ratio':1.0}
        bars=[(100,106,98,103,100)]+[(100,101,98,100,100)]*19
        a=simulate(bars,ctx,'E0_D1_FULL','S0_FIXED5')
        self.assertEqual(a['exit_reason'],'TARGET_INTRADAY');self.assertEqual(a['exit_day'],1)
    def test_same_day_stop_first(self):
        ctx={'support_level':95,'d0_high':105,'d0_close':100,'prev_low':96,'candle_type':'BULL_SMALL','d0_volume_prev20_ratio':1.0}
        bars=[(100,110,94,100,100)]+[(100,101,99,100,100)]*19
        a=simulate(bars,ctx,'E0_D1_FULL','S0_FIXED5')
        self.assertEqual(a['exit_reason'],'STOP_SAME_BAR_CONSERVATIVE')
        self.assertTrue(a['both_on_same_day'])
    def test_preknown_struct_stop_too_wide_no_entry(self):
        ctx={'support_level':75,'d0_high':105,'d0_close':100,'prev_low':96,'candle_type':'BULL_SMALL','d0_volume_prev20_ratio':.9}
        bars=[(100,101,98,100,100)]*20
        a=simulate(bars,ctx,'E0_D1_FULL','S1_STATIC_SUPPORT_CAPPED8')
        self.assertEqual(a['status'],'NO_ENTRY_INVALID_STRUCTURAL_RISK')
    def test_close_stop_next_open_gap(self):
        ctx={'support_level':95,'d0_high':105,'d0_close':100,'prev_low':96,'candle_type':'BULL_SMALL','d0_volume_prev20_ratio':1.0}
        bars=[(100,101,90,94,100),(88,91,85,90,100)]+[(90,91,85,90,100)]*18
        a=simulate(bars,ctx,'E0_D1_FULL','S2_SUPPORT_CLOSE_NEXT_OPEN')
        self.assertEqual(a['exit_day'],2)
        self.assertAlmostEqual(a['budget_net_pct'],-12.5)
    def test_confirm_no_signal_not_failure(self):
        ctx={'support_level':95,'d0_high':105,'d0_close':100,'prev_low':96,'candle_type':'BULL_SMALL','d0_volume_prev20_ratio':1.0}
        bars=[(100,102,98,100,100)]*20
        a=simulate(bars,ctx,'E2_BREAK_D0HIGH_NEXT_OPEN','S0_FIXED5')
        self.assertEqual(a['status'],'NO_SIGNAL_NO_ENTRY');self.assertEqual(a['budget_net_pct'],0)
    def test_staged_break_guard_no_entry(self):
        ctx={'support_level':95,'d0_high':105,'d0_close':94,'prev_low':96,'candle_type':'BEAR_STRONG','d0_volume_prev20_ratio':1.8}
        bars=[(100,102,98,100,100)]*20
        a=simulate(bars,ctx,'E3_CONTEXT_GATED_40_30_30','S0_FIXED5')
        self.assertEqual(a['status'],'NO_ENTRY_CONTEXT_GUARD')
    def test_policy_grid_fixed(self):
        self.assertEqual(len(ENTRIES)*len(STOPS),12)

if __name__=='__main__':unittest.main()

class ConfirmationGuardTests(unittest.TestCase):
    def test_no_staged_add_after_support_close_failure(self):
        ctx={'support_level':95,'d0_high':101,'d0_close':100,'prev_low':95,
             'candle_type':'BULL_SMALL','d0_volume_prev20_ratio':.8}
        bars=[(97,98,93.5,94,100),(96,99,95,98,100),(98,103,97,102,100)]+[(102,103,99,102,100)]*17
        a=simulate(bars,ctx,'E3_CONTEXT_GATED_40_30_30','S0_FIXED5')
        self.assertEqual(a['n_fills'],1)
        self.assertAlmostEqual(a['invested_fraction'],.4)

class OpeningGapBeforeAddTests(unittest.TestCase):
    def test_stop_gap_precedes_queued_add(self):
        ctx={'support_level':95,'d0_high':105,'d0_close':100,'prev_low':99,
             'candle_type':'BULL_SMALL','d0_volume_prev20_ratio':.8}
        bars=[(100,102,98,101,100),(90,94,89,92,100)]+[(92,94,90,92,100)]*18
        a=simulate(bars,ctx,'E3_CONTEXT_GATED_40_30_30','S0_FIXED5')
        self.assertEqual(a['exit_reason'],'STOP_GAP')
        self.assertEqual(a['n_fills'],1)
        self.assertAlmostEqual(a['invested_fraction'],.4)
    def test_target_gap_precedes_queued_add(self):
        ctx={'support_level':95,'d0_high':105,'d0_close':100,'prev_low':99,
             'candle_type':'BULL_SMALL','d0_volume_prev20_ratio':.8}
        bars=[(100,102,98,101,100),(110,111,109,110,100)]+[(110,111,109,110,100)]*18
        a=simulate(bars,ctx,'E3_CONTEXT_GATED_40_30_30','S0_FIXED5')
        self.assertEqual(a['exit_reason'],'TARGET_GAP_LIMIT')
        self.assertEqual(a['n_fills'],1)
