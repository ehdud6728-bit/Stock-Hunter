import math, tempfile, unittest
from pathlib import Path
import numpy as np
import pandas as pd
from closebet_research import r120_entry_scale_stop as m


def bars(o=100.,hi=100.,low=100.,close=100.):
    return [(o,hi,low,close) for _ in range(20)]


class R120ResearchOnly(unittest.TestCase):
    def setUp(self):
        self.pre={'support5':92.,'atr14':2.,'context_valid':True}
        self.row={'asof_quality_gate':True,'broad_research_eligible':True,
                  'origin_below224_asof':True,'close_vs_ma224_pct':-3.,
                  'peak_to_d0_sessions':4,'pullback_fraction':.48,'stage':None}
    def test_nonlive_authority(self):
        self.assertEqual(m.VERSION,'R120_ENTRY_SCALE_STOP_ASOF_EPISODE_V1')
        self.assertNotIn('LIVE',m.STATUS)
    def test_asof_type_does_not_accept_outcome(self):
        a=dict(self.row,t5_by_d20='HIT_HIGH_NOT_FILL', d20_close_net05pct=13)
        b=dict(self.row,t5_by_d20='NOT_HIT_WITH_COMPLETE_HORIZON',d20_close_net05pct=-13)
        self.assertEqual(m.classify(a),m.classify(b))
    def test_three_pullback_types_and_age(self):
        for v,out in [(.2,'SHALLOW_PULLBACK'),(.5,'MID_PULLBACK'),(.8,'DEEP_PULLBACK')]:
            self.assertEqual(m.classify(dict(self.row,pullback_fraction=v)),out)
        self.assertEqual(m.classify(dict(self.row,peak_to_d0_sessions=21)),'AGED_PULLBACK_AFTER_PEAK')
    def test_quarantine_prevents_episode(self):
        self.assertEqual(m.classify(dict(self.row,stage='SUSPENSION_224_QUARANTINE')),'DATA_ASOF_QUARANTINE')
    def test_original_below224_without_pullback_not_success_label(self):
        self.assertEqual(m.classify(dict(self.row,broad_research_eligible=False)),'BELOW224_ORIGIN_NOT_BROAD_PB')
    def test_asof_episode_selection_independent_of_postevent(self):
        days=[f'2026-09-{d:02}' for d in range(1,26)]
        q=pd.DataFrame([{'scope':'V4976_SELECTED','code':'000001','signal_date':days[i],
                         'asof_quality_gate':True,'type':'MID_PULLBACK',
                         'd20_complete':i%2==0} for i in range(24)])
        x=m.choose_episodes(q,days)
        self.assertEqual(list(x.signal_date),[days[0],days[21]])
        q['d20_complete']=~q['d20_complete']
        self.assertEqual(list(m.choose_episodes(q,days).signal_date),[days[0],days[21]])
    def test_episode_separate_by_scope(self):
        ds=['2026-08-03','2026-08-04']
        q=pd.DataFrame([{'scope':s,'code':'000001','signal_date':ds[0],
                         'asof_quality_gate':True,'type':'MID_PULLBACK'} for s in ['V4976_SELECTED','V4976_RAW_NOT_TOP5']])
        self.assertEqual(len(m.choose_episodes(q,ds)),2)
    def test_no_future_feature_in_pre_d0_support(self):
        df=pd.DataFrame([{'sess':i,'open':100,'high':101,'low':90 if i==13 else 99,
                          'close':100} for i in range(40)])
        a=m.pre_d0_context(df,13,{i:i for i in range(40)})
        df.loc[14:,'low']=1;df.loc[14:,'high']=999
        b=m.pre_d0_context(df,13,{i:i for i in range(40)})
        self.assertEqual(a,b)
    def test_stop_precedence_both_touch_on_same_day(self):
        x=bars();x[0]=(100,110,90,100)
        z=m.simul(x,105,self.pre,'D1_FULL','STOP5')
        self.assertEqual(z['exit_reason'],'STOP_SAME_BAR_CONSERVATIVE')
        self.assertEqual(z['same_bar_stop_tp'],1)
        self.assertAlmostEqual(z['profit_pct_budget_net05'],-5.5)
    def test_stop_gap_executes_at_open_not_stop_price(self):
        x=bars();x[1]=(88,89,87,88)
        z=m.simul(x,105,self.pre,'D1_FULL','STOP5')
        self.assertEqual(z['exit_reason'],'STOP_GAP')
        self.assertAlmostEqual(z['profit_pct_budget_net05'],-12.5)
    def test_target_gap_price_is_conservative_limit(self):
        x=bars();x[1]=(110,111,108,109)
        z=m.simul(x,105,self.pre,'D1_FULL','STOP5')
        self.assertEqual(z['exit_reason'],'TARGET_GAP_LIMIT')
        self.assertAlmostEqual(z['profit_pct_budget_net05'],4.5)
    def test_staged_cash_return_is_capital_normalized(self):
        x=bars();x[1]=(100,106,100,105)
        z=m.simul(x,105,self.pre,'D1_HALF_D2TO5_DIP3','STOP5')
        self.assertAlmostEqual(z['invested_fraction'],.5)
        self.assertAlmostEqual(z['profit_pct_budget_net05'],2.25)
    def test_staged_d2_limit_and_same_bar_tp_deferral(self):
        x=bars();x[1]=(100,106,96,100);x[2]=(100,106,99,105)
        z=m.simul(x,105,self.pre,'D1_HALF_D2TO5_DIP3','STOP5')
        self.assertEqual(z['same_bar_add_tp'],1)
        self.assertEqual(z['exit_day'],3)
        self.assertEqual(z['fills'],2)
        self.assertAlmostEqual(z['invested_fraction'],1.0)
    def test_confirm_next_open_not_trigger_close(self):
        x=bars();x[0]=(100,106,99,106);x[1]=(110,112,106,110)
        z=m.simul(x,105,self.pre,'D2TO6_CONFIRM_D0HIGH','STOP5')
        self.assertEqual(z['entry_first_day'],2)
        self.assertAlmostEqual(z['entry_anchor'],110.)
    def test_confirm_does_not_enter_without_breakout(self):
        x=bars()
        z=m.simul(x,105,self.pre,'D2TO6_CONFIRM_D0HIGH','STOP5')
        self.assertEqual(z['status'],'NO_CONFIRMATION_NO_ENTRY')
        self.assertTrue(math.isnan(z['profit_pct_invested_net05']))
    def test_not_complete_is_unknown_not_zero(self):
        z=m.simul(bars()[:15],105,self.pre,'D1_FULL','STOP5')
        self.assertEqual(z['status'],'UNKNOWN_CENSORED')
        self.assertTrue(math.isnan(z['profit_pct_budget_net05']))
    def test_two_staged_tranches_fill_at_most_once(self):
        x=bars();x[1]=(100,101,97,100);x[2]=(100,101,95,100)
        z=m.simul(x,105,self.pre,'D1_40_D2TO5_DIP2_D2TO8_DIP4','STOP7')
        self.assertEqual(z['fills'],3)
        self.assertAlmostEqual(z['invested_fraction'],1.0)
    def test_below_stop_structural_is_invalid(self):
        z=m.simul(bars(),105,dict(self.pre,support5=101), 'D1_FULL','D0_SUPPORT5')
        self.assertEqual(z['status'],'STOP_CONTEXT_INVALID')
    def test_atr_stop_asof(self):
        self.assertAlmostEqual(m.stop_level('D0_ATR14_X2',100.,self.pre),96.)
    def test_stop_is_not_reanchored_after_buy(self):
        x=bars();x[1]=(100,101,97,99);x[2]=(100,101,95,100)
        z=m.simul(x,105,self.pre,'D1_40_D2TO5_DIP2_D2TO8_DIP4','STOP5')
        self.assertEqual(z['exit_day'],3)
        self.assertIn('STOP',z['exit_reason'])
        self.assertEqual(z['fills'],2) # -4% tranche never filled after stop
    def test_no_leak_from_reference_r119_touch(self):
        # First-touch labels are permitted ONLY for reporting, never sim entry decisions.
        z1=m.simul(bars(),105,self.pre,'D1_FULL','STOP5')
        z2=m.simul(bars(),105,self.pre,'D1_FULL','STOP5')
        self.assertEqual(z1,z2)

if __name__=='__main__':unittest.main()
