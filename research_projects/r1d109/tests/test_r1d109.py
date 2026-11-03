"""Independent R1-D109 regression tests. Does not require upstream run artifacts."""
import sys,tempfile,unittest
from pathlib import Path
import numpy as np
import pandas as pd

HERE=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(HERE))
from turning_points import confirmed_pivots,line_transition_events,role_retest_events,slope_sign
from research_r1d109 import run,FIELDS


def toy_prices(highs=None,lows=None):
    hi=highs or [11,12,17,12,11,12,16,12,11,12,18,12]
    lo=lows or [9,8,7,8,9,8,7.5,8,9,8,7.2,8]
    n=len(hi);out=[]
    for i in range(n):
        close=(hi[i]+lo[i])/2
        out.append({'code':'000001','name':'테스트','idx':541+i,
         'date':str((pd.Timestamp('2024-01-02')+pd.Timedelta(days=i)).date()),
         'open':close,'high':hi[i],'low':lo[i],'close':close,'amount':1000+i,
         'volume':100+i,'candle_body_share_pct':20.,'candle_upper_wick_share_pct':40.,
         'candle_lower_wick_share_pct':40.,'close_vs_prev3_high_max_pct':-1.0,
         'ma20_slope20_pct':-1.,'ma20_slope_accel5_pp':0.0,
         'bb20_width_pct':8.,'bb20_width_delta5_pp':-1.,
         'env20_10_position':'INSIDE','ma_cloud_fast_thickness_pct':2.,
         'ichimoku_current_thickness_pct':3.,'amount_vs_prev20':.8})
    return pd.DataFrame(out)


def state_rows(d):
    rows=[]
    for q in d.to_dict('records'):
        for lane in ('UPPER_SHORT','UPPER_MEDIUM','LOWER_SHORT','LOWER_MEDIUM'):
            rows.append({'code':q['code'],'name':q['name'],'idx':q['idx'],'date':q['date'],
             'lane':lane,'state':'UNCHANGED','active_line_id':'','active_family':'',
             'active_line_slope_pct':np.nan,'retired_line_id':'',
             'active_line_level':np.nan})
    return pd.DataFrame(rows)


class PivotTests(unittest.TestCase):
    def test_right_two_confirmation(self):
        p,_=confirmed_pivots(toy_prices())
        hit=p[(p.pivot_type=='HIGH')&(p.pivot_idx==543)]
        self.assertEqual(len(hit),1)
        self.assertEqual(int(hit.iloc[0].confirmed_idx),545)
    def test_first_pivot_not_trend_reversal(self):
        p,s=confirmed_pivots(toy_prices().iloc[:5])
        self.assertTrue((s.pivot_structure_PIT=='INSUFFICIENT_PIVOTS').all())
    def test_future_tamper_no_past_effect(self):
        a=toy_prices();pivot,s=confirmed_pivots(a)
        b=a.copy();b[['high','low','open','close']]=b[['high','low','open','close']].astype(float);b.loc[b.idx>547,['high','low','open','close']]*=1.15
        pivot2,s2=confirmed_pivots(b)
        self.assertEqual(s[s.idx<=547][['idx','pivot_structure_PIT']].to_dict('records'),
                         s2[s2.idx<=547][['idx','pivot_structure_PIT']].to_dict('records'))
        self.assertEqual(pivot[pivot.confirmed_idx<=547][['pivot_idx','pivot_relation']].to_dict('records'),
                         pivot2[pivot2.confirmed_idx<=547][['pivot_idx','pivot_relation']].to_dict('records'))
    def test_near_equal_tolerance(self):
        a=toy_prices(highs=[11,12,17,12,11,12,17.02,12,11,12,18,12]);p,s=confirmed_pivots(a)
        hit=p[(p.pivot_type=='HIGH')&(p.pivot_idx==547)]
        self.assertEqual(hit.iloc[0].pivot_relation,'EQUAL')
    def test_missing_date_order_rejected(self):
        a=toy_prices();a.loc[3,'idx']+=10
        with self.assertRaises(ValueError):confirmed_pivots(a)
    def test_unknown_future_pivot_not_shown_as_confirmed(self):
        a=toy_prices().iloc[:4]
        p,s=confirmed_pivots(a)
        self.assertTrue(p.empty)
        self.assertEqual(s.iloc[-1].pivot_structure_PIT,'INSUFFICIENT_PIVOTS')
    def test_sign_helper(self):
        self.assertEqual(slope_sign(-.1),'NEGATIVE')
        self.assertEqual(slope_sign(.1),'POSITIVE')
        self.assertEqual(slope_sign(np.nan),'NA')


class LineChangeTests(unittest.TestCase):
    def setUp(self):
        self.d=toy_prices().iloc[:4];self.s=state_rows(self.d)
    def test_reanchor_sign_flip_not_claimed_reversal(self):
        mask=self.s.lane.eq('UPPER_SHORT')
        a=self.s[mask].index
        self.s.loc[a,'active_line_id']=['old','old','new','new']
        self.s.loc[a,'active_family']='WICK_HIGH_PIVOT2'
        self.s.loc[a,'active_line_slope_pct']=[-.5,-.5,.25,.25]
        self.s.loc[a[2],'state']='REANCHORED_NEW_PIVOT_OR_CANDLE'
        x=line_transition_events(self.s)
        y=x[x.line_state.eq('REANCHORED_NEW_PIVOT_OR_CANDLE')].iloc[0]
        self.assertTrue(y.slope_sign_flip)
        self.assertTrue(y.same_family_reanchor)
        self.assertFalse(y.can_claim_price_trend_reversal)
        self.assertAlmostEqual(y.slope_delta_pp_per_bar,.75)
    def test_new_line_after_break_has_no_slope_comparison(self):
        a=self.s[self.s.lane.eq('UPPER_SHORT')].index
        self.s.loc[a,'active_line_id']=['old','','new','new']
        self.s.loc[a,'active_line_slope_pct']=[-.2,np.nan,.4,.4]
        self.s.loc[a[1],'state']='RESISTANCE_BROKEN_UP'
        self.s.loc[a[2],'state']='NEW_LINE_AFTER_BREAK'
        x=line_transition_events(self.s)
        z=x[x.line_state.eq('NEW_LINE_AFTER_BREAK')].iloc[0]
        self.assertFalse(z.slope_sign_flip)
        self.assertTrue(pd.isna(z.old_slope_pct_per_bar))
    def test_cross_family_flag(self):
        a=self.s[self.s.lane.eq('LOWER_MEDIUM')].index
        self.s.loc[a,'active_line_id']=['o','o','n','n']
        self.s.loc[a,'active_family']=['BULL_BODY_BOTTOM','BULL_BODY_BOTTOM','WICK_LOW_PIVOT2','WICK_LOW_PIVOT2']
        self.s.loc[a,'active_line_slope_pct']=[.7,.7,-.1,-.1]
        self.s.loc[a[2],'state']='REANCHORED_NEW_PIVOT_OR_CANDLE'
        z=line_transition_events(self.s).iloc[0]
        self.assertTrue(z.slope_sign_flip)
        self.assertFalse(z.same_family_reanchor)
    def test_no_event_when_unchanged(self):
        self.assertEqual(len(line_transition_events(self.s)),0)
    def test_break_type_is_not_reversal_proof(self):
        a=self.s[self.s.lane.eq('LOWER_MEDIUM')].index
        self.s.loc[a[1],'state']='SUPPORT_BROKEN_DOWN'
        self.s.loc[a[1],'retired_line_id']='old'
        x=line_transition_events(self.s)
        self.assertEqual(x.iloc[0].transition_kind_PIT,'ACTIVE_SUPPORT_BROKEN')
        self.assertFalse(x.iloc[0].can_claim_price_trend_reversal)


class RoleRetestTests(unittest.TestCase):
    def setUp(self):
        self.d=toy_prices().iloc[:6].copy()
        self.s=state_rows(self.d)
        self.line=pd.DataFrame([{'line_id':'old','v2':11.,'slope_krw_per_bar':0.,'i2':541,
                                'code':'000001','kind':'UPPER'}])
        m=(self.s.lane=='UPPER_SHORT')&(self.s.idx==541)
        self.s.loc[m,'state']='RESISTANCE_BROKEN_UP'
        self.s.loc[m,'retired_line_id']='old'
    def test_role_observations_after_break_only(self):
        a=role_retest_events(self.d,self.s,self.line)
        self.assertTrue(all(a.idx>a.break_idx))
    def test_future_change_does_not_affect_past_role(self):
        a=role_retest_events(self.d,self.s,self.line)
        b=self.d.copy();b.loc[b.idx>543,['high','low','close']]+=2
        c=role_retest_events(b,self.s,self.line)
        aa=a[a.idx<=543][['idx','role_observation_PIT']].to_dict('records')
        cc=c[c.idx<=543][['idx','role_observation_PIT']].to_dict('records')
        self.assertEqual(aa,cc)
    def test_retired_line_does_not_disappear_from_evaluation(self):
        a=role_retest_events(self.d,self.s,self.line)
        self.assertTrue(len(a)>0)
    def test_missing_broken_line_detected(self):
        with self.assertRaises(AssertionError):role_retest_events(self.d,self.s,self.line.iloc[0:0])


class EndToEndTests(unittest.TestCase):
    def test_synthetic_outputs_and_upstream_unchanged(self):
        d=toy_prices();s=state_rows(d)
        # A single break of active line with an archive entry triggers a role overlay.
        a=s[(s.idx==545)&(s.lane=='UPPER_SHORT')].index[0]
        s.loc[a,'state']='RESISTANCE_BROKEN_UP';s.loc[a,'retired_line_id']='id1'
        l=pd.DataFrame([{'code':'000001','line_id':'id1','v2':11.,'slope_krw_per_bar':0.,'i2':542}])
        outcome=d[['code','idx','date']].copy();outcome['dp20_ret_pct_EXPOST']=np.nan
        with tempfile.TemporaryDirectory() as q:
            top=Path(q);(top/'r108').mkdir();(top/'r106').mkdir()
            d.to_csv(top/'r108'/FIELDS['full'],index=False)
            s.to_csv(top/'r108'/FIELDS['state'],index=False)
            outcome.to_csv(top/'r108'/FIELDS['expost'],index=False)
            l.to_csv(top/'r106'/FIELDS['lines'],index=False)
            result=run(base_r108=top/'r108',base_r106=top/'r106',out=top/'result',draw=False,refs=[])
            self.assertTrue(result['UPSTREAM_SHA256_UNCHANGED'])
            self.assertEqual(result['stock_dates'],len(d))
            self.assertEqual(result['source_stocks'],1)
            x=pd.read_csv(top/'result'/'R1D109_ALL_DAYS_PIT_TREND_CHANGE_FULL_CONFLUENCE.csv')
            self.assertFalse(any('EXPOST' in c for c in x.columns))
            y=pd.read_csv(top/'result'/'R1D109_TREND_CHANGE_OUTCOMES_EXPOST.csv')
            self.assertIn('dp20_ret_pct_EXPOST',y)
            self.assertEqual(x.active_resistance_breaks.sum(),1)
            self.assertGreaterEqual(x.candidate_trend_change_today.sum(),1)

if __name__=='__main__':unittest.main()
