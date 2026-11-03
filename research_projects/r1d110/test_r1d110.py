"""R1-D110 causality / ordering tests, independent of GitHub data files."""
import sys,unittest,tempfile,json
from pathlib import Path
import numpy as np
import pandas as pd
HERE=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(HERE))
from episodes import day_components,compute,expost_outcomes,pre_anchor_context,validate_source,_simultaneous_groups
from research_r1d110 import run,INPUTS


def synthetic(n=45):
    rows=[]
    for i in range(n):
        close=100+(i//5)*2; code='000001'; date=str((pd.Timestamp('2024-01-02')+pd.Timedelta(days=i)).date())
        rows.append({'code':code,'name':'테스트','idx':541+i,'date':date,
          'open':close-1,'high':close+2,'low':close-3,'close':close,
          'candidate_trend_change_today':i in (2,3,10,21,23,24,43),
          'pivot_structure_PIT':'HIGHER_HIGH_HIGHER_LOW' if i>=10 else 'LOWER_HIGH_LOWER_LOW',
          'pivot_structure_changed_today':i==10,'has_new_pivot_today':i in (10,21),
          'active_support_breaks':1 if i in (2,23) else 0,
          'active_resistance_breaks':1 if i==12 else 0,
          'active_slope_sign_flips':1 if i==13 else 0,'reanchors':1 if i in (13,23) else 0,
          'candle_body_share_pct':20. if i!=12 else 75.,
          'candle_upper_wick_share_pct':55. if i==14 else 20.,
          'candle_lower_wick_share_pct':15.,
          'candle_upper_wick_body_ratio':1.5 if i==14 else .7,
          'candle_lower_wick_body_ratio':.5,
          'candle_range_vs_prev14_atr':1.3,
          'close_vs_prev3_high_max_pct':1. if i in (12,24) else -.8,
          'low_vs_prev3_low_min_pct':-2. if i==2 else 1.,
          'ma5_slope5_pct':.5 if i>=12 else -.5,
          'ma20_slope20_pct':1. if i>=13 else -1.,
          'ma112_slope20_pct':-.5,'ma224_slope20_pct':-.3,
          'ma20_gap_pct':.3 if i>=12 else -.2,
          'bb20_width_pct':9.,
          'bb20_width_delta5_pp':.1 if i>=13 else -.1,
          'bb20_position':'ABOVE_UPPER' if i==12 else 'INSIDE',
          'env20_10_position':'ABOVE_UPPER' if i==13 else 'INSIDE',
          'env40_10_position':'INSIDE',
          'ma_cloud_fast_position':'ABOVE' if i>=13 else 'INSIDE',
          'ichimoku_current_position':'INSIDE',
          'amount_vs_prev20':1.7 if i==12 else .7,
          'volume_vs_prev20':1.5 if i==12 else .7})
    return pd.DataFrame(rows)


class EngineTests(unittest.TestCase):
    def test_episode_open_min_gap_and_censor(self):
        o,t,e,c=compute(synthetic())
        self.assertEqual(list(o.anchor_idx),[543,564,584])
        self.assertEqual(c.complete_window.tolist(),[True,True,False])
        self.assertEqual(c.bars_observed.tolist(),[20,20,2])
        self.assertEqual(len(t),42)
    def test_each_day_belongs_to_at_most_one(self):
        o,t,e,c=compute(synthetic())
        self.assertFalse(t[['code','idx']].duplicated().any())
        self.assertFalse(o.episode_id.duplicated().any())
    def test_event_onset_only_today_or_earlier(self):
        o,t,e,c=compute(synthetic())
        q=e.merge(o[['episode_id','anchor_idx']],on='episode_id')
        self.assertTrue((q.observation_idx>=q.anchor_idx).all())
        self.assertTrue((q.lag_bars_from_anchor>=0).all())
        self.assertTrue((q.lag_bars_from_anchor<20).all())
    def test_future_tamper_cannot_change_past_pit_rows(self):
        source=synthetic();o,t,e,c=compute(source)
        altered=source.copy()
        later=altered.idx>558
        altered.loc[later,'close']*=100
        altered.loc[later,'open']*=100
        altered.loc[later,'ma20_slope20_pct']*=-1
        altered.loc[later,'active_resistance_breaks']=10
        oo,tt,ee,cc=compute(altered)
        self.assertEqual(o[o.anchor_idx<=558].to_dict('records'),oo[oo.anchor_idx<=558].to_dict('records'))
        self.assertEqual(t[t.idx<=558].to_dict('records'),tt[tt.idx<=558].to_dict('records'))
        self.assertEqual(e[e.observation_idx<=558].to_dict('records'),ee[ee.observation_idx<=558].to_dict('records'))
    def test_close_summary_not_available_at_anchor(self):
        o,t,e,c=compute(synthetic())
        self.assertEqual(c.iloc[0].known_at,'EPISODE_END_DATE_CLOSE_NOT_AT_ANCHOR')
        self.assertNotIn('first_observed_component_sequence',o.columns)
        self.assertEqual(int(c.iloc[0].end_idx),int(o.iloc[0].anchor_idx)+19)
    def test_day_after_event_does_not_shift_event_date(self):
        o,t,e,c=compute(synthetic())
        z=e[e.component.eq('ACTIVE_RESISTANCE_BREAK')]
        self.assertEqual(z.observation_idx.tolist(),[553])
    def test_same_day_order_is_explicitly_tied(self):
        s=_simultaneous_groups({'a':('2024-01-05',3,'MA'),'b':('2024-01-05',3,'BB'),'c':('2024-01-04',2,'LINE')})
        self.assertIn('D+3:2024-01-05[a|b]',s)
    def test_wick_is_observation_not_future_direction(self):
        d=synthetic();prev=d.iloc[13].to_dict();r=d.iloc[14].to_dict()
        x=day_components(r,prev)
        self.assertTrue(any(t[1]=='UPPER_WICK_DOMINANT' for t in x))
        self.assertFalse(any('BEARISH_FORECAST' in t[1] for t in x))
    def test_reanchor_separate_from_pivot_proof(self):
        d=synthetic();x=day_components(d.iloc[13].to_dict(),d.iloc[12].to_dict())
        self.assertIn('SLOPE_SIGN_REANCHOR',[a[1] for a in x]);self.assertNotIn('CONFIRMED_REVERSAL',[a[1] for a in x])
    def test_indicator_order_is_first_observation_not_model_assumption(self):
        o,t,e,c=compute(synthetic())
        x=e[(e.episode_id==o.iloc[0].episode_id)&e.is_first_this_component_in_episode]
        v=x.groupby('component').lag_bars_from_anchor.min().to_dict()
        self.assertLessEqual(v['BULL_BODY_60'],v['MA20_SLOPE20_UPTURN'])
    def test_role_retest_not_assigned_break_date(self):
        d=synthetic();roles=pd.DataFrame([{'code':'000001','idx':555,'role_observation_PIT':'POTENTIAL_SUPPORT_RETEST'}])
        o,t,e,c=compute(d,roles)
        z=e[e.family.eq('ROLE')];self.assertEqual(z.observation_idx.tolist(),[555])
    def test_expost_never_present_in_pit(self):
        o,t,e,c=compute(synthetic())
        self.assertTrue(all('EXPOST' not in x.upper() for q in (o,t,e,c) for x in q.columns))
        y=expost_outcomes(synthetic(),o)
        self.assertTrue('Dp20_close_pct_EXPOST' in y)
        self.assertTrue(y.iloc[-1].Dp20_full_window_EXPOST==False)
    def test_no_date_duplicates(self):
        d=synthetic();d=pd.concat([d,d.iloc[[7]]],ignore_index=True)
        with self.assertRaises(ValueError):validate_source(d)
    def test_gapped_source_rejected(self):
        with self.assertRaises(ValueError):validate_source(synthetic().drop(index=7))
    def test_future_column_rejected(self):
        d=synthetic();d['DPLUS5_EXPOST']=1
        with self.assertRaises(ValueError):validate_source(d)
    def test_window_invalid(self):
        with self.assertRaises(ValueError):compute(synthetic(),window=2)
    def test_body_share_requires_actual_bullish_close(self):
        d=synthetic();x=d.iloc[12].to_dict();x['open']=x['close']+5
        events=day_components(x,d.iloc[11].to_dict())
        self.assertNotIn('BULL_BODY_60',[e[1] for e in events])
    def test_truncated_outcomes_null_not_zero(self):
        d=synthetic();o,*_=compute(d);y=expost_outcomes(d,o)
        tail=y.iloc[-1];self.assertTrue(pd.isna(tail['Dp20_close_pct_EXPOST']))
    def test_event_first_true_only_once_per_episode(self):
        o,t,e,c=compute(synthetic());f=e[e.is_first_this_component_in_episode]
        self.assertFalse(f[['episode_id','component']].duplicated().any())
    def test_correlation_not_aggregated_to_score(self):
        o,t,e,c=compute(synthetic())
        self.assertFalse(any('score' in z.lower() or 'profit_probability' in z.lower() for z in o.columns))
        self.assertTrue({'MA','BB','ENV','CLOUD','FLOW','CANDLE','LINE'}.issubset(set(e.family)))
    def test_preanchor_never_contains_future(self):
        d=synthetic();o,t,e,c=compute(d);before=pre_anchor_context(d,o)
        anchor=o[['episode_id','anchor_idx']]
        q=before.merge(anchor,on='episode_id',how='left')
        self.assertTrue((q.observation_idx<=q.anchor_idx).all())
        self.assertTrue((q.relative_bar_from_anchor.between(-5,0)).all())
        self.assertFalse(any('EXPOST' in name.upper() for name in before.columns))
    def test_end_to_end_driver_with_csvs_and_input_hash(self):
        d=synthetic()
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)/'input';root.mkdir()
            tables={'daily':d,
              'role':pd.DataFrame([{'code':'000001','idx':551,'date':d.iloc[10].date,
                 'broken_line_id':'old','role_observation_PIT':'POTENTIAL_SUPPORT_RETEST'}]),
              'pivot':pd.DataFrame([{'code':'000001','pivot_date':d.iloc[8].date,'confirmed_date':d.iloc[10].date}]),
              'transitions':pd.DataFrame([{'code':'000001','idx':551,'date':d.iloc[10].date}])}
            for k,table in tables.items():table.to_csv(root/INPUTS[k],index=False)
            r=run(source=root,out=Path(tmp)/'out',draw=False,case_refs=[])
            self.assertTrue(r['UPSTREAM_SHA256_UNCHANGED'])
            self.assertEqual(r['unique_episodes'],3)
            self.assertEqual(r['stock_dates'],45)
            self.assertTrue((Path(tmp)/'out'/'R1D110_PIT_PRE_ANCHOR_5BAR_CONTEXT.csv').is_file())
            self.assertTrue((Path(tmp)/'out'/'R1D110_EPISODE_OUTCOMES_EXPOST.csv').is_file())
    def test_actual_cloud_above_below_values_are_detected(self):
        d=synthetic();left=d.iloc[12].to_dict();right=d.iloc[13].to_dict()
        right['ma_cloud_fast_position']='ABOVE';right['ichimoku_current_position']='BELOW'
        seq=[key for _,key,_ in day_components(right,left)]
        self.assertIn('FAST_MA_CLOUD_ABOVE',seq)
        self.assertIn('ICHIMOKU_BELOW',seq)
    def test_prev2_and_prev3_candles_kept_separate(self):
        d=synthetic();right=d.iloc[13].to_dict()
        right['close_vs_prev2_high_max_pct']=1.2
        right['close_vs_prev3_high_max_pct']=-.2
        seq=[key for _,key,_ in day_components(right,d.iloc[12].to_dict())]
        self.assertIn('CLOSE_ABOVE_PREV2_HIGH',seq)
        self.assertNotIn('CLOSE_ABOVE_PREV3_HIGH',seq)
    def test_no_lookahead_when_future_candles_modified(self):
        d=synthetic();o,t,e,c=compute(d)
        p=d.copy();p.loc[p.idx>560,'candle_upper_wick_share_pct']=99
        oo,tt,ee,cc=compute(p)
        self.assertEqual(e[e.observation_idx<=560].to_dict('records'),ee[ee.observation_idx<=560].to_dict('records'))


if __name__=='__main__':unittest.main()
