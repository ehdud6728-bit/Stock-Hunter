"""Synthetic point-in-time checks; real-market research is the GitHub runner."""
import sys
import unittest
from pathlib import Path
import numpy as np
import pandas as pd

HERE=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(HERE))
from pre_rise import (MA_WINDOWS,BB_WINDOWS,ENV_WINDOWS,validate,build_features,
                      future_labels,choose_cases,event_snapshots,descriptive_compare,append_reference_case)


def frame(n=650,slope=-.08):
    days=pd.bdate_range('2021-01-01',periods=n)
    c=300+slope*np.arange(n,dtype=float)
    return pd.DataFrame({'date':days,'open':c,'high':c+2,'low':c-2,'close':c,
                         'volume':np.full(n,100000,dtype=int),'amount':c*100000})

class R1D101Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.d=frame()
        cls.f=build_features(cls.d)
    def test_01_default_comprehensive_periods(self):
        self.assertTrue({3,5,10,20,30,40,60,112,120,224,240,448,480}.issubset(set(MA_WINDOWS)))
    def test_02_all_mas_included(self):
        self.assertTrue(all(f'ma{n}' in self.f.columns for n in MA_WINDOWS))
    def test_03_all_ma_pairs_spread_and_delta(self):
        n=len(MA_WINDOWS);self.assertEqual(sum(z.startswith('ma') and z.endswith('_gap_pct') and '_ma' in z for z in self.f.columns),n*(n-1)//2)
    def test_04_ma_slope_and_acceleration(self):
        for n in MA_WINDOWS:
            self.assertTrue(all(f'ma{n}_{q}' in self.f for q in ('slope5_pct','slope20_pct','slope20_accel_pp','price_gap_pct')))
    def test_05_bb_all_periods_symmetric(self):
        for n in BB_WINDOWS:
            a=self.f.iloc[-1];self.assertAlmostEqual((a[f'bb{n}_upper']+a[f'bb{n}_lower'])/2,a[f'bb{n}_mid'])
    def test_06_bollinger_width_changes_data(self):
        f=build_features(frame(slope=0))
        self.assertAlmostEqual(f.iloc[-1]['bb20_width_pct'],0.,places=8)
    def test_07_env_constant_relative_width(self):
        for n in ENV_WINDOWS:
            self.assertAlmostEqual(self.f.iloc[-1][f'env{n}_10_width_rel_pct'],20.)
    def test_08_env_abs_width_changes_with_ma(self):
        a=self.f.iloc[-1];b=self.f.iloc[-30];self.assertNotEqual(a['env20_10_width_abs'],b['env20_10_width_abs'])
    def test_09_prev20_baseline_excludes_today(self):
        x=self.d.copy();x.loc[x.index[-1],'volume']=500000
        f=build_features(x);self.assertAlmostEqual(f.iloc[-1]['volume_vs_prev20'],5)
    def test_10_value_volume_distinct(self):
        x=self.d.copy();x.loc[x.index[-1],'amount']*=10
        f=build_features(x)
        self.assertGreater(f.iloc[-1]['amount_vs_prev20'],f.iloc[-1]['volume_vs_prev20'])
    def test_11_pit_future_tamper_unaltered(self):
        k=569;x=self.d.copy()
        for col in ('open','high','low','close'):
            x[col]=x[col].astype(float)
            x.loc[x.index>k,col]*=1.217
        newf=build_features(x)
        pd.testing.assert_series_equal(self.f.iloc[k],newf.iloc[k],check_names=False)
    def test_12_future_outcome_not_in_features(self):
        self.assertFalse(any('forward' in s or 'surge_label' in s for s in self.f.columns))
    def test_13_surge_forward_only_after_event(self):
        x=frame(n=680);t=555;base=float(x.loc[t,'close'])
        for k in range(t+1,len(x)):
            price=base*(1+.015*min(k-t,20))
            x.loc[k,['open','close']]=price
            x.loc[k,'high']=price+2;x.loc[k,'low']=price-2
        f=build_features(x);out=future_labels(f)
        self.assertTrue(bool(out.loc[t,'is_sustained_surge']))
        self.assertFalse(bool(out.loc[t,'is_nonrise_control']))
    def test_14_label_does_not_change_past_feature(self):
        x=self.d.copy();t=560
        f=build_features(x)
        old=f.iloc[t].copy();x.loc[t+1:,'close']=10000.;x.loc[t+1:,'open']=10000.;x.loc[t+1:,'high']=10002.;x.loc[t+1:,'low']=9998.
        new=build_features(x)
        pd.testing.assert_series_equal(old,new.iloc[t],check_names=False)
        self.assertNotEqual(float(future_labels(f).iloc[t].forward20_peak_pct),float(future_labels(new).iloc[t].forward20_peak_pct))
    def test_15_no_future_label_for_last20(self):
        out=future_labels(self.f)
        self.assertTrue(out.iloc[-20:].forward20_peak_pct.isna().all())
        self.assertFalse(out.iloc[-20:].is_sustained_surge.any())
    def test_16_control_definition(self):
        out=future_labels(self.f)
        self.assertTrue(out.iloc[550].is_nonrise_control)
    def test_17_case_labels_are_mutually_exclusive(self):
        out=future_labels(self.f)
        self.assertFalse((out.is_sustained_surge & out.is_nonrise_control).any())
    def test_18_case_selection_respects_below_224(self):
        out=future_labels(self.f)
        cases=choose_cases(self.f,out,study_start=529)
        self.assertFalse(cases.empty)
        for c in cases.itertuples():self.assertLess(self.f.iloc[int(c.idx)].close,self.f.iloc[int(c.idx)].ma224)
    def test_19_deoverlap_within_cohort(self):
        cases=choose_cases(self.f,future_labels(self.f),study_start=529,max_cases=8)
        for label in cases.label.unique():
            ids=cases[cases.label==label].idx.tolist()
            self.assertTrue(all(y-x>=20 for x,y in zip(ids,ids[1:])))
    def test_20_preevent_snapshots_never_after_anchor(self):
        cases=choose_cases(self.f,future_labels(self.f),study_start=529,max_cases=2)
        s=event_snapshots(self.f,cases)
        self.assertFalse(s.empty)
        self.assertTrue((s.relative_bar<=0).all())
        self.assertTrue((s.observation_date<=s.anchor_date).all())
    def test_21_control_and_surge_group_comparison(self):
        x=pd.DataFrame({'label':['SURGE','CONTROL'],'relative_bar':[0,0],'close':[10.,8.],
                        'ma20_price_gap_pct':[3.,-2.],'anchor_date':['2024-01-01']*2,
                        'observation_date':['2024-01-01']*2,'idx':[10,20]})
        y=descriptive_compare(pd.DataFrame(),x)
        self.assertTrue('ma20_price_gap_pct' in set(y.feature))
    def test_22_reject_unsorted_duplicate(self):
        x=self.d.copy();x.loc[3,'date']=x.loc[2,'date']
        with self.assertRaises(ValueError):build_features(x)
    def test_23_reject_invalid_ohlc(self):
        x=self.d.copy();x.loc[3,'high']=1
        with self.assertRaises(ValueError):build_features(x)
    def test_24_no_missing_latest_ma480(self):
        self.assertTrue(np.isfinite(float(self.f.iloc[-1]['ma480_slope20_accel_pp'])))
    def test_25_volume_accumulation_up_share(self):
        self.assertAlmostEqual(float(self.f.iloc[-1]['up_day_amount_share20']),0.)
    def test_26_pit_bb_width_rank_not_future(self):
        self.assertTrue(0<=self.f.iloc[-1]['bb20_width_rank120_pct']<=100)
    def test_27_configurable_ma_windows(self):
        f=build_features(frame(n=100),ma_windows=(5,10,20,40))
        self.assertIn('ma5_ma40_gap_change20_pp',f.columns)
        self.assertNotIn('ma480',f.columns)
    def test_29_reference_added_even_if_control_or_unlabeled(self):
        cases=choose_cases(self.f,future_labels(self.f),study_start=529,max_cases=2)
        date=str(self.f.iloc[610].date.date())
        enriched=append_reference_case(cases,self.f,future_labels(self.f),date)
        self.assertEqual(sum(enriched.label=='REFERENCE'),1)
        self.assertEqual(int(enriched.iloc[-1].idx),610)
        self.assertEqual(len(enriched),len(cases)+1)
    def test_30_reference_excluded_from_group_medians(self):
        cases=choose_cases(self.f,future_labels(self.f),study_start=529,max_cases=1)
        date=str(self.f.iloc[610].date.date())
        with_ref=append_reference_case(cases,self.f,future_labels(self.f),date)
        self.assertEqual((with_ref.label=='SURGE').sum(),(cases.label=='SURGE').sum())
        self.assertEqual((with_ref.label=='CONTROL').sum(),(cases.label=='CONTROL').sum())
    def test_28_invalid_window_rejected(self):
        with self.assertRaises(ValueError):build_features(self.d,ma_windows=(0,10))

if __name__=='__main__':unittest.main()
