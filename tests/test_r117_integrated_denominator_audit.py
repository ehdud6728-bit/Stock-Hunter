import unittest
import numpy as np
import pandas as pd
from closebet_research.r117_integrated_denominator_audit import (sha,check,six,features,build_cohort,build_date_matched_benchmark)

class ResearchTests(unittest.TestCase):
    def test_code_preserves_leading_zero(self):
        self.assertEqual(six('33780'),'033780')
        self.assertEqual(six('033780'),'033780')

    def test_sha_and_fail_closed(self):
        self.assertEqual(len(sha(b'ok')),64)
        with self.assertRaises(RuntimeError): check(False,'BAD_EVIDENCE')

    def test_d0_feature_future_blindness(self):
        dates=pd.date_range('2024-01-01',periods=260,freq='B').strftime('%Y-%m-%d')
        raw=pd.DataFrame({'code':['000001']*260,'date':dates,'open':100.0,'high':110.0,'low':90.0,'close':100.0,'volume':100.0,'amount':10000.0})
        old=features(raw);changed=raw.copy();changed.loc[changed.index>=230,'close']=200.0
        new=features(changed)
        for col in ['ma224_asof','vol20_to_prev20','close_vs_ma224_pct','rise_from_prior40low_pct']:
            self.assertAlmostEqual(float(old.iloc[229][col]),float(new.iloc[229][col]),places=6,msg=col)

    def test_d1open_d5close_no_signal_day_same_bar(self):
        days=pd.date_range('2024-01-01',periods=260,freq='B').strftime('%Y-%m-%d').tolist()
        raw=pd.DataFrame({'code':['000001']*260,'date':days,'open':100.0,'high':110.0,'low':90.0,'close':100.0,'volume':100.0,'amount':10000.0})
        raw.loc[days.index(days[246]),'open']=105
        raw.loc[days.index(days[250]),'close']=110
        d=features(raw)
        selected=pd.DataFrame([{'signal_date':days[245],'code':'000001','mode':'A'}]); c,_,_=build_cohort(d,selected,days)
        row=c.iloc[0]; self.assertEqual(row['d1_open'],105); self.assertEqual(row['d5_close'],110)
        self.assertAlmostEqual(row['illustrative_net_05pct'],(110/105-1)*100-.5,places=6)

    def test_missing_future_is_unknown_not_zero(self):
        days=pd.date_range('2024-01-01',periods=250,freq='B').strftime('%Y-%m-%d').tolist()
        raw=pd.DataFrame({'code':['000001']*250,'date':days,'open':100.0,'high':110.0,'low':90.0,'close':100.0,'volume':100.0,'amount':10000.0})
        c,_,_=build_cohort(features(raw),pd.DataFrame([{'signal_date':days[-1],'code':'000001','mode':'A'}]),days)
        self.assertFalse(c.iloc[0]['outcome_eligible']); self.assertTrue(np.isnan(c.iloc[0]['illustrative_net_05pct']))

    def test_no_technical_false_negative_from_absence(self):
        days=pd.date_range('2024-01-01',periods=255,freq='B').strftime('%Y-%m-%d').tolist()
        raw=pd.concat([pd.DataFrame({'code':[code]*255,'date':days,'open':100.0,'high':110.0,'low':90.0,'close':100.0,'volume':100.0,'amount':10000.0}) for code in ('000001','000002')])
        c,summary,_=build_cohort(features(raw),pd.DataFrame([{'signal_date':days[245],'code':'000001','mode':'A'}]),days)
        self.assertEqual(len(c),2);self.assertEqual(int(c.selected_any_mode.sum()),1)
        self.assertIn('NOT_FULL_MARKET',str(summary.scope_warning.iloc[0]))

    def test_date_matched_benchmark_uses_same_date(self):
        a=pd.DataFrame([{'signal_date':'2026-01-01','code':'000001','selected_any_mode':True,'outcome_eligible':True,'illustrative_net_05pct':2.,'illustrative_net_win':1,'selected_modes':'A'},
                        {'signal_date':'2026-01-01','code':'000002','selected_any_mode':False,'outcome_eligible':True,'illustrative_net_05pct':1.,'illustrative_net_win':1,'selected_modes':None},
                        {'signal_date':'2026-01-02','code':'000003','selected_any_mode':False,'outcome_eligible':True,'illustrative_net_05pct':99.,'illustrative_net_win':1,'selected_modes':None}])
        m,d=build_date_matched_benchmark(a)
        self.assertEqual(len(m),1);self.assertAlmostEqual(m.iloc[0]['selected_minus_same_date_control_net_pct'],1.0)

if __name__=='__main__': unittest.main()
