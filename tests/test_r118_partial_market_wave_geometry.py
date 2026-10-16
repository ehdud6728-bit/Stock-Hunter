import tempfile, unittest, json, hashlib
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import numpy as np
import pandas as pd
from closebet_research.r118_partial_market_wave_geometry import (
    compute_geometry, build_outputs, digest, require, read_r117, sym, VERSION)

class R118Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.official=list(pd.bdate_range('2025-01-01',periods=301).strftime('%Y-%m-%d'))
        cls.t=299

    def hist(self):
        dates=self.official
        h=pd.DataFrame({'date':dates,'code':'000050','sess':range(len(dates)),
                        'open':100.,'high':101.,'low':99.,'close':100.,'volume':1000.,'amount':100000.})
        h.loc[self.t-30,['low','close']]=[80.,95.]
        h.loc[self.t-16,'high']=120.
        h.loc[self.t,['open','high','low','close']]=[108.,111.,107.,109.]
        return h

    def cohort(self,after_ret=5.0):
        return pd.DataFrame([{'code':'000050','signal_date':self.official[self.t],
            'selected_any_mode':False,'outcome_eligible':True,'asof_quality_gate':True,
            'illustrative_net_win':float(after_ret>0), 'illustrative_net_05pct':after_ret,
            'vol20_to_prev20':.97,'vol5_to_prev20':.83,'ma224_asof':100.,
            'close_vs_ma224_pct':9.,'gross_d1open_d5close_pct':after_ret+.5,
            'd0_close':109.,'d1_open':100.,'d5_close':106.,
        }])

    def test_six_char_symbol_preserves_leading_zeros(self):
        self.assertEqual(sym('000050'),'000050')
        self.assertEqual(sym('50.0'),'000050')

    def test_below224_origin_low_peak_pullback_geometry(self):
        got=compute_geometry(self.cohort(),self.hist(),self.official).iloc[0]
        self.assertTrue(got['broad_224_pullback_candidate'])
        self.assertTrue(got['origin_below224_asof'])
        self.assertEqual(got['wave_geometry_status'],'BROAD_LOW_PEAK_PULLBACK_PROXY_ONLY')
        self.assertAlmostEqual(got['wave_rise_low_to_peak_pct'],50.)
        self.assertAlmostEqual(got['pullback_fraction'],(120.-109.)/(120.-80.))

    def test_future_bar_must_not_change_signal_geometry(self):
        original=compute_geometry(self.cohort(),self.hist(),self.official).iloc[0]
        h=self.hist();h.loc[self.t+1,['high','close','volume']]=[900.,10.,999999.]
        newer=compute_geometry(self.cohort(),h,self.official).iloc[0]
        self.assertEqual(original.wave_geometry_status,newer.wave_geometry_status)
        self.assertEqual(original.peak_date,newer.peak_date)
        self.assertEqual(original.wave_rise_low_to_peak_pct,newer.wave_rise_low_to_peak_pct)

    def test_missing_prior_session_must_fail_closed(self):
        h=self.hist();h=h[h.sess.ne(self.t-15)]
        q=compute_geometry(self.cohort(),h,self.official).iloc[0]
        self.assertFalse(q.broad_224_pullback_candidate)
        self.assertEqual(q.wave_geometry_status,'PRIOR45_OFFICIAL_SESSION_GAP')

    def test_missing_224_origin_session_must_fail_closed(self):
        h=self.hist();h=h[h.sess.ne(self.t-190)]
        q=compute_geometry(self.cohort(),h,self.official).iloc[0]
        self.assertFalse(q.broad_224_pullback_candidate)
        self.assertEqual(q.wave_geometry_status,'ORIGIN_224_MA_INSUFFICIENT_OR_GAP')

    def test_anchor_not_below_ma_not_candidate(self):
        h=self.hist();h['close']=70.;h.loc[self.t,['close','high']]=[109.,111.]
        q=compute_geometry(self.cohort(),h,self.official).iloc[0]
        self.assertFalse(q.broad_224_pullback_candidate)
        self.assertEqual(q.wave_geometry_status,'ORIGIN_NOT_BELOW224')

    def test_no_peak_after_last_origin(self):
        h=self.hist();h.loc[self.t-1,'low']=1.
        q=compute_geometry(self.cohort(),h,self.official).iloc[0]
        self.assertFalse(q.broad_224_pullback_candidate)
        self.assertEqual(q.wave_geometry_status,'NO_PRIOR_PEAK_AFTER_ORIGIN')

    def test_no_proxy_tune_with_future_outcomes(self):
        one=self.cohort(after_ret=-50.)
        two=self.cohort(after_ret=+100.)
        g=compute_geometry(one,self.hist(),self.official)
        scope=pd.DataFrame([{'signal_date':self.official[self.t],
           'full_market_krx_source_rows':2872,'stored_krx_source_rows':1}])
        out1=build_outputs(one,scope,g)[0].iloc[0]
        out2=build_outputs(two,scope,g)[0].iloc[0]
        self.assertEqual(out1['group'],out2['group'])
        self.assertEqual(out1['broad_research_eligible'],out2['broad_research_eligible'])
        self.assertEqual(out1['wave_geometry_status'],out2['wave_geometry_status'])

    def test_missing_market_source_is_not_fake_zero(self):
        scope=pd.DataFrame([{'signal_date':self.official[self.t],
           'full_market_krx_source_rows':2872,'stored_krx_source_rows':1}])
        g=compute_geometry(self.cohort(),self.hist(),self.official)
        result=build_outputs(self.cohort(),scope,g)
        self.assertEqual(int(result[2].iloc[0].missing_unobserved_full_market_count),2871)
        self.assertNotIn('true_pit_universe',result[0].columns)

    def test_corrupted_sha_requires_error(self):
        with self.assertRaises(RuntimeError):require(digest(b'wrong')==digest(b'right'),'SHA_MISMATCH')

    def test_research_version_lock(self):
        self.assertTrue(VERSION.startswith('R118_'))

if __name__=='__main__':unittest.main()
