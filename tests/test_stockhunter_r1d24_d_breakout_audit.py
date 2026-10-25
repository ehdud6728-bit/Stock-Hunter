import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
import numpy as np
import pandas as pd

SOURCE=Path(__file__).resolve().parents[1]/'research_formulas'/'stockhunter_r1d24_d_breakout_audit.py'
SPEC=importlib.util.spec_from_file_location('r1d24_d_breakout',SOURCE)
M=importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(M)
PROTOCOL=Path(__file__).resolve().parents[1]/'R1D24_PREDECLARED_PROTOCOL.json'

class R1D24Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.p=M.read_protocol(PROTOCOL)

    def make_d(self):
        return pd.DataFrame([{'strategy':'D_LEADER_CONSOLIDATION_BREAKOUT',
            'date':'2025-05-08','code':'000001','period':'LATE_2024_2025',
            'base10_range_pct':9.0,'amount_vs_prior20':3.5,'amount_KRW':20000000000,
            'ma224_slope20_pct':2.7,'distance224_pct':25.0},
           {'strategy':'D_LEADER_CONSOLIDATION_BREAKOUT',
            'date':'2025-05-08','code':'000002','period':'LATE_2024_2025',
            'base10_range_pct':16.0,'amount_vs_prior20':1.5,'amount_KRW':2000000000,
            'ma224_slope20_pct':0.5,'distance224_pct':53.0}])

    def test_fixed_protocol_exact_family(self):
        self.assertEqual(len(self.p['binary_conditions_frozen_before_return_join']),11)
        self.assertEqual(len(self.p['combinations_frozen_before_return_join']),3)
        self.assertTrue(self.p['historical_reuse_not_new_OOS'] if 'historical_reuse_not_new_OOS' in self.p else self.p['periods_are_reused_historical_not_new_OOS'])

    def test_flags_no_future_return_needed(self):
        x,names=M.define_flags(self.make_d(),self.p)
        self.assertEqual(len(names),14)
        self.assertNotIn('net_20bps_pct',x.columns)
        self.assertEqual(x['narrow10_and_rel_amount3'].to_list(),[1,0])
        self.assertEqual(x['narrow10_and_slope2'].to_list(),[1,0])
        self.assertEqual(x['broad_10d_range_ge_15pct'].to_list(),[0,1])

    def test_cutoff_inclusive(self):
        d=self.make_d()
        d.loc[0,'base10_range_pct']=10.0
        d.loc[0,'amount_vs_prior20']=3.0
        d.loc[0,'ma224_slope20_pct']=2.0
        x,_=M.define_flags(d,self.p)
        self.assertEqual(x.loc[0,'narrow10_and_rel_amount3'],1)
        self.assertEqual(x.loc[0,'narrow10_and_slope2'],1)

    def test_missing_factor_fails_closed(self):
        d=self.make_d();d.loc[0,'base10_range_pct']=np.nan
        with self.assertRaisesRegex(RuntimeError,'FACTOR_MISSING'):
            M.define_flags(d,self.p)

    def test_outcome_censored_excluded_not_forced_loss(self):
        g=pd.DataFrame([{'status_out':M.DONE,'date':'2025-01-02','code':'111111',
                          'net_20bps_pct':16.,'mae_intraday_pct':-2.,'mfe_intraday_pct':18.},
                        {'status_out':'SUSPENDED','date':'2025-01-03','code':'222222',
                          'net_20bps_pct':np.nan,'mae_intraday_pct':np.nan,'mfe_intraday_pct':np.nan}])
        x=M.summarize(g,'TEST','all',1,20,'D')
        self.assertEqual((x['complete'],x['censored'],x['final_ge15_pct']),(1,1,100.))

    def test_date_matched_no_counterpart_returns_empty(self):
        d=self.make_d();d['net_20bps_pct']=[1.,-1.]
        d['binary']=1
        x=M.date_match_within_D(d,'binary')
        self.assertEqual(x['matched_dates'],0)

    def test_date_match_equal_date_weight(self):
        a=self.make_d();a['flag']=[1,0];a['net_20bps_pct']=[4.,-4.]
        x=M.date_match_within_D(a,'flag')
        self.assertEqual(x['matched_dates'],1)
        self.assertAlmostEqual(x['mean_net_delta_pp'],8.)
        self.assertAlmostEqual(x['win_delta_pp'],100.)

    def test_archive_sha_tamper_fails(self):
        with tempfile.TemporaryDirectory() as td:
            f=Path(td)/'bad.zip';f.write_bytes(b'not verified')
            with self.assertRaisesRegex(RuntimeError,'SOURCE_GITHUB_ARTIFACT_ZIP_SHA_MISMATCH'):
                M.extract_verified_asof(f,self.p)

    def test_analyze_needs_prior_protocol_lock(self):
        with tempfile.TemporaryDirectory() as td:
            with self.assertRaisesRegex(RuntimeError,'PROTOCOL_LOCK_NOT_RUN_FIRST'):
                M.run('missing.zip',PROTOCOL,td,'analyze')

    def test_protocol_change_invalidates_lock(self):
        with tempfile.TemporaryDirectory() as td:
            M.run('',PROTOCOL,td,'lock')
            px=Path(td)/'changed.json'
            x=json.loads(PROTOCOL.read_text('utf-8'))
            x['binary_conditions_frozen_before_return_join']['rel_amount_ge_3x']['value']=3.1
            px.write_text(json.dumps(x),encoding='utf-8')
            with self.assertRaisesRegex(RuntimeError,'LOCK_OR_PROTOCOL_DRIFT'):
                M.run('missing.zip',px,td,'analyze')

if __name__=='__main__': unittest.main()
