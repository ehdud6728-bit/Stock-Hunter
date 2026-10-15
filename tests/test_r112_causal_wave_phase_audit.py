import ast
import tempfile
import unittest
import numpy as np
import pandas as pd
from pathlib import Path
from closebet_research import r112_causal_wave_phase_audit as r112

class R112CausalQualityTests(unittest.TestCase):
    @staticmethod
    def make(n=370):
        d=pd.DataFrame({'date':pd.date_range('2025-01-01',periods=n,freq='B').strftime('%Y-%m-%d'),
             'open':[100.]*n,'high':[101.]*n,'low':[99.]*n,'close':[100.]*n,'volume':[1000.]*n})
        # Wave origin under 224MA, close breaks with recently increasing volume.
        for i in range(305,311):
            d.loc[i,'low']=87; d.loc[i,'close']=90;d.loc[i,'high']=92;d.loc[i,'open']=90
        for i in range(311,317):
            d.loc[i,'volume']=2000;d.loc[i,'close']=108;d.loc[i,'high']=109;d.loc[i,'open']=106
        for i in range(317,324):
            d.loc[i,'volume']=500;d.loc[i,'close']=98;d.loc[i,'high']=99;d.loc[i,'low']=96;d.loc[i,'open']=98
        # Recovery confirmation at day 326
        d.loc[326,['open','high','low','close','volume']]=[103,106,101,105,2300]
        return d.set_index('date')

    def test_no_forward_relabel_after_signal(self):
        d=self.make();a=r112.causal_daily_states(d)
        changed=d.copy();changed.iloc[345:,[0,1,2,3,4]]=[160,180,150,170,15000]
        b=r112.causal_daily_states(changed)
        cols=['stage','origin_date','impulse_date','peak_date_asof','prior_224_corporate_action_suspect',
              'current_retrace_fraction','last5_to_impulse_volume']
        pd.testing.assert_frame_equal(a.iloc[:345][cols],b.iloc[:345][cols])

    def test_suspension_disables_wave_until_224_window_passes(self):
        d=self.make();d.loc[d.index[300],['open','high','low','volume']]=0
        x=r112.causal_daily_states(d)
        self.assertTrue(x.loc[326,'prior_224_suspended_session'])
        self.assertEqual(x.loc[326,'stage'],'SUSPENSION_224_QUARANTINE')

    def test_corporate_action_spike_not_rebranded_as_real_impulse(self):
        d=self.make();d.loc[d.index[320],['open','high','low','close']]=[250,255,240,250]
        x=r112.causal_daily_states(d)
        self.assertTrue(x.loc[320,'prior_224_corporate_action_suspect'])
        self.assertEqual(x.loc[320,'stage'],'CORP_ACTION_QUARANTINE')

    def test_wave_phase_does_not_use_future_outcome_columns(self):
        d=self.make();a=r112.causal_daily_states(d);self.assertIn('stage',a)
        self.assertNotIn('D20_close_pct',a.columns)
        self.assertNotIn('D5_close_pct',a.columns)

    def test_no_early_compressed_recovery_assumption(self):
        d=self.make();a=r112.causal_daily_states(d)
        self.assertFalse((a.iloc[:270].stage=='REBOUND_CONFIRMED').any())

    def test_frozen_thresholds_and_output_scope(self):
        self.assertEqual(r112.THRESHOLDS['origin_lookback'],40)
        self.assertEqual(r112.THRESHOLDS['minimum_history'],270)
        self.assertEqual(r112.OUTPUT_STATUS,'PASS_EXPLORATORY_NOT_TRADE_APPROVED')

    def test_python310_syntax(self):
        p=Path(r112.__file__)
        ast.parse(p.read_text(),feature_version=(3,10))

    def test_sha_guard_rejects_tampered_input(self):
        self.assertNotEqual(r112.sha(b'nonmatching'),r112.R111_EPISODES_SHA)

if __name__=='__main__':unittest.main()
