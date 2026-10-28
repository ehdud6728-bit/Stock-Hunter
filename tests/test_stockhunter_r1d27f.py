import os,pathlib,sys,unittest
import numpy as np
import pandas as pd
ROOT=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from research_formulas import stockhunter_r1d27f_checkpoint_priceflow as r

class CheckpointEvidenceFence(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.zip=pathlib.Path(os.environ.get('R1D27F_TEST_SOURCE', str(ROOT/'research_inputs'/'R1D27E_FINAL_GITHUB_40702069936.zip')))
        cls.price=ROOT/'research_inputs'/'R1D27D_03_PRICE_DAILY_D1_D15_ALL_1500.csv'
        cls.candle=ROOT/'research_inputs'/'R1D27D_02_CANDLE_MA_BB_ENV_SIGNAL_DAY_1500.csv'
        cls.flow,cls.actors,cls.signal,cls.m,cls.sha=r.load_inputs(cls.zip,cls.candle,cls.price)
        cls.predictors=r.make_checkpoints(cls.flow,cls.actors,cls.signal)
        cls.future=r.make_future_outcomes(cls.flow)
    def test_fixed_complete_cohort(self):
        self.assertEqual(len(self.flow),22245)
        self.assertEqual(len(self.predictors),4449)
        self.assertEqual(len(self.actors),44490)
        self.assertEqual(self.m['post_flow_complete'],1483)
        self.assertFalse(self.flow.daily_open.isna().any())
    def test_next_open_not_original_entry(self):
        for t in r.POINTS:
            o=self.future[self.future.checkpoint_day==t]
            expected=self.flow[self.flow.day==t+1][r.KEY+['daily_open']]
            z=o.merge(expected,on=r.KEY,validate='one_to_one')
            self.assertTrue(np.array_equal(z.decision_next_open.to_numpy(),z.daily_open.to_numpy()))
            self.assertGreater((z.decision_next_open!=self.flow[self.flow.day==1].set_index(r.KEY).entry_open.reindex(pd.MultiIndex.from_frame(z[r.KEY])).to_numpy()).sum(),0)
    def test_future_values_cannot_change_d3_features(self):
        changed=self.flow.copy()
        changed.loc[changed.day>3,'net_krw']+=9000000000
        changed.loc[changed.day>3,'daily_close']+=40000
        changed.loc[changed.day>3,'daily_high']+=50000
        modified=r.make_checkpoints(changed,self.actors,self.signal)
        a=self.predictors[self.predictors.checkpoint_day==3].sort_values(r.KEY).reset_index(drop=True)
        b=modified[modified.checkpoint_day==3].sort_values(r.KEY).reset_index(drop=True)
        pd.testing.assert_frame_equal(a,b)
    def test_cost_and_high_targets_recalculated(self):
        for t in r.POINTS:
            f=self.future[self.future.checkpoint_day==t]
            self.assertTrue(np.allclose(f.future_d15_net20bps_pct,100*(f.d15_close/f.decision_next_open-1)-.2))
            self.assertTrue(np.allclose(f.future_max_high_gross_pct,100*(f.future_max_high/f.decision_next_open-1)))
            self.assertTrue((f.future_hit10_gross==(f.future_max_high_gross_pct>=10)).all())
    def test_no_post_signal_feature_claim(self):
        m=self.m
        self.assertTrue(m['post_signal_flow_is_future_outcome'])
        self.assertFalse(m['prospective_OOS'])
    def test_candle_is_signal_time_only(self):
        self.assertTrue(self.predictors.SIGNAL_BELOW_MA224.isin([0,1]).all())
        self.assertEqual(len(self.signal),1500)

if __name__=='__main__':unittest.main()
