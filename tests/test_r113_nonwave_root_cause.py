"""Causal, corruption rejection and threshold-equivalence tests for R1.13."""
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
import numpy as np
import pandas as pd
from closebet_research import r113_nonwave_root_cause as a
from closebet_research.r112_causal_wave_phase_audit import causal_daily_states


def make_data(extend=0,shock=False):
    n=345+extend
    ds=pd.bdate_range('2025-01-02',periods=n).strftime('%Y-%m-%d')
    close=np.full(n,100.0);low=np.full(n,98.);high=np.full(n,102.)
    vol=np.full(n,100.)
    # Last signal index 325 (pre-signal history available) unaffected by later values.
    if shock:close[326:]*=2;low[326:]*=2;high[326:]*=2;vol[326:]*=10
    return pd.DataFrame({'open':close,'high':high,'low':low,'close':close,'volume':vol},index=pd.Index(ds,name='date'))


class R113Tests(unittest.TestCase):
    def test_mutating_future_does_not_change_gate_explanation(self):
        d=make_data();s=causal_daily_states(d)
        d2=make_data(30,shock=True);s2=causal_daily_states(d2)
        day=str(d.index[325]);r1=a.diagnose_one(day,'123456',d,s)
        r2=a.diagnose_one(day,'123456',d2,s2)
        self.assertEqual(r1,r2)
    def test_four_gates_are_equivalent_to_r112_threshold_expression(self):
        d=make_data();i=320;c=d.close.to_numpy();low=d.low.to_numpy();vol=d.volume.to_numpy()
        ma=d.close.rolling(224,min_periods=224).mean().to_numpy()
        x=a.gates_at(d,list(d.index),c,low,vol,ma,i)
        self.assertIsNotNone(x)
        self.assertEqual(x['gates_all'],all(x[g] for g in a.GATE_ORDER))
    def test_same_day_volume_not_in_r112_pre5_ratio(self):
        d=make_data();i=325
        low=d.low.to_numpy();close=d.close.to_numpy();vol=d.volume.to_numpy();ma=d.close.rolling(224).mean().to_numpy()
        a0=a.gates_at(d,list(d.index),close,low,vol,ma,i)
        vol[i]=100000
        a1=a.gates_at(d,list(d.index),close,low,vol,ma,i)
        self.assertEqual(a0['previous5_to_older20_volume'],a1['previous5_to_older20_volume'])
        self.assertGreater(a1['including_signal_day_volume_ratio'],a0['including_signal_day_volume_ratio'])
    def test_corrupted_r112_source_hash_fails_closed(self):
        with TemporaryDirectory() as tmp:
            p=Path(tmp)
            (p/'r112_all_selected_1059_pit_wave_stage.csv').write_bytes(b'modified')
            (p/'r112_integrity_manifest.json').write_text('{"status":"PASS_EXPLORATORY_NOT_TRADE_APPROVED","pinned_source_runs":{"r171":38664095610,"r110":38745541969,"r111":38760251901},"output_sha256":{"r112_all_selected_1059_pit_wave_stage.csv":"bad"}}')
            with self.assertRaises(RuntimeError):a.load_pinned_source(p)
    def test_unknown_wave_state_cannot_be_reported_as_explained(self):
        self.assertEqual(set(a.CAUSES),set(['RECENT_DETECTED_WAVE_RESET_OR_EXPIRED','NO_BELOW224_ANCHOR','NO_16PCT_IMPULSE','NO_1_25_PRIOR_VOLUME','NO_FIVE_CLOSE_BREAKOUT','GATES_ELIGIBLE_BUT_STATE_NONE_INSPECT']))

if __name__=='__main__':unittest.main()
