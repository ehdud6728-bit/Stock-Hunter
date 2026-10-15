"""Isolated and causal episode-label checks before expensive history downloads."""
import unittest
import pandas as pd
import numpy as np
from closebet_research import r111_episode_first_strength_audit as m

class R111IntegrityTests(unittest.TestCase):
    def setUp(self):
        self.cal=pd.bdate_range('2026-08-03',periods=30).strftime('%Y-%m-%d').tolist()
    def test_episode_resets_only_after_more_than_cooldown(self):
        sig=pd.DataFrame({'signal_date':pd.to_datetime([self.cal[i] for i in (0,3,13,24)]), 'code':['000001']*4})
        x=m.assign_episodes(sig,self.cal,10)
        self.assertEqual(x.first_in_episode.tolist(),[1,0,0,1]);self.assertEqual(x.episode_signal_number.tolist(),[1,2,3,1])
    def test_exact_gap_boundary_in_same_episode(self):
        sig=pd.DataFrame({'signal_date':pd.to_datetime([self.cal[0],self.cal[10],self.cal[21]]),'code':['000001']*3})
        x=m.assign_episodes(sig,self.cal,10)
        self.assertEqual(x.episode_first_c10.tolist() if 'episode_first_c10' in x else x.first_in_episode.tolist(),[1,0,1])
    def test_distinct_stocks_do_not_share_episode(self):
        sig=pd.DataFrame({'signal_date':pd.to_datetime([self.cal[0],self.cal[0],self.cal[3]]),'code':['000001','000002','000001']})
        x=m.assign_episodes(sig,self.cal,10)
        self.assertEqual(x.first_in_episode.tolist(),[1,1,0])
    def test_identical_event_not_duplicate(self):
        sig=pd.DataFrame({'signal_date':pd.to_datetime([self.cal[0]]*2),'code':['000001']*2})
        self.assertEqual(len(m.assign_episodes(sig,self.cal,10)),1)
    def test_labels_ignore_future_outcome_modifications(self):
        sig=pd.DataFrame({'signal_date':pd.to_datetime([self.cal[0],self.cal[3]]),'code':['000001']*2,'D20_close_pct':[9,-10]})
        x=m.assign_episodes(sig,self.cal,10)
        sig['D20_close_pct']=[-90,1000]
        y=m.assign_episodes(sig,self.cal,10)
        pd.testing.assert_frame_equal(x,y)
    def test_unsupported_session_fails_closed(self):
        sig=pd.DataFrame({'signal_date':[pd.Timestamp('2026-01-02')],'code':['000001']})
        with self.assertRaisesRegex(RuntimeError,'SIGNAL_OUTSIDE_KRX_CALENDAR'):
            m.assign_episodes(sig,self.cal,10)
    def test_source_integrity_pin_known_hash(self):
        self.assertEqual(len(m.R171_RAW_SHA),64);self.assertEqual(len(m.R110_FEATURE_SHA),64)
        with self.assertRaisesRegex(RuntimeError,'R111_FAIL_CLOSED'):
            m.require(False,'CORRUPTION')
    def test_same_stock_episode_pair_not_multiple_repeats(self):
        sig=pd.DataFrame({'signal_date':pd.to_datetime([self.cal[i] for i in (0,2,4,0,3)]),
                          'code':['000001']*3+['000002']*2,
                          'D20_complete':[1]*5,'D5_close_pct':[5,4,2,-2,-1],
                          'D20_close_pct':[7,6,0,-4,-2]})
        lab=m.assign_episodes(sig,self.cal,10)
        lab=lab.rename(columns={'episode_id':'episode_id_c10','first_in_episode':'episode_first_c10'})
        result=sig.merge(lab[['signal_date','code','episode_id_c10','episode_first_c10']],on=['signal_date','code'])
        p=m.matched_first_repeat(result)
        self.assertEqual(len(p),2);self.assertEqual(set(p.code),{'000001','000002'})

if __name__=='__main__':unittest.main()
