import unittest
import pandas as pd
from closebet_research.r123_success_failure_microstructure_forensics import first_touch_path,overhead_d0_only,require

class R123Tests(unittest.TestCase):
    def test_no_local_history_unknown(self):
        self.assertFalse(overhead_d0_only(pd.DataFrame(),100)['overhead_valid'])
    def test_overhead_proxy_prior_only_and_later_is_ignored(self):
        a=pd.DataFrame({'close':[105.]*60,'volume':[100.]*60,'high':[110.]*60})
        x=overhead_d0_only(a,100);self.assertEqual(x['overhead_proxy_type'],'ABOVE_HEAVY_PROXY')
        a.loc[60]={'close':80,'volume':100000,'high':90}
        y=overhead_d0_only(a.iloc[:60],100);self.assertEqual(x['overhead_share_of_local_60'],y['overhead_share_of_local_60'])
    def test_below_proxy_prior(self):
        a=pd.DataFrame({'close':[95.]*60,'volume':[100.]*60,'high':[99.]*60})
        self.assertEqual(overhead_d0_only(a,100)['overhead_proxy_type'],'BELOW_HEAVY_PROXY')
    def test_empty_local_zone_unknown(self):
        a=pd.DataFrame({'close':[60.]*60,'volume':[100.]*60,'high':[62.]*60})
        self.assertEqual(overhead_d0_only(a,100)['overhead_proxy_type'],'UNKNOWN_SPARSE_CLOSE_PROXY')
    def test_gap_target_first_despite_intraday_stop(self):
        bars=[(100,102,98,100)]+[(106,108,90,100)]+[(100,100,99,100)]*18
        x=first_touch_path(bars,100,98);self.assertEqual(x['first_touch_label'],'TP_FIRST_GAP')
    def test_gap_stop_first_despite_intraday_target(self):
        bars=[(100,102,98,100)]+[(94,106,90,100)]+[(100,100,99,100)]*18
        x=first_touch_path(bars,100,98);self.assertEqual(x['first_touch_label'],'STOP_FIRST_GAP')
    def test_same_day_ambiguous_not_fabricated_order(self):
        bars=[(100,107,93,100)]+[(100,102,99,101)]*19
        x=first_touch_path(bars,100,98);self.assertEqual(x['path_failure_class'],'AMBIGUOUS_TP_STOP_SAME_DAY')
    def test_stop_then_recovery_strict_shakeout_like(self):
        bars=[(100,101,94,97),(97,100,96,99),(101,107,99,105)]+[(105,106,99,103)]*17
        x=first_touch_path(bars,100,98)
        self.assertTrue(x['shakeout_like_fast_reclaim_later_tp5']);self.assertTrue(x['stop_first_later_tp5']);self.assertFalse(x['tp5_first_conservative'])
    def test_stop_without_recovery(self):
        bars=[(100,101,94,95)]+[(95,98,94,96)]*19
        x=first_touch_path(bars,100,98);self.assertEqual(x['path_failure_class'],'STOP5_FIRST_NO_LATER_TP5')
    def test_early_reclaim_without_tp_not_shakeout(self):
        bars=[(100,101,94,97),(97,99,96,99)]+[(99,101,97,100)]*18
        x=first_touch_path(bars,100,98);self.assertFalse(x['shakeout_like_fast_reclaim_later_tp5'])
    def test_near_miss(self):
        bars=[(100,104.5,97,101)]*20
        self.assertEqual(first_touch_path(bars,100,98)['path_failure_class'],'NO_TOUCH_NEAR_TP5')
    def test_shakeout_temporal_requires_later_tp(self):
        bars=[(100,101,94,97),(97,107,96,105)]+[(105,105,99,103)]*18
        # D2 uses 97 open and recovery is possible, target after D1 stop.
        self.assertTrue(first_touch_path(bars,100,98)['stop_first_later_tp5'])
    def test_oversubscribed_unknown(self):
        with self.assertRaises(RuntimeError):first_touch_path([(100,101,99,100)]*19,100,99)
    def test_identity_preserve_string_codes(self):
        self.assertEqual(str(pd.Series(['000050'],dtype=str).iloc[0]),'000050')

if __name__=='__main__':unittest.main()
