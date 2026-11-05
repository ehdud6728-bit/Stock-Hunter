import unittest,sys
from pathlib import Path
import pandas as pd
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from r118_truebreak500 import validate_event_identity,unique_stock_scale_signal_days

class TestMultiLineSameDay(unittest.TestCase):
    def line(self,lid,dt='2024-09-02',code='000001',scale='MEDIUM'):
        return {'code':code,'scale':scale,'signal_kind':'RETEST_PROJECTION_NOT_OFFICIAL_R117_ROLE',
                'signal_date':dt,'line_id':lid,'TP3SL5_net_pct':1.0}
    def test_two_distinct_lines_same_day_are_valid_raw_events(self):
        a=pd.DataFrame([self.line('LINE_A'),self.line('LINE_B')])
        self.assertEqual(1,validate_event_identity(a,stage='SHARD'))
        self.assertEqual(2,len(a))
        self.assertEqual(1,len(unique_stock_scale_signal_days(a)))
        self.assertEqual('LINE_A',unique_stock_scale_signal_days(a).iloc[0]['line_id'])
    def test_exact_same_line_same_day_is_error(self):
        a=pd.DataFrame([self.line('LINE_A'),self.line('LINE_A')])
        with self.assertRaisesRegex(ValueError,'DUPLICATE_SAME_LINE_R117_EVENT'):
            validate_event_identity(a,stage='SHARD')
    def test_overlapping_shards_duplicate_same_line_is_error(self):
        a=pd.DataFrame([self.line('LINE_A'),self.line('LINE_A')])
        with self.assertRaisesRegex(ValueError,'DUPLICATE_SAME_LINE_R117_EVENT'):
            validate_event_identity(a,stage='MERGE')
    def test_different_day_stage_or_scale_not_collapsed(self):
        a=pd.DataFrame([self.line('LINE_A'),self.line('LINE_B',dt='2024-09-03'),
                        self.line('LINE_C',scale='SHORT')])
        self.assertEqual(0,validate_event_identity(a,stage='SHARD'))
        self.assertEqual(3,len(unique_stock_scale_signal_days(a)))
    def test_null_or_empty_id_rejected(self):
        for lid in ['',None]:
            with self.assertRaisesRegex(ValueError,'TRUEBREAK_EVENT_IDENTITY_EMPTY'):
                validate_event_identity(pd.DataFrame([self.line(lid)]),stage='SHARD')
    def test_retest_multi_line_cannot_inflate_summary(self):
        a=pd.DataFrame([self.line('B'),self.line('A')]); a.loc[0,'TP3SL5_net_pct']=-5.25
        count=unique_stock_scale_signal_days(a)
        self.assertEqual(1,len(count));self.assertEqual(1.0,float(count.TP3SL5_net_pct.mean()))
        self.assertEqual(2,len(a))

if __name__=='__main__':unittest.main()
