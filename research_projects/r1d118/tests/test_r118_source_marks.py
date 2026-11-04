"""Regression: R112 has six lane rows per bar, R116 requires one OHLCV row/bar.
All tests use data known at or before the corresponding bar close.
"""
from __future__ import annotations
import sys,unittest
from pathlib import Path
import pandas as pd

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from r118_source_marks import original_daily_marks

SCALES=('SHORT','MEDIUM','LONG')
SIDES=('UPPER','LOWER')

class R118SourceMarkAdapterTests(unittest.TestCase):
    def setUp(self):
        self.raw=pd.DataFrame({'date':pd.to_datetime(['2023-01-02','2023-01-03','2023-01-04']),
                               'open':[100,101,102],'high':[103,104,105],
                               'low':[99,100,101],'close':[101.,102.,103.]})
        self.lanes=pd.DataFrame([
            {'idx':idx,'date':str(row.date.date()),'close':row.close,'scale':s,'kind':k}
            for idx,row in self.raw.iterrows() for s in SCALES for k in SIDES
        ])

    def test_r112_six_lanes_become_exactly_one_original_mark_per_bar(self):
        self.assertTrue(self.lanes[['idx','date']].duplicated().any(),
                        'Confirm this fixture reproduces the original R116 error')
        got=original_daily_marks(self.raw,self.lanes,'006400')
        self.assertEqual(len(got),len(self.raw))
        self.assertFalse(got[['idx','date']].duplicated().any())
        self.assertEqual(got.idx.tolist(),[0,1,2])
        self.assertEqual(got.date.tolist(),['2023-01-02','2023-01-03','2023-01-04'])
        self.assertEqual(got.close.tolist(),[101.,102.,103.])

    def test_reject_incomplete_r112_state_grid(self):
        with self.assertRaisesRegex(AssertionError,'R112_6_LANES_NOT_COMPLETE'):
            original_daily_marks(self.raw,self.lanes.iloc[:-1],'006400')

    def test_reject_duplicate_lane_even_if_count_unchanged(self):
        bad=self.lanes.copy()
        bad.iloc[0]=bad.iloc[1]
        with self.assertRaisesRegex(AssertionError,'R112_DUPLICATE_LANE'):
            original_daily_marks(self.raw,bad,'006400')

    def test_reject_date_mismatch_not_erase_duplicates(self):
        bad=self.lanes.copy();bad.loc[0,'date']='2023-01-03'
        with self.assertRaisesRegex(AssertionError,'R112_ORIGINAL_OHLCV_MARK_MISMATCH'):
            original_daily_marks(self.raw,bad,'006400')

    def test_reject_close_mismatch_not_erase_duplicates(self):
        bad=self.lanes.copy();bad.loc[0,'close']=999.
        with self.assertRaisesRegex(AssertionError,'R112_ORIGINAL_OHLCV_MARK_MISMATCH'):
            original_daily_marks(self.raw,bad,'006400')

    def test_reject_missing_index_even_if_length_same(self):
        bad=self.lanes.copy();bad.loc[bad.idx.eq(2),'idx']=3
        with self.assertRaisesRegex(AssertionError,'R112_BAR_INDEX_MISMATCH'):
            original_daily_marks(self.raw,bad,'006400')

    def test_require_real_six_distinct_scales_sides(self):
        bad=self.lanes.copy();bad.loc[0,'scale']='UNKNOWN'
        with self.assertRaisesRegex(AssertionError,'R112_SIX_LANE_GRID_INVALID'):
            original_daily_marks(self.raw,bad,'006400')

    def test_real_r111_r117_lineage_on_small_krx_sample_when_sources_present(self):
        # GitHub Actions checkout has R111/112/116/117 modules. Test the whole
        # ORIGINAL lineage without downloading data and without changing it.
        root=Path(__file__).resolve().parents[2]
        needed=['r1d111/algorithms.py','r1d112/multiscale.py',
                'r1d116/recent_turn.py','r1d117/stable_roles.py']
        if not all((root/n).is_file() for n in needed):
            self.skipTest('Historical original R111-R117 modules unavailable in isolated local patch')
        from auto_trendlines import generate_one
        sample=Path(__file__).resolve().parents[1]/'sample'/'R109_3STOCK_PARTIAL_NOT_FULL.csv'
        if not sample.is_file():
            self.skipTest('R109 real OHLCV test fixture not installed')
        raw=pd.read_csv(sample,dtype={'code':str})
        raw=raw[raw.code.eq('006400')].sort_values('date').head(90).reset_index(drop=True)
        self.assertEqual(len(raw),90)
        states,roles,audit=generate_one('006400',raw)
        self.assertEqual(len(states),90*6)
        self.assertEqual(audit['R117_lane_rows'],540)
        self.assertEqual(states.groupby(['code','date']).size().unique().tolist(),[6])

if __name__=='__main__': unittest.main()
