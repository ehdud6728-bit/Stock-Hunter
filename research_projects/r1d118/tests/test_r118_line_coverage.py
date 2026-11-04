"""R118 post-run audit: never miscount a sparse role retest as missing line geometry."""
import sys
import unittest
from pathlib import Path
import pandas as pd
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from research_r1d118 import verify_line_input_coverage, SCALES, KINDS

class TestSixLineCoverageSeparation(unittest.TestCase):
    def example(self):
        d={f"{scale}_{kind}_coverage":["VALID_SOURCE_DATE"]
           for scale in SCALES for kind in KINDS}
        d["exact_role_event_coverage"]=["SOURCE_PRESENT_EVENT_SPARSE"]
        return pd.DataFrame(d)
    def test_sparse_role_feed_does_not_fail_line_coverage(self):
        self.assertTrue(verify_line_input_coverage(self.example(),object()))
    def test_real_missing_line_lane_fails(self):
        x=self.example();x.loc[0,"LONG_UPPER_coverage"]="MISSING_SOURCE_DATE"
        self.assertFalse(verify_line_input_coverage(x,object()))
    def test_missing_line_source_fails(self):
        self.assertFalse(verify_line_input_coverage(self.example(),None))
    def test_no_active_id_is_not_missing_source_date(self):
        # Active ID may be absent; row's validated 6-lane source date is still real.
        x=self.example();x["LONG_LOWER_id"]=[""]
        self.assertTrue(verify_line_input_coverage(x,object()))
    def test_missing_sixth_lane_rejected(self):
        x=self.example().drop(columns=["MEDIUM_LOWER_coverage"])
        with self.assertRaisesRegex(ValueError,"MISSING_SIX_LANE_COVERAGE_COLUMNS"):
            verify_line_input_coverage(x,object())
    def test_empty_is_not_a_pass(self):
        self.assertFalse(verify_line_input_coverage(self.example().iloc[:0],object()))

if __name__=="__main__":unittest.main()
