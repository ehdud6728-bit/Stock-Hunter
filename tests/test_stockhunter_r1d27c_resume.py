import unittest, tempfile, zipfile, pathlib, shutil, json, io, hashlib, pandas as pd
from research_formulas import stockhunter_r1d27c_flow60 as s

class ResumeTests(unittest.TestCase):
    def test_checkpoint_sha_is_frozen(self):
        self.assertEqual(len(s.PINNED_FULL_FREEZE_SHA256),64)
        self.assertEqual(s.PINNED_FULL_FREEZE_SHA256,'c6dcccdc5ecd5d98d3f1ceae441c34814c062710118148832271ff9191474093')
    def test_mode_keeps_original_version(self):
        self.assertEqual(s.VERSION,'R1D27C_FROZEN300_60SESS_GROSS_BUY_SELL_NET_VALUE_VOLUME_V1')
    def test_six_invariants_dont_accept_missing(self):
        with self.assertRaises(s.AuditError):s.parse_provider(pd.DataFrame(),['2025-01-02'])
    def test_no_large_batch(self):
        with self.assertRaises(s.AuditError):s.collect_resume('/not-used','/not-used','/not-used',{},batch_limit=41)
    def test_raw_six_actor_rows(self):
        self.assertEqual(s.ACTORS,['기관합계','외국인합계','개인','기타법인'])
        self.assertEqual(s.PERIODS,(5,10,20,40,60))

if __name__=='__main__':unittest.main()
