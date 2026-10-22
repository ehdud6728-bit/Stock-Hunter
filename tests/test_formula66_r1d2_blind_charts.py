import csv,gzip,sys,tempfile,pathlib,unittest
from unittest import mock
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from research_formulas import formula66_r1d2_blind_charts as m
class BlindTests(unittest.TestCase):
 def test_no_forward_outcomes_file_opened(self):
  s=pathlib.Path(m.__file__).read_text()
  self.assertNotIn("open(root/'FORWARD_PRICE_OUTCOMES",s)
 def test_explicit_future_bar_rejected(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=pathlib.Path(tmp)
   with (root/'BLIND_SEALED_CHART_REVIEW_KEYS.csv').open('w',newline='') as f:
    w=csv.DictWriter(f,fieldnames=['review_id','signal_date']);w.writeheader();w.writerow({'review_id':'0123456789','signal_date':'2026-07-01'})
   with gzip.open(root/'BLIND_CHART_PRE_SIGNAL_ONLY.csv.gz','wt',newline='') as f:
    w=csv.DictWriter(f,fieldnames=['review_id','date','open','high','low','close','actual_amount_KRW']);w.writeheader()
    w.writerow({'review_id':'0123456789','date':'2026-07-02','open':1,'high':1,'low':1,'close':1,'actual_amount_KRW':1})
   with self.assertRaisesRegex(RuntimeError,'BLIND_FUTURE_BAR_LEAK_BLOCKED'):
    m.main(['--result-root',str(root)])
if __name__=='__main__':unittest.main()
