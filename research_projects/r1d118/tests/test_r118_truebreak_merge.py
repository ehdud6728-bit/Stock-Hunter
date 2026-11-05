import json,tempfile,sys,unittest
from pathlib import Path
import pandas as pd
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from r118_truebreak500 import merge_shards
class MergeTests(unittest.TestCase):
 def test_merge_5_verified_stage_files(self):
  with tempfile.TemporaryDirectory() as t:
   root=Path(t);folders=[]
   for i in range(5):
    p=root/str(i);p.mkdir();folders.append(p)
    rows=[]
    for j in range(25):
     code=f'{i*25+j+1:06d}'
     rows.append({'code':code,'signal_date':'2023-02-01','signal_kind':'EARLY_FIRST_CLOSE',
         'scale':'MEDIUM','line_id':'L', 'high10_net_pct':4,'d1_net_pct':1,'d5_net_pct':2,'d10_net_pct':3,
         'TP3SL5_net_pct':2.75,'TP3SL5_ambiguous':False,'market_down':False,'c_gt_c3':True,
         'bar3_up':True,'strong_candle':True,'ma20_rising':True,'cloud_position':'ABOVE'})
    pd.DataFrame(rows).to_csv(p/'R118_TRUEBREAK_3CANDLE_SHARD.csv',index=False)
   report=merge_shards(folders,root/'out')
   self.assertEqual(125,report['event_rows'])
   self.assertTrue((root/'out'/'R118_TRUEBREAK_500_SUMMARY.csv').exists())
 def test_missing_one_shard_cannot_merge(self):
  with tempfile.TemporaryDirectory() as t:
   root=Path(t);folders=[]
   for i in range(5):
    p=root/str(i);p.mkdir();folders.append(p)
    if i<4:pd.DataFrame([{'code':f'{i+1:06d}','signal_date':'2023-02-01'}]).to_csv(p/'R118_TRUEBREAK_3CANDLE_SHARD.csv',index=False)
   with self.assertRaisesRegex(ValueError,'TRUEBREAK_SHARD_MISSING'):
    merge_shards(folders,root/'out')
if __name__=='__main__':unittest.main()
