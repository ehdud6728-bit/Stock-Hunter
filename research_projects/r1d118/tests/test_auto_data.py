"""No-network contract tests for pinned archival KRX-derived research inputs."""
import hashlib,sys,tempfile,unittest
from pathlib import Path
import pandas as pd

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from auto_data import blob_hash,download_locked,normalize_marcap,make_cohort,build_inputs,INDEX_SHA,MARCAP_SHA
from research_r1d118 import verified_files,run

class RealDataSourceTests(unittest.TestCase):
 def frame(self,year,n=160):
  dates=pd.bdate_range(f'{year}-05-02',periods=n)
  dates=dates[dates.year==year]
  records=[]
  for i,dt in enumerate(dates):
   for code,cap,market in [('000001',200,'KOSPI'),('000002',300,'KOSDAQ'),('000003',100,'KOSPI')]:
    px=100+(i%11)
    records.append({'Date':dt,'Code':code,'Market':market,'Open':px,'High':px+2,'Low':px-2,
                    'Close':px,'Volume':50000+i,'Amount':1e10+i*1e8,'Marcap':cap})
  return pd.DataFrame(records)
 def test_blob_verification_and_cache(self):
  with tempfile.TemporaryDirectory() as t:
   b=b'hello frozen';h=blob_hash(b)
   self.assertEqual(h,hashlib.sha1(b'blob 12\0'+b).hexdigest())
   f=Path(t)/'x.bin';download_locked('none',f,h,fetcher=lambda _:b)
   self.assertEqual(download_locked('none',f,h,fetcher=lambda _:b),f)
   with self.assertRaisesRegex(ValueError,'PINNED_SOURCE_CHANGED'):
    download_locked('none',f,'0'*40,fetcher=lambda _:b)
 def test_frozen_external_provenance_pins(self):
  self.assertEqual(len(MARCAP_SHA),3)
  self.assertEqual(len(INDEX_SHA),6)
  for s in list(MARCAP_SHA.values())+list(INDEX_SHA.values()):
   self.assertEqual(len(s),40)
 def test_no_future_selection_and_real_price_shape(self):
  original={y:normalize_marcap(self.frame(y),y)[0] for y in [2022,2023,2024]}
  selected,dt=make_cohort(original,count=2,min_history=120)
  self.assertEqual(selected,['000002','000001'])
  # Future megacap does not change a selection made in 2022.
  original[2024].loc[original[2024].code=='000003','Marcap']=1e14
  self.assertEqual(make_cohort(original,count=2,min_history=120)[0],selected)
 def test_market_membership_is_historical_and_sector_unverified(self):
  annual={y:normalize_marcap(self.frame(y),y)[0] for y in [2022,2023,2024]}
  idx={}
  for y in [2022,2023,2024]:
   for market in ['KOSPI','KOSDAQ']:
    idx[(market,y)]=pd.DataFrame({'market':market,'date':pd.bdate_range(f'{y}-05-02',periods=160).strftime('%Y-%m-%d'),
           'close':1000,'amount':1e11})
  # Synthetic 2022 through 2024 including 2022 warmup.
  with tempfile.TemporaryDirectory() as t:
   result=build_inputs(annual,idx,Path(t)/'inputs',count=2)
   files=verified_files(result['manifest']);self.assertEqual(len(files),2)
   s=pd.read_csv(result['sector'],dtype={'code':str})
   self.assertEqual(set(s.sector),{'UNVERIFIED_SECTOR'})
   self.assertEqual(set(s.market),{'KOSPI','KOSDAQ'})
   for c,p,_ in files:
    d=pd.read_csv(p,dtype={'code':str});self.assertEqual(set(d.code),{c})
    self.assertEqual(set(['volume','amount','date','open']).difference(d.columns),set())
 def test_real_market_sector_cannot_claim_sector_verified(self):
  with tempfile.TemporaryDirectory() as t:
   n=150;dates=pd.bdate_range('2023-01-02',periods=n).strftime('%Y-%m-%d')
   import numpy as np
   arr=np.array([110+7*np.sin(i/6) for i in range(n)])
   code='123456'
   df=pd.DataFrame({'code':[code]*n,'date':dates,'open':arr-1,'high':arr+3,'low':arr-3,'close':arr,'amount':[2e10]*n})
   sec=pd.DataFrame({'code':code,'date':dates,'sector':'UNVERIFIED_SECTOR','market':'KOSDAQ'})
   sec.to_csv(Path(t)/'sector.csv',index=False)
   result=run({code:df},Path(t)/'report',sample=False,required_min_stocks=1,
      study_start='2023-03-01',sector_file=Path(t)/'sector.csv')
   self.assertFalse(result['sector_pit_connected'])
   self.assertIn('SECTOR_CLASSIFICATION_UNVERIFIED_OR_MISSING',result['blocked_reasons'])
   ev=pd.read_csv(Path(t)/'report'/'R118_EVENT_LEDGER_PIT.csv')
   self.assertTrue(ev.signal_date.ge('2023-03-01').all())

if __name__=='__main__':unittest.main()
