import csv,hashlib,importlib.util,pathlib,tempfile,sys
from collections import Counter
ROOT=pathlib.Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('collect', ROOT/'research_formulas/stockhunter_r1d27r_flow_collect.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
def test_freeze():
 f,cal,s=m.load(ROOT,'PILOT_300');assert len(f)==300 and len(cal)==1907
 assert all(x['bb_used_for_selection']=='False' and x['flow_used_for_selection']=='False' for x in f)
 assert Counter(x['year'] for x in f)=={'2021':60,'2022':60,'2023':60,'2024':60,'2025':60}
 ff,_,_=m.load(ROOT,'FULL_1500');assert len(ff)==1500
 assert set((x['date'],x['code']) for x in f).issubset({(x['date'],x['code']) for x in ff})
 return f,cal

def test_source_verify_and_checkpoint():
 f,cal=test_freeze();row=f[0];days=cal[int(row['signal_ix'])-60:int(row['signal_ix'])+1]
 def parser(frame,expected):
  assert expected==days
  return frame
 def fetcher(start,end,code):
  assert start==days[0].replace('-','') and end==days[-1].replace('-','') and code==row['code']
  return {(kind,side):{d:{'전체':1000 if kind=='VOLUME' else 100_000,'기관합계':250 if kind=='VOLUME' else 25000,
   '외국인합계':250 if kind=='VOLUME' else 25000,'개인':250 if kind=='VOLUME' else 25000,'기타법인':250 if kind=='VOLUME' else 25000}
   if side in ('매수','매도') else {k:0 for k in ['전체',*m.ACTORS]} for d in days} for kind in ('VOLUME','VALUE') for side in ('매수','매도','순매수')}
 with tempfile.TemporaryDirectory() as tmp:
  ck=pathlib.Path(tmp)/'a';v=m.collect(ROOT,'PILOT_300',ck,batch=1,cooldown=0,fetcher=fetcher,parser=parser)
  assert v['n_complete']==1 and v['n_unattempted']==299 and not v['provider_aborted']
  # Re-run with one batch: first completed event must NEVER be requested again.
  def fail_other(start,end,code):
   assert code!=row['code'], 'refetched completed event'
   raise TimeoutError('provider unavailable')
  nxt=m.collect(ROOT,'PILOT_300',pathlib.Path(tmp)/'b',seed=ck,batch=1,cooldown=0,fetcher=fail_other,parser=parser)
  assert nxt['n_complete']==1 and nxt['n_pending']==1 and nxt['n_unattempted']==298
  assert nxt['files_sha256'][m.RAW]==v['files_sha256'][m.RAW]

if __name__=='__main__':
 test_freeze();test_source_verify_and_checkpoint();print('R1D27R_OFFLINE_TESTS_PASS')
