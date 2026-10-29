#!/usr/bin/env python3
"""R1D27R BB-independent asof frozen sample, pilot 300/FULL 1500 source-only collector.
Never scores/trades/changes LIVE. Missing provider data remain missing, not zero.
Reads the frozen cohort and KRX official trading calendar pinned by SHA.
Each batch resumes SHA-verified checkpoints. No refetch of successful or quarantined rows.
"""
import argparse,csv,gzip,hashlib,io,json,os,pathlib,time

PILOT_SHA='b77df4c88e8a64e2e6f2e5d2056275bd7bb37e9ed6c0e86ac6c0e10ed7482bb2'
FULL_SHA='190f050954a94d9fc87b1bf091dc268f447cdffbdee4f825b520eaf3d2feed01'
PILOT='research_inputs/R1D27R_PILOT_FLOW_BLIND_FROZEN_300.csv'
FULL='research_inputs/R1D27R_FLOW_BLIND_FROZEN_1500.csv'
CALENDAR='research_inputs/R1D27R_ORIGINAL_KRX_TRADING_CALENDAR.csv'
ACTORS=['기관합계','외국인합계','개인','기타법인'];KEY=['date','code']
RAW='R1D27R_FLOW61_4ACTOR_GROSS_NET.csv.gz';AUD='R1D27R_FLOW_AUDIT.csv';MAN='R1D27R_FLOW_MANIFEST.json'
RUNTIME=['trade_date','actor','buy_krw','sell_krw','net_krw','total_krw','buy_shares','sell_shares','net_shares','total_shares']
RAWCOL=KEY+RUNTIME;AUDCOL=KEY+['status','source_complete','reason_code','attempts']
def sha(b):return hashlib.sha256(b).hexdigest()
def check(cond,msg):
 if not cond:raise RuntimeError('R1D27R_'+msg)
def read_csv_blob(b):return list(csv.DictReader(io.StringIO(b.decode('utf-8-sig'))))
def emit(p,rows,cols):
 b=io.StringIO();w=csv.DictWriter(b,fieldnames=cols,lineterminator='\n',extrasaction='ignore');w.writeheader();w.writerows(rows)
 val=b.getvalue().encode();
 if p.suffix=='.gz':
  out=io.BytesIO();f=gzip.GzipFile(fileobj=out,mode='wb',mtime=0);f.write(val);f.close();val=out.getvalue()
 p.write_bytes(val)
def load(base,cohort):
 base=pathlib.Path(base);n=300 if cohort=='PILOT_300' else 1500
 file=PILOT if n==300 else FULL;expected=PILOT_SHA if n==300 else FULL_SHA
 b=(base/file).read_bytes();check(sha(b)==expected,'FROZEN_SHA_MISMATCH');f=read_csv_blob(b)
 check(len(f)==n and len(set((v['date'],v['code']) for v in f))==n,'DUP_OR_COUNT')
 days=[x['date'] for x in read_csv_blob((base/CALENDAR).read_bytes())];check(len(days)==1907 and days[0]=='2019-01-02','CALENDAR')
 for x in f:
  i=int(x['signal_ix']);check(60<=i<len(days) and days[i]==x['date'] and days[i-60]==x['pre60_from'] and days[i-1]==x['pre60_to'],'ASOF_DATE_MISMATCH')
 return f,days,expected

def verify61(feeds,days,parser):
 # Adapt 60-session R1D27C verifier to precisely 61 D-60..D0 sessions.
 parsed={(kind,side):parser(feeds[kind,side],days) for kind in ('VALUE','VOLUME') for side in ('매수','매도','순매수')}
 check(any(parsed['VALUE','매수'][d]['전체']>0 and parsed['VOLUME','매수'][d]['전체']>0 for d in days),'ZERO_ALL_PROVIDER')
 rows=[]
 for day in days:
  for kind in ('VALUE','VOLUME'):
   b,s,n=(parsed[kind,side][day] for side in ('매수','매도','순매수'))
   check(b['전체']>=0 and s['전체']>=0,'TOTAL_NEGATIVE')
   check(abs(b['전체']-s['전체'])<=max(2,b['전체']*.00001),'TOTAL_BUY_SELL_MISMATCH')
   check(abs(n['전체'])<=max(2,b['전체']*.00001),'TOTAL_NET_MISMATCH')
   check(abs(sum(b[a] for a in ACTORS)-b['전체'])<=max(1000,b['전체']*.005),'GROUP_BUY_MISMATCH')
   check(abs(sum(s[a] for a in ACTORS)-s['전체'])<=max(1000,s['전체']*.005),'GROUP_SELL_MISMATCH')
   for a in ACTORS:
    check(b[a]>=0 and s[a]>=0,'GROSS_NEGATIVE')
    check(abs(b[a]-s[a]-n[a])<=max(2,(b[a]+s[a])*.000001),'NET_IDENTITY')
  for a in ACTORS:
   row=dict(trade_date=day,actor=a)
   for kind,suff in [('VALUE','krw'),('VOLUME','shares')]:
    for side,k in [('매수','buy'),('매도','sell'),('순매수','net')]:row[k+'_'+suff]=parsed[kind,side][day][a]
    row['total_'+suff]=parsed[kind,'매수'][day]['전체']
   rows.append(row)
 check(len(rows)==61*4,'FLOW61_ACTORS')
 return rows

def retryable(s):
 return any(k in s.upper() for k in ['PROVIDER_EMPTY','TIMEOUT','JSONDECODE','CONNECTION','HTTPERROR','429','REMOTE','RATE','REQUEST'])

def read_seed(seed,cohort,freeze_sha,allowed):
 if not seed:return [],{}
 p=pathlib.Path(seed);m=json.loads((p/MAN).read_text());check(m['cohort']==cohort and m['freeze_sha256']==freeze_sha,'SEED_FREEZE_DRIFT')
 for n in (RAW,AUD):check(sha((p/n).read_bytes())==m['files_sha256'][n],'SEED_CORRUPT_'+n)
 a=read_csv_blob((p/AUD).read_bytes());x=read_csv_blob(gzip.decompress((p/RAW).read_bytes()))
 old={(r['date'],r['code']):r for r in a};check(len(old)==len(a) and set(old).issubset(allowed),'SEED_AUDIT_KEYS')
 complete={k for k,r in old.items() if r['source_complete']=='1'}
 check(len(x)==len(complete)*61*4,'SEED_RAW_COUNT')
 d=__import__('collections').defaultdict(list)
 for r in x:d[(r['date'],r['code'])].append(r)
 check(set(d)==complete,'SEED_KEYSET')
 for k,rows in d.items():
  check(len(rows)==61*4 and len(set((r['trade_date'],r['actor']) for r in rows))==61*4,'SEED_DAY_ACTOR_DUP')
  for r in rows:
   for field in ('krw','shares'):
    check(abs(int(r['buy_'+field])-int(r['sell_'+field])-int(r['net_'+field]))<=2,'SEED_MATH')
 return x,old

def collect(source_root,cohort,output,seed=None,batch=20,cooldown=.8,max_fail=3,fetcher=None,parser=None):
 check(cohort in ('PILOT_300','FULL_1500'),'COHORT');check(1<=batch<=30,'BATCH')
 f,cal,fs=load(source_root,cohort);allowed={(r['date'],r['code']) for r in f}
 raw,old=read_seed(seed,cohort,fs,allowed)
 if fetcher is None or parser is None:
  from research_formulas import stockhunter_r1d27c_flow60 as c27
  if fetcher is None:fetcher=c27.get_flow
  if parser is None:parser=c27.parse_provider
 attempted=0;success=0;fail_streak=0;abort=False
 try:
  for x in f:
   k=(x['date'],x['code']);prev=old.get(k)
   if prev and (prev['source_complete']=='1' or prev['status']=='QUARANTINED_NO_IMPUTATION'):continue
   if attempted>=batch:break
   i=int(x['signal_ix']);days=cal[i-60:i+1];check(len(days)==61 and days[-1]==x['date'],'DAY61')
   attempted+=1
   try:
    feeds=fetcher(days[0].replace('-',''),days[-1].replace('-',''),x['code'])
    vals=verify61(feeds,days,parser)
    raw.extend([{**dict(zip(KEY,k)),**r} for r in vals]);status='COMPLETE';reason='';success+=1;fail_streak=0
   except Exception as exc:
    reason=(str(exc) if str(exc).startswith('R1D27') else type(exc).__name__)[:200]
    status='RETRY_PENDING' if retryable(reason) else 'QUARANTINED_NO_IMPUTATION'
    fail_streak+=1
   old[k]=dict(date=k[0],code=k[1],status=status,source_complete=int(status=='COMPLETE'),reason_code=reason,attempts=int(prev['attempts'])+1 if prev else 1)
   if attempted%5==0:print('R1D27R_COLLECT',attempted,'success',success,'total_attempted',len(old),'provider_fails',fail_streak,flush=True)
   if fail_streak>=max_fail:abort=True;break
   if cooldown:time.sleep(cooldown)
 finally:
  p=pathlib.Path(output);p.mkdir(parents=True,exist_ok=True)
  a=[old[k] for k in sorted(old)];v=sorted(raw,key=lambda r:(r['date'],r['code'],r['trade_date'],r['actor']))
  emit(p/AUD,a,AUDCOL);emit(p/RAW,v,RAWCOL)
  n_ok=sum(int(r['source_complete']) for r in a);check(len(v)==n_ok*61*4,'RAW_COUNT')
  man=dict(version='R1D27R_FLOW61_SOURCE_ONLY_V1',cohort=cohort,freeze_sha256=fs,events_frozen=len(f),
   n_complete=n_ok,n_attempted_unique=len(a),n_unattempted=len(f)-len(a),n_pending=sum(x['status']=='RETRY_PENDING' for x in a),
   n_quarantined=sum(x['status']=='QUARANTINED_NO_IMPUTATION' for x in a),batch_attempted=attempted,batch_complete=success,
   provider_aborted=abort,source_time_window='D-60..D0_INCLUSIVE_61_OFFICIAL_SESSIONS',actor_types=ACTORS,
   no_post_signal_flow_used=True,no_imputation=True,no_live_changes=True,research_only=True,prospective_oos=False,
   files_sha256={n:sha((p/n).read_bytes()) for n in (RAW,AUD)})
  (p/MAN).write_text(json.dumps(man,indent=2,ensure_ascii=False)+'\n')
 return man

if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--source-root',default='.');p.add_argument('--cohort',choices=['PILOT_300','FULL_1500'],default='PILOT_300');p.add_argument('--out',required=True);p.add_argument('--seed-root');p.add_argument('--batch',type=int,default=20);a=p.parse_args()
 m=collect(a.source_root,a.cohort,a.out,a.seed_root,a.batch)
 print('R1D27R_CHECKPOINT',json.dumps(m,ensure_ascii=False),flush=True)
 if m['provider_aborted']:raise SystemExit(3)
