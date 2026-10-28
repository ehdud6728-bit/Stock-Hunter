#!/usr/bin/env python3
"""R1D27E post-signal investor FLOW15, research diagnostic ONLY.

A D+1..D+15 future investor path is NOT a signal-date predictor. Never import
into live scan/rank/order. Frozen signal cohort and forward-price calendar.
Source: retrospective KRX via pykrx, not a point-in-time certified dataset.
"""
from __future__ import annotations
import argparse,gzip,hashlib,io,json,os,pathlib,time,urllib.request,urllib.error
import pandas as pd
from research_formulas import stockhunter_r1d27c_flow60 as c27

VERSION='R1D27E_POST15_FUTURE_FLOW_DIAGNOSTIC_V1'
FROZEN_SHA='545f73cf0cdee0b1c3d2912edcc7a05fac1f9e11c72d19d10eba956ac50bbfba'
BASE='R1D27E_POST15_FIXED_PRICE_BASE_1500.csv.gz'
BASEMAN='R1D27E_POST15_FIXED_PRICE_BASE_MANIFEST.json'
PRE='R1D27E_PRE_SIGNAL_MAGNITUDE_1481.csv.gz'
KEY=['date','code','bb_period','event_type']
RAW='R1D27E_POST15_DAILY_ACTOR_VALUE.csv.gz';AUD='R1D27E_POST15_SOURCE_AUDIT.csv';MAN='R1D27E_POST15_MANIFEST.json'
ACTORS=c27.ACTORS
RAWCOL=[*KEY,'day','trade_date','actor','buy_krw','sell_krw','net_krw','total_krw']
AUDCOL=[*KEY,'source_complete','reason_code','status']

def sha(b):return hashlib.sha256(b).hexdigest()
def chk(ok,why):
    if not ok:raise RuntimeError('R1D27E_'+str(why))
def key_of(t):return tuple(str(getattr(t,k)) for k in KEY)
def read_base(base_dir,freeze_file):
    base_dir=pathlib.Path(base_dir)
    m=json.loads((base_dir/BASEMAN).read_text())
    chk(m['version']==VERSION and m['frozen_R1D27D_SHA']==FROZEN_SHA,'BASE_MANIFEST')
    b=(base_dir/BASE).read_bytes();chk(sha(b)==m['base_sha256'],'BASE_SHA')
    frozen=pathlib.Path(freeze_file).read_bytes();chk(sha(frozen)==FROZEN_SHA,'FROZEN_SHA')
    f=pd.read_csv(io.BytesIO(frozen),dtype={'code':str,'bb_period':str,'date':str,'event_type':str})
    p=pd.read_csv(io.BytesIO(gzip.decompress(b)),dtype={'code':str,'bb_period':str,'date':str,'event_type':str,'price_day':str})
    chk(len(f)==1500 and len(p)==22500 and f.code.nunique()==1500,'COUNTS')
    chk(not p[KEY+['day']].duplicated().any(),'DUP_PRICE_DAY')
    chk(p[KEY].drop_duplicates().shape[0]==1500,'BASE_UNIQUE')
    chk(set(map(tuple,p[KEY].drop_duplicates().to_numpy()))==set(map(tuple,f[KEY].to_numpy())),'BASE_FREEZE_JOIN')
    for _,g in p.groupby(KEY,sort=False):
        chk(sorted(g.day.tolist())==list(range(1,16)),'EXACT_D1_D15')
        chk(g.sort_values('day').price_day.is_monotonic_increasing and g.price_day.nunique()==15,'PRICE_DAYS')
        chk(g.price_day.min()>g.date.iloc[0],'POST_SIGNAL_ONLY')
    return f,p,m

def fetch_three(days,code,fetch=None,cooldown=0.2):
    if fetch is None:
        from pykrx import stock
        fetch=lambda side,s,e,c:stock.get_market_trading_value_by_date(s,e,c,on=side)
    frames={}
    for side in ('매수','매도','순매수'):
        frames[side]=fetch(side,days[0].replace('-',''),days[-1].replace('-',''),code)
        if cooldown:time.sleep(cooldown)
    parsed={side:c27.parse_provider(frame,days) for side,frame in frames.items()}
    out=[]
    for i,day in enumerate(days,1):
        b,s,n=(parsed[side][day] for side in ('매수','매도','순매수'))
        chk(b['전체']>=0 and s['전체']>=0,'TOTAL_NEGATIVE')
        chk(abs(b['전체']-s['전체'])<=max(2,b['전체']*.00001),'TOTAL_BALANCE')
        chk(abs(n['전체'])<=max(2,b['전체']*.00001),'NET_BALANCE')
        chk(abs(sum(b[a] for a in ACTORS)-b['전체'])<=max(1000,b['전체']*.005),'GROUP_BUY_SUM')
        chk(abs(sum(s[a] for a in ACTORS)-s['전체'])<=max(1000,s['전체']*.005),'GROUP_SELL_SUM')
        for actor in ACTORS:
            bv,sv,nv=int(b[actor]),int(s[actor]),int(n[actor])
            chk(bv>=0 and sv>=0,'NEGATIVE_GROSS')
            chk(abs(bv-sv-nv)<=max(2,(bv+sv)*.000001),'GROSS_NET_IDENTITY')
            out.append({'day':i,'trade_date':day,'actor':actor,'buy_krw':bv,'sell_krw':sv,'net_krw':nv,'total_krw':int(b['전체'])})
    chk(len(out)==60,'RAW_ROWS_60')
    return out

def read_seed(seed_root,f,p,base_sha):
    if not seed_root:return [],{}
    root=pathlib.Path(seed_root);m=json.loads((root/MAN).read_text())
    chk(m['version']==VERSION and m['base_sha256']==base_sha and m['frozen_sha256']==FROZEN_SHA,'SEED_IDENTITY')
    for name in (RAW,AUD):chk(sha((root/name).read_bytes())==m['files_sha256'][name],'SEED_SHA_'+name)
    audit=pd.read_csv(root/AUD,dtype={x:str for x in KEY}).fillna('')
    raw=pd.read_csv(root/RAW,dtype={x:str for x in KEY+['trade_date','actor']})
    if len(audit)==0:audit=pd.DataFrame(columns=AUDCOL)
    if len(raw)==0:raw=pd.DataFrame(columns=RAWCOL)
    idx={key_of(x):x for x in f.itertuples(index=False)}
    by_key={k:g.sort_values('day') for k,g in p.groupby(KEY,sort=False)}
    old={key_of(x):dict(zip(AUDCOL,row)) for x,row in zip(audit.itertuples(index=False),audit[AUDCOL].itertuples(index=False,name=None))}
    chk(len(old)==len(audit) and set(old).issubset(idx),'SEED_AUDIT_UNIQUE')
    chk(not raw[KEY+['day','actor']].duplicated().any(),'SEED_RAW_DUP')
    completed={k for k,v in old.items() if str(v['source_complete'])=='1'}
    chk(len(raw)==len(completed)*60,'SEED_RAW_COUNT')
    chk(set(map(tuple,raw[KEY].to_numpy()))==completed,'SEED_RAW_IDENTITY')
    for k,g in raw.groupby(KEY,sort=False):
        chk(len(g)==60 and g.day.nunique()==15 and g.trade_date.nunique()==15,'SEED_DAYS')
        days=by_key[k].price_day.tolist()
        for day,part in g.groupby('day'):
            chk(len(part)==4 and set(part.actor)==set(ACTORS) and set(part.trade_date)=={days[int(day)-1]},'SEED_ACTORS_CALENDAR')
            chk(((part.buy_krw-part.sell_krw-part.net_krw).abs()<=2).all(),'SEED_GROSS_NET')
    return raw.to_dict('records'),old

def emit(path,rows,cols):
    df=pd.DataFrame(rows,columns=cols)
    if len(df):df=df.sort_values(KEY+(['day','actor'] if 'day' in cols else []),kind='stable')
    body=df.to_csv(index=False,lineterminator='\n').encode('utf-8')
    if path.suffix=='.gz':
        bio=io.BytesIO()
        with gzip.GzipFile(fileobj=bio,mode='wb',mtime=0,filename='') as z:z.write(body)
        path.write_bytes(bio.getvalue())
    else:path.write_bytes(body)

def collect(freeze_file,base_dir,out,seed_root=None,batch=30,cooldown=.8,fetch=None,max_fail=3):
    chk(1<=batch<=30,'BATCH_LIMIT')
    f,p,base_m=read_base(base_dir,freeze_file)
    by_key={k:g.sort_values('day') for k,g in p.groupby(KEY,sort=False)}
    raw,old=read_seed(seed_root,f,p,base_m['base_sha256'])
    attempted=0;success=0;failrun=0;abort=False
    try:
        for e in f.itertuples(index=False):
            k=key_of(e)
            if k in old:continue
            if attempted>=batch:break
            days=by_key[k].price_day.tolist()
            attempted+=1
            try:
                src=fetch_three(days,e.code,fetch=fetch)
                raw.extend([{**dict(zip(KEY,k)),**r} for r in src])
                old[k]=dict(zip(AUDCOL,[*k,1,'','COMPLETE_VALUE3_POST15_NOT_PIT']))
                success+=1;failrun=0
            except Exception as ex:
                error=str(ex) if isinstance(ex,(c27.AuditError,RuntimeError)) else type(ex).__name__
                old[k]=dict(zip(AUDCOL,[*k,0,error[:200],'QUARANTINED_NO_IMPUTATION']))
                failrun+=1
            if attempted%5==0:print('R1D27E_BATCH',attempted,'new_success',success,'known',len(old),'consecutive_errors',failrun,flush=True)
            if failrun>=max_fail:abort=True;break
            if cooldown:time.sleep(cooldown)
    finally:
        root=pathlib.Path(out);root.mkdir(parents=True,exist_ok=True)
        emit(root/RAW,raw,RAWCOL);emit(root/AUD,list(old.values()),AUDCOL)
        n_ok=sum(str(q['source_complete'])=='1' for q in old.values())
        chk(len(raw)==n_ok*60,'SOURCE_ROW_COUNT')
        m=dict(version=VERSION,phase='POST_SIGNAL_OUTCOME_ONLY',base_sha256=base_m['base_sha256'],
            frozen_sha256=FROZEN_SHA,n_frozen=1500,n_attempted=len(old),n_complete=n_ok,
            n_quarantined=len(old)-n_ok,n_unattempted=1500-len(old),provider_abort=abort,
            batch_attempted=attempted,batch_success=success,post_signal_feature_not_tradeable_at_signal=True,
            research_only=True,live_changed=False,orders_placed=0,prospective_OOS=False,
            PIT_certified=False,missing_source_not_zero=True)
        m['files_sha256']={z:sha((root/z).read_bytes()) for z in (RAW,AUD)}
        (root/MAN).write_text(json.dumps(m,ensure_ascii=False,indent=2)+'\n')
        print('R1D27E_POST15_CHECKPOINT',json.dumps(m,ensure_ascii=False),flush=True)
    return m

def stage(mpath):return json.loads(pathlib.Path(mpath).read_text())

def dispatch(mpath,depth,abort_streak,run_id,part,branch,workflow,token,pause=180):
    m=stage(mpath)
    chk(m['version']==VERSION,'DISPATCH_MANIFEST')
    if m['n_unattempted']==0:
        print('R1D27E_AUTOCONTINUE',json.dumps({'status':'COMPLETE','complete':m['n_complete'],'quarantined':m['n_quarantined']}))
        return
    if not m['provider_abort']:next_streak=0
    else:next_streak=int(abort_streak)+1
    if next_streak>=3:
        print('::warning::R1D27E_THREE_PROVIDER_ABORT_RUNS_STOPPED_CHECKPOINT_SAFE')
        return
    if int(depth)>=11:
        print('::warning::R1D27E_CHAIN_DEPTH_LIMIT_CHECKPOINT_SAFE')
        return
    # Must be the actual uploaded step, not inferred from total attempted count.
    chk(part in range(1,9),'PART_RANGE')
    if m['provider_abort']:time.sleep(pause)
    repo=os.environ.get('GITHUB_REPOSITORY','')
    chk(bool(token) and repo.count('/')==1,'GITHUB_AUTH')
    data=json.dumps({'ref':branch,'inputs':{'seed_run_id':str(run_id),'seed_part':f'{part:02d}',
        'chain_depth':str(int(depth)+1),'abort_streak':str(next_streak)}}).encode()
    req=urllib.request.Request(f'https://api.github.com/repos/{repo}/actions/workflows/{workflow}/dispatches',
        data=data,headers={'Authorization':f'Bearer {token}','Accept':'application/vnd.github+json',
        'X-GitHub-Api-Version':'2022-11-28','Content-Type':'application/json'},method='POST')
    with urllib.request.urlopen(req,timeout=30) as response:chk(response.status==204,'DISPATCH_HTTP')
    print('R1D27E_AUTOCONTINUE',json.dumps({'status':'DISPATCHED','seed_run':run_id,'seed_part':part,'next_depth':int(depth)+1,
        'remaining':m['n_unattempted'],'abort_streak':next_streak}))

def main():
    ap=argparse.ArgumentParser();sp=ap.add_subparsers(dest='cmd',required=True)
    co=sp.add_parser('collect');co.add_argument('--freeze-file',required=True);co.add_argument('--base-dir',required=True);co.add_argument('--out',required=True);co.add_argument('--seed-root');co.add_argument('--batch',type=int,default=30)
    di=sp.add_parser('dispatch');di.add_argument('--manifest',required=True);di.add_argument('--depth',type=int,required=True);di.add_argument('--abort-streak',type=int,required=True);di.add_argument('--run-id',required=True);di.add_argument('--part',type=int,required=True);di.add_argument('--branch',default='main');di.add_argument('--workflow',required=True)
    a=ap.parse_args()
    if a.cmd=='collect':collect(a.freeze_file,a.base_dir,a.out,a.seed_root,a.batch)
    else:dispatch(a.manifest,a.depth,a.abort_streak,a.run_id,a.part,a.branch,a.workflow,os.environ.get('GITHUB_TOKEN',''))
if __name__=='__main__':main()
