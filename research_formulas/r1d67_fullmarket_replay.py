#!/usr/bin/env python3
"""R1-D67 independent all-KRX frozen-archive price parity + strict 4-pivot geometry.
RESEARCH_ONLY. Original daily OHLC is treated as price BASIS UNVERIFIED for corporate actions.
Source is recovered 8 GitHub Actions artifacts from run 39295295197, not R1D50's seven arrays.
Never claims PIT-adjusted prices or replays any trading returns.
"""
import argparse,csv,gzip,hashlib,io,json,zipfile,glob,sys,collections,pathlib,math,os
import numpy as np
import pandas as pd
from r1d65_strict_triangle import geometry

RUN=39295295197
EXPECTED_ARTIFACT_NAMES=[f'stockhunter-allkrx-price-shard-{i}' for i in range(8)]
ARCHIVE_SHA_EXPECTED={
0:'84dd5aa62f0c9538cd9c2d0ed0540c8f74063b21be49d9ecfc81e4b1583368b5',
1:'3d372eebf915c870badf103356c3700d5fa914a32d79ce8057ef1f4c4f89295b',
2:'5c49df64ad10bafbc09dd7ea78b9377804735fda6ad861018750b396ab2a27cb',
3:'7a359f5959e2374622c40d068920adf9d695142b4a61b540d0752a8cb9bb4dd8',
4:'88b35ea4c9a8a6a0ed4b61250a8018d49ee7ba4489c7345f04012915bb60f997',
5:'eb503f7cabcc40991037092c6463437c8a8b71d43fb26fe646ea4f56885b3814',
6:'b3cc055d919c7ab92d7f27b1b3ce6d3e56df787b107af961eaa43dedb885d6cd',
7:'9563747eb8b27928ba0a0d2b7d3cf02e787acd2e9e4a656d410c286139321478'}
ROOTNAME='R1D56_STRUCTURAL_FULLMARKET_20261030/'
COLS=['date','code','market','event_type','tri40_triangle_breakout','asof_status','ma224_dist_pct','amount_vs_prior20']
CONTRACT_DAY_COUNT=1907
CONTRACT_ROW_COUNT=5090428

def sha(b):return hashlib.sha256(b).hexdigest()
def get_features(d56):
    if str(d56).endswith('.csv.gz'):
        f=pd.read_csv(d56,compression='gzip',dtype={'code':str},usecols=COLS,low_memory=False)
    else:
        with zipfile.ZipFile(d56) as z:
            name=ROOTNAME+'01_ASOF_STRUCTURAL_FEATURES.csv.gz'
            b=z.read(name)
            f=pd.read_csv(io.BytesIO(b),compression='gzip',dtype={'code':str},usecols=COLS,low_memory=False)
    f['code']=f['code'].astype(str).str.zfill(6)
    if len(f)!=84922 or f[['date','code','event_type']].duplicated().any():raise ValueError('D56_FROZEN_IDENTITY_FAILURE')
    return f

def recover(archive_paths,target_codes,lookback_days=None):
    # 8 separate internally authenticated date-interleaved archives; no online fetch here.
    file_index={};master_sha={};seen_shards=set()
    opened=[];all_mfs=[]
    for path in archive_paths:
        z=zipfile.ZipFile(path);opened.append(z)
        mfpaths=[k for k in z.namelist() if k.endswith('.json') and 'manifest_shard' in k]
        if len(mfpaths)!=1:raise ValueError('INVALID_SHARD_MANIFEST_COUNT:'+str(path))
        mf=json.loads(z.read(mfpaths[0]));s=int(mf['shard'])
        if s in seen_shards:raise ValueError('DUPLICATE_SHARD')
        if mf.get('status') not in ('COMPLETE','PASS','SUCCESS','OK'):
            # Preserve status for audit, but do not silently dismiss unknown status.
            if mf.get('status')!='COMPLETE_PRICE_COLLECTION_ONLY':raise ValueError('SHARD_STATUS_INVALID:'+str(mf.get('status')))
        seen_shards.add(s);all_mfs.append(mf)
        master_sha[str(s)]=sha(pathlib.Path(path).read_bytes())
        if master_sha[str(s)]!=ARCHIVE_SHA_EXPECTED[s]:raise ValueError("ZIP_SHA256_MISMATCH_SHARD_"+str(s))
        for day in mf['files']:
            date=day['date'];inner='shard_%02d/'%s+day['file']
            if date in file_index:raise ValueError('DUPLICATE_DATE_IN_SHARDS:'+date)
            if inner not in z.namelist():raise ValueError('SHARD_DAY_MISSING:'+inner)
            file_index[date]=(z,inner,day)
    if seen_shards!=set(range(8)):raise ValueError('NOT_EIGHT_SHARDS:'+str(seen_shards))
    dates=sorted(file_index)
    if len(dates)!=CONTRACT_DAY_COUNT:raise ValueError('DAY_COUNT_MISMATCH:'+str(len(dates)))
    if dates[0]!='2019-01-02' or dates[-1]!='2026-10-12':raise ValueError('CALENDAR_RANGE_MISMATCH')
    codes=sorted(target_codes);code_ix={code:i for i,code in enumerate(codes)}
    day_ix={date:i for i,date in enumerate(dates)}
    arrays={k:np.full((len(codes),len(dates)),np.nan,dtype=np.float64) for k in ('open','high','low','close','amount')}
    obs=0;matched=0;nontrading=0
    try:
        for nd,date in enumerate(dates):
            z,inner,expected=file_index[date]
            compressed=z.read(inner)
            if sha(compressed)!=expected['sha256']:raise ValueError('SHA256_DAILY_MISMATCH:'+date)
            lines=gzip.decompress(compressed).decode('utf-8')
            reader=csv.DictReader(io.StringIO(lines));daycount=0
            for row in reader:
                daycount+=1
                code=row['code'];ix=code_ix.get(code)
                if ix is None:continue
                matched+=1
                for field in arrays: arrays[field][ix,nd]=float(row[field])
                if float(row['open'])<=0 or float(row['amount'])<=0:nontrading+=1
            if daycount!=expected['rows']:raise ValueError('DAY_ROWS_MISMATCH:'+date)
            obs+=daycount
        if obs!=CONTRACT_ROW_COUNT:raise ValueError('TOTAL_RAW_ROWS_MISMATCH:'+str(obs))
    finally:
        for z in opened:z.close()
    return dates,codes,arrays,dict(artifact_zip_sha256_by_inner_shard=master_sha,full_market_price_rows=obs,retained_event_code_rows=matched,nontrade_event_code_rows=nontrading,unique_event_tickers=len(codes),historical_dates=len(dates),source='KRX via pykrx 2026-10-19 immutable 8 Actions artifacts',authority='RAW_OR_UNKNOWN_UNCERTIFIED_CORPORATE_ACTION')

def parity(d,dates,code_ix,day_ix,arrays):
    """Check D56 MA224 distance and amount-vs-prior20 against independent raw KRX source."""
    c=d['code'];date=f'{int(d["date"]):08d}';date=f'{date[:4]}-{date[4:6]}-{date[6:]}'
    x=code_ix.get(c);t=day_ix.get(date)
    if x is None or t is None:return 'MISSING_TICKER_DATE',None,None,None
    if t<223:return 'INSUFFICIENT_HISTORY',x,t,None
    close=arrays['close'][x];amt=arrays['amount'][x]
    prev=close[t-223:t+1];turn=amt[t-20:t]
    if not np.isfinite(prev).all() or (prev<=0).any() or not np.isfinite(turn).all() or (turn<=0).any() or not np.isfinite(amt[t]) or amt[t]<=0:
        return 'INVALID_RAW_HISTORY',x,t,None
    dist=100*(close[t]/float(prev.mean())-1);ratio=amt[t]/float(turn.mean())
    dist_prev=float(d['ma224_dist_pct']);amt_prev=float(d['amount_vs_prior20'])
    if not np.isfinite(dist_prev) or not np.isfinite(amt_prev):return 'D56_NO_FEATURE',x,t,(dist,ratio)
    # float floats are stored in D56; +/-0.01 percentage points is a strict practical tolerance.
    if abs(dist-dist_prev)>0.01 or abs(ratio-amt_prev)>0.01:
        return 'ASOF_PRICE_BASIS_OR_SOURCE_MISMATCH',x,t,(dist,ratio)
    return 'ASOF_PRICE_PARITY',x,t,(dist,ratio)

def run(archive_paths,d56,out,only_tri40=False):
    out=pathlib.Path(out);out.mkdir(exist_ok=True,parents=True)
    f=get_features(d56)
    srcsha=sha(pathlib.Path(d56).read_bytes())
    if only_tri40: f=f.loc[f.tri40_triangle_breakout.eq(1)].copy()
    days,codes,arrays,source=recover(archive_paths,set(f.code))
    ci={code:i for i,code in enumerate(codes)};di={date:i for i,date in enumerate(days)}
    count=collections.Counter();rows=[];ca_dates={'094940':'2021-02-16','357230':'2024-03-06','089850':'2024-04-02'}
    # Note: only a PARTIAL list of verified corporate action cases. Absence != clean.
    for n,d in enumerate(f.to_dict(orient='records')):
        state,x,t,comparables=parity(d,days,ci,di,arrays)
        if state!='ASOF_PRICE_PARITY':z={'status':'SOURCE_INVALID','reason':state,'geometry_valid':False,'breakout_geometry':False,'qualified_below224':False,'qualified_with_amount':False}
        else:
            v={k:arrays[k][x] for k in arrays}
            # independent source parity only. No future-day prices in geometry.
            ma224=float(np.mean(v['close'][t-223:t+1]))
            z=geometry(v,t,ma224=ma224)
        count['PARITY_'+state]+=1;count['GEOMETRY_'+z['reason']]+=1
        if z.get('geometry_valid'):count['GEOMETRY_VALID']+=1
        if z.get('breakout_geometry'):count['BREAKOUT_GEOMETRY']+=1
        if z.get('qualified_with_amount'):count['QUALIFIED_PROVISIONAL_PRICE_ONLY']+=1
        sig=f'{int(d["date"]):08d}';sig=f'{sig[:4]}-{sig[4:6]}-{sig[6:]}'
        action=ca_dates.get(d['code']);ca_status='VERIFIED_CA_DAY_IN_FUTURE_WINDOW' if action and (sig<=action<=days[min(t+20,len(days)-1)] if t is not None else False) else 'CA_HISTORY_INCOMPLETE'
        note='CA_REPLAY_REQUIRED' if ca_status=='VERIFIED_CA_DAY_IN_FUTURE_WINDOW' else 'CA_LEDGER_UNCERTIFIED'
        rows.append({'date':sig,'code':d['code'],'market':d['market'],'event_type':d['event_type'],
            'legacy_tri40':int(float(d['tri40_triangle_breakout'] or 0)==1),
            'original_asof_status':d['asof_status'],'price_parity':state,'price_authority_status':note,
            'known_ca_ex_date':action or '',
            'new_geometry_valid':int(bool(z.get('geometry_valid'))),'new_fresh_breakout':int(bool(z.get('breakout_geometry'))),
            'new_strict_candidate_provisional':int(bool(z.get('qualified_with_amount'))),
            'new_geometry_status':z['status'],'new_geometry_reason':z['reason'],
            'H1':z.get('H1',''),'H2':z.get('H2',''),'L1':z.get('L1',''),'L2':z.get('L2',''),
            'H1_date':days[z['H1_ix']] if t is not None and isinstance(z.get('H1_ix'),int) else '',
            'H2_date':days[z['H2_ix']] if t is not None and isinstance(z.get('H2_ix'),int) else '',
            'L1_date':days[z['L1_ix']] if t is not None and isinstance(z.get('L1_ix'),int) else '',
            'L2_date':days[z['L2_ix']] if t is not None and isinstance(z.get('L2_ix'),int) else '',
            'contraction_pct':z.get('contraction_pct',''), 'amount_ratio_recomputed':z.get('amount_ratio',''),
            'new_live_eligible':0,'return_pct_recomputed':''})
    details=pd.DataFrame(rows)
    details.to_csv(out/'R1D67_INDEPENDENT_PRICE_PARITY_AND_STRICT_TRIANGLE.csv.gz',index=False,compression='gzip')
    for name,sub in [('R1D67_LEGACY_TRI40_RECHECK.csv',details[details.legacy_tri40.eq(1)]),('R1D67_PULOON_20210125.csv',details[(details.code=='094940')&(details.date=='2021-01-25')])]:sub.to_csv(out/name,index=False,encoding='utf-8-sig')
    score=pd.DataFrame([{'market':m,'year':y,'event_type':etype,'n':len(s),'price_parity_n':int((s.price_parity=='ASOF_PRICE_PARITY').sum()),'legacy_tri40_n':int(s.legacy_tri40.sum()),'new_strict_candidate_provisional_n':int(s.new_strict_candidate_provisional.sum())} for (m,y,etype),s in details.assign(year=lambda x:x.date.str.slice(0,4)).groupby(['market','year','event_type'])])
    score.to_csv(out/'R1D67_MARKET_YEAR_COUNTS.csv',index=False)
    audit=dict(study='R1D67_KRX_8SHARD_INDEPENDENT_PRICE_PARITY_AND_STRICT_GEOMETRY',source_run=RUN,source_identity=source,d56_sha256=srcsha,events_evaluated=len(details),old_tri40_n=int(details.legacy_tri40.sum()),price_parity_pass=int((details.price_parity=='ASOF_PRICE_PARITY').sum()),price_parity_fail_by_reason=dict(details.price_parity.value_counts()),legacy_tri40_strict_n=int(details.loc[details.legacy_tri40.eq(1),'new_strict_candidate_provisional'].sum()),strict_candidate_provisional_n=int(details.new_strict_candidate_provisional.sum()),known_ca_future_window_flag_n=int((details.price_authority_status=='CA_REPLAY_REQUIRED').sum()),failure_and_success_counts=dict(count),research_only=True,live_modified=False,price_basis_certified=False,full_CA_ledger=False,performance_recomputed=False,prospective_OOS=False,source_shard_may_differ_from_R1D50=True)
    (out/'R1D67_AUDIT.json').write_text(json.dumps(audit,ensure_ascii=False,indent=2,default=lambda x:x.item() if isinstance(x,np.generic) else str(x)),encoding='utf8')
    print(json.dumps({k:audit[k] for k in ('events_evaluated','old_tri40_n','price_parity_pass','legacy_tri40_strict_n','strict_candidate_provisional_n','known_ca_future_window_flag_n')},ensure_ascii=False,indent=2),flush=True)
    return audit

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--shards',nargs='+',required=True);p.add_argument('--d56',required=True);p.add_argument('--out',required=True);p.add_argument('--only-tri40',action='store_true')
    a=p.parse_args();run(a.shards,a.d56,a.out,a.only_tri40)
