#!/usr/bin/env python3
"""StockHunter Stage6A: dated KRX market-cap + share-count EVIDENCE, not PIT stock-type approval.

RESEARCH_ONLY. Same KRX information family as the price source; no independent
cross-source proof of shares, security type, official halts, ISIN, or tradeability.
Immutable Stage5 market roster and Stage1 price signatures are re-checked. No
LIVE, scoring, ranking, portfolio, or actual trade functions are imported.
"""
from __future__ import annotations
import argparse
import csv
import gzip
import hashlib
import io
import json
import pathlib
import re
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime,timezone

import pit_stage5_daily_roster as prior

SCHEMA='STOCKHUNTER_PIT_STAGE6_DATE_CAP_EVIDENCE_V1'
STAGE5_RUN=39383970402
STAGE5_SHA='6d643b3aade052ebf7e330c5bc005cff36847cf2'
STAGE4_RUN=39373355974
CAP_COLUMNS=('종가','시가총액','거래량','거래대금','상장주식수')
ROW_COLUMNS=('code','close','market_cap_krw','volume','amount_krw','listed_shares')
DAY_COUNT=1907
EXPECTED_ROWS=5090428
SHARDS=8


def stop(x):raise RuntimeError('FAIL_CLOSED_STAGE6_'+str(x))
def digest(b):return hashlib.sha256(b).hexdigest()
def output(path,b):
    p=pathlib.Path(path);p.parent.mkdir(parents=True,exist_ok=True)
    if isinstance(b,str):b=b.encode('utf-8')
    q=p.with_name(p.name+'.part');q.write_bytes(b);q.replace(p)
def jsonbytes(x):return (json.dumps(x,ensure_ascii=False,sort_keys=True,separators=(',',':'))+'\n').encode()
def dump(path,x):output(path,json.dumps(x,ensure_ascii=False,indent=2,sort_keys=True)+'\n')
def load(path):return json.loads(pathlib.Path(path).read_text(encoding='utf-8'))
def csvbytes(rows,columns):
    f=io.StringIO(newline='');w=csv.DictWriter(f,fieldnames=columns);w.writeheader();w.writerows(rows)
    return f.getvalue().encode('utf-8')
def csvrows(raw):
    return list(csv.DictReader(io.StringIO(raw.decode('utf-8-sig'),newline='')))

def validate_manifest(stage5,shard,strict=True):
    base=pathlib.Path(stage5)/('shard_%02d'%shard)
    m=load(base/'manifest.json')
    if (m.get('status')!='COMPLETE_DATED_MARKET_ROSTER_EVIDENCE_ONLY' or
        m.get('canary') is not False or m.get('shard_index')!=shard or
        m.get('shard_count')!=SHARDS or m.get('source_price_run')!=prior.P_SOURCE_RUN or
        m.get('source_stage2_run')!=prior.P_STAGE2_RUN):
        stop('STAGE5_MANIFEST_MISMATCH:'+str(shard))
    files=m.get('files')
    if not isinstance(files,list) or (strict and len(files) not in (238,239)):
        stop('STAGE5_SHARD_INCOMPLETE:'+str(shard))
    if len({x['date'] for x in files})!=len(files):stop('STAGE5_DUPLICATE_DATES')
    return base,m

def stage5_day(base,entry):
    day=entry['date']
    if not re.fullmatch(r'20\d{2}-\d\d-\d\d',day):stop('BAD_DATE:'+day)
    expected='days/'+day.replace('-','')+'.json.gz'
    if entry.get('file')!=expected:stop('UNSAFE_STAGE5_DAY_FILE:'+str(entry.get('file')))
    data=(base/expected).read_bytes()
    if digest(data)!=entry.get('sha256'):stop('TAMPERED_STAGE5_ROSTER:'+day)
    item=json.loads(gzip.decompress(data))
    if item.get('schema')!=prior.SCHEMA or item.get('date')!=day or item.get('strategy_ready') is not False:
        stop('STAGE5_DAY_SOURCE_DRIFT:'+day)
    valid=prior.normalize_roster(day,item['market_codes'])
    if (valid['market_codes']!=item['market_codes'] or
        valid['listed_union_rows']!=item['listed_union_rows'] or
        item['listed_union_rows']!=entry['market_rows'] or
        entry.get('count_mismatch') or entry['market_rows']!=entry['expected_price_rows']):
        stop('STAGE5_ROSTER_IDENTITY_MISMATCH:'+day)
    return set().union(*(set(t) for t in item['market_codes'].values()))


def integer_exact(x,field,code):
    # No silent downcasts, broken missing values, float/truncation, or shares from future data.
    v=str(x).strip().replace(',','')
    if not re.fullmatch(r'[0-9]+',v):stop('NON_INTEGER_'+field+':'+code)
    return int(v)


def normalize_cap(date, frame, expected_codes):
    """Require exact dated KRX ticker identities; zero cap is uncertainty, not delisting."""
    if frame is None or not hasattr(frame,'index') or not hasattr(frame,'columns'):
        stop('KRX_EMPTY_CAP_FRAME:'+date)
    cols=set(map(str,frame.columns))
    if not set(CAP_COLUMNS)<=cols:stop('KRX_CAP_COLUMNS_MISSING:'+date+':'+str(cols))
    names=[str(x).strip() for x in frame.index.tolist()]
    if any(not prior.CODE_RE.fullmatch(s) for s in names):stop('BAD_CAP_CODES:'+date)
    if len(names)!=len(set(names)):stop('DUPLICATE_CAP_CODES:'+date)
    if not names:stop('EMPTY_CAP_DAY:'+date)
    if set(names)!=expected_codes:
        stop('CAP_CODE_ROSTER_MISMATCH:'+date+':missing='+str(len(expected_codes-set(names)))+
             ':extra='+str(len(set(names)-expected_codes)))
    records=[]
    for code in sorted(expected_codes):
        item=frame.loc[code]
        # `frame` must have unique index (checked above).
        cols_int=[integer_exact(item[col],col,code) for col in CAP_COLUMNS]
        close,cap,vol,amt,shares=cols_int
        if cap>0 and shares==0:stop('POSITIVE_CAP_WITH_ZERO_SHARES:'+date+':'+code)
        if amt>0 and vol==0:stop('POSITIVE_AMOUNT_ZERO_VOLUME:'+date+':'+code)
        records.append(dict(zip(ROW_COLUMNS,[code,close,cap,vol,amt,shares])))
    return records


def provider_cap(stock,day,pause=1.4,retries=3):
    exc=None
    for attempt in range(retries):
        try:
            # alternative=False forbids silent fallback to the previous session.
            ans=stock.get_market_cap_by_ticker(day.replace('-',''),market='ALL',alternative=False)
            if ans is None or len(ans)==0:stop('KRX_CAP_EMPTY:'+day)
            return ans
        except Exception as e:
            exc=e
            if attempt+1<retries:time.sleep(pause*(attempt+1))
    stop('KRX_MARKET_CAP_PROVIDER_FAILED:'+day+':'+str(exc)[:160])


def save_cap_day(day,rows,base):
    name='days/'+day.replace('-','')+'.csv.gz'
    payload=gzip.compress(csvbytes(rows,ROW_COLUMNS),mtime=0)
    output(pathlib.Path(base)/name,payload)
    return dict(date=day,file=name,sha256=digest(payload),rows=len(rows),
                zero_market_cap=sum(x['market_cap_krw']==0 for x in rows),
                zero_volume=sum(x['volume']==0 for x in rows))


def scan(stage5_root,out,stock,shard, pause=1.4,strict=True):
    if not 0<=shard<SHARDS:stop('INVALID_SHARD')
    base,manifest=validate_manifest(stage5_root,shard,strict=strict)
    root=pathlib.Path(out)
    saved=[]
    for n,entry in enumerate(manifest['files'],1):
        day=entry['date']; roster=stage5_day(base,entry)
        frame=provider_cap(stock,day,pause=pause)
        cap=normalize_cap(day,frame,roster)
        saved.append(save_cap_day(day,cap,root))
        dump(root/'manifest.json',dict(schema=SCHEMA,status='PARTIAL',shard=shard,
            shards=SHARDS,source_stage5_run=STAGE5_RUN,source_stage5_sha=STAGE5_SHA,
            source_stage1_run=prior.P_SOURCE_RUN,stage5_manifest_sha256=digest(jsonbytes(manifest)),
            files=saved,strategy_ready=False))
        if n%25==0:print('STAGE6_CAP_PROGRESS',shard,n,len(manifest['files']),day,flush=True)
        if pause:time.sleep(pause)
    summary=dict(schema=SCHEMA,status='COMPLETE_DATED_CAP_SOURCE_ONLY',shard=shard,
        shards=SHARDS,source_stage5_run=STAGE5_RUN,source_stage5_sha=STAGE5_SHA,
        source_stage1_run=prior.P_SOURCE_RUN,stage5_manifest_sha256=digest(jsonbytes(manifest)),
        days=len(saved),rows=sum(x['rows'] for x in saved),files=saved,
        security_type_verified=False,official_halts_verified=False,strategy_ready=False)
    dump(root/'manifest.json',summary)
    print(json.dumps({k:summary[k] for k in ('status','shard','days','rows')},sort_keys=True),flush=True)
    return summary


def canary(stage2_dir,out,stock,pause=1.4):
    dates=prior.source_stage2(stage2_dir,strict=True)
    snapshots=load(pathlib.Path(stage2_dir)/'dated_market_membership_snapshots.json')
    if len(snapshots)!=4:stop('CANARY_DATE_COUNT')
    records=[]
    for item in snapshots:
        day=item['date']
        if day not in dates:stop('CANARY_INVALID_DAY')
        markets=item['market_codes']
        normalized=prior.normalize_roster(day,markets)
        roster=set().union(*(set(x) for x in normalized['market_codes'].values()))
        if len(roster)!=dates[day]:stop('CANARY_STAGE2_ROWS_MISMATCH')
        cap=normalize_cap(day,provider_cap(stock,day,pause=pause),roster)
        records.append(save_cap_day(day,cap,out))
        if pause:time.sleep(pause)
    summary=dict(status='CAP_CANARY_FOUR_HISTORICAL_DATES_PASS',schema=SCHEMA,
        source_stage2_run=prior.P_STAGE2_RUN,dates=[r['date'] for r in records],
        rows=sum(x['rows'] for x in records),market_cap_source_provenance='KRX get_market_cap_by_ticker(AS_OF_DATE, ALL)',
        historic_security_type_verified=False,official_halt_verified=False,
        isin_verified=False,strategy_ready=False,files=records)
    dump(pathlib.Path(out)/'canary_summary.json',summary)
    print(json.dumps({k:summary[k] for k in ('status','dates','rows')},ensure_ascii=False),flush=True)
    return summary


def read_verified_rows(root,entry,day):
    fname='days/'+day.replace('-','')+'.csv.gz'
    if entry['date']!=day or entry['file']!=fname:stop('CAP_MANIFEST_DAY_OR_PATH_MISMATCH:'+day)
    data=(pathlib.Path(root)/fname).read_bytes()
    if digest(data)!=entry['sha256']:stop('CAP_STORED_SHA_DRIFT:'+day)
    try:rows=csvrows(gzip.decompress(data))
    except (OSError,UnicodeError,csv.Error) as e:stop('CAP_GZIP_INVALID:'+day+':'+str(e))
    if len(rows)!=entry['rows']:stop('CAP_ROW_COUNT_DRIFT:'+day)
    if rows and tuple(rows[0])!=ROW_COLUMNS:stop('CAP_COLUMNS_DRIFT:'+day)
    codes=set()
    for row in rows:
        code=row['code']
        if not prior.CODE_RE.fullmatch(code) or code in codes:stop('CAP_DUP_CODE:'+day+':'+code)
        codes.add(code)
        for field in ROW_COLUMNS[1:]:row[field]=integer_exact(row[field],field,code)
    return {row['code']:row for row in rows}


def price_day_values(base, manifest, date, expected):
    info=next((x for x in manifest['files'] if x['date']==date),None)
    if not info:stop('PRICE_DAY_NOT_IN_MANIFEST:'+date)
    p=pathlib.Path(base)/info['file']
    data=p.read_bytes()
    if prior.digest(data)!=info['sha256']:stop('PRICE_DAY_SHA_DRIFT:'+date)
    rows={}
    try:
        with gzip.open(io.BytesIO(data),'rt',encoding='utf-8',newline='') as g:
            reader=csv.DictReader(g)
            if tuple(reader.fieldnames or [])!=('code','date','open','high','low','close','volume','amount'):
                stop('PRICE_SOURCE_COLUMNS_DRIFT:'+date)
            for item in reader:
                code=item['code']
                if not prior.CODE_RE.fullmatch(code) or code in rows or item['date']!=date:
                    stop('PRICE_SOURCE_ROW_BAD:'+date+':'+code)
                rows[code]={v:integer_exact(item[v],v,code) for v in ('close','volume','amount')}
    except (OSError,UnicodeError,csv.Error) as e:stop('PRICE_DECOMPRESSION_FAILED:'+date+':'+str(e))
    if len(rows)!=expected or info['stored_rows']!=expected:stop('PRICE_ROWS_DRIFT:'+date)
    return rows


def verify(stage6, stage5, price, stage2, stage4, out,strict=True):
    s2=prior.source_stage2(stage2,strict=strict)
    types=prior.source_stage4(stage4,strict=strict)
    if strict and (len(s2)!=DAY_COUNT or len(types)!=prior.EXPECTED_CODES):stop('PINNED_SOURCE_SET_MISMATCH')
    base6=pathlib.Path(stage6);base5=pathlib.Path(stage5);base1=pathlib.Path(price);out=pathlib.Path(out)
    m6s={};m5s={};pms={}
    pinned_price_evidence=load(pathlib.Path(stage2)/'source_fullmarket_price_verification.json') if strict else {}
    all_days=list(s2);unresolved={c for c,v in types.items() if v['type_evidence_label']=='UNRESOLVED_NO_2019_2026_EVENT'}
    if strict and len(unresolved)!=prior.EXPECTED_UNRESOLVED:stop('STAGE4_TYPE_RECLASSIFIED')
    for shard in range(SHARDS):
        b5,m5=validate_manifest(base5,shard,strict=strict)
        m6=load(base6/('shard_%02d'%shard)/'manifest.json')
        pm=load(base1/('shard_%02d'%shard)/('manifest_shard_%02d.json'%shard))
        want=all_days[shard::SHARDS]
        if ([x['date'] for x in m6.get('files',[])]!=want or
            [x['date'] for x in m5.get('files',[])]!=want or
            [x['date'] for x in pm.get('files',[])]!=want):stop('SHARD_DAY_ORDER_MISMATCH:'+str(shard))
        if (m6.get('status')!='COMPLETE_DATED_CAP_SOURCE_ONLY' or m6.get('shard')!=shard or
            m6.get('shards')!=SHARDS or m6.get('source_stage5_run')!=STAGE5_RUN or
            m6.get('source_stage5_sha')!=STAGE5_SHA or
            m6.get('source_stage1_run')!=prior.P_SOURCE_RUN or
            m6.get('stage5_manifest_sha256')!=digest(jsonbytes(m5)) or
            m6.get('strategy_ready') is not False):stop('UNTRUSTED_CAP_SHARD:'+str(shard))
        if (pm.get('status')!='COMPLETE_PRICE_COLLECTION_ONLY' or pm.get('universe_filter')!='NONE' or
            (strict and (pm.get('shard')!=shard or pm.get('shards')!=SHARDS or
                pm.get('calendar_sha256')!=pinned_price_evidence.get('calendar_sha256') or
                pm.get('index_sha256')!=pinned_price_evidence.get('index_sha256') or
                pm.get('official_days')!=DAY_COUNT or pm.get('start')!=prior.P_DATE_START or
                pm.get('end')!=prior.P_DATE_END))):
            stop('PRICE_SHARD_MISMATCH:'+str(shard))
        m6s[shard]=m6;m5s[shard]=m5;pms[shard]=pm
    issues=[];daily=[];profiles=defaultdict(Counter);all_codes=set()
    mismatch_days=0;volume_diff=0;amount_diff=0;close_diff=0
    zero_cap=zero_shares=zero_volume=0;positive_cap=0;rows_n=0
    for i,day in enumerate(all_days):
        shard=i%SHARDS;j=i//SHARDS
        b5=base5/('shard_%02d'%shard)
        roster=stage5_day(b5,m5s[shard]['files'][j])
        cap=read_verified_rows(base6/('shard_%02d'%shard),m6s[shard]['files'][j],day)
        px=price_day_values(base1/('shard_%02d'%shard),pms[shard],day,s2[day])
        if set(px)!=roster or set(cap)!=roster:stop('PRICE_ROSTER_CAP_CODES_MISMATCH:'+day)
        cd=vd=ad=0
        for code,vals in cap.items():
            p=px[code]
            if vals['close']!=p['close']:cd+=1
            if vals['volume']!=p['volume']:vd+=1
            if vals['amount_krw']!=p['amount']:ad+=1
            if vals['market_cap_krw']==0:zero_cap+=1
            else:positive_cap+=1
            if vals['listed_shares']==0:zero_shares+=1
            if vals['volume']==0:zero_volume+=1
            profiles[code]['observed_cap_sessions']+=1
            profiles[code]['zero_cap_sessions']+=int(vals['market_cap_krw']==0)
            profiles[code]['zero_volume_proxy_sessions']+=int(vals['volume']==0)
            profiles[code]['cap_source_mismatch_sessions']+=int(vals['close']!=p['close'] or vals['volume']!=p['volume'] or vals['amount_krw']!=p['amount'])
        all_codes|=roster
        volume_diff+=vd;amount_diff+=ad;close_diff+=cd
        rows_n+=len(roster)
        any_diff=bool(cd or vd or ad)
        mismatch_days+=any_diff
        if any_diff:
            issues.append(dict(date=day,price_vs_cap_close_mismatches=cd,
                price_vs_cap_volume_mismatches=vd,price_vs_cap_amount_mismatches=ad))
        daily.append(dict(date=day,price_rows=len(px),cap_rows=len(cap),listed_market_rows=len(roster),
                          code_set_exact=True,close_different=cd,volume_different=vd,amount_different=ad,
                          zero_cap_rows=sum(v['market_cap_krw']==0 for v in cap.values()),
                          zero_volume_rows=sum(v['volume']==0 for v in cap.values())))
        if (i+1)%250==0:print('STAGE6_VERIFY_PROGRESS',i+1,len(all_days),flush=True)
    if strict and (rows_n!=EXPECTED_ROWS or len(all_codes)!=prior.EXPECTED_CODES):stop('EXPECTED_SOURCE_TOTAL_DRIFT')
    profiles_out=[]
    for code in sorted(all_codes):
        prof=profiles[code]
        profiles_out.append(dict(code=code,stage4_event_type_evidence_label=types[code]['type_evidence_label'],
            observed_cap_sessions=prof['observed_cap_sessions'],zero_cap_sessions=prof['zero_cap_sessions'],
            zero_volume_proxy_sessions=prof['zero_volume_proxy_sessions'],
            cap_source_mismatch_sessions=prof['cap_source_mismatch_sessions'],
            original_historical_security_type_verified=False,official_suspension_verified=False,
            original_security_isin_verified=False,strategy_ready=False))
    output(out/'all_1907_cap_price_comparisons.csv',csvbytes(daily,list(daily[0])))
    output(out/'historical_cap_provenance_conflicts.csv',csvbytes(issues,['date','price_vs_cap_close_mismatches','price_vs_cap_volume_mismatches','price_vs_cap_amount_mismatches']))
    output(out/'all_3341_market_cap_observation_profiles.csv',csvbytes(profiles_out,list(profiles_out[0])))
    output(out/'unresolved_2023_cap_profiles.csv',csvbytes([x for x in profiles_out if x['code'] in unresolved],list(profiles_out[0])))
    summary=dict(schema=SCHEMA,status='HISTORICAL_CAP_SOURCE_MATCHED_CODE_SETS_ONLY' if mismatch_days==0 else 'HISTORICAL_CAP_SOURCE_HAS_PRICE_DISAGREEMENTS',
        research_only=True,strategy_ready=False,source_stage1_run=prior.P_SOURCE_RUN,source_stage2_run=prior.P_STAGE2_RUN,
        source_stage4_run=STAGE4_RUN,source_stage5_run=STAGE5_RUN,source_stage5_head_sha=STAGE5_SHA,
        sessions=len(daily),cap_rows=rows_n,distinct_codes=len(all_codes),exact_cap_roster_date_sets=len(daily),
        code_mismatch_dates=0,price_vs_cap_value_mismatch_dates=mismatch_days,
        close_mismatch_rows=close_diff,volume_mismatch_rows=volume_diff,amount_mismatch_rows=amount_diff,
        positive_cap_rows=positive_cap,zero_cap_rows=zero_cap,zero_shares_rows=zero_shares,zero_volume_proxy_rows=zero_volume,
        stage4_unresolved_type_codes=len(unresolved),historical_security_type_verified=False,
        official_daily_suspensions_verified=False,isin_identity_verified=False,
        full_market_cap_evidence_collected=True,historical_market_cap_price_source_agrees=(mismatch_days==0),
        important_limit='KRX 12001 via pykrx is same upstream family as Stage1 prices, NOT independent proof. Zero-volume/zero-cap are NOT official trading halts or tradability proofs. NO date-scoped ordinary-share/ISIN/security-type evidence was collected.',
        generated_at_utc=datetime.now(timezone.utc).isoformat())
    dump(out/'stage6_historical_market_cap_audit_summary.json',summary)
    print(json.dumps(summary,ensure_ascii=False,sort_keys=True),flush=True)
    if mismatch_days:stop('CAP_VS_ORIGINAL_PRICE_VALUE_CONFLICTS:'+str(mismatch_days))
    return summary


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--mode',choices=['canary','scan','verify'],required=True)
    p.add_argument('--stage2-dir',default='stage2-source')
    p.add_argument('--stage4-dir',default='stage4-source')
    p.add_argument('--stage5-dir',default='stage5-rosters')
    p.add_argument('--stage6-dir',default='stage6-cap')
    p.add_argument('--price-dir',default='pitdata')
    p.add_argument('--out',default='reports/pit-stage6')
    p.add_argument('--shard',type=int,default=0)
    p.add_argument('--pause',type=float,default=1.4)
    a=p.parse_args(argv)
    if a.mode=='verify':verify(a.stage6_dir,a.stage5_dir,a.price_dir,a.stage2_dir,a.stage4_dir,a.out)
    else:
        from pykrx import stock
        if a.mode=='canary':canary(a.stage2_dir,a.out,stock,pause=a.pause)
        else:scan(a.stage5_dir,a.out,stock,a.shard,pause=a.pause)

if __name__=='__main__':
    try:main()
    except Exception as e:
        print('FAIL_CLOSED_STAGE6',type(e).__name__,str(e),file=sys.stderr,flush=True)
        sys.exit(2)
