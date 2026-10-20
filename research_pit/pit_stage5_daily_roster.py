#!/usr/bin/env python3
"""PIT Stage5 -- KRX *dated market rosters*, cross-check with immutable price-source codes.

RESEARCH_ONLY. Dated KOSPI/KOSDAQ/KONEX membership does NOT establish issue type,
ISIN, corporate action, trading suspension, ordinary shares, capitalization,
tradability, financials, or a backtest-eligible security master. No LIVE imports.
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
from datetime import datetime, timezone

SCHEMA='STOCKHUNTER_PIT_STAGE5_DATED_KRX_MARKET_ROSTER_V1'
MARKETS=('KOSPI','KOSDAQ','KONEX')
P_SOURCE_RUN=39295295197
P_SOURCE_SHA='08e328c101f0774a2236c0a44cc1fdbbaa59e96e'
P_STAGE2_RUN=39343301692
P_STAGE4_RUN=39373355974
EXPECTED_DAYS=1907
EXPECTED_ROWS=5090428
EXPECTED_CODES=3341
EXPECTED_UNRESOLVED=2023
P_DATE_START='2019-01-02'
P_DATE_END='2026-10-12'
CODE_RE=re.compile(r'[0-9][0-9A-Z]{5}\Z')


def stop(msg):raise RuntimeError('FAIL_CLOSED_STAGE5_'+str(msg))
def digest(data):return hashlib.sha256(data).hexdigest()
def write(path,data):
    p=pathlib.Path(path);p.parent.mkdir(parents=True,exist_ok=True)
    if isinstance(data,str):data=data.encode('utf-8')
    tmp=p.with_name(p.name+'.part');tmp.write_bytes(data);tmp.replace(p)
def jbytes(v):return (json.dumps(v,sort_keys=True,ensure_ascii=False,separators=(',',':'))+'\n').encode('utf-8')
def wjson(path,v):write(path,json.dumps(v,ensure_ascii=False,sort_keys=True,indent=2)+'\n')
def wcsv(path,fields,rows):
    b=io.StringIO(newline='');w=csv.DictWriter(b,fieldnames=fields,extrasaction='ignore');w.writeheader();w.writerows(rows);write(path,b.getvalue())
def rjson(p):return json.loads(pathlib.Path(p).read_text(encoding='utf-8'))


def source_stage2(stage2_dir, strict=True):
    root=pathlib.Path(stage2_dir)
    summary=rjson(root/'lifecycle_summary.json')
    price=rjson(root/'source_fullmarket_price_verification.json')
    if strict and (summary.get('source_run_id')!=P_SOURCE_RUN or summary.get('source_head_sha')!=P_SOURCE_SHA or
                   summary.get('observed_codes')!=EXPECTED_CODES or summary.get('price_rows')!=EXPECTED_ROWS or
                   price.get('full_market_price_rows')!=EXPECTED_ROWS or price.get('sessions')!=EXPECTED_DAYS or
                   price.get('whole_market_pit_strategy_ready') is not False):
        stop('UNPINNED_STAGE2_SOURCE')
    dates={}
    with (root/'all_session_observation_counts.csv').open(encoding='utf-8-sig',newline='') as f:
        for row in csv.DictReader(f):
            d=row['date'];n=int(row['raw_price_rows'])
            if d in dates or n<=0:stop('BAD_STAGE2_DATE:'+d)
            dates[d]=n
    if strict and (len(dates)!=EXPECTED_DAYS or sorted(dates)[0]!=P_DATE_START or sorted(dates)[-1]!=P_DATE_END or
                   sum(dates.values())!=EXPECTED_ROWS or
                   digest('\n'.join(sorted(dates)).encode())!=price.get('calendar_sha256')):
        stop('STAGE2_CALENDAR_COUNT_OR_SHA_MISMATCH')
    return dict(sorted(dates.items()))


def source_stage4(stage4_dir, strict=True):
    root=pathlib.Path(stage4_dir)
    report=rjson(root/'stage4_event_type_audit_summary.json')
    if strict and (report.get('observed_codes')!=EXPECTED_CODES or report.get('codes_without_usable_event_type')!=EXPECTED_UNRESOLVED or
                   report.get('strategy_ready') is not False or report.get('source_stage2_run')!=P_STAGE2_RUN or
                   report.get('source_stage3_run')!=39364301572 or
                   report.get('source_price_sha')!=P_SOURCE_SHA or report.get('source_price_run')!=P_SOURCE_RUN):
        stop('UNPINNED_STAGE4_TYPE_EVIDENCE')
    codes={}
    with (root/'all_3341_event_type_evidence.csv').open(encoding='utf-8-sig',newline='') as f:
        for row in csv.DictReader(f):
            c=row['code']
            if not CODE_RE.fullmatch(c) or c in codes:stop('BAD_STAGE4_CODE:'+c)
            codes[c]=row
    if strict and (len(codes)!=EXPECTED_CODES or sum(x['type_evidence_label']=='UNRESOLVED_NO_2019_2026_EVENT' for x in codes.values())!=EXPECTED_UNRESOLVED):
        stop('STAGE4_TYPE_EVIDENCE_COUNT_DRIFT')
    return codes


def normalize_roster(day, market_rows):
    """All source tickers retained verbatim; no historical label derived from code format."""
    observed={};markets={}
    for m in MARKETS:
        values=market_rows.get(m)
        if not isinstance(values,(list,tuple)) or len(values)<15:stop('EMPTY_DATED_MARKET:'+day+':'+m)
        codes=[str(v).strip() for v in values]
        if any(not CODE_RE.fullmatch(c) for c in codes):stop('BAD_KRX_MARKET_CODE:'+day+':'+m)
        if len(codes)!=len(set(codes)):stop('DUPLICATE_MARKET_CODE:'+day+':'+m)
        for code in codes:
            if code in observed:stop('TICKER_IN_MULTIPLE_MARKETS:'+day+':'+code)
            observed[code]=m
        markets[m]=sorted(codes)
    return {'schema':SCHEMA,'date':day,'market_codes':markets,'listed_union_rows':len(observed),
            'source':'KRX stock.get_market_ticker_list(dated) - same exchange, not independent audit',
            'security_type_verified':False,'strategy_ready':False}


def query_one_day(stock, day, pause=1.2, retries=3):
    markets={}
    for m in MARKETS:
        failure=None
        for attempt in range(retries):
            try:
                vals=stock.get_market_ticker_list(day.replace('-',''),market=m)
                if not isinstance(vals,(tuple,list)) or len(vals)<15:stop('KRX_UNDERPOPULATED:'+day+':'+m)
                markets[m]=list(vals);failure=None;break
            except Exception as e:
                failure=e
                if attempt+1<retries:time.sleep(max(pause,0.1)*(attempt+1))
        if failure:stop('KRX_ROSTER_RETRY_EXHAUSTED:'+day+':'+m+':'+str(failure)[:150])
        if pause:time.sleep(pause)
    return normalize_roster(day,markets)


def scan(stage2, out, stock, shard_index=0, shard_count=8, pause=1.2, canary=False, strict=True):
    dates=source_stage2(stage2,strict=strict)
    root=pathlib.Path(out)
    if canary:
        pin=rjson(pathlib.Path(stage2)/'dated_market_membership_snapshots.json')
        wanted=sorted({item['date'] for item in pin})
        if strict and len(wanted)!=4:stop('STAGE2_CANARY_EXPECTED_FOUR_DATES')
    else:
        if not (0<=shard_index<shard_count<=16):stop('BAD_SHARD_INDEX_OR_COUNT')
        wanted=list(dates)[shard_index::shard_count]
    shards=[];failures=[];mismatch=[]
    for n,day in enumerate(wanted,1):
        payload=query_one_day(stock,day,pause=pause)
        count_mismatch=payload['listed_union_rows']!=dates[day]
        if count_mismatch:mismatch.append({'date':day,'price_rows':dates[day],'market_rows':payload['listed_union_rows']})
        if canary:
            matched=next(x for x in pin if x['date']==day)
            if payload['market_codes']!={m:sorted(matched['market_codes'][m]) for m in MARKETS}:
                stop('DATED_SOURCE_CANARY_IDENTITY_MISMATCH:'+day)
        b=gzip.compress(jbytes(payload),mtime=0)
        relative=day.replace('-','')+'.json.gz'
        write(root/'days'/relative,b)
        shards.append({'date':day,'file':'days/'+relative,'sha256':digest(b),
                       'market_rows':payload['listed_union_rows'],'expected_price_rows':dates[day],
                       'count_mismatch':count_mismatch})
        wjson(root/'manifest.json',{'schema':SCHEMA,'status':'INCOMPLETE','canary':canary,
                                   'shard_index':shard_index,'shard_count':shard_count,'files':shards,
                                   'source_price_run':P_SOURCE_RUN,'source_stage2_run':P_STAGE2_RUN})
        if n%25==0:print('STAGE5_PROGRESS',shard_index,n,len(wanted),day,flush=True)
    report={'schema':SCHEMA,'status':'COMPLETE_DATED_MARKET_ROSTER_EVIDENCE_ONLY','canary':canary,
            'shard_index':shard_index,'shard_count':shard_count,'files':shards,
            'source_price_run':P_SOURCE_RUN,'source_stage2_run':P_STAGE2_RUN,
            'count_mismatch_days':mismatch,'sessions':len(shards),
            'date_scoped_security_type_verified':False,'daily_tradability_verified':False,
            'strategy_ready':False}
    wjson(root/'manifest.json',report)
    print(json.dumps({k:report[k] for k in ('status','sessions','canary','shard_index','count_mismatch_days')},ensure_ascii=False),flush=True)
    if canary and mismatch:stop('CANARY_ROW_COUNTS_MISMATCH')
    return report


def read_day_price_codes(base, manifest, date, exp):
    info=next((e for e in manifest['files'] if e['date']==date),None)
    if info is None:stop('MISSING_PRICE_DAY_MANIFEST:'+date)
    f=base/info['file']
    if not f.is_file():stop('MISSING_PRICE_DAY:'+date)
    raw=f.read_bytes()
    if digest(raw)!=info['sha256']:stop('BAD_PRICE_DAY_SHA:'+date)
    codes=set()
    try:
        with gzip.open(io.BytesIO(raw),'rt',encoding='utf-8',newline='') as g:
            rd=csv.DictReader(g)
            if tuple(rd.fieldnames or ())!=('code','date','open','high','low','close','volume','amount'):
                stop('BAD_PRICE_SOURCE_HEADER:'+date)
            for row in rd:
                c=row['code']
                if row['date']!=date or not CODE_RE.fullmatch(c) or c in codes:
                    stop('BAD_PRICE_SOURCE_IDENTITY:'+date+':'+c)
                codes.add(c)
    except (OSError,UnicodeError,csv.Error) as ex:stop('UNREADABLE_SOURCE_CSV:'+date+':'+repr(ex))
    if len(codes)!=exp or info['stored_rows']!=exp:stop('PRICE_SOURCE_ROWS_MISMATCH:'+date)
    return codes


def verify_full(roster_dir, price_dir, stage2, stage4, out, shards=8, strict=True):
    dates=source_stage2(stage2,strict=strict)
    types=source_stage4(stage4,strict=strict)
    rbase=pathlib.Path(roster_dir);pbase=pathlib.Path(price_dir)
    manifest_by_shard={};pmanifest_by_shard={}
    source_price_evidence=rjson(pathlib.Path(stage2)/'source_fullmarket_price_verification.json')
    for shard in range(shards):
        rm=rjson(rbase/f'shard_{shard:02d}'/'manifest.json')
        pm=rjson(pbase/f'shard_{shard:02d}'/f'manifest_shard_{shard:02d}.json')
        if (rm.get('status')!='COMPLETE_DATED_MARKET_ROSTER_EVIDENCE_ONLY' or rm.get('canary') or
            rm.get('shard_index')!=shard or rm.get('shard_count')!=shards or
            rm.get('source_price_run')!=P_SOURCE_RUN or rm.get('source_stage2_run')!=P_STAGE2_RUN):
            stop('ROSTER_SHARD_MANIFEST_NOT_TRUSTED:'+str(shard))
        if (pm.get('status')!='COMPLETE_PRICE_COLLECTION_ONLY' or pm.get('universe_filter')!='NONE' or
            pm.get('shard')!=shard or pm.get('shards')!=8):stop('PRICE_SHARD_MANIFEST_NOT_TRUSTED:'+str(shard))
        if strict and (pm.get('calendar_sha256')!=source_price_evidence['calendar_sha256'] or
                       pm.get('index_sha256')!=source_price_evidence['index_sha256'] or
                       pm.get('official_days')!=EXPECTED_DAYS or pm.get('start')!=P_DATE_START or
                       pm.get('end')!=P_DATE_END):stop('PRICE_SHARD_PROVENANCE_DRIFT:'+str(shard))
        expected=list(dates)[shard::shards]
        if [f['date'] for f in rm['files']]!=expected or [f['date'] for f in pm['files']]!=expected:
            stop('SHARD_DATE_GAP_OR_ORDER:'+str(shard))
        manifest_by_shard[shard]=rm
        pmanifest_by_shard[shard]=pm
    summary_rows=[];issues=[];groups=defaultdict(Counter);periods=defaultdict(list)
    all_roster_codes=set();price_total=0;roster_total=0;mismatches=0
    for i,day in enumerate(dates):
        s=i%shards;rm=manifest_by_shard[s];pm=pmanifest_by_shard[s]
        re=rm['files'][i//shards];root=rbase/f'shard_{s:02d}'
        data=(root/re['file']).read_bytes()
        if digest(data)!=re['sha256']:stop('ROSTER_SHA_FAILURE:'+day)
        try:roster=json.loads(gzip.decompress(data))
        except (OSError,ValueError) as e:stop('BAD_ROSTER_GZIP:'+day+':'+str(e))
        if roster.get('date')!=day or roster.get('schema')!=SCHEMA or roster.get('strategy_ready') is not False:
            stop('INVALID_DATED_ROSTER:'+day)
        markets=roster['market_codes'];check=normalize_roster(day,markets)
        if check['market_codes']!=markets or check['listed_union_rows']!=roster.get('listed_union_rows'):
            stop('MUTATED_MARKET_ROSTER:'+day)
        seen=set().union(*map(set,markets.values()))
        if re['market_rows']!=len(seen) or re['expected_price_rows']!=dates[day]:stop('ROSTER_MANIFEST_ROWS_DRIFT:'+day)
        source=read_day_price_codes(pbase/f'shard_{s:02d}',pm,day,dates[day])
        extra=sorted(seen-source);absent=sorted(source-seen)
        if extra or absent:mismatches+=1;issues.append({'date':day,'roster_only_codes':';'.join(extra),'price_only_codes':';'.join(absent),'roster_only_n':len(extra),'price_only_n':len(absent)})
        for market,codes in markets.items():
            for code in codes:
                groups[code][market]+=1
                periods[code].append((day,market))
        all_roster_codes|=seen;price_total+=len(source);roster_total+=len(seen)
        summary_rows.append({'date':day,'price_rows':len(source), 'roster_rows':len(seen),
                             'kospi':len(markets['KOSPI']),'kosdaq':len(markets['KOSDAQ']),'konex':len(markets['KONEX']),
                             'roster_only_codes':len(extra),'price_only_codes':len(absent),'exact_code_match':not (extra or absent)})
        if (i+1)%250==0:print('STAGE5_VERIFY_PROGRESS',i+1,len(dates),flush=True)
    terminal_rows=[];by_code=[]
    unresolved=0;unresolved_on_roster=0
    for code in sorted(set(types)|all_roster_codes):
        info=types.get(code)
        item=periods.get(code,[])
        label=info['type_evidence_label'] if info else 'NOT_IN_STAGE4_PRICE_SOURCE'
        if label=='UNRESOLVED_NO_2019_2026_EVENT':
            unresolved+=1
            if item:unresolved_on_roster+=1
        ms=sorted(set(m for _,m in item))
        row={'code':code,'stage4_type_evidence_label':label,
             'stage4_classification_retained_unmodified':True,
             'dated_kospi_sessions':groups[code]['KOSPI'],'dated_kosdaq_sessions':groups[code]['KOSDAQ'],
             'dated_konex_sessions':groups[code]['KONEX'],'dated_listed_sessions':len(item),
             'first_roster_date':item[0][0] if item else '',
             'last_roster_date':item[-1][0] if item else '',
             'markets_seen':';'.join(ms),'market_transition_count':sum(x[1]!=y[1] for x,y in zip(item,item[1:])),
             'pit_listed_market_observed':bool(item),
             'ordinary_share_verified':False,'security_type_verified':False,
             'isin_verified':False,'official_suspension_verified':False,'historical_market_cap_verified':False,
             'strategy_ready':False}
        by_code.append(row)
        if label=='UNRESOLVED_NO_2019_2026_EVENT':terminal_rows.append(row)
    out=pathlib.Path(out);out.mkdir(parents=True,exist_ok=True)
    wcsv(out/'all_1907_dates_crossendpoint_comparison.csv',list(summary_rows[0]),summary_rows)
    wcsv(out/'date_code_identity_mismatches.csv',['date','roster_only_codes','price_only_codes','roster_only_n','price_only_n'],issues)
    wcsv(out/'all_3341_dated_market_observation_profiles.csv',list(by_code[0]),by_code)
    wcsv(out/'unresolved_2023_dated_market_profiles.csv',list(by_code[0]),terminal_rows)
    report={'schema':SCHEMA,'status':'DATED_KRX_MARKET_ROSTER_VERIFIED_PRICE_IDENTITIES' if mismatches==0 and strict and len(dates)==EXPECTED_DAYS else 'DATED_MARKET_EVIDENCE_INCOMPLETE_OR_DIFFERENT',
            'source_stage1_run':P_SOURCE_RUN,'source_stage1_commit':P_SOURCE_SHA,'source_stage2_run':P_STAGE2_RUN,
            'source_stage4_run':P_STAGE4_RUN,'sessions':len(dates),'price_rows':price_total,'roster_rows':roster_total,
            'exact_same_date_code_match_days':len(dates)-mismatches,'mismatch_days':mismatches,
            'source_stage4_codes':len(types),'roster_distinct_codes':len(all_roster_codes),
            'unresolved_stage4_type_codes':unresolved,'unresolved_codes_with_dated_market_roster':unresolved_on_roster,
            'historical_security_type_verified':False,'historical_market_cap_verified':False,
            'historical_isin_verified':False,'official_suspension_verified':False,
            'pit_ordinary_share_master_ready':False,'strategy_ready':False,'research_only':True,
            'important_limit':'KRX dated-market listings corroborate exchange membership; they do not establish common-stock class, ISIN, daily suspension, eligibility or capitalization. Stage4 2,023 unresolved security types MUST remain unresolved.'}
    wjson(out/'stage5_dated_market_roster_audit_summary.json',report)
    print(json.dumps(report,ensure_ascii=False,sort_keys=True),flush=True)
    if mismatches or len(by_code)!=len(types) or (strict and (price_total!=EXPECTED_ROWS or len(dates)!=EXPECTED_DAYS or len(by_code)!=EXPECTED_CODES or unresolved!=EXPECTED_UNRESOLVED)):
        stop('FULL_DATE_PRICE_ROSTER_IDENTITIES_NOT_APPROVED')
    return report


def main(argv=None):
    p=argparse.ArgumentParser()
    p.add_argument('--mode',choices=['canary','scan','verify'],required=True)
    p.add_argument('--stage2-dir',default='stage2-source')
    p.add_argument('--stage4-dir',default='stage4-source')
    p.add_argument('--root',default='pitdata')
    p.add_argument('--roster-dir',default='stage5-rosters')
    p.add_argument('--out',default='reports/pit-stage5')
    p.add_argument('--shard-index',type=int,default=0)
    p.add_argument('--shards',type=int,default=8)
    p.add_argument('--pause',type=float,default=1.2)
    v=p.parse_args(argv)
    if v.mode in ('canary','scan'):
        from pykrx import stock
        scan(v.stage2_dir,v.out,stock,shard_index=v.shard_index,shard_count=v.shards,
             canary=v.mode=='canary',pause=v.pause)
    else:verify_full(v.roster_dir,v.root,v.stage2_dir,v.stage4_dir,v.out,shards=v.shards)

if __name__=='__main__':
    try:main()
    except Exception as e:
        print('FAIL_CLOSED_STAGE5_RUN',type(e).__name__,str(e),file=sys.stderr,flush=True)
        raise SystemExit(2)
