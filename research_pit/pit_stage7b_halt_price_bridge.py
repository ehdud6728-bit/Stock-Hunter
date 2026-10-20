#!/usr/bin/env python3
"""Stage7-B: official KRX halt START events vs immutable daily PRICE/VOLUME prints.

RESEARCH_ONLY. No suspension end dates or daily trading eligibility are inferred
from zero volume, a later positive trade, or the provider's LST_TRD_DD.
No strategy/LIVE/order imports. This program NEVER promotes strategy_ready.
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
from collections import defaultdict
from datetime import datetime, timezone

SCHEMA='STOCKHUNTER_PIT_STAGE7B_OFFICIAL_HALT_PRICE_OBSERVATION_BRIDGE_V1'
SOURCE_STAGE1_RUN=39295295197
SOURCE_STAGE1_SHA='08e328c101f0774a2236c0a44cc1fdbbaa59e96e'
SOURCE_STAGE2_RUN=39343301692
SOURCE_STAGE6_RUN=39408210227
SOURCE_STAGE7A_RUN=39430555533
SOURCE_STAGE7A_SHA='d60639f9f9920290b51dbb9e502ead0a4cbe254f'
EXPECTED_DAYS=1907
EXPECTED_ROWS=5090428
EXPECTED_CODES=3341
EXPECTED_ZERO_VOLUME_ALL=229867
SAMPLE_CODES=('377460','418250')
STARTS={'377460':'2023-11-28','418250':'2024-04-05'}
MARKET_HEADER=('code','date','open','high','low','close','volume','amount')
DATE_RE=re.compile(r'\d{4}-\d{2}-\d{2}\Z')


def reject(detail):raise RuntimeError('FAIL_CLOSED_STAGE7B_'+str(detail))
def digest(b):return hashlib.sha256(b).hexdigest()
def jload(p):return json.loads(pathlib.Path(p).read_text(encoding='utf8'))
def output(path,data):
    p=pathlib.Path(path);p.parent.mkdir(parents=True,exist_ok=True)
    b=data.encode('utf8') if isinstance(data,str) else data
    tmp=p.with_name(p.name+'.part');tmp.write_bytes(b);tmp.replace(p)
def jout(path,data):output(path,json.dumps(data,sort_keys=True,ensure_ascii=False,indent=2)+'\n')
def cout(path,fields,rows):
    s=io.StringIO(newline='');w=csv.DictWriter(s,fieldnames=fields,extrasaction='ignore')
    w.writeheader();w.writerows(rows);output(path,s.getvalue())
def cinput(path):
    with pathlib.Path(path).open(encoding='utf-8-sig',newline='') as f:return list(csv.DictReader(f))
def dated(s):
    if not DATE_RE.fullmatch(str(s)):reject('INVALID_ISO_DATE:'+str(s))
    try:return datetime.strptime(s,'%Y-%m-%d').date()
    except ValueError:reject('INVALID_DATE:'+str(s))


def source_calendar(stage2,strict=True):
    root=pathlib.Path(stage2)
    summary=jload(root/'lifecycle_summary.json')
    reference=jload(root/'source_fullmarket_price_verification.json')
    values={}
    for row in cinput(root/'all_session_observation_counts.csv'):
        day=row['date'];n=int(row['raw_price_rows']);dated(day)
        if day in values or n<1:reject('BAD_STAGE2_DAY:'+day)
        values[day]=n
    values=dict(sorted(values.items()))
    if strict:
        if (summary.get('source_run_id')!=SOURCE_STAGE1_RUN or
            summary.get('source_head_sha')!=SOURCE_STAGE1_SHA or
            summary.get('observed_codes')!=EXPECTED_CODES or
            summary.get('price_rows')!=EXPECTED_ROWS or
            reference.get('full_market_price_rows')!=EXPECTED_ROWS or
            reference.get('sessions')!=EXPECTED_DAYS or
            reference.get('whole_market_pit_strategy_ready') is not False or
            len(values)!=EXPECTED_DAYS or sum(values.values())!=EXPECTED_ROWS or
            next(iter(values))!='2019-01-02' or list(values)[-1]!='2026-10-12' or
            digest('\n'.join(values).encode())!=reference.get('calendar_sha256')):
            reject('STAGE2_CALENDAR_OR_PINS_MISMATCH')
    return values,reference


def load_events(stage7, strict=True):
    root=pathlib.Path(stage7);summary=jload(root/'stage7_official_halt_canary_summary.json')
    mf=jload(root/'official_halt_raw_manifest.json')
    if (mf.get('status')!='COMPLETE' or not isinstance(mf.get('files'),list)):
        reject('INCOMPLETE_STAGE7A_MANIFEST')
    if strict and (summary.get('source_stage1_run')!=SOURCE_STAGE1_RUN or
                   summary.get('source_stage6_run')!=SOURCE_STAGE6_RUN or
                   summary.get('source_stage3_run')!=39364301572 or
                   summary.get('provider_code_window_queries')!=48 or
                   summary.get('provider_rows_returned')!=2 or
                   summary.get('provider_mapped_halt_event_rows')!=2 or
                   summary.get('provider_unmapped_or_conflicting_rows')!=0 or
                   summary.get('strategy_ready') is not False or
                   summary.get('official_halt_coverage_claim') is not False or
                   summary.get('status')!='PROVIDER_HALT_CANARY_HAS_MAPPED_EVENTS'):
        reject('STAGE7A_EXPECTED_POSITIVE_EVIDENCE_DRIFT')
    # Verify entire Stage7-A SHA-indexed 48-response raw source, not only positives.
    windows={};seen=set();events=[];total_rows=0
    for e in mf['files']:
        code=e['code'];start=e['start'];end=e['end'];rel=e['raw_file']
        if not re.fullmatch(r'raw_krx/issue_[0-9A-Z]{6}_\d{8}_\d{8}\.json',rel):reject('INVALID_RAW_PATH')
        key=(code,start,end)
        if key in seen:reject('DUPLICATE_STAGE7A_QUERY')
        seen.add(key);windows.setdefault(code,[]).append((start,end))
        raw=(root/rel).read_bytes()
        if digest(raw)!=e['sha256']:reject('STAGE7A_RAW_SHA_MISMATCH:'+rel)
        obj=json.loads(raw)
        rows=obj.get('output',obj.get('OutBlock_1'))
        if not isinstance(rows,list) or not all(isinstance(v,dict) for v in rows):reject('BAD_STAGE7A_ROWS:'+rel)
        total_rows+=len(rows)
        for row in rows:
            if code not in SAMPLE_CODES:reject('UNEXPECTED_EVENT_ROW:'+code)
            name=row.get('ISU_NM','');halt=row.get('TRD_HALT_DD','').replace('/','-')
            if row.get('ISU_CD')!=code or halt!=STARTS[code] or not (start<=halt.replace('-','')<=end):
                reject('MISMATCHED_POSITIVE_HALT_IDENTITY:'+code)
            resumed=row.get('RESUMP_DD','')
            if resumed: reject('UNEXPECTED_NONEMPTY_OFFICIAL_RESUME:'+code)
            events.append({'code':code,'name':name,'official_halt_event_date':halt,
                           'official_resume_date':None,'provider_last_trade_date_snapshot_only':row.get('LST_TRD_DD',''),
                           'stage7a_source_file':rel,'stage7a_source_sha256':e['sha256'],
                           'issue_isin_from_2019_2026_listing_event':e['source_listing_isin'],
                           'issue_isin_verified_for_all_historical_days':False})
    if strict and (len(mf['files'])!=48 or len(windows)!=12 or total_rows!=2 or len(events)!=2 or
                   {e['code'] for e in events}!=set(SAMPLE_CODES)):
        reject('STAGE7A_UNEXPECTED_EVENT_OR_QUERY_COUNTS')
    return sorted(events,key=lambda e:e['code']),{'source_manifest_sha256':digest((root/'official_halt_raw_manifest.json').read_bytes()),
        'raw_query_files_verified':len(seen),'official_halt_events':len(events),
        'explicit_official_resume_dates':sum(bool(e['official_resume_date']) for e in events)}


def verify_stage6_profiles(stage6,rows_by_code,strict=True):
    root=pathlib.Path(stage6);s=jload(root/'stage6_historical_market_cap_audit_summary.json')
    if strict and (s.get('source_stage1_run')!=SOURCE_STAGE1_RUN or
        s.get('source_stage5_run')!=39383970402 or
        s.get('cap_rows')!=EXPECTED_ROWS or s.get('distinct_codes')!=EXPECTED_CODES or
        s.get('zero_volume_proxy_rows')!=EXPECTED_ZERO_VOLUME_ALL or
        s.get('historical_market_cap_price_source_agrees') is not True or
        s.get('strategy_ready') is not False):reject('STAGE6_SOURCE_DRIFT')
    profiles=cinput(root/'all_3341_market_cap_observation_profiles.csv')
    if strict and len(profiles)!=EXPECTED_CODES:reject('STAGE6_CODEWISE_PROFILE_COUNT_DRIFT')
    by={x['code']:x for x in profiles}
    for code in SAMPLE_CODES:
        p=by.get(code)
        if not p or len(rows_by_code[code])!=int(p['observed_cap_sessions']) or sum(x['volume']==0 for x in rows_by_code[code])!=int(p['zero_volume_proxy_sessions']):
            reject('STAGE1_VS_STAGE6_OBSERVATION_OR_ZERO_VOLUME_DIFF:'+code)
        if p.get('official_suspension_verified') not in ('False',False):reject('STAGE6_FALSE_SUSPENSION_CLAIM:'+code)
    return {code:{'stage6_observed_cap_sessions':int(by[code]['observed_cap_sessions']),
                  'stage6_zero_volume_proxy_sessions':int(by[code]['zero_volume_proxy_sessions'])}
            for code in SAMPLE_CODES}


def price_observations(price_root,calendar,reference,targets=SAMPLE_CODES,shards=8,strict=True):
    root=pathlib.Path(price_root);target=set(targets)
    manifests={};sums=0;seen_dates=set()
    days=list(calendar)
    for shard in range(shards):
        base=root/f'shard_{shard:02d}'
        mf=jload(base/f'manifest_shard_{shard:02d}.json')
        expected=days[shard::shards]
        if (mf.get('status')!='COMPLETE_PRICE_COLLECTION_ONLY' or mf.get('universe_filter')!='NONE' or
            mf.get('shard')!=shard or mf.get('shards')!=shards or
            [f['date'] for f in mf['files']]!=expected):reject('STAGE1_PRICE_MANIFEST_DRIFT:'+str(shard))
        if strict and (mf.get('calendar_sha256')!=reference['calendar_sha256'] or
            mf.get('index_sha256')!=reference['index_sha256'] or
            mf.get('official_days')!=EXPECTED_DAYS or mf.get('start')!='2019-01-02' or mf.get('end')!='2026-10-12'):
            reject('STAGE1_PRICE_PROVENANCE_DRIFT:'+str(shard))
        manifests[shard]=(base,mf)
    output_rows={x:[] for x in target}
    manifest_sha={}   # SHA of each originally pinned manifest file
    for shard,(base,mf) in manifests.items():
        manifest_sha[shard]=digest((base/f'manifest_shard_{shard:02d}.json').read_bytes())
        for row_meta in mf['files']:
            day=row_meta['date'];seen_dates.add(day)
            raw=(base/row_meta['file']).read_bytes()
            if digest(raw)!=row_meta['sha256']:reject('SOURCE_PRICE_DAY_SHA_MISMATCH:'+day)
            if not row_meta['file'].startswith(day.replace('-','')):reject('BAD_DAY_FILE_PATH:'+day)
            found={};cnt=0
            try:
                with gzip.GzipFile(fileobj=io.BytesIO(raw)) as f:
                    stream=io.TextIOWrapper(f,encoding='utf8',newline='')
                    reader=csv.DictReader(stream)
                    if tuple(reader.fieldnames or ())!=MARKET_HEADER:reject('PRICE_BAD_CSV_COLUMNS:'+day)
                    for v in reader:
                        cnt+=1
                        if v['code'] in target:
                            code=v['code']
                            if code in found or v['date']!=day:reject('DUPLICATE_OR_MISDATED_FOCUS_CODE:'+day+':'+code)
                            found[code]=True
                            try:
                                close=int(v['close']);volume=int(v['volume']);amount=int(v['amount'])
                                if close<0 or volume<0 or amount<0:raise ValueError('negative')
                            except (ValueError,TypeError):reject('FOCUS_NUMERIC_VALUE_INVALID:'+code+':'+day)
                            output_rows[code].append({'code':code,'date':day,'close':close,'volume':volume,'amount':amount})
            except (OSError,UnicodeError,csv.Error) as e:reject('BAD_PRICE_CSV_GZ:'+day+':'+str(e))
            if cnt!=calendar[day] or cnt!=row_meta['stored_rows']:reject('PRICE_DAY_ROW_COUNT_DIFF:'+day)
            sums+=cnt
    if len(seen_dates)!=len(calendar) or sums!=sum(calendar.values()) or (strict and sums!=EXPECTED_ROWS):
        reject('SOURCE_PRICE_TOTAL_MISMATCH')
    for code in target:
        output_rows[code].sort(key=lambda v:v['date'])
        if not output_rows[code]:reject('FOCUS_CODE_MISSING:'+code)
        if len(output_rows[code])!=len({x['date'] for x in output_rows[code]}):reject('FOCUS_REPEATED_SESSION:'+code)
    return output_rows,{'validated_price_shard_manifests':shards,'original_price_manifest_sha256':manifest_sha,
                         'dated_price_files_sha_verified':len(seen_dates), 'all_market_price_rows_counted':sums,
                         'price_source_recollected':False}


def make_bridge(events,observations,days_before=12,days_after=15):
    summaries=[];all_rows=[];neighborhood=[]
    for event in events:
        code=event['code'];halt=event['official_halt_event_date'];seq=observations[code]
        before=[x for x in seq if x['date']<halt];on=[x for x in seq if x['date']==halt]
        after=[x for x in seq if x['date']>halt]
        if not before or not after or len(on)!=1:reject('FOCUS_MISSING_PRE_POST_HALT_DAY:'+code)
        on=on[0]
        last_pre_positive=next((v for v in reversed(before) if v['volume']>0),None)
        first_after_positive=next((v for v in after if v['volume']>0),None)
        last_after_positive=next((v for v in reversed(after) if v['volume']>0),None)
        first_after_zero=next((v for v in after if v['volume']==0),None)
        # Provider's LST_TRD_DD is a returned snapshot field, not a dated PIT execution fact.
        provider_day_txt=str(event.get('provider_last_trade_date_snapshot_only','')).replace('/','-')
        provider_day=provider_day_txt if DATE_RE.fullmatch(provider_day_txt) else ''
        recorded_on_provider_day=next((v for v in seq if v['date']==provider_day),None)
        post_run=0
        for v in after:
            if v['volume']!=0:break
            post_run+=1
        rec=dict(event,
            price_observed_first=seq[0]['date'],price_observed_last=seq[-1]['date'],
            price_total_observed_sessions=len(seq),price_all_history_zero_volume_sessions=sum(x['volume']==0 for x in seq),
            positive_volume_last_pre_event=last_pre_positive['date'] if last_pre_positive else '',
            last_pre_event_volume=before[-1]['volume'],
            event_day_price_observed=True,event_day_volume=on['volume'],event_day_amount=on['amount'],
            event_day_volume_positive=(on['volume']>0),
            provider_last_trade_date_price_observed=(recorded_on_provider_day is not None),
            provider_last_trade_date_price_volume=(recorded_on_provider_day['volume'] if recorded_on_provider_day is not None else ''),
            provider_last_trade_date_positive_volume_corrob=(recorded_on_provider_day['volume']>0 if recorded_on_provider_day is not None else False),
            provider_last_trade_date_is_official_resume_evidence=False,
            provider_last_trade_date_semantics_unverified=True,
            strictly_post_event_observed_sessions=len(after),
            strictly_post_event_zero_volume_sessions=sum(x['volume']==0 for x in after),
            strictly_post_event_positive_volume_sessions=sum(x['volume']>0 for x in after),
            zero_volume_run_immediately_after_event_sessions=post_run,
            first_zero_volume_after_event=first_after_zero['date'] if first_after_zero else '',
            first_positive_volume_after_event=first_after_positive['date'] if first_after_positive else '',
            last_positive_volume_after_event=last_after_positive['date'] if last_after_positive else '',
            actual_official_resume_date_confirmed=False,
            after_event_positive_trade_is_official_resume_evidence=False,
            daily_halt_state_confirmed=False,
            source_event_start_verified=True,
            official_halt_duration_days_confirmed=False,
            event_day_positive_volume_does_not_invalidate_halt_start=True,
            assessment='OFFICIAL_HALT_START_CONFIRMED; DAILY_HALT/RESUME/EXECUTION_STATUS_UNKNOWN')
        summaries.append(rec)
        around=before[-days_before:]+[on]+after[:days_after]
        around_key={x['date'] for x in around}
        for v in seq:
            relation=('BEFORE_EVENT' if v['date']<halt else 'EVENT_START_DAY' if v['date']==halt else 'AFTER_EVENT')
            point={'code':code,'date':v['date'],'official_halt_event_date':halt,'time_relation_to_halt_event':relation,
                   'close':v['close'],'volume':v['volume'],'amount':v['amount'],
                   'zero_volume_observed_proxy':v['volume']==0,
                   'official_daily_halt_status':'UNKNOWN',
                   'official_resume_date':'UNKNOWN',
                   'tradability_for_backtest':'UNKNOWN',
                   'research_only':True,'strategy_ready':False}
            all_rows.append(point)
            if v['date'] in around_key:neighborhood.append(dict(point))
    return summaries,all_rows,neighborhood


def run(stage2,stage6,stage7,price,out,strict=True):
    root=pathlib.Path(out);root.mkdir(parents=True,exist_ok=True)
    # Preserve diagnostics after each independent provenance check.
    calendar,ref=source_calendar(stage2,strict)
    events,ev_audit=load_events(stage7,strict)
    source_rows,px_audit=price_observations(price,calendar,ref,strict=strict)
    cap_profiles=verify_stage6_profiles(stage6,source_rows,strict)
    events_summary,detail,around=make_bridge(events,source_rows)
    assert all(x['official_halt_duration_days_confirmed'] is False for x in events_summary)
    cout(root/'stage7b_2_official_events_vs_price.csv',list(events_summary[0]),events_summary)
    cout(root/'stage7b_2_codes_all_observed_price_days.csv',list(detail[0]),detail)
    cout(root/'stage7b_2_event_neighborhood.csv',list(around[0]),around)
    summary={
        'schema':SCHEMA,'status':'OFFICIAL_HALT_START_VS_IMMUTABLE_PRICE_OBSERVATION_MATCH_ONLY',
        'research_only':True,'strategy_ready':False,'production_or_rank_changed':False,
        'source_stage1_run':SOURCE_STAGE1_RUN,'source_stage1_commit':SOURCE_STAGE1_SHA,
        'source_stage2_run':SOURCE_STAGE2_RUN,'source_stage6_run':SOURCE_STAGE6_RUN,
        'source_stage7a_run':SOURCE_STAGE7A_RUN,'source_stage7a_commit':SOURCE_STAGE7A_SHA,
        'reviewed_codes':list(SAMPLE_CODES),'official_halt_start_event_rows':len(events_summary),
        'official_resume_event_rows':0,'focus_price_observations':len(detail),
        'source_original_daily_price_sha_verified':True,
        'historical_isin_all_dates_verified':False,'historical_security_type_all_dates_verified':False,
        'official_halt_duration_verified':False,'full_market_official_halt_coverage_verified':False,
        'price_positive_is_official_resume':False,'price_zero_is_official_suspension':False,
        'daily_execution_feasibility_verified':False,'source_stage7a':ev_audit,
        'source_stage1':px_audit,'source_stage6_codewise_profiles':cap_profiles,
        'focus_event_comparisons':events_summary,
        'generated_at_utc':datetime.now(timezone.utc).isoformat(),
        'interpretation':'Start-day event is official; post-event zero volume is only a price observation. '
             'A positive print, including on the halt date, is not an official resume, and LST_TRD_DD is not historical PIT resume evidence. '
             'Unknown status remains UNKNOWN. Backtesting gate remains CLOSED.'}
    jout(root/'stage7b_halt_price_bridge_summary.json',summary)
    print(json.dumps({k:summary[k] for k in ('status','reviewed_codes','official_halt_start_event_rows','focus_price_observations','strategy_ready')},ensure_ascii=False),flush=True)
    for v in events_summary:
        print('STAGE7B_EVENT',v['code'],'halt=',v['official_halt_event_date'],'startday_volume=',v['event_day_volume'],
              'zero_after=',v['strictly_post_event_zero_volume_sessions'],
              'positive_after=',v['strictly_post_event_positive_volume_sessions'],
              'official_resume=UNKNOWN',flush=True)
    return summary


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--stage2-dir',default='stage2-source')
    p.add_argument('--stage6-dir',default='stage6-source')
    p.add_argument('--stage7-dir',default='stage7a-source')
    p.add_argument('--price-dir',default='pitdata')
    p.add_argument('--out',default='reports/pit-stage7b')
    a=p.parse_args()
    run(a.stage2_dir,a.stage6_dir,a.stage7_dir,a.price_dir,a.out)

if __name__=='__main__':
    try:main()
    except Exception as e:
        print('STAGE7B_FAIL_CLOSED',type(e).__name__,str(e),file=sys.stderr,flush=True)
        sys.exit(2)
