#!/usr/bin/env python3
"""CORE224/LOW224 KRX-native source truth R1 (RESEARCH_ONLY).

Immutable existing LOW224 587 signals are a source-drift test cohort, NOT an OOS
profitability sample. Historical KRX cross-sectional original price + actual
Amount share the same session source. Raw source file validation is strict.
No live, trading, ranking or 224-rule changes.
"""
from __future__ import annotations
import argparse
import csv
import gzip
import hashlib
import io
import json
import math
from pathlib import Path
from collections import defaultdict
import pandas as pd

FORMAT = 'STOCKHUNTER_KRX_ALL_SESSION_V1'
FIELDS = ['code','date','open','high','low','close','volume','amount']
FROZEN_SHA = '1999b2bdf6bd172642bf5348b778d03b6184afe28c6357c5a103a053cb731c8d'
REPORT_SCHEMA = 'CORE224_KRX_NATIVE_SOURCE_TRUTH_R1'
HORIZONS = (5,10,15)
COST_ROUND_TRIP_PCT = 0.20  # purely illustrative 20bp; include alternative stress scenarios later

def sha(b): return hashlib.sha256(b).hexdigest()
def fail(s): raise RuntimeError('FAIL_CLOSED_'+str(s))

def write_csv(rows,path,cols=None):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    if not cols:
        cols=[];known=set()
        for r in rows:
            for key in r:
                if key not in known:cols.append(key);known.add(key)
        if not cols:cols=['status']
    with path.open('w',encoding='utf-8-sig',newline='') as f:
        w=csv.DictWriter(f,fieldnames=cols,extrasaction='ignore');w.writeheader();w.writerows(rows)

def load_frozen(path):
    p=Path(path);raw=p.read_bytes()
    if sha(raw)!=FROZEN_SHA:fail('FROZEN_BASELINE_SHA_DRIFT')
    rows=list(csv.DictReader(io.StringIO(raw.decode('utf-8-sig'))))
    if len(rows)!=587:fail('FROZEN_BASELINE_SIZE_DRIFT')
    if sum(r['group']=='CANDIDATE_RECLAIM' for r in rows)!=251:fail('FROZEN_CANDIDATE_COUNT_DRIFT')
    if sum(r['group']=='CONTROL_NO_RECLAIM' for r in rows)!=336:fail('FROZEN_CONTROL_COUNT_DRIFT')
    if any(not (r['event_date'].startswith('2025-') or r['event_date'].startswith('2026-')) for r in rows):fail('BASELINE_DATE_DRIFT')
    if len({(r['code'],r['event_date']) for r in rows})!=len(rows):fail('FROZEN_DUPLICATE_EVENTS')
    return rows

def read_reference(p):
    rows=list(csv.DictReader(Path(p).open(encoding='utf-8-sig',newline='')))
    out={}
    for r in rows:
        d=str(r.get('date','')).strip();n=int(r['expected_all_market_rows'])
        if d in out or not d or n<500:fail('INVALID_KRX_DAY_REFERENCE')
        out[d]=n
    return out

def validate_stored_csv(raw,date,expected,min_rows=500):
    try:df=pd.read_csv(io.BytesIO(gzip.decompress(raw)),dtype={'code':str,'date':str},low_memory=False)
    except Exception as e:fail('BROKEN_KRX_RAW:'+date+':'+type(e).__name__)
    if list(df.columns)!=FIELDS or len(df)!=expected or len(df)<min_rows:fail('KRX_BAR_COUNT_SCHEMA:'+date)
    if df.code.isna().any() or df.code.duplicated().any():fail('KRX_CODE_DUPLICATE:'+date)
    if not df.code.str.fullmatch(r'[0-9][0-9A-Z]{5}').all():fail('KRX_CODE_INVALID:'+date)
    if set(df.date)!={date}:fail('KRX_DATE_MISMATCH:'+date)
    for c in FIELDS[2:]:
        df[c]=pd.to_numeric(df[c],errors='coerce')
        if not df[c].map(math.isfinite).all() or df[c].lt(0).any():fail('KRX_BAR_INVALID:'+date+':'+c)
    if df.high.lt(df.low).any():fail('KRX_HIGH_LOW_INVALID:'+date)
    traded=df.open.gt(0)&df.high.gt(0)&df.low.gt(0)&df.close.gt(0)&df.volume.gt(0)&df.amount.gt(0)
    if (traded & (df.high.lt(df[['open','close']].max(axis=1))|df.low.gt(df[['open','close']].min(axis=1)))).any():
        fail('KRX_TRADED_OHLC_INCONSISTENT:'+date)
    return df

def verify_and_load(root,reference,start,end,wanted_codes=None,expected_shards=8,min_rows=500):
    root=Path(root);ref=read_reference(reference) if not isinstance(reference,dict) else reference
    all_days=sorted(d for d in ref if start<=d<=end)
    if not all_days:fail('NO_OFFICIAL_DAYS_IN_RANGE')
    manifests=[]
    for i in range(expected_shards):
        p=root/f'shard_{i:02d}'/f'manifest_shard_{i:02d}.json'
        if not p.is_file():fail('MISSING_SHARD_MANIFEST:'+str(i))
        m=json.loads(p.read_text(encoding='utf-8'))
        if m.get('status')!='COMPLETE_PRICE_COLLECTION_ONLY' or m.get('format')!=FORMAT or m.get('shard')!=i or m.get('shards')!=expected_shards:
            fail('SHARD_NOT_COMPLETE_OR_WRONG_FORMAT:'+str(i))
        if m.get('start')!=start or m.get('end')!=end or m.get('universe_filter')!='NONE':
            fail('WRONG_OR_FILTERED_SOURCE_SCOPE:'+str(i))
        if m.get('official_days')!=len(all_days) or m.get('calendar_sha256')!=sha('\n'.join(all_days).encode()):
            fail('OFFICIAL_DAY_CALENDAR_SHA_DRIFT')
        manifests.append(m)
    if len({m['index_sha256'] for m in manifests})!=1:fail('OFFICIAL_INDEX_SHA_CONFLICT')
    index_sha=manifests[0]['index_sha256']
    day_records={};by_code=defaultdict(list);seen_dates=set();coverage=[]
    for i,m in enumerate(manifests):
        folder=root/f'shard_{i:02d}'
        ix=folder/'official_indices.csv'
        if not ix.is_file() or sha(ix.read_bytes())!=index_sha:fail('OFFICIAL_INDEX_BYTES_SHA_MISMATCH')
        entries=m.get('files',[])
        if sorted(x['date'] for x in entries)!=all_days[i::expected_shards]:fail('INCOMPLETE_OR_EXTRA_DAY_SHARD:'+str(i))
        for e in entries:
            day=e['date'];p=folder/e['file']
            if day in seen_dates:fail('DUPLICATE_KRX_SESSION:'+day)
            if not p.is_file() or sha(p.read_bytes())!=e['sha256']:fail('RAW_SHA_MISSING_OR_MISMATCH:'+day)
            frame=validate_stored_csv(p.read_bytes(),day,ref[day],min_rows=min_rows)
            if len(frame)!=e['source_rows'] or len(frame)!=e['stored_rows']:fail('HISTORICAL_ALL_MARKET_ROW_DRIFT:'+day)
            seen_dates.add(day)
            day_records[day]={'sha256':e['sha256'],'rows':len(frame)}
            coverage.append({'date':day,'all_market_rows':len(frame),'zero_volume_rows':int(frame.volume.eq(0).sum()),
                             'zero_amount_rows':int(frame.amount.eq(0).sum()),'source_sha256':e['sha256']})
            if wanted_codes is not None:frame=frame[frame.code.isin(wanted_codes)]
            for tup in frame[FIELDS].itertuples(index=False,name=None):
                by_code[tup[0]].append(dict(zip(FIELDS,tup)))
    if seen_dates!=set(all_days):fail('MISSING_KRX_SESSION')
    for k in by_code:by_code[k].sort(key=lambda x:x['date'])
    return all_days,by_code,coverage,day_records

def traded(r):
    return r is not None and all(float(r[k])>0 for k in ('open','high','low','close','volume','amount'))

def detect_discontinuity(rows):
    jumps=set();last=None
    for r in rows:
        close=float(r['close'])
        if last and close>0 and last>0 and abs(close/last-1)>0.36:
            jumps.add(r['date'])
        if close>0:last=close
    return jumps

def sample_outcome(code,day,by_code,all_days,h=15):
    positions={d:i for i,d in enumerate(all_days)}
    if day not in positions:return {'status':'SESSION_NOT_IN_REFERENCE'}
    i=positions[day]
    if i+h>=len(all_days):return {'status':'NOT_MATURED_MARKET_SESSIONS'}
    lookup={r['date']:r for r in by_code.get(code,[])}
    anchor=lookup.get(day);nxt=lookup.get(all_days[i+1]);
    if not traded(anchor):return {'status':'SIGNAL_SESSION_NONTRADEABLE_OR_MISSING'}
    if not traded(nxt):return {'status':'NEXT_OPEN_NOT_TRADEABLE_OR_MISSING'}
    valid_jumps=detect_discontinuity(by_code.get(code,[]))
    days=all_days[i+1:i+h+1]
    if any(not traded(lookup.get(d)) for d in days):return {'status':'HOLD_PATH_UNKNOWN_OR_SUSPENDED'}
    if any(d in valid_jumps for d in days):return {'status':'CORPORATE_ACTION_OR_SOURCE_JUMP_REVIEW'}
    entry=float(nxt['open'])
    if entry<=0:return {'status':'NEXT_OPEN_INVALID'}
    ret={}
    for horizon in HORIZONS:
        if horizon>h:continue
        path=days[:horizon]
        ret[f'd{horizon}_close_pct']=100*(float(lookup[path[-1]]['close'])/entry-1)
        ret[f'd{horizon}_mfe_pct']=100*(max(float(lookup[d]['high']) for d in path)/entry-1)
        ret[f'd{horizon}_mae_pct']=100*(min(float(lookup[d]['low']) for d in path)/entry-1)
        ret[f'd{horizon}_net20bp_pct']=ret[f'd{horizon}_close_pct']-COST_ROUND_TRIP_PCT
    return {'status':'COMPLETE_RAW_KRX_PRICE_ONLY_NOT_EXECUTABLE', 'entry_next_open':entry,
            'signal_close':float(anchor['close']),**ret}

def summarize(rows):
    out=[]
    for group in ('CANDIDATE_RECLAIM','CONTROL_NO_RECLAIM'):
        subset=[x for x in rows if x['legacy_group']==group]
        for h in HORIZONS:
            key=f'd{h}_net20bp_pct';vals=[float(x[key]) for x in subset if x.get('outcome_status')=='COMPLETE_RAW_KRX_PRICE_ONLY_NOT_EXECUTABLE' and x.get(key) not in ('',None)]
            s=pd.Series(vals,dtype=float)
            out.append({'group':group,'horizon':h,'frozen_events':len(subset),'full_krx_price_paths':len(s),
                        'missing_or_blocked_paths':len(subset)-len(s),
                        'median_net20bp_pct':float(s.median()) if len(s) else '',
                        'mean_net20bp_pct':float(s.mean()) if len(s) else '',
                        'positive_net20bp_pct':float(100*(s>0).mean()) if len(s) else '',
                        'interpretation':'HISTORICAL_LEGACY_COHORT_REPRICING_NOT_OOS_NOT_SIGNAL_RESCREEN'})
    return out



class RawKRXDayUniverse:
    """Research-only same-session traded-bar proxy, NOT a PIT security master."""
    def __init__(self, by_code):
        self.available={code:{r['date'] for r in arr if traded(r)} for code,arr in by_code.items()}
    def lookup(self, day, code):
        d=pd.Timestamp(day).strftime('%Y-%m-%d')
        yes=d in self.available.get(code,set())
        return yes,d,0,'KRX_OBSERVED_TRADED_BAR_PROXY_NOT_OFFICIAL_MASTER' if yes else 'NO_SAME_DAY_TRADEABLE_BAR'


def krx_frozen_detector_replay(by_code,all_days,frozen):
    """Exactly reuse LOW224 frozen structure detector on KRX-only, unadjusted data.

    IMPORTANT: Re-run only frozen 499 code cohort for source parity, not
    contemporaneous ALL market screening. Never interpret as OOS evidence.
    """
    from low224_accum_wave1_pb_research import detect_code,CONFIG
    universe=RawKRXDayUniverse(by_code)
    start=pd.Timestamp(min(r['event_date'] for r in frozen))
    end=pd.Timestamp(max(r['event_date'] for r in frozen))
    stages=[];gates=defaultdict(int)
    for c,arr in sorted(by_code.items()):
        raw={r['date']:r for r in arr}
        dates=[d for d in all_days if d<=end.strftime('%Y-%m-%d')]
        # Missing/halted daily OHLC becomes NaN. Never compress gaps away.
        data=[]
        for day in dates:
            r=raw.get(day)
            good=r if traded(r) else None
            data.append({'date':pd.Timestamp(day),'open':float(good['open']) if good else float('nan'),
                         'high':float(good['high']) if good else float('nan'),
                         'low':float(good['low']) if good else float('nan'),
                         'close':float(good['close']) if good else float('nan'),
                         'volume':float(good['volume']) if good else float('nan'),
                         'amount':float(good['amount']) if good else float('nan')})
        frame=pd.DataFrame(data)
        if len(frame)<CONFIG.ma_long:continue
        part,gg=detect_code(c,frame,'KRX_NATIVE_UNADJUSTED_ACTUAL_AMOUNT',universe,start,end,CONFIG)
        stages.extend(part)
        for k,n in gg.items():gates[k]+=int(n)
    replay={}
    for s in stages:
        if s.get('stage')!='REACCELERATION':continue
        key=(str(s['code']),str(s['event_date']))
        val=int(s.get('reaccel_reclaims_prior_wave_high',0))
        if key in replay:fail('KRX_DETECTOR_DUPLICATED_REACCEL_EVENT')
        replay[key]='CANDIDATE_RECLAIM' if val==1 else 'CONTROL_NO_RECLAIM'
    parity=[]
    for e in frozen:
        key=(e['code'],e['event_date'])
        group=replay.get(key)
        parity.append({'code':key[0],'event_date':key[1],
            'legacy_group':e['group'],'krx_frozen_detector_group':group or '',
            'parity': 'SAME_DATE_SAME_GROUP' if group==e['group'] else
                      ('SAME_DATE_GROUP_CHANGED' if group else 'LEGACY_EVENT_NOT_REPRODUCED')})
    return parity,stages,dict(gates)


def legacy_cache_compare(root,amount_root,asof_root,by_code,frozen):
    """Optional read-only comparison against existing historical price cache;
    never network-fetch, never claim cached frame's provider was KRX or FDR.
    """
    from triangle1pb_research import _load_any,normalize_price_frame,AmountAuthority
    root=Path(root)
    if not root.exists():return [],{'status':'LEGACY_CACHE_ABSENT','source_provider':'UNKNOWN'}
    target={(r['code'],r['event_date']) for r in frozen}
    wanted={r['code'] for r in frozen}
    auth=AmountAuthority(Path(amount_root),Path(asof_root))
    legacy={};read_errors=0;read_files=0;conflicts=set()
    for p in sorted(root.rglob('*')):
        if not p.is_file():continue
        read_files+=1
        try:
            val=normalize_price_frame(_load_any(p),p,auth)
            if not val:continue
            code,frame,src,datesrc=val
            if code not in wanted:continue
            for t in frame.itertuples(index=False):
                d=pd.Timestamp(t.date).strftime('%Y-%m-%d');key=(code,d)
                if key not in target:continue
                now={'close':float(t.close),'volume':float(t.volume),
                     'open':float(t.open),'high':float(t.high),'low':float(t.low),
                     'amount':float(t.amount) if pd.notna(t.amount) else None,
                     'legacy_amount_adapter':src}
                if key in legacy:
                    if any(legacy[key][f]!=now[f] for f in ('open','high','low','close','volume')):conflicts.add(key)
                else:legacy[key]=now
        except Exception:read_errors+=1
    lookups={c:{r['date']:r for r in arr} for c,arr in by_code.items()}
    out=[]
    for c,d in sorted(target):
        v=legacy.get((c,d));k=lookups.get(c,{}).get(d)
        if (c,d) in conflicts:status='LEGACY_DUPLICATE_CONFLICT'
        elif v is None:status='LEGACY_PRICE_NOT_OBSERVED'
        elif k is None:status='KRX_SIGNAL_DAY_NOT_OBSERVED'
        elif not traded(k):status='KRX_SIGNAL_DAY_NONTRADEABLE'
        else:
            match=all(math.isfinite(v[f]) and math.isclose(v[f],float(k[f]),rel_tol=0.00001,abs_tol=0.00001)
                      for f in ('open','high','low','close','volume'))
            status='EXACT_OHLCV_MATCH' if match else 'SOURCE_OHLCV_MISMATCH'
        row={'code':c,'event_date':d,'parity_status':status,
             'legacy_amount_adapter':v.get('legacy_amount_adapter','') if v else ''}
        for f in ('open','high','low','close','volume','amount'):
            row['legacy_'+f]=v.get(f,'') if v else ''
            row['krx_'+f]=float(k[f]) if k else ''
        out.append(row)
    summary={'status':'LEGACY_CACHE_COMPARED_UNVERIFIED_PROVIDER','cache_files_seen':read_files,
             'cache_files_failed_to_parse':read_errors,
             'date_code_rows_compared':len(out),
             'legacy_price_present':len(legacy),'duplicate_cache_conflict_rows':len(conflicts),
             'exact_ohlcv_matches':sum(r['parity_status']=='EXACT_OHLCV_MATCH' for r in out),
             'ohlcv_mismatch':sum(r['parity_status']=='SOURCE_OHLCV_MISMATCH' for r in out),
             'price_cache_provider_certified':False}
    return out,summary

def main(argv=None):
    p=argparse.ArgumentParser()
    p.add_argument('--mode',required=True,choices=['smoke','full']);p.add_argument('--root',required=True)
    p.add_argument('--reference',required=True);p.add_argument('--frozen',required=True)
    p.add_argument('--start',required=True);p.add_argument('--end',required=True)
    p.add_argument('--shards',type=int,default=8);p.add_argument('--out',required=True)
    p.add_argument('--legacy-price-cache',default='');p.add_argument('--legacy-amount-cache',default='');p.add_argument('--legacy-asof-cache',default='')
    a=p.parse_args(argv);out=Path(a.out);out.mkdir(parents=True,exist_ok=True)
    frozen=load_frozen(a.frozen)
    all_days,by_code,coverage,day_records=verify_and_load(a.root,a.reference,a.start,a.end,
         wanted_codes={r['code'] for r in frozen} if a.mode=='full' else set(), expected_shards=a.shards)
    write_csv(coverage,out/'krx_sessions_coverage.csv')
    report={'schema':REPORT_SCHEMA,'mode':a.mode,'krx_collection_source':'pykrx.get_market_ohlcv_by_ticker(market=ALL,alternative=False)',
            'strict_krx_only_raw_prices':True,'no_fdr_or_naver_provider_calls':True,
            'start':a.start,'end':a.end,'official_sessions_verified':len(all_days),
            'all_market_rows':sum(x['all_market_rows'] for x in coverage),
            'session_manifest_sha256':sha(json.dumps(day_records,sort_keys=True).encode()),
            'legacy_frozen_587_used_only_as_drift_cohort':True,
            'official_daily_tradeability_unknown':True,'historical_delisted_and_security_type_unverified':True,
            'corporate_action_adjustment_verified':False,'15_30_only_price_certified':False,
            'signal_rescreened_on_krx':False,'oos_profitability_proven':False,
            'production_signal_changes':False,'production_rank_order_changes':False,
            'strategy_ready':False,'status':'KRX_SOURCE_SMOKE_PASSED_NO_PROFIT_INFERENCE' if a.mode=='smoke' else 'KRX_FROZEN_LEGACY_COHORT_REPRICED_NOT_OOS'}
    if a.mode=='full':
        by_date={c:{r['date']:r for r in rr} for c,rr in by_code.items()}
        rows=[]
        for r in frozen:
            code,date=r['code'],r['event_date'];anchor=by_date.get(code,{}).get(date)
            res=sample_outcome(code,date,by_code,all_days)
            rec={'code':code,'event_date':date,'legacy_group':r['group'],
                 'krx_signal_bar_present':int(anchor is not None),'krx_signal_traded_proxy':int(traded(anchor)),
                 'outcome_status':res.pop('status'),**res}
            rows.append(rec)
        col=['code','event_date','legacy_group','krx_signal_bar_present','krx_signal_traded_proxy','outcome_status',
             'entry_next_open','signal_close']+[f'd{h}_{z}' for h in HORIZONS for z in ('close_pct','mfe_pct','mae_pct','net20bp_pct')]
        write_csv(rows,out/'krx_frozen_587_reprice_diagnostic.csv',col)
        write_csv(summarize(rows),out/'krx_frozen_cohort_descriptive_summary.csv')
        # Replay the already-frozen 224MA/Accum/Wave/PB/Reaccel definition,
        # over the frozen 499-code diagnostic cohort only (never an OOS edge).
        parity,stages,gates=krx_frozen_detector_replay(by_code,all_days,frozen)
        write_csv(parity,out/'krx_frozen_detector_parity.csv')
        write_csv(stages,out/'krx_frozen_499code_detector_stages.csv')
        (out/'krx_detector_funnel.json').write_text(json.dumps(gates,indent=2),encoding='utf-8')
        report.update({'krx_replayed_detector_reaccel_events':sum(s.get('stage')=='REACCELERATION' for s in stages),
                       'legacy_events_reproduced_same_group':sum(x['parity']=='SAME_DATE_SAME_GROUP' for x in parity),
                       'legacy_events_missing_or_group_changed':sum(x['parity']!='SAME_DATE_SAME_GROUP' for x in parity),
                       'frozen_detector_sha_must_be_workflow_pinned':True,
                       'only_frozen_499_code_cohort_screened':True,
                       'signal_rescreened_on_krx':True})
        if a.legacy_price_cache:
            diag,diag_summary=legacy_cache_compare(a.legacy_price_cache,a.legacy_amount_cache,a.legacy_asof_cache,by_code,frozen)
            write_csv(diag,out/'krx_vs_legacy_cached_price_parity.csv')
            (out/'krx_vs_legacy_cached_price_summary.json').write_text(json.dumps(diag_summary,ensure_ascii=False,indent=2),encoding='utf-8')
            report['legacy_cache_parity_summary']=diag_summary
        report.update({'legacy_events':len(rows),'same_date_krx_bars_present':sum(x['krx_signal_bar_present'] for x in rows),
                       'next_open_complete_d15_raw_paths':sum(x['outcome_status']=='COMPLETE_RAW_KRX_PRICE_ONLY_NOT_EXECUTABLE' for x in rows),
                       'blocked_or_unknown_paths':sum(x['outcome_status']!='COMPLETE_RAW_KRX_PRICE_ONLY_NOT_EXECUTABLE' for x in rows),
                       'missing_or_halted_not_deleted':True,'signal_rescreening_required_before_alpha_claim':True})
    (out/'krx_truth_gate.json').write_text(json.dumps(report,ensure_ascii=False,indent=2,sort_keys=True),encoding='utf-8')
    print('CORE224_KRX_TRUTH_REPORT',json.dumps(report,ensure_ascii=False),flush=True)

if __name__=='__main__':main()
