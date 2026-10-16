"""R1.15 - research only: pinned R1.14 evidence, code source coverage, time-bucket data feasibility.

No fetching market data, no orders, no synthetic minute bars, no trade rules.
Optional supplied intraday CSV/ZIP must contain verifiable timestamp and provenance fields;
quality gates ensure provider-claimed data cannot be mislabeled as independently exchange certified.
"""
from __future__ import annotations
import argparse
import csv
import hashlib
import io
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from zipfile import ZipFile

R114_MANIFEST_SHA = 'f58876ecc18dbf8894bc3df8718d41b8b2ec2374b91279228d5c67123c6c984a'
R114_STATUS = 'PASS_DESCRIPTIVE_AND_BLIND_GALLERY_ONLY_NOT_TRADE_APPROVED'
RUNS = {'r114':38831145566,'r113':38826734127,'r112':38823870672,'r171':38664095610}
HEADER = ['trade_date','code','bar_start_kst','bar_end_kst','volume_delta','turnover_krw',
          'venue','session','record_kind','provider','provider_evidence_id','quality_flag']
WINDOWS = [ ('OPEN_0900_1000','09:00','10:00'), ('DAY_1000_1430','10:00','14:30'),
            ('LATE_1430_1500','14:30','15:00'),('PRE_1500_1503','15:00','15:03'),
            ('PRE_1503_1520','15:03','15:20'),
            ('AUCTION_1530','15:20','15:30'),
            ('KRX_AFTER_1540_1600','15:40','16:00'),
            ('KRX_AFTER_1600_2000','16:00','20:00'),
            ('NXT_AUCTION_1540','15:30','15:40'),('NXT_AFTER_1540_2000','15:40','20:00')]
FEATURE_FIELDS = ['trade_date','code','window','venue','intervals','minutes_observed','minutes_expected',
                  'coverage_pct','volume_delta','turnover_krw','quality','eligible_for_price_volume_research',
                  'available_after_kst','eligible_at_1503','prior20_fully_covered_days','same_window_volume_ratio_20d']
ALLOWED_VENUES = {'KRX','NXT'}
ALLOWED_QUALITY = {'BROKER_REPORTED','EXCHANGE_REPORTED','RESEARCH_UNVERIFIED'}

def sha(b:bytes)->str: return hashlib.sha256(b).hexdigest()
def need(cond:bool,reason:str):
    if not cond: raise ValueError(reason)
def csv_bytes(rows,fields):
    f=io.StringIO(); w=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore',lineterminator='\n');w.writeheader();w.writerows(rows)
    return f.getvalue().encode('utf-8-sig')
def write_csv(path,rows,fields):path.write_bytes(csv_bytes(rows,fields))
def load_csv(path):
    with Path(path).open(encoding='utf-8-sig',newline='') as f:return list(csv.DictReader(f))

def verify_r114(folder:Path):
    mpath=folder/'r114_integrity_manifest.json'
    need(mpath.is_file() and sha(mpath.read_bytes())==R114_MANIFEST_SHA,'R114_MANIFEST_PIN_MISMATCH')
    m=json.loads(mpath.read_text(encoding='utf8'))
    need(m['status']==R114_STATUS and m['no_wave_proxy']==659 and m['total_unique_signals']==1059,'R114_EVIDENCE_CONTRACT_MISMATCH')
    need(m['charts_generated']==40 and m['krx_dated_sha_verified']==1907 and m['native_full_ohlcv_bars_verified']==1059,'R114_COUNTER_MISMATCH')
    for filename,digest in m['output_sha256'].items():
        p=folder/filename
        need(p.is_file() and sha(p.read_bytes())==digest,'R114_FILE_MISMATCH:'+filename)
    cross=load_csv(folder/'r114_blind_gallery_crosswalk.csv')
    labeled=load_csv(folder/'r114_manual_label_blank_template.csv')
    metrics=load_csv(folder/'r114_1059_exante_descriptive_metrics.csv')
    need(len(cross)==40 and len(labeled)==40 and len(metrics)==1059,'R114_ROWS_MISMATCH')
    need(len({(x['signal_date'],x['code']) for x in metrics})==1059,'R114_SIGNAL_DUPLICATES')
    need(len({x['blind_case_id'] for x in cross})==40,'R114_CASE_DUPLICATES')
    need({x['blind_case_id'] for x in cross}=={x['blind_case_id'] for x in labeled},'R114_CASE_IDENTITY_BAD')
    need(all(not x.get('reviewer_asof_wave_label','').strip() for x in labeled),'R114_ALREADY_LABELED_UNEXPECTED')
    need(len(list((folder/'charts').glob('R113-*.png')))==40,'R114_BLIND_CHART_MISSING')
    return m,cross,metrics

def audit_code(repo:Path):
    cases=[
      ('ham_restart_research.py','HAM_TIME_TRUNCATION','time',r'\[\s*\(x\["time"\]\s*>=\s*"09:00"\)\s*&\s*\(x\["time"\]\s*<=\s*"15:03"\)\s*\]'),
      ('ham_restart_research.py','HAM_PROXY_CLOSE_X_VOLUME','value',r'PROXY_CLOSE_X_VOLUME'),
      ('ham_restart_research.py','HAM_MINUTE_PERSISTENCE','cache',r'ham_minute_history'),
      ('presurge_scanner.py','PRESURGE_CUMULATIVE_DIV_PREVIOUS_DAY','volume',r'vol_ratio\s*=\s*volume\s*/\s*prev_vol'),
      ('presurge_scanner.py','PRESURGE_1520_1530_NAME_ONLY','comment',r'15:20\s*[~～\-]\s*15:30'),
      ('quick_open_checker.py','NAVER_UNOFFICIAL_MINUTE_PROVIDER','provider',r'api\.finance\.naver\.com/siseJson\.naver'),
      ('odoli_after_session_context_r1.py','NXT_SESSION_ONLY_SNAPSHOT','after',r'NXT_OVERMARKET'),
    ]
    rows=[]
    for filename,rule,category,regex in cases:
        path=repo/filename
        need(path.is_file(),'SOURCE_FILE_MISSING:'+filename)
        raw=path.read_bytes();s=raw.decode('utf-8-sig')
        found=re.search(regex,s,flags=re.I|re.S) is not None
        rows.append(dict(file=filename,file_sha256=sha(raw),rule=rule,category=category,found=str(found),
            evidence_type='CODE_STATIC_NOT_MARKET_DATA',comment={
                'HAM_TIME_TRUNCATION':'HAM normalize function excludes post-15:03 bars: not adequate for closing auction.',
                'HAM_PROXY_CLOSE_X_VOLUME':'Estimated turnover close*volume is proxy, not venue-reported turnover.',
                'HAM_MINUTE_PERSISTENCE':'A persistent minute history path is coded; historical cache existence unverified.',
                'PRESURGE_CUMULATIVE_DIV_PREVIOUS_DAY':'Cumulative today / previous entire day is not incremental closing-minute volume.',
                'PRESURGE_1520_1530_NAME_ONLY':'Claim is a time-window description; actual 15:30 auction evidence not established.',
                'NAVER_UNOFFICIAL_MINUTE_PROVIDER':'Third-party endpoint is code-declared; availability/retention not tested.',
                'NXT_SESSION_ONLY_SNAPSHOT':'NXT post-session payload cannot be assumed equal to KRX closing auction.'}[rule]))
    return rows

def _dt(s):
    need(bool(re.fullmatch(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}(?::\d{2})?',s)),'BAD_TIMESTAMP_FORMAT:'+str(s))
    return datetime.strptime(s,'%Y-%m-%d %H:%M:%S' if len(s)==19 else '%Y-%m-%d %H:%M')
def _float(s,which):
    try:v=float(s)
    except Exception:raise ValueError('BAD_NUMERIC:'+which)
    need(v>=0 and v<float('inf'),'BAD_VOLUME_OR_AMOUNT:'+which); return v

def _read_minute_files(source:Path):
    if not source or not source.exists(): return []
    if source.is_dir():
        files=sorted(x for x in source.rglob('*.csv') if x.is_file())
        need(len(files)<=50,'EXCESSIVE_MINUTE_FILES')
        return [(str(x),x.read_bytes()) for x in files]
    if source.suffix.lower()=='.zip':
        with ZipFile(source) as z:
            members=[x for x in z.infolist() if x.filename.lower().endswith('.csv') and not x.is_dir()]
            need(len(members)<=50 and all(x.file_size<=200_000_000 for x in members),'BAD_SOURCE_ZIP_CAP')
            return [(x.filename,z.read(x)) for x in members]
    need(source.suffix.lower()=='.csv','UNSUPPORTED_MINUTE_SOURCE')
    return [(source.name,source.read_bytes())]

def ingest_minutes(source:Path):
    files=_read_minute_files(source)
    res=[];audit=[];ident=set();groups=defaultdict(list)
    for filename,blob in files:
        with io.StringIO(blob.decode('utf-8-sig'),newline='') as fp:
            d=csv.DictReader(fp)
            need(d.fieldnames is not None and set(HEADER).issubset(set(d.fieldnames)),'MINUTE_SCHEMA_INVALID:'+filename)
            rows=list(d)
        need(len(rows)<=500_000,'MINUTE_TOO_MANY_ROWS:'+filename)
        audit.append(dict(source_file=filename,source_sha256=sha(blob),rows=len(rows),format='STRICT_DELTA_WITH_EXPLICIT_SESSION_AND_PROVENANCE'))
        for k,r in enumerate(rows,2):
            date=r['trade_date'];code=r['code']
            need(bool(re.fullmatch(r'\d{4}-\d{2}-\d{2}',date)),'BAD_DATE')
            need(bool(re.fullmatch(r'[0-9A-Za-z]{6}',code)),'BAD_TICKER')
            start,end=_dt(r['bar_start_kst']),_dt(r['bar_end_kst'])
            need(start.date().isoformat()==date and end.date().isoformat()==date,'CROSS_DATE')
            need(timedelta(seconds=0)<end-start<=timedelta(minutes=30),'INVALID_INTERVAL')
            v=_float(r['volume_delta'],'volume_delta')
            amount=_float(r['turnover_krw'],'turnover_krw') if r['turnover_krw'].strip() else None
            venue=r['venue'].strip().upper();session=r['session'].strip().upper();kind=r['record_kind'].strip().upper()
            need(venue in ALLOWED_VENUES,'VENUE_UNVERIFIED')
            need(r['quality_flag'] in ALLOWED_QUALITY,'UNKNOWN_QUALITY_FLAG')
            need(bool(r['provider'].strip()) and bool(r['provider_evidence_id'].strip()),'PROVENANCE_REQUIRED')
            if kind=='REGULAR_INTERVAL':
                need(venue=='KRX' and session=='KRX_REGULAR' and start.time()>=_dt(date+' 09:00').time() and end.time()<=_dt(date+' 15:20').time(),'REGULAR_SESSION_MISMATCH')
            elif kind=='AUCTION_ACTUAL_1530':
                need(venue=='KRX' and session=='KRX_CLOSING_AUCTION' and start.strftime('%H:%M')=='15:20' and end.strftime('%H:%M')=='15:30','AUCTION_TIME_INVALID')
            elif kind=='NXT_AUCTION_ACTUAL_1540':
                need(venue=='NXT' and session=='NXT_CLOSING_AUCTION' and start.strftime('%H:%M')=='15:30' and end.strftime('%H:%M')=='15:40','NXT_AUCTION_TIME_INVALID')
            elif kind=='AFTER_SESSION_INTERVAL':
                need(session in {'KRX_AFTER','NXT_AFTER'} and start.time()>=_dt(date+' 15:40').time(),'AFTER_SESSION_MISMATCH')
                need((venue=='KRX')==(session=='KRX_AFTER'),'AFTER_VENUE_MISMATCH')
                if venue=='KRX' and start.time()>=_dt(date+' 16:00').time():
                    need(date>='2026-09-14','KRX_AFTER_1600_PRELAUNCH_INVALID')
            else:raise ValueError('UNKNOWN_RECORD_KIND:'+kind)
            key=(date,code,venue,kind,start,end)
            need(key not in ident,'DUPLICATE_MINUTE_INTERVAL')
            ident.add(key)
            gkey=(date,code,venue,kind)
            for old_start,old_end in groups[gkey]:
                need(not(start<old_end and end>old_start),'OVERLAPPING_MINUTE_INTERVAL')
            groups[gkey].append((start,end))
            res.append(dict(trade_date=date,code=code,start=start,end=end,volume=v,amount=amount,
                venue=venue,session=session,kind=kind,quality=r['quality_flag'],provider=r['provider'],evidence=r['provider_evidence_id']))
    return res,audit

def summarize_minutes(bars):
    by=defaultdict(list)
    for b in bars:by[(b['trade_date'],b['code'])].append(b)
    out=[]
    for (date,code),bs in sorted(by.items()):
        for name,a,z in WINDOWS:
            start=_dt(date+' '+a);end=_dt(date+' '+z)
            venue='NXT' if name.startswith('NXT_') else 'KRX'
            kind=('AUCTION_ACTUAL_1530' if name=='AUCTION_1530' else 'NXT_AUCTION_ACTUAL_1540' if name=='NXT_AUCTION_1540' else 'AFTER_SESSION_INTERVAL' if '_AFTER_' in name else 'REGULAR_INTERVAL')
            accepted=[b for b in bs if b['venue']==venue and b['kind']==kind and b['start']>=start and b['end']<=end]
            # Closing auction is ONE actual executed print/event, not 10 continuous minutes.
            n=len(accepted) if kind in {'AUCTION_ACTUAL_1530','NXT_AUCTION_ACTUAL_1540'} else sum(int((b['end']-b['start']).total_seconds()/60) for b in accepted)
            expected=1 if kind in {'AUCTION_ACTUAL_1530','NXT_AUCTION_ACTUAL_1540'} else int((end-start).total_seconds()/60)
            cover=round(100*n/expected,4)
            need(n<=expected,'INTERVAL_OVERCOUNT')
            amt_known=bool(accepted) and all(x['amount'] is not None for x in accepted)
            qu='NO_BARS' if not accepted else ('SOME_UNVERIFIED' if any(x['quality']=='RESEARCH_UNVERIFIED' for x in accepted) else 'REPORTED_BUT_NOT_INDEPENDENTLY_CERTIFIED')
            eligible=n==expected and amt_known and qu=='REPORTED_BUT_NOT_INDEPENDENTLY_CERTIFIED'
            out.append(dict(trade_date=date,code=code,window=name,venue=venue,intervals=len(accepted),minutes_observed=n,minutes_expected=expected,coverage_pct=cover,volume_delta=sum(x['volume'] for x in accepted) if accepted else '',turnover_krw=sum(x['amount'] for x in accepted) if amt_known else '',quality=qu,eligible_for_price_volume_research=str(eligible),available_after_kst=z,eligible_at_1503=str(eligible and z<='15:03'),prior20_fully_covered_days=0,same_window_volume_ratio_20d=''))
    # Ratios use only 20 *prior* full days of same stock/window; no futures and no overlapping same-day leakage.
    history=defaultdict(list)
    for r in out:
        k=(r['code'],r['window'])
        past=history[k]
        if r['eligible_for_price_volume_research']=='True':
            eligible_past=[p for p in past if p['eligible_for_price_volume_research']=='True']
            n=len(eligible_past);r['prior20_fully_covered_days']=min(n,20)
            if n>=20:
                baseline=sum(float(p['volume_delta']) for p in eligible_past[-20:])/20
                if baseline>0:r['same_window_volume_ratio_20d']=round(float(r['volume_delta'])/baseline,5)
        past.append(r)
    return out

def run(args):
    root=Path(args.out);root.mkdir(parents=True,exist_ok=True)
    r114=Path(args.r114_dir); repo=Path(args.repo_root)
    manifest,cases,metrics=verify_r114(r114)
    code=audit_code(repo)
    bars,raw_audit=ingest_minutes(Path(args.minute_source)) if args.minute_source else ([],[])
    minute=summarize_minutes(bars)
    known={(r['signal_date'],r['code']) for r in metrics}
    signal_join=[r for r in minute if (r['trade_date'],r['code']) in known]
    # Explicitly do not attempt ungrounded link of minute dates not matching known signal dates.
    if args.require_intraday:need(bool(signal_join),'REQUIRED_INTRADAY_SIGNAL_JOIN_MISSING')
    statuses=Counter(r['quality'] for r in minute)
    write_csv(root/'r115_source_code_semantics_audit.csv',code,['file','file_sha256','rule','category','found','evidence_type','comment'])
    write_csv(root/'r115_minute_raw_source_inventory.csv',raw_audit,['source_file','source_sha256','rows','format'])
    write_csv(root/'r115_minute_KRX_timebucket_coverage.csv',minute,FEATURE_FIELDS)
    write_csv(root/'r115_signal_minute_join_coverage.csv',signal_join,FEATURE_FIELDS)
    blank=load_csv(r114/'r114_manual_label_blank_template.csv')
    write_csv(root/'r115_40_preD0_blind_review_to_complete.csv',blank,['blind_case_id','reviewer_asof_wave_label','reviewer_confidence','reviewer_notes_no_future','allowed_labels_hint'])
    chart=load_csv(r114/'r114_blind_gallery_crosswalk.csv')
    # A neutral triage table, not human judgement: reuse R113 cause labels without asserting actual wave truth.
    counts=Counter(x['primary_explanation'] for x in chart)
    write_csv(root/'r115_blind_case_sampling_inventory.csv',
        [dict(blind_case_id=x['blind_case_id'],r113_algorithmic_cause=x['primary_explanation'],mode=x['source_modes'],chart_file=x['image_file'],review_status='NOT_INDEPENDENTLY_LABELED') for x in chart],
        ['blind_case_id','r113_algorithmic_cause','mode','chart_file','review_status'])
    state=('NO_VERIFIED_HISTORICAL_INTRADAY_INPUT' if not bars else
           'PARTIAL_OR_UNVERIFIED_INTRADAY_EVIDENCE' if not any(x['eligible_for_price_volume_research']=='True' for x in signal_join) else
           'SOME_REPORTED_INTRADAY_SIGNAL_COVERAGE_UNDER_REVIEW')
    m=dict(status='PASS_READINESS_ONLY_NOT_A_MINUTE_BACKTEST',r114_pinned_manifest_sha256=R114_MANIFEST_SHA,
        upstream_run_ids=RUNS,signals=1059,algorithm_no_wave=659,blind_charts_pre_D0=40,
        independently_labeled_blind_charts=0,raw_minute_files=len(raw_audit),raw_minute_rows=len(bars),
        minute_bucket_rows=len(minute),signal_day_bucket_rows=len(signal_join),
        signal_full_reported_coverage_buckets=sum(x['eligible_for_price_volume_research']=='True' for x in signal_join),
        intraday_readiness=state,code_audit_found={x['rule']:x['found'] for x in code},
        limits={'source_code_evidence':'STATIC_CODE_ONLY','intraday_verification':'PROVIDER_CLAIMS_NOT_EXCHANGE_INDEPENDENT',
                'historical_cache_observed':'NO_UNLESS_EXTERNAL_DATA_SUPPLIED','trade_approval':False,
                'no_synthetic_minutes':True,'no_1530_feature_at_1503':True,'KRX_NXT_SEPARATION':True,
                'r114_waves_human_ground_truth':'NOT_LABELED','winrates_calculated':'NO'},
        output_sha256={})
    for path in sorted(root.glob('*.csv')):m['output_sha256'][path.name]=sha(path.read_bytes())
    (root/'r115_integrity_manifest.json').write_text(json.dumps(m,ensure_ascii=False,indent=2)+'\n',encoding='utf8')
    readme='''# R1.15: 분봉·종가 수급 연구 준비도 감사 (연구 전용)\n\n- GitHub R1.14 실제 아티팩트의 고정 SHA 및 1,059 신호/659 미검출/40 차트 입력 무결성 검사.\n- 저장소 소스코드에서 HAM 15:03 절단, 추정 거래대금, Presurge의 당일 누적/전일 전체 거래량 비교, NXT 애프터 세션 분리를 검사.\n- 기본 실행에는 검증된 과거 분봉/15:30 종가 실제 체결 자료가 **없음**. 일봉을 분봉으로 생성하지 않음.\n- 40개 차트는 블라인드 판정 기록지로 인계하되, 아직 독립 판정된 적이 없음을 명시.\n- 입력 데이터가 주어지는 경우 `trade_date,code,bar_start_kst,bar_end_kst,volume_delta,turnover_krw,venue,session,record_kind,provider,provider_evidence_id,quality_flag` 컬럼 필수. `volume_delta`는 **구간별 거래량**, 누적값 금지. timestamp는 KST.\n- `record_kind`는 `REGULAR_INTERVAL`(09:00~15:20), `AUCTION_ACTUAL_1530`(15:20~15:30 실제 체결만), `NXT_AUCTION_ACTUAL_1540`(NXT 15:40 실제 단일가), `AFTER_SESSION_INTERVAL`(해당 시장 15:40 이후)로 분리.\n- KRX/정규장과 NXT/애프터 데이터 혼합 금지. 아티팩트 출처가 있다는 것만으로 거래소 독립 검증을 의미하지 않음.\n- 동일 종목·시간 구간 중복/중첩 금지. 분봉 누락은 추정 0이 아닌 미확인, 정상 입력에도 세션별 100% 분 단위 관측만 완전 구간 판정.\n- 15:03 판단에는 15:03 이후 체결량과 종가 동시호가를 절대 사용하지 않음. 소급 분봉 재구성, 미관측 거래량 대입, 미래 결과에 맞춘 거래 규칙 선택 금지.\n- 역사적 분봉이 없으면 승률, 알파, 15:30 체결 집중 통계는 **계산 불가능**이며 패치가 데이터를 만들어 내지는 않음.\n- LIVE / 주문 / 검색식 / 알림 변경 0. 모든 파일 재계산 전까지 R1.14 완성 상태를 유지.\n'''
    (root/'README_R115.txt').write_text(readme,encoding='utf8')
    print('R115_READINESS_PASS '+json.dumps({k:m[k] for k in ['intraday_readiness','raw_minute_rows','signal_day_bucket_rows','signals','algorithm_no_wave','blind_charts_pre_D0']},ensure_ascii=False))
    return m

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--r114-dir',required=True);p.add_argument('--repo-root',required=True);p.add_argument('--minute-source',default='')
    p.add_argument('--out',required=True);p.add_argument('--require-intraday',action='store_true')
    run(p.parse_args())
if __name__=='__main__':main()
