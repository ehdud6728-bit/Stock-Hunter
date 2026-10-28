"""R1D27D full KRX market audit & historical ticker-disjoint flow cohort.

Isolation: selection ONLY reads price/ASOF features and 300 original outcome-blind
sample; the historical outcome data are read AFTER the freeze is committed.
Research-only. NO LIVE, no orders, no forward outcome dependent selection.
"""
from __future__ import annotations
import argparse, gzip, hashlib, io, json, pathlib, zipfile
from collections import defaultdict
import networkx as nx
import pandas as pd

VERSION='R1D27D_ALLMARKET_PRICE_133180_TICKER_DISJOINT_1500_V1'
A27_ZIP_SHA='0a756a97da9a5d2ef8b475a222a9ff27eed3091807632b19b171da0967ba6f79'
FROZEN300_SHA='813a7b887f9b5a4c86b44f5f88a2fc1289cf724c3c9f46136208376d59344d15'
ASOF_SHA='a2d9bf8105fec12227167a117a730d2da73b1d5f0fb173587465e2e55f7f1304'
OUTCOME_SHA='57774314a3be0cc74cee02d6814edeee8cf02b403290fb27f76d690621da7378'
A='/reports/r1d27a/result/census/R1D27A_ALLMARKET_BB203040_ASOF_ONLY.csv.gz'
F='/reports/r1d27a/result/census/R1D27A_FROZEN_FLOW_SAMPLE_OUTCOME_BLIND.csv'
O='/reports/r1d27a/result/evaluate/R1D27A_ALLMARKET_D5_10_15_20_30_OUTCOMES.csv.gz'
STRATA=['year','bb_period','event_type']
KEY=['date','code','bb_period','event_type']
MAX_EVENTS=1500
EACH_STRATUM=50
SEED='R1D27D_PREDECLARED_TICKER_DISJOINT_1500_20261028_V1'

def check(ok,code):
    if not ok:raise ValueError('R1D27D_'+code)
def sha(b):return hashlib.sha256(b).hexdigest()
def write_json(path,obj):path.write_text(json.dumps(obj,indent=2,ensure_ascii=False,sort_keys=True)+'\n',encoding='utf-8')
def load_csv_bytes(b,compressed=False):
    return pd.read_csv(io.BytesIO(gzip.decompress(b) if compressed else b),dtype={'date':str,'code':str,'event_type':str,'bb_period':str})

def source(z):
    bb=z.read(A.lstrip('/'));ff=z.read(F.lstrip('/'))
    check(sha(bb)==ASOF_SHA,'ASOF_SOURCE_DRIFT');check(sha(ff)==FROZEN300_SHA,'INITIAL_FROZEN_DRIFT')
    allmarket=load_csv_bytes(bb,True);sample=load_csv_bytes(ff)
    for d in [allmarket,sample]:
        d['code']=d['code'].str.zfill(6)
        d['bb_period']=d['bb_period'].astype(str)
    check(len(allmarket)==133180 and len(sample)==300,'SOURCE_COUNTS')
    check(allmarket[KEY].duplicated().sum()==0,'DUPLICATE_SIGNAL')
    check(sample.code.nunique()==300 and sample[KEY].duplicated().sum()==0,'FROZEN300_IDENTITY')
    return allmarket,sample

def days_from_shard(z):
    b=z.read('shard_00/official_indices.csv'); f=pd.read_csv(io.BytesIO(b),dtype={'date':str})
    f=f[f['index']=='KOSPI'].copy()
    dates=f.date.sort_values().drop_duplicates().to_list()
    check(len(dates)==1907 and dates[0]=='2019-01-02' and dates[-1]=='2026-10-12','KRX_DAYS_INTEGRITY')
    return dates

def freeze_sample(allmarket,old300,days):
    """Min-hash ASOF choice within each group×code, then unique-code max flow.

    Max flow removes greedy-first-strata exhaustion; all 30 strata get exactly 50
    unrelated tickers, and NONE overlap the original 300 ticker identifiers.
    """
    pool=allmarket.loc[~allmarket.code.isin(set(old300.code))].copy()
    pool['year']=pool.date.str[:4]
    pool['select_hash']=[sha(f'{SEED}|{x.date}|{x.code}|{x.bb_period}|{x.event_type}'.encode()) for x in pool.itertuples(index=False)]
    check(pool.code.nunique()==1925,'POOL_DISTINCT_TICKERS')
    pool=pool.sort_values(['select_hash','date','code'],kind='stable')
    groups=list(pool.groupby(STRATA,sort=True))
    check(len(groups)==30,'STRATA_COUNT')
    graph=nx.DiGraph();src='R1D27D_SOURCE';sink='R1D27D_TARGET';index={}
    for j,(group,frame) in enumerate(groups):
        group_name=f'G{j:02d}';graph.add_edge(src,group_name,capacity=EACH_STRATUM)
        options=frame.drop_duplicates('code',keep='first')
        for row in options.itertuples(index=False):
            graph.add_edge(group_name,'T'+row.code,capacity=1)
            index[(group_name,row.code)]=row
    for code in sorted(pool.code.unique()):graph.add_edge('T'+code,sink,capacity=1)
    maximum,alloc=nx.maximum_flow(graph,src,sink)
    check(maximum==MAX_EVENTS,'NO_FULL_1500_TICKER_DISJOINT_ALLOCATION')
    selected=[]
    for j,(group,_) in enumerate(groups):
        group_name=f'G{j:02d}';codes=[t[1:] for t,v in alloc[group_name].items() if v==1]
        check(len(codes)==50,'STRATUM_ALLOCATION_FAILED')
        selected.extend(index[(group_name,code)] for code in codes)
    records=[]
    for e in selected:
        ix=int(e.signal_ix)
        check(60<=ix<len(days) and days[ix]==e.date,'SIGNAL_IX_DATE_MISMATCH')
        records.append(dict(date=e.date,code=e.code,bb_period=e.bb_period,event_type=e.event_type,
            year=int(e.year),signal_ix=ix,pre60_from=days[ix-60],pre60_to=days[ix-1],
            select_hash=e.select_hash,source='R1D27A_ASOF_SHA_TICKER_DISJOINT_NO_OUTCOME'))
    df=pd.DataFrame(records).sort_values(STRATA+['select_hash'],kind='stable').reset_index(drop=True)
    check(len(df)==MAX_EVENTS and df.code.nunique()==MAX_EVENTS,'UNIQUE_CODE')
    check(not set(df.code).intersection(old300.code),'OLD300_TICKER_OVERLAP')
    check((df.groupby(STRATA).size()==50).all() and len(df.groupby(STRATA))==30,'BALANCED_STRATA')
    check(not df[KEY].duplicated().any(),'DUP_FROZEN_EVENT')
    return df

def save_freeze(a_zip,krx_zip,out):
    a_zip=pathlib.Path(a_zip);krx_zip=pathlib.Path(krx_zip);out=pathlib.Path(out);out.mkdir(parents=True,exist_ok=True)
    check(sha(a_zip.read_bytes())==A27_ZIP_SHA,'ARTIFACT_SHA_DRIFT')
    with zipfile.ZipFile(a_zip) as z,zipfile.ZipFile(krx_zip) as calz:
        allmarket,old300=source(z)
        dates=days_from_shard(calz)
        selection=freeze_sample(allmarket,old300,dates)
        f=out/'R1D27D_FROZEN_1500_TICKER_DISJOINT_OUTCOME_BLIND.csv'
        selection.to_csv(f,index=False,lineterminator='\n')
        meta=dict(version=VERSION,phase='freeze',research_only=True,prospective_oos=False,
            independent_temporal_holdout=False,independent_original300_tickers=True,
            point_in_time_certified=False,strategy_ready=False,orders_placed=0,
            selection_time_outcomes_opened=False,source_zip_sha256=A27_ZIP_SHA,source_asof_sha256=ASOF_SHA,
            original300_sha256=FROZEN300_SHA,source_allmarket_events=133180,source_unique_stock_dates=85576,
            source_unique_tickers=2225,excluded_original_tickers=300,available_other_tickers=1925,
            selection_seed=SEED,selected_events=len(selection),selected_tickers=selection.code.nunique(),
            selection_per_year_band_event=50,groups=30,krx_sessions=len(dates),
            signal_time_features_only=True,no_future_flow_or_price_for_selection=True,
            selected_rows_sha256=sha(f.read_bytes()),no_live_changes=True)
        write_json(out/'R1D27D_FROZEN_MANIFEST.json',meta)
        print('FROZEN_1500',meta['selected_rows_sha256'],flush=True)
        return meta

def price_allmarket(a_zip,out):
    """Price-only census and outcomes; NEVER reads newly selected holdout flows."""
    out=pathlib.Path(out);out.mkdir(parents=True,exist_ok=True)
    check(sha(pathlib.Path(a_zip).read_bytes())==A27_ZIP_SHA,'ARTIFACT_SHA_DRIFT')
    with zipfile.ZipFile(a_zip) as z:
        bb=z.read(O.lstrip('/'));check(sha(bb)==OUTCOME_SHA,'OUTCOME_SOURCE_SHA_DRIFT')
        df=load_csv_bytes(bb,True)
    check(len(df)==665900,'OUTCOME_COUNT')
    df['year']=df['date'].str[:4]
    df['complete']=df.status_out.eq('PRICE_PATH_COMPLETE_NOT_FILL_CERTIFIED')
    # Draws from a retrospective price universe; not an investor-flow test.
    rows=[]
    for (year,band,event,h),g in df.groupby(['year','bb_period','event_type','horizon']):
        valid=g.loc[g.complete,'net_20bps_pct'].dropna()
        rows.append(dict(year=year,bb_period=band,event_type=event,horizon=int(h),
            n_price_events=len(g),n_complete=len(valid),n_censored=len(g)-len(valid),
            mean_return_pct=float(valid.mean()) if len(valid) else None,
            median_return_pct=float(valid.median()) if len(valid) else None,
            win_rate=float((valid>0).mean()) if len(valid) else None,
            loss_below_10pct=float((valid<-10).mean()) if len(valid) else None))
    result=pd.DataFrame(rows)
    target=out/'R1D27D_FULLMARKET_PRICE_ONLY_133180_STRATA_HORIZON.csv';result.to_csv(target,index=False,lineterminator='\n')
    meta=dict(version=VERSION,phase='price_only',source_zip_sha256=A27_ZIP_SHA,
        event_count=133180,observations=len(df),completed_price_paths=int(df.complete.sum()),
        censored=int((~df.complete).sum()),price_only_not_investor_flow=True,
        duplicate_events_within_stock_date_possible=True,fill_certified=False,
        prospective_oos=False,research_only=True,no_live_changes=True,files_sha256={target.name:sha(target.read_bytes())})
    write_json(out/'R1D27D_PRICE_ONLY_MANIFEST.json',meta)
    return meta

def fullmarket_flow_request_queue(a_zip,krx_zip,out):
    """All 85,576 distinct stock-date source requests for 133,180 BB events.

    Source intent only. NO provider data are queried here; 60-session histories
    and eventual dedup/caching costs belong to a later verified collection lane.
    """
    out=pathlib.Path(out);out.mkdir(parents=True,exist_ok=True)
    check(sha(pathlib.Path(a_zip).read_bytes())==A27_ZIP_SHA,'FULL_QUEUE_ORIGIN')
    with zipfile.ZipFile(a_zip) as z,zipfile.ZipFile(krx_zip) as calz:
        allmarket,_=source(z);days=days_from_shard(calz)
    counts=allmarket.groupby(['date','code'],sort=True).agg(n_band_events=('event_type','size'),
                       signal_ix=('signal_ix','first')).reset_index()
    check(len(counts)==85576 and counts.n_band_events.sum()==133180,'FULL_REQUEST_COUNT')
    check(counts.n_band_events.between(1,6).all(),'FULL_REQUEST_EVENT_RANGE')
    ix=counts.signal_ix.to_numpy(dtype=int)
    check((ix>=60).all() and (ix<len(days)).all(),'FULL_REQUEST_DAY_RANGE')
    check((counts.date.to_numpy()==[days[q] for q in ix]).all(),'FULL_REQUEST_EVENT_TIME')
    counts['pre60_from']=[days[q-60] for q in ix]
    counts['pre60_to']=[days[q-1] for q in ix]
    counts['future_outcomes_used']=False
    out_csv=out/'R1D27D_ALLMARKET_85576_TICKER_DATES_SOURCE_QUEUE.csv.gz'
    counts.to_csv(out_csv,index=False,compression={'method':'gzip','compresslevel':6,'mtime':0})
    meta=dict(version=VERSION,phase='fullmarket_queued_not_fetched',
              n_source_requests=len(counts),n_bb_events=int(counts.n_band_events.sum()),
              unique_tickers=int(counts.code.nunique()),source_days=1907,
              data_retrieved=False,source_partial=False,source_request_dedup_by_ticker_date=True,
              future_price_or_flow_in_selection=False,not_pit_certified=True,
              no_live_changes=True,queued_sha256=sha(out_csv.read_bytes()))
    write_json(out/'R1D27D_FULLMARKET_SOURCE_QUEUE_MANIFEST.json',meta)
    return meta

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--mode',choices=['freeze','price-only','fullqueue'],required=True)
    p.add_argument('--source',required=True);p.add_argument('--calendar');p.add_argument('--out',required=True)
    x=p.parse_args()
    if x.mode=='freeze':check(bool(x.calendar),'NEED_CALENDAR');m=save_freeze(x.source,x.calendar,x.out)
    elif x.mode=='fullqueue':check(bool(x.calendar),'NEED_CALENDAR');m=fullmarket_flow_request_queue(x.source,x.calendar,x.out)
    else:m=price_allmarket(x.source,x.out)
    print(json.dumps(m,ensure_ascii=False))
