#!/usr/bin/env python3
"""R1-D26L: immutable historical source pilot for chronological daily-OHLC exit tests.

RESEARCH_ONLY, NOT PIT, NOT OOS, NO LIVE/ORDERS. The selection is created from
signal-time features ONLY. Exit models distinguish intraday ambiguous ordering.
"""
from __future__ import annotations
import argparse, csv, gzip, hashlib, io, json, math, time, zipfile
from datetime import date
from pathlib import Path
import pandas as pd

C='C_ABOVE224_RISING_WAVE_PULLBACK'
D='D_LEADER_CONSOLIDATION_BREAKOUT'
STRATS=(C,D)
N_PER_CELL=6
COLS=('strategy','date','code')
STRATEGIES=(('HOLD_D30',None,None),('TP5_SL5',.05,-.05),('TP10_SL5',.10,-.05),('TP15_SL7',.15,-.07),('TP10_SL7',.10,-.07),('HALF_TP10_BE',.10,-.05))


def sha(b:bytes)->str:return hashlib.sha256(b).hexdigest()
def write_csv(path,rows,cols):
    p=Path(path);p.parent.mkdir(parents=True,exist_ok=True)
    with p.open('w',newline='',encoding='utf-8') as f:
        w=csv.DictWriter(f,fieldnames=cols,extrasaction='ignore');w.writeheader();w.writerows(rows)
def read_csv(path):
    with open(path,encoding='utf-8-sig',newline='') as f:return list(csv.DictReader(f))
def member(z,contains):
    names=[n for n in z.namelist() if contains in n]
    if len(names)!=1:raise ValueError('AMBIGUOUS_OR_MISSING_SOURCE_'+contains)
    n=names[0];b=z.read(n)
    return gzip.decompress(b) if n.endswith('.gz') else b

def create_frozen_selection(d25_zip):
    # Explicitly NEVER read the future-outcomes member here.
    with zipfile.ZipFile(d25_zip) as z:
        b=member(z,'ALL_STRATEGIES_SIGNALTIME_448_FLOW_RS_NO_OUTCOMES')
    df=pd.read_csv(io.BytesIO(b),dtype={'code':str,'date':str},low_memory=False)
    df.columns=df.columns.str.lstrip('\ufeff')
    df=df[df.strategy.isin(STRATS)].copy()
    df['code']=df.code.astype(str).str.zfill(6)
    assert len(df)==10310 and not df.duplicated(list(COLS)).any(), 'ASOF_CARDINALITY'
    used=set();rows=[]
    for year in range(2021,2026):
        for s in STRATS:
            part=df[(df.date.str[:4]==str(year))&(df.strategy==s)][list(COLS)].copy()
            part['rank']=part.apply(lambda x:sha(f'R1D26L_LOCKED_ASOF_V1|{s}|{x.date}|{x.code}'.encode()),axis=1)
            for rec in part.sort_values(['rank','date','code']).itertuples(index=False):
                if rec.code in used:continue
                rows.append({'strategy':s,'date':rec.date,'code':rec.code,'year':year,'selection_basis':'SHA256_ASOF_ONLY'})
                used.add(rec.code)
                if sum(x['year']==year and x['strategy']==s for x in rows)==N_PER_CELL:break
            assert sum(x['year']==year and x['strategy']==s for x in rows)==N_PER_CELL,'NOT_ENOUGH_DISJOINT_ASOF'
    assert len(rows)==60 and len(used)==60
    return rows

def verify_frozen(csvfile,protocol):
    b=Path(csvfile).read_bytes()
    if sha(b)!=protocol['selection_csv_sha256']:raise ValueError('FROZEN_ASOF_SELECTION_SHA_MISMATCH')
    rows=read_csv(csvfile)
    if len(rows)!=60 or len({r['code'] for r in rows})!=60:raise ValueError('ASOF_SELECTION_OR_TICKER_DRIFT')
    c={(y,s):0 for y in range(2021,2026) for s in STRATS}
    for r in rows:
        if r['strategy'] not in STRATS or not (2021<=int(r['year'])<=2025):raise ValueError('INVALID_ASOF_STRATUM')
        if r['date'][:4]!=r['year'] or r['selection_basis']!='SHA256_ASOF_ONLY':raise ValueError('ASOF_SELECTION_METADATA')
        c[int(r['year']),r['strategy']]+=1
    if any(x!=6 for x in c.values()):raise ValueError('STRATIFICATION_DRIFT')
    return rows


def normalize_ohlcv(raw,selection):
    """Validate non-adjusted pykrx Korean-column OHLC; reject invalid/missing bars.

    The provider's historical values are retrieved today and are NOT PIT-certified.
    """
    if raw is None or raw.empty:raise ValueError('NO_PROVIDER_ROWS')
    out=[]
    if not raw.index.is_unique:raise ValueError('DUPLICATE_TRADE_DAY')
    names=['시가','고가','저가','종가','거래량','거래대금']
    if any(x not in raw.columns for x in names):raise ValueError('OHLC_COLUMN_MISSING')
    if raw.index.hasnans:raise ValueError('MISSING_TRADE_DATE')
    if not raw.index.is_monotonic_increasing:raw=raw.sort_index()
    for dt,r in raw.iterrows():
        day=pd.Timestamp(dt).date().isoformat()
        q={k:int(r[v]) for k,v in zip(['open','high','low','close','volume','amount'],names)}
        if not (0<q['low']<=min(q['open'],q['close'])<=max(q['open'],q['close'])<=q['high']):raise ValueError('OHLC_INCONSISTENT')
        if q['volume']<0 or q['amount']<0:raise ValueError('OHLC_NEGATIVE_ACTIVITY')
        out.append({**{x:selection[x] for x in COLS},'trade_date':day,**q,'provider':'pykrx_KRX_UNADJUSTED','source_PIT':'NOT_CERTIFIED'})
    return out


def provider_fetch(selection,start,end,delay=0.8):
    from pykrx import stock
    last=None
    for attempt in range(3):
        try:
            raw=stock.get_market_ohlcv_by_date(start.replace('-',''),end.replace('-',''),selection['code'],adjusted=False)
            bars=normalize_ohlcv(raw,selection)
            if delay:time.sleep(delay)
            return bars
        except (OSError,ConnectionError,TimeoutError) as exc:
            last=exc
            if delay:time.sleep(delay*(attempt+1))
    raise ValueError('PROVIDER_FETCH_RETRIES_EXHAUSTED') from last


def source_collect(selection_csv,protocol,dest,fetch_fn=provider_fetch):
    out=Path(dest);out.mkdir(parents=True,exist_ok=True)
    selected=verify_frozen(selection_csv,protocol)
    # Reference dates are frozen in the source before price retrieval. This is a
    # selected-signal-only list, not a result-conditioned selection.
    ranges=protocol['ranges']
    allbars=[];audit=[]
    for i,s in enumerate(selected):
        key='|'.join(s[x] for x in COLS)
        rg=ranges.get(key)
        if rg is None:raise ValueError('RANGE_NOT_PREDECLARED')
        start,end=rg['entry_day'],rg['exit_day']
        if not (s['date']<start<=end):raise ValueError('FORWARD_CALENDAR_RANGE_INVALID')
        reason='COMPLETE';bars=[]
        try:
            bars=fetch_fn(s,start,end)
            if len(bars)!=30 or bars[0]['trade_date']!=start or bars[-1]['trade_date']!=end:
                reason='DATE_OR_30_TRADING_SESSIONS_MISMATCH'
            if len({r['trade_date'] for r in bars})!=len(bars):reason='DUPLICATE_TRADE_DAYS'
            if any(not (start<=r['trade_date']<=end) for r in bars):reason='BAR_OUTSIDE_DECLARED_RANGE'
            # A >35% overnight gap might reflect an adjustment/corporate action;
            # this is a warning, NEVER proof of a particular corporate action.
            if len(bars)>1 and any(abs(bars[j]['open']/bars[j-1]['close']-1)>.35 for j in range(1,len(bars))):
                reason='OVERNIGHT_GAP_GT_35PCT_ACTION_OR_EXTREME_RISK'
        except Exception as exc:
            reason='REJECTED_'+type(exc).__name__
            bars=[]
        # Invalid source rows are not emitted to the executable OHLC archive.
        if reason=='COMPLETE':allbars.extend(bars)
        audit.append({**s,'start':start,'end':end,'retrieved_rows':len(bars),'status':reason})
        if (i+1)%10==0:print('R1D26L_COLLECTION_PROGRESS',i+1,'/60','valid',sum(x['status']=='COMPLETE' for x in audit),flush=True)
    csvfile=out/'R1D26L_RETRIEVED_UNADJUSTED_DAILY_OHLCV.csv.gz'
    bc=['strategy','date','code','trade_date','open','high','low','close','volume','amount','provider','source_PIT']
    with gzip.open(csvfile,'wt',encoding='utf-8',newline='') as f:
        w=csv.DictWriter(f,fieldnames=bc);w.writeheader();w.writerows(allbars)
    write_csv(out/'R1D26L_SOURCE_FETCH_AUDIT.csv',audit,['strategy','date','code','year','selection_basis','start','end','retrieved_rows','status'])
    m={'version':'R1D26L_DAILY_OHLC_60_PILOT_SOURCE_V1','phase':'collect','total_selected':len(selected),
       'source_valid_events':sum(x['status']=='COMPLETE' for x in audit),'source_raw_rows':len(allbars),
       'collection_attempted_events':len(audit),
       'source_invalid_events':sum(x['status']!='COMPLETE' for x in audit),
       'source_daily_sha256':sha(csvfile.read_bytes()),'selection_csv_sha256':protocol['selection_csv_sha256'],
       'source':'pykrx_KRX_adjusted_FALSE','official_independent_crosscheck':'PENDING',
       'corporate_action_PIT':'NOT_CERTIFIED','strategy_ready':False,'live_changed':False,'orders_placed':0}
    (out/'R1D26L_COLLECTION_MANIFEST.json').write_text(json.dumps(m,ensure_ascii=False,indent=2),encoding='utf-8')
    print('R1D26L_SOURCE_COLLECTION',json.dumps(m,ensure_ascii=False),flush=True)
    return m


def exit_fixed(bars,tp,sl,ordering='conservative'):
    """Return exit price/day/reason. Includes entry day. Stop loss gaps use OPEN.

    Same-day overlap is unknowable with OHLC and always reported ambiguous.
    """
    entry=bars[0]['open'];assert entry>0
    if tp is None:return float(bars[-1]['close']),bars[-1]['trade_date'],'D30_CLOSE',False
    tpprice=entry*(1+tp);slprice=entry*(1+sl)
    for r in bars:
        o,h,l=float(r['open']),float(r['high']),float(r['low'])
        if o<=slprice:return o,r['trade_date'],'STOP_GAP_OPEN',False
        if o>=tpprice:return tpprice,r['trade_date'],'TAKE_PROFIT_GAP_LIMIT',False
        a=l<=slprice;b=h>=tpprice
        if a and b:
            return ((slprice if ordering=='conservative' else tpprice),r['trade_date'],
                    'BOTH_INTRADAY_ORDER_UNKNOWN_'+ordering,True)
        if a:return slprice,r['trade_date'],'STOP_INTRADAY',False
        if b:return tpprice,r['trade_date'],'TAKE_PROFIT_INTRADAY',False
    return float(bars[-1]['close']),bars[-1]['trade_date'],'D30_CLOSE',False


def exit_half_then_be(bars,ordering='conservative'):
    """Sell half at TP10; other half only gets BE stop on following session.
    Initial pre-TP hard stop is -5%; zero-stop never retroactively acts same day.
    """
    entry=float(bars[0]['open']);tp=entry*1.10;sl=entry*.95
    for i,r in enumerate(bars):
        o,h,l=map(float,(r['open'],r['high'],r['low']))
        if o<=sl:return o,r['trade_date'],'FULL_STOP_GAP',False
        if o<tp and h>=tp and l<=sl:
            if ordering=='conservative':return sl,r['trade_date'],'FULL_BOTH_AMBIG_STOP_FIRST',True
            ambiguity=True
        else:ambiguity=False
        if o>=tp or h>=tp:
            for v in bars[i+1:]:
                vo=float(v['open']);vl=float(v['low'])
                if vo<=entry:return .5*tp+.5*vo,v['trade_date'],'HALF_TP10_AND_REMAINDER_BE_GAP',ambiguity
                if vl<=entry:return .5*tp+.5*entry,v['trade_date'],'HALF_TP10_AND_REMAINDER_BE',ambiguity
            return .5*tp+.5*float(bars[-1]['close']),bars[-1]['trade_date'],'HALF_TP10_AND_REMAINDER_D30',ambiguity
        if l<=sl:return sl,r['trade_date'],'FULL_STOP_INTRADAY',False
    return float(bars[-1]['close']),bars[-1]['trade_date'],'D30_CLOSE_NO_TP',False


def simulate(bars,cost_bps=20):
    if len(bars)!=30:raise ValueError('NOT_30_BARS')
    entry=float(bars[0]['open']);res=[]
    for name,tp,sl in STRATEGIES:
        for ordering in ('conservative','optimistic'):
            if name=='HALF_TP10_BE':price,day,why,amb=exit_half_then_be(bars,ordering)
            else:price,day,why,amb=exit_fixed(bars,tp,sl,ordering)
            pct=(price/entry-1)*100-cost_bps/100
            res.append({'scenario':name,'ordering':ordering,'entry_open':entry,'exit_price':price,
                        'exit_day':day,'exit_reason':why,'same_day_ambiguous':int(amb),
                        'net_return_pct':round(pct,8)})
    return res


def evaluation(source_dir,selection_csv,protocol,d25_zip,outdir):
    p=Path(source_dir);out=Path(outdir);out.mkdir(parents=True,exist_ok=True)
    m=json.loads((p/'R1D26L_COLLECTION_MANIFEST.json').read_text())
    selected=verify_frozen(selection_csv,protocol)
    if (m['collection_attempted_events']!=60 or
        m['source_raw_rows']!=30*m['source_valid_events'] or
        m['source_valid_events']<protocol['min_ohlc_valid_events']):
        raise ValueError('R1D26L_COLLECTION_TOO_SPARSE_TO_ANALYZE')
    f=p/'R1D26L_RETRIEVED_UNADJUSTED_DAILY_OHLCV.csv.gz'
    if sha(f.read_bytes())!=m['source_daily_sha256']:raise ValueError('SOURCE_BYTES_DRIFT')
    with gzip.open(f,'rt',encoding='utf-8') as z:bars=list(csv.DictReader(z))
    grouped={}
    for r in bars:
        k='|'.join(r[x] for x in COLS)
        for x in ('open','high','low','close','volume','amount'):r[x]=int(r[x])
        grouped.setdefault(k,[]).append(r)
    # Price outcomes accessed ONLY AFTER source fully sealed.
    with zipfile.ZipFile(d25_zip) as z:b=member(z,'D5_D10_D15_D20_D30_FUTURE_PRICE_PATHS')
    ref=pd.read_csv(io.BytesIO(b),dtype={'code':str,'date':str},low_memory=False)
    ref.columns=ref.columns.str.lstrip('\ufeff')
    ref=ref[(ref.horizon==30)&ref.strategy.isin(STRATS)]
    ref['code']=ref.code.astype(str).str.zfill(6)
    refidx={ '|'.join([r.strategy,r.date,r.code]):r for r in ref.itertuples(index=False)}
    details=[];simrows=[]
    for s in selected:
        key='|'.join(s[x] for x in COLS)
        v=sorted(grouped.get(key,[]),key=lambda r:r['trade_date'])
        rr=refidx.get(key)
        if rr is None:raise ValueError('REFERENCE_ROW_NOT_FOUND')
        if len(v)!=30:
            details.append({**s,'status':'SOURCE_OHLC_NOT_30_VALID_BARS','net_difference_pp':'','mfe_difference_pp':'','mae_difference_pp':''});continue
        if rr.status_out!='PRICE_PATH_COMPLETE_NOT_EXECUTION_CERTIFIED':
            details.append({**s,'status':'SOURCE_D30_CENSORED','net_difference_pp':'','mfe_difference_pp':'','mae_difference_pp':''});continue
        entry=v[0]['open'];net=100*(v[-1]['close']/entry-1)-.20
        mfe=100*(max(x['high'] for x in v)/entry-1)
        mae=100*(min(x['low'] for x in v)/entry-1)
        diff=[net-float(rr.net_20bps_pct),mfe-float(rr.mfe_intraday_pct),mae-float(rr.mae_intraday_pct)]
        # exact unadjusted base-vs-new daily OHLC check. A high mismatch may arise from provider/corporate action.
        valid=all(abs(d)<=0.01 for d in diff)
        details.append({**s,'status':'BASELINE_OHLC_PARITY_PASS' if valid else 'BASELINE_OHLC_PARITY_FAIL',
                        'net_difference_pp':round(diff[0],8),'mfe_difference_pp':round(diff[1],8),'mae_difference_pp':round(diff[2],8)})
        if valid:
            for row in simulate(v,cost_bps=protocol['round_trip_cost_bps']):
                simrows.append({**s,**row})
    if sum(x['status']=='BASELINE_OHLC_PARITY_PASS' for x in details)<protocol['min_parity_pass_events']:
        raise ValueError('R1D26L_TOO_FEW_OHLC_BASELINE_PARITY_PASSES')
    write_csv(out/'R1D26L_BASELINE_PRICE_PARITY.csv',details,['strategy','date','code','year','selection_basis','status','net_difference_pp','mfe_difference_pp','mae_difference_pp'])
    scols=['strategy','date','code','year','scenario','ordering','entry_open','exit_price','exit_day','exit_reason','same_day_ambiguous','net_return_pct']
    write_csv(out/'R1D26L_EXIT_SIMULATION_PAIRED_EVENTS.csv',simrows,scols)
    overview=[];yearrows=[]
    sumcols=['strategy','scenario','ordering','n','mean_net_pct','median_net_pct','positive_pct','loss5_pct','ambiguous_count','mean_delta_vs_hold_pp']
    yrcols=['strategy','year','scenario','ordering','n','mean_net_pct','median_net_pct','mean_delta_vs_hold_pp','ambiguous_count']
    if simrows:
        df=pd.DataFrame(simrows)
        baselines=df[df.scenario=='HOLD_D30'][['strategy','date','code','ordering','net_return_pct']].rename(columns={'net_return_pct':'hold_net_pct'})
        df=df.merge(baselines,on=['strategy','date','code','ordering'],validate='many_to_one')
        df['delta_vs_hold_pp']=df.net_return_pct.astype(float)-df.hold_net_pct.astype(float)
        for (strat,sc,order),g in df.groupby(['strategy','scenario','ordering']):
            returns=g.net_return_pct.astype(float)
            overview.append({'strategy':strat,'scenario':sc,'ordering':order,'n':len(g),
                             'mean_net_pct':round(float(returns.mean()),4),'median_net_pct':round(float(returns.median()),4),
                             'positive_pct':round(100*float((returns>0).mean()),3),
                             'loss5_pct':round(100*float((returns<=-5).mean()),3),
                             'ambiguous_count':int(g.same_day_ambiguous.sum()),
                             'mean_delta_vs_hold_pp':round(float(g.delta_vs_hold_pp.mean()),4)})
        for (strat,year,sc,order),g in df.groupby(['strategy','year','scenario','ordering']):
            yearrows.append({'strategy':strat,'year':year,'scenario':sc,'ordering':order,'n':len(g),
                             'mean_net_pct':round(float(g.net_return_pct.mean()),4),
                             'median_net_pct':round(float(g.net_return_pct.median()),4),
                             'mean_delta_vs_hold_pp':round(float(g.delta_vs_hold_pp.mean()),4),
                             'ambiguous_count':int(g.same_day_ambiguous.sum())})
    write_csv(out/'R1D26L_SCENARIO_SUMMARY.csv',overview,sumcols)
    write_csv(out/'R1D26L_YEAR_STRATEGY_SCENARIO_SUMMARY.csv',yearrows,yrcols)
    qa={'version':'R1D26L_EXIT_PATH_FIRST_CHRONOLOGICAL_PILOT_V1','frozen_signals':60,
        'source_complete':m['source_valid_events'],'reference_D30_complete':sum(x['status'] in ('BASELINE_OHLC_PARITY_PASS','BASELINE_OHLC_PARITY_FAIL') for x in details),
        'source_missing_or_invalid':sum(x['status']=='SOURCE_OHLC_NOT_30_VALID_BARS' for x in details),
        'reference_D30_censored_with_complete_OHLC':sum(x['status']=='SOURCE_D30_CENSORED' for x in details),
        'baseline_price_parity':sum(x['status']=='BASELINE_OHLC_PARITY_PASS' for x in details),
        'baseline_price_mismatch':sum(x['status']=='BASELINE_OHLC_PARITY_FAIL' for x in details),
        'not_fill_certified':True,'same_day_unknown_modelled_both_orders':True,'round_trip_cost_bps':protocol['round_trip_cost_bps'],
        'source_official_crosscheck':'PENDING','corporate_action_PIT':'NOT_VERIFIED','prospective_OOS':False,
        'strategy_ready':False,'live_changed':False,'orders_placed':0,
        'file_sha256':{f.name:sha(f.read_bytes()) for f in out.glob('*.csv')}}
    (out/'R1D26L_QA_MANIFEST.json').write_text(json.dumps(qa,ensure_ascii=False,indent=2),encoding='utf-8')
    print('R1D26L_EXIT_RESEARCH_NOT_LIVE',json.dumps(qa,ensure_ascii=False),flush=True)
    return qa


def main():
    ap=argparse.ArgumentParser();ap.add_argument('--mode',required=True,choices=['collect','evaluate'])
    ap.add_argument('--selection',required=True);ap.add_argument('--protocol',required=True)
    ap.add_argument('--out',required=True);ap.add_argument('--source-dir',default='');ap.add_argument('--d25-zip',default='');ap.add_argument('--sleep-seconds',type=float,default=.8)
    args=ap.parse_args();p=json.loads(Path(args.protocol).read_text(encoding='utf-8'))
    if args.mode=='collect':source_collect(args.selection,p,args.out,lambda s,a,b:provider_fetch(s,a,b,args.sleep_seconds))
    else:evaluation(args.source_dir,args.selection,p,args.d25_zip,args.out)
if __name__=='__main__':main()
