#!/usr/bin/env python3
"""R1-D26N immutable structural support / support-break stop research.
RESEARCH ONLY. Historical unadjusted daily provider snapshots, NOT certified PIT,
not real execution, not independent OOS. No outcome data enters support formation.
"""
import argparse, csv, gzip, hashlib, io, json, statistics, time, zipfile
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

C='C_ABOVE224_RISING_WAVE_PULLBACK'
D='D_LEADER_CONSOLIDATION_BREAKOUT'
EVENT=('strategy','date','code')
OHLC=('open','high','low','close','volume','amount')
PRE_NAME='R1D26N_PRE_SIGNAL_120_UNADJUSTED_OHLCV.csv.gz'
FETCH_NAME='R1D26N_PRE_SIGNAL_FETCH_AUDIT.csv'
RESULT_NAME='R1D26N_PAIRED_STRUCTURE_STOP_EVENTS.csv'
LEVEL_NAME='R1D26N_SIGNAL_TIME_SUPPORT_LEVELS.csv'
SUMMARY_NAME='R1D26N_SUPPORT_EXIT_SUMMARY.csv'
YEAR_NAME='R1D26N_YEAR_STRATEGY_SUPPORT_SUMMARY.csv'
CASE_NAME='R1D26N_SUPPORT_BREAK_CASE_CATEGORIES.csv'


def digest(b):return hashlib.sha256(b).hexdigest()
def jwrite(path,obj):Path(path).write_text(json.dumps(obj,ensure_ascii=False,indent=2,sort_keys=True)+'\n',encoding='utf-8')
def csvread_bytes(b, zipped=False):return list(csv.DictReader(io.StringIO(gzip.decompress(b).decode('utf8') if zipped else b.decode('utf-8-sig'))))
def cwrite(path,rows,cols):
    with open(path,'w',encoding='utf-8',newline='') as f:
        w=csv.DictWriter(f,fieldnames=cols,extrasaction='ignore');w.writeheader();w.writerows(rows)
def ident(r):return '|'.join(r[k] for k in EVENT)
def events(selection,protocol):
    p=Path(selection); b=p.read_bytes()
    if digest(b)!=protocol['selection_csv_sha256']:raise ValueError('FROZEN_SELECTION_SHA_DRIFT')
    rows=csvread_bytes(b)
    if len(rows)!=60 or len({ident(r) for r in rows})!=60:raise ValueError('60_FROZEN_IDENTITIES_INVALID')
    if len({r['code'] for r in rows})!=60:raise ValueError('TICKER_DISJOINT_INVALID')
    if any(len(r['code'])!=6 or r['date'][:4]!=r['year'] or r['strategy'] not in (C,D) for r in rows):raise ValueError('EVENT_FIELDS_INVALID')
    return rows


def validate_bar(b):
    try:
        x={k:int(b[k]) for k in OHLC}
        if not 0<x['low']<=min(x['open'],x['close'])<=max(x['open'],x['close'])<=x['high']:raise ValueError()
        if x['volume']<0 or x['amount']<0:raise ValueError()
        return x
    except Exception: raise ValueError('PRICE_OHLC_INVALID')


def parse_pre_bars(raw,event):
    if raw is None or raw.empty:raise ValueError('PROVIDER_EMPTY')
    if not raw.index.is_unique:raise ValueError('DUPLICATE_DAYS')
    names={'open':'시가','high':'고가','low':'저가','close':'종가','volume':'거래량','amount':'거래대금'}
    if not all(v in raw.columns for v in names.values()):raise ValueError('PROVIDER_PRICE_COLUMNS_MISSING')
    bars=[]
    for dt,r in raw.sort_index().iterrows():
        d=dt.strftime('%Y-%m-%d')
        if d>event['date']:raise ValueError('FUTURE_BAR_IN_PRE_SIGNAL')
        bar={**{k:event[k] for k in EVENT},'trade_date':d,**{k:r[names[k]] for k in OHLC}}
        bar.update(validate_bar(bar));bars.append(bar)
    return bars


def fetch_provider(event,delay):
    from pykrx import stock
    start=(date.fromisoformat(event['date'])-timedelta(days=240)).strftime('%Y%m%d')
    end=event['date'].replace('-','')
    raw=stock.get_market_ohlcv_by_date(start,end,event['code'],adjusted=False)
    out=parse_pre_bars(raw,event)
    if delay:time.sleep(delay)
    return out[-120:]


def collect(selection,protocol,outdir,fetch=fetch_provider,sleep_seconds=1):
    out=Path(outdir);out.mkdir(parents=True,exist_ok=True)
    chosen=events(selection,protocol)
    joined=[];aud=[]
    for i,event in enumerate(chosen,1):
        status='COMPLETE'; bars=[]
        try:
            bars=fetch(event,sleep_seconds)
            days=[r['trade_date'] for r in bars]
            if len(bars)<65 or len(bars)>120 or not days or days[-1]!=event['date'] or len(days)!=len(set(days)) or days!=sorted(days):status='INSUFFICIENT_PRE_SIGNAL_HISTORY_OR_ASOF_DRIFT'
            elif any(abs(bars[j]['open']/bars[j-1]['close']-1)>.35 for j in range(1,len(bars))):status='PRE_SIGNAL_LARGE_GAP_GT35PCT_ACTION_RISK'
            if any(x['trade_date']>event['date'] for x in bars):status='FUTURE_PRE_SIGNAL_LEAK'
        except Exception as ex:status='PROVIDER_REJECTED_'+type(ex).__name__
        if status=='COMPLETE':joined.extend(bars)
        aud.append({**event,'provider_start':(date.fromisoformat(event['date'])-timedelta(days=240)).isoformat(), 'provider_end':event['date'],'retained_rows':len(bars),'status':status})
        if i%10==0:print('R1D26N_PRE_COLLECTION',i,'/60 valid',sum(r['status']=='COMPLETE' for r in aud),flush=True)
    f=out/PRE_NAME
    with gzip.open(f,'wt',encoding='utf-8',newline='') as z:
        names=[*EVENT,'trade_date',*OHLC]
        w=csv.DictWriter(z,fieldnames=names);w.writeheader();w.writerows(joined)
    cwrite(out/FETCH_NAME,aud,[*EVENT,'provider_start','provider_end','retained_rows','status'])
    m={'version':'R1D26N_PRE_SIGNAL_SOURCE_V1','total_selected':60,'attempted':len(aud),'valid_events':sum(r['status']=='COMPLETE' for r in aud),'invalid_events':sum(r['status']!='COMPLETE' for r in aud),'valid_rows':len(joined),'source_file_sha256':digest(f.read_bytes()),'selection_csv_sha256':protocol['selection_csv_sha256'],'provider':'pykrx_KRX_adjusted_FALSE','corporate_actions_PIT':'NOT_VERIFIED','independent_official_source':'PENDING','strategy_ready':False,'live_changed':False,'orders_placed':0}
    jwrite(out/'R1D26N_PRE_SIGNAL_SOURCE_MANIFEST.json',m)
    print('R1D26N_PRE_SOURCE_SEALED',json.dumps({k:m[k] for k in ['total_selected','valid_events','invalid_events','valid_rows']},ensure_ascii=False))
    return m


def atr14(rows):
    if len(rows)<16:raise ValueError('ATR_LOOKBACK_MISSING')
    t=[]
    for j in range(len(rows)-14,len(rows)):
        b=rows[j];p=rows[j-1]
        t.append(max(b['high']-b['low'],abs(b['high']-p['close']),abs(b['low']-p['close'])))
    return sum(t)/14


def pivots(rows,window=40):
    # Two confirmation bars after a pivot; all bars must be on/before signal date.
    n=len(rows);res=[]
    for i in range(max(2,n-window),n-2):
        x=rows[i]['low']
        if all(x<rows[j]['low'] for j in [i-2,i-1,i+1,i+2]):res.append((i,x))
    return res


def levels(pre,event):
    if len(pre)<65 or pre[-1]['trade_date']!=event['date'] or any(r['trade_date']>event['date'] for r in pre):raise ValueError('SUPPORT_FUTURE_OR_SHORT_HISTORY')
    n=len(pre);a=atr14(pre);ma20=sum(r['close'] for r in pre[-20:])/20;ma60=sum(r['close'] for r in pre[-60:])/60
    if event['strategy']==C:
        p=pivots(pre)
        recent=p[-1][1] if p else None
        d={'C_CONFIRMED_SWING_LOW':recent,'C_SWING_ATR05':(recent-.5*a) if recent else None,'C_PULLBACK_LOW20':min(r['low'] for r in pre[-20:]),'C_MA20_STATIC':ma20,'C_MA60_STATIC':ma60}
    else:
        # Lookback excludes signal day: the breakout bar cannot create its own threshold.
        prev=pre[-21:-1]
        top=max(r['high'] for r in prev)
        d={'D_PRE20_BREAKOUT_RETEST':top,'D_BREAKOUT_ATR05':top-.5*a,'D_PRE20_BOX_FLOOR':min(r['low'] for r in prev),'D_MA20_STATIC':ma20,'D_MA60_STATIC':ma60}
    return d,round(a,4)


def stop_intraday(bars,threshold,cost_bps=20):
    entry=bars[0]['open']
    if not (0<threshold<entry):return None,'SUPPORT_NOT_BELOW_ENTRY',0
    for r in bars:
        if r['open']<=threshold:return (r['open']/entry-1)*100-cost_bps/100,'STOP_GAP_OPEN',r['trade_date']
        if r['low']<=threshold:return (threshold/entry-1)*100-cost_bps/100,'STOP_INTRADAY_ASSUMED_AT_LEVEL',r['trade_date']
    return (bars[-1]['close']/entry-1)*100-cost_bps/100,'HOLD_TO_D30',bars[-1]['trade_date']


def stop_close_confirm(bars,threshold,cost_bps=20):
    entry=bars[0]['open']
    if not (0<threshold<entry):return None,'SUPPORT_NOT_BELOW_ENTRY',0
    for i,r in enumerate(bars[:-1]):
        if r['close']<threshold:return (bars[i+1]['open']/entry-1)*100-cost_bps/100,'CONFIRMED_CLOSE_NEXT_OPEN',bars[i+1]['trade_date']
    return (bars[-1]['close']/entry-1)*100-cost_bps/100,'HOLD_TO_D30',bars[-1]['trade_date']


def grouped_bars(rows,kind):
    out=defaultdict(list)
    for r in rows:
        t={**r}
        for k in OHLC:t[k]=int(t[k]);validate_bar(t)
        if t['trade_date']<=t['date'] and kind=='post':raise ValueError('POST_BAR_AT_OR_BEFORE_SIGNAL')
        if t['trade_date']>t['date'] and kind=='pre':raise ValueError('FUTURE_DATA_IN_PRE_SIGNAL')
        out[ident(t)].append(t)
    for b in out.values():
        b.sort(key=lambda x:x['trade_date'])
        if len({r['trade_date'] for r in b})!=len(b):raise ValueError('DUPLICATE_TRADE_DAY')
    return out


def load_original_l(zip_path,expected_digest,selection_sha):
    p=Path(zip_path)
    if digest(p.read_bytes())!=expected_digest:raise ValueError('L_ORIGINAL_ZIP_BYTES_DRIFT')
    with zipfile.ZipFile(p) as z:
        root='reports/r1d26l/result/source/'
        man=json.loads(z.read(root+'R1D26L_COLLECTION_MANIFEST.json'))
        raw=z.read(root+'R1D26L_RETRIEVED_UNADJUSTED_DAILY_OHLCV.csv.gz')
        s=z.read('data/R1D26L_FROZEN_60_ASOF_EVENTS.csv')
        parity=csvread_bytes(z.read('reports/r1d26l/result/exit_pilot/R1D26L_BASELINE_PRICE_PARITY.csv'))
    if digest(s)!=selection_sha or man['selection_csv_sha256']!=selection_sha:raise ValueError('L_FROZEN_SELECTION_CHANGED')
    if digest(raw)!=man['source_daily_sha256'] or man['source_valid_events']!=59 or man['source_raw_rows']!=1770:raise ValueError('L_PRICE_SOURCE_INVALID')
    if sum(r['status']=='BASELINE_OHLC_PARITY_PASS' for r in parity)!=59:raise ValueError('L_PRICE_PARITY_NOT_VERIFIED')
    return grouped_bars(csvread_bytes(raw,zipped=True),'post')


def evaluate(selection,protocol,pre_source,lzip,outdir):
    out=Path(outdir);out.mkdir(parents=True,exist_ok=True)
    chosen=events(selection,protocol)
    p=Path(pre_source)
    sm=json.loads((p/'R1D26N_PRE_SIGNAL_SOURCE_MANIFEST.json').read_text())
    b=(p/PRE_NAME).read_bytes()
    if sm['source_file_sha256']!=digest(b) or sm['selection_csv_sha256']!=protocol['selection_csv_sha256']:raise ValueError('PRE_SOURCE_SHA_DRIFT')
    if sm['attempted']!=60 or sm['valid_events']<protocol['minimum_pre_valid_for_exploration'] or sm['valid_rows']<sm['valid_events']*65:raise ValueError('INSUFFICIENT_PRE_SIGNAL_COVERAGE')
    pre=grouped_bars(csvread_bytes(b,zipped=True),'pre')
    post=load_original_l(lzip,protocol['r1d26l_zip_sha256'],protocol['selection_csv_sha256'])
    level_rows=[];paired=[];case_rows=[]
    cost=protocol['round_trip_cost_bps']
    for s in chosen:
        k=ident(s); pb=pre.get(k);future=post.get(k)
        if not pb or not future:
            case_rows.append({**s,'status':'PRE_SIGNAL_SOURCE_MISSING' if not pb else 'POST_SIGNAL_SOURCE_MISSING','method':'','exit_type':'','case':'CENSORED_NO_IMPUTATION','hold_net_pct':'','stop_net_pct':'','delta_pp':''});continue
        if len(future)!=30 or future[0]['trade_date']<=s['date']:raise ValueError('L_POST_SOURCE_INCOMPLETE')
        entry=future[0]['open'];hold=(future[-1]['close']/entry-1)*100-cost/100
        lines,atr=levels(pb,s)
        for method,support in lines.items():
            distance=((entry-support)/entry*100) if support is not None else None
            asof_close=pb[-1]['close']
            valid=support is not None and support>0 and support<entry and support<asof_close
            diagnostic= ('NO_CONFIRMED_PIVOT' if support is None else 'LINE_ALREADY_BROKEN_ON_SIGNAL_CLOSE' if support>=asof_close else 'ENTRY_OPEN_BELOW_LINE' if support>=entry else 'WIDE_RISK_GT10PCT' if distance>10 else 'VALID')
            level_rows.append({**s,'method':method,'signal_time_support':round(support,4) if support is not None else '', 'signal_close':asof_close,'entry_open_observed':entry,'risk_distance_pct':round(distance,4) if distance is not None else '', 'ATR14_asof':atr,'pre_bars':len(pb),'status':diagnostic})
            for mode in ('intraday_assumed_stop','close_confirm_next_open'):
                if valid:
                    ret,reason,day=(stop_intraday if mode=='intraday_assumed_stop' else stop_close_confirm)(future,support,cost)
                else:ret,reason,day=None,diagnostic,''
                triggered= ret is not None and reason not in ('HOLD_TO_D30',)
                case='NO_TRADE_LINE_UNUSABLE' if ret is None else 'NO_BREAK_HOLD' if not triggered else 'LOSS_PROTECTED' if hold<=-10 and ret>hold else 'MISSED_REBOUND' if hold>=10 and ret<hold else 'EARLY_STOP_OTHER'
                try:
                    trigger_idx=next((i for i,b in enumerate(future) if b['trade_date']==day),None) if triggered else None
                    # This is a POST-EXIT LABEL ONLY, never a stop decision input.
                    later=future[trigger_idx:min(len(future),trigger_idx+5)] if trigger_idx is not None else []
                    reclaim_any=int(any(b['close']>=support for b in later)) if later else ''
                except Exception:reclaim_any=''
                paired.append({**s,'reclaim_in_5d_after_exit_POSTLABEL':reclaim_any,'method':method,'exit_type':mode,'support':round(support,4) if support is not None else '', 'support_risk_pct':round(distance,4) if distance is not None else '', 'status':diagnostic,'exit_reason':reason,'exit_date':day,'hold_net_pct':round(hold,7),'stop_net_pct':round(ret,7) if ret is not None else '', 'delta_pp':round(ret-hold,7) if ret is not None else '', 'case':case,'entry_open':entry})
                if triggered:case_rows.append({**s,'status':diagnostic,'method':method,'exit_type':mode,'case':case,'hold_net_pct':round(hold,7),'stop_net_pct':round(ret,7),'delta_pp':round(ret-hold,7)})
    summary=[]
    for strategy in (C,D):
        for method in sorted({r['method'] for r in paired if r['strategy']==strategy}):
            for mode in ('intraday_assumed_stop','close_confirm_next_open'):
                a=[r for r in paired if r['strategy']==strategy and r['method']==method and r['exit_type']==mode]
                good=[r for r in a if r['stop_net_pct']!='']
                if not good:continue
                hit=[r for r in good if r['exit_reason']!='HOLD_TO_D30']
                summary.append({'strategy':strategy,'method':method,'exit_type':mode,'total_with_both_sources':len(a),'eligible_lines':len(good),'invalid_support':len(a)-len(good),'triggered_stops':len(hit),'mean_hold_net_pct':round(statistics.mean(float(r['hold_net_pct']) for r in good),4),'mean_stop_net_pct':round(statistics.mean(float(r['stop_net_pct']) for r in good),4),'mean_delta_pp':round(statistics.mean(float(r['delta_pp']) for r in good),4),'median_delta_pp':round(statistics.median(float(r['delta_pp']) for r in good),4),'protected_10pct_cases':sum(r['case']=='LOSS_PROTECTED' for r in good),'missed_rebound_10pct_cases':sum(r['case']=='MISSED_REBOUND' for r in good),'wide_stop_gt10pct':sum(float(r['support_risk_pct'])>10 for r in good),'reclaimed_5d_after_trigger':sum(r['reclaim_in_5d_after_exit_POSTLABEL']==1 for r in hit)})
    for t in (level_rows,paired,case_rows):
        for r in t:r['year']=r['date'][:4]
    cwrite(out/LEVEL_NAME,level_rows,[*EVENT,'year','method','signal_time_support','signal_close','entry_open_observed','risk_distance_pct','ATR14_asof','pre_bars','status'])
    cwrite(out/RESULT_NAME,paired,[*EVENT,'year','method','exit_type','support','support_risk_pct','status','exit_reason','exit_date','hold_net_pct','stop_net_pct','delta_pp','case','reclaim_in_5d_after_exit_POSTLABEL','entry_open'])
    cwrite(out/CASE_NAME,case_rows,[*EVENT,'year','status','method','exit_type','case','hold_net_pct','stop_net_pct','delta_pp'])
    cwrite(out/SUMMARY_NAME,summary,['strategy','method','exit_type','total_with_both_sources','eligible_lines','invalid_support','triggered_stops','mean_hold_net_pct','mean_stop_net_pct','mean_delta_pp','median_delta_pp','protected_10pct_cases','missed_rebound_10pct_cases','wide_stop_gt10pct','reclaimed_5d_after_trigger'])
    yr=[]
    for strat in (C,D):
      for year in sorted({r['year'] for r in paired}):
       for method in sorted({r['method'] for r in paired if r['strategy']==strat}):
        for mode in ('intraday_assumed_stop','close_confirm_next_open'):
         q=[r for r in paired if r['strategy']==strat and r['year']==year and r['method']==method and r['exit_type']==mode and r['stop_net_pct']!='']
         if not q:continue
         yr.append({'strategy':strat,'year':year,'method':method,'exit_type':mode,'eligible_n':len(q),'mean_delta_pp':round(statistics.mean(float(r['delta_pp']) for r in q),4),'protected_count':sum(r['case']=='LOSS_PROTECTED' for r in q),'missed_rebound_count':sum(r['case']=='MISSED_REBOUND' for r in q)})
    cwrite(out/YEAR_NAME,yr,['strategy','year','method','exit_type','eligible_n','mean_delta_pp','protected_count','missed_rebound_count'])
    n=len(set(ident(r) for r in level_rows))
    manifest={'version':'R1D26N_STRUCTURAL_SUPPORT_D30_PILOT_V1','selected_unchanged':60,'pre_valid':sm['valid_events'],'post_valid':len(post),'paired_pre_post_valid_events':n,'eligible_trade_rule_rows':sum(r['stop_net_pct']!='' for r in paired),'support_from_signal_time_only':True,'support_static_after_signal':True,'daily_high_low_stop_assumed_not_fills':True,'price_PIT':'NOT_CERTIFIED','official_independent_crosscheck':'PENDING','corporate_action_PIT':'NOT_CERTIFIED','prospective_OOS':False,'strategy_ready':False,'live_changed':False,'orders_placed':0,'source_pre_SHA256':sm['source_file_sha256'],'source_L_artifact_SHA256':protocol['r1d26l_zip_sha256'],'file_sha256':{n:digest((out/n).read_bytes()) for n in (LEVEL_NAME,RESULT_NAME,CASE_NAME,SUMMARY_NAME,YEAR_NAME)}}
    jwrite(out/'R1D26N_QA_MANIFEST.json',manifest)
    print('R1D26N_STRUCTURAL_SUPPORT_QA',json.dumps({k:manifest[k] for k in ['selected_unchanged','pre_valid','post_valid','paired_pre_post_valid_events','eligible_trade_rule_rows','strategy_ready']},ensure_ascii=False))
    return manifest


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--mode',choices=['collect','evaluate'],required=True)
    ap.add_argument('--selection',required=True)
    ap.add_argument('--protocol',required=True)
    ap.add_argument('--out',required=True)
    ap.add_argument('--pre-source-dir')
    ap.add_argument('--l-zip')
    ap.add_argument('--sleep-seconds',type=float,default=1)
    a=ap.parse_args();pr=json.loads(Path(a.protocol).read_text())
    if a.mode=='collect':collect(a.selection,pr,a.out,sleep_seconds=a.sleep_seconds)
    else:
        if not a.pre_source_dir or not a.l_zip:ap.error('--pre-source-dir and --l-zip required for evaluate')
        evaluate(a.selection,pr,a.pre_source_dir,a.l_zip,a.out)
if __name__=='__main__':main()
