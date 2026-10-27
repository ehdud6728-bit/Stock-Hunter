#!/usr/bin/env python3
"""R1D26O read-only retrospective fixed-support reentry audit (NOT PIT/OOS/fills).

Data: immutable N historical pre-signal support reports, and L historical 30-bar
unadjusted OHLC. This module contains no provider fetching, order, live, selection
or risk-parameter optimization logic. Reclaim/retest is only discovered at close,
with earliest possible reentry the following session OPEN.
"""
from __future__ import annotations
import argparse, collections, csv, gzip, hashlib, io, json, math, statistics, zipfile
from pathlib import Path

N_PRICE = 'reports/r1d26n/result/audit/R1D26N_PAIRED_STRUCTURE_STOP_EVENTS.csv'
N_LEVEL = 'reports/r1d26n/result/audit/R1D26N_SIGNAL_TIME_SUPPORT_LEVELS.csv'
N_QA = 'reports/r1d26n/result/audit/R1D26N_QA_MANIFEST.json'
L_PRICE = 'reports/r1d26l/result/source/R1D26L_RETRIEVED_UNADJUSTED_DAILY_OHLCV.csv.gz'
L_QA = 'reports/r1d26l/result/source/R1D26L_COLLECTION_MANIFEST.json'
SELECTION = 'data/R1D26L_FROZEN_60_ASOF_EVENTS.csv'
C='C_ABOVE224_RISING_WAVE_PULLBACK'
D='D_LEADER_CONSOLIDATION_BREAKOUT'
EVENT=('strategy','date','code')
RESULT='R1D26O_PAIRED_REENTRY_590_ROWS.csv'
SUMMARY='R1D26O_STRATEGY_SUPPORT_REENTRY_SUMMARY.csv'
YEAR='R1D26O_YEAR_STRATEGY_REENTRY_SUMMARY.csv'
CASES='R1D26O_REENTRY_AND_GIVEBACK_CASES.csv'
MANIFEST='R1D26O_QA_MANIFEST.json'


def sha(data):return hashlib.sha256(data).hexdigest()
def readcsv(blob,compress=False):
    if compress: blob=gzip.decompress(blob)
    return list(csv.DictReader(io.StringIO(blob.decode('utf-8-sig'))))
def savecsv(path,rows,fields):
    with Path(path).open('w',newline='',encoding='utf-8') as h:
        w=csv.DictWriter(h,fields,extrasaction='ignore');w.writeheader();w.writerows(rows)
def eid(r):return tuple(str(r[k]) for k in EVENT)
def closepct(price,entry,cost_bps):return (price/entry-1)*100-cost_bps/100

def assert_source_integrity(n_zip,l_zip,p):
    nb=Path(n_zip).read_bytes();lb=Path(l_zip).read_bytes()
    if sha(nb)!=p['parent_n_zip_sha256'] or sha(lb)!=p['parent_l_zip_sha256']:raise ValueError('PINNED_UPSTREAM_ZIP_SHA_MISMATCH')
    with zipfile.ZipFile(io.BytesIO(nb)) as n,zipfile.ZipFile(io.BytesIO(lb)) as l:
        nm=json.loads(n.read(N_QA));lm=json.loads(l.read(L_QA))
        if nm['selected_unchanged']!=60 or nm['pre_valid']!=60 or nm['post_valid']!=59 or nm['paired_pre_post_valid_events']!=59:raise ValueError('N_SOURCE_COUNT_DRIFT')
        if not nm['support_from_signal_time_only'] or not nm['support_static_after_signal'] or nm['strategy_ready'] or nm['prospective_OOS']:raise ValueError('N_ASOF_OR_LIVE_DRIFT')
        if lm['source_valid_events']!=59 or lm['source_raw_rows']!=1770 or lm['strategy_ready']:raise ValueError('L_PRICE_COUNT_DRIFT')
        if sha(n.read(SELECTION))!=p['selection_csv_sha256'] or sha(l.read(SELECTION))!=p['selection_csv_sha256']:raise ValueError('EVENT_SELECTION_CHANGED')
        for name,dig in nm['file_sha256'].items():
            if sha(n.read('reports/r1d26n/result/audit/'+name))!=dig:raise ValueError('N_INTERNAL_RESULT_SHA_DRIFT:'+name)
        if sha(l.read(L_PRICE))!=lm['source_daily_sha256']:raise ValueError('L_INTERNAL_PRICE_SHA_DRIFT')
        source_n=readcsv(n.read(N_PRICE));levels=readcsv(n.read(N_LEVEL));source_l=readcsv(l.read(L_PRICE),True)
    if len(source_n)!=590 or len(levels)!=295 or len(source_l)!=1770:raise ValueError('UNEXPECTED_SOURCE_CARDINALITIES')
    gr=collections.defaultdict(list)
    for x in source_l:
        y={**x}
        for k in ('open','high','low','close'): y[k]=float(y[k])
        if any(not math.isfinite(y[k]) or y[k]<=0 for k in ('open','high','low','close')):raise ValueError('INVALID_DAILY_OHLC')
        if not(y['low']<=min(y['open'],y['close'])<=max(y['open'],y['close'])<=y['high']):raise ValueError('BROKEN_DAILY_OHLC')
        if y['trade_date']<=y['date']:raise ValueError('POST_CONTAINS_SIGNAL_OR_PRE')
        gr[eid(x)].append(y)
    for key,bars in gr.items():
        bars.sort(key=lambda x:x['trade_date'])
        if len(bars)!=30 or len({b['trade_date'] for b in bars})!=30:raise ValueError('INVALID_POST_PATH:'+str(key))
    if len(gr)!=59 or len({eid(x) for x in levels})!=59:raise ValueError('INVALID_UNIQUE_EVENTS')
    return source_n,levels,gr,nm,lm


def first_stop(bars,threshold,mode):
    """Return (exit_index, net_sale_price, decision_index, reason).
    For intraday, the fill at support is assumed. For close-confirm, settlement
    is the FOLLOWING open (last bar close is no stop). The decision_index is
    the original close breach, not the later execution index.
    """
    if mode=='intraday_assumed_stop':
        for i,b in enumerate(bars):
            if b['open']<=threshold:return i,b['open'],i,'STOP_GAP_OPEN'
            if b['low']<=threshold:return i,threshold,i,'STOP_INTRADAY_ASSUMED_AT_LEVEL'
    elif mode=='close_confirm_next_open':
        for i,b in enumerate(bars[:-1]):
            if b['close']<threshold:return i+1,bars[i+1]['open'],i,'CONFIRMED_CLOSE_NEXT_OPEN'
    else:raise ValueError('UNKNOWN_EXIT_MODE')
    return len(bars)-1,bars[-1]['close'],None,'HOLD_TO_D30'


def reclaim_close_index(bars,support,after_exit_index):
    """Earliest reclamation CLOSE only AFTER exit execution: if intraday exit,
    the exit day's close is observable (at EOD). For next-open exit, that new
    execution day close can be used; never the breach day's close as recovery.
    """
    return next((i for i in range(after_exit_index,len(bars)-1) if bars[i]['close']>=support),None)


def reentry_signal(bars,support,stop_idx,style,p):
    rec=reclaim_close_index(bars,support,stop_idx)
    if rec is None:return {'reclaim_i':None,'signal_i':None,'entry_i':None,'status':'NO_RECLAIM_CLOSE_BEFORE_D30'}
    if style=='reclaim_next_open':sig=rec
    elif style=='retest_next_open':
        # A retest may happen only on a subsequent trading session AFTER the
        # reclaim close (never use same-day high/low to validate its own close).
        # Observe <=2% above fixed support and close at/above line. This is
        # predeclared, not tuned from outcomes. Search at most five sessions.
        xs=range(rec+1,min(len(bars)-1,rec+p['retest_max_observation_sessions']+1))
        sig=next((i for i in xs if bars[i]['low']<=support*(1+p['retest_upper_tolerance_pct']/100) and bars[i]['close']>=support),None)
        if sig is None:return {'reclaim_i':rec,'signal_i':None,'entry_i':None,'status':'RECLAIM_WITHOUT_CONFIRMED_RETEST'}
    else:raise ValueError('UNKNOWN_REENTRY_STYLE')
    ent=sig+1
    if ent>=len(bars):return {'reclaim_i':rec,'signal_i':sig,'entry_i':None,'status':'NO_NEXT_SESSION_BEFORE_D30'}
    if bars[ent]['open']<=support:return {'reclaim_i':rec,'signal_i':sig,'entry_i':None,'status':'NEXT_OPEN_ALREADY_BELOW_LINE_SKIP'}
    return {'reclaim_i':rec,'signal_i':sig,'entry_i':ent,'status':'REENTERED'}


def secondary_exit(bars,support,entry_idx,mode):
    """Single reentry only; fixed same support, no third entry.
    A close-break second stop executes following OPEN if available.
    """
    if mode=='intraday_assumed_stop':
        for j in range(entry_idx,len(bars)):
            b=bars[j]
            if b['open']<=support:return j,b['open'],'SECOND_STOP_GAP_OPEN'
            if b['low']<=support:return j,support,'SECOND_STOP_INTRADAY_ASSUMED'
    else:
        for j in range(entry_idx,len(bars)-1):
            if bars[j]['close']<support:return j+1,bars[j+1]['open'],'SECOND_STOP_CLOSE_NEXT_OPEN'
    return len(bars)-1,bars[-1]['close'],'SECOND_LEG_D30_CLOSE'


def simulate_pair(r,bars,p):
    k=eid(r);entry=bars[0]['open'];support=float(r['support']) if r['support'] else None
    if r['status'] not in ('VALID','WIDE_RISK_GT10PCT') or r['stop_net_pct']=='':
        return dict(**r,calc_status='LINE_UNUSABLE',baseline_confirmed=0,initial_stop_confirmed=0)
    if support is None or support<=0 or support>=entry:raise ValueError('N_SUPPORT_INVALID_WITH_ELIGIBLE')
    mode=r['exit_type']; cost=p['round_trip_cost_bps_per_leg']
    i,price,decision,reason=first_stop(bars,support,mode)
    oldstop=closepct(price,entry,cost);oldhold=closepct(bars[-1]['close'],entry,cost)
    if abs(oldhold-float(r['hold_net_pct']))>0.00001:raise ValueError('N_HOLD_RECONCILIATION_FAIL:'+str(k))
    if abs(oldstop-float(r['stop_net_pct']))>0.00001:raise ValueError('N_STOP_RECONCILIATION_FAIL:'+str(k)+':'+mode)
    if reason!=r['exit_reason'] or bars[i]['trade_date']!=r['exit_date']:raise ValueError('N_EXIT_TIME_OR_REASON_DRIFT:'+str(k)+':'+mode)
    common={**r,'calc_status':'VALID','baseline_confirmed':1,'initial_stop_confirmed':1,
            'recomputed_exit_price':round(price,5),'original_exit_day_i':i,'original_stop_triggered':int(decision is not None),
            'stop_net_reproduced_pct':round(oldstop,7),'hold_net_reproduced_pct':round(oldhold,7)}
    if decision is None:
        return {**common,'reclaim_signal_date':'','retest_signal_date':'','reentry_time':'','reentry_price':'',
                'reclaim_return_net_pct':round(oldstop,7),'retest_return_net_pct':round(oldstop,7),
                'reclaim_status':'NO_INITIAL_STOP','retest_status':'NO_INITIAL_STOP',
                'reclaim_reentry':0,'retest_reentry':0,'reclaim_second_stop':0,'retest_second_stop':0,
                'reclaim_second_exit_date':'','retest_second_exit_date':'',
                'reclaim_second_exit_reason':'','retest_second_exit_reason':'',
                'reclaim_delta_pp_vs_hold':round(oldstop-oldhold,7),'retest_delta_pp_vs_hold':round(oldstop-oldhold,7),
                'reclaim_delta_pp_vs_stop':0.0,'retest_delta_pp_vs_stop':0.0}
    data={}
    for style,prefix in [('reclaim_next_open','reclaim'),('retest_next_open','retest')]:
        sig=reentry_signal(bars,support,i,style,p)
        ret=oldstop;second=False;exit_date='';exit_reason=''
        if sig['entry_i'] is not None:
            ent=sig['entry_i'];ji,sale,exit_reason=secondary_exit(bars,support,ent,mode)
            # Two sequential fully invested trades. Flat between legs; separate
            # 20bp round-trip cost on EACH leg. No hindsight selection.
            leg2=closepct(sale,bars[ent]['open'],cost)
            ret=((1+oldstop/100)*(1+leg2/100)-1)*100
            second=exit_reason.startswith('SECOND_STOP')
            exit_date=bars[ji]['trade_date']
        data.update({f'{prefix}_status':sig['status'],f'{prefix}_reentry':int(sig['entry_i'] is not None),
             f'{prefix}_second_stop':int(second),f'{prefix}_second_exit_date':exit_date,
             f'{prefix}_second_exit_reason':exit_reason,
             f'{prefix}_return_net_pct':round(ret,7),
             f'{prefix}_delta_pp_vs_hold':round(ret-oldhold,7),
             f'{prefix}_delta_pp_vs_stop':round(ret-oldstop,7),
             f'{prefix}_signal_date':bars[sig['signal_i']]['trade_date'] if sig['signal_i'] is not None else '',
             f'{prefix}_reentry_date':bars[sig['entry_i']]['trade_date'] if sig['entry_i'] is not None else '',
             f'{prefix}_reentry_price':bars[sig['entry_i']]['open'] if sig['entry_i'] is not None else '',
             f'{prefix}_reclaim_close_date':bars[sig['reclaim_i']]['trade_date'] if sig['reclaim_i'] is not None else ''})
    return {**common,**data}


def audit(nzip,lzip,protocol,outdir):
    p=json.loads(Path(protocol).read_text(encoding='utf-8'))
    if not p['research_only'] or p['strategy_ready'] or p['live_changed'] or p['orders_placed'] or p['prospective_OOS']:raise ValueError('PROTOCOL_NOT_READONLY')
    if p['retest_max_observation_sessions']!=5 or p['retest_upper_tolerance_pct']!=2 or p['max_reentries_per_signal']!=1 or p['round_trip_cost_bps_per_leg']!=20:raise ValueError('PREDECLARED_RULE_DRIFT')
    nrows,levels,bars,nm,lm=assert_source_integrity(nzip,lzip,p)
    out=Path(outdir);out.mkdir(parents=True,exist_ok=True)
    rows=[]
    for r in nrows:
        if eid(r) not in bars:
            # preserve one fully censored signal from L; it has no N paired rows
            raise ValueError('N_CASE_WITHOUT_L_PRICE')
        rows.append(simulate_pair(r,bars[eid(r)],p))
    if len(rows)!=590:raise ValueError('PAIR_ROW_DRIFT')
    fields=list(dict.fromkeys(k for r in rows for k in r))
    savecsv(out/RESULT,rows,fields)
    def asf(r,k):return float(r[k])
    groups=collections.defaultdict(list)
    for r in rows:groups[(r['strategy'],r['method'],r['exit_type'])].append(r)
    summary=[];yr=[];cases=[]
    for (strategy,method,mode),group in sorted(groups.items()):
        good=[r for r in group if r['calc_status']=='VALID']
        if not good:continue
        trigger=[r for r in good if r['original_stop_triggered']==1]
        row={'strategy':strategy,'method':method,'exit_type':mode,'n_all':len(group),'n_valid':len(good),
             'n_unusable':len(group)-len(good),'initial_stops':len(trigger),
             'mean_hold_net_pct':round(statistics.mean(asf(x,'hold_net_pct') for x in good),4),
             'mean_stop_net_pct':round(statistics.mean(asf(x,'stop_net_pct') for x in good),4)}
        for x in ('reclaim','retest'):
            row.update({f'{x}_reentries':sum(r[f'{x}_reentry'] for r in good),
                        f'{x}_second_stops':sum(r[f'{x}_second_stop'] for r in good),
                        f'{x}_mean_return_net_pct':round(statistics.mean(asf(r,f'{x}_return_net_pct') for r in good),4),
                        f'{x}_mean_delta_vs_hold_pp':round(statistics.mean(asf(r,f'{x}_delta_pp_vs_hold') for r in good),4),
                        f'{x}_mean_delta_vs_stop_pp':round(statistics.mean(asf(r,f'{x}_delta_pp_vs_stop') for r in good),4),
                        f'{x}_avoided_loss10_count':sum(asf(r,'hold_net_pct')<=-10 and asf(r,f'{x}_return_net_pct')>-10 for r in good),
                        f'{x}_missed_up10_count':sum(asf(r,'hold_net_pct')>=10 and asf(r,f'{x}_return_net_pct')<10 for r in good)})
        summary.append(row)
        for year in sorted(set(r['date'][:4] for r in good)):
            sub=[r for r in good if r['date'][:4]==year]
            yr.append({'year':year,'strategy':strategy,'method':method,'exit_type':mode,'n':len(sub),
                       'reclaim_reentries':sum(r['reclaim_reentry'] for r in sub),
                       'retest_reentries':sum(r['retest_reentry'] for r in sub),
                       'reclaim_mean_delta_vs_hold_pp':round(statistics.mean(asf(r,'reclaim_delta_pp_vs_hold') for r in sub),4),
                       'retest_mean_delta_vs_hold_pp':round(statistics.mean(asf(r,'retest_delta_pp_vs_hold') for r in sub),4)})
    for r in rows:
        if r['calc_status']!='VALID' or r['original_stop_triggered']!=1:continue
        for x in ('reclaim','retest'):
            q={**{z:r[z] for z in ('strategy','date','code','method','exit_type','exit_date','hold_net_pct','stop_net_pct')},
                'reentry_style':x,'reentry_status':r[f'{x}_status'],
                'reentry_date':r.get(f'{x}_reentry_date',''),'reentry_result_net_pct':r[f'{x}_return_net_pct'],
                'delta_vs_stop_pp':r[f'{x}_delta_pp_vs_stop'],'delta_vs_hold_pp':r[f'{x}_delta_pp_vs_hold'],
                'second_stop':r[f'{x}_second_stop'],
                'case':('REGAINED_10PCT_UPSIDE' if float(r['hold_net_pct'])>=10 and float(r[f'{x}_return_net_pct'])>=10 else
                        'AVOIDED_10PCT_LOSS' if float(r['hold_net_pct'])<=-10 and float(r[f'{x}_return_net_pct'])>-10 else
                        'SECOND_STOP' if r[f'{x}_second_stop'] else
                        'RECOVERED_UPSIDE' if float(r[f'{x}_delta_pp_vs_stop'])>0 else
                        'REENTRY_WORSE' if float(r[f'{x}_delta_pp_vs_stop'])<0 else 'NO_REENTRY')}
            cases.append(q)
    savecsv(out/SUMMARY,summary,list(summary[0]));savecsv(out/YEAR,yr,list(yr[0]));savecsv(out/CASES,cases,list(cases[0]));
    m={'version':'R1D26O_LOCKED_N_SUPPORT_RECLAIM_REENTRY_RETROSPECTIVE_V1',
       'research_only':True,'strategy_ready':False,'prospective_OOS':False,'live_changed':False,'orders_placed':0,
       'n_frozen_selection':60,'n_post_complete':len(bars),'n_N_paired_input':len(nrows),
       'n_N_eligible':sum(r['calc_status']=='VALID' for r in rows),'n_N_unusable':sum(r['calc_status']!='VALID' for r in rows),
       'n_initial_stops':sum(r.get('original_stop_triggered',0)==1 for r in rows),
       'reclaim_reentries':sum(r.get('reclaim_reentry',0) for r in rows),
       'retest_reentries':sum(r.get('retest_reentry',0) for r in rows),
       'n_second_stops_reclaim':sum(r.get('reclaim_second_stop',0) for r in rows),
       'n_second_stops_retest':sum(r.get('retest_second_stop',0) for r in rows),
       'source_n_zip_sha256':p['parent_n_zip_sha256'],'source_l_zip_sha256':p['parent_l_zip_sha256'],
       'selection_csv_sha256':p['selection_csv_sha256'],
       'N_support_pre_signal_only':True,'N_support_no_recomputation':True,
       'independent_official_crosscheck':'PENDING','historical_price_PIT':'NOT_CERTIFIED',
       'daily_intra_bar_order':'UNKNOWN_ASSUMED_STOP_FILL',
       'reentry_limit':1,'round_trip_cost_bps_per_leg':20,
       'reclaim_after_exit_close_only':True,'reentry_at_next_open_only':True,
       'files_sha256':{q:sha((out/q).read_bytes()) for q in (RESULT,SUMMARY,YEAR,CASES)}}
    (out/MANIFEST).write_text(json.dumps(m,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print('R1D26O_RESEARCH_PASS_NOT_LIVE_NOT_PIT_NOT_OOS',json.dumps({k:m[k] for k in ('n_frozen_selection','n_post_complete','n_N_eligible','n_initial_stops','reclaim_reentries','retest_reentries','n_second_stops_reclaim','n_second_stops_retest')},ensure_ascii=False))
    return m

if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--n-zip',required=True);parser.add_argument('--l-zip',required=True)
    parser.add_argument('--protocol',default='R1D26O_PREDECLARED_PROTOCOL.json');parser.add_argument('--out',required=True)
    args=parser.parse_args()
    audit(args.n_zip,args.l_zip,args.protocol,args.out)
