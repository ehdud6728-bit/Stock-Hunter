#!/usr/bin/env python3
"""R118 research-only TRUE R117 original-machine break study.

Source signal dates are original R117 daily `event`, not inferred proxy from R118
band/ENV signal dates. All rows carry next-session tradability tests and use
causal 3-candle/MA/Ichimoku. R117 is unchanged. No writes to production paths.

EARLY: R117 FIRST_CLOSE_OUTSIDE_PENDING (post-close t)
CONFIRMED: R117 TWO_CLOSE_BREAK_ARCHIVE (post-close t)
RETEST_PROJECTION: optional first price retest of originally archived line, NOT
  an official R117 role-confirmation (the frozen role events aren't exported).
"""
from __future__ import annotations
import argparse,hashlib,json,math
from pathlib import Path
import pandas as pd
import numpy as np
from r118_causal_indicators import feat, exit_path, COST

STUDY_START='2023-01-01'; STUDY_END='2024-12-31'
ALGORITHM='TS_BODY_WICK_PROXY'
R117_EVENTS={'FIRST_CLOSE_OUTSIDE_PENDING':'EARLY_FIRST_CLOSE',
              'TWO_CLOSE_BREAK_ARCHIVE':'CONFIRMED_TWO_CLOSE'}
SCALES={'SHORT','MEDIUM','LONG'}
N_EXIT=10

def sha256(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()

def validate_subset_manifest(manifest):
    from research_r1d118 import verified_files
    v=verified_files(manifest)
    if len(v)!=100 or len({x[0] for x in v})!=100:raise ValueError('TRUEBREAK_EXPECT_100_STOCK_SHARD')
    return v

def get_market_calendar(market_path):
    m=pd.read_csv(market_path,dtype={'market':str,'date':str}).sort_values(['market','date'])
    if m.duplicated(['market','date']).any():raise ValueError('DUPLICATE_INDEX_DATE')
    x={}
    for market,g in m.groupby('market'):
        d=g.date.astype(str).tolist();cl=g.close.astype(float).to_numpy()
        return20={day:100*(cl[i]/cl[i-20]-1) if i>=20 else np.nan for i,day in enumerate(d)}
        x[market]={'dates':d,'offset':{v:i for i,v in enumerate(d)},'ret20':return20}
    return x

def validate_next_market10(calendar,market,date,stockdates,days=N_EXIT):
    if market not in calendar:return False
    c=calendar[market];pos=c['offset'].get(date)
    if pos is None or pos+days>=len(c['dates']):return False
    return c['dates'][pos+1:pos+days+1] == list(stockdates)

def event_candidates(states):
    """Returns validated original R117 state-machine signals. Confirmed needs
    exact source identity and yesterday's FIRST_CLOSE_OUTSIDE_PENDING event.
    """
    keep={'code','algorithm','scale','kind','idx','date','event','line_id',
          'previous_line_id','exact_level','known_idx','known_date','close','status'}
    if not keep.issubset(states):raise ValueError('MISSING_R117_DAILY_COLUMNS '+str(sorted(keep-set(states))))
    if states.duplicated(['code','algorithm','scale','kind','idx']).any():raise ValueError('DUPLICATE_R117_LANE')
    d=states[(states.algorithm==ALGORITHM)&(states.kind=='UPPER')&(states.scale.isin(SCALES))].copy()
    d=d.sort_values(['code','scale','idx'],kind='stable')
    results=[]
    for (code,scale),g in d.groupby(['code','scale'],sort=False):
        g=g.reset_index(drop=True)
        for j,row in g.iterrows():
            if row.event not in R117_EVENTS or not STUDY_START<=row.date<=STUDY_END:continue
            typ=R117_EVENTS[row.event]; t=int(row.idx)
            if j<1 or int(g.at[j-1,'idx'])!=t-1:raise ValueError('MISSING_PREVIOUS_R117_STATE '+str(code))
            prev=g.iloc[j-1]
            if typ=='EARLY_FIRST_CLOSE':
                line=str(row.line_id) if pd.notna(row.line_id) else ''
                if not line or not np.isfinite(row.exact_level) or float(row.close)<=float(row.exact_level)*1.004:
                    raise ValueError('FIRST_BREAK_SOURCE_MISMATCH '+str(code)+' '+row.date)
                if pd.isna(row.known_idx) or int(row.known_idx)>=t or str(row.known_date)>row.date:
                    raise ValueError('PIT_LINE_KNOWN_AFTER_SIGNAL')
                level=float(row.exact_level)
                if str(prev.line_id)==line and pd.notna(prev.exact_level):
                    priorlevel=float(prev.exact_level)
                    slope=level-priorlevel
                else:slope=np.nan
            else:
                # Confirmed day retires the line.  Current active line_id is
                # blank; original previous_line_id is the identity witness.
                line=str(row.previous_line_id) if pd.notna(row.previous_line_id) else ''
                pline=str(prev.line_id) if pd.notna(prev.line_id) else ''
                if (not line or line!=pline or prev.event!='FIRST_CLOSE_OUTSIDE_PENDING'):
                    raise ValueError('CONFIRMED_NOT_LINKED_TO_PREV_PENDING '+str(code)+' '+row.date)
                if not np.isfinite(prev.exact_level):raise ValueError('CONFIRMED_PREV_LEVEL_MISSING')
                if j>=2 and str(g.at[j-2,'line_id'])==line and np.isfinite(g.at[j-2,'exact_level']):
                    slope=float(prev.exact_level)-float(g.at[j-2,'exact_level'])
                    level=float(prev.exact_level)+slope
                else:slope=np.nan;level=np.nan
            results.append({'code':str(code),'scale':scale,'signal_kind':typ,
                            'line_id':line,'signal_idx':t,'signal_date':str(row.date),
                            'original_event':str(row.event),'r117_source_close':float(row.close),'line_level_at_signal':level,
                            'line_projection_slope':slope,'r117_signal_verified':True,
                            'original_line_state':'R117_DAILY_SIX_LANES_CAUSAL'})
    return results

def annotate_signals(signals,verified_prices,market_path):
    cal=get_market_calendar(market_path)
    selected={}
    for code,path,sh in verified_prices:
        raw=pd.read_csv(path,dtype={'code':str,'date':str},encoding='utf-8-sig').sort_values('date').reset_index(drop=True)
        if raw.code.nunique()!=1 or raw.code.iloc[0]!=code:raise ValueError('PRICE_CODE_MISMATCH')
        selected[code]=feat(raw)
    out=[];unmatured=[]
    by_code={}
    for e in signals:by_code.setdefault(e['code'],[]).append(e)
    market_map={}
    for code,prices in selected.items():
        # Market membership is derived from the daily annual source audit or
        # R118 sector PIT. Passed as an explicit asof market map in run().
        market_map[code]=None
    return selected,cal,by_code

def calculate_for_signal(s,signal,calendar,market,*,check_market=True):
    date=signal['signal_date'];code=signal['code'];t=signal['signal_idx']
    if t>=len(s) or t<3 or str(s.at[t,'date'])!=date:
        raise ValueError('R117_PRICE_IDX_DATE_MISMATCH '+str(code)+' '+date)
    if abs(float(s.at[t,'close'])-float(signal['r117_source_close']))>0.01 and signal['signal_kind']!='RETEST_PROJECTION_NOT_OFFICIAL_R117_ROLE':
        raise ValueError('R117_PRICE_CLOSE_MISMATCH '+str(code)+' '+date)
    if t+N_EXIT>=len(s):return None,'HORIZON_NOT_COMPLETE'
    if check_market and not validate_next_market10(calendar,market,date,s.date.iloc[t+1:t+11].tolist()):
        return None,'MARKET_TRADING_DAY_GAP'
    x=dict(signal)
    x['market']=market;x['market_ret20_pct']=calendar[market]['ret20'].get(date,np.nan)
    x['market_down']=bool(pd.notna(x['market_ret20_pct']) and x['market_ret20_pct']<-2)
    x['entry_date']=str(s.at[t+1,'date']);x['entry_open']=float(s.at[t+1,'open'])
    x['d1_net_pct']=100*(s.at[t+1,'close']/s.at[t+1,'open']-1)-COST
    x['d5_net_pct']=100*(s.at[t+5,'close']/s.at[t+1,'open']-1)-COST
    x['d10_net_pct']=100*(s.at[t+10,'close']/s.at[t+1,'open']-1)-COST
    x['high10_net_pct']=100*(s.high.iloc[t+1:t+11].max()/s.at[t+1,'open']-1)-COST
    x['low10_net_pct']=100*(s.low.iloc[t+1:t+11].min()/s.at[t+1,'open']-1)-COST
    x['high10_day']=int(s.high.iloc[t+1:t+11].to_numpy().argmax()+1)
    x['bar3_up']=bool(s.at[t,'3upcloses']);x['bar3_higher_low']=bool(s.at[t,'3higherlows'])
    x['strong_candle']=bool(s.at[t,'strong_candle']);x['engulf']=bool(s.at[t,'engulf'])
    x['c_gt_c3']=bool(s.at[t,'close']>s.at[t,'c3ago'])
    x['break_3_prev_high']=bool(s.at[t,'prev3_high_break'])
    for name in ['above_ma20','above_ma60','above_ma224','ma20_rising','ma20_gt_ma60',
                 'tenkan_gt_kijun','close_above_kijun','ma7_gt_ma20',
                 'ma7_crossup_ma20','close_crossup_ma20','tk_bull_cross','cloud_bull','cloud_entry_up']:
        x[name]=bool(s.at[t,name])
    x['cloud_position']=s.at[t,'cloud_position']
    x['ma20_slope5_pct']=float(s.at[t,'ma20_slope5_pct']) if pd.notna(s.at[t,'ma20_slope5_pct']) else np.nan
    x['ma20_level']=float(s.at[t,'ma20']) if pd.notna(s.at[t,'ma20']) else np.nan
    x['ma60_level']=float(s.at[t,'ma60']) if pd.notna(s.at[t,'ma60']) else np.nan
    x['cloud_top_level']=float(s.at[t,'cloud_top']) if pd.notna(s.at[t,'cloud_top']) else np.nan
    x['cloud_bottom_level']=float(s.at[t,'cloud_bottom']) if pd.notna(s.at[t,'cloud_bottom']) else np.nan
    x['suspect_big_move'] = bool((s.close.pct_change().abs().iloc[max(1,t-3):t+11]>.30).any() or
                                   (s.open/s.close.shift(1)-1).abs().iloc[max(1,t-3):t+11].gt(.30).any())
    x['suspect_zero_volume']=bool(s.volume.iloc[t:t+11].eq(0).any()) if 'volume' in s else False
    for tp,sl in [(2,3),(3,3),(3,5),(5,3),(5,5)]:
        res=exit_path(s,t,tp,sl,max_hold=10)
        name=f'TP{tp}SL{sl}'
        x[name+'_net_pct']=res['exit_net_pct']
        x[name+'_reason']=res['exit_reason']
        x[name+'_day']=res['exit_day']
        x[name+'_ambiguous']=res['intraday_ambiguous']
    return x,None

def select_retests(breaks,selected,calendar,market_by_day):
    # This is an independent proxy of retesting the original archived line,
    # distinct from the native R117 role-watch state machine (not materialized).
    conf=[x for x in breaks if x['signal_kind']=='CONFIRMED_TWO_CLOSE' and pd.notna(x['line_projection_slope'])]
    returns=[];rejected=0
    for e in conf:
        code=e['code'];s=selected[code];t=int(e['signal_idx']);base=float(e['line_level_at_signal']);slope=e['line_projection_slope']
        for k in range(1,13):
            j=t+k
            if j+N_EXIT>=len(s):break
            lv=base+slope*k
            if not np.isfinite(lv) or lv<=0:break
            tol=0.004*float(s.at[j,'close'])
            if (s.at[j,'low']<=lv+tol and s.at[j,'high']>=lv-tol and s.at[j,'close']>=lv):
                sig=dict(e)
                sig.update(signal_date=str(s.at[j,'date']),signal_idx=int(j),
                           signal_kind='RETEST_PROJECTION_NOT_OFFICIAL_R117_ROLE',
                           original_event='PRICE_RETEST_AFTER_R117_CONFIRMED',
                           line_level_at_signal=lv,r117_signal_verified=False,
                           original_line_state='R117_ARCHIVED_LINE_GEOMETRY_PROJECTION')
                v,reason=calculate_for_signal(s,sig,calendar,market_by_day[(code,sig['signal_date'])])
                if v is not None:returns.append(v)
                else:rejected+=1
                break
    return returns,rejected

def run_shard(manifest,line_states,sector_pit,market,out):
    files=validate_subset_manifest(manifest)
    selected={}
    for code,path,_ in files:
        s=pd.read_csv(path,encoding='utf-8-sig',dtype={'code':str,'date':str}).sort_values('date').reset_index(drop=True)
        selected[code]=feat(s)
    member=pd.read_csv(sector_pit,usecols=['code','date','market'],dtype={'code':str,'date':str,'market':str})
    if member.duplicated(['code','date']).any():raise ValueError('MARKET_CODE_DATE_DUPLICATE')
    market_map={(r.code,r.date):r.market for r in member.itertuples(index=False)}
    if not set(selected).issubset(set(x[0] for x in market_map)):raise ValueError('MISSING_MARKET_IDENTITY')
    fields=['code','algorithm','scale','kind','idx','date','event','line_id',
       'previous_line_id','exact_level','known_idx','known_date','close','status']
    daily=pd.read_csv(line_states,usecols=fields,dtype={'code':str,'date':str,'line_id':str,'previous_line_id':str,'known_date':str},low_memory=False)
    state_codes=set(daily.code.unique())
    if state_codes!=set(selected):raise ValueError('R117_STOCK_COVERAGE_FAIL')
    n_expected=sum(len(s)*6 for s in selected.values())
    if len(daily)!=n_expected:raise ValueError('R117_DAILY_6_LANE_COVERAGE_FAIL')
    cal=get_market_calendar(market)
    signals=event_candidates(daily)
    records=[]; reasons={}
    for sg in signals:
        code=sg['code'];mk=market_map.get((code,sg['signal_date']))
        if mk is None:raise ValueError('SIGNAL_DATE_MARKET_MISSING')
        if mk not in cal:raise ValueError('MARKET_NOT_FOUND '+mk)
        p,why=calculate_for_signal(selected[code],sg,cal,mk)
        if why:reasons[why]=reasons.get(why,0)+1
        else:records.append(p)
    retests,ret_skips=select_retests(records,selected,cal,market_map)
    records+=retests
    out=Path(out);out.parent.mkdir(parents=True,exist_ok=True)
    result=pd.DataFrame(records)
    if result.empty:raise ValueError('NO_TRUE_BREAK_EVENTS_MATURED_FAIL_CLOSED')
    if result.duplicated(['code','scale','signal_kind','signal_date']).any():
        raise ValueError('DUPLICATE_SAME_DAY_R117_EVENT')
    result=result.sort_values(['code','signal_date','scale','signal_kind'])
    result.to_csv(out,index=False,encoding='utf-8-sig')
    audit={'source':'ORIGINAL_R117_6_DAILY_LANES','signals_source_count':len(signals),
           'output_events':len(records),'ret_skips':ret_skips,
           'incomplete_or_market_gapped':reasons,'stocks_verified':len(selected),
           'source_daily_rows':len(daily),'source_files_sha256':sha256(line_states),
           'research_only':True,'live_unmodified':True}
    out.with_suffix('.audit.json').write_text(json.dumps(audit,indent=2,ensure_ascii=False))
    return audit

def merge_shards(roots,out):
    roots=[Path(x) for x in roots];out=Path(out);out.mkdir(parents=True,exist_ok=True)
    all_data=[];files=[]
    for i,root in enumerate(roots):
        path=root/'R118_TRUEBREAK_3CANDLE_SHARD.csv'
        if not path.is_file():raise ValueError('TRUEBREAK_SHARD_MISSING_'+str(i))
        all_data.append(pd.read_csv(path,dtype={'code':str,'signal_date':str,'line_id':str}))
        files.append({'shard':i,'sha256':sha256(path),'rows':len(all_data[-1])})
    df=pd.concat(all_data,ignore_index=True)
    if df.duplicated(['code','signal_date','scale','signal_kind']).any():raise ValueError('TRUEBREAK_DUPLICATE_ACROSS_SHARDS')
    if df.code.nunique()<100:raise ValueError('TRUEBREAK_TOO_FEW_STOCKS_WITH_SIGNALS_FOR_TRUST')
    raw=out/'R118_TRUEBREAK_500_EVENTS.csv';df.to_csv(raw,index=False,encoding='utf-8-sig')
    metrics=[]
    def ag(g):
        return {'n':len(g),'stocks':g.code.nunique(),'signal_dates':g.signal_date.nunique(),
             'mean_high10':g.high10_net_pct.mean(),'median_high10':g.high10_net_pct.median(),
             'tp3_high_hit_pct':100*(g.high10_net_pct>=3).mean(),
             'tp5_high_hit_pct':100*(g.high10_net_pct>=5).mean(),
             'd1_mean':g.d1_net_pct.mean(),'d5_mean':g.d5_net_pct.mean(),
             'd10_mean':g.d10_net_pct.mean(),
             'TP3SL5_mean':g.TP3SL5_net_pct.mean(),
             'TP3SL5_win_pct':100*(g.TP3SL5_net_pct>0).mean(),
             'TP3SL5_date_equal':g.groupby('signal_date').TP3SL5_net_pct.mean().mean(),
             'TP3SL5_same_day_ambiguous':int(g.TP3SL5_ambiguous.sum())}
    for stage,grp in df.groupby('signal_kind'):
        for basis,g in [('ALL',grp),('MARKET_DOWN',grp[grp.market_down]),('CLOSE_GT_3AGO',grp[grp.c_gt_c3]),
                        ('3UP_CANDLES',grp[grp.bar3_up]),('STRONG_CANDLE',grp[grp.strong_candle]),
                        ('MA20_UP',grp[grp.ma20_rising]),('CLOUD_ABOVE',grp[grp.cloud_position=='ABOVE']),
                        ('MARKET_DOWN_AND_CLOSE_GT_3AGO',grp[grp.market_down&grp.c_gt_c3])]:
            if len(g):metrics.append({'stage':stage,'condition':basis,**ag(g)})
        for scale,s in grp.groupby('scale'):
            for cond,g in [('ALL',s),('GT3',s[s.c_gt_c3])]:
                if len(g):metrics.append({'stage':stage,'condition':scale+'_'+cond,**ag(g)})
    summary=pd.DataFrame(metrics);summary.to_csv(out/'R118_TRUEBREAK_500_SUMMARY.csv',index=False,encoding='utf-8-sig')
    report={'source_shards':files,'event_rows':len(df),'true_r117_early':int(df.signal_kind.eq('EARLY_FIRST_CLOSE').sum()),
            'true_r117_confirmed':int(df.signal_kind.eq('CONFIRMED_TWO_CLOSE').sum()),
            'projected_retests_not_R117_confirmed':int(df.signal_kind.str.startswith('RETEST_PROJECTION').sum()),
            'source_semantics':'FIRST pending; TWO confirmed. Each entry next session open, cloud at D uses D-26, stop-first ambiguity.',
            'no_future_data_for_entry_filters':True,'2023_2024_exploratory_not_oos':True,
            'limitations':['Corporate actions not independently certified','Same-day stop+target unknown order, conservative stop first',
                           'Projected retest is not original R117 role event','No liquidity/capital constraints or portfolio overlapping holdings',
                           '500 stocks static at 2022 cutoff','Same event can occur on three line scales; de-duplicate for portfolio use']}
    (out/'R118_TRUEBREAK_500_MERGE_AUDIT.json').write_text(json.dumps(report,indent=2,ensure_ascii=False))
    return report


def main():
    a=argparse.ArgumentParser()
    sub=a.add_subparsers(dest='mode',required=True)
    sh=sub.add_parser('shard');sh.add_argument('--manifest',required=True);sh.add_argument('--lines',required=True)
    sh.add_argument('--sector',required=True);sh.add_argument('--market',required=True);sh.add_argument('--out',required=True)
    me=sub.add_parser('merge');me.add_argument('--shards',nargs=5,required=True);me.add_argument('--out',required=True)
    q=a.parse_args()
    if q.mode=='shard':r=run_shard(q.manifest,q.lines,q.sector,q.market,q.out)
    else:r=merge_shards(q.shards,q.out)
    print(json.dumps(r,ensure_ascii=False,indent=2))

if __name__=='__main__':main()
