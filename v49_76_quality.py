from __future__ import annotations

import json
import math
import os
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

STRATEGIES = ('L','LP','SLOCK','S','G','IT','H','I','A','B1','B2','C')

# Independent machine-readable proxies for the human intent described in search_spec_v7.
# These are an audit lens, not replacements for the authoritative predicates.
INTENT_PROXY_SPECS: dict[str,list[dict[str,Any]]] = {
    'L': [
        {'feature':'entry_gap_pct','kind':'between','lo':1.0,'hi':18.0,'label':'리더 갭 범위'},
        {'feature':'entry_vol50_ratio','kind':'ge','value':1.20,'label':'50일 거래량 확대'},
        {'feature':'entry_close_loc_pct','kind':'ge','value':60.0,'label':'종가 상단'},
        {'feature':'entry_high20_dist_pct','kind':'ge','value':-8.0,'label':'고점/박스 근접'},
        {'feature':'entry_amount_b','kind':'ge','value':100.0,'label':'거래대금'},
    ],
    'LP': [
        {'feature':'entry_close_loc_pct','kind':'ge','value':45.0,'label':'눌림 후 종가 품질'},
        {'feature':'entry_upper_wick_pct','kind':'le','value':45.0,'label':'과도한 윗꼬리 제외'},
        {'feature':'entry_ma20_dist_pct','kind':'between','lo':-10.0,'hi':22.0,'label':'기준선 훼손/과열 제한'},
        {'feature':'entry_high20_dist_pct','kind':'ge','value':-18.0,'label':'선행 리더갭 구조 유지'},
        {'feature':'entry_amount_b','kind':'ge','value':60.0,'label':'유동성'},
    ],
    'SLOCK': [
        {'feature':'entry_close_loc_pct','kind':'ge','value':72.0,'label':'상단 잠김'},
        {'feature':'entry_upper_wick_pct','kind':'le','value':25.0,'label':'윗꼬리 제한'},
        {'feature':'entry_vol20_ratio','kind':'ge','value':1.20,'label':'거래량 재유입'},
        {'feature':'entry_high20_dist_pct','kind':'ge','value':-6.0,'label':'고점권'},
        {'feature':'entry_amount_b','kind':'ge','value':50.0,'label':'거래대금'},
    ],
    'S': [
        {'feature':'entry_close_loc_pct','kind':'ge','value':62.0,'label':'종가 강도'},
        {'feature':'entry_upper_wick_pct','kind':'le','value':38.0,'label':'윗꼬리 제한'},
        {'feature':'entry_vol20_ratio','kind':'ge','value':1.10,'label':'고점권 재유입'},
        {'feature':'entry_high20_dist_pct','kind':'ge','value':-10.0,'label':'박스/전고점 위치'},
        {'feature':'entry_amount_b','kind':'ge','value':40.0,'label':'유동성'},
    ],
    'G': [
        {'feature':'entry_gap_pct','kind':'between','lo':2.0,'hi':12.0,'label':'2~12% 모랄레스 갭'},
        {'feature':'entry_vol50_ratio','kind':'ge','value':1.50,'label':'Vol50 1.5배'},
        {'feature':'entry_close_loc_pct','kind':'ge','value':50.0,'label':'캔들 중간 이상 마감'},
        {'feature':'entry_upper_wick_pct','kind':'le','value':45.0,'label':'클라이맥스 윗꼬리 제한'},
        {'feature':'entry_ma20_dist_pct','kind':'le','value':28.0,'label':'20일 이격 과열 제한'},
    ],
    'IT': [
        {'feature':'entry_close_loc_pct','kind':'ge','value':55.0,'label':'촉발 종가 위치'},
        {'feature':'entry_ma60_dist_pct','kind':'between','lo':-10.0,'hi':30.0,'label':'장기선 이격'},
        {'feature':'entry_vol20_ratio','kind':'ge','value':1.0,'label':'재료 거래량'},
        {'feature':'entry_amount_b','kind':'ge','value':40.0,'label':'최소 거래대금'},
    ],
    'I': [
        {'feature':'entry_close_loc_pct','kind':'ge','value':50.0,'label':'종가 위치'},
        {'feature':'entry_ma60_dist_pct','kind':'between','lo':-10.0,'hi':30.0,'label':'장기선 시세분출 구간'},
        {'feature':'entry_amount_b','kind':'ge','value':40.0,'label':'유동성'},
        {'feature':'entry_vol20_ratio','kind':'ge','value':0.8,'label':'거래량 기반'},
    ],
    'H': [
        {'feature':'entry_high20_dist_pct','kind':'ge','value':-5.0,'label':'신고가/전고점 회복'},
        {'feature':'entry_close_loc_pct','kind':'ge','value':50.0,'label':'종가 품질'},
        {'feature':'entry_upper_wick_pct','kind':'le','value':45.0,'label':'분배형 윗꼬리 제한'},
        {'feature':'entry_amount_b','kind':'ge','value':40.0,'label':'거래대금'},
    ],
    'A': [
        {'feature':'entry_close_loc_pct','kind':'ge','value':65.0,'label':'강한 돌파 종가'},
        {'feature':'entry_vol20_ratio','kind':'ge','value':1.40,'label':'거래량 돌파'},
        {'feature':'entry_high20_dist_pct','kind':'ge','value':-5.0,'label':'고점 돌파'},
        {'feature':'entry_amount_b','kind':'ge','value':40.0,'label':'거래대금'},
    ],
    'B1': [
        {'feature':'entry_close_loc_pct','kind':'ge','value':52.0,'label':'종가 회복'},
        {'feature':'entry_ma20_dist_pct','kind':'between','lo':-12.0,'hi':25.0,'label':'기준선 구조'},
        {'feature':'entry_vol20_ratio','kind':'ge','value':0.85,'label':'거래량 확인'},
        {'feature':'entry_amount_b','kind':'ge','value':30.0,'label':'유동성'},
    ],
    'B2': [
        {'feature':'entry_close_loc_pct','kind':'ge','value':55.0,'label':'재상승 종가'},
        {'feature':'entry_ma20_dist_pct','kind':'between','lo':-10.0,'hi':25.0,'label':'기준선 구조'},
        {'feature':'entry_vol20_ratio','kind':'ge','value':0.90,'label':'거래량 확인'},
        {'feature':'entry_amount_b','kind':'ge','value':30.0,'label':'유동성'},
    ],
    'C': [
        {'feature':'entry_close_loc_pct','kind':'ge','value':48.0,'label':'눌림 재상승 종가'},
        {'feature':'entry_ma20_dist_pct','kind':'between','lo':-15.0,'hi':22.0,'label':'장기구조 훼손/과열 제한'},
        {'feature':'entry_high20_dist_pct','kind':'ge','value':-20.0,'label':'전고점 구조'},
        {'feature':'entry_amount_b','kind':'ge','value':25.0,'label':'거래대금'},
    ],
}


def _num(df: pd.DataFrame, col: str, default=np.nan) -> pd.Series:
    if col in df.columns:
        return pd.to_numeric(df[col], errors='coerce')
    return pd.Series(default, index=df.index, dtype=float)


def _check(values: pd.Series, rule: dict[str,Any]) -> pd.Series:
    if rule['kind']=='ge': return values.ge(float(rule['value']))
    if rule['kind']=='le': return values.le(float(rule['value']))
    if rule['kind']=='between': return values.ge(float(rule['lo'])) & values.le(float(rule['hi']))
    raise KeyError(rule['kind'])


def attach_intent_proxy(df: pd.DataFrame) -> pd.DataFrame:
    z=df.copy()
    z['intent_proxy_version']='v49.76-machine-intent-proxy-v2-sample-authority-safe'
    z['intent_proxy_scope']='INDEPENDENT_AUDIT_NOT_AUTHORITATIVE_PREDICATE'
    z['intent_checks_total']=0; z['intent_checks_observed']=0; z['intent_checks_passed']=0
    z['intent_match_ratio']=np.nan; z['intent_coverage_ratio']=np.nan
    z['intent_status']='UNASSESSED'; z['intent_failed_checks']=''; z['intent_passed_checks']=''
    mode=z.get('mode',pd.Series('',index=z.index)).astype(str)
    for m,spec in INTENT_PROXY_SPECS.items():
        idx=mode.eq(m)
        if not idx.any(): continue
        total=len(spec)
        observed=pd.Series(0,index=z.index,dtype=int); passed=pd.Series(0,index=z.index,dtype=int)
        fail_text=pd.Series('',index=z.index,dtype=object); pass_text=pd.Series('',index=z.index,dtype=object)
        for rule in spec:
            vals=_num(z,rule['feature'])
            obs=vals.notna() & idx
            ok=_check(vals,rule).fillna(False) & idx
            observed += obs.astype(int); passed += ok.astype(int)
            fail_mask=obs & ~ok
            pass_mask=ok
            fail_text.loc[fail_mask]=fail_text.loc[fail_mask].astype(str).apply(lambda x:(x+' | ' if x else '')+rule['label'])
            pass_text.loc[pass_mask]=pass_text.loc[pass_mask].astype(str).apply(lambda x:(x+' | ' if x else '')+rule['label'])
        coverage=observed/float(total)
        ratio=passed/observed.replace(0,np.nan)
        status=np.select(
            [coverage.lt(.60), ratio.ge(.80), ratio.ge(.60)],
            ['UNASSESSED','INTENT_MATCH','PARTIAL_MATCH'], default='INTENT_MISMATCH')
        z.loc[idx,'intent_checks_total']=total
        z.loc[idx,'intent_checks_observed']=observed.loc[idx]
        z.loc[idx,'intent_checks_passed']=passed.loc[idx]
        z.loc[idx,'intent_coverage_ratio']=coverage.loc[idx]
        z.loc[idx,'intent_match_ratio']=ratio.loc[idx]
        z.loc[idx,'intent_status']=pd.Series(status,index=z.index).loc[idx]
        z.loc[idx,'intent_failed_checks']=fail_text.loc[idx]
        z.loc[idx,'intent_passed_checks']=pass_text.loc[idx]
    return z


def _cliffs_delta(a: pd.Series,b: pd.Series,max_n:int=400) -> float:
    x=pd.to_numeric(a,errors='coerce').dropna().to_numpy(dtype=float)
    y=pd.to_numeric(b,errors='coerce').dropna().to_numpy(dtype=float)
    if len(x)==0 or len(y)==0: return np.nan
    if len(x)>max_n: x=x[np.linspace(0,len(x)-1,max_n).astype(int)]
    if len(y)>max_n: y=y[np.linspace(0,len(y)-1,max_n).astype(int)]
    return float((np.greater.outer(x,y).sum()-np.less.outer(x,y).sum())/(len(x)*len(y)))


def intent_summary(z:pd.DataFrame)->pd.DataFrame:
    rows=[]
    for m,g in z.groupby(z.get('mode',pd.Series('',index=z.index)).astype(str)):
        for status,sg in g.groupby('intent_status'):
            ret=_num(sg,'_net50')
            rows.append({'strategy':m,'intent_status':status,'n':len(sg),'share_pct':len(sg)/len(g)*100.0,
                         'net50_mean_pct':ret.mean(),'net50_median_pct':ret.median(),
                         'win50_pct':ret.gt(0).mean()*100.0,
                         'big_pct':sg.get('outcome_class',pd.Series('',index=sg.index)).astype(str).eq('BIG_CAPTURABLE').mean()*100.0,
                         'stop_first_pct':sg.get('outcome_class',pd.Series('',index=sg.index)).astype(str).eq('STOP_FIRST').mean()*100.0,
                         'coverage_mean_pct':_num(sg,'intent_coverage_ratio').mean()*100.0,
                         'match_ratio_mean_pct':_num(sg,'intent_match_ratio').mean()*100.0})
    return pd.DataFrame(rows)


def winner_loser_contrast(z:pd.DataFrame, features:list[str])->pd.DataFrame:
    rows=[]
    if 'sample_scope' in z.columns:
        oos=z[z['sample_scope'].astype(str).eq('OOS')].copy()
    elif 'sample_stage' in z.columns:
        oos=z[~z['sample_stage'].astype(str).eq('TRAIN')].copy()
    else:
        d=pd.to_datetime(z.get('signal_date'),errors='coerce'); split=d.min()+(d.max()-d.min())*.60 if d.notna().any() else pd.NaT
        oos=z[d.ge(split)].copy() if pd.notna(split) else z.iloc[0:0].copy()
    for m in ('ALL',)+STRATEGIES:
        q=oos if m=='ALL' else oos[oos.get('mode',pd.Series('',index=oos.index)).astype(str).eq(m)]
        if q.empty: continue
        win=q[_num(q,'_net50').gt(0)]; loss=q[_num(q,'_net50').le(0)]
        _oc=q.get('outcome_class',pd.Series('',index=q.index)).astype(str)
        big=q[_oc.eq('BIG_CAPTURABLE')]; stop=q[_oc.eq('STOP_FIRST')]
        for feat in features:
            a=_num(win,feat); b=_num(loss,feat)
            rows.append({'strategy':m,'contrast':'WIN50_vs_LOSS50','feature':feat,'winner_n':a.notna().sum(),'loser_n':b.notna().sum(),
                         'winner_median':a.median(),'loser_median':b.median(),'median_delta':a.median()-b.median(),
                         'cliffs_delta':_cliffs_delta(a,b)})
            a=_num(big,feat); b=_num(stop,feat)
            rows.append({'strategy':m,'contrast':'BIG_vs_STOP_FIRST','feature':feat,'winner_n':a.notna().sum(),'loser_n':b.notna().sum(),
                         'winner_median':a.median(),'loser_median':b.median(),'median_delta':a.median()-b.median(),
                         'cliffs_delta':_cliffs_delta(a,b)})
    return pd.DataFrame(rows)


def casebook(z:pd.DataFrame,per_strategy:int=12)->pd.DataFrame:
    rows=[]
    cols=['signal_date','code','name','mode','score','grade','intent_status','intent_match_ratio','intent_failed_checks',
          'outcome_class','_net50','rule35_pnl','path_max_high_ret','path_min_low_ret','path_first_plus3_day','path_first_stop_day',
          'entry_stock_ret_1d','entry_stock_ret_5d','entry_stock_ret_20d','entry_close_loc_pct','entry_upper_wick_pct','entry_gap_pct',
          'entry_vol20_ratio','entry_vol50_ratio','entry_amount_b','entry_amount20_ratio','entry_ma20_dist_pct','entry_ma60_dist_pct',
          'entry_high20_dist_pct','market_m5_t1','market_ret_5d_t1','stock_excess_5d','sector_label','event_theme_bucket']
    for m,g in z.groupby(z.get('mode',pd.Series('',index=z.index)).astype(str)):
        q=g.copy(); q['_ret']=_num(q,'_net50'); q['_mfe']=_num(q,'path_max_high_ret'); q['_mae']=_num(q,'path_min_low_ret')
        groups={
            'TOP_WINNER':q.sort_values(['_ret','_mfe'],ascending=False).head(per_strategy),
            'TOP_LOSER':q.sort_values(['_ret','_mae'],ascending=True).head(per_strategy),
            'STOP_FIRST':q[_num(q,'stop_first_flag',0).fillna(0).gt(0)].sort_values('_ret').head(per_strategy),
            'INTENT_MISMATCH_WIN':q[q.intent_status.eq('INTENT_MISMATCH') & q['_ret'].gt(0)].sort_values('_ret',ascending=False).head(per_strategy),
            'INTENT_MATCH_LOSS':q[q.intent_status.eq('INTENT_MATCH') & q['_ret'].le(0)].sort_values('_ret').head(per_strategy),
        }
        for typ,sg in groups.items():
            if sg.empty: continue
            keep=[c for c in cols if c in sg.columns]
            tmp=sg[keep].copy(); tmp.insert(0,'case_type',typ); rows.append(tmp)
    return pd.concat(rows,ignore_index=True,sort=False) if rows else pd.DataFrame()


def ranking_audit(raw:pd.DataFrame,selected:pd.DataFrame)->tuple[pd.DataFrame,pd.DataFrame]:
    r=raw.copy(); s=selected.copy()
    for df in (r,s):
        df['signal_date']=df.get('signal_date',pd.Series('',index=df.index)).astype(str)
        df['code']=df.get('code',pd.Series('',index=df.index)).astype(str).str.zfill(6)
        df['mode']=df.get('mode',pd.Series('',index=df.index)).astype(str)
    r['_score']=_num(r,'score',0).fillna(0); r['_amount']=_num(r,'amount_b',0).fillna(0); r['_ret']=_num(r,'rule35_pnl')-.50
    r=r.sort_values(['signal_date','mode','_score','_amount','code'],ascending=[True,True,False,False,True])
    r['rank_within_date_mode']=r.groupby(['signal_date','mode']).cumcount()+1
    skeys=set(zip(s['signal_date'],s['code'],s['mode']))
    r['selected_top5']= [int((a,b,c) in skeys) for a,b,c in zip(r['signal_date'],r['code'],r['mode'])]
    r['rank_bucket']=np.select([r.rank_within_date_mode.eq(1),r.rank_within_date_mode.le(5)],['RANK1','RANK2_5'],default='OUTSIDE_TOP5')
    rows=[]
    for (m,b),g in r.groupby(['mode','rank_bucket']):
        rows.append({'strategy':m,'rank_bucket':b,'n':len(g),'net50_mean_pct':g['_ret'].mean(),'net50_median_pct':g['_ret'].median(),
                     'win50_pct':g['_ret'].gt(0).mean()*100.0,'big_pct':_num(g,'path_first_plus10_day',0).fillna(0).gt(0).mean()*100.0})
    summary=pd.DataFrame(rows)
    detail_cols=[c for c in ['signal_date','code','name','mode','rank_within_date_mode','rank_bucket','selected_top5','score','amount_b','vol_ratio','grade','rule35_pnl','path_max_high_ret','path_min_low_ret'] if c in r.columns]
    return summary,r[detail_cols]


def opportunity_audit(opportunities:pd.DataFrame,raw:pd.DataFrame,selected:pd.DataFrame)->tuple[pd.DataFrame,pd.DataFrame,pd.DataFrame,pd.DataFrame,pd.DataFrame]:
    if opportunities.empty:
        return pd.DataFrame(),pd.DataFrame(),pd.DataFrame(),pd.DataFrame(),pd.DataFrame()
    o=opportunities.copy()
    for df in (o,raw,selected):
        df['signal_date']=df.get('signal_date',pd.Series('',index=df.index)).astype(str)
        df['code']=df.get('code',pd.Series('',index=df.index)).astype(str).str.zfill(6)
    raw_keys=set(zip(raw.signal_date,raw.code)); sel_keys=set(zip(selected.signal_date,selected.code))
    o['raw_search_hit']= [int((d,c) in raw_keys) for d,c in zip(o.signal_date,o.code)]
    o['selected_top5_any_strategy']= [int((d,c) in sel_keys) for d,c in zip(o.signal_date,o.code)]
    deep=pd.to_numeric(o.get('deep_audit_performed',0),errors='coerce').fillna(0).astype(int)
    full=o.get('deep_audit_hit_modes',pd.Series('',index=o.index)).fillna('').astype(str).str.strip()
    planned=o.get('planned_modes',pd.Series('',index=o.index)).fillna('').astype(str).str.strip()
    o['miss_stage']=np.select([
        o.selected_top5_any_strategy.eq(1),
        o.raw_search_hit.eq(1),
        deep.eq(1)&full.ne(''),
        deep.eq(1)&full.eq('')&planned.ne(''),
        deep.eq(1)&full.eq('')&planned.eq(''),
        planned.ne('')],
        ['SELECTED_TOP5','RAW_HIT_NOT_TOP5','TECHNICAL_FALSE_NEGATIVE','PREDICATE_REJECTED_CONFIRMED','STRATEGIC_MISS_CONFIRMED','PREDICATE_REJECTED_UNCONFIRMED'],
        default='VECTOR_GATE_REJECTED_UNVERIFIED')

    def _modes(v):
        return {x.strip() for x in str(v or '').split(',') if x.strip()}
    gate_missing=[]; planned_missed=[]; root=[]
    for pm,fm,stage in zip(planned,full,o['miss_stage']):
        ps=_modes(pm); fs=_modes(fm)
        gm=sorted(fs-ps); mm=sorted(fs&ps)
        gate_missing.append(','.join(gm)); planned_missed.append(','.join(mm))
        if stage!='TECHNICAL_FALSE_NEGATIVE': root.append('')
        elif gm and mm: root.append('MIXED_GATE_AND_NORMAL_PATH_MISMATCH')
        elif gm: root.append('VECTOR_GATE_MISS')
        elif mm: root.append('NORMAL_PATH_EXECUTION_MISMATCH')
        else: root.append('UNRESOLVED')
    o['gate_missing_modes']=gate_missing
    o['planned_but_missed_modes']=planned_missed
    o['technical_fn_root_cause']=root

    rows=[]
    for stage,g in o.groupby('miss_stage'):
        rows.append({'miss_stage':stage,'n':len(g),'share_pct':len(g)/len(o)*100.0,
                     'next_close_mean_pct':_num(g,'op_ret_next_close').mean(),
                     'max3_mean_pct':_num(g,'op_ret_max_high_3d').mean(),
                     'max5_mean_pct':_num(g,'op_ret_max_high_5d').mean(),
                     'max10_mean_pct':_num(g,'op_ret_max_high_10d').mean(),
                     'fixed35_mean_pct':_num(g,'op_fixed35_pnl').mean()})
    summary=pd.DataFrame(rows)

    path_rows=[]
    if 'opportunity_path_class' in o.columns:
        for (stage,path_class),g in o.groupby(['miss_stage','opportunity_path_class'],dropna=False):
            path_rows.append({'miss_stage':stage,'opportunity_path_class':path_class,'n':len(g),
                              'share_within_stage_pct':len(g)/max(1,int((o.miss_stage==stage).sum()))*100.0,
                              'next_close_mean_pct':_num(g,'op_ret_next_close').mean(),
                              'max5_mean_pct':_num(g,'op_ret_max_high_5d').mean(),
                              'mae10_mean_pct':_num(g,'op_mae_10d').mean(),
                              'fixed35_mean_pct':_num(g,'op_fixed35_pnl').mean()})
    path_summary=pd.DataFrame(path_rows)

    # Technical false negatives are always retained in full. The manual casebook cap only
    # applies to the remaining strongest missed opportunities.
    score=np.nanmax(np.vstack([_num(o,'op_ret_next_close').fillna(-999),_num(o,'op_ret_max_high_3d').fillna(-999),_num(o,'op_ret_max_high_5d').fillna(-999),_num(o,'op_ret_max_high_10d').fillna(-999)]),axis=0)
    o['_op_strength']=score
    tech=o[o.miss_stage.eq('TECHNICAL_FALSE_NEGATIVE')].sort_values(['signal_date','code']).copy()
    max_cases=int(os.environ.get('CLOSING_BET_V4970_OPPORTUNITY_CASEBOOK_MAX','500') or 500)
    others=o[o.miss_stage.ne('SELECTED_TOP5') & o.miss_stage.ne('TECHNICAL_FALSE_NEGATIVE')].sort_values('_op_strength',ascending=False).head(max_cases)
    cases=pd.concat([tech,others],ignore_index=True,sort=False).drop_duplicates(['signal_date','code'],keep='first')
    forensic_cols=['signal_date','code','name','miss_stage','technical_fn_root_cause','gate_missing_modes','planned_but_missed_modes',
                   'planned_modes','main_hit_modes','deep_audit_performed','deep_audit_hit_modes','deep_audit_error',
                   'opportunity_path_class','op_first_plus3_day','op_first_minus3_day','op_first_minus5_day',
                   'op_ret_next_close','op_ret_close_3d','op_ret_close_5d','op_ret_close_10d',
                   'op_ret_max_high_3d','op_ret_max_high_5d','op_ret_max_high_10d','op_mfe_10d','op_mae_10d','op_pre_plus3_mae',
                   'op_fixed35_pnl','op_fixed35_exit','opportunity_labels','entry_close_loc_pct','entry_upper_wick_pct','entry_gap_pct',
                   'entry_vol20_ratio','entry_vol50_ratio','entry_amount_b','entry_ma20_dist_pct','entry_ma60_dist_pct','entry_high20_dist_pct']
    # v49.76: retain A safe-superset/authority parity columns in both the casebook and
    # the dedicated technical-FN forensic so a future mismatch is diagnosable without the full artifact.
    a_cols=sorted(c for c in o.columns if str(c).startswith('a_'))
    near_cols=[c for c in forensic_cols+a_cols if c in cases.columns]
    tech_cols=[c for c in forensic_cols+a_cols if c in tech.columns]
    return summary,o,cases[near_cols],tech[tech_cols],path_summary

def drift_audit(z:pd.DataFrame,features:list[str])->pd.DataFrame:
    q=z.copy(); q['_date']=pd.to_datetime(q.get('signal_date'),errors='coerce')
    if q['_date'].notna().sum()==0: return pd.DataFrame()
    end=q['_date'].max(); recent_start=end-pd.Timedelta(days=int(os.environ.get('CLOSING_BET_V4970_DRIFT_RECENT_DAYS','28') or 28)); prior_start=recent_start-pd.Timedelta(days=int(os.environ.get('CLOSING_BET_V4970_DRIFT_PRIOR_DAYS','84') or 84))
    rows=[]
    for m in ('ALL',)+STRATEGIES:
        g=q if m=='ALL' else q[q.get('mode',pd.Series('',index=q.index)).astype(str).eq(m)]
        recent=g[g._date.gt(recent_start)]; prior=g[g._date.gt(prior_start)&g._date.le(recent_start)]
        for feat in features+['_net50','stop_first_flag','big_before_stop']:
            a=_num(recent,feat).dropna(); b=_num(prior,feat).dropna()
            if len(a)<5 or len(b)<10: continue
            scale=float(b.std(ddof=0)); shift=(float(a.median())-float(b.median()))/(scale if scale>1e-9 else np.nan)
            status='DRIFT_ALERT' if pd.notna(shift) and abs(shift)>=1.0 else ('WATCH' if pd.notna(shift) and abs(shift)>=0.5 else 'STABLE')
            rows.append({'strategy':m,'feature':feat,'recent_start':recent_start.date(),'end_date':end.date(),'recent_n':len(a),'prior_n':len(b),
                         'recent_median':a.median(),'prior_median':b.median(),'standardized_median_shift':shift,'status':status})
    return pd.DataFrame(rows)


def run_quality_audit(raw:pd.DataFrame,selected_ctx:pd.DataFrame,selected:pd.DataFrame,opportunities:pd.DataFrame,out:Path,feature_meta:dict[str,tuple[str,str]])->dict[str,Any]:
    out=Path(out); out.mkdir(parents=True,exist_ok=True)
    enriched=attach_intent_proxy(selected_ctx)
    intent=intent_summary(enriched)
    features=[f for f in feature_meta if f in enriched.columns]
    contrast=winner_loser_contrast(enriched,features)
    cases=casebook(enriched)
    rank_summary,rank_detail=ranking_audit(raw,selected)
    opp_summary,opp_detail,opp_cases,technical_fn,path_summary=opportunity_audit(opportunities,raw,selected)
    drift=drift_audit(enriched,features)
    enriched.to_csv(out/'v49_76_search_intent_enriched.csv',index=False,encoding='utf-8-sig')
    intent.to_csv(out/'v49_76_search_intent_summary.csv',index=False,encoding='utf-8-sig')
    contrast.to_csv(out/'v49_76_winner_loser_effect_size.csv',index=False,encoding='utf-8-sig')
    cases.to_csv(out/'v49_76_manual_casebook.csv',index=False,encoding='utf-8-sig')
    rank_summary.to_csv(out/'v49_76_ranking_summary.csv',index=False,encoding='utf-8-sig')
    rank_detail.to_csv(out/'v49_76_ranking_detail.csv',index=False,encoding='utf-8-sig')
    opp_summary.to_csv(out/'v49_76_opportunity_miss_summary.csv',index=False,encoding='utf-8-sig')
    opp_detail.to_csv(out/'v49_76_opportunity_census_global.csv',index=False,encoding='utf-8-sig')
    opp_cases.to_csv(out/'v49_76_missed_winner_casebook.csv',index=False,encoding='utf-8-sig')
    technical_fn.to_csv(out/'v49_76_technical_false_negative.csv',index=False,encoding='utf-8-sig')
    path_summary.to_csv(out/'v49_76_opportunity_path_summary.csv',index=False,encoding='utf-8-sig')
    drift.to_csv(out/'v49_76_distribution_drift.csv',index=False,encoding='utf-8-sig')
    forensic={'version':'v49.76','count':int(len(technical_fn)),'rows':technical_fn.to_dict(orient='records') if not technical_fn.empty else [],
              'fail_closed':True,'interpretation':'Any authoritative hit absent from the normal path invalidates search quality.'}
    (out/'v49_76_technical_false_negative.json').write_text(json.dumps(forensic,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
    tech_fn=int(len(technical_fn))
    deep_n=int(pd.to_numeric(opp_detail.get('deep_audit_performed',pd.Series(dtype=float)),errors='coerce').fillna(0).sum()) if not opp_detail.empty else 0
    missed_n=int((opp_detail.get('selected_top5_any_strategy',pd.Series(dtype=float)).fillna(0).astype(int)==0).sum()) if not opp_detail.empty else 0
    intent_assessed=int(enriched.intent_status.ne('UNASSESSED').sum()) if len(enriched) else 0
    root_counts=technical_fn.get('technical_fn_root_cause',pd.Series(dtype=str)).value_counts().to_dict() if not technical_fn.empty else {}
    path_counts=opp_detail.get('opportunity_path_class',pd.Series(dtype=str)).value_counts().to_dict() if not opp_detail.empty else {}
    manifest={'status':'INVALID' if tech_fn>0 else ('PARTIAL-VALID' if opportunities.empty or deep_n==0 else 'FULL-VALID'),
              'intent_proxy_scope':'INDEPENDENT_MACHINE_PROXY_NOT_HUMAN_CHART_LABEL',
              'selected_rows':len(enriched),'intent_assessed_rows':intent_assessed,
              'opportunity_rows':len(opp_detail),'missed_opportunity_rows':missed_n,'deep_audit_rows':deep_n,'technical_false_negative_rows':tech_fn,
              'technical_false_negative_root_causes':root_counts,'opportunity_path_counts':path_counts,
              'a_gate_zero_body_policy':'MAX_ABS_CLOSE_OPEN_1E-9_MATCHES_AUTHORITATIVE_A',
              'ranking_rows':len(rank_detail),'casebook_rows':len(cases),'drift_alerts':int(drift.status.eq('DRIFT_ALERT').sum()) if not drift.empty else 0,
              'auto_apply':0,'paper_only':True,'real_orders':0}
    (out/'v49_76_search_quality_manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
    return {'enriched':enriched,'intent':intent,'contrast':contrast,'cases':cases,'rank_summary':rank_summary,'rank_detail':rank_detail,
            'opp_summary':opp_summary,'opp_detail':opp_detail,'opp_cases':opp_cases,'technical_fn':technical_fn,'path_summary':path_summary,
            'drift':drift,'manifest':manifest}

