from __future__ import annotations

import json
import math
import os
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

STRATEGIES = ('L','LP','SLOCK','S','G','IT','H','I','A','B1','B2','C')
FORBIDDEN_FEATURE_TOKENS = ('path_', 'rule35', 'ret_next', 'ret_close', 'hit3', 'stop_before', 'first_event', 'eval_', 'outcome', 'future', 'forward', 'pnl', '_net')


def _num(df: pd.DataFrame, col: str, default=np.nan) -> pd.Series:
    if col in df.columns:
        return pd.to_numeric(df[col], errors='coerce')
    return pd.Series(default, index=df.index, dtype=float)


def _date(df: pd.DataFrame) -> pd.Series:
    return pd.to_datetime(df.get('signal_date', pd.Series(pd.NaT, index=df.index)), errors='coerce').dt.normalize()


def _safe_float(v: Any, default=np.nan) -> float:
    try:
        x=float(v)
        return x if np.isfinite(x) else default
    except Exception:
        return default


def _cliffs_delta(a: pd.Series, b: pd.Series, max_n: int = 500) -> float:
    x=pd.to_numeric(a,errors='coerce').dropna().to_numpy(dtype=float)
    y=pd.to_numeric(b,errors='coerce').dropna().to_numpy(dtype=float)
    if len(x)==0 or len(y)==0:
        return np.nan
    if len(x)>max_n:
        x=x[np.linspace(0,len(x)-1,max_n).astype(int)]
    if len(y)>max_n:
        y=y[np.linspace(0,len(y)-1,max_n).astype(int)]
    return float((np.greater.outer(x,y).sum()-np.less.outer(x,y).sum())/(len(x)*len(y)))


def _causal_features(feature_meta: dict[str, tuple[str,str]], df: pd.DataFrame) -> list[str]:
    out=[]
    for feat, meta in feature_meta.items():
        causal=str(meta[1] if len(meta)>1 else '')
        lf=feat.lower()
        if feat not in df.columns or causal=='AUDIT_PROXY':
            continue
        if any(tok in lf for tok in FORBIDDEN_FEATURE_TOKENS):
            raise RuntimeError(f'V49_71_CAUSAL_FEATURE_LEAKAGE: {feat}')
        out.append(feat)
    return out


def _oos(df: pd.DataFrame) -> pd.DataFrame:
    if 'sample_scope' in df.columns:
        return df[df['sample_scope'].astype(str).eq('OOS')].copy()
    if 'sample_stage' in df.columns:
        return df[~df['sample_stage'].astype(str).eq('TRAIN')].copy()
    d=_date(df)
    if not d.notna().any():
        return df.iloc[0:0].copy()
    split=d.min()+(d.max()-d.min())*.60
    return df[d.ge(split)].copy()


def _third_labels(df: pd.DataFrame) -> pd.Series:
    d=_date(df)
    out=pd.Series('INVALID_DATE',index=df.index,dtype=object)
    valid=d.notna()
    if not valid.any():
        return out
    uniq=np.array(sorted(d[valid].unique()))
    if len(uniq)<3:
        out.loc[valid]='ALL'
        return out
    i1=max(1,int(math.ceil(len(uniq)/3)))-1
    i2=max(i1+1,int(math.ceil(2*len(uniq)/3)))-1
    cut1=pd.Timestamp(uniq[min(i1,len(uniq)-1)])
    cut2=pd.Timestamp(uniq[min(i2,len(uniq)-1)])
    out.loc[valid & d.le(cut1)]='EARLY'
    out.loc[valid & d.gt(cut1) & d.le(cut2)]='MID'
    out.loc[valid & d.gt(cut2)]='RECENT'
    return out


def _stable_sign(vals: list[float]) -> tuple[int,str]:
    finite=[float(v) for v in vals if np.isfinite(v) and abs(float(v))>1e-12]
    if len(finite)<3:
        return 0,'INSUFFICIENT'
    pos=all(v>0 for v in finite); neg=all(v<0 for v in finite)
    return (1,'POSITIVE' if pos else 'NEGATIVE') if (pos or neg) else (0,'MIXED')


def missed_immediate_audit(opp: pd.DataFrame, selected_ctx: pd.DataFrame, feature_meta: dict[str,tuple[str,str]]) -> tuple[pd.DataFrame,pd.DataFrame,pd.DataFrame,pd.DataFrame]:
    if opp is None or opp.empty:
        return (pd.DataFrame(),)*4
    o=opp.copy()
    strategic=o.get('miss_stage',pd.Series('',index=o.index)).astype(str).eq('STRATEGIC_MISS_CONFIRMED')
    immediate=o.get('opportunity_path_class',pd.Series('',index=o.index)).astype(str).eq('IMMEDIATE_MISS')
    miss=o[strategic & immediate].copy()
    if miss.empty:
        return pd.DataFrame(),pd.DataFrame(),pd.DataFrame(),pd.DataFrame()
    features=_causal_features(feature_meta, miss)

    s=_oos(selected_ctx).copy()
    if s.empty:
        s=selected_ctx.copy()
    plus3=_num(s,'path_first_plus3_day',0).fillna(0)
    stop=_num(s,'stop_first_flag',0).fillna(0)
    net=_num(s,'_net50')
    captured=s[plus3.between(1,3) & stop.eq(0) & net.gt(0)].copy()
    if captured.empty:
        captured=s[net.gt(0)].copy()

    # Strategy prototypes are descriptive only: future outcomes define the reference set, while
    # distances use entry-time features only. Nothing here can be auto-applied.
    proto={}
    for mode,g in captured.groupby(captured.get('mode',pd.Series('',index=captured.index)).astype(str)):
        if mode not in STRATEGIES or len(g)<5: continue
        p={}
        for feat in features:
            v=_num(g,feat).dropna()
            if len(v)<5: continue
            med=float(v.median()); q1=float(v.quantile(.25)); q3=float(v.quantile(.75)); scale=max(abs(q3-q1),abs(med)*.05,1e-6)
            p[feat]=(med,scale)
        if len(p)>=4: proto[mode]=p

    nearest=[]; distances=[]; used=[]; deviations=[]
    for _,r in miss.iterrows():
        best=None
        for mode,p in proto.items():
            ds=[]; parts=[]
            for feat,(med,scale) in p.items():
                v=_safe_float(r.get(feat,np.nan))
                if not np.isfinite(v): continue
                z=(v-med)/scale; ds.append(abs(z)); parts.append((abs(z),feat,z))
            if len(ds)<4: continue
            dist=float(np.mean(ds))
            if best is None or dist<best[0]: best=(dist,mode,len(ds),parts)
        if best is None:
            nearest.append('UNASSESSED'); distances.append(np.nan); used.append(0); deviations.append('')
        else:
            dist,mode,n,parts=best
            nearest.append(mode); distances.append(dist); used.append(n)
            top=sorted(parts,reverse=True)[:3]
            deviations.append(' | '.join(f'{feat}:{z:+.2f}IQR' for _,feat,z in top))
    miss['nearest_strategy_proxy']=nearest
    miss['nearest_strategy_distance']=distances
    miss['nearest_strategy_features_used']=used
    miss['nearest_strategy_top_deviation']=deviations
    miss['audit_scope']='POSTHOC_IMMEDIATE_STRATEGIC_MISS_NEAREST_STRATEGY_PROXY'
    miss['auto_apply']=0

    summary_rows=[]
    for mode,g in miss.groupby('nearest_strategy_proxy',dropna=False):
        summary_rows.append({
            'nearest_strategy_proxy':mode,'n':len(g),'share_pct':len(g)/len(miss)*100.0,
            'next_close_mean_pct':_num(g,'op_ret_next_close').mean(),
            'max5_mean_pct':_num(g,'op_ret_max_high_5d').mean(),
            'fixed35_mean_pct':_num(g,'op_fixed35_pnl').mean(),
            'median_distance':_num(g,'nearest_strategy_distance').median(),
            'auto_apply':0,
        })
    summary=pd.DataFrame(summary_rows)

    contrast_rows=[]
    for mode,mg in miss.groupby('nearest_strategy_proxy'):
        cg=captured[captured.get('mode',pd.Series('',index=captured.index)).astype(str).eq(mode)]
        if len(mg)<5 or len(cg)<5: continue
        for feat in features:
            a=_num(mg,feat); b=_num(cg,feat)
            if a.notna().sum()<5 or b.notna().sum()<5: continue
            contrast_rows.append({
                'nearest_strategy_proxy':mode,'feature':feat,'feature_label':feature_meta.get(feat,(feat,''))[0],
                'miss_n':int(a.notna().sum()),'captured_n':int(b.notna().sum()),
                'miss_median':a.median(),'captured_median':b.median(),'median_delta':a.median()-b.median(),
                'cliffs_delta_miss_vs_capture':_cliffs_delta(a,b),'auto_apply':0,
            })
    contrast=pd.DataFrame(contrast_rows)
    if not contrast.empty:
        contrast['abs_cliffs']=contrast['cliffs_delta_miss_vs_capture'].abs()
        contrast=contrast.sort_values(['nearest_strategy_proxy','abs_cliffs'],ascending=[True,False])

    case_cols=[c for c in ['signal_date','code','name','nearest_strategy_proxy','nearest_strategy_distance','nearest_strategy_top_deviation',
                           'op_ret_next_close','op_ret_close_3d','op_ret_close_5d','op_ret_max_high_5d','op_mfe_10d','op_mae_10d',
                           'op_first_plus3_day','op_first_minus3_day','op_fixed35_pnl','entry_stock_ret_1d','entry_stock_ret_5d',
                           'entry_stock_ret_20d','entry_close_loc_pct','entry_upper_wick_pct','entry_gap_pct','entry_vol20_ratio',
                           'entry_vol50_ratio','entry_amount_b','entry_amount20_ratio','entry_ma20_dist_pct','entry_ma60_dist_pct',
                           'entry_high20_dist_pct'] if c in miss.columns]
    cases=miss.sort_values(['op_fixed35_pnl','op_ret_next_close','op_ret_max_high_5d'],ascending=False).head(500)[case_cols].copy()
    return summary,contrast,cases,miss


def ranking_inversion_audit(raw: pd.DataFrame, feature_meta: dict[str,tuple[str,str]]) -> tuple[pd.DataFrame,pd.DataFrame,pd.DataFrame]:
    if raw is None or raw.empty:
        return pd.DataFrame(),pd.DataFrame(),pd.DataFrame()
    r=raw.copy()
    r['signal_date']=r.get('signal_date',pd.Series('',index=r.index)).astype(str)
    r['mode']=r.get('mode',pd.Series('',index=r.index)).astype(str)
    r['code']=r.get('code',pd.Series('',index=r.index)).astype(str).str.zfill(6)
    r['_score71']=_num(r,'score',0).fillna(0); r['_amount71']=_num(r,'amount_b',_num(r,'entry_amount_b',0)).fillna(0)
    r['_net50_71']=_num(r,'rule35_pnl')-.50
    r=r[r['mode'].isin(['LP','G']) & r['_net50_71'].notna()].copy()
    if r.empty:
        return pd.DataFrame(),pd.DataFrame(),pd.DataFrame()
    features=_causal_features(feature_meta,r)
    r=r.sort_values(['signal_date','mode','_score71','_amount71','code'],ascending=[True,True,False,False,True])
    r['rank71']=r.groupby(['signal_date','mode']).cumcount()+1
    rows=[]
    for (d,m),g in r.groupby(['signal_date','mode']):
        q=g[g.rank71.le(5)]
        if len(q)<2: continue
        one=q[q.rank71.eq(1)].iloc[0]
        alt=q[q.rank71.ge(2)].sort_values('_net50_71',ascending=False).iloc[0]
        rec={
            'signal_date':d,'strategy':m,
            'rank1_code':one.get('code',''),'rank1_name':one.get('name',''),'rank1_net50':_safe_float(one.get('_net50_71')),
            'rank1_score':_safe_float(one.get('_score71')),'rank1_amount_b':_safe_float(one.get('_amount71')),
            'best_alt_rank':int(alt.get('rank71',0)),'best_alt_code':alt.get('code',''),'best_alt_name':alt.get('name',''),
            'best_alt_net50':_safe_float(alt.get('_net50_71')),'best_alt_score':_safe_float(alt.get('_score71')),
            'best_alt_amount_b':_safe_float(alt.get('_amount71')),
        }
        rec['alt_minus_rank1_pctp']=rec['best_alt_net50']-rec['rank1_net50']
        rec['ranking_inversion']=int(rec['alt_minus_rank1_pctp']>0)
        rec['actionable_inversion']=int(rec['rank1_net50']<=0 and rec['best_alt_net50']>0)
        for feat in features:
            a=_safe_float(alt.get(feat,np.nan)); b=_safe_float(one.get(feat,np.nan))
            rec[f'delta__{feat}']=a-b if np.isfinite(a) and np.isfinite(b) else np.nan
        rows.append(rec)
    cases=pd.DataFrame(rows)
    if cases.empty:
        return pd.DataFrame(),pd.DataFrame(),cases
    cases['_third']=_third_labels(cases)
    summary_rows=[]
    for m,g in cases.groupby('strategy'):
        summary_rows.append({
            'strategy':m,'date_groups_n':len(g),
            'rank1_net50_mean_pct':_num(g,'rank1_net50').mean(),
            'best_rank2_5_net50_mean_pct':_num(g,'best_alt_net50').mean(),
            'alt_minus_rank1_mean_pctp':_num(g,'alt_minus_rank1_pctp').mean(),
            'inversion_pct':_num(g,'ranking_inversion',0).mean()*100.0,
            'actionable_inversion_pct':_num(g,'actionable_inversion',0).mean()*100.0,
            'actionable_inversion_n':int(_num(g,'actionable_inversion',0).sum()),
            'audit_only':1,'auto_apply':0,
        })
    summary=pd.DataFrame(summary_rows)

    feat_rows=[]
    for m,g0 in cases[cases.actionable_inversion.eq(1)].groupby('strategy'):
        for feat in features:
            col=f'delta__{feat}'; v=_num(g0,col)
            if v.notna().sum()<8: continue
            thirds=[]; third_ns=[]
            for t in ['EARLY','MID','RECENT']:
                tv=_num(g0[g0._third.eq(t)],col).dropna(); thirds.append(float(tv.median()) if len(tv) else np.nan); third_ns.append(int(len(tv)))
            stable,direction=_stable_sign(thirds)
            scale=max(float(_num(r[r['mode'].eq(m)],feat).quantile(.75)-_num(r[r['mode'].eq(m)],feat).quantile(.25)) if _num(r[r['mode'].eq(m)],feat).notna().sum() else 0.0,1e-6)
            med=float(v.median())
            feat_rows.append({
                'strategy':m,'feature':feat,'feature_label':feature_meta.get(feat,(feat,''))[0],
                'actionable_inversion_n':int(v.notna().sum()),'alt_minus_rank1_median':med,
                'standardized_median_delta_iqr':med/scale,
                'positive_delta_pct':float(v.gt(0).mean()*100.0),
                'early_median_delta':thirds[0],'mid_median_delta':thirds[1],'recent_median_delta':thirds[2],
                'early_n':third_ns[0],'mid_n':third_ns[1],'recent_n':third_ns[2],
                'stable_direction_3of3':stable,'stable_direction':direction,
                'research_signal':int(stable and min(third_ns)>=5 and abs(med/scale)>=.25),
                'selection_authority':'NONE_DIAGNOSTIC_ONLY','auto_apply':0,
            })
    feats=pd.DataFrame(feat_rows)
    if not feats.empty:
        feats=feats.sort_values(['strategy','research_signal','standardized_median_delta_iqr'],ascending=[True,False,False],key=lambda x:x.abs() if x.name=='standardized_median_delta_iqr' else x)
    return summary,feats,cases.drop(columns=['_third'],errors='ignore')


def a_big_stop_audit(selected_ctx: pd.DataFrame, feature_meta: dict[str,tuple[str,str]]) -> tuple[pd.DataFrame,pd.DataFrame,pd.DataFrame]:
    z=_oos(selected_ctx)
    z=z[z.get('mode',pd.Series('',index=z.index)).astype(str).eq('A')].copy()
    if z.empty:
        return pd.DataFrame(),pd.DataFrame(),pd.DataFrame()
    features=_causal_features(feature_meta,z)
    big=z[_num(z,'big_before_stop',0).fillna(0).gt(0)].copy()
    stop=z[_num(z,'stop_first_flag',0).fillna(0).gt(0)].copy()
    summary=pd.DataFrame([{
        'strategy':'A','oos_n':len(z),'big_n':len(big),'stop_first_n':len(stop),
        'big_share_pct':len(big)/len(z)*100.0,'stop_first_share_pct':len(stop)/len(z)*100.0,
        'big_net50_mean_pct':_num(big,'_net50').mean(),'stop_net50_mean_pct':_num(stop,'_net50').mean(),
        'big_mfe_mean_pct':_num(big,'path_max_high_ret').mean(),'stop_mae_mean_pct':_num(stop,'path_min_low_ret').mean(),
        'audit_only':1,'auto_apply':0,
    }])
    z['_third71']=_third_labels(z)
    rows=[]
    for feat in features:
        a=_num(big,feat); b=_num(stop,feat)
        if a.notna().sum()<10 or b.notna().sum()<10: continue
        delta=_cliffs_delta(a,b); med=float(a.median()-b.median())
        third_delta=[]; third_n_big=[]; third_n_stop=[]
        for t in ['EARLY','MID','RECENT']:
            q=z[z._third71.eq(t)]
            ab=_num(q[_num(q,'big_before_stop',0).fillna(0).gt(0)],feat)
            sb=_num(q[_num(q,'stop_first_flag',0).fillna(0).gt(0)],feat)
            third_delta.append(float(ab.median()-sb.median()) if ab.notna().sum() and sb.notna().sum() else np.nan)
            third_n_big.append(int(ab.notna().sum())); third_n_stop.append(int(sb.notna().sum()))
        stable,direction=_stable_sign(third_delta)
        rows.append({
            'feature':feat,'feature_label':feature_meta.get(feat,(feat,''))[0],
            'big_n':int(a.notna().sum()),'stop_n':int(b.notna().sum()),'big_median':a.median(),'stop_median':b.median(),
            'median_delta_big_minus_stop':med,'cliffs_delta_big_vs_stop':delta,
            'early_delta':third_delta[0],'mid_delta':third_delta[1],'recent_delta':third_delta[2],
            'early_big_n':third_n_big[0],'mid_big_n':third_n_big[1],'recent_big_n':third_n_big[2],
            'early_stop_n':third_n_stop[0],'mid_stop_n':third_n_stop[1],'recent_stop_n':third_n_stop[2],
            'stable_direction_3of3':stable,'stable_direction':direction,
            'research_signal':int(stable and min(third_n_big)>=10 and min(third_n_stop)>=8 and np.isfinite(delta) and abs(delta)>=.25),
            'selection_authority':'NONE_DIAGNOSTIC_ONLY','auto_apply':0,
        })
    contrast=pd.DataFrame(rows)
    if not contrast.empty:
        contrast['abs_cliffs']=contrast['cliffs_delta_big_vs_stop'].abs()
        contrast=contrast.sort_values(['research_signal','abs_cliffs'],ascending=[False,False])
    case_cols=[c for c in ['signal_date','code','name','mode','score','intent_status','outcome_class','_net50','path_max_high_ret','path_min_low_ret',
                           'path_first_plus3_day','path_first_stop_day','entry_stock_ret_1d','entry_stock_ret_5d','entry_stock_ret_20d',
                           'entry_close_loc_pct','entry_upper_wick_pct','entry_gap_pct','entry_vol20_ratio','entry_vol50_ratio','entry_amount_b',
                           'entry_amount20_ratio','entry_ma20_dist_pct','entry_ma60_dist_pct','entry_high20_dist_pct','market_ret_5d_t1','stock_excess_5d'] if c in z.columns]
    cb=[]
    if len(big):
        x=big.sort_values(['path_max_high_ret','_net50'],ascending=False).head(100)[case_cols].copy(); x.insert(0,'case_type','A_BIG'); cb.append(x)
    if len(stop):
        x=stop.sort_values(['_net50','path_min_low_ret'],ascending=True).head(100)[case_cols].copy(); x.insert(0,'case_type','A_STOP_FIRST'); cb.append(x)
    cases=pd.concat(cb,ignore_index=True,sort=False) if cb else pd.DataFrame()
    return summary,contrast,cases


H_DRYUP_V2_CHECKS = (
    ('days_since_high_breakout', lambda x: x.ge(1)&x.le(7), '돌파후1~7일'),
    ('breakout_vol60_ratio', lambda x: x.ge(1.5), '돌파거래량1.5배'),
    ('breakout_day_ret_pct', lambda x: x.ge(7.0), '돌파일+7%'),
    ('breakout_body_pct', lambda x: x.ge(5.0), '돌파몸통5%'),
    ('breakout_close_loc_pct', lambda x: x.ge(75.0), '돌파상단마감'),
    ('breakout_upper_wick_pct', lambda x: x.le(25.0), '돌파윗꼬리제한'),
    ('high_dryup_volume_dry', lambda x: x.ge(1), '거래량마름'),
    ('high_dryup_short_candle', lambda x: x.ge(1), '짧은타점봉'),
    ('high_dryup_ma5_close_hold', lambda x: x.ge(1), '5일선위종가'),
    ('high_dryup_entry_close_loc_ok', lambda x: x.ge(1), '타점봉중상단'),
    ('high_dryup_zone_hold', lambda x: x.ge(1), '돌파권유지'),
    ('high_dryup_proper_pullback', lambda x: x.ge(1), '눌림1~10%'),
)

H_CP6_V2_CHECKS = (
    ('cp6_enough_adjust', lambda x: x.ge(1), '충분조정'),
    ('cp6_near_prior_high', lambda x: x.ge(1), '전고점근접'),
    ('cp6_clean_bull', lambda x: x.ge(1), '깔끔양봉'),
    ('cp6_money_reignite', lambda x: x.ge(1), '거래량·거래대금재점화'),
    ('cp6_ma_reclaim', lambda x: x.ge(1), '5/20/60선회복'),
    ('cp6_overextended', lambda x: x.le(0), '과확장아님'),
)


def _h_profile_series(h: pd.DataFrame) -> pd.Series:
    band=h.get('band_type',pd.Series('',index=h.index)).astype(str)
    cp6=_num(h,'cp6_signal',0).fillna(0).gt(0) | band.eq('CP6_PRIOR_HIGH_RECLAIM')
    dry=band.eq('HIGH_DRYUP_STRICT')
    # Historical rows may have lost band_type while retaining authority-specific payload fields.
    cp6 = cp6 | (_num(h,'cp6_enough_adjust').notna() & _num(h,'cp6_near_prior_high').notna())
    dry = dry | (_num(h,'high_dryup_volume_dry').notna() & _num(h,'days_since_high_breakout').notna())
    return pd.Series(np.select([cp6,dry],['CP6_PRIOR_HIGH_RECLAIM','HIGH_DRYUP_STRICT'],default='UNKNOWN_H_PROFILE'),index=h.index,dtype=object)


def _audit_h_profile(h: pd.DataFrame, mask: pd.Series, profile: str, checks, critical: set[str]) -> tuple[pd.DataFrame,list[dict]]:
    idx=h.index[mask]
    if len(idx)==0:
        return h,[]
    total=len(checks)
    observed=pd.Series(0,index=idx,dtype=int); passed=pd.Series(0,index=idx,dtype=int)
    critical_pass=pd.Series(0,index=idx,dtype=int); failed_text=pd.Series('',index=idx,dtype=object)
    cond_rows=[]
    for col,fn,label in checks:
        vals=_num(h.loc[idx],col)
        obs=vals.notna(); ok=fn(vals).fillna(False)&obs
        observed+=obs.astype(int); passed+=ok.astype(int)
        if col in critical: critical_pass+=ok.astype(int)
        failed=obs&~ok
        for i in idx[failed]:
            cur=str(failed_text.loc[i] or '')
            failed_text.loc[i]=(cur+' | ' if cur else '')+label
        cond_rows.append({'h_intent_profile':profile,'condition':col,'label':label,
                          'observed_n':int(obs.sum()),'pass_n':int(ok.sum()),
                          'pass_pct':float(ok[obs].mean()*100.0) if obs.any() else np.nan})
    coverage=observed/float(total); ratio=passed/observed.replace(0,np.nan)
    crit_needed=max(1,len(critical)-1)
    core_name='H_CP6_CORE_INTENT' if profile.startswith('CP6') else 'H_DRYUP_CORE_INTENT'
    relaxed_name='H_CP6_RELAXED_INTENT' if profile.startswith('CP6') else 'H_DRYUP_RELAXED_INTENT'
    weak_name='H_CP6_WEAK_INTENT' if profile.startswith('CP6') else 'H_DRYUP_WEAK_INTENT'
    status=np.select([
        coverage.lt(.65),
        ratio.ge(.80)&critical_pass.ge(crit_needed),
        ratio.ge(.65),
    ],['UNASSESSED',core_name,relaxed_name],default=weak_name)
    h.loc[idx,'h_intent_v2_checks_total']=total
    h.loc[idx,'h_intent_v2_observed']=observed
    h.loc[idx,'h_intent_v2_passed']=passed
    h.loc[idx,'h_intent_v2_coverage']=coverage
    h.loc[idx,'h_intent_v2_match_ratio']=ratio
    h.loc[idx,'h_intent_v2_critical_passed']=critical_pass
    h.loc[idx,'h_intent_v2_failed_checks']=failed_text
    h.loc[idx,'h_intent_v2_status']=status
    return h,cond_rows


def h_intent_v2_audit(selected_ctx: pd.DataFrame) -> tuple[pd.DataFrame,pd.DataFrame,pd.DataFrame,pd.DataFrame]:
    h=selected_ctx[selected_ctx.get('mode',pd.Series('',index=selected_ctx.index)).astype(str).eq('H')].copy()
    if h.empty:
        return (pd.DataFrame(),)*4
    h['h_intent_profile']=_h_profile_series(h)
    for c in ['h_intent_v2_checks_total','h_intent_v2_observed','h_intent_v2_passed','h_intent_v2_coverage','h_intent_v2_match_ratio','h_intent_v2_critical_passed']:
        h[c]=np.nan
    h['h_intent_v2_failed_checks']=''
    h['h_intent_v2_status']='UNASSESSED'
    cond_rows=[]
    h,rows=_audit_h_profile(
        h,h['h_intent_profile'].eq('HIGH_DRYUP_STRICT'),'HIGH_DRYUP_STRICT',H_DRYUP_V2_CHECKS,
        {'high_dryup_volume_dry','high_dryup_short_candle','high_dryup_ma5_close_hold','high_dryup_zone_hold'}
    ); cond_rows.extend(rows)
    h,rows=_audit_h_profile(
        h,h['h_intent_profile'].eq('CP6_PRIOR_HIGH_RECLAIM'),'CP6_PRIOR_HIGH_RECLAIM',H_CP6_V2_CHECKS,
        {'cp6_enough_adjust','cp6_near_prior_high','cp6_clean_bull','cp6_money_reignite','cp6_ma_reclaim'}
    ); cond_rows.extend(rows)
    h['h_intent_v2_scope']='AUTHORITY_DUAL_PROFILE_SEMANTIC_PROXY_AUDIT_ONLY'
    h['auto_apply']=0

    summary_rows=[]
    for (profile,status),g in h.groupby(['h_intent_profile','h_intent_v2_status'],dropna=False):
        oo=_oos(g)
        summary_rows.append({'h_intent_profile':profile,'h_intent_v2_status':status,'n':len(g),'oos_n':len(oo),
                             'net50_mean_pct':_num(g,'_net50').mean(),'oos_net50_mean_pct':_num(oo,'_net50').mean(),
                             'big_pct':_num(g,'big_before_stop',0).fillna(0).gt(0).mean()*100.0,
                             'stop_first_pct':_num(g,'stop_first_flag',0).fillna(0).gt(0).mean()*100.0,
                             'auto_apply':0})
    summary=pd.DataFrame(summary_rows)
    old=h.get('intent_status',pd.Series('UNASSESSED',index=h.index)).astype(str).rename('old_intent_status')
    matrix=(pd.DataFrame({'old_intent_status':old,'h_intent_profile':h['h_intent_profile'],'h_intent_v2_status':h['h_intent_v2_status']})
              .groupby(['old_intent_status','h_intent_profile','h_intent_v2_status'],dropna=False).size().reset_index(name='n'))
    conditions=pd.DataFrame(cond_rows)
    return summary,matrix,conditions,h

def _hypothesis_registry(miss_summary:pd.DataFrame, rank_summary:pd.DataFrame, rank_features:pd.DataFrame,
                         a_summary:pd.DataFrame, a_contrast:pd.DataFrame, h_summary:pd.DataFrame, h_matrix:pd.DataFrame)->pd.DataFrame:
    rows=[]
    nmiss=int(miss_summary['n'].sum()) if not miss_summary.empty and 'n' in miss_summary else 0
    rows.append({'lane':'IMMEDIATE_STRATEGIC_MISS','status':'DATA_AVAILABLE' if nmiss else 'NO_CASES','evidence_n':nmiss,
                 'finding':'Search-rejected immediate winners are mapped to nearest captured-strategy entry-time prototype; no rule is promoted.',
                 'next_action':'CASE_REVIEW_AND_REPEATABILITY_ONLY','auto_apply':0})
    for m in ['LP','G']:
        q=rank_summary[rank_summary.strategy.eq(m)] if not rank_summary.empty else pd.DataFrame()
        sig=rank_features[(rank_features.strategy.eq(m))&(rank_features.research_signal.eq(1))] if not rank_features.empty else pd.DataFrame()
        n=int(q.iloc[0].actionable_inversion_n) if len(q) else 0
        rows.append({'lane':f'{m}_RANK_INVERSION','status':'RECURRING_FEATURE_DIRECTION' if len(sig) else ('DATA_AVAILABLE' if len(q) else 'NO_DATA'),
                     'evidence_n':n,'finding':f'Rank1-vs-rank2~5 actionable inversion; stable entry-feature directions={len(sig)}.',
                     'next_action':'DO_NOT_RERANK; ACCUMULATE_PROSPECTIVE_PAPER','auto_apply':0})
    sig_a=a_contrast[a_contrast.research_signal.eq(1)] if not a_contrast.empty else pd.DataFrame()
    n_a=int(a_summary.iloc[0].oos_n) if len(a_summary) else 0
    rows.append({'lane':'A_BIG_VS_STOP','status':'RECURRING_FEATURE_DIRECTION' if len(sig_a) else ('DATA_AVAILABLE' if n_a else 'NO_DATA'),
                 'evidence_n':n_a,'finding':f'A BIG-vs-STOP_FIRST stable entry-feature directions={len(sig_a)}.',
                 'next_action':'HYPOTHESIS_REGISTRATION_ONLY','auto_apply':0})
    reclass=0
    if not h_matrix.empty:
        reclass=int(h_matrix[(h_matrix.old_intent_status=='INTENT_MISMATCH')&h_matrix.h_intent_v2_status.astype(str).str.contains('_(?:CORE|RELAXED)_INTENT$',regex=True)]['n'].sum())
    rows.append({'lane':'H_INTENT_REAUDIT','status':'PROXY_MISALIGNMENT_FOUND' if reclass else ('DATA_AVAILABLE' if len(h_summary) else 'NO_DATA'),
                 'evidence_n':reclass,'finding':f'Old H MISMATCH reclassified to authority-semantic core/relaxed={reclass}.',
                 'next_action':'USE_V2_FOR_AUDIT_LABEL_ONLY; AUTHORITATIVE_H_UNCHANGED','auto_apply':0})
    return pd.DataFrame(rows)


def run_insight_audit(raw:pd.DataFrame, selected_ctx:pd.DataFrame, opp_detail:pd.DataFrame, out:Path,
                      feature_meta:dict[str,tuple[str,str]]) -> dict[str,Any]:
    out=Path(out); out.mkdir(parents=True,exist_ok=True)
    # Hard leakage assertion before any diagnostic lane can inspect candidate features.
    _causal_features(feature_meta, selected_ctx if selected_ctx is not None else pd.DataFrame())

    miss_summary,miss_contrast,miss_cases,miss_detail=missed_immediate_audit(opp_detail,selected_ctx,feature_meta)
    rank_summary,rank_features,rank_cases=ranking_inversion_audit(raw,feature_meta)
    a_summary,a_contrast,a_cases=a_big_stop_audit(selected_ctx,feature_meta)
    h_summary,h_matrix,h_conditions,h_detail=h_intent_v2_audit(selected_ctx)
    registry=_hypothesis_registry(miss_summary,rank_summary,rank_features,a_summary,a_contrast,h_summary,h_matrix)

    outputs={
        'v49_71_immediate_strategic_miss_summary.csv':miss_summary,
        'v49_71_immediate_strategic_miss_feature_contrast.csv':miss_contrast,
        'v49_71_immediate_strategic_miss_casebook.csv':miss_cases,
        'v49_71_immediate_strategic_miss_detail.csv':miss_detail,
        'v49_71_rank_inversion_summary.csv':rank_summary,
        'v49_71_rank_inversion_feature_stability.csv':rank_features,
        'v49_71_rank_inversion_cases.csv':rank_cases,
        'v49_71_a_big_vs_stop_summary.csv':a_summary,
        'v49_71_a_big_vs_stop_feature_stability.csv':a_contrast,
        'v49_71_a_big_vs_stop_casebook.csv':a_cases,
        'v49_71_h_intent_v2_summary.csv':h_summary,
        'v49_71_h_intent_v1_v2_matrix.csv':h_matrix,
        'v49_71_h_intent_v2_condition_pass.csv':h_conditions,
        'v49_71_h_intent_v2_detail.csv':h_detail,
        'v49_71_hypothesis_registry.csv':registry,
    }
    for name,df in outputs.items():
        df.to_csv(out/name,index=False,encoding='utf-8-sig')

    lanes={
        'immediate_strategic_miss_n':int(miss_summary['n'].sum()) if not miss_summary.empty else 0,
        'lp_actionable_rank_inversion_n':int(rank_summary.loc[rank_summary.strategy.eq('LP'),'actionable_inversion_n'].sum()) if not rank_summary.empty else 0,
        'g_actionable_rank_inversion_n':int(rank_summary.loc[rank_summary.strategy.eq('G'),'actionable_inversion_n'].sum()) if not rank_summary.empty else 0,
        'rank_stable_feature_signals':int(rank_features.research_signal.sum()) if not rank_features.empty else 0,
        'a_big_n':int(a_summary.iloc[0].big_n) if len(a_summary) else 0,
        'a_stop_first_n':int(a_summary.iloc[0].stop_first_n) if len(a_summary) else 0,
        'a_stable_feature_signals':int(a_contrast.research_signal.sum()) if not a_contrast.empty else 0,
        'h_v2_rows':int(len(h_detail)),
        'h_old_mismatch_reclassified_core_relaxed':int(h_matrix[(h_matrix.old_intent_status=='INTENT_MISMATCH')&h_matrix.h_intent_v2_status.astype(str).str.contains('_(?:CORE|RELAXED)_INTENT$',regex=True)]['n'].sum()) if not h_matrix.empty else 0,
    }
    nonempty=sum(int(v>0) for k,v in lanes.items() if k.endswith('_n') or k.endswith('_rows'))
    status='FULL-VALID' if nonempty>=4 else ('PARTIAL-VALID' if nonempty else 'NO-DATA')
    manifest={
        'version':'v49.71','status':status,'audit_scope':'MISSED_WINNER_RANK_INVERSION_A_BIGSTOP_H_INTENT',
        'feature_authority':'ENTRY_TIME_FEATURES_ONLY','future_outcomes':'LABELS_AND_POSTHOC_COMPARATORS_ONLY',
        'threshold_selection':'NONE','rank_change':'NONE','search_change':'NONE','exit_change':'NONE',
        'auto_apply':0,'paper_only':True,'real_orders':0,'lanes':lanes,
        'files':list(outputs.keys()),
    }
    (out/'v49_71_insight_manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
    return {
        'manifest':manifest,'miss_summary':miss_summary,'miss_contrast':miss_contrast,'miss_cases':miss_cases,
        'rank_summary':rank_summary,'rank_features':rank_features,'rank_cases':rank_cases,
        'a_summary':a_summary,'a_contrast':a_contrast,'a_cases':a_cases,
        'h_summary':h_summary,'h_matrix':h_matrix,'h_conditions':h_conditions,'registry':registry,
    }
