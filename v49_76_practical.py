from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def _num(df: pd.DataFrame, col: str, default=np.nan) -> pd.Series:
    if isinstance(df, pd.DataFrame) and col in df.columns:
        return pd.to_numeric(df[col], errors='coerce')
    return pd.Series(default, index=df.index if isinstance(df, pd.DataFrame) else pd.RangeIndex(0), dtype=float)


def _dates(df: pd.DataFrame) -> pd.Series:
    return pd.to_datetime(df.get('signal_date'), errors='coerce').dt.normalize()


def _mean(s: pd.Series) -> float:
    s=pd.to_numeric(s,errors='coerce').dropna()
    return float(s.mean()) if len(s) else np.nan


def _top_removed_mean(s: pd.Series, k: int=3) -> float:
    s=pd.to_numeric(s,errors='coerce').dropna().sort_values(ascending=False)
    if len(s)<=k: return np.nan
    return float(s.iloc[k:].mean())


def _simple_mdd(df: pd.DataFrame, ret_col: str='_net50') -> float:
    if df is None or df.empty or ret_col not in df.columns: return np.nan
    z=df.copy(); z['_d']=_dates(z); z['_r']=pd.to_numeric(z[ret_col],errors='coerce')
    z=z[z['_d'].notna()&z['_r'].notna()].sort_values('_d')
    if z.empty: return np.nan
    # Equal-weight daily mean; research diagnostic only, not portfolio accounting authority.
    daily=z.groupby('_d')['_r'].mean()/100.0
    eq=(1.0+daily).cumprod(); peak=eq.cummax(); dd=(eq/peak-1.0)*100.0
    return float(dd.min()) if len(dd) else np.nan


def _stage_metrics(df: pd.DataFrame, threshold: float) -> dict[str,Any]:
    z=df.copy() if isinstance(df,pd.DataFrame) else pd.DataFrame()
    if z.empty:
        return {'base_n':0,'filtered_n':0,'base_net50':np.nan,'filtered_net50':np.nan,'lift_pctp':np.nan,'top3_removed_net50':np.nan,'mdd_pct':np.nan,'big_pct':np.nan,'stop_pct':np.nan}
    z['_x']=_num(z,'entry_ma60_dist_pct'); z['_r']=_num(z,'_net50')
    z=z[z['_r'].notna()].copy(); f=z[z['_x'].ge(float(threshold))].copy()
    cls=z.get('outcome_class',pd.Series('',index=z.index)).astype(str)
    fcls=f.get('outcome_class',pd.Series('',index=f.index)).astype(str)
    return {
        'base_n':int(len(z)), 'filtered_n':int(len(f)),
        'base_net50':_mean(z['_r']), 'filtered_net50':_mean(f['_r']),
        'lift_pctp':_mean(f['_r'])-_mean(z['_r']) if len(f) else np.nan,
        'top3_removed_net50':_top_removed_mean(f['_r'],3), 'mdd_pct':_simple_mdd(f,'_r'),
        'base_big_pct':float(cls.eq('BIG_CAPTURABLE').mean()*100.0) if len(z) else np.nan,
        'big_pct':float(fcls.eq('BIG_CAPTURABLE').mean()*100.0) if len(f) else np.nan,
        'base_stop_pct':float(cls.eq('STOP_FIRST').mean()*100.0) if len(z) else np.nan,
        'stop_pct':float(fcls.eq('STOP_FIRST').mean()*100.0) if len(f) else np.nan,
    }


def a_ma60_fixed_hypothesis(enriched: pd.DataFrame, out: Path, min_validation_n: int=15, min_test_n: int=15, min_lift_pctp: float=0.30) -> dict[str,Any]:
    """Fixed-direction A hypothesis.

    Threshold is derived once from TRAIN outcome anatomy as the midpoint between the TRAIN
    BIG_CAPTURABLE and STOP_FIRST MA60-distance medians. It is not re-selected on validation/test.
    """
    out=Path(out); out.mkdir(parents=True,exist_ok=True)
    a=enriched[enriched.get('mode',pd.Series('',index=enriched.index)).astype(str).eq('A')].copy() if isinstance(enriched,pd.DataFrame) else pd.DataFrame()
    if a.empty or 'sample_stage' not in a.columns:
        manifest={'status':'NO_DATA','threshold':None,'direction':'HIGH_GOOD','auto_apply':0,'paper_only':True}
        pd.DataFrame().to_csv(out/'v49_76_a_ma60_fixed_hypothesis.csv',index=False)
        (out/'v49_76_a_ma60_fixed_hypothesis_manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')
        return {'manifest':manifest,'summary':pd.DataFrame(),'cases':pd.DataFrame()}
    tr=a[a.sample_stage.astype(str).eq('TRAIN')].copy(); big=tr[tr.get('outcome_class',pd.Series('',index=tr.index)).astype(str).eq('BIG_CAPTURABLE')]; stop=tr[tr.get('outcome_class',pd.Series('',index=tr.index)).astype(str).eq('STOP_FIRST')]
    bmed=_num(big,'entry_ma60_dist_pct').median(); smed=_num(stop,'entry_ma60_dist_pct').median()
    if pd.isna(bmed) or pd.isna(smed) or float(bmed)<=float(smed):
        manifest={'status':'TRAIN_DIRECTION_NOT_CONFIRMED','train_big_median':None if pd.isna(bmed) else float(bmed),'train_stop_median':None if pd.isna(smed) else float(smed),'threshold':None,'direction':'HIGH_GOOD','auto_apply':0,'paper_only':True}
        pd.DataFrame().to_csv(out/'v49_76_a_ma60_fixed_hypothesis.csv',index=False)
        (out/'v49_76_a_ma60_fixed_hypothesis_manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')
        return {'manifest':manifest,'summary':pd.DataFrame(),'cases':pd.DataFrame()}
    threshold=round((float(bmed)+float(smed))/2.0,4)
    rows=[]
    for stage in ['TRAIN','VALIDATION','LOCKED_TEST']:
        z=a[a.sample_stage.astype(str).eq(stage)].copy(); m=_stage_metrics(z,threshold); m.update({'stage':stage,'threshold':threshold,'direction':'HIGH_GOOD','rule_text':f'entry_ma60_dist_pct >= {threshold:.4f}','auto_apply':0}); rows.append(m)
    summary=pd.DataFrame(rows)
    v=summary[summary.stage.eq('VALIDATION')].iloc[0]; t=summary[summary.stage.eq('LOCKED_TEST')].iloc[0]
    def _pass(r,min_n):
        vals=[r.get('filtered_net50'),r.get('lift_pctp'),r.get('top3_removed_net50')]
        return int(r.get('filtered_n',0))>=int(min_n) and all(pd.notna(x) for x in vals) and float(r.filtered_net50)>0 and float(r.lift_pctp)>=float(min_lift_pctp) and float(r.top3_removed_net50)>0
    vpass=_pass(v,min_validation_n); tpass=_pass(t,min_test_n) if vpass else False
    status='LOCKED_TEST_PASS' if vpass and tpass else ('LOCKED_TEST_FAIL' if vpass else 'VALIDATION_FAIL')
    manifest={'status':status,'threshold':threshold,'direction':'HIGH_GOOD','train_big_median':float(bmed),'train_stop_median':float(smed),'validation_pass':bool(vpass),'locked_test_pass':bool(tpass),'min_validation_n':int(min_validation_n),'min_test_n':int(min_test_n),'min_lift_pctp':float(min_lift_pctp),'threshold_authority':'TRAIN_BIG_STOP_MEDIAN_MIDPOINT_FIXED_ONCE','validation_reselection':'NONE','locked_test_access':'ONCE','search_change':'NONE','rank_change':'NONE','auto_apply':0,'paper_only':True,'real_orders':0}
    cases=a.copy(); cases['a_ma60_fixed_threshold']=threshold; cases['a_ma60_fixed_pass']=_num(cases,'entry_ma60_dist_pct').ge(threshold).astype(int); keep=[c for c in ['signal_date','code','name','mode','sample_stage','outcome_class','_net50','entry_ma60_dist_pct','entry_high20_dist_pct','entry_stock_ret_20d','entry_amount_b','score','a_ma60_fixed_threshold','a_ma60_fixed_pass'] if c in cases.columns]; cases=cases[keep].sort_values(['sample_stage','signal_date','a_ma60_fixed_pass'],ascending=[True,True,False]) if keep else pd.DataFrame()
    summary.to_csv(out/'v49_76_a_ma60_fixed_hypothesis.csv',index=False,encoding='utf-8-sig'); cases.to_csv(out/'v49_76_a_ma60_fixed_hypothesis_cases.csv',index=False,encoding='utf-8-sig'); (out/'v49_76_a_ma60_fixed_hypothesis_manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
    return {'manifest':manifest,'summary':summary,'cases':cases}


def h_high_dryup_prospective(h_detail: pd.DataFrame, out: Path, latest_signal_date=None) -> dict[str,Any]:
    out=Path(out); out.mkdir(parents=True,exist_ok=True)
    h=h_detail.copy() if isinstance(h_detail,pd.DataFrame) else pd.DataFrame()
    if h.empty:
        manifest={'status':'NO_DATA','latest_signal_date':'','current_candidates':0,'auto_apply':0,'paper_only':True}; cur=pd.DataFrame()
    else:
        h['_d']=_dates(h); observed_latest=h['_d'].dropna().max(); requested_latest=pd.to_datetime(latest_signal_date,errors='coerce') if latest_signal_date is not None else pd.NaT; latest=(requested_latest.normalize() if pd.notna(requested_latest) else observed_latest); dry=h[h.get('h_intent_profile',pd.Series('',index=h.index)).astype(str).eq('HIGH_DRYUP_STRICT')].copy(); core=dry[dry.get('h_intent_v2_status',pd.Series('',index=dry.index)).astype(str).isin(['H_DRYUP_CORE_INTENT','H_DRYUP_RELAXED_INTENT'])].copy(); cur=core[core['_d'].eq(latest)].copy() if pd.notna(latest) else core.iloc[0:0].copy(); cur['prospective_label']='H_HIGH_DRYUP_PAPER_OBSERVE_ONLY'; cur['auto_apply']=0
        manifest={'status':'READY_FOR_PROSPECTIVE_PAPER' if len(cur) else 'NO_CURRENT_CANDIDATE','latest_signal_date':latest.strftime('%Y-%m-%d') if pd.notna(latest) else '','historical_dryup_rows':int(len(dry)),'historical_core_relaxed_rows':int(len(core)),'current_candidates':int(len(cur)),'rule':'AUTHORITY_H_UNCHANGED; semantic profile label only','search_change':'NONE','auto_apply':0,'paper_only':True,'real_orders':0}
    keep=[c for c in ['signal_date','code','name','h_intent_profile','h_intent_v2_status','_net50','outcome_class','entry_amount_b','entry_ma60_dist_pct','score','prospective_label','auto_apply'] if c in cur.columns]; cur=cur[keep] if keep else cur
    cur.to_csv(out/'v49_76_h_high_dryup_prospective_candidates.csv',index=False,encoding='utf-8-sig'); (out/'v49_76_h_high_dryup_prospective_manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
    return {'manifest':manifest,'current':cur}


def preignition_casebook(miss_detail: pd.DataFrame, out: Path, top_n: int=80) -> dict[str,Any]:
    out=Path(out); out.mkdir(parents=True,exist_ok=True); z=miss_detail.copy() if isinstance(miss_detail,pd.DataFrame) else pd.DataFrame()
    if z.empty:
        casebook=pd.DataFrame(); manifest={'status':'NO_DATA','cases':0,'auto_apply':0,'paper_only':True}
    else:
        policy=z.get('miss_policy_class',pd.Series('',index=z.index)).astype(str)
        true=z[policy.eq('TRUE_PATTERN_MISS_WITHIN_CURRENT_POLICY')].copy()
        # Retrospective casebook score only. This score is never injected into LIVE/search.
        vol=_num(true,'entry_vol50_ratio'); loc=_num(true,'entry_close_loc_pct'); r1=_num(true,'entry_stock_ret_1d'); h20=_num(true,'entry_high20_dist_pct')
        score=pd.Series(0.0,index=true.index)
        score += np.clip((1.0-vol.fillna(1.0))/1.0,0,1)*35
        score += np.clip((35-loc.fillna(35))/35,0,1)*25
        score += np.clip((-r1.fillna(0))/8,0,1)*20
        score += np.clip((-h20.fillna(0))/35,0,1)*20
        true['preignition_research_score']=score.round(1)
        true['preignition_archetype']=np.select([
            vol.lt(.7)&loc.lt(25), vol.lt(.7), loc.lt(25)
        ],['LOW_VOL_DEEP_CLOSE','LOW_VOL_ONLY','DEEP_CLOSE_ONLY'],default='OTHER_TRUE_MISS')
        true['research_only']=1; true['auto_apply']=0
        casebook=true.sort_values(['preignition_research_score','signal_date'],ascending=[False,False]).head(int(top_n)).copy()
        manifest={'status':'CASEBOOK_READY' if len(casebook) else 'NO_TRUE_PATTERN_MISS','true_pattern_miss_rows':int(len(true)),'cases':int(len(casebook)),'top_n':int(top_n),'score_scope':'RETROSPECTIVE_CASE_REVIEW_ONLY','live_candidate_generator':False,'search_change':'NONE','auto_apply':0,'paper_only':True,'real_orders':0}
    keep=[c for c in ['signal_date','code','name','nearest_strategy','nearest_mode','nearest_distance','entry_vol50_ratio','entry_close_loc_pct','entry_stock_ret_1d','entry_high20_dist_pct','entry_amount_b','ret_next_close_pct','ret_high_5d_pct','preignition_research_score','preignition_archetype','research_only','auto_apply'] if c in casebook.columns]; casebook=casebook[keep] if keep else casebook
    casebook.to_csv(out/'v49_76_preignition_casebook.csv',index=False,encoding='utf-8-sig'); (out/'v49_76_preignition_manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
    return {'manifest':manifest,'casebook':casebook}


def practical_readiness(perf: pd.DataFrame, a_manifest: dict, h_manifest: dict, out: Path) -> pd.DataFrame:
    out=Path(out); rows=[]; p=perf.copy() if isinstance(perf,pd.DataFrame) else pd.DataFrame()
    for mode in ['LP','SLOCK','G','IT','I','L','S','B1','B2','C','H','A']:
        q=p[(p.get('strategy',pd.Series('',index=p.index)).astype(str).eq(mode))] if not p.empty else pd.DataFrame(); r=q.iloc[0] if len(q) else {}
        net=float(r.get('oos_net50_mean_pct',np.nan)) if hasattr(r,'get') else np.nan; n=int(r.get('oos_n',0) or 0) if hasattr(r,'get') else 0
        if mode=='LP' and n>=100 and pd.notna(net) and net>0: tier='CORE_PAPER'
        elif mode=='SLOCK' and n>0 and pd.notna(net) and net>0: tier='SMALL_N_AUX_PAPER'
        elif mode=='G' and n>=30 and pd.notna(net) and net>0: tier='WATCH_PAPER'
        elif mode=='A' and str(a_manifest.get('status'))=='LOCKED_TEST_PASS': tier='FIXED_HYPOTHESIS_PAPER_ONLY'
        elif mode=='H' and str(h_manifest.get('status')) in ('READY_FOR_PROSPECTIVE_PAPER','NO_CURRENT_CANDIDATE'): tier='PROFILE_RESEARCH_ONLY'
        else: tier='RESEARCH_ONLY'
        rows.append({'strategy':mode,'oos_n':n,'oos_net50_mean_pct':net,'practical_tier':tier,'auto_apply':0,'real_orders':0})
    df=pd.DataFrame(rows); df.to_csv(out/'v49_76_practical_readiness.csv',index=False,encoding='utf-8-sig'); return df


def run_practical_audit(enriched: pd.DataFrame, perf: pd.DataFrame, insight_dir: Path, out: Path, *, min_validation_n:int=15,min_test_n:int=15,min_lift_pctp:float=.30) -> dict[str,Any]:
    out=Path(out); insight_dir=Path(insight_dir)
    a=a_ma60_fixed_hypothesis(enriched,out,min_validation_n,min_test_n,min_lift_pctp)
    try: h_detail=pd.read_csv(insight_dir/'v49_76_h_intent_v2_detail.csv',dtype={'code':str},low_memory=False)
    except Exception: h_detail=pd.DataFrame()
    _latest=_dates(enriched).dropna().max() if isinstance(enriched,pd.DataFrame) and not enriched.empty else pd.NaT
    h=h_high_dryup_prospective(h_detail,out,_latest)
    try: miss=pd.read_csv(insight_dir/'v49_76_immediate_strategic_miss_detail.csv',dtype={'code':str},low_memory=False)
    except Exception: miss=pd.DataFrame()
    pre=preignition_casebook(miss,out)
    readiness=practical_readiness(perf,a['manifest'],h['manifest'],out)
    manifest={'version':'v49.76','status':'FULL_VALID' if a['manifest'].get('status') not in ('NO_DATA','TRAIN_DIRECTION_NOT_CONFIRMED') else 'PARTIAL_VALID','lane':'PRACTICAL_PAPER_EXECUTION_RESEARCH_INTEGRATION','a_ma60_status':a['manifest'].get('status'),'h_high_dryup_status':h['manifest'].get('status'),'preignition_status':pre['manifest'].get('status'),'practical_tiers':readiness.set_index('strategy')['practical_tier'].to_dict() if len(readiness) else {},'search_change':'NONE','rank_change':'NONE','exit_change':'NONE','live_auto_change':'NONE','paper_only':True,'real_orders':0,'auto_apply':0}
    (out/'v49_76_practical_manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
    return {'manifest':manifest,'a':a,'h':h,'preignition':pre,'readiness':readiness}
