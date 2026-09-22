#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, glob, hashlib, json, math, re
from pathlib import Path
from typing import Any, Dict, List
import numpy as np
import pandas as pd

RESEARCH_ID='REAL_FULL_ORIGINAL_THESIS_FORENSIC_R1_2_1'
REVISION='R1_2_1_PAIR_REPORT_HOLDOUT_VISIBILITY_20260922'
DISCOVERY_END=pd.Timestamp('2026-08-28')
HOLDOUT_START=pd.Timestamp('2026-09-01')
HOLDOUT_END=pd.Timestamp('2026-09-30')

# Frozen descriptive rules. They are not trading gates and must not be retuned on this sample.
MATCH_MAX_DISTANCE=15.0
MATCH_MAX_MA224_DIFF=12.0
MATCH_MAX_WAVE1_DIFF=20.0
EARLY_RULES={
    'd1_price_weak_pct':-3.0,
    'd3_liquidity_collapse_ratio':0.60,
    'd3_selling_pressure_ratio':1.20,
    'd3_early_giveback_mfe_pct':5.0,
    'd3_rebound_amount_ratio':0.80,
}

def num(v:Any)->float:
    try:
        x=float(v); return x if math.isfinite(x) else float('nan')
    except Exception:return float('nan')

def text(v:Any)->str:
    try:
        if v is None or pd.isna(v): return ''
    except Exception: pass
    s=str(v).strip(); return '' if s.lower() in {'','nan','none','nat'} else s

def code(v:Any)->str:
    s=re.sub(r'\D','',str(v or '')); return s[-6:].zfill(6) if s else ''

def sha(x:Any)->str:
    return hashlib.sha256(json.dumps(x,ensure_ascii=False,sort_keys=True,default=str,separators=(',',':')).encode()).hexdigest()

def read_csv(p)->pd.DataFrame:
    p=Path(p)
    if not p.exists() or p.stat().st_size==0:return pd.DataFrame()
    try:return pd.read_csv(p,dtype={'code':str},low_memory=False)
    except pd.errors.EmptyDataError:return pd.DataFrame()

def load_price(cache:Path,c:str,sd:pd.Timestamp,allow_refetch:bool)->pd.DataFrame:
    files=[]
    for pat in (f'{c}_*.pkl.gz',f'{c}*.pkl*'): files += glob.glob(str(cache/pat))
    best=None; source='EXISTING_CACHE'
    for f in sorted(set(files)):
        try:
            o=pd.read_pickle(f); fr=o.get('frame') if isinstance(o,dict) else o
            if isinstance(fr,pd.DataFrame) and not fr.empty and (best is None or len(fr)>len(best)):best=fr.copy()
        except Exception:pass
    if best is None and allow_refetch:
        try:
            import FinanceDataReader as fdr
            start=(sd-pd.Timedelta(days=650)).strftime('%Y-%m-%d')
            end=(pd.Timestamp.now(tz='Asia/Seoul').normalize()+pd.Timedelta(days=1)).strftime('%Y-%m-%d')
            fr=fdr.DataReader(c,start,end)
            if isinstance(fr,pd.DataFrame) and not fr.empty:
                best=fr.copy(); source='FDR_HISTORICAL_REFETCH_RESEARCH_ONLY'
                cache.mkdir(parents=True,exist_ok=True)
                pd.to_pickle({'frame':best.copy(),'source':source},cache/f'{c}_forensic_r12.pkl.gz',compression='gzip')
        except Exception:best=None
    if best is None:return pd.DataFrame()
    q=best.copy()
    if 'Date' in q:q['date']=pd.to_datetime(q['Date'],errors='coerce')
    elif 'date' in q:q['date']=pd.to_datetime(q['date'],errors='coerce')
    else:q['date']=pd.to_datetime(q.index,errors='coerce')
    ren={}
    for col in q.columns:
        lc=str(col).lower()
        if lc in {'open','high','low','close','volume'}:ren[col]=lc
        elif lc in {'amount','value','tradingvalue','trading_value','거래대금'}:ren[col]='amount'
    q=q.rename(columns=ren)
    q=q[[x for x in ['date','open','high','low','close','volume','amount'] if x in q]].dropna(subset=['date'])
    q['date']=pd.to_datetime(q['date'],errors='coerce').dt.normalize(); q=q.sort_values('date').drop_duplicates('date',keep='last')
    for x in ['open','high','low','close','volume','amount']:
        if x in q:q[x]=pd.to_numeric(q[x],errors='coerce')
    if 'amount' not in q or not q['amount'].notna().any():q['amount']=q['close']*q['volume']
    q['ma20']=q['close'].rolling(20,min_periods=20).mean()
    q.attrs['source']=source
    return q.reset_index(drop=True)

def taxonomy_state(r:pd.Series)->str:
    scanner=text(r.get('origin_search_pattern'))
    canonical=text(r.get('origin_canonical_pattern'))
    exact=text(r.get('origin_pattern_exact_combo')).upper()
    raw=text(r.get('structure_tags'))
    raw_known=bool(raw and raw!='UNCLASSIFIED')
    scanner_known=bool(scanner or canonical)
    exact_known=bool(exact and exact not in {'UNCLASSIFIED','UNKNOWN','NONE','NA','N/A'})
    if scanner_known and raw_known and exact_known:return 'KNOWN_SCANNER_EXACT_AND_RAW'
    if scanner_known and raw_known and not exact_known:return 'SCANNER_KNOWN_EXACT_GAP_RAW_KNOWN'
    if scanner_known and not raw_known:return 'SCANNER_ONLY_RAW_UNCLASSIFIED'
    if (not scanner_known) and raw_known:return 'RAW_STRUCTURE_ONLY_NO_SCANNER_LABEL'
    return 'TRUE_UNKNOWN_NO_SCANNER_NO_RAW_STRUCTURE'

def build_taxonomy(df:pd.DataFrame)->pd.DataFrame:
    z=df.copy(); z['taxonomy_state']=z.apply(taxonomy_state,axis=1)
    z['taxonomy_gap_candidate']=z['taxonomy_state'].isin(['SCANNER_KNOWN_EXACT_GAP_RAW_KNOWN','RAW_STRUCTURE_ONLY_NO_SCANNER_LABEL']).astype(int)
    z['true_unknown_candidate']=z['taxonomy_state'].eq('TRUE_UNKNOWN_NO_SCANNER_NO_RAW_STRUCTURE').astype(int)
    return z

def strict_pairs(df:pd.DataFrame)->pd.DataFrame:
    z=df[df.forensic_group.isin(['SUCCESS','FAILURE'])].copy(); rows=[]; used=set()
    for dt,g in z.groupby('origin_date'):
        s=g[g.forensic_group.eq('SUCCESS')]; f=g[g.forensic_group.eq('FAILURE')]
        for si,r in s.iterrows():
            cand=[]
            for fi,q in f.iterrows():
                if (dt,fi) in used:continue
                ma1=num(r.get('f_close_vs_ma224_pct')); ma2=num(q.get('f_close_vs_ma224_pct'))
                w1=num(r.get('wave1_pct')); w2=num(q.get('wave1_pct'))
                rk1=num(r.get('origin_rank')); rk2=num(q.get('origin_rank'))
                if not (math.isfinite(ma1) and math.isfinite(ma2)):continue
                dma=abs(ma1-ma2)
                if dma>MATCH_MAX_MA224_DIFF:continue
                dw=abs(w1-w2) if math.isfinite(w1) and math.isfinite(w2) else 10.0
                if math.isfinite(w1) and math.isfinite(w2) and dw>MATCH_MAX_WAVE1_DIFF:continue
                dr=abs(rk1-rk2) if math.isfinite(rk1) and math.isfinite(rk2) else 10.0
                score=dma+0.25*dw+0.20*dr
                if score<=MATCH_MAX_DISTANCE:cand.append((score,fi,q,dma,dw,dr))
            if not cand:continue
            score,fi,q,dma,dw,dr=sorted(cand,key=lambda x:x[0])[0]; used.add((dt,fi))
            rows.append({
                'origin_date':str(dt),'success_code':r.code,'success_name':r.get('name',''),'failure_code':q.code,'failure_name':q.get('name',''),
                'match_distance':score,'ma224_diff_abs':dma,'wave1_diff_abs':dw,'rank_diff_abs':dr,
                'success_path':r.path_class,'failure_path':q.path_class,'success_tags':r.structure_tags,'failure_tags':q.structure_tags,
                'success_ma224_pct':num(r.get('f_close_vs_ma224_pct')),'failure_ma224_pct':num(q.get('f_close_vs_ma224_pct')),'success_wave1_pct':num(r.get('wave1_pct')),'failure_wave1_pct':num(q.get('wave1_pct')),
                'success_last5_amount_vs_pre20':r.get('last5_amount_vs_pre20'),'failure_last5_amount_vs_pre20':q.get('last5_amount_vs_pre20'),
                'success_ma_cluster_width_pct':r.get('f_ma_cluster_width_pct'),'failure_ma_cluster_width_pct':q.get('f_ma_cluster_width_pct'),
                'success_d20':r.get('d20_close_ret_pct'),'failure_d20':q.get('d20_close_ret_pct')})
    return pd.DataFrame(rows)

def early_warning(fr:pd.DataFrame,sd:pd.Timestamp,r:pd.Series)->Dict[str,Any]:
    out={'early_warning_status':'PRICE_MISSING'}
    if fr.empty:return out
    hist=fr[fr.date.le(sd)].copy(); fut=fr[fr.date.gt(sd)].sort_values('date').head(3).copy()
    if hist.empty or len(fut)<1:return {'early_warning_status':'IMMATURE_D1'}
    sig=hist[hist.date.eq(sd)]
    if sig.empty:return {'early_warning_status':'SIGNAL_DATE_MISSING'}
    entry=num(sig.iloc[-1].close); pre20=hist.iloc[:-1].tail(20); pre20_amt=pd.to_numeric(pre20.amount,errors='coerce').median() if len(pre20) else np.nan
    d1=fut.iloc[0]; d1_close=(num(d1.close)/entry-1)*100 if entry>0 else np.nan
    out={'early_warning_status':'D1_READY','d1_monitor_close_ret_pct':d1_close,
         'ew_d1_price_weak':int(math.isfinite(d1_close) and d1_close<=EARLY_RULES['d1_price_weak_pct'])}
    if len(fut)<3:
        out['d3_monitor_complete']=0; return out
    out['early_warning_status']='D3_READY';out['d3_monitor_complete']=1
    p=fut.iloc[:3].copy(); last=p.iloc[-1]
    amounts=pd.to_numeric(p.amount,errors='coerce'); out['d3_amount_vs_pre20']=float(amounts.median()/pre20_amt) if math.isfinite(num(pre20_amt)) and pre20_amt>0 else np.nan
    down=p[p.close<p.open]; down_amt=pd.to_numeric(down.amount,errors='coerce').median() if len(down) else np.nan
    out['d3_down_amount_vs_pre20']=float(down_amt/pre20_amt) if math.isfinite(num(down_amt)) and math.isfinite(num(pre20_amt)) and pre20_amt>0 else np.nan
    d3close=(num(last.close)/entry-1)*100 if entry>0 else np.nan; hi=pd.to_numeric(p.high,errors='coerce').max(); lo=pd.to_numeric(p.low,errors='coerce').min()
    out['d3_monitor_close_ret_pct']=d3close;out['d3_monitor_mfe_pct']=(hi/entry-1)*100 if entry>0 and math.isfinite(num(hi)) else np.nan;out['d3_monitor_mae_pct']=(lo/entry-1)*100 if entry>0 and math.isfinite(num(lo)) else np.nan
    ma20=num(last.get('ma20')); out['d3_close_vs_ma20_pct']=(num(last.close)/ma20-1)*100 if ma20>0 else np.nan
    pb=num(r.get('pullback_low')); out['d3_pb_low_breached']=int(math.isfinite(pb) and math.isfinite(num(lo)) and lo<pb) if math.isfinite(pb) else np.nan
    out['ew_d3_liquidity_collapse']=int(math.isfinite(num(out['d3_amount_vs_pre20'])) and out['d3_amount_vs_pre20']<EARLY_RULES['d3_liquidity_collapse_ratio'])
    out['ew_d3_selling_pressure']=int(math.isfinite(num(out['d3_down_amount_vs_pre20'])) and out['d3_down_amount_vs_pre20']>EARLY_RULES['d3_selling_pressure_ratio'])
    out['ew_d3_below_ma20']=int(math.isfinite(num(out['d3_close_vs_ma20_pct'])) and out['d3_close_vs_ma20_pct']<0)
    out['ew_d3_early_giveback']=int(math.isfinite(num(out['d3_monitor_mfe_pct'])) and out['d3_monitor_mfe_pct']>=EARLY_RULES['d3_early_giveback_mfe_pct'] and math.isfinite(d3close) and d3close<=0)
    out['ew_d3_low_volume_rebound']=int(math.isfinite(d3close) and d3close>0 and math.isfinite(num(out['d3_amount_vs_pre20'])) and out['d3_amount_vs_pre20']<EARLY_RULES['d3_rebound_amount_ratio'])
    warning_cols=['ew_d1_price_weak','ew_d3_liquidity_collapse','ew_d3_selling_pressure','ew_d3_below_ma20','ew_d3_early_giveback']
    out['early_warning_count']=sum(int(num(out.get(c)) or 0) for c in warning_cols)
    tags=[c.replace('ew_','') for c in warning_cols if int(num(out.get(c)) or 0)==1]
    out['early_warning_tags']='|'.join(tags) if tags else 'NONE'
    return out

def warning_occurrence_summary(df:pd.DataFrame)->pd.DataFrame:
    rows=[]
    cols=['ew_d1_price_weak','ew_d3_liquidity_collapse','ew_d3_selling_pressure','ew_d3_below_ma20','ew_d3_early_giveback','ew_d3_low_volume_rebound']
    for c in cols:
        if c not in df: continue
        s=pd.to_numeric(df[c],errors='coerce')
        rows.append({'warning':c,'events_total':len(df),'ready_n':int(s.notna().sum()),'flagged_n':int(s.eq(1).sum()),'flag_rate':float(s.eq(1).mean()) if len(s) else np.nan,'outcome_maturity':'MONITOR_ONLY_NO_D20_REQUIRED'})
    return pd.DataFrame(rows)

def warning_summary(df:pd.DataFrame)->pd.DataFrame:
    rows=[]; mature=df[pd.to_numeric(df.get('d20_complete'),errors='coerce').eq(1)].copy()
    cols=['ew_d1_price_weak','ew_d3_liquidity_collapse','ew_d3_selling_pressure','ew_d3_below_ma20','ew_d3_early_giveback','ew_d3_low_volume_rebound']
    for c in cols:
        if c not in mature:continue
        g=mature[pd.to_numeric(mature[c],errors='coerce').eq(1)]
        rows.append({'warning':c,'events':len(g),'success_rate':g.forensic_group.eq('SUCCESS').mean() if len(g) else np.nan,'failure_rate':g.forensic_group.eq('FAILURE').mean() if len(g) else np.nan,'early_giveback_rate':g.path_class.eq('EARLY_SPIKE_GIVEBACK').mean() if len(g) else np.nan,'d20_close_median':pd.to_numeric(g.get('d20_close_ret_pct'),errors='coerce').median() if len(g) else np.nan})
    return pd.DataFrame(rows)

def tag_interaction_summary(df:pd.DataFrame)->pd.DataFrame:
    mature=df[pd.to_numeric(df.get('d20_complete'),errors='coerce').eq(1)].copy(); tags=[c for c in mature.columns if c.startswith('tag_')]
    rows=[]
    for i,a in enumerate(tags):
        for b in tags[i+1:]:
            g=mature[pd.to_numeric(mature[a],errors='coerce').eq(1) & pd.to_numeric(mature[b],errors='coerce').eq(1)]
            if len(g)<5:continue
            rows.append({'tag_a':a,'tag_b':b,'events':len(g),'success_rate':g.forensic_group.eq('SUCCESS').mean(),'failure_rate':g.forensic_group.eq('FAILURE').mean(),'giveback_rate':g.path_class.eq('EARLY_SPIKE_GIVEBACK').mean(),'d20_close_median':pd.to_numeric(g.d20_close_ret_pct,errors='coerce').median()})
    return pd.DataFrame(rows).sort_values(['events','success_rate'],ascending=[False,False]) if rows else pd.DataFrame()

def run(a):
    out=Path(a.output_dir);out.mkdir(parents=True,exist_ok=True)
    src=read_csv(a.r11_master)
    if src.empty:raise SystemExit('R11_MASTER_MISSING_OR_EMPTY')
    src['origin_date']=pd.to_datetime(src.origin_date,errors='coerce').dt.normalize();src['code']=src.code.map(code)
    src=build_taxonomy(src)
    rows=[];missing=[]
    cache=Path(a.price_cache_dir)
    for _,r in src.iterrows():
        sd=pd.Timestamp(r.origin_date);fr=load_price(cache,r.code,sd,a.allow_price_refetch);d=r.to_dict();d['origin_date']=sd.date().isoformat()
        if fr.empty:
            missing.append(f'{sd.date()}|{r.code}');d['early_warning_status']='PRICE_MISSING'
        else:d.update(early_warning(fr,sd,r))
        rows.append(d)
    z=pd.DataFrame(rows)
    z.to_csv(out/'forensic_r12_event_master.csv',index=False,encoding='utf-8-sig')
    disc=z[pd.to_datetime(z.origin_date).le(DISCOVERY_END)].copy(); hold=z[(pd.to_datetime(z.origin_date).ge(HOLDOUT_START))&(pd.to_datetime(z.origin_date).le(HOLDOUT_END))].copy()
    disc.to_csv(out/'discovery_r12.csv',index=False,encoding='utf-8-sig');hold.to_csv(out/'holdout_r12.csv',index=False,encoding='utf-8-sig')
    tax=z.groupby('taxonomy_state',dropna=False).size().reset_index(name='events');tax.to_csv(out/'pattern_taxonomy_state_summary.csv',index=False,encoding='utf-8-sig')
    z[z.taxonomy_gap_candidate.eq(1)].to_csv(out/'taxonomy_gap_candidates.csv',index=False,encoding='utf-8-sig')
    z[z.true_unknown_candidate.eq(1)].to_csv(out/'true_unknown_candidates.csv',index=False,encoding='utf-8-sig')
    pairs=strict_pairs(disc[pd.to_numeric(disc.get('d20_complete'),errors='coerce').eq(1)])
    if len(pairs):
        pairs['pair_report_ma224_diff_check']=(pd.to_numeric(pairs['success_ma224_pct'],errors='coerce')-pd.to_numeric(pairs['failure_ma224_pct'],errors='coerce')).abs()
        pairs['pair_report_wave1_diff_check']=(pd.to_numeric(pairs['success_wave1_pct'],errors='coerce')-pd.to_numeric(pairs['failure_wave1_pct'],errors='coerce')).abs()
        pairs['pair_report_integrity_pass']=(((pairs['pair_report_ma224_diff_check']-pd.to_numeric(pairs['ma224_diff_abs'],errors='coerce')).abs()<1e-9) & ((pairs['pair_report_wave1_diff_check']-pd.to_numeric(pairs['wave1_diff_abs'],errors='coerce')).abs()<1e-9)).astype(int)
    pairs.to_csv(out/'strict_matched_success_failure_pairs.csv',index=False,encoding='utf-8-sig')
    pd.DataFrame([{'pairs':len(pairs),'integrity_pass':int(pairs.get('pair_report_integrity_pass',pd.Series(dtype=int)).sum()) if len(pairs) else 0,'integrity_fail':int((pairs.get('pair_report_integrity_pass',pd.Series(dtype=int))==0).sum()) if len(pairs) else 0}]).to_csv(out/'strict_pair_reporting_integrity_audit.csv',index=False,encoding='utf-8-sig')
    warning_summary(disc).to_csv(out/'discovery_early_warning_summary.csv',index=False,encoding='utf-8-sig')
    warning_summary(hold).to_csv(out/'holdout_early_warning_summary.csv',index=False,encoding='utf-8-sig')
    warning_occurrence_summary(hold).to_csv(out/'holdout_warning_occurrence_monitor_only.csv',index=False,encoding='utf-8-sig')
    tag_interaction_summary(disc).to_csv(out/'pairwise_structure_interactions.csv',index=False,encoding='utf-8-sig')
    # fixed warning-count lens; descriptive only
    for cohort_name,g in [('DISCOVERY',disc),('HOLDOUT',hold)]:
        rows2=[]
        for k,h in g.groupby(pd.to_numeric(g.get('early_warning_count'),errors='coerce').fillna(-1)):
            mature=h[pd.to_numeric(h.get('d20_complete'),errors='coerce').eq(1)]
            rows2.append({'cohort':cohort_name,'warning_count':int(k),'events':len(h),'d20_mature':len(mature),'success_rate':mature.forensic_group.eq('SUCCESS').mean() if len(mature) else np.nan,'failure_rate':mature.forensic_group.eq('FAILURE').mean() if len(mature) else np.nan,'giveback_rate':mature.path_class.eq('EARLY_SPIKE_GIVEBACK').mean() if len(mature) else np.nan,'d20_close_median':pd.to_numeric(mature.get('d20_close_ret_pct'),errors='coerce').median() if len(mature) else np.nan})
        pd.DataFrame(rows2).to_csv(out/f'{cohort_name.lower()}_warning_count_lens.csv',index=False,encoding='utf-8-sig')
    design={'revision':REVISION,'discovery_end':'2026-08-28','holdout_start':'2026-09-01','match_max_distance':MATCH_MAX_DISTANCE,'match_max_ma224_diff':MATCH_MAX_MA224_DIFF,'match_max_wave1_diff':MATCH_MAX_WAVE1_DIFF,'early_rules':EARLY_RULES,'taxonomy_states':['KNOWN_SCANNER_EXACT_AND_RAW','SCANNER_KNOWN_EXACT_GAP_RAW_KNOWN','SCANNER_ONLY_RAW_UNCLASSIFIED','RAW_STRUCTURE_ONLY_NO_SCANNER_LABEL','TRUE_UNKNOWN_NO_SCANNER_NO_RAW_STRUCTURE'],'research_only':True,'post_signal_monitor_not_entry_predictor':True}
    meta={'research_id':RESEARCH_ID,'revision':REVISION,'status':'PASS','events':len(z),'discovery_events':len(disc),'holdout_events':len(hold),'strict_pairs':len(pairs),'taxonomy_gap_candidates':int(z.taxonomy_gap_candidate.sum()),'true_unknown_candidates':int(z.true_unknown_candidate.sum()),'missing_price':len(missing),'research_only':True,'production_eligible':False,'selection_logic_changed':False,'score_rank_changed':False,'order_logic_changed':False,'same_sample_retuning':False,'post_signal_monitor_only':True,'pair_reporting_bug_fixed':True,'holdout_monitor_visibility_without_d20':True,'design_hash':sha(design),'design':design}
    (out/'forensic_r12_meta.json').write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding='utf-8')
    rep=[f'🧪 [{RESEARCH_ID}]',f'status=PASS revision={REVISION}',f'events={len(z)} discovery={len(disc)} holdout={len(hold)} missing_price={len(missing)}',f'taxonomy_gap={int(z.taxonomy_gap_candidate.sum())} true_unknown={int(z.true_unknown_candidate.sum())}',f'strict_matched_pairs={len(pairs)} max_distance={MATCH_MAX_DISTANCE}',f'post_signal_early_warning=D1/D3 MONITOR_ONLY','MA224 remains one structure axis; no MA224-only filtering.','research_only=1 production changes=0 same_sample_retuning=0']
    (out/'forensic_r12_report.txt').write_text('\n'.join(rep)+'\n',encoding='utf-8');print('\n'.join(rep));return 0

def self_test():
    t=pd.DataFrame([{'origin_search_pattern':'P','origin_canonical_pattern':'C','origin_pattern_exact_combo':'UNCLASSIFIED','structure_tags':'ma_compression|shallow_pullback'},{'origin_search_pattern':'','origin_canonical_pattern':'','origin_pattern_exact_combo':'','structure_tags':'UNCLASSIFIED'}])
    q=build_taxonomy(t);assert q.iloc[0].taxonomy_state=='SCANNER_KNOWN_EXACT_GAP_RAW_KNOWN';assert q.iloc[1].true_unknown_candidate==1
    dates=pd.bdate_range('2026-01-01',periods=180);close=np.linspace(90,100,180);fr=pd.DataFrame({'date':dates,'open':close,'high':close*1.01,'low':close*.99,'close':close,'volume':1000.,'amount':close*1000});fr['ma20']=fr.close.rolling(20,min_periods=20).mean();sd=dates[-4];r=pd.Series({'pullback_low':90});e=early_warning(fr,sd,r);assert e['early_warning_status']=='D3_READY';assert 'early_warning_count' in e
    print('REAL_FULL_ORIGINAL_THESIS_FORENSIC_R1_2_1_SELF_TEST PASS');return 0

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--r11-master',default='source_r11/reports/real_full_original_thesis_forensic_r11/forensic_event_master.csv');ap.add_argument('--price-cache-dir',default='reports/.cache/real_full_original_thesis_forensic_r12/price_history');ap.add_argument('--output-dir',default='reports/real_full_original_thesis_forensic_r12');ap.add_argument('--allow-price-refetch',action='store_true');ap.add_argument('--self-test',action='store_true');a=ap.parse_args();return self_test() if a.self_test else run(a)
if __name__=='__main__':raise SystemExit(main())
