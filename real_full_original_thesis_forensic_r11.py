#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, glob, hashlib, json, math, re
from pathlib import Path
from typing import Any, Dict, List, Tuple
import numpy as np
import pandas as pd

RESEARCH_ID='REAL_FULL_ORIGINAL_THESIS_FORENSIC_R1_1'
REVISION='R1_1_MULTI_STRUCTURE_FORENSIC_20260922'
DISCOVERY_END=pd.Timestamp('2026-08-28')
HOLDOUT_START=pd.Timestamp('2026-09-01')
HOLDOUT_END=pd.Timestamp('2026-09-30')

# Fixed descriptive bins. These are NOT trading gates and must not be tuned on this sample.
MA224_BINS=[-5,5,15,30]


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
    return pd.read_csv(p,dtype={'code':str},low_memory=False)

def load_price(cache:Path,c:str,sd:pd.Timestamp,allow_refetch:bool)->pd.DataFrame:
    files=[]
    for pat in (f'{c}_*.pkl.gz',f'{c}*.pkl*'):
        files += glob.glob(str(cache/pat))
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
                pd.to_pickle({'frame':best.copy(),'source':source},cache/f'{c}_forensic_r11.pkl.gz',compression='gzip')
        except Exception: best=None
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
    for n in [5,20,60,112,224]:q[f'ma{n}']=q['close'].rolling(n,min_periods=n).mean()
    q['ret1']=q['close'].pct_change()*100
    q['range_pct']=(q['high']/q['low']-1)*100
    q.attrs['source']=source
    return q.reset_index(drop=True)

def med(s):
    x=pd.to_numeric(s,errors='coerce').dropna(); return float(x.median()) if len(x) else np.nan

def safe_ratio(a,b):
    a=num(a);b=num(b);return a/b if math.isfinite(a) and math.isfinite(b) and b!=0 else np.nan

def ma224_bucket(x):
    x=num(x)
    if not math.isfinite(x):return 'MA224_NA'
    if x<-5:return 'MA224_BELOW_LT_M5'
    if x<=5:return 'MA224_NEAR_PM5'
    if x<=15:return 'MA224_ABOVE_5_15'
    if x<=30:return 'MA224_ABOVE_15_30'
    return 'MA224_EXTENDED_GT30'

def infer_structure(fr:pd.DataFrame,sd:pd.Timestamp)->Dict[str,Any]:
    h=fr[fr.date.le(sd)].copy()
    if h.empty or not h.date.eq(sd).any():return {'forensic_status':'SIGNAL_DATE_MISSING'}
    cur=h[h.date.eq(sd)].iloc[-1]; entry=num(cur.close)
    p40=h.iloc[:-1].tail(40).copy(); p20=h.iloc[:-1].tail(20).copy(); p5=h.iloc[:-1].tail(5).copy(); p60=h.iloc[:-1].tail(60).copy(); p120=h.iloc[:-1].tail(120).copy(); p250=h.iloc[:-1].tail(250).copy()
    out={'forensic_status':'READY','forensic_entry_close':entry,'price_source':fr.attrs.get('source','UNKNOWN')}
    for n in [20,60,112,224]:
        mv=num(cur.get(f'ma{n}')); out[f'f_ma{n}']=mv; out[f'f_close_vs_ma{n}_pct']=(entry/mv-1)*100 if mv>0 else np.nan
    out['ma224_bucket']=ma224_bucket(out['f_close_vs_ma224_pct'])
    vals=[out.get(f'f_ma{n}') for n in [20,60,112,224] if math.isfinite(num(out.get(f'f_ma{n}'))) and num(out.get(f'f_ma{n}'))>0]
    out['f_ma_cluster_width_pct']=(max(vals)/min(vals)-1)*100 if len(vals)>=3 else np.nan
    out['tag_ma_compression']=int(math.isfinite(num(out['f_ma_cluster_width_pct'])) and out['f_ma_cluster_width_pct']<=12)

    # Fixed causal Wave1 proxy: lowest low in prior40 -> highest high after that low and before signal.
    out['wave1_ready']=0; out['wave1_low']=np.nan; out['wave1_high']=np.nan; out['wave1_pct']=np.nan; out['wave1_peak_days_ago']=np.nan
    if len(p40)>=10:
        lows=pd.to_numeric(p40.low,errors='coerce'); li=lows.idxmin() if lows.notna().any() else None
        if li is not None:
            after=p40.loc[li:]
            highs=pd.to_numeric(after.high,errors='coerce')
            if highs.notna().any():
                hi=highs.idxmax(); lo=num(p40.loc[li,'low']); high=num(p40.loc[hi,'high'])
                if lo>0 and high>=lo:
                    out['wave1_ready']=1; out['wave1_low']=lo; out['wave1_high']=high; out['wave1_pct']=(high/lo-1)*100
                    peak_date=pd.Timestamp(p40.loc[hi,'date']); out['wave1_peak_days_ago']=int((h.date>peak_date).sum()-1)
                    post_peak=h[(h.date.gt(peak_date)) & (h.date.le(sd))]
                    pb_low=med(post_peak.low) if False else (pd.to_numeric(post_peak.low,errors='coerce').min() if len(post_peak) else np.nan)
                    out['pullback_low']=pb_low
                    out['pullback_from_peak_pct']=(entry/high-1)*100 if high>0 else np.nan
                    out['pullback_depth_from_peak_low_pct']=(pb_low/high-1)*100 if math.isfinite(num(pb_low)) and high>0 else np.nan
                    out['pullback_days']=len(post_peak)
    # Accumulation / supply / liquidity
    base60=med(p60.amount); med20a=med(p20.amount); med5a=med(p5.amount); med20v=med(p20.volume); med5v=med(p5.volume)
    out['pre20_amount_vs_pre60']=safe_ratio(med20a,base60)
    out['last5_amount_vs_pre20']=safe_ratio(med5a,med20a); out['last5_volume_vs_pre20']=safe_ratio(med5v,med20v)
    ratios=pd.to_numeric(p20.amount,errors='coerce')/base60 if math.isfinite(num(base60)) and base60>0 else pd.Series(dtype=float)
    out['gradual_amount_days_1p2_2x']=int(((ratios>=1.2)&(ratios<2.0)).sum()) if len(ratios) else np.nan
    out['spike_amount_days_ge2x']=int((ratios>=2.0).sum()) if len(ratios) else np.nan
    down20=p20[pd.to_numeric(p20.ret1,errors='coerce').lt(0)]
    out['down_day_amount_vs_all20']=safe_ratio(med(down20.amount),med20a)
    out['tag_gradual_accumulation']=int((num(out['gradual_amount_days_1p2_2x'])>=3) and (num(out['spike_amount_days_ge2x'])<=3)) if math.isfinite(num(out['gradual_amount_days_1p2_2x'])) else 0
    out['tag_supply_drying']=int(math.isfinite(num(out['last5_amount_vs_pre20'])) and out['last5_amount_vs_pre20']<0.8 and (not math.isfinite(num(out['down_day_amount_vs_all20'])) or out['down_day_amount_vs_all20']<=1.0))
    out['tag_liquidity_retained']=int(math.isfinite(num(out['last5_amount_vs_pre20'])) and 0.35<=out['last5_amount_vs_pre20']<=1.2)

    # Volatility compression: recent 5 median range lower than prior20 median by >=25%.
    r5=med(p5.range_pct); r20=med(p20.range_pct); out['range5_vs_range20']=safe_ratio(r5,r20)
    out['tag_volatility_compression']=int(math.isfinite(num(out['range5_vs_range20'])) and out['range5_vs_range20']<=0.75)

    # Signal reacceleration: positive day + closes above prior 5d median/high-ish with non-collapsed amount.
    prev_close=num(h.iloc[-2].close) if len(h)>=2 else np.nan; sigret=(entry/prev_close-1)*100 if prev_close>0 else np.nan
    prior5_high=pd.to_numeric(p5.high,errors='coerce').max() if len(p5) else np.nan
    sig_amt=num(cur.amount); sig_amt_ratio=safe_ratio(sig_amt,med20a)
    out['signal_ret_pct']=sigret; out['signal_amount_vs_pre20']=sig_amt_ratio; out['signal_close_vs_prior5_high_pct']=(entry/prior5_high-1)*100 if math.isfinite(num(prior5_high)) and prior5_high>0 else np.nan
    out['tag_reacceleration']=int(math.isfinite(num(sigret)) and sigret>0 and math.isfinite(num(sig_amt_ratio)) and sig_amt_ratio>=0.8 and math.isfinite(num(prior5_high)) and entry>=prior5_high*0.98)

    # Strong/shallow/deep structure tags, purely descriptive.
    w=num(out['wave1_pct']); pb=num(out.get('pullback_from_peak_pct'))
    out['tag_strong_wave1']=int(math.isfinite(w) and w>=15)
    out['tag_shallow_pullback']=int(math.isfinite(pb) and -10<=pb<=0)
    out['tag_deep_pullback']=int(math.isfinite(pb) and pb<-15)
    out['tag_ma224_base_or_reclaim']=int(math.isfinite(num(out['f_close_vs_ma224_pct'])) and -10<=out['f_close_vs_ma224_pct']<=10)
    out['tag_overextended']=int((math.isfinite(num(out['f_close_vs_ma224_pct'])) and out['f_close_vs_ma224_pct']>30) or (math.isfinite(num(sigret)) and sigret>12))

    # KKI/history descriptors: rolling 5d +10% bursts and amount >=2x prior20 median, signal-date-causal.
    def kki_count(hist,days):
        x=hist.iloc[:-1].tail(days).copy()
        if len(x)<25:return (np.nan,np.nan)
        x['ret5']=x.close.pct_change(5)*100; burst=int((pd.to_numeric(x.ret5,errors='coerce')>=10).sum())
        amt=pd.to_numeric(x.amount,errors='coerce'); roll=amt.rolling(20,min_periods=20).median(); spikes=int((amt>=2*roll).sum())
        return burst,spikes
    b120,s120=kki_count(h,120); b250,s250=kki_count(h,250)
    out['kki_5d_plus10_count_120']=b120; out['kki_amount_spike2x_count_120']=s120; out['kki_5d_plus10_count_250']=b250; out['kki_amount_spike2x_count_250']=s250
    out['tag_prior_kki']=int((math.isfinite(num(b120)) and b120>=2) or (math.isfinite(num(s120)) and s120>=2))

    tags=[k[4:] for k,v in out.items() if k.startswith('tag_') and int(num(v) or 0)==1]
    out['structure_tags']='|'.join(sorted(tags)); out['structure_tag_count']=len(tags)
    if not tags: out['structure_tags']='UNCLASSIFIED'
    return out

def group_label(path):
    if path in {'FAST_SUCCESS','SHAKEOUT_THEN_GO','DELAYED_SWING'}:return 'SUCCESS'
    if path in {'EARLY_SPIKE_GIVEBACK','FAILURE','STRUCTURE_BREAK'}:return 'FAILURE'
    if path=='TIME_FAILURE':return 'TIME_FAILURE'
    return 'IMMATURE'

def compare_groups(df:pd.DataFrame,a:str,b:str,label_col='forensic_group')->pd.DataFrame:
    feats=['f_close_vs_ma224_pct','f_ma_cluster_width_pct','wave1_pct','pullback_from_peak_pct','pullback_days','pre20_amount_vs_pre60','last5_amount_vs_pre20','last5_volume_vs_pre20','down_day_amount_vs_all20','range5_vs_range20','signal_ret_pct','signal_amount_vs_pre20','kki_5d_plus10_count_120','kki_amount_spike2x_count_120']
    tagcols=[c for c in df.columns if c.startswith('tag_')]
    rows=[]
    A=df[df[label_col].eq(a)]; B=df[df[label_col].eq(b)]
    for f in feats:
        if f not in df:continue
        av=pd.to_numeric(A[f],errors='coerce'); bv=pd.to_numeric(B[f],errors='coerce')
        rows.append({'comparison':f'{a}_VS_{b}','feature':f,'kind':'NUMERIC','n_a':int(av.notna().sum()),'n_b':int(bv.notna().sum()),'a_median':av.median(),'b_median':bv.median(),'median_diff_a_minus_b':av.median()-bv.median()})
    for f in tagcols:
        av=pd.to_numeric(A[f],errors='coerce'); bv=pd.to_numeric(B[f],errors='coerce')
        rows.append({'comparison':f'{a}_VS_{b}','feature':f,'kind':'TAG_RATE','n_a':int(av.notna().sum()),'n_b':int(bv.notna().sum()),'a_rate':av.mean(),'b_rate':bv.mean(),'rate_diff_a_minus_b':av.mean()-bv.mean()})
    return pd.DataFrame(rows)

def combo_summary(df:pd.DataFrame)->pd.DataFrame:
    z=df[df.d20_complete.eq(1)].copy() if 'd20_complete' in df else df.copy()
    rows=[]
    for k,g in z.groupby('structure_tags',dropna=False):
        if len(g)<3:continue
        rows.append({'structure_tags':k,'events':len(g),'success_rate':g.forensic_group.eq('SUCCESS').mean(),'failure_rate':g.forensic_group.eq('FAILURE').mean(),'giveback_rate':g.path_class.eq('EARLY_SPIKE_GIVEBACK').mean(),'d20_close_median':pd.to_numeric(g.d20_close_ret_pct,errors='coerce').median(),'d20_mfe_median':pd.to_numeric(g.d20_mfe_pct,errors='coerce').median(),'d20_mae_median':pd.to_numeric(g.d20_mae_pct,errors='coerce').median()})
    return pd.DataFrame(rows).sort_values(['events','success_rate'],ascending=[False,False]) if rows else pd.DataFrame()

def matched_pairs(df:pd.DataFrame)->pd.DataFrame:
    # Same signal date whenever possible; pair SUCCESS to nearest FAILURE by MA224 distance + origin rank.
    z=df[df.forensic_group.isin(['SUCCESS','FAILURE'])].copy(); rows=[]; used=set()
    for dt,g in z.groupby('origin_date'):
        s=g[g.forensic_group.eq('SUCCESS')]; f=g[g.forensic_group.eq('FAILURE')]
        for _,r in s.iterrows():
            if f.empty:continue
            best=None;bestscore=1e18
            for j,q in f.iterrows():
                key=(dt,j)
                if key in used:continue
                dm=abs(num(r.get('f_close_vs_ma224_pct'))-num(q.get('f_close_vs_ma224_pct'))) if math.isfinite(num(r.get('f_close_vs_ma224_pct'))) and math.isfinite(num(q.get('f_close_vs_ma224_pct'))) else 50
                dr=abs(num(r.get('origin_rank'))-num(q.get('origin_rank'))) if math.isfinite(num(r.get('origin_rank'))) and math.isfinite(num(q.get('origin_rank'))) else 20
                sc=dm+0.25*dr
                if sc<bestscore:bestscore=sc;best=(j,q)
            if best:
                j,q=best;used.add((dt,j)); rows.append({'origin_date':dt,'success_code':r.code,'success_name':r.get('name',''),'failure_code':q.code,'failure_name':q.get('name',''),'match_distance':bestscore,'success_path':r.path_class,'failure_path':q.path_class,'success_tags':r.structure_tags,'failure_tags':q.structure_tags,'success_ma224_pct':r.get('f_close_vs_ma224_pct'),'failure_ma224_pct':q.get('f_close_vs_ma224_pct'),'success_wave1_pct':r.get('wave1_pct'),'failure_wave1_pct':q.get('wave1_pct'),'success_supply_ratio':r.get('last5_amount_vs_pre20'),'failure_supply_ratio':q.get('last5_amount_vs_pre20'),'success_d20':r.get('d20_close_ret_pct'),'failure_d20':q.get('d20_close_ret_pct')})
    return pd.DataFrame(rows)

def run(a):
    out=Path(a.output_dir);out.mkdir(parents=True,exist_ok=True)
    src=read_csv(a.event_master)
    if src.empty:raise SystemExit('R1_EVENT_MASTER_MISSING_OR_EMPTY')
    src['origin_date']=pd.to_datetime(src.origin_date,errors='coerce').dt.normalize(); src['code']=src.code.map(code)
    rows=[];missing=[]
    for _,r in src.iterrows():
        sd=pd.Timestamp(r.origin_date); fr=load_price(Path(a.price_cache_dir),r.code,sd,a.allow_price_refetch)
        d=r.to_dict(); d['origin_date']=sd.date().isoformat()
        if fr.empty: d.update({'forensic_status':'PRICE_MISSING','structure_tags':'UNCLASSIFIED','structure_tag_count':0});missing.append(f'{sd.date()}|{r.code}')
        else:d.update(infer_structure(fr,sd))
        d['forensic_group']=group_label(text(r.get('path_class'))); rows.append(d)
    z=pd.DataFrame(rows)
    z.to_csv(out/'forensic_event_master.csv',index=False,encoding='utf-8-sig')
    mature=z[pd.to_numeric(z.get('d20_complete'),errors='coerce').eq(1)].copy()
    compare_groups(mature,'SUCCESS','FAILURE').to_csv(out/'success_vs_failure_feature_comparison.csv',index=False,encoding='utf-8-sig')
    # Specific early spike giveback vs fast success comparison
    q=mature.copy();q['specific_group']=q.path_class
    compare_groups(q,'FAST_SUCCESS','EARLY_SPIKE_GIVEBACK','specific_group').to_csv(out/'fast_success_vs_early_giveback.csv',index=False,encoding='utf-8-sig')
    combo_summary(mature).to_csv(out/'multi_structure_combo_summary.csv',index=False,encoding='utf-8-sig')
    matched_pairs(mature).to_csv(out/'matched_success_failure_pairs.csv',index=False,encoding='utf-8-sig')
    # tag marginal summary
    tags=[c for c in z if c.startswith('tag_')]; tsum=[]
    for t in tags:
        g=mature[pd.to_numeric(mature[t],errors='coerce').eq(1)]
        tsum.append({'tag':t,'events':len(g),'success_rate':g.forensic_group.eq('SUCCESS').mean() if len(g) else np.nan,'failure_rate':g.forensic_group.eq('FAILURE').mean() if len(g) else np.nan,'giveback_rate':g.path_class.eq('EARLY_SPIKE_GIVEBACK').mean() if len(g) else np.nan,'d20_close_median':pd.to_numeric(g.get('d20_close_ret_pct'),errors='coerce').median() if len(g) else np.nan})
    pd.DataFrame(tsum).to_csv(out/'structure_tag_marginal_summary.csv',index=False,encoding='utf-8-sig')
    # unknown pattern candidates = unclassified by new tags OR weak/rare scanner taxonomy
    unknown=z[(z.structure_tags.eq('UNCLASSIFIED')) | z.get('origin_pattern_exact_combo',pd.Series('',index=z.index)).fillna('').astype(str).isin(['','UNCLASSIFIED'])].copy()
    unknown.to_csv(out/'unknown_pattern_candidates.csv',index=False,encoding='utf-8-sig')
    # holdout kept separate; no retuning
    z[z.origin_date.le(DISCOVERY_END.strftime('%Y-%m-%d'))].to_csv(out/'discovery_forensic.csv',index=False,encoding='utf-8-sig')
    z[(z.origin_date.ge(HOLDOUT_START.strftime('%Y-%m-%d'))) & (z.origin_date.le(HOLDOUT_END.strftime('%Y-%m-%d')))].to_csv(out/'holdout_forensic.csv',index=False,encoding='utf-8-sig')
    design={'revision':REVISION,'discovery_end':'2026-08-28','ma224_buckets':['<-5','-5..+5','+5..+15','+15..+30','>+30'],'wave1_lookback':40,'supply_last5_vs_pre20':0.8,'liquidity_retained_range':[0.35,1.2],'volatility_compression_ratio':0.75,'strong_wave1_pct':15,'shallow_pullback_pct':[-10,0],'deep_pullback_lt_pct':-15,'kki_5d_plus10_min_count':2,'research_only':True}
    meta={'research_id':RESEARCH_ID,'revision':REVISION,'status':'PASS','events':len(z),'d20_mature':len(mature),'missing_price':len(missing),'discovery_end':'2026-08-28','holdout_start':'2026-09-01','research_only':True,'production_eligible':False,'selection_logic_changed':False,'score_rank_changed':False,'order_logic_changed':False,'same_sample_retuning':False,'design_hash':sha(design),'design':design}
    (out/'forensic_meta.json').write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding='utf-8')
    rep=[f'🧪 [{RESEARCH_ID}]',f'status=PASS revision={REVISION}',f'events={len(z)} d20_mature={len(mature)} missing_price={len(missing)}',f'SUCCESS={int(mature.forensic_group.eq("SUCCESS").sum())} FAILURE={int(mature.forensic_group.eq("FAILURE").sum())} TIME_FAILURE={int(mature.forensic_group.eq("TIME_FAILURE").sum())}',f'new_structure_tags={len(tags)} combos_n3plus={len(combo_summary(mature))}',f'unknown_candidates={len(unknown)}','MA224 is one axis only; no MA224-only filtering is applied.','All structure features use bars <= signal date. Future bars are inherited outcomes only.','research_only=1 production changes=0 same_sample_retuning=0']
    (out/'forensic_report.txt').write_text('\n'.join(rep)+'\n',encoding='utf-8');print('\n'.join(rep));return 0

def self_test():
    dates=pd.bdate_range('2025-08-01',periods=300); close=np.linspace(80,100,300); close[-40:-30]=np.linspace(82,100,10); close[-30:]=np.linspace(98,94,30)
    fr=pd.DataFrame({'date':dates,'open':close,'high':close*1.02,'low':close*.98,'close':close,'volume':1000.0,'amount':close*1000})
    for n in [5,20,60,112,224]:fr[f'ma{n}']=fr.close.rolling(n,min_periods=n).mean()
    fr['ret1']=fr.close.pct_change()*100;fr['range_pct']=(fr.high/fr.low-1)*100
    x=infer_structure(fr,dates[-1]); assert x['forensic_status']=='READY'; assert 'ma224_bucket' in x and 'structure_tags' in x
    t=pd.DataFrame([{'forensic_group':'SUCCESS','path_class':'FAST_SUCCESS','d20_complete':1,'d20_close_ret_pct':10,'d20_mfe_pct':15,'d20_mae_pct':-2,'tag_prior_kki':1,'f_close_vs_ma224_pct':2},{'forensic_group':'FAILURE','path_class':'EARLY_SPIKE_GIVEBACK','d20_complete':1,'d20_close_ret_pct':-10,'d20_mfe_pct':12,'d20_mae_pct':-15,'tag_prior_kki':0,'f_close_vs_ma224_pct':30}])
    c=compare_groups(t,'SUCCESS','FAILURE');assert len(c)>0
    print('REAL_FULL_ORIGINAL_THESIS_FORENSIC_R1_1_SELF_TEST PASS');return 0

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--event-master',default='source_r1/reports/real_full_original_thesis_r1/real_full_original_thesis_event_master.csv');ap.add_argument('--price-cache-dir',default='reports/.cache/real_full_original_thesis_forensic_r11/price_history');ap.add_argument('--output-dir',default='reports/real_full_original_thesis_forensic_r11');ap.add_argument('--allow-price-refetch',action='store_true');ap.add_argument('--self-test',action='store_true');a=ap.parse_args();return self_test() if a.self_test else run(a)
if __name__=='__main__':raise SystemExit(main())
