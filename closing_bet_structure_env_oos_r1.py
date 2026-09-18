#!/usr/bin/env python3
from __future__ import annotations
import argparse,json,math,re
from pathlib import Path
from typing import Any
import numpy as np
import pandas as pd

REVISION='CLOSEBET_STRUCTURE_ENV_OOS_R1_20260918'
FEATURES=['ma_cluster_width_pct','ma_cluster_compression_5d_pctp','ma_cluster_compression_10d_pctp','price_to_ma224_abs_pct','vol_med5_vs_prev20','amount_med5_vs_prev20','down_volume_share10','volume_cv10','atr5_vs_atr20','close_ret10_pct']
MACRO={'USDKRW':'KRW=X','KOSPI':'^KS11','KOSDAQ':'^KQ11','NASDAQ':'^IXIC','SOX':'^SOX','VIX':'^VIX','US10Y':'^TNX','WTI':'CL=F','DXY':'DX-Y.NYB'}

def norm_code(v:Any)->str:
    s=str(v or '').strip().upper()
    if s.endswith('.0') and s[:-2].isdigit(): s=s[:-2]
    for suf in ('.KS','.KQ','.KRX'):
        if s.endswith(suf): s=s[:-len(suf)]
    s=''.join(ch for ch in s if ch.isalnum())
    if len(s)==7 and s.startswith('A'): s=s[1:]
    return s.zfill(6) if s.isdigit() and len(s)<=6 else (s[-6:] if len(s)>=6 else s)

def read_csv(p:Path)->pd.DataFrame:
    if not p or not p.exists() or p.stat().st_size==0:return pd.DataFrame()
    for enc in ('utf-8-sig','utf-8','cp949'):
        try:return pd.read_csv(p,low_memory=False,encoding=enc)
        except Exception:pass
    return pd.DataFrame()

def find(root:Path,name:str):
    h=list(root.rglob(name)); h.sort(key=lambda p:(0 if 'reports' in p.parts else 1,len(p.parts),str(p)))
    return h[0] if h else None

def pick(df,names):
    mp={str(c).lower().replace(' ','').replace('_',''):c for c in df.columns}
    for n in names:
        k=n.lower().replace(' ','').replace('_','')
        if k in mp:return mp[k]
    return None

def prep_hist(df):
    cc=pick(df,['code','Code','종목코드']); dc=pick(df,['date','Date','날짜','일자']); oc=pick(df,['Open','시가']); hc=pick(df,['High','고가']); lc=pick(df,['Low','저가']); cl=pick(df,['Close','종가']); vc=pick(df,['Volume','거래량']); ac=pick(df,['Amount','actual_amount','trade_amount','거래대금'])
    if any(x is None for x in [cc,dc,hc,lc,cl,vc]): raise SystemExit('HISTORY_SCHEMA_MISSING')
    q=pd.DataFrame({'code':df[cc].map(norm_code),'date':pd.to_datetime(df[dc],errors='coerce').dt.normalize(),'Open':pd.to_numeric(df[oc],errors='coerce') if oc else np.nan,'High':pd.to_numeric(df[hc],errors='coerce'),'Low':pd.to_numeric(df[lc],errors='coerce'),'Close':pd.to_numeric(df[cl],errors='coerce'),'Volume':pd.to_numeric(df[vc],errors='coerce'),'Amount':pd.to_numeric(df[ac],errors='coerce') if ac else np.nan})
    return q[q.code.ne('')&q.date.notna()].sort_values(['code','date']).drop_duplicates(['code','date'],keep='last')

def prep_evt(df):
    cc=pick(df,['code','Code','종목코드']); dc=pick(df,['signal_date','date','신호일'])
    if not cc or not dc: raise SystemExit('EVENT_SCHEMA_MISSING')
    q=df.copy(); q['code']=q[cc].map(norm_code); q['signal_date']=pd.to_datetime(q[dc],errors='coerce').dt.normalize(); q=q[q.code.ne('')&q.signal_date.notna()].copy()
    aliases={'ret1':['ret1','return_d1'],'ret3':['ret3','return_d3'],'ret5':['ret5','return_d5'],'ret10':['ret10','return_d10'],'mfe':['mfe','MFE'],'mae':['mae','MAE'],'evaluation_ret':['evaluation_ret','ret5','return_d5']}
    for t,a in aliases.items():
        c=pick(q,a); q[t]=pd.to_numeric(q[c],errors='coerce') if c else np.nan
    fc=pick(q,['primary_formula','formula','검색식','pattern_combo']); fl=pick(q,['formula_list','search_pattern_matches','pattern_combo']); fn=pick(q,['formula_count','overlap'])
    q['primary_formula']=q[fc].fillna('UNCLASSIFIED').astype(str) if fc else 'UNCLASSIFIED'; q['formula_list']=q[fl].fillna('').astype(str) if fl else q['primary_formula']
    q['formula_count']=pd.to_numeric(q[fn],errors='coerce') if fn else q['formula_list'].map(lambda s:max(1,len(set(z.strip() for z in re.split(r'[|,+/]',str(s)) if z.strip()))))
    for c in ['market_state','sector_state','context_alignment','catalyst_state','supply_drying','price_compression','restart_trigger','market_breadth','market_turnover_ratio','sector_breadth','sector_turnover_ratio']:
        if c not in q:q[c]=np.nan
    return q

def true_range(q):
    p=q.Close.shift(); return pd.concat([(q.High-q.Low).abs(),(q.High-p).abs(),(q.Low-p).abs()],axis=1).max(axis=1)

def metrics(h,code,d):
    q=h[(h.code==code)&(h.date<=d)].sort_values('date').tail(320).reset_index(drop=True)
    if len(q)<224:return {'structure_status':f'INSUFFICIENT_{len(q)}'}
    for n in (20,60,112,224):q[f'MA{n}']=q.Close.rolling(n,min_periods=n).mean()
    q['TR']=true_range(q); cur=q.iloc[-1]; px=float(cur.Close)
    def width(i):
        r=q.iloc[i]; vals=[float(r[f'MA{n}']) for n in (20,60,112,224) if pd.notna(r[f'MA{n}'])]; p=float(r.Close)
        return (max(vals)-min(vals))/p*100 if len(vals)==4 and p>0 else np.nan
    w0,w5,w10=width(-1),width(-6),width(-11); m224=float(cur.MA224)
    v=q.Volume; pv=v.iloc[-25:-5].median(); v5=v.iloc[-5:].median(); a=q.Amount
    if a.notna().sum()>=25: a5=a.iloc[-5:].median(); pa=a.iloc[-25:-5].median(); asrc='ACTUAL_AMOUNT'
    else: z=q.Close*q.Volume; a5=z.iloc[-5:].median(); pa=z.iloc[-25:-5].median(); asrc='CLOSE_X_VOLUME_PROXY'
    ret=q.Close.pct_change(); v10=v.iloc[-10:]; down=ret.iloc[-10:]<0; dshare=float(v10[down.values].sum()/v10.sum()) if v10.sum()>0 else np.nan; cv=float(v10.std(ddof=0)/v10.mean()) if v10.mean()>0 else np.nan
    atr5=q.TR.iloc[-5:].mean(); atr20=q.TR.iloc[-20:].mean()
    return {'structure_status':'PASS','ma_cluster_width_pct':w0,'ma_cluster_compression_5d_pctp':w5-w0,'ma_cluster_compression_10d_pctp':w10-w0,'price_to_ma224_abs_pct':abs(px/m224-1)*100 if m224>0 else np.nan,'close_vs_ma224_pct':(px/m224-1)*100 if m224>0 else np.nan,'vol_med5_vs_prev20':float(v5/pv) if pv>0 else np.nan,'amount_med5_vs_prev20':float(a5/pa) if pa>0 else np.nan,'amount_metric_source':asrc,'down_volume_share10':dshare,'volume_cv10':cv,'atr5_vs_atr20':float(atr5/atr20) if atr20>0 else np.nan,'close_ret10_pct':float((q.Close.iloc[-1]/q.Close.iloc[-11]-1)*100),'signal_volume':float(cur.Volume),'signal_amount':float(cur.Amount) if pd.notna(cur.Amount) else np.nan}

def perf(g):
    r=pd.to_numeric(g.evaluation_ret,errors='coerce').dropna(); mfe=pd.to_numeric(g.mfe,errors='coerce').dropna(); mae=pd.to_numeric(g.mae,errors='coerce').dropna()
    return {'n':len(g),'evaluated_n':len(r),'mean_ret':r.mean() if len(r) else np.nan,'median_ret':r.median() if len(r) else np.nan,'win_rate':(r>0).mean()*100 if len(r) else np.nan,'mfe_median':mfe.median() if len(mfe) else np.nan,'mae_median':mae.median() if len(mae) else np.nan,'d1_median':pd.to_numeric(g.ret1,errors='coerce').median(),'d3_median':pd.to_numeric(g.ret3,errors='coerce').median(),'d5_median':pd.to_numeric(g.ret5,errors='coerce').median(),'d10_median':pd.to_numeric(g.ret10,errors='coerce').median()}

def cuts(d):
    out={}
    for f in FEATURES:
        s=pd.to_numeric(d.get(f),errors='coerce').dropna()
        if len(s)>=10: out[f]={'q33':float(s.quantile(1/3)),'q67':float(s.quantile(2/3)),'n':len(s)}
    return out

def bucket(v,c):
    try:x=float(v)
    except:return 'UNKNOWN'
    if not math.isfinite(x):return 'UNKNOWN'
    return 'LOW' if x<c['q33'] else ('MID' if x<=c['q67'] else 'HIGH')

def group(df,cols,kind):
    rows=[]; cols=[c for c in cols if c in df]
    if not cols:return pd.DataFrame()
    for k,g in df.groupby(cols,dropna=False):
        if not isinstance(k,tuple):k=(k,)
        z={'group_type':kind,**{c:str(v) for c,v in zip(cols,k)}}; z.update(perf(g)); rows.append(z)
    return pd.DataFrame(rows)

def macro_fetch(start,end):
    try:import yfinance as yf
    except Exception as e:return pd.DataFrame(),{'status':'IMPORT_FAIL','reason':str(e)}
    st=(pd.Timestamp(start)-pd.Timedelta(days=20)).strftime('%Y-%m-%d'); en=(pd.Timestamp(end)+pd.Timedelta(days=3)).strftime('%Y-%m-%d'); parts=[]; meta={}
    for name,sym in MACRO.items():
        try:
            d=yf.download(sym,start=st,end=en,progress=False,auto_adjust=False,threads=False)
            if d.empty:meta[name]='EMPTY';continue
            if isinstance(d.columns,pd.MultiIndex):d.columns=[c[0] for c in d.columns]
            z=pd.DataFrame({'date':pd.to_datetime(d.index).tz_localize(None).normalize(),'macro':name,'close':pd.to_numeric(d.Close,errors='coerce').values}); z['ret1_pct']=z.close.pct_change()*100; z['ret5_pct']=z.close.pct_change(5)*100; parts.append(z); meta[name]=f'OK:{len(z)}'
        except Exception as e:meta[name]=f'ERROR:{type(e).__name__}'
    return (pd.concat(parts,ignore_index=True) if parts else pd.DataFrame()),{'status':'OK_PARTIAL' if parts else 'NO_MACRO','sources':meta}

def macro_join(ev,m):
    if m.empty:return pd.DataFrame()
    rows=[]
    for d in sorted(ev.signal_date.unique()):
        dt=pd.Timestamp(d); rec={'signal_date':dt}
        for name,g in m.groupby('macro'):
            q=g[g.date<=dt].sort_values('date')
            if len(q):
                r=q.iloc[-1]; rec[f'{name}_close']=r.close; rec[f'{name}_ret1_pct']=r.ret1_pct; rec[f'{name}_ret5_pct']=r.ret5_pct
        rows.append(rec)
    return pd.DataFrame(rows)

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--source-root',default='source_artifacts'); ap.add_argument('--output-dir',default='reports/closebet_structure_env_oos_r1'); ap.add_argument('--discovery-end',default='2026-08-18'); ap.add_argument('--oos-start',default='2026-08-19'); ap.add_argument('--oos-end',default='2026-09-18'); a=ap.parse_args()
    root=Path(a.source_root); out=Path(a.output_dir); out.mkdir(parents=True,exist_ok=True); ef=find(root,'v73_backtest_event_master.csv'); hf=find(root,'v49_76_history_authority_global.csv')
    if not ef or not hf:
        (out/'meta.json').write_text(json.dumps({'status':'FAIL_CLOSED','event':str(ef),'history':str(hf),'research_only':True},ensure_ascii=False,indent=2)); raise SystemExit('INPUT_MISSING')
    e=prep_evt(read_csv(ef)).sort_values(['signal_date','code']).drop_duplicates(['signal_date','code']); h=prep_hist(read_csv(hf)); mm=[]
    for r in e[['signal_date','code']].itertuples(index=False):
        z={'signal_date':r.signal_date,'code':r.code}; z.update(metrics(h,r.code,r.signal_date)); mm.append(z)
    x=e.merge(pd.DataFrame(mm),on=['signal_date','code'],how='left'); de=pd.Timestamp(a.discovery_end); osd=pd.Timestamp(a.oos_start); oed=pd.Timestamp(a.oos_end); disc=x[x.signal_date<=de].copy()
    mac,macmeta=macro_fetch(a.oos_start,a.oos_end); ms=macro_join(x,mac)
    if len(ms): x=x.merge(ms,on='signal_date',how='left'); disc=x[x.signal_date<=de].copy()
    oos=x[(x.signal_date>=osd)&(x.signal_date<=oed)].copy(); cs=cuts(disc); rows=[]
    for f,c in cs.items():
        oos[f+'_bucket']=oos[f].map(lambda v:bucket(v,c))
        for b in ('LOW','MID','HIGH'):
            z={'feature':f,'bucket':b,**c}; z.update(perf(oos[oos[f+'_bucket'].eq(b)])); rows.append(z)
    common=[]; er=pd.to_numeric(oos.evaluation_ret,errors='coerce'); q=oos[er.notna()].copy(); q['_ret']=er[er.notna()]
    if len(q):
        hi,lo=q._ret.quantile(.75),q._ret.quantile(.25)
        for f in FEATURES:
            w=pd.to_numeric(q.loc[q._ret>=hi,f],errors='coerce').dropna(); l=pd.to_numeric(q.loc[q._ret<=lo,f],errors='coerce').dropna(); common.append({'feature':f,'top_q_median':w.median() if len(w) else np.nan,'bottom_q_median':l.median() if len(l) else np.nan,'gap':w.median()-l.median() if len(w) and len(l) else np.nan,'top_n':len(w),'bottom_n':len(l)})
    x.to_csv(out/'event_master_enriched.csv',index=False,encoding='utf-8-sig'); pd.DataFrame([{'feature':k,**v} for k,v in cs.items()]).to_csv(out/'discovery_frozen_bins.csv',index=False,encoding='utf-8-sig'); pd.DataFrame(rows).to_csv(out/'oos_structure_bins.csv',index=False,encoding='utf-8-sig'); group(oos,['primary_formula'],'PATTERN').to_csv(out/'oos_pattern_performance.csv',index=False,encoding='utf-8-sig'); group(oos,['formula_count'],'OVERLAP').to_csv(out/'oos_overlap_performance.csv',index=False,encoding='utf-8-sig'); group(oos,['market_state','sector_state','context_alignment'],'CONTEXT').to_csv(out/'oos_context_performance.csv',index=False,encoding='utf-8-sig'); group(oos,['catalyst_state'],'CATALYST').to_csv(out/'oos_catalyst_performance.csv',index=False,encoding='utf-8-sig'); pd.DataFrame(common).to_csv(out/'oos_common_features.csv',index=False,encoding='utf-8-sig'); ms.to_csv(out/'macro_causal_snapshot.csv',index=False,encoding='utf-8-sig')
    meta={'revision':REVISION,'status':'PASS','discovery_end':a.discovery_end,'oos_start':a.oos_start,'oos_end':a.oos_end,'event_rows':len(x),'discovery_rows':len(disc),'oos_rows':len(oos),'structure_pass_rows':int(x.structure_status.eq('PASS').sum()),'frozen_feature_count':len(cs),'macro':macmeta,'geopolitical_risk_status':'NEXT_REVISION_CAUSAL_EVENT_CALENDAR_REQUIRED','research_only':True,'production_eligible':False,'selection_logic_changed':False,'score_rank_changed':False,'order_logic_changed':False,'same_sample_retuning':False}; (out/'meta.json').write_text(json.dumps(meta,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
    report='\n'.join(['[CLOSING BET · STRUCTURE + ENVIRONMENT OOS R1]','status=PASS',f'discovery<= {a.discovery_end} n={len(disc)} | OOS {a.oos_start}~{a.oos_end} n={len(oos)}',f'structure PASS={meta["structure_pass_rows"]} | frozen features={len(cs)}','연구축: 거래량/거래대금 지속성 · MA20/60/112/224 압축 · 하락일 거래량 비중 · ATR 압축 · 패턴 겹침 · 시장/섹터/재료 · USDKRW/VIX/US10Y 등','정책: RESEARCH_ONLY, LIVE 검색/점수/랭킹/주문 변경 0']); (out/'report.txt').write_text(report,encoding='utf-8'); print(report)
if __name__=='__main__': main()
