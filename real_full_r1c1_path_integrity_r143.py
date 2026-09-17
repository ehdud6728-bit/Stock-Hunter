#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse, hashlib, io, json, math, os, zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests

REVISION = "R1_4_3_PROSPECTIVE_PATH_INTEGRITY_20260917"
SHADOW_ID = "REAL_FULL_R1C1"
SHADOW_REVISION = "R1C1_PB_VOLUME_CONTRACTION_V1"
HORIZONS = (1, 3, 5, 10)
STATE_DIR = Path("reports/.cache/real_full_trust_r1/r143")
OUT_DIR = Path("reports/real_full_r1c1_r143")
SEED_LEDGER = STATE_DIR / "path_seed_ledger.csv"
OUTCOME_LEDGER = STATE_DIR / "path_outcome_ledger.csv"
META_JSON = OUT_DIR / "real_full_r1c1_r143_meta.json"
REPORT_TXT = OUT_DIR / "real_full_r1c1_r143_report.txt"
SEED_EXPORT = OUT_DIR / "real_full_r1c1_r143_seed_ledger.csv"
OUTCOME_EXPORT = OUT_DIR / "real_full_r1c1_r143_outcome_ledger.csv"

SEED_HASH_FIELDS = [
    "event_key","signal_date","rank","code","name","entry_price",
    "v72_pullback_restart_score","r1c1_shadow_eligible","r1c1_shadow_state",
    "wave1_low_date","wave1_low_price","wave1_high_date","wave1_high_price",
    "pullback_low_date","pullback_low_price","wave1_volume_median","pb_volume_median",
    "pb_volume_vs_wave1","signal_volume","signal_amount_proxy","signal_close_vs_ma224_pct",
    "ma224_slope_20d_pct","observer_metric_status","ma224_status",
]

def utc_now(): return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def norm_code(v: Any) -> str:
    s=str(v or "").strip().upper()
    if s.endswith('.0') and s[:-2].isdigit(): s=s[:-2]
    for x in ('.KS','.KQ','.KRX'):
        if s.endswith(x): s=s[:-len(x)]
    s=''.join(ch for ch in s if ch.isalnum())
    if len(s)==7 and s.startswith('A'): s=s[1:]
    return s.zfill(6) if s.isdigit() and len(s)<=6 else s[-6:]

def num(v: Any) -> float:
    try:
        x=float(v); return x if math.isfinite(x) else float('nan')
    except Exception: return float('nan')

def boolish(v: Any) -> bool:
    if isinstance(v,(bool,np.bool_)): return bool(v)
    try:
        if isinstance(v,(int,float,np.integer,np.floating)): return math.isfinite(float(v)) and float(v)!=0
    except Exception: pass
    return str(v or '').strip().lower() in {'1','true','yes','y','on','t'}

def canon(v: Any):
    if v is None: return None
    if isinstance(v,(bool,np.bool_)): return bool(v)
    if isinstance(v,(int,np.integer)): return int(v)
    if isinstance(v,(float,np.floating)):
        x=float(v); return None if not math.isfinite(x) else round(x,10)
    try:
        if pd.isna(v): return None
    except Exception: pass
    s=str(v).strip(); return s if s and s.lower() not in {'nan','none','nat'} else None

def sha_obj(o):
    return hashlib.sha256(json.dumps(o,ensure_ascii=False,sort_keys=True,separators=(',',':'),default=str).encode()).hexdigest()

def seed_hash(row): return sha_obj({k:canon(row.get(k)) for k in SEED_HASH_FIELDS})

def read_csv(p):
    if not p.exists(): return pd.DataFrame()
    try: return pd.read_csv(p,dtype={'code':str},low_memory=False)
    except pd.errors.EmptyDataError: return pd.DataFrame()

def atomic_csv(df,p):
    p.parent.mkdir(parents=True,exist_ok=True); t=p.with_suffix(p.suffix+'.tmp'); df.to_csv(t,index=False,encoding='utf-8-sig'); t.replace(p)

def atomic_json(o,p):
    p.parent.mkdir(parents=True,exist_ok=True); t=p.with_suffix(p.suffix+'.tmp'); t.write_text(json.dumps(o,ensure_ascii=False,indent=2,default=str),encoding='utf-8'); t.replace(p)

def member(z, basename):
    hits=[n for n in z.namelist() if Path(n).name==basename]
    if not hits: return None
    pref=[n for n in hits if '/reports/' in '/'+n]
    return pref[0] if pref else hits[0]

def read_bundle(raw):
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        names={
            'obs':'real_full_r1c1_structure_observer_r142.csv',
            'meta':'real_full_r1c1_structure_observer_r142_meta.json',
            'side':'real_full_r1c1_v72_runtime_sidecar.csv',
            'anchor':'v72_formula_selector_anchor_serialization.csv'}
        m={k:member(z,v) for k,v in names.items()}
        miss=[names[k] for k,v in m.items() if not v]
        if miss: raise ValueError('R143_SOURCE_FILES_MISSING:'+','.join(miss))
        meta=json.loads(z.read(m['meta']).decode('utf-8-sig'))
        if meta.get('status')!='PASS' or bool(meta.get('production_eligible')) or bool(meta.get('network_refetch_used')):
            raise ValueError('R142_AUTHORITY_INVALID')
        obs=pd.read_csv(io.BytesIO(z.read(m['obs'])),dtype={'code':str})
        side=pd.read_csv(io.BytesIO(z.read(m['side'])),dtype={'code':str})
        anchor=pd.read_csv(io.BytesIO(z.read(m['anchor'])),dtype={'code':str})
        return obs,side,anchor,meta

def gh_get(url,token):
    h={'Accept':'application/vnd.github+json','X-GitHub-Api-Version':'2022-11-28'}
    if token: h['Authorization']='Bearer '+token
    r=requests.get(url,headers=h,timeout=45,allow_redirects=True); r.raise_for_status(); return r

def resolve_artifact(repo,token,source_run_id=0):
    base=f'https://api.github.com/repos/{repo}'
    if source_run_id: run_ids=[int(source_run_id)]
    else:
        js=gh_get(f'{base}/actions/workflows/run_scanner.yml/runs?status=success&per_page=30',token).json()
        run_ids=[int(x['id']) for x in js.get('workflow_runs',[])]
    errors=[]
    for rid in run_ids:
        try:
            arts=gh_get(f'{base}/actions/runs/{rid}/artifacts?per_page=100',token).json().get('artifacts',[])
            for a in arts:
                if a.get('expired'): continue
                aid=int(a['id']); raw=gh_get(f'{base}/actions/artifacts/{aid}/zip',token).content
                try:
                    read_bundle(raw); return rid,aid,raw
                except Exception as e: errors.append(f'{rid}/{aid}:{e}')
        except Exception as e: errors.append(f'{rid}:{e}')
    raise RuntimeError('R143_NO_VALID_R142_ARTIFACT:'+' | '.join(errors[-8:]))

def build_seeds(obs,side,anchor,run_id,artifact_id):
    for d in (obs,side,anchor): d['code']=d['code'].map(norm_code)
    if any(d['code'].duplicated().any() for d in (obs,side,anchor)): raise ValueError('R143_SOURCE_DUPLICATE_CODE')
    m=obs.merge(side[['code','source_entry_price','snapshot_price','causal_invariant']],on='code',how='left',validate='one_to_one')
    ac=[c for c in ['code','wave1_low_price','wave1_high_price','pullback_low_price','temporal_invariant','predictor_causal_invariant'] if c in anchor.columns]
    m=m.merge(anchor[ac],on='code',how='left',validate='one_to_one')
    out=[]
    for _,r in m.iterrows():
        code=norm_code(r.code); sd=str(r.signal_date)[:10]; rank=int(num(r['rank']))
        entry=num(r.get('source_entry_price')); entry=entry if math.isfinite(entry) and entry>0 else num(r.get('snapshot_price'))
        if not code or not sd or not math.isfinite(entry) or entry<=0: raise ValueError('R143_BAD_SEED_IDENTITY:'+code)
        if str(r.get('causal_invariant',''))!='PASS' or str(r.get('observer_metric_status',''))!='PASS': raise ValueError('R143_SOURCE_NOT_PASS:'+code)
        row={
            'schema':'REAL_FULL_R1C1_R143_SEED_V1','revision':REVISION,'shadow_id':SHADOW_ID,'shadow_revision':SHADOW_REVISION,
            'event_key':f'{sd}|{code}','signal_date':sd,'rank':rank,'code':code,'name':canon(r.get('name')) or '',
            'entry_price':entry,'v72_pullback_restart_score':num(r.get('v72_pullback_restart_score')),
            'r1c1_shadow_eligible':boolish(r.get('r1c1_shadow_eligible')),'r1c1_shadow_state':canon(r.get('r1c1_shadow_state')) or '',
            'wave1_low_date':canon(r.get('wave1_low_date')),'wave1_low_price':num(r.get('wave1_low_price')),
            'wave1_high_date':canon(r.get('wave1_high_date')),'wave1_high_price':num(r.get('wave1_high_price')),
            'pullback_low_date':canon(r.get('pullback_low_date')),'pullback_low_price':num(r.get('pullback_low_price')),
            'wave1_volume_median':num(r.get('wave1_volume_median')),'pb_volume_median':num(r.get('pb_volume_median')),'pb_volume_vs_wave1':num(r.get('pb_volume_vs_wave1')),
            'signal_volume':num(r.get('signal_volume')),'signal_amount_proxy':num(r.get('signal_amount_proxy')),
            'signal_close_vs_ma224_pct':num(r.get('signal_close_vs_ma224_pct')),'ma224_slope_20d_pct':num(r.get('ma224_slope_20d_pct')),
            'observer_metric_status':canon(r.get('observer_metric_status')) or '', 'ma224_status':canon(r.get('ma224_status')) or '',
            'source_run_id':int(run_id),'source_artifact_id':int(artifact_id),'source_locked_at_utc':utc_now(),
            'production_eligible':False,'same_sample_retuning':False,'new_gate_added':False}
        row['seed_hash']=seed_hash(row); out.append(row)
    return pd.DataFrame(out)

def append_seeds(new):
    old=read_csv(SEED_LEDGER)
    if old.empty: atomic_csv(new,SEED_LEDGER); return new,len(new)
    if old['event_key'].duplicated().any(): raise ValueError('R143_EXISTING_SEED_DUPLICATE')
    om=old.set_index('event_key'); add=[]
    for _,r in new.iterrows():
        if r.event_key in om.index:
            if str(om.loc[r.event_key].get('seed_hash',''))!=str(r.seed_hash): raise ValueError('R143_APPEND_ONLY_SEED_DRIFT:'+r.event_key)
        else: add.append(r.to_dict())
    out=pd.concat([old,pd.DataFrame(add)],ignore_index=True) if add else old; atomic_csv(out,SEED_LEDGER); return out,len(add)

def norm_ohlcv(fr):
    if fr is None or fr.empty: return pd.DataFrame()
    q=fr.rename(columns={'시가':'Open','고가':'High','저가':'Low','종가':'Close','거래량':'Volume','open':'Open','high':'High','low':'Low','close':'Close','volume':'Volume'}).copy()
    q.index=pd.to_datetime(q.index,errors='coerce').normalize(); q=q[q.index.notna()].sort_index()
    for c in ['Open','High','Low','Close','Volume']:
        if c in q.columns: q[c]=pd.to_numeric(q[c],errors='coerce')
    return q

def fetch_history(code,signal_date,today):
    try:
        import FinanceDataReader as fdr
        sd=pd.Timestamp(signal_date); start=(sd-pd.Timedelta(days=520)).strftime('%Y-%m-%d'); end=(pd.Timestamp(today)+pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        q=norm_ohlcv(fdr.DataReader(code,start,end))
        if not q.empty: return q,'FDR_RESEARCH_OUTCOME_ONLY'
    except Exception: pass
    return pd.DataFrame(),'UNAVAILABLE'

def mature(seeds,old,today):
    done=set() if old.empty else {(str(x.event_key),int(x.horizon_td)) for x in old.itertuples(index=False)}
    rows=[]; unavailable=[]
    for s in seeds.itertuples(index=False):
        need=[h for h in HORIZONS if (str(s.event_key),h) not in done]
        if not need: continue
        fr,source=fetch_history(str(s.code),str(s.signal_date),today)
        if fr.empty: unavailable.append(str(s.event_key)); continue
        sd=pd.Timestamp(str(s.signal_date)).normalize(); future=fr.loc[fr.index>sd].copy(); entry=num(s.entry_price)
        for h in need:
            if len(future)<h: continue
            p=future.iloc[:h]; end=p.iloc[-1]; hd=p.index[-1]; close=num(end.get('Close')); high=num(pd.to_numeric(p['High'],errors='coerce').max()); low=num(pd.to_numeric(p['Low'],errors='coerce').min())
            if not all(math.isfinite(x) for x in (close,high,low)): continue
            vol=pd.to_numeric(p.get('Volume'),errors='coerce'); amt=pd.to_numeric(p.get('Close'),errors='coerce')*vol
            wh=num(s.wave1_high_price); pl=num(s.pullback_low_price); sigv=num(s.signal_volume); siga=num(s.signal_amount_proxy)
            pre=fr.loc[fr.index<=hd]; ma=pd.to_numeric(pre['Close'],errors='coerce').rolling(224,min_periods=224).mean(); hma=num(ma.iloc[-1]); hv=(close/hma-1)*100 if math.isfinite(hma) and hma>0 else float('nan')
            row={'schema':'REAL_FULL_R1C1_R143_OUTCOME_V1','revision':REVISION,'shadow_id':SHADOW_ID,'shadow_revision':SHADOW_REVISION,
                 'event_key':str(s.event_key),'seed_hash':str(s.seed_hash),'signal_date':str(s.signal_date),'code':str(s.code),'name':str(s.name),'rank':int(s.rank),
                 'v72_pullback_restart_score':num(s.v72_pullback_restart_score),'r1c1_shadow_eligible':boolish(s.r1c1_shadow_eligible),'r1c1_shadow_state':str(s.r1c1_shadow_state),
                 'horizon_td':h,'horizon_date':hd.date().isoformat(),'entry_price':entry,'horizon_close':close,'close_ret_pct':(close/entry-1)*100,'mfe_pct':(high/entry-1)*100,'mae_pct':(low/entry-1)*100,
                 'hit_plus3':bool(high>=entry*1.03),'hit_plus5':bool(high>=entry*1.05),'wave1_high_retested':bool(math.isfinite(wh) and high>=wh),'pullback_low_breached':bool(math.isfinite(pl) and low<pl),
                 'signal_close_vs_ma224_pct':num(s.signal_close_vs_ma224_pct),'horizon_close_vs_ma224_pct':hv,'horizon_ma224_state':'ABOVE' if math.isfinite(hv) and hv>=0 else ('BELOW' if math.isfinite(hv) else 'UNAVAILABLE'),
                 'post_signal_volume_median_vs_signal':num(vol.median())/sigv if math.isfinite(sigv) and sigv>0 else float('nan'),
                 'post_signal_amount_median_vs_signal':num(amt.median())/siga if math.isfinite(siga) and siga>0 else float('nan'),
                 'outcome_price_source':source,'matured_at_utc':utc_now(),'production_eligible':False,'same_sample_retuning':False,'new_gate_added':False}
            row['outcome_hash']=sha_obj({k:canon(v) for k,v in row.items() if k!='matured_at_utc'}); rows.append(row)
    return rows,unavailable

def append_outcomes(rows):
    old=read_csv(OUTCOME_LEDGER)
    if not old.empty and old.duplicated(['event_key','horizon_td']).any(): raise ValueError('R143_EXISTING_OUTCOME_DUPLICATE')
    keys=set() if old.empty else {(str(x.event_key),int(x.horizon_td)) for x in old.itertuples(index=False)}
    add=[r for r in rows if (str(r['event_key']),int(r['horizon_td'])) not in keys]
    out=pd.concat([old,pd.DataFrame(add)],ignore_index=True) if add else old; atomic_csv(out,OUTCOME_LEDGER); return out,len(add)

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--github-repo',default=os.getenv('GITHUB_REPOSITORY','ehdud6728-bit/Stock-Hunter')); ap.add_argument('--github-token',default=os.getenv('GITHUB_TOKEN','')); ap.add_argument('--source-run-id',type=int,default=0); ap.add_argument('--source-zip',default=''); ap.add_argument('--today',default=''); a=ap.parse_args()
    STATE_DIR.mkdir(parents=True,exist_ok=True); OUT_DIR.mkdir(parents=True,exist_ok=True); today=a.today or pd.Timestamp.now(tz='Asia/Seoul').date().isoformat(); status='PASS'
    try:
        if a.source_zip: raw=Path(a.source_zip).read_bytes(); rid=int(a.source_run_id or 0); aid=0
        else: rid,aid,raw=resolve_artifact(a.github_repo,a.github_token,a.source_run_id)
        obs,side,anchor,r142=read_bundle(raw); new=build_seeds(obs,side,anchor,rid,aid); seeds,sa=append_seeds(new); old=read_csv(OUTCOME_LEDGER); rows,un=mature(seeds,old,today); outcomes,oa=append_outcomes(rows)
        atomic_csv(seeds,SEED_EXPORT); atomic_csv(outcomes,OUTCOME_EXPORT)
        by={str(h):int((pd.to_numeric(outcomes.get('horizon_td'),errors='coerce')==h).sum()) if not outcomes.empty else 0 for h in HORIZONS}
        meta={'revision':REVISION,'status':'PASS','reason':'OK','today':today,'source_run_id':rid,'source_artifact_id':aid,'source_signal_date':r142.get('signal_date'),'source_r142_status':r142.get('status'),'seed_rows':len(seeds),'seed_rows_added':sa,'outcome_rows':len(outcomes),'outcome_rows_added':oa,'outcome_rows_by_horizon':by,'temporarily_unavailable_event_count':len(set(un)),'research_only':True,'production_eligible':False,'selection_logic_changed':False,'score_rank_changed':False,'order_logic_changed':False,'new_gate_added':False,'threshold_changed':False,'same_sample_retuning':False,'predictor_refetch_used':False,'outcome_network_fetch_allowed':True,'outcome_network_fetch_scope':'POST_SIGNAL_OUTCOME_ONLY_NOT_PREDICTOR','horizons_trading_days':list(HORIZONS)}
    except Exception as e:
        status='FAIL_CLOSED'; meta={'revision':REVISION,'status':status,'reason':f'{type(e).__name__}:{e}','today':today,'research_only':True,'production_eligible':False,'selection_logic_changed':False,'score_rank_changed':False,'order_logic_changed':False,'new_gate_added':False,'threshold_changed':False,'same_sample_retuning':False,'predictor_refetch_used':False,'outcome_network_fetch_allowed':True}
    atomic_json(meta,META_JSON)
    REPORT_TXT.write_text('🧪 [REAL_FULL R1C1 R1.4.3 PROSPECTIVE PATH INTEGRITY]\n'+f"status={meta.get('status')} reason={meta.get('reason')} today={today}\n"+f"source_run={meta.get('source_run_id','')} source_signal={meta.get('source_signal_date','')} seed_rows={meta.get('seed_rows',0)} seed_added={meta.get('seed_rows_added',0)}\n"+f"outcome_rows={meta.get('outcome_rows',0)} outcome_added={meta.get('outcome_rows_added',0)} horizons={meta.get('outcome_rows_by_horizon',{})}\n"+'research_only=1 production_eligible=0 predictor_refetch=0 new_gate=0 threshold_change=0 same_sample_retuning=0\n'+'outcome_fetch=POST_SIGNAL_ONLY; production REAL_FULL selection/score/rank/order untouched\n',encoding='utf-8')
    print(REPORT_TXT.read_text(encoding='utf-8')); return 0 if status=='PASS' else 2

if __name__=='__main__': raise SystemExit(main())
