#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, math, hashlib
from pathlib import Path
import numpy as np, pandas as pd

RESEARCH_ID='REAL_FULL_WATERMELON_LONGMA_AUDIT_R1'
REVISION='R1_0_DEFINITION_LINEAGE_LONGMA_CONTEXT_20260923'
DISCOVERY_END=pd.Timestamp('2026-08-28')
HOLDOUT_START=pd.Timestamp('2026-09-01')


def num(v):
    try:
        x=float(v); return x if math.isfinite(x) else np.nan
    except Exception:return np.nan

def code(v):
    s=''.join(ch for ch in str(v or '') if ch.isalnum())
    return s.zfill(6) if s.isdigit() else s

def sha(x): return hashlib.sha256(json.dumps(x,ensure_ascii=False,sort_keys=True,default=str).encode()).hexdigest()

def price(code_, sd):
    import FinanceDataReader as fdr
    st=(sd-pd.Timedelta(days=950)).strftime('%Y-%m-%d'); en=(sd+pd.Timedelta(days=2)).strftime('%Y-%m-%d')
    q=fdr.DataReader(code_,st,en)
    if q is None or q.empty:return pd.DataFrame()
    q=q.copy(); q.index=pd.to_datetime(q.index,errors='coerce'); q=q[q.index.notna() & (q.index<=sd)].sort_index()
    if q.empty:return q
    q.columns=[str(c).title() for c in q.columns]
    need=['Open','High','Low','Close','Volume']
    if not all(c in q.columns for c in need):return pd.DataFrame()
    for c in need:q[c]=pd.to_numeric(q[c],errors='coerce')
    return q.dropna(subset=['Close']).tail(900)

def enrich(q):
    x=q.copy(); c=x.Close; v=x.Volume
    for n in [5,10,20,40,60,112,224,448]:
        x[f'MA{n}']=c.rolling(n,min_periods=n).mean(); x[f'VMA{n}']=v.rolling(n,min_periods=n).mean()
    s20=c.rolling(20).std(); s40=c.rolling(40).std()
    x['BB20_Upper']=x.MA20+2*s20; x['BB20_Lower']=x.MA20-2*s20; x['BB20_Width']=4*s20/x.MA20*100
    x['BB40_Upper']=x.MA40+2*s40; x['BB40_Lower']=x.MA40-2*s40; x['BB40_Width']=4*s40/x.MA40*100
    x['VWMA40']=(c*v).rolling(40).sum()/v.rolling(40).sum()
    x['Vol_Avg']=v.rolling(20).mean(); x['Vol_Accel']=v/v.rolling(5).mean()
    x['OBV']=(np.sign(c.diff())*v).fillna(0).cumsum(); x['OBV_MA10']=x.OBV.rolling(10).mean(); x['OBV_Rising']=x.OBV>x.OBV_MA10
    tp=(x.High+x.Low+x.Close)/3; mf=tp*v
    pos=mf.where(tp>tp.shift(1),0).rolling(14).sum(); neg=mf.where(tp<tp.shift(1),0).rolling(14).sum()
    x['MFI']=100-(100/(1+pos/neg.replace(0,np.nan))); x['MFI_Strong']=x.MFI>50
    x['Buy_Power']=v*(c-x.Open); x['Buy_Power_MA']=x.Buy_Power.rolling(10).mean(); x['Buying_Pressure']=x.Buy_Power>x.Buy_Power_MA
    delta=c.diff(); gain=delta.where(delta>0,0).ewm(com=13,adjust=False).mean(); loss=(-delta.where(delta<0,0)).ewm(com=13,adjust=False).mean(); x['RSI']=100-(100/(1+gain/loss.replace(0,np.nan)))
    redscore=x.OBV_Rising.astype(int)+x.MFI_Strong.astype(int)+x.Buying_Pressure.astype(int)
    x['WM_Color']=np.where(redscore>=2,'red','green'); x['WM_RedScore']=redscore
    x['WM_GreenDays10']=(pd.Series(x.WM_Color,index=x.index).shift(1)=='green').rolling(10).sum()
    cc=(x.WM_Color=='red') & (pd.Series(x.WM_Color,index=x.index).shift(1)=='green')
    x['WM_Legacy12']=cc & (x.WM_GreenDays10>=7) & (v>=x.VMA20*1.2)
    x['WM_Legacy15']=cc & (x.WM_GreenDays10>=7) & (v>=x.VMA20*1.5)
    x['WM_Fire']=(c/x.VWMA40-1)*100*x.Vol_Accel
    x['WM_BB40_Green']=(c>x.VWMA40)&(x.BB40_Width<10)
    x['WM_BB40_Red']=x.WM_BB40_Green & (x.WM_Fire>5)
    return x

def pct(a,b): return (a/b-1)*100 if math.isfinite(num(a)) and math.isfinite(num(b)) and num(b)!=0 else np.nan

def old_real_wm(x):
    if len(x)<449:return (0,{})
    cur=x.iloc[-1]; prev=x.iloc[-2]
    gc=(prev.MA5<prev.MA112) and (cur.MA5>=cur.MA112)
    app=(prev.MA5<prev.MA112) and (cur.MA112*.98<=cur.MA5<=cur.MA112*1.03)
    cond={
      'cross':bool(gc or app), 'inverse_mid':bool(cur.MA112<cur.MA224), 'below_448':bool(cur.Close<cur.MA448),
      'ma224_range':bool(-3<=pct(cur.Close,cur.MA224)<=5), 'bb40_range':bool(-7<=pct(cur.Close,cur.BB40_Upper)<=3),
      'vol300_50':bool(((x.Volume/x.Volume.shift(1).replace(0,np.nan))>=3).tail(50).any()),
      'break448_50':bool((x.High>x.MA448).tail(50).any())}
    return int(all(cond.values())),cond

def modern_state(x):
    try:
        from scanner.watermelon_core import build_watermelon_state_bundle
        b=build_watermelon_state_bundle(x.copy())
        return str(b.get('wm_final_state') or b.get('wm_state_name') or ''), int(bool(b.get('wm_state_green'))), int(bool(b.get('wm_state_red'))), int(bool(b.get('wm_state_blue')))
    except Exception as e:
        return f'IMPORT_OR_EVAL_ERROR:{type(e).__name__}',0,0,0

def longma_context(x):
    r=x.iloc[-1]; close=num(r.Close); out={}
    bits=[]
    vals=[]
    for n in [112,224,448]:
        ma=num(r.get(f'MA{n}')); d=pct(close,ma); out[f'close_vs_ma{n}_pct']=d; out[f'above_ma{n}']=int(math.isfinite(d) and d>=0) if math.isfinite(d) else np.nan
        bits.append('A' if math.isfinite(d) and d>=0 else ('B' if math.isfinite(d) else 'N')); vals.append(ma)
        prev=x.iloc[-2] if len(x)>=2 else r; pclose=num(prev.Close); pma=num(prev.get(f'MA{n}'))
        out[f'reclaim_ma{n}_today']=int(math.isfinite(ma) and math.isfinite(pma) and pclose<pma and close>=ma)
        out[f'below_ma{n}_days60']=int(((x.Close<x[f'MA{n}']).tail(60)).sum()) if f'MA{n}' in x else np.nan
    out['longma_position_code']='112'+bits[0]+'_224'+bits[1]+'_448'+bits[2]
    finite=[z for z in vals if math.isfinite(z)]
    out['longma_112_224_448_spread_pct']=((max(finite)-min(finite))/close*100) if len(finite)==3 and close else np.nan
    return out

def outcome_summary(df, col):
    rows=[]
    if col not in df:return pd.DataFrame()
    m=df[pd.to_numeric(df.get('d20_complete'),errors='coerce').eq(1)].copy()
    for k,g in m.groupby(col,dropna=False):
        rows.append({'axis':col,'group':str(k),'events':len(g),'success_rate':g.forensic_group.eq('SUCCESS').mean(),'failure_rate':g.forensic_group.eq('FAILURE').mean(),'giveback_rate':g.path_class.eq('EARLY_SPIKE_GIVEBACK').mean(),'d20_close_median':pd.to_numeric(g.d20_close_ret_pct,errors='coerce').median()})
    return pd.DataFrame(rows)

def run(a):
    out=Path(a.output_dir); out.mkdir(parents=True,exist_ok=True)
    src=pd.read_csv(a.source_master); src['origin_date']=pd.to_datetime(src.origin_date,errors='coerce').dt.normalize(); src['code']=src.code.map(code)
    rows=[]; missing=[]
    for _,r in src.iterrows():
        sd=pd.Timestamp(r.origin_date); q=price(r.code,sd); d=r.to_dict(); d['origin_date']=sd.date().isoformat()
        if q.empty:
            d['wm_price_status']='MISSING'; missing.append(f'{sd.date()}|{r.code}'); rows.append(d); continue
        x=enrich(q); cur=x.iloc[-1]; d['wm_price_status']='READY'
        d.update({'wm_legacy12':int(bool(cur.WM_Legacy12)),'wm_legacy15':int(bool(cur.WM_Legacy15)),'wm_bb40_green':int(bool(cur.WM_BB40_Green)),'wm_bb40_red':int(bool(cur.WM_BB40_Red)),'wm_fire':num(cur.WM_Fire),'bb20_width':num(cur.BB20_Width),'bb40_width':num(cur.BB40_Width),'volume_vs_vma20':num(cur.Volume/cur.VMA20) if num(cur.VMA20)>0 else np.nan})
        real,conds=old_real_wm(x); d['wm_old_real448']=real
        for k,v in conds.items(): d['wm_oldreal_'+k]=int(v)
        st,g,rr,b=modern_state(x); d['wm_modern_state']=st; d['wm_modern_green']=g; d['wm_modern_red']=rr; d['wm_modern_blue']=b
        d.update(longma_context(x)); rows.append(d)
    z=pd.DataFrame(rows); z.to_csv(out/'watermelon_event_master.csv',index=False,encoding='utf-8-sig')
    disc=z[pd.to_datetime(z.origin_date).le(DISCOVERY_END)].copy(); hold=z[pd.to_datetime(z.origin_date).ge(HOLDOUT_START)].copy()
    disc.to_csv(out/'discovery_watermelon_longma.csv',index=False,encoding='utf-8-sig'); hold.to_csv(out/'holdout_watermelon_longma_FROZEN_NO_RETUNING.csv',index=False,encoding='utf-8-sig')
    lineage=pd.DataFrame([
      ['LEGACY_GREEN_RED_1P2','Green→Red + prior green 7/10 + Volume>=VMA20*1.2','indicator_engine / older tests','BB20 indirect; volume direct; no long-MA gate'],
      ['LEGACY_GREEN_RED_1P5','same but Volume>=VMA20*1.5','main7_bugfix lineage','volume threshold tightened'],
      ['VWMA40_BB40_FIRE','Close>VWMA40 + BB40 width<10; red if Fire>5','indicator_engine','BB40+VWMA40+volume acceleration'],
      ['OLD_REAL_WM_448','MA5↔MA112 cross/approach + MA112<MA224 + close<MA448 + MA224/BB40 ranges + prior 3x volume + MA448 break','indicator_engine/main6/main7','explicit MA112/224/448'],
      ['MODERN_STATE_MACHINE','intro/pullback/Blue-1/Blue-2/late','scanner/watermelon_core.py','MA20/60/112 + box/volume/OBV; long-MA context analyzed separately'],
    ],columns=['definition_id','definition','repo_lineage','interpretation'])
    lineage.to_csv(out/'watermelon_definition_lineage.csv',index=False,encoding='utf-8-sig')
    sums=[]
    for c in ['wm_legacy12','wm_legacy15','wm_bb40_green','wm_bb40_red','wm_old_real448','wm_modern_state','longma_position_code']:
        q=outcome_summary(disc,c)
        if not q.empty:sums.append(q)
    pd.concat(sums,ignore_index=True).to_csv(out/'discovery_definition_x_longma_outcome_summary.csv',index=False,encoding='utf-8-sig') if sums else pd.DataFrame().to_csv(out/'discovery_definition_x_longma_outcome_summary.csv',index=False)
    cross=disc.groupby(['wm_modern_state','longma_position_code'],dropna=False).size().reset_index(name='events'); cross.to_csv(out/'modern_state_x_longma_context_counts.csv',index=False,encoding='utf-8-sig')
    design={'discovery_end':'2026-08-28','holdout_start':'2026-09-01','definitions':['LEGACY12','LEGACY15','BB40_FIRE','OLD_REAL448','MODERN_STATE'],'research_only':True,'production_eligible':False,'same_sample_retuning':False,'thresholds_newly_optimized':False}
    meta={'research_id':RESEARCH_ID,'revision':REVISION,'status':'PASS','events':len(z),'discovery_events':len(disc),'holdout_events':len(hold),'missing_price':len(missing),'research_only':True,'production_eligible':False,'selection_logic_changed':False,'score_rank_changed':False,'order_logic_changed':False,'same_sample_retuning':False,'design_hash':sha(design),'design':design}
    (out/'watermelon_longma_meta.json').write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding='utf-8')
    (out/'watermelon_longma_report.txt').write_text('\n'.join([f'🧪 [{RESEARCH_ID}]',f'status=PASS revision={REVISION}',f'events={len(z)} discovery={len(disc)} holdout={len(hold)} missing_price={len(missing)}','existing definitions reproduced side-by-side; no new production rule','MA112/224/448 context is descriptive research only','research_only=1 production changes=0 same_sample_retuning=0'])+'\n',encoding='utf-8')
    print((out/'watermelon_longma_report.txt').read_text())

def self_test():
    n=520; idx=pd.bdate_range('2024-01-01',periods=n); c=np.linspace(80,100,n); q=pd.DataFrame({'Open':c*.995,'High':c*1.01,'Low':c*.99,'Close':c,'Volume':100000},index=idx); x=enrich(q); assert 'MA448' in x and 'WM_Legacy12' in x; assert len(longma_context(x))>5; print('REAL_FULL_WATERMELON_LONGMA_AUDIT_R1_SELF_TEST PASS')

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--source-master',default='source_pattern/reports/real_full_pattern_truth_unknown_r1/pattern_truth_event_master.csv'); ap.add_argument('--output-dir',default='reports/real_full_watermelon_longma_audit_r1'); ap.add_argument('--self-test',action='store_true'); a=ap.parse_args(); return self_test() if a.self_test else run(a)
if __name__=='__main__': main()
