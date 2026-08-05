from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys

import numpy as np
import pandas as pd
import Closing_bet_scanner_v2 as s


def file_sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def f(v):
    try:
        if pd.isna(v): return '평가없음'
        return f'{float(v):+.2f}%'
    except Exception:
        return '평가없음'


def _read_csvs(paths, **kwargs):
    frames=[]
    for p in paths:
        try:
            x=pd.read_csv(p,**kwargs)
        except pd.errors.EmptyDataError:
            x=pd.DataFrame()
        if not x.empty: frames.append(x)
    return pd.concat(frames,ignore_index=True,sort=False) if frames else pd.DataFrame()


def _deliver_telegram(parts: list[list[str]], out: Path, requested: bool) -> dict:
    delivery={'requested':bool(requested),'route_validated':str(os.environ.get('TELEGRAM_ROUTE_VALIDATED','') or ''),'pair_source':str(os.environ.get('TELEGRAM_ROUTE_PAIR_SOURCE','') or ''),'alias_source':str(os.environ.get('TELEGRAM_ROUTE_ALIAS_SOURCE','') or ''),'chat_id_masked':str(os.environ.get('TELEGRAM_ROUTE_CHAT_MASKED','') or ''),'parts':[],'status':'SKIPPED'}
    path=out/'v49_64_telegram_delivery_manifest.json'
    try:
        if requested:
            if delivery['route_validated']!='1': raise RuntimeError('v49.64 Telegram requested but unified route preflight was not validated')
            if not s._telegram_route_ready(): raise RuntimeError('v49.64 Telegram requested but scanner route is not ready after validated preflight')
            for part_no,part in enumerate(parts,start=1):
                result=s.send_telegram_photo('\n'.join(part),[])
                delivery['parts'].append({'part':part_no,**result})
                if int(result.get('success_count',0) or 0)<1: raise RuntimeError(f'v49.64 Telegram delivery failed for part {part_no}: {result.get("errors",[])}')
            delivery['status']='DELIVERED'; delivery['success_count']=sum(int(x.get('success_count',0) or 0) for x in delivery['parts'])
            print(f"TELEGRAM DELIVERY ACK ✅ · parts {len(delivery['parts'])}/{len(parts)} · success {delivery['success_count']} · pair {delivery['pair_source']} · chat {delivery['chat_id_masked']}")
        path.write_text(json.dumps(delivery,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
        return delivery
    except Exception as exc:
        delivery['status']='FAILED'; delivery['error']=f'{type(exc).__name__}: {exc}'
        path.write_text(json.dumps(delivery,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
        raise


def _lp_validation(selected: pd.DataFrame, out: Path, start_date: str, end_date: str) -> pd.DataFrame:
    z=selected[selected.get('mode',pd.Series('',index=selected.index)).astype(str).eq('LP')].copy()
    if z.empty:
        r=pd.DataFrame(columns=['fold','start','end','n','net20_mean_pct','net50_mean_pct','positive_month50_pct','status'])
        r.to_csv(out/'v49_64_lp_walk_forward.csv',index=False,encoding='utf-8-sig'); return r
    z['_date']=pd.to_datetime(z.get('signal_date'),errors='coerce'); z['_gross']=pd.to_numeric(z.get('rule35_pnl'),errors='coerce'); z=z[z['_date'].notna()&z['_gross'].notna()].copy(); z['_net20']=z['_gross']-.20; z['_net50']=z['_gross']-.50
    split=pd.Timestamp(getattr(s,'CLOSING_BET_V4940_OOS_SPLIT_DATE','2026-01-01'))
    if split<=pd.Timestamp(start_date) or split>pd.Timestamp(end_date): split=pd.Timestamp(start_date)+(pd.Timestamp(end_date)-pd.Timestamp(start_date))*.70
    rows=[]; cur=split; fold=1; months=int(getattr(s,'CLOSING_BET_V4964_LP_WALK_MONTHS',3)); min_n=int(getattr(s,'CLOSING_BET_V4964_LP_MIN_FOLD_N',15)); data_end=pd.Timestamp(end_date)
    while cur<=data_end:
        nxt=min(data_end,cur+pd.DateOffset(months=months)-pd.Timedelta(days=1)); q=z[z['_date'].between(cur,nxt)]
        if len(q):
            monthly=q.set_index('_date')['_net50'].resample('ME').sum()
            rows.append({'fold':fold,'start':cur.strftime('%Y-%m-%d'),'end':nxt.strftime('%Y-%m-%d'),'n':len(q),'net20_mean_pct':float(q['_net20'].mean()),'net50_mean_pct':float(q['_net50'].mean()),'positive_month50_pct':float((monthly>0).mean()*100) if len(monthly) else np.nan,'status':'VALID' if len(q)>=min_n else 'LOW-N'})
        cur=nxt+pd.Timedelta(days=1); fold+=1
    r=pd.DataFrame(rows); r.to_csv(out/'v49_64_lp_walk_forward.csv',index=False,encoding='utf-8-sig'); return r


def _iit_split(selected: pd.DataFrame, out: Path, start_date: str, end_date: str) -> pd.DataFrame:
    x=selected.copy(); x['_date']=pd.to_datetime(x.get('signal_date'),errors='coerce'); x['_gross']=pd.to_numeric(x.get('rule35_pnl'),errors='coerce'); x=x[x['_date'].notna()&x['_gross'].notna()].copy(); x['_net20']=x['_gross']-.20; x['_net50']=x['_gross']-.50
    split=pd.Timestamp(getattr(s,'CLOSING_BET_V4940_OOS_SPLIT_DATE','2026-01-01'))
    if split<=pd.Timestamp(start_date) or split>pd.Timestamp(end_date): split=pd.Timestamp(start_date)+(pd.Timestamp(end_date)-pd.Timestamp(start_date))*.70
    src=x.get('i_flow_source',x.get('flow_source',pd.Series('',index=x.index))).astype(str)
    x['_flow_group']=np.where(src.str.contains('proxy',case=False,na=False),'PROXY','REAL_OR_CACHE')
    rows=[]
    for mode in ('I','IT'):
        for grp in ('REAL_OR_CACHE','PROXY'):
            z=x[(x.get('mode',pd.Series('',index=x.index)).astype(str)==mode)&(x['_flow_group']==grp)]; oo=z[z['_date']>=split]
            rows.append({'strategy':mode,'flow_group':grp,'n':len(z),'oos_n':len(oo),'net20_mean_pct':float(z['_net20'].mean()) if len(z) else np.nan,'net50_mean_pct':float(z['_net50'].mean()) if len(z) else np.nan,'oos_net20_mean_pct':float(oo['_net20'].mean()) if len(oo) else np.nan,'oos_net50_mean_pct':float(oo['_net50'].mean()) if len(oo) else np.nan,'promotion_eligible':int(grp=='REAL_OR_CACHE' and len(oo)>0)})
    r=pd.DataFrame(rows); r.to_csv(out/'v49_64_iit_flow_split.csv',index=False,encoding='utf-8-sig'); return r


def main() -> int:
    ap=argparse.ArgumentParser(description='v49.64 global shard merge + strategy funnel audit')
    ap.add_argument('--input-root',default='v49_64_downloads'); ap.add_argument('--prepare-dir',default='v49_64_prepare_output'); ap.add_argument('--output-dir',default='reports'); ap.add_argument('--start-date',default=''); ap.add_argument('--end-date',default=''); ap.add_argument('--top-per-strategy',type=int,default=5); ap.add_argument('--shard-count',type=int,default=8); ap.add_argument('--send-telegram',action='store_true')
    args=ap.parse_args(); root=Path(args.input_root); prep_root=Path(args.prepare_dir)
    if not prep_root.exists(): prep_root=root
    preflight_path=next(iter(prep_root.rglob('v49_64_preflight.json')),None); universe_path=next(iter(prep_root.rglob('v49_64_universe.json')),None)
    if preflight_path is None or universe_path is None: raise RuntimeError('prepare artifacts missing')
    preflight=json.loads(preflight_path.read_text(encoding='utf-8')); universe=json.loads(universe_path.read_text(encoding='utf-8'))
    if preflight.get('status')!='VALID': raise RuntimeError(f'preflight invalid: {preflight}')

    manifests=sorted(root.rglob('shard_*_manifest.json')); raws=sorted(root.rglob('shard_*_raw.csv')); completes=sorted(root.rglob('shard_*_complete.json')); funnels=sorted(root.rglob('shard_*_strategy_funnel.csv')); zero_files=sorted(root.rglob('shard_*_zero_mode_audit.csv')); exc_files=sorted(root.rglob('shard_*_predicate_exceptions.csv'))
    counts={'manifest':len(manifests),'raw':len(raws),'complete':len(completes),'funnel':len(funnels),'zero':len(zero_files),'exceptions':len(exc_files)}
    if any(v!=args.shard_count for v in counts.values()): raise RuntimeError(f'shard artifacts incomplete: {counts}, expected {args.shard_count}')
    manifest_docs=[json.loads(p.read_text(encoding='utf-8')) for p in manifests]; complete_docs=[json.loads(p.read_text(encoding='utf-8')) for p in completes]
    start_dates={str(x.get('start_date','')) for x in manifest_docs}; end_dates={str(x.get('end_date','')) for x in manifest_docs}
    if len(start_dates)!=1 or len(end_dates)!=1: raise RuntimeError(f'shard date consensus failed: start={start_dates}, end={end_dates}')
    args.start_date=next(iter(start_dates)); args.end_date=next(iter(end_dates))
    expected_ids=list(range(args.shard_count)); shard_ids=sorted(int(x.get('shard_index',-1)) for x in manifest_docs)
    if shard_ids!=expected_ids: raise RuntimeError(f'shard ids mismatch: {shard_ids} != {expected_ids}')
    if {int(x.get('global_count',0) or 0) for x in manifest_docs}!={int(preflight.get('universe_count',0))} or {str(x.get('global_fingerprint','')) for x in manifest_docs}!={str(preflight.get('universe_fingerprint',''))}: raise RuntimeError('global universe consensus failed')
    manifest_by_id={int(x.get('shard_index',-1)):x for x in manifest_docs}; manifest_path_by_id={int(d.get('shard_index',-1)):p for p,d in zip(manifests,manifest_docs)}; complete_by_id={int(x.get('shard_index',-1)):x for x in complete_docs}
    def idmap(paths):
        out={}
        for p in paths: out[int(p.stem.split('_')[1])]=p
        return out
    raw_by=idmap(raws); funnel_by=idmap(funnels); zero_by=idmap(zero_files); exc_by=idmap(exc_files)
    for sid in expected_ids:
        p=raw_by[sid]; m=manifest_by_id[sid]; c=complete_by_id[sid]; d=m.get('diagnostics',{}) or {}
        if file_sha(p)!=str(m.get('raw_sha256','')) or file_sha(p)!=str(c.get('raw_sha256','')): raise RuntimeError(f'raw sha mismatch shard {sid}')
        if str(m.get('engine_status','')).upper()!='VALID': raise RuntimeError(f'shard engine invalid {sid}')
        if str(m.get('control_flow',''))!='V49_64_SHARD_EXPORT_RETURN_MAIN_EXIT' or str(m.get('research_lane',''))!='SKIPPED' or str(m.get('telegram',''))!='SKIPPED' or str(m.get('post_export_network',''))!='FORBIDDEN': raise RuntimeError(f'shard isolation invalid {sid}')
        if str(c.get('status',''))!='SHARD_ONLY_COMPLETE' or file_sha(manifest_path_by_id[sid])!=str(c.get('manifest_sha256','')): raise RuntimeError(f'shard completion invalid {sid}')
        for key,path in [('funnel_sha256',funnel_by[sid]),('zero_audit_sha256',zero_by[sid]),('exceptions_sha256',exc_by[sid])]:
            if file_sha(path)!=str(d.get(key,'')): raise RuntimeError(f'diagnostic sha mismatch shard={sid} key={key}')

    raw_df=_read_csvs(raws,dtype={'code':str}); funnel_raw=_read_csvs(funnels); zero_df=_read_csvs(zero_files,dtype={'code':str}); exc_df=_read_csvs(exc_files,dtype={'code':str})
    if raw_df.empty: raise RuntimeError('all shard raw signals empty')
    if 'code' in raw_df: raw_df['code']=raw_df['code'].astype(str).str.zfill(6)
    defaults={'grade':'','score':0.0,'amount_b':0.0,'vol_ratio':0.0,'rule35_pnl':float('nan'),'ret_next_close':float('nan'),'hit3_before_stop':0,'band_type':''}
    for col,val in defaults.items():
        if col not in raw_df.columns: raw_df[col]=val
    canonical=s._v4940_canonical_df(raw_df); selected=s._select_backtest_top(canonical,top_per_strategy=max(1,args.top_per_strategy),all_candidates=False)
    if selected.empty: raise RuntimeError('global selected population empty')
    out=Path(args.output_dir); out.mkdir(parents=True,exist_ok=True)
    raw_path=out/'v49_64_global_raw.csv'; canonical_path=out/'v49_64_global_canonical.csv'; selected_path=out/'v49_64_global_selected_top5.csv'
    raw_df.to_csv(raw_path,index=False,encoding='utf-8-sig'); canonical.to_csv(canonical_path,index=False,encoding='utf-8-sig'); selected.to_csv(selected_path,index=False,encoding='utf-8-sig')

    # Funnel census and zero-mode classification.
    metrics=['eligible_dates','gate_admitted','predicate_called','predicate_hit_dates','predicate_hit_records','eval_success','eval_fail','exceptions','zero_audit_tested','zero_audit_hits']
    for c in metrics:
        if c not in funnel_raw: funnel_raw[c]=0
        funnel_raw[c]=pd.to_numeric(funnel_raw[c],errors='coerce').fillna(0).astype(int)
    funnel=funnel_raw.groupby('strategy',as_index=False)[metrics].sum()
    selected_counts=selected.get('mode',pd.Series(dtype=str)).astype(str).value_counts(); raw_counts=raw_df.get('mode',pd.Series(dtype=str)).astype(str).value_counts()
    zero_file_hits=(pd.to_numeric(zero_df.get('full_predicate_hit',pd.Series(0,index=zero_df.index)),errors='coerce').fillna(0).groupby(zero_df.get('strategy',pd.Series('',index=zero_df.index)).astype(str)).sum().to_dict() if not zero_df.empty else {})
    exc_file_counts=(exc_df.get('strategy',pd.Series('',index=exc_df.index)).astype(str).value_counts().to_dict() if not exc_df.empty else {})
    status_rows=[]; min_audit=int(getattr(s,'CLOSING_BET_V4964_ZERO_AUDIT_MIN_GLOBAL',100))
    for mode in s.CLOSING_BET_V4958_PRIMARY_PRIORITY:
        rr=funnel[funnel.strategy.eq(mode)]; vals={k:(int(rr.iloc[0][k]) if len(rr) else 0) for k in metrics}; sel=int(selected_counts.get(mode,0)); raw_n=int(raw_counts.get(mode,0))
        _funnel_zero=int(vals.get('zero_audit_hits',0)); _file_zero=int(zero_file_hits.get(mode,0) or 0); _funnel_exc=int(vals.get('exceptions',0)); _file_exc=int(exc_file_counts.get(mode,0) or 0)
        vals['zero_audit_hits']=max(_funnel_zero,_file_zero); vals['exceptions']=max(_funnel_exc,_file_exc)
        vals['diagnostic_consistent']=int(_funnel_zero==_file_zero and _funnel_exc==_file_exc)
        if vals['zero_audit_hits']>0: status='INVALID-FALSE-NEGATIVE'; reason='vector gate rejected a real full-predicate hit'
        elif vals['exceptions']>0: status='INVALID-PREDICATE-EXCEPTION'; reason='predicate exception rows present'
        elif sel>0: status='VALID'; reason='selected performance population exists'
        elif vals['predicate_called']<=0: status='ZERO-HIT-UNEXPLAINED'; reason='predicate was never called'
        elif vals['zero_audit_tested']>=min_audit or vals['gate_admitted']>=vals['eligible_dates']:
            status='ZERO-HIT-EXPLAINED'; reason='authoritative predicate called; rejected-date full audit found no hit'
        else: status='ZERO-HIT-UNEXPLAINED'; reason=f'zero audit {vals["zero_audit_tested"]} < required {min_audit}'
        if not vals['diagnostic_consistent']:
            status='INVALID-DIAGNOSTIC-MISMATCH'; reason=f'funnel/file mismatch zero {_funnel_zero}/{_file_zero}, exceptions {_funnel_exc}/{_file_exc}'
        status_rows.append({'strategy':mode,'raw_n':raw_n,'selected_n':sel,**vals,'status':status,'reason':reason})
    strategy_status=pd.DataFrame(status_rows)
    invalid=bool(strategy_status.status.str.startswith('INVALID').any()); partial=bool(strategy_status.status.eq('ZERO-HIT-UNEXPLAINED').any())
    funnel_status='INVALID' if invalid else ('PARTIAL-VALID' if partial else 'FULL-VALID')
    if zero_df.empty: zero_df=pd.DataFrame(columns=['strategy','code','name','signal_date','index','gate_admitted','full_predicate_hit','hit_records','error','status'])
    if exc_df.empty: exc_df=pd.DataFrame(columns=['strategy','code','name','signal_date','exception_type','exception_message','context'])
    funnel.to_csv(out/'v49_64_strategy_funnel.csv',index=False,encoding='utf-8-sig'); strategy_status.to_csv(out/'v49_64_strategy_status.csv',index=False,encoding='utf-8-sig'); zero_df.to_csv(out/'v49_64_zero_mode_audit.csv',index=False,encoding='utf-8-sig'); exc_df.to_csv(out/'v49_64_predicate_exceptions.csv',index=False,encoding='utf-8-sig')

    s.INDEX_MAP={str(k).zfill(6):str(v) for k,v in dict(universe.get('index_map') or {}).items()}; s.MARCAP_MAP={str(k).zfill(6):float(v or 0) for k,v in dict(universe.get('marcap_map') or {}).items()}; s._V4959_STRATEGY_ENGINE_AUDIT={'status':'VALID','detail':{'shard_count':args.shard_count,'global_fingerprint':preflight.get('universe_fingerprint')}}
    perf=s._v4959_build_common_performance_audit(raw_df,selected,args.start_date,args.end_date,{'source_codes':preflight.get('universe_count',0)}); perf_status=str(perf.get('status','INVALID'))
    generated_root=Path('reports')
    if out.resolve()!=generated_root.resolve():
        import shutil
        for gp in generated_root.glob('v49_64_common_strategy_*'): shutil.copy2(gp,out/gp.name)
    performance_csv=out/'v49_64_common_strategy_performance.csv'; portfolio_csv=out/'v49_64_common_strategy_portfolio.csv'; stress_csv=out/'v49_64_common_strategy_topwinner_stress.csv'; m5_csv=out/'v49_64_common_strategy_m5.csv'
    ptab=pd.read_csv(performance_csv) if performance_csv.exists() else pd.DataFrame(); port=pd.read_csv(portfolio_csv) if portfolio_csv.exists() else pd.DataFrame(); stress=pd.read_csv(stress_csv) if stress_csv.exists() else pd.DataFrame(); m5=pd.read_csv(m5_csv) if m5_csv.exists() else pd.DataFrame()
    allrow=ptab[ptab.strategy.eq('ALL')].iloc[0].to_dict() if not ptab.empty and ptab.strategy.eq('ALL').any() else {}; p1_50=port[(port.strategy=='ALL')&(port.day_limit==1)&(port.cost_bps==50)].iloc[0].to_dict() if not port.empty and len(port[(port.strategy=='ALL')&(port.day_limit==1)&(port.cost_bps==50)]) else {}; p1_20=port[(port.strategy=='ALL')&(port.day_limit==1)&(port.cost_bps==20)].iloc[0].to_dict() if not port.empty and len(port[(port.strategy=='ALL')&(port.day_limit==1)&(port.cost_bps==20)]) else {}
    lpwf=_lp_validation(selected,out,args.start_date,args.end_date); iit=_iit_split(selected,out,args.start_date,args.end_date)

    technical_status='INVALID' if preflight.get('status')!='VALID' or perf_status!='VALID' or funnel_status=='INVALID' else ('PARTIAL-VALID' if funnel_status=='PARTIAL-VALID' else 'FULL-VALID')
    lines=['(1/3)','⚙️ 공통 검색식 성과검증 | v49.64','──────────',f'버전: {s.CLOSING_BET_SCANNER_VERSION}',f'기간: {args.start_date} ~ {args.end_date} | prepared universe {preflight.get("universe_count")} · shards {args.shard_count}','[기술 검증]',f'- PREPARED UNIVERSE: VALID ✅ · fp {preflight.get("universe_fingerprint")}',f'- SHARD CONSENSUS: {len(manifests)}/{args.shard_count} VALID ✅ · global merge before TOP selection',f'- SEARCH CONTRACT: {"VALID ✅" if preflight.get("contract_valid") else "INVALID ⛔"} · explicit {preflight.get("explicit_hist_failures")} · boundary {preflight.get("boundary_failures")} · thread {preflight.get("thread_isolation_failures")} · determinism {preflight.get("determinism_failures")}',f'- STRATEGY POPULATION: {technical_status} · funnel {funnel_status} · false-negative {int(strategy_status.zero_audit_hits.sum())} · predicate exceptions {int(strategy_status.exceptions.sum())}',f'- GLOBAL PIPELINE: raw {len(raw_df)} → canonical {len(canonical)} → date×strategy TOP{args.top_per_strategy} {len(selected)}',f'- PERFORMANCE ENGINE: {perf_status} {"✅" if perf_status=="VALID" else "⛔"}','','[전체 성과]',f'- 거래평균 gross/net20/net50: {f(allrow.get("gross_mean_pct"))} / {f(allrow.get("net20_mean_pct"))} / {f(allrow.get("net50_mean_pct"))}',f'- OOS 평균 net20/net50: {f(allrow.get("oos_net20_mean_pct"))} / {f(allrow.get("oos_net50_mean_pct"))} · OOS n {int(allrow.get("oos_n",0) or 0)}',f'- 하루1종목 20bp: 누적 {f(p1_20.get("total"))} · MDD {f(p1_20.get("mdd"))}',f'- 하루1종목 50bp: 누적 {f(p1_50.get("total"))} · MDD {f(p1_50.get("mdd"))} · 양수월 {f(p1_50.get("positive_month"))}','','[운용 잠금]','- PAPER 유지 · 실제주문 0건','- FULL-VALID와 50bp OOS·MDD·대박제거를 함께 통과하기 전 LIVE 자동전환 금지']
    lines2=['(2/3)','📊 전략별 OOS | v49.64','──────────']
    if not ptab.empty:
        for _,r in ptab[ptab.strategy.ne('ALL')].sort_values(['oos_net50_mean_pct','oos_n'],ascending=[False,False]).iterrows():
            st=strategy_status[strategy_status.strategy.eq(r['strategy'])]; label=str(st.iloc[0].status) if len(st) else 'UNKNOWN'; lines2.append(f'- {r["strategy"]}: {label} · n {int(r["n"])} · OOS {int(r["oos_n"])} · net20/50 {f(r["oos_net20_mean_pct"])}/{f(r["oos_net50_mean_pct"])} · 전체50 {f(r["net50_mean_pct"])}')
    lines2+=['','- I/IT는 REAL_OR_CACHE 성과만 승격 검토 · proxy는 별도 표본','- Lifecycle/Runner/FAIL/BIG/Cluster는 이번 성과 전용 실행과 분리']
    lines3=['(3/3)','🔬 전략 Funnel·0건 원인 | v49.64','──────────']
    for _,r in strategy_status.iterrows(): lines3.append(f'- {r.strategy}: {r.status} · eligible {int(r.eligible_dates)} → gate {int(r.gate_admitted)} → call {int(r.predicate_called)} → hit {int(r.predicate_hit_records)} → eval {int(r.eval_success)} · zeroAudit {int(r.zero_audit_tested)}/{int(r.zero_audit_hits)} · exc {int(r.exceptions)}')
    if not lpwf.empty:
        valid=lpwf[lpwf.status.eq('VALID')]; lines3+=['',f'- LP WALK: folds {len(lpwf)} · VALID {len(valid)} · net50 양수 {int((valid.net50_mean_pct>0).sum())}/{len(valid)}' if len(valid) else '- LP WALK: 유효 fold 없음']
    real_iit=iit[iit.flow_group.eq('REAL_OR_CACHE')]; proxy_iit=iit[iit.flow_group.eq('PROXY')]
    lines3.append('- I/IT REAL: '+(' · '.join(f'{r.strategy} OOS n{int(r.oos_n)} net50 {f(r.oos_net50_mean_pct)}' for _,r in real_iit.iterrows()) or '없음'))
    lines3.append('- I/IT PROXY(승격제외): '+(' · '.join(f'{r.strategy} n{int(r.n)}' for _,r in proxy_iit.iterrows()) or '없음'))
    report='\n'.join(lines+['']+lines2+['']+lines3); (out/'v49_64_global_summary.txt').write_text(report,encoding='utf-8')
    manifest={'version':s.CLOSING_BET_SCANNER_VERSION,'status':technical_status,'funnel_status':funnel_status,'preflight':preflight,'shards':manifest_docs,'shard_completions':complete_docs,'global_raw_rows':len(raw_df),'global_canonical_rows':len(canonical),'global_selected_rows':len(selected),'performance_status':perf_status,'false_negative_hits':int(strategy_status.zero_audit_hits.sum()),'predicate_exceptions':int(strategy_status.exceptions.sum()),'files':{p.name:file_sha(p) for p in [raw_path,canonical_path,selected_path,out/'v49_64_strategy_funnel.csv',out/'v49_64_strategy_status.csv',out/'v49_64_zero_mode_audit.csv',out/'v49_64_predicate_exceptions.csv',out/'v49_64_lp_walk_forward.csv',out/'v49_64_iit_flow_split.csv']},'paper_only':True,'real_orders':0}
    (out/'v49_64_global_manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
    print(report); _deliver_telegram([lines,lines2,lines3],out,bool(args.send_telegram))
    return 3 if technical_status=='INVALID' else 0


if __name__=='__main__': sys.exit(main())
