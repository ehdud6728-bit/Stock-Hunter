from __future__ import annotations
import hashlib, importlib.util, inspect, json, math, os, re, sys
from pathlib import Path
from typing import Any
import pandas as pd

VERSION='V73.3.6.6.6'
RESEARCH_ONLY=True
HEADER='🧾 [전체 검색식 계산 진실성 전수감사 · RESEARCH_ONLY]'
REGISTRY_FILE='search_formula_contract_registry.json'
REPORT_FILE='v72_search_formula_truth_report_block.txt'
BOOL_EXCEPTIONS={'triangle_apex','dolbanzi_Count','maejip_score_now','maejip_recent3','closing_bet_grade','style'}
ANCHOR_COLUMNS=['triangle_start_date','triangle_end_date','wave1_low_date','wave1_high_date','pullback_low_date','restart_signal_date','bb40_lower_break_date','bb40_reclaim_date','ma5_break_date','ma5_reclaim_date']


def _root(): return Path(__file__).resolve().parent

def _safe(v:Any):
    if v is None:return None
    if isinstance(v,(bool,int,str)):return v
    if isinstance(v,float):return None if math.isnan(v) or math.isinf(v) else round(v,8)
    try:
        if pd.isna(v):return None
    except Exception:pass
    if hasattr(v,'item'):
        try:return _safe(v.item())
        except Exception:pass
    if isinstance(v,(list,tuple,set)):return [_safe(x) for x in v]
    if isinstance(v,dict):return {str(k):_safe(x) for k,x in v.items()}
    return str(v)

def _loads(v,default):
    if isinstance(v,(dict,list)):return v
    try:return json.loads(str(v))
    except Exception:return default

def load_registry(path:str|Path=''):
    p=Path(path) if path else _root()/REGISTRY_FILE
    return json.loads(p.read_text(encoding='utf-8'))

_REG=load_registry()
REGISTRY_SHA=_REG.get('registry_sha256','')


def capture_truth(signals:dict,combo_table:list[dict]):
    """Evaluate every COMBO_TABLE condition on the exact effective signal map.
    Returns compact audit metadata only; it never changes ranking or filtering.
    """
    reg=_REG['combos']; bits=[]; true_rows=[]; errors=[]; missing={}; values={}
    all_keys=sorted({k for c in reg for k in c.get('referenced_keys',[])})
    for k in all_keys:
        if k not in signals: missing[k]='KEY_ABSENT'
        values[k]=_safe(signals.get(k)) if k in signals else '__MISSING__'
    for i,c in enumerate(reg):
        status='F'; score=None
        try:
            combo=combo_table[i]
            ok=bool(combo['cond'](signals))
            if ok:
                status='T'
                try: score=combo['score_fn'](signals) if 'score_fn' in combo else combo.get('score')
                except Exception as e:
                    errors.append({'index':i,'formula':c['combination'],'stage':'score_fn','error':f'{type(e).__name__}:{e}'})
                true_rows.append({'index':i,'formula':c['combination'],'score':_safe(score)})
        except Exception as e:
            status='E';errors.append({'index':i,'formula':c['combination'],'stage':'condition','error':f'{type(e).__name__}:{e}'})
        bits.append(status)
    return {
        'registry_sha256':REGISTRY_SHA,
        'bitmap':''.join(bits),
        'true_formulas':[x['formula'] for x in true_rows],
        'true_scores':true_rows,
        'errors':errors,
        'missing_keys':missing,
        'inputs':values,
    }


def attach_result(result:dict,truth:dict):
    result=dict(result or {})
    result['formula_truth_registry_sha256']=truth.get('registry_sha256','')
    result['formula_truth_bitmap']=truth.get('bitmap','')
    result['formula_truth_true']=' / '.join(truth.get('true_formulas',[]))
    result['formula_truth_scores_json']=json.dumps(truth.get('true_scores',[]),ensure_ascii=False,separators=(',',':'))
    result['formula_truth_errors_json']=json.dumps(truth.get('errors',[]),ensure_ascii=False,separators=(',',':'))
    result['formula_truth_missing_json']=json.dumps(truth.get('missing_keys',{}),ensure_ascii=False,separators=(',',':'))
    result['formula_truth_inputs_json']=json.dumps(truth.get('inputs',{}),ensure_ascii=False,separators=(',',':'))
    return result




def attach_post_result(result:dict,truth:dict):
    result=dict(result or {})
    result['formula_post_truth_registry_sha256']=truth.get('registry_sha256','')
    result['formula_post_truth_bitmap']=truth.get('bitmap','')
    result['formula_post_truth_true']=' / '.join(truth.get('true_formulas',[]))
    result['formula_post_truth_scores_json']=json.dumps(truth.get('true_scores',[]),ensure_ascii=False,separators=(',',':'))
    result['formula_post_truth_errors_json']=json.dumps(truth.get('errors',[]),ensure_ascii=False,separators=(',',':'))
    result['formula_post_truth_missing_json']=json.dumps(truth.get('missing_keys',{}),ensure_ascii=False,separators=(',',':'))
    result['formula_post_truth_inputs_json']=json.dumps(truth.get('inputs',{}),ensure_ascii=False,separators=(',',':'))
    return result

def _col(df,*names):
    return next((n for n in names if n in df.columns),None)

def _split_matches(v):
    return [x.strip() for x in re.split(r'\s*/\s*|\s*\|\s*',str(v or '')) if x.strip()]

def _num(s):return pd.to_numeric(s,errors='coerce')

def _perf(q):
    r=_num(q.get('next3_close_ret',pd.Series(dtype=float))).dropna()
    if r.empty:return {'n':len(q),'d3_mean':float('nan'),'d3_median':float('nan'),'d3_trim10':float('nan'),'d3_ex_top2':float('nan')}
    x=r.sort_values();k=int(len(x)*.10);trim=x.iloc[k:len(x)-k] if len(x)-2*k>0 else x
    ex=x.iloc[:-2] if len(x)>2 else pd.Series(dtype=float)
    return {'n':len(q),'d3_mean':r.mean(),'d3_median':r.median(),'d3_trim10':trim.mean(),'d3_ex_top2':ex.mean() if len(ex) else float('nan')}


def _runtime_aux_source_contract(out:Path):
    """Resolve auxiliary selector source at runtime without inventing missing code.
    Existing GitHub repositories may carry triangle_combo_analyzer.py even when a release
    overlay ZIP intentionally does not replace it.
    """
    rows=[]
    root=_root()
    tri_path=root/'triangle_combo_analyzer.py'
    tri_mod=None
    if tri_path.is_file():
        try:
            spec=importlib.util.spec_from_file_location('_v733665_triangle_contract',tri_path)
            if spec and spec.loader:
                tri_mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(tri_mod)
        except Exception:
            tri_mod=None
    for a in _REG.get('aux_selectors',[]):
        fn_name=str(a.get('function','')).split('[')[0]
        source='';source_file='';found=False;reason=''
        if fn_name in ('build_triangle_pre_squeeze_top3_df','build_triangle_squeeze_top5_df'):
            fn=getattr(tri_mod,fn_name,None) if tri_mod else None
            source_file=str(tri_path) if tri_path.is_file() else ''
            if callable(fn):
                try:source=inspect.getsource(fn);found=True
                except Exception as e:reason=f'INSPECT_ERROR:{type(e).__name__}'
            else:reason='TRIANGLE_SOURCE_MISSING_OR_FUNCTION_ABSENT'
        else:
            # These selectors are embedded in main7_bugfix_2.py and already hashed in registry.
            found=bool(a.get('function_found'));source_file=str(root/'main7_bugfix_2.py') if found else '';reason='' if found else 'MAIN_FUNCTION_NOT_FOUND'
            source_sha=str(a.get('source_sha256',''))
            rows.append({**{k:v for k,v in a.items() if k not in ('source_excerpt','source_sha256')},'runtime_found':found,'runtime_source_file':source_file,'runtime_source_sha256':source_sha,'runtime_reason':reason})
            continue
        rows.append({**{k:v for k,v in a.items() if k not in ('source_excerpt','source_sha256')},'runtime_found':found,'runtime_source_file':source_file,'runtime_source_sha256':hashlib.sha256(source.encode()).hexdigest() if source else '', 'runtime_reason':reason})
    df=pd.DataFrame(rows)
    df.to_csv(out/'v72_search_formula_aux_selector_runtime_source_contract.csv',index=False,encoding='utf-8-sig')
    return df


def _aux_runtime_membership(out:Path):
    src=out/'v72_aux_candidate_group_shadow_eval.csv'
    if not src.exists():
        df=pd.DataFrame(columns=['group','rows','codes','dates','source_status'])
        df.to_csv(out/'v72_search_formula_aux_runtime_membership_audit.csv',index=False,encoding='utf-8-sig')
        return df
    try:q=pd.read_csv(src)
    except Exception:
        q=pd.DataFrame()
    rows=[]
    if not q.empty:
        gcol=next((c for c in ['v7337_candidate_groups','candidate_groups','후보군'] if c in q.columns),None)
        if gcol:
            ex=[]
            for _,r in q.iterrows():
                for g in [x for x in str(r.get(gcol,'')).split('|') if x and x not in ('MAIN_TOP15','OBSERVE_ANY')]:
                    z=r.to_dict();z['_group']=g;ex.append(z)
            e=pd.DataFrame(ex)
            if not e.empty:
                ccol=next((c for c in ['code','Code','종목코드'] if c in e.columns),None);dcol=next((c for c in ['signal_date','날짜'] if c in e.columns),None)
                for g,z in e.groupby('_group'):
                    rows.append({'group':g,'rows':len(z),'codes':z[ccol].nunique() if ccol else None,'dates':z[dcol].nunique() if dcol else None,'source_status':'RUNTIME_MEMBERSHIP_AVAILABLE'})
    df=pd.DataFrame(rows)
    df.to_csv(out/'v72_search_formula_aux_runtime_membership_audit.csv',index=False,encoding='utf-8-sig')
    return df


def _serialize_inventory(out:Path):
    inv=[]
    producers=_REG.get('signal_producers',{})
    for c in _REG['combos']:
        src=[]
        for k in c.get('referenced_keys',[]):
            for p in producers.get(k,[]):src.append(f"{k}:{p.get('kind')}@{p.get('line')}={p.get('source','')}")
        inv.append({**{k:v for k,v in c.items() if k!='condition_normalized'},'producer_contract':' || '.join(src)})
    pd.DataFrame(inv).to_csv(out/'v72_search_formula_contract_inventory.csv',index=False,encoding='utf-8-sig')
    pd.DataFrame(_REG.get('duplicates',[])).to_csv(out/'v72_search_formula_static_duplicate_audit.csv',index=False,encoding='utf-8-sig')
    timing=[]
    for x in _REG.get('producer_timing',[]):
        timing.append({**{k:v for k,v in x.items() if k!='post_evaluation_overrides'},'post_evaluation_overrides_json':json.dumps(x.get('post_evaluation_overrides',[]),ensure_ascii=False)})
    pd.DataFrame(timing).to_csv(out/'v72_search_formula_producer_timing_audit.csv',index=False,encoding='utf-8-sig')
    aux=[{k:v for k,v in x.items() if k!='source_excerpt'} for x in _REG.get('aux_selectors',[])]
    pd.DataFrame(aux).to_csv(out/'v72_search_formula_aux_selector_contract.csv',index=False,encoding='utf-8-sig')
    sem=[{k:v for k,v in x.items() if k!='source_excerpt'} for x in _REG.get('semantic_helpers',[])]
    pd.DataFrame(sem).to_csv(out/'v72_search_formula_helper_semantic_contract.csv',index=False,encoding='utf-8-sig')


def run_backtest(eval_df:pd.DataFrame|None,output_dir:str='reports'):
    out=Path(output_dir or 'reports');out.mkdir(parents=True,exist_ok=True);_serialize_inventory(out)
    aux_source_df=_runtime_aux_source_contract(out)
    aux_membership_df=_aux_runtime_membership(out)
    df=eval_df.copy() if isinstance(eval_df,pd.DataFrame) else pd.DataFrame()
    bcol=_col(df,'검색식진실비트맵','formula_truth_bitmap')
    icol=_col(df,'검색식원천값JSON','formula_truth_inputs_json')
    ecol=_col(df,'검색식오류JSON','formula_truth_errors_json')
    mcol=_col(df,'검색식매칭','search_pattern_matches')
    pcol=_col(df,'검색식대표','search_pattern_primary','N조합')
    rcol=_col(df,'검색식진실레지스트리','formula_truth_registry_sha256')
    tcol=_col(df,'검색식삼각원천JSON','formula_triangle_native_json')
    pbcol=_col(df,'검색식사후진실비트맵','formula_post_truth_bitmap')
    pecol=_col(df,'검색식사후오류JSON','formula_post_truth_errors_json')
    picol=_col(df,'검색식사후원천값JSON','formula_post_truth_inputs_json')
    prcol=_col(df,'formula_post_truth_registry_sha256','검색식사후진실레지스트리')
    datecol=_col(df,'signal_date','날짜','replay_asof_date')
    codecol=_col(df,'code','Code','종목코드')
    runtime=[];recon=[];types=[];manual=[];temporal=[]
    full_coverage=0;post_full_coverage=0;registry_mismatch=0;post_registry_mismatch=0
    for ridx,r in df.iterrows():
        bitmap=str(r.get(bcol,'') or '') if bcol else ''
        post_bitmap=str(r.get(pbcol,'') or '') if pbcol else ''
        if len(bitmap)==len(_REG['combos']):full_coverage+=1
        if len(post_bitmap)==len(_REG['combos']):post_full_coverage+=1
        regsha=str(r.get(rcol,'') or '') if rcol else ''
        post_regsha=str(r.get(prcol,'') or '') if prcol else ''
        if regsha and regsha!=REGISTRY_SHA:registry_mismatch+=1
        if post_regsha and post_regsha!=REGISTRY_SHA:post_registry_mismatch+=1
        inputs=_loads(r.get(icol,'{}') if icol else '{}',{})
        errors=_loads(r.get(ecol,'[]') if ecol else '[]',[])
        post_inputs=_loads(r.get(picol,'{}') if picol else '{}',{})
        post_errors=_loads(r.get(pecol,'[]') if pecol else '[]',[])
        rec=set(_split_matches(r.get(mcol,''))) if mcol else set()
        primary=str(r.get(pcol,'') or '') if pcol else ''
        true_names=[]
        true_scores=_loads(r.get('검색식진실점수JSON',r.get('formula_truth_scores_json','[]')),[])
        scoremap={str(x.get('formula')):x.get('score') for x in true_scores if isinstance(x,dict)}
        for c in _REG['combos']:
            i=c['index'];status=bitmap[i] if i<len(bitmap) else 'U'
            if status=='T':true_names.append(c['combination'])
            missing=[k for k in c['referenced_keys'] if inputs.get(k,'__MISSING__')=='__MISSING__']
            post_status=post_bitmap[i] if i<len(post_bitmap) else 'U'
            runtime.append({'row_id':ridx,'signal_date':r.get(datecol,'') if datecol else '','code':r.get(codecol,'') if codecol else '', 'formula_index':i,'formula':c['combination'],'pre_bitmap_complete':len(bitmap)==len(_REG['combos']),'post_bitmap_complete':len(post_bitmap)==len(_REG['combos']),'truth_status':status,'post_truth_status':post_status,'post_eval_changed':status!=post_status and post_status!='U','became_true_after_evaluation':status!='T' and post_status=='T','recorded_match':c['combination'] in rec,'representative':c['combination']==primary,'missing_keys':'|'.join(missing),'condition_error':next((e.get('error','') for e in errors if e.get('formula')==c['combination']),''),'post_condition_error':next((e.get('error','') for e in post_errors if e.get('formula')==c['combination']),''),'base_score_runtime':scoremap.get(c['combination']), 'next3_close_ret':r.get('next3_close_ret'), 'market_regime':r.get('market_regime',r.get('v733663_market_regime',''))})
            for k in c['referenced_keys']:
                v=inputs.get(k,'__MISSING__'); expected='NONBOOL_ALLOWED' if k in BOOL_EXCEPTIONS else 'BOOL'
                actual='MISSING' if v=='__MISSING__' else type(v).__name__
                bad=expected=='BOOL' and v!='__MISSING__' and not isinstance(v,bool)
                types.append({'row_id':ridx,'formula':c['combination'],'signal_key':k,'expected_type':expected,'actual_type':actual,'value':v,'type_anomaly':bad})
        true_set=set(true_names)
        omitted=true_set-rec
        false_recorded=rec-true_set
        expected_primary=''
        scored=[(float(scoremap.get(n,-1e18) or -1e18),n) for n in true_names]
        if scored:expected_primary=max(scored,key=lambda x:x[0])[1]
        recon.append({'row_id':ridx,'signal_date':r.get(datecol,'') if datecol else '','code':r.get(codecol,'') if codecol else '', 'true_count':len(true_set),'recorded_count':len(rec),'true_not_recorded':'|'.join(sorted(omitted)),'recorded_but_false':'|'.join(sorted(false_recorded)),'primary':primary,'expected_highest_base_score':expected_primary,'primary_base_score_mismatch':bool(expected_primary and primary and expected_primary!=primary),'registry_sha256':regsha})
        # temporal native evidence: do not invent absent anchors.
        tri=_loads(r.get(tcol,'{}') if tcol else '{}',{})
        anchor={k:tri.get(k) for k in ANCHOR_COLUMNS if k in tri}
        temporal.append({'row_id':ridx,'signal_date':r.get(datecol,'') if datecol else '','code':r.get(codecol,'') if codecol else '', 'native_anchor_keys':'|'.join(anchor.keys()),'native_anchor_count':len(anchor),'status':'AVAILABLE' if anchor else 'NATIVE_ANCHOR_MISSING','native_anchor_json':json.dumps(anchor,ensure_ascii=False)})
    rdf=pd.DataFrame(runtime);rcf=pd.DataFrame(recon);tdf=pd.DataFrame(types);tempdf=pd.DataFrame(temporal)
    summary=[]
    if not rdf.empty:
        for c in _REG['combos']:
            q=rdf[rdf['formula'].eq(c['combination'])]
            tq=q[q['truth_status'].eq('T')]
            z={'formula_index':c['index'],'formula':c['combination'],'grade':c['grade'],'base_score':c['base_score'],'evaluated_rows':len(q),'true_rows':len(tq),'post_true_rows':int(q['post_truth_status'].eq('T').sum()),'post_eval_change_rows':int(q['post_eval_changed'].sum()),'became_true_after_evaluation_rows':int(q['became_true_after_evaluation'].sum()),'hit_rate_pct':len(tq)/len(q)*100 if len(q) else float('nan'),'condition_error_rows':int(q['truth_status'].eq('E').sum()),'missing_input_rows':int(q['missing_keys'].astype(str).ne('').sum()),'recorded_true_rows':int((q['truth_status'].eq('T')&q['recorded_match']).sum()),'truth_record_mismatch_rows':int((q['truth_status'].eq('T')&~q['recorded_match']).sum()+(q['truth_status'].ne('T')&q['recorded_match']).sum())}
            z.update(_perf(tq.rename(columns={'next3_close_ret':'next3_close_ret'})))
            summary.append(z)
    sdf=pd.DataFrame(summary)
    # Manual chart sample manifest from every formula that fired.
    if not rdf.empty:
        for formula,q in rdf[rdf['truth_status'].eq('T')].groupby('formula'):
            q=q.copy();q['_r']=_num(q['next3_close_ret'])
            picks=[]
            for label,qq in [('BEST',q.sort_values('_r',ascending=False)),('WORST',q.sort_values('_r')),('NEUTRAL',q.assign(_abs=q['_r'].abs()).sort_values('_abs'))]:
                if not qq.empty:
                    x=qq.iloc[0];key=(x.get('signal_date',''),x.get('code',''))
                    if key not in [(p[1].get('signal_date',''),p[1].get('code','')) for p in picks]:picks.append((label,x))
            for label,x in picks:
                manual.append({'formula':formula,'sample_role':label,'signal_date':x.get('signal_date',''),'code':x.get('code',''),'next3_close_ret':x.get('next3_close_ret'),'review_items':'formula visual match|source values|anchor order|market regime|resistance space|volume contraction'})
    mdf=pd.DataFrame(manual)
    for name,d in [('v72_search_formula_runtime_truth_audit.csv',rdf),('v72_search_formula_formula_summary.csv',sdf),('v72_search_formula_reconciliation_audit.csv',rcf),('v72_search_formula_input_type_audit.csv',tdf),('v72_search_formula_temporal_anchor_audit.csv',tempdf),('v72_search_formula_manual_chart_sample_manifest.csv',mdf)]:
        d.to_csv(out/name,index=False,encoding='utf-8-sig')
    combo_n=len(_REG['combos']);aux_n=len(_REG.get('aux_selectors',[]))
    aux_source_missing=aux_source_df.loc[~aux_source_df['runtime_found'].astype(bool),'group'].astype(str).tolist() if not aux_source_df.empty else [x.get('group','') for x in _REG.get('aux_selectors',[]) if not x.get('function_found')]
    never=int((sdf['true_rows'].eq(0)).sum()) if not sdf.empty else combo_n
    errors_n=int(sdf['condition_error_rows'].sum()) if not sdf.empty else 0
    missing_n=int(sdf['missing_input_rows'].sum()) if not sdf.empty else 0
    recon_bad=int((rcf['recorded_but_false'].astype(str).ne('')|rcf['primary_base_score_mismatch']).sum()) if not rcf.empty else 0
    post_change_rows=int(rdf['post_eval_changed'].sum()) if not rdf.empty else 0
    post_became_true=int(rdf['became_true_after_evaluation'].sum()) if not rdf.empty else 0
    static_late=[x for x in _REG.get('producer_timing',[]) if x.get('status')!='OK']
    helper_found=[x for x in _REG.get('semantic_helpers',[]) if x.get('found')]
    helper_no_order=[x for x in helper_found if not x.get('uses_date_order')]
    anchors_ok=int(tempdf['status'].eq('AVAILABLE').sum()) if not tempdf.empty else 0
    min_required=math.ceil(len(df)*0.95) if len(df) else 0
    audit_valid=bool(len(df)>0 and full_coverage>=min_required and registry_mismatch==0)
    invalid_reasons=[]
    if len(df)==0: invalid_reasons.append('NO_EVAL_ROWS')
    if full_coverage<min_required: invalid_reasons.append(f'PRE_BITMAP_COVERAGE_{full_coverage}/{len(df)}')
    if registry_mismatch: invalid_reasons.append(f'PRE_REGISTRY_MISMATCH_{registry_mismatch}')
    lines=[HEADER,
           f'📌 {VERSION} · FULL_SEARCH_FORMULA_TRUTH_TEMPORAL_PROVENANCE_AUDIT · RESEARCH_ONLY=True',
           f"- AUDIT STATUS: {'✅ VALID' if audit_valid else '⛔ INVALID'} · reason {','.join(invalid_reasons) if invalid_reasons else 'OK'}",
           f'- 전수 범위: COMBO_TABLE {combo_n}개 + AUX/LIVE selector {aux_n}개 = 총 {combo_n+aux_n}개 · registry {REGISTRY_SHA[:16]}…',
           f'- Runtime truth PRE: 분석 {len(df)}행 · 완전 비트맵 {full_coverage}행 · registry mismatch {registry_mismatch}행',
           f'- Runtime truth POST: 완전 비트맵 {post_full_coverage}행 · registry mismatch {post_registry_mismatch}행',
           f'- 공식 상태: 실제 점등 0회 식 {never}/{combo_n} · condition error {errors_n}건 · 입력누락 formula-row {missing_n}건',
           f'- 매칭 회계: 대표/기록 불일치 행 {recon_bad} · 원천 anchor AVAILABLE {anchors_ok}/{len(tempdf)}',
           f'- 계산순서 감사: 정적 post-eval override 식 {len(static_late)}개 · runtime 판정변경 {post_change_rows} formula-row · 뒤늦게 TRUE {post_became_true}건',
           f"- 시간순서 helper 감사: 원문확보 {len(helper_found)}개 · 명시적 날짜/순서 확인 불가 {len(helper_no_order)}개(자동 오류판정 아님, 수동검토 대상)",
           f"- AUX 소스계약: runtime 원문 미확인 {','.join(aux_source_missing) if aux_source_missing else '-'} · 외부 triangle_combo_analyzer가 있으면 함수 원문 SHA까지 고정",
           f"- AUX runtime membership: 평가가능 그룹 {len(aux_membership_df)}개 · 원장 v72_aux_candidate_group_shadow_eval.csv 연동",
           '- 원칙: 성과가 나쁘다는 이유로 식을 즉시 수정하지 않고, 조건 원문→원천값→판정→기록매칭→시간순서가 일치하는지 먼저 확인합니다.']
    if not audit_valid:
        lines += ['⛔ [해석 차단]','- 전수감사 입력계약이 충족되지 않았으므로 검색식별 TRUE/FALSE·FAIL-CALC·PERFORMANCE_FAIL 판정을 금지합니다.','- 다른 독립 연구 블록은 유지하되 이 감사의 검색식 성과표는 근거로 사용하지 않습니다.']
    if audit_valid and not sdf.empty:
        active=sdf.sort_values(['true_rows','truth_record_mismatch_rows'],ascending=[False,False]).head(8)
        lines.append('🔍 [점등빈도 상위 식]')
        for _,x in active.iterrows():lines.append(f"- {x['formula']}: true {int(x['true_rows'])}/{int(x['evaluated_rows'])} · mismatch {int(x['truth_record_mismatch_rows'])} · D3중앙 {x['d3_median']:+.2f}%" if not pd.isna(x['d3_median']) else f"- {x['formula']}: true {int(x['true_rows'])}/{int(x['evaluated_rows'])} · mismatch {int(x['truth_record_mismatch_rows'])}")
    lines += ['🧭 [전수검사 판정 규칙]','- FAIL-CALC: condition error·원천키 누락·기록은 TRUE인데 재계산 FALSE·시간순서 위반','- REVIEW: 점등 0회·비정상 고빈도·다른 상위식에 계속 가려짐·native anchor 미저장','- PERFORMANCE_FAIL: 계산진실성 PASS 이후 OOS 중앙/절사/상위2개 제외/비용후 초과수익이 모두 열위','- LIVE 변경 금지 · selector/점수/진입/익절/손절 자동변경 0', '- Actions CSV: v72_search_formula_contract_inventory.csv · producer_timing_audit.csv · runtime_truth_audit.csv · formula_summary.csv · reconciliation_audit.csv · input_type_audit.csv · temporal_anchor_audit.csv · manual_chart_sample_manifest.csv']
    report='\n'.join(lines)
    (out/REPORT_FILE).write_text(report,encoding='utf-8')
    return report,{'inventory':pd.DataFrame(_REG['combos']),'runtime':rdf,'summary':sdf,'reconciliation':rcf,'types':tdf,'temporal':tempdf,'manual':mdf,'audit_valid':audit_valid,'pre_full_coverage':full_coverage,'post_full_coverage':post_full_coverage}


def force_report(text:str,output_dir:str='reports',eval_df:pd.DataFrame|None=None):
    s=str(text or '')
    if HEADER in s:
        st=s.find(HEADER);ends=[s.find(h,st+1) for h in ['\n🌙 [전일 야간환경','\n🏆 [V48/V61','\n🛡️ [손절거리'] if s.find(h,st+1)>=0]
        s=s[:st].rstrip()+(('\n\n'+s[min(ends):].lstrip('\n')) if ends else '')
    if eval_df is not None:
        block,_=run_backtest(eval_df,output_dir)
    else:
        p=Path(output_dir or 'reports')/REPORT_FILE
        block=p.read_text(encoding='utf-8') if p.exists() else '\n'.join([HEADER,f'📌 {VERSION} · RESEARCH_ONLY=True','- 아직 실행 원장이 없습니다. DIRECT_REPLAY 재생성 필요'])
    anchors=['\n🌙 [전일 야간환경','\n🏆 [V48/V61','\n🛡️ [손절거리']
    pos=[s.find(a) for a in anchors if s.find(a)>=0]
    return s[:min(pos)].rstrip()+'\n\n'+block+'\n'+s[min(pos):] if pos else s.rstrip()+'\n\n'+block
