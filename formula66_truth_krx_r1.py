#!/usr/bin/env python3
"""Immutable main7 66-COMBO + 6 AUX research, never a LIVE strategy.

Phases:
  contracts: repository static PRE timing and semantic question inventory;
  capture_truth: replay the originally captured PRE signal-map, NOT reconstructed later values;
  capture_krx_reprice: historical source signals independently repriced on verified
      original KRX all-market per-session OHLCV+actual Amount; NOT KRX signal rescreen.
Any missing evidence is UNKNOWN / a fail-closed diagnostic, never a false signal.
"""
from __future__ import annotations

import argparse
import ast
import csv
import gzip
import hashlib
import json
import math
import re
from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean, median

import pandas as pd

VERSION = 'STOCKHUNTER_MAIN7_66_KRX_FORMULA_TRUTH_R1'
RESEARCH_ONLY = True
PIN_MAIN7_GIT_BLOB = '8d210abdb3b01180fb0b5c108e36159f8deec527'
PIN_REGISTRY_GIT_BLOB = 'b477e069c310efed4011fe0a7f2ca394b35cdb72'
PIN_KRX_COLLECTOR_GIT_BLOB = '03ea1bee1ddc9ededb246c3c076908f58c343128'
PIN_KRX_REPRICE_ADAPTER_GIT_BLOB = 'e0fd01bcb6eb365bb55922a632a4eef18c5fe57c'
KRX_FULL_START = '2024-01-02'
KRX_FULL_END = '2026-09-30'
HORIZONS = (5, 10, 15)
ROUND_TRIP_COST_BPS = (20, 50, 100)  # illustrative stress only
REQUIRED_REGISTRY_KEYS = {'source_file', 'source_sha256', 'registry_sha256', 'combo_count', 'aux_count', 'combos', 'aux_selectors', 'producer_timing'}
CAPTURE_COLUMNS = {'code', 'signal_date', 'formula_truth_bitmap', 'formula_truth_inputs_json', 'formula_truth_registry_sha256'}


def fail(reason):
    raise RuntimeError('FORMULA66_FAIL_CLOSED_' + str(reason))


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def blob_sha(path):
    raw = Path(path).read_bytes()
    return hashlib.sha1(f'blob {len(raw)}\0'.encode() + raw).hexdigest()


def write_json(path, value):
    p=Path(path);p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=str)+'\n', encoding='utf-8')


def csv_out(path, rows, fields=None):
    p=Path(path);p.parent.mkdir(parents=True,exist_ok=True)
    rows=list(rows)
    if fields is None:
        fields=list(dict.fromkeys(k for r in rows for k in r)) or ['status']
    with p.open('w',encoding='utf-8-sig',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore');w.writeheader();w.writerows(rows)


def get_registry(registry_path, source_path=None, verify_pins=True):
    p=Path(registry_path)
    if not p.is_file(): fail('REGISTRY_MISSING')
    raw=p.read_bytes()
    try: data=json.loads(raw)
    except Exception: fail('REGISTRY_JSON_INVALID')
    missing=REQUIRED_REGISTRY_KEYS-set(data)
    if missing: fail('REGISTRY_SCHEMA_MISSING:'+','.join(sorted(missing)))
    if int(data.get('combo_count',-1))!=66 or int(data.get('aux_count',-1))!=6:fail('REGISTRY_66_6_IDENTITY_CHANGED')
    combos=data['combos'];aux=data['aux_selectors'];timing=data['producer_timing']
    if len(combos)!=66 or len(aux)!=6 or len(timing)!=66:fail('REGISTRY_ENUM_LENGTH_MISMATCH')
    ids=[x.get('index') for x in combos]
    if ids!=list(range(66)):fail('REGISTRY_FORMULA_ORDER_CHANGED')
    if len({str(x.get('combination')) for x in combos})!=66:fail('REGISTRY_DUPLICATE_FORMULA_NAME')
    if len({int(x.get('formula_index',-1)) for x in timing})!=66:fail('REGISTRY_TIMING_DUPLICATE_INDEX')
    for x,t in zip(combos,timing):
        if int(x['index'])!=int(t.get('formula_index',-1)) or x.get('combination')!=t.get('formula'):
            fail('REGISTRY_TIMING_FORMULA_JOIN_DRIFT')
        if not isinstance(x.get('referenced_keys'),list):fail('REGISTRY_MISSING_REFERENCED_KEYS')
        if not str(x.get('condition_source','')).lstrip().startswith('lambda e:'):
            fail('REGISTRY_CONDITION_NOT_LAMBDA:'+str(x.get('index')))
    if str(data.get('source_file'))!='main7_bugfix_2.py':fail('MAIN7_SOURCE_IDENTITY_CHANGED')
    status_counts=Counter(x.get('status','UNKNOWN') for x in timing)
    if verify_pins:
        if blob_sha(p)!=PIN_REGISTRY_GIT_BLOB:fail('REGISTRY_GIT_BLOB_DRIFT')
        if not source_path or not Path(source_path).is_file():fail('MAIN7_SOURCE_MISSING')
        if blob_sha(source_path)!=PIN_MAIN7_GIT_BLOB:fail('MAIN7_SOURCE_GIT_BLOB_DRIFT')
    data['_audit']={'registry_file_sha256':sha(raw), 'registry_git_blob_sha':blob_sha(p),
                    'main7_git_blob_sha':blob_sha(source_path) if source_path and Path(source_path).is_file() else '',
                    'producer_status_counts':dict(status_counts)}
    return data


def _classify(name):
    # Name taxonomy ONLY; it is not a claim that runtime charts satisfy the label.
    s=str(name)
    if any(k in s for k in ('종가배팅','종베')):return 'CLOSE_BET'
    if any(k in s for k in ('피보','피봇')):return 'FIB_SUPPORT'
    if 'BB' in s:return 'BOLLINGER'
    if '돌반지' in s:return 'DOLBANJI'
    if '삼각' in s or '수렴' in s:return 'TRIANGLE_CONVERGENCE'
    if '매집' in s or 'OBV' in s:return 'ACCUMULATION'
    if '수박' in s:return 'WATERMELON_FLOW'
    if '눌림' in s:return 'PULLBACK'
    if '돌파' in s or '발사' in s:return 'BREAKOUT'
    return 'OTHER'


def inventory(reg, out_dir):
    out=Path(out_dir);out.mkdir(parents=True,exist_ok=True)
    tmap={x['formula_index']:x for x in reg['producer_timing']}
    rows=[];issues=[];all_keys=set()
    for c in reg['combos']:
        i=c['index'];t=tmap[i];all_keys.update(c['referenced_keys'])
        concerns=list(t.get('post_evaluation_override_keys') or [])
        row={'formula_index':i,'formula':c['combination'],'name_category_hypothesis':_classify(c['combination']),
             'grade_display_only':c.get('grade',''),'legacy_base_score_not_performance':c.get('base_score',''),
             'condition_source':c['condition_source'],'referenced_keys':'|'.join(c['referenced_keys']),
             'source_line':c.get('source_line',''),'calculation_timing_status':t.get('status','UNKNOWN'),
             'post_evaluation_override_keys':'|'.join(concerns),
             'semantic_chart_match':'UNTESTED_NEEDS_BLIND_CHART_AUDIT',
             'krx_reproducible_signal':'UNTESTED_NEEDS_KRX_ONLY_RESCREEN',
             'krx_performance_status':'NOT_ESTABLISHED'}
        rows.append(row)
        if concerns:
            issues.append({'severity':'REVIEW','type':'STATIC_POST_EVALUATION_OVERRIDE',
                           'formula_index':i,'formula':c['combination'],
                           'detail':'|'.join(concerns),'decision':'MUST_COMPARE_ACTUAL_PRE_VS_POST_CAPTURE'})
    for a in reg['aux_selectors']:
        if not a.get('function_found'):
            issues.append({'severity':'REVIEW','type':'AUX_FUNCTION_NOT_FOUND_IN_REGISTRY_SOURCE',
                           'formula_index':'AUX','formula':a.get('group',''),
                           'detail':'runtime source resolution may differ',
                           'decision':'CHECK_RUNTIME_AUX_SELECTOR_SOURCE_AND_MEMBERSHIP'})
    csv_out(out/'formula66_static_inventory.csv',rows)
    csv_out(out/'formula66_static_review_items.csv',issues)
    aux_inventory=[{'aux_index':i,'aux_group':a.get('group',''),
                    'registry_source_found':int(bool(a.get('function_found'))),
                    'runtime_selector_membership':'UNPROVEN_REQUIRES_DIRECT_REPLAY',
                    'independent_krx_only_replay':'NOT_DONE',
                    'semantic_accuracy':'NEEDS_BLIND_REVIEW'} for i,a in enumerate(reg['aux_selectors'])]
    csv_out(out/'formula66_aux_six_inventory.csv',aux_inventory)
    
    assumptions=[{'question':'Does the formula select intended chart shape?',
                  'decision':'UNPROVEN_UNTIL_BLINDED_MANUAL_CHART_REVIEW'},
                 {'question':'Which true signals have KRX-only returns?',
                  'decision':'UNPROVEN_UNTIL_PINNED_SIGNAL_CAPTURE_AND_KRX_PRICES'},
                 {'question':'Can KRX original 224MA change across corporate action?',
                  'decision':'YES_POSSIBLE; DO_NOT_RECLASSIFY_WITHOUT_CORPORATE_ACTION_AUDIT'},
                 {'question':'Are all 66 formulas full-universe tested?',
                  'decision':'NO; a captured source must prove denominator coverage'},
                 {'question':'Are AUX 6 functions counted among 66?',
                  'decision':'NO; separate inventory and runtime coverage'},
                 {'question':'Was any historical signal selected after observing future price?',
                  'decision':'NOT_ESTABLISHED; must inspect capture as-of evidence'},
                 {'question':'Do results represent live-fillable returns?',
                  'decision':'NO; daily OHLCV cannot certify open/stop execution or PIT listings'}]
    csv_out(out/'formula66_research_questions.csv',assumptions)
    manifest={'revision':VERSION,'phase':'CONTRACTS_ONLY','status':'PASS_STATIC_INVENTORY_NO_PERFORMANCE_CLAIM',
              'strategy_ready':False,'live_changed':False,'trades_placed':0,
              'formula_count':66,'aux_count':6,'unique_formula_referenced_keys':len(all_keys),
              'static_timing_review_formulas':sum(bool(t.get('post_evaluation_override_keys')) for t in reg['producer_timing']),
              'aux_registry_function_not_found':sum(not a.get('function_found') for a in reg['aux_selectors']),
              'static_review_issue_count':len(issues),
              'provider_of_performance':'NOT_EVALUATED', 'source_authority':reg['_audit'],
              'warning':'Even 66/66 formulas indexed does not prove historic runtime activation or chart semantic correctness.'}
    write_json(out/'formula66_manifest.json',manifest)
    return manifest


ALLOWED_FUNCTIONS={'isinstance':isinstance,'int':int,'float':float,'str':str,'bool':bool,
                   'len':len,'abs':abs,'min':min,'max':max}
BANNED_NODES=(ast.Import,ast.ImportFrom,ast.Attribute,ast.Lambda,ast.Call,ast.NamedExpr,ast.ListComp,
              ast.SetComp,ast.DictComp,ast.GeneratorExp,ast.Await,ast.Yield,ast.YieldFrom)


def safe_formula(condition):
    """Compile a pinned lambda with narrow AST validation. No arbitrary imports / calls."""
    try:tree=ast.parse(str(condition),mode='eval')
    except Exception as e:fail('CONDITION_AST_INVALID:'+type(e).__name__)
    if not isinstance(tree.body,ast.Lambda) or [a.arg for a in tree.body.args.args]!=['e'] or tree.body.args.defaults:
        fail('CONDITION_NOT_SINGLE_E_LAMBDA')
    for node in ast.walk(tree.body.body):
        if isinstance(node,ast.Call):
            f=node.func
            if isinstance(f,ast.Name) and f.id in ALLOWED_FUNCTIONS:pass
            elif isinstance(f,ast.Attribute) and f.attr=='get' and isinstance(f.value,ast.Name) and f.value.id=='e':pass
            else:fail('CONDITION_UNSAFE_CALL')
        elif isinstance(node,ast.Attribute):
            if not (node.attr=='get' and isinstance(node.value,ast.Name) and node.value.id=='e'):
                fail('CONDITION_UNSAFE_ATTRIBUTE')
        elif isinstance(node,(ast.NamedExpr,ast.ListComp,ast.SetComp,ast.DictComp,ast.GeneratorExp,ast.Await,
                              ast.Yield,ast.YieldFrom,ast.Lambda)):
            fail('CONDITION_UNSAFE_NODE:'+type(node).__name__)
        elif isinstance(node,ast.Name) and node.id not in {'e',*ALLOWED_FUNCTIONS}:
            fail('CONDITION_UNSAFE_NAME:'+node.id)
    return eval(compile(tree,'<main7-pinned-formula>','eval'),{'__builtins__':{},**ALLOWED_FUNCTIONS},{})


def _load_inputs(raw):
    if raw is None:return {}
    try:x=json.loads(raw) if isinstance(raw,str) else raw
    except Exception:fail('CAPTURE_INPUT_JSON_INVALID')
    if not isinstance(x,dict):fail('CAPTURE_INPUT_JSON_NOT_OBJECT')
    return {str(k):v for k,v in x.items() if v!='__MISSING__'}


def parse_capture(path,reg):
    p=Path(path)
    if not p.is_file() or not p.stat().st_size:fail('CAPTURE_FILE_MISSING')
    try: frame=pd.read_csv(p,dtype=str,keep_default_na=False,low_memory=False)
    except pd.errors.EmptyDataError:fail('CAPTURE_EMPTY_CANNOT_PROVE_ZERO_SIGNALS')
    missing=CAPTURE_COLUMNS-set(frame)
    if missing:fail('CAPTURE_REQUIRED_COLUMNS_MISSING:'+','.join(sorted(missing)))
    if frame.empty:fail('CAPTURE_EMPTY_CANNOT_PROVE_ZERO_SIGNALS')
    if not frame['formula_truth_registry_sha256'].eq(str(reg['registry_sha256'])).all():
        fail('CAPTURE_REGISTRY_SHA_CHANGED_OR_MISSING')
    if not frame['formula_truth_bitmap'].str.fullmatch('[TFE]{66}').all():
        fail('CAPTURE_PRE_BITMAP_NOT_EXACT_66_TFE')
    if not bool(frame['code'].str.fullmatch(r'[0-9][0-9A-Z]{5}').all()):
        fail('CAPTURE_BAD_CODE')
    dates=pd.to_datetime(frame['signal_date'],errors='coerce',format='%Y-%m-%d')
    if dates.isna().any():fail('CAPTURE_BAD_SIGNAL_DATE')
    frame['signal_date']=dates.dt.strftime('%Y-%m-%d')
    frame['combo_invocation']=pd.to_numeric(frame.get('combo_invocation',pd.Series(['0']*len(frame),index=frame.index)),errors='coerce').fillna(0)
    frame=frame.sort_values(['signal_date','code','combo_invocation'],kind='stable')
    dup=frame.duplicated(['signal_date','code'],keep=False)
    collision=0
    if dup.any():
        cols=['formula_truth_bitmap','formula_truth_inputs_json','formula_truth_registry_sha256']
        if 'formula_post_truth_bitmap' in frame:cols.append('formula_post_truth_bitmap')
        for _,grp in frame[dup].groupby(['signal_date','code']):
            if any(grp[c].nunique()!=1 for c in cols):collision+=1
    if collision:fail('CONFLICTING_DUPLICATE_CAPTURE_SAME_CODE_DATE:'+str(collision))
    unique=frame.drop_duplicates(['signal_date','code'],keep='first').reset_index(drop=True)
    return unique,{'raw_invocations':len(frame),'deduped_rows':len(unique),'duplicate_identical':len(frame)-len(unique),'collision':collision,'source_sha256':sha(p.read_bytes())}


def replay_truth(frame,reg,out_dir):
    """Stream 66*event full truth to gzip while keeping only actual TRUE event rows in RAM.

    LARGE RUN SAFETY: full-universe historical input can have 50K+ event keys.
    Never create a giant 66*event Python list in memory.
    """
    out=Path(out_dir);out.mkdir(parents=True,exist_ok=True)
    formulas=reg['combos']
    engines=[safe_formula(x['condition_source']) for x in formulas]
    buckets=[{'total':0,'hit':0,'error':0,'mismatch':0,'post_only':0,'codes':set(),'days':set()} for _ in range(66)]
    errors=[];event_rows=[];true_events=[];blind_samples=defaultdict(list)
    mismatch=post_only_count=total_true=total_evals=pre_error_count=0
    fields=['code','signal_date','formula_index','formula','name_category_hypothesis',
            'pre_captured','post_captured','recomputed_pre','recomputed_post',
            'pre_match','post_only_true','valid_timing_consistent_pre']
    stream=out/'formula66_capture_truth_per_event_formula.csv.gz'
    with gzip.open(stream,'wt',encoding='utf-8-sig',newline='',compresslevel=6) as f:
        writer=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore');writer.writeheader()
        for tuple_row in frame.itertuples(index=False,name='CapturedEvent'):
            row=tuple_row._asdict()
            pre=_load_inputs(row['formula_truth_inputs_json'])
            post_s=str(row.get('formula_post_truth_bitmap','')).strip()
            if post_s and not re.fullmatch(r'[TFE]{66}',post_s):fail('CAPTURE_POST_BITMAP_NOT_EXACT_66_TFE')
            post_in=row.get('formula_post_truth_inputs_json','')
            post=_load_inputs(post_in) if post_in else None
            bits=row['formula_truth_bitmap'];day=row['signal_date'];code=row['code']
            event={'code':code,'signal_date':day,
                   'pre_hit_formulas':bits.count('T'),
                   'PRE_CAPTURED_SIGNAL_NOT_KRX_ONLY_RESCREEN':1,
                   'analyze_returned':str(row.get('analyze_returned','')).lower() in ('true','1','y'),
                   'captured_runtime_consistent':1}
            for i,c in enumerate(formulas):
                b=buckets[i];b['total']+=1;total_evals+=1
                bit=bits[i];post_bit=post_s[i] if post_s else 'UNKNOWN'
                key={'code':code,'signal_date':day,'formula_index':i,'formula':c['combination'],
                     'name_category_hypothesis':_classify(c['combination']),
                     'pre_captured':bit,'post_captured':post_bit}
                try: actual=bool(engines[i](pre));reconstructed='T' if actual else 'F'
                except Exception as ex:
                    reconstructed='E'
                    if len(errors)<5000:errors.append({**key,'exception':type(ex).__name__+':'+str(ex)[:180]})
                key['recomputed_pre']=reconstructed
                key['pre_match']=int(bit==reconstructed)
                if not key['pre_match']:
                    mismatch+=1;b['mismatch']+=1;event['captured_runtime_consistent']=0
                if post is not None:
                    try:key['recomputed_post']='T' if bool(engines[i](post)) else 'F'
                    except Exception:key['recomputed_post']='E'
                else:key['recomputed_post']='UNKNOWN'
                post_only=int(bit!='T' and post_s!='' and post_bit=='T')
                key['post_only_true']=post_only
                key['valid_timing_consistent_pre']=int(bit=='T' and reconstructed=='T')
                if bit=='E':b['error']+=1;pre_error_count+=1
                if bit=='T':
                    b['hit']+=1;total_true+=1;b['codes'].add(code);b['days'].add(day)
                    true_events.append(key.copy())
                b['post_only']+=post_only;post_only_count+=post_only
                if reconstructed==bit and bit in {'T','F'}:
                    limit=5 if bit=='T' else 2
                    sortkey=sha((str(i)+'|'+code+'|'+day).encode())
                    lst=blind_samples[(i,bit)]
                    lst.append((sortkey,code,day,c['combination']))
                    if len(lst)>limit:
                        lst.sort(key=lambda x:x[0]);lst.pop()
                writer.writerow(key)
            event_rows.append(event)
    summary=[]
    for c in formulas:
        i=c['index'];b=buckets[i]
        summary.append({'formula_index':i,'formula':c['combination'],
                        'name_category_hypothesis':_classify(c['combination']),
                        'captured_universe_rows':b['total'],'captured_pre_true':b['hit'],
                        'captured_pre_error':b['error'],'recompute_pre_mismatch':b['mismatch'],
                        'post_only_became_true':b['post_only'],
                        'code_count':len(b['codes']), 'signal_day_count':len(b['days']),
                        'status':'INVALID_PRE_TRUTH' if b['mismatch'] else 'CAPTURE_TRUTH_PASSED_SEMANTICS_UNPROVEN',
                        'performance':'PENDING_KRX_PRICE_REPRICE_AND_KRX_ONLY_RESCREEN'})
    csv_out(out/'formula66_capture_event_keys.csv',event_rows)
    csv_out(out/'formula66_capture_formula_summary.csv',summary)
    csv_out(out/'formula66_capture_recalc_errors.csv',errors)
    blind=[]
    for i,c in enumerate(formulas):
        for truth,role in [('T','PRE_TRUE_FROM_CAPTURE'),('F','PRE_FALSE_CONTROL_WITHIN_CAPTURE_ONLY')]:
            for _,code,day,name in sorted(blind_samples.get((i,truth),[])):
                blind.append({'formula_index':i,'formula':name,'code':code,'signal_date':day,
                              'review_role':role, 'chart_outcomes_hidden':1,
                              'review_is_unbiased_full_market_sample':0,
                              'question':'Does the chart at signal date match the claimed named pattern?',
                              'manual_grade':'NOT_YET_REVIEWED'})
    csv_out(out/'formula66_blind_chart_review_queue.csv',blind)
    status='PASS_CAPTURE_PRE_RECALC_NOT_PRICE_VERIFIED' if not mismatch else 'FAIL_CAPTURE_PRE_RECALC_DISAGREES'
    doc={'status':status,'captured_rows':len(frame),'formula_evaluations':total_evals,
         'pre_mismatch':mismatch,'pre_captured_error_count':pre_error_count,
         'condition_exceptions_logged':len(errors),
         'post_only_true_total':post_only_count,'captured_pre_true_total':total_true,
         'full_pre_matrix_filename':stream.name,
         'krx_price_confirmed':False,'strategy_ready':False,
         'full_universe_coverage':'UNPROVEN_FROM_CAPTURE_ONLY',
         'event_keys':len(event_rows),
         'warning':'Only original PRE matches are eligible. Never treat POST-only TRUE as historical signal. Not full KRX market universe unless independently proven.'}
    write_json(out/'formula66_capture_manifest.json',doc)
    if mismatch:fail('CAPTURE_PRE_RECALC_MISMATCH:'+str(mismatch))
    if pre_error_count:fail('CAPTURE_PRE_CONDITION_ERRORS:'+str(pre_error_count))
    return doc,true_events


def read_official_index(root):
    p=Path(root)/'shard_00'/'official_indices.csv'
    if not p.is_file():fail('KRX_INDEX_FILE_MISSING')
    df=pd.read_csv(p,dtype={'date':str,'index_code':str},low_memory=False)
    if not {'date','index','index_code','close'}.issubset(df):fail('KRX_INDEX_BAD_SCHEMA')
    if df.duplicated(['date','index']).any():fail('KRX_INDEX_DUPLICATE')
    result={}
    for m in ['KOSPI','KOSDAQ']:
        q=df[df['index']==m].copy().sort_values('date')
        q['close']=pd.to_numeric(q['close'],errors='coerce')
        if q.empty or q['close'].isna().any() or q['close'].le(0).any():fail('KRX_INDEX_BAD_VALUE_'+m)
        q['ret20_pct']=(q['close']/q['close'].shift(20)-1)*100
        for r in q.to_dict('records'):
            rt=float(r['ret20_pct']) if pd.notna(r['ret20_pct']) else float('nan')
            state='UNKNOWN' if not math.isfinite(rt) else 'BULL_RET20_GTE_5' if rt>=5 else 'BEAR_RET20_LTE_MINUS5' if rt<=-5 else 'SIDEWAYS_RET20_BETWEEN'
            result[(str(r['date']),m)]={'regime':state,'index_close':float(r['close']),'index_ret20_pct':rt}
    return result


def krx_reprice(loaded_events,reg,krx_root,ref_path,out_dir):
    from research_core224.krx_truth_r1 import verify_and_load,sample_outcome
    out=Path(out_dir);out.mkdir(parents=True,exist_ok=True)
    codes={r['code'] for r in loaded_events if r['pre_captured']=='T' and r['recomputed_pre']=='T'}
    if not codes:fail('NO_PRE_TRUE_EVENTS_FOR_KRX_REPRICE')
    if not Path(krx_root).is_dir():fail('KRX_FULL_RAW_SHARDS_MISSING')
    all_days,by_code,coverage,day_records=verify_and_load(krx_root,ref_path,KRX_FULL_START,KRX_FULL_END,wanted_codes=codes,expected_shards=8)
    if len(all_days)<224:fail('KRX_SESSION_WARMUP_TOO_SHORT')
    idx=read_official_index(krx_root)
    keyset={(r['code'],r['signal_date']) for r in loaded_events if r['pre_captured']=='T'}
    outcomes={}
    for code,day in sorted(keyset):
        if day not in day_records:
            outcomes[(code,day)]={'status':'OUTSIDE_PINNED_KRX_2024_2026_PERIOD'}
        else: outcomes[(code,day)]=sample_outcome(code,day,by_code,all_days,h=15)
    rows=[];report=[]
    for r in loaded_events:
        if r['pre_captured']!='T' or r['recomputed_pre']!='T':continue
        a=dict(r)
        o=outcomes[(r['code'],r['signal_date'])]
        a.update({'price_source':'KRX_DAY_ALL_ORIGINAL_UNADJUSTED',
                  'signal_source':'LEGACY_CAPTURE_NOT_KRX_ONLY_RESCREEN',
                  'outcome_status':o.get('status','UNKNOWN'),
                  'tradeability':'UNKNOWN_OFFICIAL_PIT_NOT_PROVEN'})
        for h in HORIZONS:
            gross=o.get(f'd{h}_close_pct')
            a[f'd{h}_gross_pct']=gross
            for bp in ROUND_TRIP_COST_BPS:
                a[f'd{h}_net_{bp}bps_pct']=(gross-bp/100) if gross is not None else ''
            a[f'd{h}_mfe_pct']=o.get(f'd{h}_mfe_pct','')
            a[f'd{h}_mae_pct']=o.get(f'd{h}_mae_pct','')
        for m in ['KOSPI','KOSDAQ']:
            ix=idx.get((r['signal_date'],m),{'regime':'UNKNOWN','index_ret20_pct':float('nan')})
            a[f'{m.lower()}_regime_at_signal']=ix['regime']
            a[f'{m.lower()}_ret20_pct_at_signal']=ix['index_ret20_pct']
        rows.append(a)
    csv_out(out/'formula66_krx_event_repricing.csv',rows)
    # Blind review chart bars end AT signal day. Future bars remain in outcomes file only.
    blind_path=out/'formula66_blind_chart_review_queue.csv'
    bar_rows=[]
    if blind_path.is_file():
        blind=pd.read_csv(blind_path,dtype={'code':str,'signal_date':str},low_memory=False)
        for rr in blind.to_dict('records'):
            code=str(rr['code']);day=str(rr['signal_date'])
            hist=[x for x in by_code.get(code,[]) if x['date']<=day][-260:]
            # Raw KRX has valid suspended bars with zeros; do not erase them.
            for k,z in enumerate(hist):
                bar_rows.append({'code':code,'signal_date':day,'formula_index':rr['formula_index'],
                                 'formula':rr['formula'],'review_role':rr['review_role'],
                                 'date':z['date'],'bar_before_or_on_signal':1,
                                 'open':z['open'],'high':z['high'],'low':z['low'],
                                 'close':z['close'],'volume':z['volume'],'krx_actual_amount':z['amount'],
                                 'observed_raw_krx_nonzero_trades':int(all(float(z[a])>0 for a in ('open','high','low','close','volume','amount'))),
                                 'review_state':'NO_FUTURE_OUTCOME_IN_THIS_FILE'})
    csv_out(out/'formula66_blind_review_krx_pre_signal_bars.csv',bar_rows)
    
    grouped=defaultdict(list)
    for r in rows:
        for h in HORIZONS:grouped[(r['formula_index'],r['formula'],h,'ALL')].append(r)
        for m in ['KOSPI','KOSDAQ']:
            for h in HORIZONS:
                grouped[(r['formula_index'],r['formula'],h,m+'_REGIME_'+str(r[f'{m.lower()}_regime_at_signal']))].append(r)
    for (i,f,h,regime),batch in sorted(grouped.items()):
        val=[float(x[f'd{h}_net_20bps_pct']) for x in batch if x.get('outcome_status')=='COMPLETE_RAW_KRX_PRICE_ONLY_NOT_EXECUTABLE' and x.get(f'd{h}_net_20bps_pct') not in ('',None)]
        valid_dates=len({x['signal_date'] for x in batch if x.get('outcome_status')=='COMPLETE_RAW_KRX_PRICE_ONLY_NOT_EXECUTABLE'})
        report.append({'formula_index':i,'formula':f,'horizon':h,'market_regime_hypothesis':regime,
                       'captured_true_rows':len(batch),'krx_price_path_completed':len(val),
                       'blocked_missing_or_corp_action':len(batch)-len(val),
                       'distinct_dates_total':len({x['signal_date'] for x in batch}),
                       'distinct_dates_completed':valid_dates,
                       'n_unique_codes_total':len({x['code'] for x in batch}),
                       'win_net20bps_rate_pct':100*sum(x>0 for x in val)/len(val) if val else '',
                       'mean_net20bps_pct':mean(val) if val else '',
                       'median_net20bps_pct':median(val) if val else '',
                       'status':'EXPLORATORY_NOT_ACTIONABLE_NOT_KRX_RESCREEN' if val else 'INSUFFICIENT_KRX_OUTCOME',
                       'statistical_multiple_testing':'66_FORMULAS_X_MULTIPLE_REGIMES_NOT_CORRECTED_YET'})
    csv_out(out/'formula66_krx_repricing_by_formula_regime.csv',report)
    complete=sum(r['outcome_status']=='COMPLETE_RAW_KRX_PRICE_ONLY_NOT_EXECUTABLE' for r in rows)
    manifest={'status':'KRX_REPRICING_OF_LEGACY_CAPTURE_COMPLETE_NOT_SIGNAL_AUTHORITY',
              'source':'KRX per-session original + actual Amount; legacy input ONLY defines PRE historical cohort',
              'events_with_pre_TRUE_formula':len(rows),'unique_event_code_dates':len(keyset),
              'complete_formula_event_price_paths':complete,'unresolved_paths':len(rows)-complete,
              'historical_krx_sessions':len(all_days),'historical_krx_source_sha256_by_session':len(day_records),
              'strategies_proven_profitable':0,'strategy_ready':False,'full_market_pit_strategy_ready':False,
              'lookahead_and_original_signal_quality':'NOT_PROVEN',
              'corporate_actions_official_PIT':'NOT_FULLY_PROVEN',
              'warning':'Same legacy historical signals can be repriced by KRX, but this DOES NOT prove KRX native rescreen or independent OOS profitability.'}
    write_json(out/'formula66_krx_manifest.json',manifest)
    return manifest


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--mode',choices=['contracts','capture_truth','capture_krx_reprice'],default='contracts')
    p.add_argument('--registry',default='search_formula_contract_registry.json')
    p.add_argument('--main7',default='main7_bugfix_2.py')
    p.add_argument('--out',default='reports/formula66_krx_r1')
    p.add_argument('--capture',default='')
    p.add_argument('--krx-root',default='')
    p.add_argument('--krx-reference',default='research_pit/historical_market_counts_20190102_20261012.csv')
    p.add_argument('--allow-fixture-unpinned',action='store_true',help=argparse.SUPPRESS)
    args=p.parse_args(argv)
    if args.allow_fixture_unpinned:fail('PRODUCTION_PIN_BYPASS_FORBIDDEN')
    reg=get_registry(args.registry,args.main7,verify_pins=True)
    inventory(reg,args.out)
    if args.mode in ('capture_truth','capture_krx_reprice'):
        frame,capmeta=parse_capture(args.capture,reg)
        write_json(Path(args.out)/'formula66_capture_ingest.json',capmeta)
        replaymeta,events=replay_truth(frame,reg,args.out)
        if args.mode=='capture_krx_reprice':
            if not Path('research_core224/krx_truth_r1.py').is_file() or blob_sha('research_core224/krx_truth_r1.py')!=PIN_KRX_REPRICE_ADAPTER_GIT_BLOB:
                fail('KRX_REPRICE_ADAPTER_MISSING_OR_DRIFT')
            if blob_sha('research_pit/krx_full_market_pit.py')!=PIN_KRX_COLLECTOR_GIT_BLOB:
                fail('KRX_COLLECTOR_SOURCE_DRIFT')
            krx_reprice(events,reg,args.krx_root,args.krx_reference,args.out)
    print('FORMULA66_RESEARCH_R1',json.dumps({'mode':args.mode,'out':args.out,'status':'RESEARCH_ONLY_FINISHED',
         'strategy_ready':False,'formulas':66,'aux':6},ensure_ascii=False),flush=True)
    return 0


if __name__=='__main__':
    raise SystemExit(main())
