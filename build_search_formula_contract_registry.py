from __future__ import annotations
import ast, hashlib, json, re
from pathlib import Path

VERSION = "V73.3.6.6.5"
AUX_SELECTORS = [
    {"group":"PRE_TRIANGLE","function":"build_triangle_pre_squeeze_top3_df","source_kind":"GLOBAL_OR_TRIANGLE_MODULE","native_anchor_required":True},
    {"group":"TRIANGLE_PULLBACK","function":"build_triangle_squeeze_top5_df","source_kind":"GLOBAL_OR_TRIANGLE_MODULE","native_anchor_required":True},
    {"group":"VALUE_WAVE_BB40","function":"_v1046_build_ymgp_bb40_top5_df","source_kind":"LIVE_SELECTOR","native_anchor_required":True},
    {"group":"WATERMELON_PULLBACK","function":"build_watermelon_state_top5[4]","source_kind":"LIVE_SELECTOR_TUPLE","native_anchor_required":True},
    {"group":"DANTE_WATCH","function":"build_watermelon_state_top5[9]","source_kind":"LIVE_SELECTOR_TUPLE","native_anchor_required":False},
    {"group":"BREAKOUT_WATCH","function":"build_watermelon_state_top5[11]","source_kind":"LIVE_SELECTOR_TUPLE","native_anchor_required":True},
]

def seg(src,node):
    return ast.get_source_segment(src,node) or ""

def norm(s):
    return re.sub(r"\s+","",s or "")

def keys_from_lambda(node):
    out=[]
    for n in ast.walk(node):
        if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and n.func.attr=='get' and isinstance(n.func.value,ast.Name) and n.func.value.id=='e' and n.args and isinstance(n.args[0],ast.Constant):
            out.append(str(n.args[0].value))
        elif isinstance(n,ast.Subscript) and isinstance(n.value,ast.Name) and n.value.id=='e':
            sl=n.slice
            if isinstance(sl,ast.Constant): out.append(str(sl.value))
    return sorted(set(out))

def find_function(tree,name):
    for n in tree.body:
        if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)) and n.name==name:return n
    return None

def build(source: Path, out: Path):
    src=source.read_text(encoding='utf-8')
    tree=ast.parse(src,filename=str(source))
    combos=[]
    assign=None
    for n in tree.body:
        if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='COMBO_TABLE' for t in n.targets): assign=n
    if assign is None or not isinstance(assign.value,ast.List): raise RuntimeError('COMBO_TABLE_NOT_FOUND')
    for i,e in enumerate(assign.value.elts):
        if not isinstance(e,ast.Dict):continue
        d={}
        for k,v in zip(e.keys,e.values):
            if isinstance(k,ast.Constant):d[str(k.value)]=v
        name=ast.literal_eval(d['combination'])
        cond=seg(src,d['cond'])
        combos.append({
            'index':i,'combination':name,
            'grade':ast.literal_eval(d['grade']),
            'base_score':ast.literal_eval(d['score']),
            'type':ast.literal_eval(d['type']),
            'condition_source':cond,
            'condition_normalized':norm(cond),
            'referenced_keys':keys_from_lambda(d['cond']),
            'source_line':getattr(e,'lineno',None),
            'score_fn_source':seg(src,d['score_fn']) if 'score_fn' in d else '',
            'tag_fn_source':seg(src,d['tag_fn']) if 'tag_fn' in d else '',
        })
    # Map default signal producer expressions and later overrides.
    producers={}
    fn=find_function(tree,'build_default_signals')
    if fn:
        for n in ast.walk(fn):
            if isinstance(n,ast.Return) and isinstance(n.value,ast.Dict):
                for k,v in zip(n.value.keys,n.value.values):
                    if isinstance(k,ast.Constant):
                        producers.setdefault(str(k.value),[]).append({'kind':'DEFAULT','line':getattr(v,'lineno',None),'source':seg(src,v)})
    for n in ast.walk(tree):
        if isinstance(n,ast.Assign):
            for t in n.targets:
                if isinstance(t,ast.Subscript) and isinstance(t.value,ast.Name) and t.value.id=='signals' and isinstance(t.slice,ast.Constant):
                    producers.setdefault(str(t.slice.value),[]).append({'kind':'OVERRIDE','line':getattr(n,'lineno',None),'source':seg(src,n.value)})
    # Evaluation-order contract: signal overrides that happen after winner selection
    # cannot affect the actual COMBO_TABLE result in the current implementation.
    eval_line=None
    for n in ast.walk(tree):
        if isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='judge_trade_with_sequence':
            eval_line=getattr(n,'lineno',None);break
    producer_timing=[]
    for c in combos:
        late=[]
        for key in c.get('referenced_keys',[]):
            for pr in producers.get(key,[]):
                if pr.get('kind')=='OVERRIDE' and eval_line and (pr.get('line') or 0)>eval_line:
                    late.append({'key':key,**pr})
        producer_timing.append({'formula':c['combination'],'formula_index':c['index'],'evaluation_line':eval_line,'post_evaluation_overrides':late,'post_evaluation_override_keys':sorted({x['key'] for x in late}),'status':'REVIEW_POST_EVAL_OVERRIDE' if late else 'OK'})

    # Static duplicate contracts.
    dup=[]
    by={}
    for c in combos: by.setdefault(c['condition_normalized'],[]).append(c['combination'])
    for k,v in by.items():
        if k and len(v)>1:dup.append({'kind':'EXACT_CONDITION_DUPLICATE','formulas':' | '.join(v),'condition_normalized':k})
    names={}
    for c in combos:names.setdefault(c['combination'],0);names[c['combination']]+=1
    for name,n in names.items():
        if n>1:dup.append({'kind':'DUPLICATE_NAME','formulas':name,'count':n})
    # Auxiliary function source contracts.
    aux=[]
    for a in AUX_SELECTORS:
        base=a['function'].split('[')[0]
        f=find_function(tree,base)
        source_text=seg(src,f) if f else ''
        aux.append({**a,'function_found':bool(f),'source_line':getattr(f,'lineno',None) if f else None,'source_sha256':hashlib.sha256(source_text.encode()).hexdigest() if source_text else '', 'source_excerpt':source_text[:12000]})
    # Known semantic review points are audit flags, not automatic logic changes.
    semantic=[]
    helper_names=['check_force_pullback','check_bb40_second_wave','check_watermelon_relaunch','check_obv_acc_breakout','check_bb40_ross','check_bb40_rsi_div','check_bb40_reclaim_rsi_div','jongbe_triangle_combo_v3']
    for h in helper_names:
        f=find_function(tree,h); text=seg(src,f) if f else ''
        semantic.append({'helper':h,'found':bool(f),'source_line':getattr(f,'lineno',None) if f else None,'source_sha256':hashlib.sha256(text.encode()).hexdigest() if text else '', 'uses_any':'.any()' in text,'uses_shift':'shift(' in text,'uses_date_order':any(tok in text for tok in ['idxmax','idxmin','get_loc','< pull','< peak','low_date','high_date']), 'source_excerpt':text[:14000]})
    payload={'version':VERSION,'source_file':source.name,'source_sha256':hashlib.sha256(src.encode()).hexdigest(),'combo_count':len(combos),'aux_count':len(aux),'combos':combos,'signal_producers':producers,'evaluation_line':eval_line,'producer_timing':producer_timing,'duplicates':dup,'aux_selectors':aux,'semantic_helpers':semantic}
    canonical=json.dumps(payload,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()
    payload['registry_sha256']=hashlib.sha256(canonical).hexdigest()
    out.write_text(json.dumps(payload,ensure_ascii=False,indent=2),encoding='utf-8')
    return payload

if __name__=='__main__':
    root=Path(__file__).resolve().parent
    p=build(root/'main7_bugfix_2.py',root/'search_formula_contract_registry.json')
    print(f"SEARCH_FORMULA_REGISTRY version={VERSION} combo={p['combo_count']} aux={p['aux_count']} sha256={p['registry_sha256']}")
