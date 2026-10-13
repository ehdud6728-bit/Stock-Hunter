#!/usr/bin/env python3
"""Read-only source forensic for CORE224 LIVE date routing. No imports of application code."""
import argparse, ast, csv, json, os, pathlib, re, hashlib, datetime
ROOT=pathlib.Path('.')
TERMS=('V1081_DIRECT_DATE_MODE','V1081_DIRECT_MAX_DATES','V1080_BACKTEST_HOLD_DAYS','V1080_BACKTEST_WEEKS','V25_TWO_YEAR_END_DATE','V25_CURRENT_LIVE_END_DATE','WEEK_LAST','_v1081_make_signal_rows_for_asof','selected_dates','asof_dates','V23_SHARD_WORKER_ONLY')
FILES=('main7_bugfix_2.py','direct_replay_performance_v20.py','direct_replay_sharding_v22.py','direct_replay_materialized_v23.py','original_thesis_reconstruction.py','.github/workflows/run_scanner.yml','apply_stockhunter_research_patch.py')

def main():
    p=argparse.ArgumentParser();p.add_argument('--target',default='2026-10-12');p.add_argument('--out',default='reports/core224_live_date_forensic_r2');a=p.parse_args()
    target=datetime.date.fromisoformat(a.target); out=pathlib.Path(a.out);out.mkdir(parents=True,exist_ok=True)
    findings=[];functions=[];inventory=[]
    for filename in FILES:
        path=ROOT/filename
        if not path.is_file():inventory.append({'file':filename,'status':'MISSING','bytes':0,'sha256':''});continue
        data=path.read_bytes();content=data.decode('utf-8',errors='replace');lines=content.splitlines()
        inventory.append({'file':filename,'status':'PRESENT','bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()})
        if filename.endswith('.py'):
            try:
                tree=ast.parse(content,filename=filename)
                for node in ast.walk(tree):
                    if isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef)):
                        body='\n'.join(lines[node.lineno-1:min(node.end_lineno or node.lineno,len(lines))]);score=sum(s in body for s in TERMS)
                        if score:
                            functions.append({'file':filename,'name':node.name,'first_line':node.lineno,'last_line':node.end_lineno or node.lineno,'term_hits':score,'signature':lines[node.lineno-1][:220]})
            except (SyntaxError,ValueError) as e:
                inventory.append({'file':filename,'status':'PARSE_WARNING:'+type(e).__name__,'bytes':len(data),'sha256':''})
        for i,line in enumerate(lines):
            hits=[t for t in TERMS if t in line]
            if hits:
                findings.append({'file':filename,'line':i+1,'terms':','.join(hits),'source':line[:450]})
    def save(name,headers,rows):
        with (out/name).open('w',encoding='utf-8-sig',newline='') as f:
            w=csv.DictWriter(f,fieldnames=headers);w.writeheader();w.writerows(rows)
    save('source_inventory.csv',['file','status','bytes','sha256'],inventory)
    save('date_routing_source_hits.csv',['file','line','terms','source'],findings)
    save('date_routing_functions.csv',['file','name','first_line','last_line','term_hits','signature'],sorted(functions,key=lambda x:-x['term_hits']))
    # Interpret explicitly what is proven, not what is merely suspected.
    source={x['file']:x for x in inventory}
    flags={
        'target_date':str(target),
        'main_source_present':source.get('main7_bugfix_2.py',{}).get('status')=='PRESENT',
        'main_source_bytes':source.get('main7_bugfix_2.py',{}).get('bytes',0),
        'date_selection_exactly_reproduced':False,
        'date_selection_cause_proven':False,
        'production_changed':False,
        'research_only':True,
        'next_action':'REVIEW_SOURCE_HITS_AND_FUNCTIONS; DO_NOT_APPLY_LIVE_PATCH_WITHOUT_CAUSE_PROOF',
        'historical_observation':{'run_id':'38279295289','expected':'2026-10-12','selected':'2026-09-17','note':'FROM_PRIOR_LOG_NOT_R2_REPRODUCTION'},
        'critical_guard':'EXACT_DATE_MISMATCH_MUST_REMAIN_FAIL_CLOSED',
        'confidence':'STATIC_SOURCE_FORENSIC_ONLY'
    }
    (out/'summary.json').write_text(json.dumps(flags,ensure_ascii=False,indent=2),encoding='utf-8')
    with (out/'source_context.txt').open('w',encoding='utf-8') as f:
        for hit in findings:
            if hit['file']=='main7_bugfix_2.py' and hit['terms']:
                path=ROOT/hit['file'];lines=path.read_text(encoding='utf-8',errors='replace').splitlines()
                i=hit['line']-1;lo=max(0,i-5);hi=min(len(lines),i+7)
                f.write('\n### {}:{} [{}]\n'.format(hit['file'],i+1,hit['terms']))
                for k in range(lo,hi):f.write(f'{k+1:>6} {lines[k][:450]}\n')
    print('CORE224_R2_STATIC_FORENSIC',json.dumps(flags,ensure_ascii=False),flush=True)
    print('CORE224_R2_SOURCE_HITS',len(findings),'FUNCTIONS',len(functions),'MAIN_PRESENT',flags['main_source_present'],flush=True)
    return 0
if __name__=='__main__':raise SystemExit(main())
