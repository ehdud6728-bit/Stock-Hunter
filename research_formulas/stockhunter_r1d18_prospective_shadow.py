#!/usr/bin/env python3
"""R1-D18, research-only protocol and future REAL_FULL snapshot capture.

Never submits orders, modifies ranks, refetches prices, evaluates future P&L,
or regenerates historical as-used labels. Capture is an auditable, post-close
prospective feature log; new OOS performance requires later independent D+5/D+10.
"""
from __future__ import annotations
import argparse, csv, datetime as dt, hashlib, io, json, math, re, sys, zipfile
from pathlib import Path
from zoneinfo import ZoneInfo

PROTOCOL_ID = "STOCKHUNTER_R1D18_PROSPECTIVE_PREDECLARED_SHADOW"
MAIN_OVERLAY = "reports/real_full_research_overlay_r1/real_full_research_overlay.csv"
OVERLAY_META = "reports/real_full_research_overlay_r1/real_full_research_overlay_meta.json"
SOURCE_META = "reports/real_full_trust_source_meta.json"
POST_GUARD = "reports/real_full_source_postrun_guard.json"
BASE_GRADES = "reports/r1d17/result/R1D17_HISTORIC_GRADE_180_SIGNALS.csv"
BASE_OUTCOMES = "reports/r1d17/result/R1D17_R143_MATCHED_HORIZON_345_ROWS.csv"
BASE_MANIFEST = "reports/r1d17/result/R1D17_MANIFEST.json"
BOOL_TRUE = {"true", "1"}
BOOL_FALSE = {"false", "0"}
REQUIRED_FEATURES = ("ma_compression", "shallow_pullback", "gradual_accumulation", "liquidity_retained", "strong_wave1")


def dump(path, value):
    p=Path(path); p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)+"\n",encoding="utf-8")


def load_protocol(path):
    p=json.loads(Path(path).read_text(encoding="utf-8"))
    assert p["research_id"]==PROTOCOL_ID and p["authority"]=="RESEARCH_ONLY", "WRONG_PROTOCOL"
    assert p["training_cutoff"] < p["effective_signal_date_min"], "OVERLAPPING_TRAIN_TEST"
    assert p["treatment_primary"] == "ma_compression AND shallow_pullback", "TREATMENT_DRIFT"
    assert p["primary_horizon_trading_days"]==5, "HORIZON_DRIFT"
    assert len(p["four_groups"])==4, "GROUPS_DRIFT"
    c=p["research_constraints"]
    assert all(c.values()), "RESEARCH_SAFETY_GUARD"
    assert p["baseline_pin"]["run_id"] == 40141210428, "BASELINE_RUN_CHANGED"
    assert re.fullmatch('[0-9a-f]{64}',p['baseline_pin']['zip_sha256']), "BASELINE_DIGEST_INVALID"
    return p


def source_bytes(source,member):
    p=Path(source)
    if p.is_dir():
        f=p/member
        if not f.is_file(): raise ValueError('REQUIRED_SOURCE_MEMBER_MISSING:'+member)
        return f.read_bytes()
    with zipfile.ZipFile(p) as z:
        try: return z.read(member)
        except KeyError as exc: raise ValueError('REQUIRED_SOURCE_MEMBER_MISSING:'+member) from exc


def parse_csv(data):
    s=data.decode('utf-8-sig')
    return list(csv.DictReader(io.StringIO(s)))


def as_bool(v):
    s=str(v).strip().lower()
    if s in BOOL_TRUE:return True
    if s in BOOL_FALSE:return False
    raise ValueError('INVALID_BOOLEAN:'+str(v))


def exact_code(v):
    s=str(v).strip().upper()
    if not re.fullmatch(r'[A-Z0-9]{6}',s): raise ValueError('LOSSY_OR_INVALID_CODE:'+repr(v))
    return s


def grade(s):
    if str(s).startswith('🟢'):return 'GREEN'
    if str(s).startswith('🟡'):return 'YELLOW'
    if str(s).startswith('🔴'):return 'RED'
    raise ValueError('UNKNOWN_OR_DRIFTED_GRADE:'+repr(s))


def group(comp,shallow):
    return ('COMPRESSION_AND_SHALLOW' if comp and shallow else 'COMPRESSION_ONLY' if comp else 'SHALLOW_ONLY' if shallow else 'NEITHER')


def validate_provenance(v,p):
    assert v['workflow_path']==p['source_workflow_path'], 'NOT_ORIGINAL_SCANNER_WORKFLOW'
    assert v['conclusion']=='success', 'SOURCE_RUN_NOT_SUCCESS'
    assert int(v['run_id'])>0 and int(v['artifact_id'])>0, 'INVALID_SOURCE_IDS'
    assert re.fullmatch('[a-f0-9]{40}',v['head_sha']), 'INVALID_HEAD_SHA'
    assert re.fullmatch('[a-f0-9]{64}',v['artifact_sha256']), 'INVALID_ARCHIVE_DIGEST'
    assert 'causal-universe-full-denom-pattern-oos-exit-reports' in v['artifact_name'], 'INVALID_SCANNER_ARTIFACT'
    return v


def frozen_overlay(source,asof,p):
    meta=json.loads(source_bytes(source,SOURCE_META))
    guard=json.loads(source_bytes(source,POST_GUARD))
    ovmeta=json.loads(source_bytes(source,OVERLAY_META))
    rows=parse_csv(source_bytes(source,MAIN_OVERLAY))
    assert dt.date.fromisoformat(asof) >= dt.date.fromisoformat(p['effective_signal_date_min']), 'TRAIN_DATE_NOT_PROSPECTIVE'
    assert dt.date.fromisoformat(asof) > dt.date.fromisoformat(p['training_cutoff']), 'TRAIN_DATE_IN_TEST'
    assert meta.get('signal_date')==asof and guard.get('signal_date')==asof, 'SOURCE_DATE_MISMATCH'
    assert meta.get('capture_status')==p['required_capture_status'] and meta.get('source_quality')==p['required_source_quality'], 'SOURCE_NOT_READY'
    assert str(meta.get('trust_eligible'))=='1' and str(meta.get('postrun_guard_pass'))=='1','SOURCE_NOT_TRUST_ELIGIBLE'
    assert str(guard.get('pass'))=='1' and str(guard.get('trust_eligible'))=='1','POSTRUN_GUARD_FAILED'
    assert ovmeta.get('research_id')==p['expected_research_overlay_id'], 'WRONG_OVERLAY'
    assert ovmeta.get('research_only') is True and not any(ovmeta.get(k) for k in ('production_search_changed','production_score_changed','production_rank_changed','production_order_changed','same_sample_retuning')), 'PRODUCTION_LOGIC_DRIFT'
    assert str(ovmeta.get('rows'))==str(len(rows)) and len(rows)>0, 'INVALID_OVERLAY_ROWS'
    tm=dt.datetime.fromisoformat(meta['captured_at_kst']).astimezone(ZoneInfo('Asia/Seoul'))
    assert tm.date().isoformat()==asof, 'CAPTURE_CLOCK_DATE_MISMATCH'
    assert (tm.hour,tm.minute)>=(15,30), 'CAPTURE_PRE_MARKET_CLOSE'
    seen=set(); out=[]
    for r in rows:
        code=exact_code(r['code']);key=(asof,code)
        assert key not in seen, 'DUPLICATE_SIGNAL_EVENT';seen.add(key)
        assert as_bool(r['research_only']) and not as_bool(r['production_changed']), 'LIVE_LOGIC_CHANGED'
        fs={f:as_bool(r[f]) for f in REQUIRED_FEATURES}
        g=group(fs['ma_compression'],fs['shallow_pullback'])
        maphase=r.get('ma224_phase','')
        if maphase not in ('BELOW','RECLAIM_20','ABOVE_STABLE','UNKNOWN'):raise ValueError('BAD_MA224_PHASE')
        row={
            'signal_date':asof,'code':code,'name':r['name'], 'source_rank':int(r['rank']),
            'original_grade':grade(r['research_level']),'frozen_group':g,
            'primary_combo':int(g=='COMPRESSION_AND_SHALLOW'),
            'ma_compression':int(fs['ma_compression']),'shallow_pullback':int(fs['shallow_pullback']),
            'gradual_accumulation':int(fs['gradual_accumulation']),
            'liquidity_retained':int(fs['liquidity_retained']), 'strong_wave1':int(fs['strong_wave1']),
            'ma224_phase':maphase,'ma224_below':int(maphase=='BELOW'),
            'source_proxy_amount_only':1,'research_only':1,
            'outcome_status':'PENDING_UNOBSERVED','oos_performance_claim':0
        }
        out.append(row)
    ranks=[r['source_rank'] for r in out]
    assert len(ranks)==len(set(ranks)), 'DUPLICATE_SOURCE_RANK'
    assert all(1<=k<=50 for k in ranks),'BAD_SOURCE_RANK'
    return out,meta,ovmeta


def write_csv(path,rows):
    p=Path(path);p.parent.mkdir(parents=True,exist_ok=True)
    if not rows: raise ValueError('EMPTY_OUTPUT_ROWS')
    with p.open('w',encoding='utf-8',newline='') as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0]),lineterminator='\n');w.writeheader();w.writerows(rows)


def baseline(source,protocol,out):
    p=load_protocol(protocol)
    data=parse_csv(source_bytes(source,BASE_GRADES))
    outcome=parse_csv(source_bytes(source,BASE_OUTCOMES))
    manifest=json.loads(source_bytes(source,BASE_MANIFEST))
    assert len(data)==p['baseline_pin']['expected_historic_rows']==180, 'BASELINE_ROWS_DRIFT'
    assert len(outcome)==p['baseline_pin']['expected_horizon_rows']==345, 'BASELINE_OUTCOMES_DRIFT'
    assert manifest['historical_graded_signal_rows']==180 and manifest['matured_outcome_horizon_rows']==345, 'BASELINE_MANIFEST_DRIFT'
    assert not manifest.get('orders_placed') and manifest['production_unchanged'], 'BASELINE_LIVE_DRIFT'
    assert all(r['signal_date']<=p['training_cutoff'] for r in data), 'FUTURE_IN_BASELINE'
    keys=set()
    for r in data:
        key=(r['signal_date'],exact_code(r['code']))
        assert key not in keys,'DUPLICATE_BASELINE_EVENT';keys.add(key)
    assert len(set(x['signal_date'] for x in data))==12,'BASELINE_DATE_COUNT_DRIFT'
    summary=[]
    # Only prior exploration statistics. No future observations enter these stats.
    for h in (1,3,5,10):
        matched=[r for r in outcome if str(r.get('horizon_td','')).rstrip('0').rstrip('.') == str(h)]
        for label in ('COMPRESSION_AND_SHALLOW','COMPRESSION_ONLY','SHALLOW_ONLY','NEITHER'):
            group_rows=[r for r in matched if group(as_bool(r['ma_compression']),as_bool(r['shallow_pullback']))==label]
            vals=[float(r['close_ret_pct']) for r in group_rows if r.get('close_ret_pct','') not in ('','nan','NaN') and math.isfinite(float(r['close_ret_pct']))]
            vals.sort(); n=len(vals)
            wins=sum(v>0 for v in vals)
            med=(vals[(n-1)//2]+vals[n//2])/2 if n else None
            summary.append({'horizon':h,'historical_group':label,'n':n,'wins':wins,'win_rate_pct':round(100*wins/n,4) if n else '', 'median_pct':round(med,6) if med is not None else '', 'distinct_dates':len({r['signal_date'] for r in group_rows}), 'not_oos':1})
    out=Path(out);out.mkdir(parents=True,exist_ok=True)
    write_csv(out/'R1D18_PREDECLARED_HISTORIC_BASELINE_NOT_OOS.csv',summary)
    report={'research_id':PROTOCOL_ID,'mode':'PREDECLARE','baseline_rows':len(data),'baseline_outcome_rows':len(outcome),'past_signal_dates':12,'future_evaluations':0,'oos_confirmed':False,'training_cutoff':p['training_cutoff'],'prospective_start':p['effective_signal_date_min'],'new_yaml_files':0,'orders_placed':0,'live_logic_changed':False,'next':'Future captured exact source-run signal_date, then wait until horizon maturity before any win rate.'}
    dump(out/'R1D18_MANIFEST.json',report)
    return report


def capture(source,protocol,asof,provenance,out):
    p=load_protocol(protocol)
    pv=validate_provenance(json.loads(Path(provenance).read_text(encoding='utf-8')),p)
    rows,meta,om=frozen_overlay(source,asof,p)
    out=Path(out);out.mkdir(parents=True,exist_ok=True)
    for r in rows:
        r['source_run_id']=pv['run_id'];r['source_head_sha']=pv['head_sha'];r['source_artifact_id']=pv['artifact_id'];r['source_artifact_sha256']=pv['artifact_sha256']
    write_csv(out/'R1D18_NEW_SIGNAL_SHADOW_CAPTURE.csv',rows)
    groups={g:sum(r['frozen_group']==g for r in rows) for g in p['four_groups']}
    rep={'research_id':PROTOCOL_ID,'mode':'NEW_SIGNAL_CAPTURE','signal_date':asof,'signals':len(rows),'distinct_codes':len({r['code'] for r in rows}),'groups':groups,'source_run_id':pv['run_id'],'source_artifact_sha256':pv['artifact_sha256'],'future_outcome_rows':0,'success_rate':None,'status':'SHADOW_CAPTURE_ONLY_WAIT_FOR_INDEPENDENT_MATURED_OUTCOMES','authoritative_actual_amount':False,'signal_close_not_an_executable_entry':True,'outcome_mature':False,'oos_performance_confirmed':False,'orders_placed':0,'live_logic_changed':False,'production_score_rank_unchanged':True}
    dump(out/'R1D18_MANIFEST.json',rep);dump(out/'R1D18_SOURCE_PROVENANCE.json',pv)
    return rep


def validate_github_source(repo,token,run_id,protocol,output,output_github=None):
    """Run-time immutable provenance pin from GitHub REST API. No price data fetch."""
    import urllib.request
    p=load_protocol(protocol);rid=int(run_id)
    if rid <= 0 or not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+',repo):raise ValueError('INVALID_REPO_RUN')
    base=f'https://api.github.com/repos/{repo}'
    def get(path):
        req=urllib.request.Request(base+path,headers={'Authorization':'Bearer '+token,'Accept':'application/vnd.github+json','X-GitHub-Api-Version':'2022-11-28'})
        with urllib.request.urlopen(req,timeout=30) as r:return json.load(r)
    run=get('/actions/runs/'+str(rid))
    assert run['id']==rid and run['conclusion']=='success' and run['status']=='completed','SCANNER_RUN_FAILED'
    path=run.get('path','').split('@')[0]
    assert path==p['source_workflow_path'], 'WRONG_SCANNER_WORKFLOW'
    created=dt.datetime.fromisoformat(run['created_at'].replace('Z','+00:00')).astimezone(ZoneInfo('Asia/Seoul'))
    assert created.date().isoformat()>=p['effective_signal_date_min'],'PRE_TRAINING_SOURCE_RUN'
    artifacts=get('/actions/runs/'+str(rid)+'/artifacts?per_page=100')['artifacts']
    valid=[a for a in artifacts if ('causal-universe-full-denom-pattern-oos-exit-reports' in a['name']) and a.get('digest','').startswith('sha256:') and not a.get('expired')]
    assert len(valid)==1,'AMBIGUOUS_OR_MISSING_SCANNER_ARTIFACT'
    a=valid[0]
    rec={'run_id':rid,'head_sha':run['head_sha'],'workflow_path':path,'conclusion':run['conclusion'],'created_at_utc':run['created_at'],'artifact_id':a['id'],'artifact_name':a['name'],'artifact_sha256':a['digest'][7:]}
    validate_provenance(rec,p);dump(output,rec)
    if output_github:
        with Path(output_github).open('a',encoding='utf-8') as f:f.write('artifact_name='+a['name']+'\n')
    return rec


def main():
    pa=argparse.ArgumentParser();pa.add_argument('--mode',required=True,choices=['predeclare','capture','source-pin']);pa.add_argument('--protocol',required=True);pa.add_argument('--out',required=True);pa.add_argument('--source');pa.add_argument('--asof');pa.add_argument('--provenance');pa.add_argument('--repo');pa.add_argument('--token');pa.add_argument('--run-id');pa.add_argument('--github-output')
    a=pa.parse_args();load_protocol(a.protocol)
    if a.mode=='predeclare': result=baseline(a.source,a.protocol,a.out)
    elif a.mode=='capture':result=capture(a.source,a.protocol,a.asof,a.provenance,a.out)
    else:result=validate_github_source(a.repo,a.token,a.run_id,a.protocol,a.out,a.github_output)
    print('R1D18_RESEARCH_ONLY_SUCCESS '+json.dumps(result,ensure_ascii=False,sort_keys=True))

if __name__=='__main__':main()
