#!/usr/bin/env python3
"""Run R1-D65 strictly on existing, explicitly selected R1D27R 300 pilot.
NOT a full-market strategy backtest or R1-D56 2581 event reclassification.
"""
import argparse,csv,gzip,collections,hashlib,json,pathlib,subprocess,sys,statistics
from dataclasses import asdict
from r1d65_strict_triangle import geometry,RULE,positive_bars

SOURCE='research_inputs/R1D27R_PILOT_DMINUS60_DPLUS40_PRICE_CONTEXT.csv.gz'
FROZEN='research_inputs/R1D27R_PILOT_FLOW_BLIND_FROZEN_300.csv'
# Git blob identity confirmed with GitHub repository metadata on 2026-10-30.
EXPECTED_BLOB={SOURCE:'ba5ece4feda6fb0191c0da17da0fd38c905a83f4',FROZEN:'77d5446c44ef7fa5ae5db98b06cb9dd88ada649c'}

def blob_sha(path):
    b=path.read_bytes();return hashlib.sha1(b'blob '+str(len(b)).encode()+b'\0'+b).hexdigest()

def run(root,out):
    root=pathlib.Path(root);out=pathlib.Path(out);out.mkdir(parents=True,exist_ok=True)
    for rel,expected in EXPECTED_BLOB.items():
        p=root/rel
        if not p.exists():raise FileNotFoundError('MISSING_FROZEN_SOURCE:'+str(p))
        actual=blob_sha(p)
        if actual!=expected:raise ValueError('GIT_BLOB_SHA_MISMATCH:'+rel+':'+actual)
    with (root/FROZEN).open(encoding='utf-8',newline='') as fp:
        freeze=list(csv.DictReader(fp))
    frozen={(str(r['date']),str(r['code']).zfill(6)) for r in freeze}
    if len(freeze)!=300 or len(frozen)!=300:raise ValueError('FROZEN_300_IDENTITY_INVALID')
    contexts=collections.defaultdict(dict)
    with gzip.open(root/SOURCE,'rt',encoding='utf-8',newline='') as fp:
        for row in csv.DictReader(fp):
            key=(str(row['date']),str(row['code']).zfill(6));d=int(row['D'])
            if key not in frozen or d in contexts[key]:raise ValueError('CONTEXT_FOREIGN_OR_DUPLICATE')
            contexts[key][d]=row
    if len(contexts)!=300 or any(set(v)!=set(range(-60,41)) for v in contexts.values()):
        raise ValueError('CONTEXT_300_BY_101_MISSING')
    detail=[];tally=collections.Counter();parity_checked=0;parity_failed=0
    # D0 frozen event only. Other future days are NOT used to optimize / redefine
    # the D0 signal. D+1..+40 prices exist but are NEVER read here.
    for key in sorted(frozen):
        rows=[contexts[key][d] for d in range(-60,1)]
        v={c:[r[c] for r in rows] for c in ('open','high','low','close','amount','valid_price')}
        # A well-formed MA224 field was calculated by original source at signal D0.
        # Provenance is described as unverified PIT / corporate-action adjustment.
        ma224=rows[-1].get('MA224','')
        try:ma224=float(ma224)
        except (ValueError,TypeError):ma224=None
        # Independent AS-OF parity against precomputed MA20/MA60 in source.
        # Detect raw/adjusted price basis disagreements before accepting geometry.
        parity='NOT_EVALUABLE'
        if positive_bars(v,1,60):
            try:
                for period in (20,60):
                    expected=float(rows[-1][f'MA{period}'])
                    computed=sum(float(x) for x in v['close'][61-period:61])/period
                    if expected<=0 or abs(computed/expected-1)>0.0005:
                        raise ValueError('MA_PARITY')
                parity='PASS';parity_checked+=1
            except (ValueError,TypeError,KeyError,ZeroDivisionError):
                parity='FAIL';parity_failed+=1
        if parity=='FAIL':
            res={'status':'INVALID_SOURCE_PARITY','reason':'PRICE_BASIS_MA20_MA60_MISMATCH',
                'geometry_valid':False,'breakout_geometry':False,
                'qualified_below224':False,'qualified_with_amount':False}
        else:
            res=geometry(v,60,ma224=ma224)
        res['price_ma20_ma60_parity']=parity
        tally[res['reason']]+=1
        detail.append({'date':key[0],'code':key[1],**res})
    cols=list(dict.fromkeys(k for row in detail for k in row))
    with (out/'R1D65_PILOT300_GEOMETRY_REASONS.csv').open('w',newline='',encoding='utf-8-sig') as fp:
        w=csv.DictWriter(fp,fieldnames=cols,extrasaction='ignore');w.writeheader();w.writerows(detail)
    passed=[r for r in detail if r['status']=='PASS']
    audit={'study':'R1D65_PILOT300_GEOMETRY_ASOF_ONLY',
        'scope':'300 preselected R1D27R events; not all KRX dates, NOT R1D56 2581 geometry',
        'source_git_blob_shas':EXPECTED_BLOB, 'input_frozen_n':len(frozen),
        'n_geometry_valid':sum(bool(r['geometry_valid']) for r in detail),
        'n_fresh_breakout':sum(bool(r['breakout_geometry']) for r in detail),
        'n_below224':sum(bool(r['qualified_below224']) for r in detail),
        'n_qualified':len(passed),'fail_reasons':dict(tally),
        'n_price_ma20_ma60_parity_pass':parity_checked,
        'n_price_ma20_ma60_parity_fail':parity_failed,
        'research_only':True,'live_changes':False,'future_d1_d40_never_read_for_signal':True,
        'corporate_actions_PIT_prices_not_certified':True,
        'performance_evaluated':False,'prospective_OOS':False,
        'pu_reun_20210125_validated_here':False,'rules':asdict(RULE)}
    (out/'R1D65_PILOT300_AUDIT.json').write_text(json.dumps(audit,ensure_ascii=False,indent=2),encoding='utf8')
    print(json.dumps(audit,ensure_ascii=False,indent=2))
    return audit

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',default='.');p.add_argument('--out',default='reports/r1d65');a=p.parse_args();run(a.root,a.out)
