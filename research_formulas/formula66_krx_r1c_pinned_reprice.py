#!/usr/bin/env python3
"""Read-only stage R1-C: original KRX OHLCV/amount reprice of 66 *legacy* PRE signal hits.

The KRX source is the SHA-pinned, whole-market, eight-shard 2019-2026 historical
collection. Historical signal membership is STILL from the old Direct Replay and
was not recalculated from native KRX. This code does not execute orders, produce
LIVE candidates, or mark any strategy ready.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
import pandas as pd

CAPTURE_RUN=39738384453
CAPTURE_ARTIFACT_ID=12408056768
CAPTURE_ARTIFACT_DIGEST='sha256:04a6dad8581e595bbf33442e095f163abae7e35d88d8078721dd68866767ee6d'
HISTORIC_CAPTURE_RAW_SHA='da46adb7994e94d9d2a12aaaeeaf9b25eb16a4d7cee91eb77108ce834dd31ddb'
KRX_RUN=39295295197
KRX_EXPECTED_RANGE=('2019-01-02','2026-10-12')
KRX_EXPECTED_SHARDS=8
KRX_SHARD_DIGESTS={
0:(12208596109,'sha256:84dd5aa62f0c9538cd9c2d0ed0540c8f74063b21be49d9ecfc81e4b1583368b5'),
1:(12207608558,'sha256:3d372eebf915c870badf103356c3700d5fa914a32d79ce8057ef1f4c4f89295b'),
2:(12209930511,'sha256:5c49df64ad10bafbc09dd7ea78b9377804735fda6ad861018750b396ab2a27cb'),
3:(12209696204,'sha256:7a359f5959e2374622c40d068920adf9d695142b4a61b540d0752a8cb9bb4dd8'),
4:(12208882046,'sha256:88b35ea4c9a8a6a0ed4b61250a8018d49ee7ba4489c7345f04012915bb60f997'),
5:(12208443687,'sha256:eb503f7cabcc40991037092c646ea4f56885b3814'),
6:(12211336402,'sha256:b3cc055d919c7ab92d7f27b1b3ce6d3e56df787b107af961eaa43dedb885d6cd'),
7:(12212241289,'sha256:9563747eb8b27928ba0a0d2b7d3cf02e787acd2e9e4a656d410c286139321478'),
}
PIN_KRX_ADAPTER_BLOB='e0fd01bcb6eb365bb55922a632a4eef18c5fe57c'
PIN_KRX_SOURCE_BLOB='03ea1bee1ddc9ededb246c3c076908f58c343128'
PIN_FORMULA_R1B_BLOB='1ea6803912d5a0decc1cc3518b84ddd9d39d4002'
PIN_KRX_REFERENCE_SHA256='e733c3bd67a4cf06e93954888ef1ea539ec2e04d193f9d8a2406ef55fc248af1'
PIN_FORMULA_REGISTRY_BLOB='b477e069c310efed4011fe0a7f2ca394b35cdb72'
PIN_MAIN7_BLOB='8d210abdb3b01180fb0b5c108e36159f8deec527'


def fail(s):
    raise RuntimeError('FORMULA66_R1C_FAIL_CLOSED_'+str(s))


def sha_file(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def blob_sha(path):
    raw=Path(path).read_bytes()
    return hashlib.sha1(b'blob '+str(len(raw)).encode()+b'\x00'+raw).hexdigest()


def check_source_pins(root):
    root=Path(root)
    files={
        'main7_bugfix_2.py':PIN_MAIN7_BLOB,
        'search_formula_contract_registry.json':PIN_FORMULA_REGISTRY_BLOB,
        'research_formulas/formula66_truth_krx_r1.py':PIN_FORMULA_R1B_BLOB,
        'research_core224/krx_truth_r1.py':PIN_KRX_ADAPTER_BLOB,
        'research_pit/krx_full_market_pit.py':PIN_KRX_SOURCE_BLOB,
    }
    for rel,pin in files.items():
        if not (root/rel).is_file() or blob_sha(root/rel)!=pin:
            fail('SOURCE_BLOB_MISSING_OR_DRIFT:'+rel)
    ref=root/'research_pit/historical_market_counts_20190102_20261012.csv'
    if not ref.is_file() or sha_file(ref)!=PIN_KRX_REFERENCE_SHA256:
        fail('KRX_REFERENCE_SHA_MISMATCH')
    return files


def validate_remote_source_metadata(meta):
    """Call with JSON returned from gh api (never trust a user-typed artifact name)."""
    cr,ka=meta.get('capture_run',{}),meta.get('krx_run',{})
    if (cr.get('id')!=CAPTURE_RUN or cr.get('conclusion')!='success' or
        ka.get('id')!=KRX_RUN or ka.get('conclusion')!='success'):
        fail('UPSTREAM_RUN_STATUS_OR_ID_INVALID')
    if cr.get('head_sha')!='b03252627d1dde889415492fd66093839a504bdd':
        fail('UPSTREAM_CAPTURE_HEAD_SHA_DRIFT')
    if ka.get('head_sha')!='08e328c101f0774a2236c0a44cc1fdbbaa59e96e':
        fail('UPSTREAM_KRX_HEAD_SHA_DRIFT')
    def check_item(items,id_,digest,name):
        found=[x for x in items if x.get('id')==id_ and x.get('name')==name]
        if len(found)!=1 or found[0].get('digest')!=digest or found[0].get('expired',False):
            fail('ARTIFACT_ID_NAME_DIGEST_MISMATCH:'+name)
    check_item(meta.get('capture_artifacts',[]),CAPTURE_ARTIFACT_ID,CAPTURE_ARTIFACT_DIGEST,
               'stockhunter-main7-formula66-r1-capture-capture_truth')
    for shard,(artifact_id,digest) in KRX_SHARD_DIGESTS.items():
        check_item(meta.get('krx_artifacts',[]),artifact_id,digest,f'stockhunter-allkrx-price-shard-{shard}')
    return True


def read_pinned_capture(capture):
    """Restore only PRE-true rows; never promote 710 POST-only observations."""
    folder=Path(capture)
    mf=folder/'formula66_capture_manifest.json'
    ingest=folder/'formula66_capture_ingest.json'
    raw=folder/'formula66_capture_truth_per_event_formula.csv.gz'
    if not all(p.is_file() for p in (mf,ingest,raw)):
        fail('REQUIRED_CAPTURE_INPUT_MISSING')
    m=json.loads(mf.read_text('utf-8'));i=json.loads(ingest.read_text('utf-8'))
    if m.get('status')!='PASS_CAPTURE_PRE_RECALC_NOT_PRICE_VERIFIED' or m.get('strategy_ready') is not False or m.get('krx_price_confirmed') is not False:
        fail('CAPTURE_ORIGINAL_TRUTH_MANIFEST_INVALID')
    if i.get('source_sha256')!=HISTORIC_CAPTURE_RAW_SHA or i.get('deduped_rows')!=1979:
        fail('HISTORIC_SOURCE_CAPTURE_PIN_DRIFT')
    if (m.get('captured_rows')!=1979 or m.get('formula_evaluations')!=130614 or
        m.get('captured_pre_true_total')!=3884 or m.get('post_only_true_total')!=710 or
        m.get('pre_mismatch')!=0 or m.get('condition_exceptions_logged')!=0):
        fail('R1B_CAPTURE_COUNTS_DRIFT')
    q=pd.read_csv(raw,dtype={'code':str,'signal_date':str,'pre_captured':str,'recomputed_pre':str,
                             'post_captured':str,'recomputed_post':str,'formula':str},low_memory=False)
    required={'code','signal_date','formula_index','formula','pre_captured','post_captured',
              'recomputed_pre','recomputed_post','pre_match','post_only_true'}
    if len(q)!=1979*66 or not required.issubset(q.columns):
        fail('CAPTURE_MATRIX_SHAPE_SCHEMA_DRIFT')
    if (q['formula_index'].isna().any() or
        set(q['formula_index'].unique())!=set(range(66)) or
        q.groupby('formula_index').size().ne(1979).any()):
        fail('CAPTURE_MATRIX_FORMULA_COVERAGE_DRIFT')
    if q.duplicated(['code','signal_date','formula_index']).any():
        fail('CAPTURE_DUPLICATE_EVENT_FORMULA')
    if (not q['code'].str.fullmatch(r'[0-9][0-9A-Z]{5}').all() or
        not q['signal_date'].str.fullmatch(r'\d{4}-\d{2}-\d{2}').all()):
        fail('BAD_CAPTURE_EVENT_ID')
    if (not set(q['pre_captured']).issubset({'T','F'}) or
        not set(q['recomputed_pre']).issubset({'T','F'}) or
        not set(q['post_captured']).issubset({'T','F'}) or
        not set(q['recomputed_post']).issubset({'T','F'})):
        fail('INVALID_CAPTURE_TRUTH_ENUM')
    if (not q['pre_captured'].eq(q['recomputed_pre']).all() or
        not q['post_captured'].eq(q['recomputed_post']).all() or
        not q['pre_match'].eq(1).all()):
        fail('CAPTURE_PRE_OR_POST_RECALC_CONFLICT')
    if int(q['pre_captured'].eq('T').sum())!=3884 or int(q['post_only_true'].sum())!=710:
        fail('CAPTURE_PRE_OR_POST_TOTAL_DRIFT')
    events=q[q['pre_captured']=='T'].copy()
    if events[['code','signal_date']].drop_duplicates().shape[0]!=1492:
        fail('CAPTURE_EVENT_KEYS_DRIFT')
    names=q[['formula_index','formula']].drop_duplicates()
    if len(names)!=66: fail('CAPTURE_FORMULA_ID_NAME_DRIFT')
    return events.to_dict('records'),names.sort_values('formula_index').to_dict('records'),m


def check_krx_source(root,reference):
    """Check 8 original manifests before passing to the pinned full 1907-day validator."""
    root=Path(root)
    if not root.is_dir():fail('KRX_RAW_DIRECTORY_MISSING')
    manifests=[]
    for idx in range(8):
        file=root/f'shard_{idx:02d}'/f'manifest_shard_{idx:02d}.json'
        if not file.is_file():fail('KRX_SHARD_MANIFEST_MISSING:'+str(idx))
        m=json.loads(file.read_text(encoding='utf-8'))
        if (m.get('shard')!=idx or m.get('shards')!=8 or m.get('status')!='COMPLETE_PRICE_COLLECTION_ONLY'
            or m.get('format')!='STOCKHUNTER_KRX_ALL_SESSION_V1' or
            (m.get('start'),m.get('end'))!=KRX_EXPECTED_RANGE or
            m.get('official_days')!=1907 or m.get('universe_filter')!='NONE'):
            fail('KRX_SHARD_SCOPE_OR_STATUS_DRIFT:'+str(idx))
        manifests.append(m)
    if len({m.get('calendar_sha256') for m in manifests})!=1 or len({m.get('index_sha256') for m in manifests})!=1:
        fail('KRX_SHARD_COMMON_SOURCE_SHA_MISMATCH')
    if sha_file(reference)!=PIN_KRX_REFERENCE_SHA256:
        fail('KRX_REFERENCE_SHA_MISMATCH')
    return manifests


def export_all_66(original_summary,per_formula_regime,output):
    """Include 23 zero-hit formulas; no winning strategy is selected from 13 training dates."""
    pdf=pd.read_csv(per_formula_regime,dtype={'formula_index':int},low_memory=False)
    rows=[]
    for item in original_summary:
        i=int(item['formula_index']); f=item['formula']
        for horizon in (5,10,15):
            a=pdf[(pdf['formula_index']==i)&(pdf['horizon']==horizon)&(pdf['market_regime_hypothesis']=='ALL')]
            if len(a)>1:fail('REPRICE_REPORT_FORMULA_HORIZON_DUPLICATE')
            values=a.iloc[0].to_dict() if len(a)==1 else {}
            rows.append({'formula_index':i,'formula':f,'horizon':horizon,
                         'legacy_PRE_events':int(values.get('captured_true_rows') or 0),
                         'KRX_raw_price_complete':int(values.get('krx_price_path_completed') or 0),
                         'blocked_paths':int(values.get('blocked_missing_or_corp_action') or 0),
                         'KRX_descriptive_20bps_mean':values.get('mean_net20bps_pct','') if values else '',
                         'KRX_descriptive_20bps_median':values.get('median_net20bps_pct','') if values else '',
                         'KRX_descriptive_20bps_win_rate':values.get('win_net20bps_rate_pct','') if values else '',
                         'source':'LEGACY_NON_KRX_PRE_SIGNALS__OFFICIAL_KRX_RAW_PRICE_ONLY',
                         'status':('ZERO_PRE_HITS_FROM_13_LEGACY_DATES' if not values else
                                   'EXPLORATORY_NON_EXECUTABLE_NEEDS_KRX_NATIVE_RESCREEN_AND_OOS')})
    if len(rows)!=198:fail('NOT_66_X_3_REPORT')
    out=Path(output);out.parent.mkdir(parents=True,exist_ok=True)
    pd.DataFrame(rows).to_csv(out,index=False,encoding='utf-8-sig')
    return rows


def run_reprice(capture,krx_root,reference,out_dir):
    import research_formulas.formula66_truth_krx_r1 as old
    folder=Path(out_dir);folder.mkdir(parents=True,exist_ok=True)
    events,names,base=read_pinned_capture(capture)
    manifests=check_krx_source(krx_root,reference)
    reg=old.get_registry('search_formula_contract_registry.json','main7_bugfix_2.py',verify_pins=True)
    # R1-B's original range is 2024-01-02..2026-09-30. This task uses the already
    # collected VERIFIED full 2019-01-02..2026-10-12 eight-shard KRX source instead.
    # Only the research-only interval constant is bound to its original source manifest;
    # no original code, registry, workflow or LIVE logic is edited.
    old.KRX_FULL_START,old.KRX_FULL_END=KRX_EXPECTED_RANGE
    outcome=old.krx_reprice(events,reg,krx_root,reference,folder)
    if (outcome.get('events_with_pre_TRUE_formula')!=3884 or
        outcome.get('unique_event_code_dates')!=1492 or
        outcome.get('historical_krx_sessions')!=1907 or
        outcome.get('historical_krx_source_sha256_by_session')!=1907 or
        outcome.get('strategy_ready') is not False):
        fail('KRX_REPRICE_RESULT_INTEGRITY_ERROR')
    rows=export_all_66(names,folder/'formula66_krx_repricing_by_formula_regime.csv',
                       folder/'formula66_krx_all66_3horizon_scorecard.csv')
    summary={
       'phase':'R1C_KRX_PRICE_ONLY_OF_PINNED_LEGACY_PRE_CAPTURE',
       'status':'SUCCESS_EXPLORATORY_KRX_PRICE_REPRICE_NOT_NATIVE_SIGNAL_RESCREEN',
       'source_r1b_run':CAPTURE_RUN,'source_r1b_artifact_id':CAPTURE_ARTIFACT_ID,
       'source_original_KRX_run':KRX_RUN,
       'source_original_KRX_shard_artifact_ids':[KRX_SHARD_DIGESTS[i][0] for i in range(8)],
       'source_original_KRX_period':list(KRX_EXPECTED_RANGE),
       'source_original_KRX_shard_calendar_sha256':manifests[0]['calendar_sha256'],
       'source_original_KRX_shard_index_sha256':manifests[0]['index_sha256'],
       'KRX_sessions_verified':outcome['historical_krx_sessions'],
       'KRX_actual_amount_shared_original_source':True,
       'PRICE_DATA_5M_ROWS_SOURCE_VERIFIED':True,
       'PRE_true_formula_events':3884,
       'unique_legacy_PRE_events':1492,
       'formula_rows_in_output':len(rows),
       'formulas_with_ZERO_original_PRE_signals':sum(not any(r['legacy_PRE_events'] for r in rows if r['formula_index']==i) for i in range(66)),
       'raw_KRX_formula_event_paths_complete':outcome['complete_formula_event_price_paths'],
       'raw_KRX_formula_event_paths_blocked':outcome['unresolved_paths'],
       'NO_KRX_NATIVE_FORMULA_RESCREEN':True,
       'NO_ORIGINAL_TRADEABILITY_PIT_CERTIFICATION':True,
       'NO_CORPORATE_ACTION_FULL_VERIFICATION':True,
       'NO_INDEPENDENT_OOS_PROFITABILITY':True,
       'INDEX_REGIMES':'KOSPI_AND_KOSDAQ_INDEX_SERIES_PARALLEL__NO_PER_STOCK_MARKET_MEMBERSHIP_CERTIFICATION',
       'RETURN_DEFINITION':'entry next trading session open; D+5/10/15 close; original unadjusted KRX prices',
       'ROUND_TRIP_COST_BPS_STRESS':[20,50,100],
       'LIVE_CHANGED':False,'ORDERS_PLACED':0,'strategy_ready':False,
       'warning':'Do not rank these outcomes as profitable LIVE strategies: legacy source selection, halted paths, corporate action and independent OOS require verification.'
    }
    (folder/'formula66_r1c_source_and_outcome_manifest.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2,sort_keys=True)+'\n',encoding='utf-8')
    print('FORMULA66_R1C_COMPLETE',json.dumps(summary,ensure_ascii=False,sort_keys=True),flush=True)
    return summary


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--mode',required=True,choices=['source_preflight','reprice'])
    p.add_argument('--repo-root',default='.')
    p.add_argument('--remote-meta',default='')
    p.add_argument('--capture',default='')
    p.add_argument('--krx-root',default='')
    p.add_argument('--reference',default='research_pit/historical_market_counts_20190102_20261012.csv')
    p.add_argument('--out',default='reports/formula66_r1c')
    a=p.parse_args(argv)
    check_source_pins(a.repo_root)
    if not a.remote_meta or not Path(a.remote_meta).is_file():fail('REMOTE_SOURCE_METADATA_MISSING')
    validate_remote_source_metadata(json.loads(Path(a.remote_meta).read_text('utf-8')))
    if a.mode=='source_preflight':
        print('FORMULA66_R1C_SOURCE_PINS_PASS',CAPTURE_RUN,KRX_RUN,flush=True)
        return 0
    if not a.capture or not a.krx_root:fail('CAPTURE_OR_KRX_ROOT_MISSING')
    run_reprice(a.capture,a.krx_root,a.reference,a.out)
    return 0


if __name__=='__main__':
    raise SystemExit(main())
