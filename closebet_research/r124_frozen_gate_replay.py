#!/usr/bin/env python3
"""R1.24 research: freeze a few D0-only support/volume/MA gates for future
prospective OOS. Replay old R1.23 for *exploratory/diagnostic* comparison only.
Never infer motive (shakeout) or real order-book supply from EOD bars.
NO LIVE search/rank/orders/telegram writes.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import numpy as np
import pandas as pd

REV = 'R124_FROZEN_D0_PATTERNS_RETROSPECTIVE_CHECK_NOT_OOS_V1'
SOURCE_R123_RUN = 38898887953
SOURCE_R123_ARTIFACT = f'closebet-r123-success-failure-path-forensics-{SOURCE_R123_RUN}'
PINNED_MANIFEST_SHA = '7d3ddfd6d5d4e285002ca8a96dd3ddb87a0c0e9d6a26f54d7cffbe5f1839b95d'
GATES = ('BASE_ALL','H1_PREV_LOW_HELD','H2_HELD_MA20_UP_ABOVE_20_60',
         'H3_HELD_ACCUM20_DRY5','NEG_OVERHEAD_HEAVY')
KEY = ['scope','code','signal_date']
SUCCESS = 'TP5_FIRST'
LATER = 'STOP5_FIRST_THEN_LATER_TP5'
FAIL = 'STOP5_FIRST_NO_LATER_TP5'
UNKNOWN = 'AMBIGUOUS_TP_STOP_SAME_DAY'
OUTCOMES = (SUCCESS, LATER, FAIL, 'NO_TOUCH_NEAR_TP5','NO_TOUCH_WEAK_LIFT', UNKNOWN)
SRC_PRE = 'r123_d0_asof_overhead_proxy_NO_FUTURE_COLUMNS.csv'
SRC_POST = 'r123_success_failure_path_ledger.csv'


def sha(data):
    return hashlib.sha256(Path(data).read_bytes()).hexdigest()


def require(cond,reason):
    if not bool(cond): raise ValueError(reason)


def validate_policy_file(path):
    p=json.loads(Path(path).read_text(encoding='utf-8'))
    require(p['research_revision']=='R124_FROZEN_D0_GATE_V1','POLICY_REVISION_INVALID')
    require(p['candidate_gates']==list(GATES[1:4]),'GATE_SIGNATURE_DRIFT')
    require(p['controls']==['BASE_ALL','NEG_OVERHEAD_HEAVY'],'CONTROL_SIGNATURE_DRIFT')
    require(p['future_oos_complete'] is False and p['research_only'] is True,'POLICY_AUTHORITY_BAD')
    require(p['never_changes_live_or_shadow_or_orders'] is True,'LIVE_AUTHORITY_BAD')
    require(p['no_tuning_after_seeing_future_labels'] is True,'TUNING_FORBIDDEN')
    return p


def load_pinned_r123(root):
    root=Path(root)
    manifest=root/'r123_integrity_manifest.json'
    require(manifest.is_file(), 'R123_MANIFEST_MISSING')
    require(sha(manifest)==PINNED_MANIFEST_SHA,'R123_MANIFEST_SHA_CHANGED')
    meta=json.loads(manifest.read_text(encoding='utf-8'))
    require(meta['revision']=='R123_SHAKEOUT_LIKE_RECOVERY_VS_OVERHEAD_SUPPLY_PROXY_DIAGNOSTICS_V1','R123_REVISION_BAD')
    require(meta['status']=='PASS_SOURCE_LIMITED_D20_PATH_FORENSIC_DIAGNOSTIC_ONLY','R123_STATUS_INVALID')
    require(meta['source_eligible_events']==3077 and meta['d0_feature_valid_events']==4453,'R123_COUNTS_CHANGED')
    require(meta['krx_day_sha_verified']==1907,'R123_KRX_DAYS_BAD')
    require(meta['future_outcomes_not_used_for_D0_features'] is True and meta['full_market_PIT_available'] is False,'SCOPE_BAD')
    require(meta['trade_authority'] is False and meta['live_changed'] is False,'LIVE_AUTHORITY_BAD')
    for fn,hash_ in meta['sha256'].items():
        require((root/fn).is_file() and sha(root/fn)==hash_,'R123_MEMBER_CHANGED:'+fn)
    pre=pd.read_csv(root/SRC_PRE,dtype={'code':'string'},low_memory=False)
    post=pd.read_csv(root/SRC_POST,dtype={'code':'string'},low_memory=False)
    require(len(pre)==4453 and len(post)==3077,'SOURCE_LENGTH_DRIFT')
    require(not pre.duplicated(KEY).any() and not post.duplicated(KEY).any(),'SOURCE_DUPLICATE')
    require(pre.code.str.fullmatch(r'\d{6}').all() and post.code.str.fullmatch(r'\d{6}').all(),'CODE_ZEROS_LOST')
    require('path_failure_class' not in pre.columns and 'first_tp5_day' not in pre.columns and 'd1_open' not in pre.columns,'FUTURE_LEAK_IN_D0')
    require(set(post.path_failure_class.unique()).issubset(set(OUTCOMES)),'OUTCOME_UNKNOWN')
    require(not post[KEY].merge(pre[KEY],on=KEY,how='left',indicator=True)._merge.ne('both').any(),'OUTCOME_WITHOUT_D0')
    return pre,post,meta


def gate_matrix(pre):
    """Only D0 known features, never read post outcomes. Fixed gates, no score tuning."""
    needed=['prev_low_relation','volume_context','ma_band','ma20_slope20_pct','overhead_proxy_type']
    require(set(needed).issubset(pre.columns),'MISSING_D0_FIELDS')
    p=pre.copy()
    p['BASE_ALL']=True
    p['H1_PREV_LOW_HELD']=p.prev_low_relation.eq('HOLD_PREV_LOW')
    p['H2_HELD_MA20_UP_ABOVE_20_60']=(p.H1_PREV_LOW_HELD &
        (pd.to_numeric(p.ma20_slope20_pct,errors='coerce')>0) & p.ma_band.eq('ABOVE_20_60'))
    p['H3_HELD_ACCUM20_DRY5']=p.H1_PREV_LOW_HELD & p.volume_context.eq('ACCUM20_RECENT_DRY5')
    p['NEG_OVERHEAD_HEAVY']=p.overhead_proxy_type.eq('ABOVE_HEAVY_PROXY')
    require(p[list(GATES)].notna().all().all(),'GATE_NA')
    return p


def failure_bucket(post):
    bucket=np.select([post.path_failure_class.eq(SUCCESS),post.path_failure_class.eq(LATER),
                      post.path_failure_class.eq(FAIL),post.path_failure_class.eq(UNKNOWN)],
                     ['TP_FIRST','STOP_THEN_LATER_TP','STOP_NO_TP','AMBIGUOUS'],default='NO_TOUCH')
    return bucket


def summaries(d0,joined):
    rows=[]; daily=[]; diagnostic=[]
    d0_counts={g:int(d0[g].sum()) for g in GATES}
    for scope, grp in joined.groupby('scope',sort=True):
        for gate in GATES:
            x=grp[grp[gate]].copy()
            n=len(x)
            if not n: rows.append(dict(scope=scope,gate=gate,n=0,d0_gate_count=int(d0[d0.scope==scope][gate].sum()),status='NO_MATCH'));continue
            cats=x['bucket'].value_counts()
            hit=int(cats.get('TP_FIRST',0));amb=int(cats.get('AMBIGUOUS',0))
            stop_later=int(cats.get('STOP_THEN_LATER_TP',0));stop_fail=int(cats.get('STOP_NO_TP',0));no_touch=int(cats.get('NO_TOUCH',0))
            # Report both + possible optimistic bound if SAME_DAY order was TP-first.
            rows.append(dict(scope=scope,gate=gate,n=n,d0_gate_count=int(d0[d0.scope==scope][gate].sum()),
                tp5_first_count=hit,stop_then_later_tp_count=stop_later,stop_no_tp_count=stop_fail,
                no_touch_count=no_touch,same_day_unknown_count=amb,
                tp5_first_conservative_pct=100*hit/n,tp5_first_optimistic_bound_pct=100*(hit+amb)/n,
                stop_first_count=stop_later+stop_fail,
                stop_then_later_tp_rate_pct=100*stop_later/n,
                stop_no_tp_rate_pct=100*stop_fail/n,
                status='HISTORICAL_EXPLORATORY_ONLY'))
            for date,y in x.groupby('signal_date'):
                daily.append(dict(scope=scope,gate=gate,signal_date=date,n=len(y),
                                  tp5_first=int(y.bucket.eq('TP_FIRST').sum()),
                                  stop_then_later_tp=int(y.bucket.eq('STOP_THEN_LATER_TP').sum()),
                                  stop_no_tp=int(y.bucket.eq('STOP_NO_TP').sum()),
                                  ambiguous=int(y.bucket.eq('AMBIGUOUS').sum())))
            for typ,z in x.groupby('prev_low_relation',dropna=False):
                diagnostic.append(dict(scope=scope,gate=gate,prev_low_relation=str(typ),n=len(z),
                                      tp5_first=int(z.bucket.eq('TP_FIRST').sum()),
                                      stop_then_later_tp=int(z.bucket.eq('STOP_THEN_LATER_TP').sum()),
                                      stop_no_tp=int(z.bucket.eq('STOP_NO_TP').sum())))
    require(d0_counts['BASE_ALL']==4453,'SOURCE_D0_LOST')
    return pd.DataFrame(rows),pd.DataFrame(daily),pd.DataFrame(diagnostic)


def paired_delta(d0,joined):
    """Selection-vs-its-excluded-control per *same date and same scope*.
    Not randomized, not causal; return small-sample shares and date-matched delta.
    """
    out=[]
    for scope,g in joined.groupby('scope',sort=True):
        for gate in GATES[1:]:
            good=g[g[gate]]
            bad=g[~g[gate]]
            by=[]
            for date,x in g.groupby('signal_date'):
                t=x[x[gate]];c=x[~x[gate]]
                if len(t) and len(c):
                    by.append(dict(date=date,n_gate=len(t),n_control=len(c),
                        win_gate=float(t.bucket.eq('TP_FIRST').mean()),
                        win_control=float(c.bucket.eq('TP_FIRST').mean()),
                        delta_pp=100*(t.bucket.eq('TP_FIRST').mean()-c.bucket.eq('TP_FIRST').mean())))
            q=pd.DataFrame(by)
            out.append(dict(scope=scope,gate=gate,gate_n=len(good),control_n=len(bad),
                matched_days=len(q),mean_date_matched_delta_pp=float(q.delta_pp.mean()) if len(q) else np.nan,
                median_date_matched_delta_pp=float(q.delta_pp.median()) if len(q) else np.nan,
                gate_hit_pct=100*good.bucket.eq('TP_FIRST').mean() if len(good) else np.nan,
                control_hit_pct=100*bad.bucket.eq('TP_FIRST').mean() if len(bad) else np.nan,
                no_matching_day=len(q)==0,not_out_of_sample=True))
    return pd.DataFrame(out)


def forecast_contract():
    return {
      'mode':'PROSPECTIVE_EVIDENCE_INTAKE_SPECIFICATION_NOT_ENABLED',
      'asof_signal_min_exclusive':'2026-10-16',
      'source_requirement':'VERIFIED_FULL_KRX_SNAPSHOT_SAVED_ON_EACH_SIGNAL_DATE_WITH_TIMESTAMP',
      'required_source_fields':['snapshot_captured_utc','signal_date','code','open','high','low','close','volume','amount','universe_membership_asof'],
      'future_outcome_requirements':'D+1..D+20 OFFICIAL KRX BARS, post-date frozen evidence, unknown when incomplete',
      'forward_available_at_generation':False,
      'applies_to_trade':False,
      'history_only_reference':'R123_RUN_38898887953',
      'hypotheses':list(GATES),
      'reasons_blocked':['historical 907 saved codes not true full PIT universe',
          'as-of full KRX day snapshots and forward labels not yet collected and independently time stamped',
          'actual closing-auction execution cannot be derived from OHLCV'],
      'unfrozen_decisions_forbidden':True
    }


def run(source,policy_file,out):
    validate_policy_file(policy_file)
    src_pre,src_post,source_manifest=load_pinned_r123(source)
    d0=gate_matrix(src_pre)
    merged=d0.merge(src_post[KEY+['path_failure_class','shakeout_like_fast_reclaim_later_tp5',
                                  'first_tp5_day','first_stop5_day']],on=KEY,validate='one_to_one',how='left',indicator=True)
    require((merged._merge.value_counts().get('both',0)==3077),'OUTCOME_JOIN_DRIFT')
    known=merged[merged._merge.eq('both')].copy()
    known['bucket']=failure_bucket(known)
    sums,daily,diagnostics=summaries(d0,known)
    paired=paired_delta(d0,known)
    require(len(known)==3077 and len(d0)==4453,'COHORT_MISMATCH')
    require(known.bucket.value_counts().to_dict()=={'TP_FIRST':1868,'STOP_NO_TP':552,'STOP_THEN_LATER_TP':526,'NO_TOUCH':101,'AMBIGUOUS':30},'BASELINE_OUTCOME_DRIFT')
    # No future labels in D0-only export (even d20_complete, signal path or maturity).
    d0out=d0[KEY+['type','prev_low_relation','volume_context','ma_band','ma20_slope20_pct','overhead_proxy_type']+list(GATES)].copy()
    require(not any('future' in v.lower() or 'outcome' in v.lower() or 'tp5' in v.lower() or 'stop5' in v.lower() for v in d0out.columns),'ASOF_OUTPUT_FUTURE_LEAK')
    future=forecast_contract()
    out=Path(out);out.mkdir(parents=True,exist_ok=True)
    frames={'r124_4453_d0_only_frozen_gate_decisions.csv':d0out,
            'r124_3077_retrospective_labelled_gate_ledger_NOT_OOS.csv':known[KEY+list(GATES)+['prev_low_relation','volume_context','ma_band','bucket','first_tp5_day','first_stop5_day','shakeout_like_fast_reclaim_later_tp5']],
            'r124_gate_x_scope_failure_path_summary.csv':sums,
            'r124_gate_x_scope_daily_counts.csv':daily,
            'r124_gate_x_scope_date_matched_delta.csv':paired,
            'r124_failure_composition_by_preknown_gate.csv':diagnostics}
    for name,frame in frames.items():frame.to_csv(out/name,index=False,encoding='utf-8-sig')
    (out/'r124_FUTURE_PROSPECTIVE_READINESS_BLOCKERS.json').write_text(json.dumps(future,indent=2,ensure_ascii=False)+'\n',encoding='utf-8')
    (out/'r124_frozen_hypotheses_COPIED.json').write_bytes(Path(policy_file).read_bytes())
    files={f.name:sha(f) for f in sorted(out.iterdir()) if f.is_file()}
    meta={'revision':REV,'status':'PASS_HISTORICAL_REPLAY_FROZEN_RULES_OOS_PENDING_NOT_TRADE_APPROVED',
          'r123_run':SOURCE_R123_RUN,'r123_artifact':SOURCE_R123_ARTIFACT,
          'r123_manifest_sha256':PINNED_MANIFEST_SHA,'source_verified_krx_days':1907,
          'asof_decisions':len(d0),'mature_post_events':len(known),'not_labelled_due_to_maturity_or_quality':len(d0)-len(known),
          'source_scope':'907_POSTHOC_CODE_ARCHIVE_ONLY_NOT_FULL_MARKET_PIT',
          'historical_replay_is_independent_oos':False,'prospective_full_market_readiness':False,
          'policy_sha256':sha(policy_file),'no_live_changes':True,'no_order_changes':True,
          'no_shadow_changes':True,'no_telegram_changes':True,
          'no_proven_shakeout_or_real_orderbook_supply':True,'file_sha256':files}
    (out/'r124_integrity_manifest.json').write_text(json.dumps(meta,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print('R124_RESEARCH_PASS '+json.dumps({k:meta[k] for k in ('status','asof_decisions','mature_post_events','not_labelled_due_to_maturity_or_quality','historical_replay_is_independent_oos')},ensure_ascii=False))
    return meta


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--r123-root',required=True);p.add_argument('--rules',required=True);p.add_argument('--output-dir',required=True)
    a=p.parse_args();run(a.r123_root,a.rules,a.output_dir)

if __name__=='__main__':main()
