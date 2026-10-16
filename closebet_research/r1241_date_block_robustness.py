#!/usr/bin/env python3
"""R1.24.1: pre-existing R1.24 gate robustness audit on pinned R1.23/R1.24 outputs.

This does NOT train, discover, select, or tune any trading condition. ALL output is
retrospective, source-limited research; a 20-day backtest is not a filled trade.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import numpy as np
import pandas as pd

REV = 'R1241_FROZEN_R124_GATE_DATE_BLOCK_DIAGNOSTIC_NOT_OOS_V1'
R123_SHA = '7d3ddfd6d5d4e285002ca8a96dd3ddb87a0c0e9d6a26f54d7cffbe5f1839b95d'
R124_SHA = 'ab0c57fc01bd341b5d3766e8c4ee1afde772e3bd7245d9df4f90aebe951d5e02'
R123_RUN = 38898887953
R124_RUN = 38902124049
R123_ARTIFACT = 'closebet-r123-success-failure-path-forensics-38898887953'
R124_ARTIFACT = 'closebet-r124-frozen-d0-gates-historical-replay-38902124049'
K = ['scope','code','signal_date']
GATES = ('BASE_ALL','H1_PREV_LOW_HELD','H2_HELD_MA20_UP_ABOVE_20_60','H3_HELD_ACCUM20_DRY5','NEG_OVERHEAD_HEAVY')
OUTCOMES = ('TP_FIRST','STOP_THEN_LATER_TP','STOP_NO_TP','NO_TOUCH','AMBIGUOUS')
EXPECTED_SCOPE = {'BROAD_NOT_RAW':1432,'OTHER_COVERED':744,'V4976_RAW_NOT_TOP5':693,'V4976_SELECTED':208}

def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def require(ok,why):
    if not bool(ok):
        raise ValueError(why)

def source_verify(root,which):
    root=Path(root)
    fn='r123_integrity_manifest.json' if which=='R123' else 'r124_integrity_manifest.json'
    expected=R123_SHA if which=='R123' else R124_SHA
    mp=root/fn
    require(mp.is_file() and sha(mp)==expected,which+'_PINNED_MANIFEST_CHANGED')
    man=json.loads(mp.read_text(encoding='utf-8'))
    members=man['sha256'] if which=='R123' else man['file_sha256']
    for name,digest in members.items():
        p=root/name
        require(p.is_file() and sha(p)==digest,which+'_FILE_SHA_BAD:'+name)
    if which=='R123':
        require(man['status']=='PASS_SOURCE_LIMITED_D20_PATH_FORENSIC_DIAGNOSTIC_ONLY','R123_STATUS_DRIFT')
        require(man['krx_day_sha_verified']==1907 and man['source_eligible_events']==3077,'R123_COUNT_DRIFT')
        require(not man['trade_authority'] and not man['full_market_PIT_available'],'R123_SCOPE_INVALID')
    else:
        require(man['status']=='PASS_HISTORICAL_REPLAY_FROZEN_RULES_OOS_PENDING_NOT_TRADE_APPROVED','R124_STATUS_DRIFT')
        require(man['asof_decisions']==4453 and man['mature_post_events']==3077,'R124_COUNT_DRIFT')
        require(not man['historical_replay_is_independent_oos'] and not man['prospective_full_market_readiness'],'R124_OOS_INVALID')
    return man

def load_pinned(r123,r124):
    m123=source_verify(r123,'R123');m124=source_verify(r124,'R124')
    q=Path(r124)
    decisions=pd.read_csv(q/'r124_4453_d0_only_frozen_gate_decisions.csv',dtype={'code':'string'},low_memory=False)
    ledger=pd.read_csv(q/'r124_3077_retrospective_labelled_gate_ledger_NOT_OOS.csv',dtype={'code':'string'},low_memory=False)
    orig=pd.read_csv(Path(r123)/'r123_success_failure_path_ledger.csv',dtype={'code':'string'},low_memory=False)
    require(len(decisions)==4453 and len(ledger)==len(orig)==3077,'ROW_COUNT_DRIFT')
    for x in (decisions,ledger,orig):
        require(not x.duplicated(K).any(),'DUPLICATE_EPISODE')
        require(x.code.str.fullmatch(r'\d{6}').all(),'LOST_ZERO_PADDING')
    require(set(ledger.bucket.unique()) <= set(OUTCOMES),'UNEXPECTED_OUTCOME')
    require(ledger.scope.value_counts().to_dict()==EXPECTED_SCOPE,'SCOPE_COUNT_DRIFT')
    require(set(GATES).issubset(decisions.columns) and set(GATES).issubset(ledger.columns),'GATE_MISSING')
    for g in GATES:
        require(decisions[g].notna().all() and ledger[g].notna().all(),'GATE_NA')
        # Fail closed if duplicate gate decision differs between the frozen D0 and post labels.
        x=ledger[K+[g]].merge(decisions[K+[g]],on=K,validate='one_to_one',how='left',indicator=True,suffixes=('_label','_d0'))
        require(x._merge.eq('both').all() and x[g+'_label'].eq(x[g+'_d0']).all(),'FROZEN_GATE_CHANGED:'+g)
    require(ledger.BASE_ALL.all() and decisions.BASE_ALL.all(),'BASELINE_BAD')
    require(orig.path_failure_class.map({'TP5_FIRST':'TP_FIRST','STOP5_FIRST_THEN_LATER_TP5':'STOP_THEN_LATER_TP','STOP5_FIRST_NO_LATER_TP5':'STOP_NO_TP',
        'NO_TOUCH_NEAR_TP5':'NO_TOUCH','NO_TOUCH_WEAK_LIFT':'NO_TOUCH','AMBIGUOUS_TP_STOP_SAME_DAY':'AMBIGUOUS'}).notna().all(),'UNEXPECTED_R123_OUTCOME')
    source_labels=orig[K+['path_failure_class','r122_baseline_budget_net_pct','shakeout_like_fast_reclaim_later_tp5','d0_close_location','d0_upper_wick_fraction','ma20_slope20_pct','close_vs_ma20_pct','overhead_proxy_type']].copy()
    mapped=source_labels.path_failure_class.map({'TP5_FIRST':'TP_FIRST','STOP5_FIRST_THEN_LATER_TP5':'STOP_THEN_LATER_TP','STOP5_FIRST_NO_LATER_TP5':'STOP_NO_TP',
        'NO_TOUCH_NEAR_TP5':'NO_TOUCH','NO_TOUCH_WEAK_LIFT':'NO_TOUCH','AMBIGUOUS_TP_STOP_SAME_DAY':'AMBIGUOUS'})
    source_labels['expected_bucket']=mapped
    joined=ledger.merge(source_labels,on=K,validate='one_to_one',how='inner')
    require(len(joined)==3077 and joined.bucket.eq(joined.expected_bucket).all(),'SOURCE_LABEL_MISMATCH')
    require(joined.shakeout_like_fast_reclaim_later_tp5_x.eq(joined.shakeout_like_fast_reclaim_later_tp5_y).all(),'SHAKEOUT_MISMATCH')
    require((joined.bucket.eq('TP_FIRST').sum(),joined.bucket.eq('AMBIGUOUS').sum())==(1868,30),'LABEL_COUNT_DRIFT')
    # Explicitly avoid treating future features as legitimate search-day features.
    require(not any('tp5' in f.lower() or 'stop5' in f.lower() or 'outcome' in f.lower() for f in decisions.columns),'D0_FUTURE_LEAK')
    return decisions,joined,m123,m124

def date_bootstrap_ci(paired:pd.DataFrame,seed:int=124124,B:int=5000):
    """Resample matching signal dates (not rows). Diagnostic uncertainty, NOT OOS."""
    if len(paired)<5:
        return (float('nan'),float('nan'))
    vals=paired.delta_win_pp.to_numpy(float)
    rng=np.random.default_rng(seed)
    idx=rng.integers(0,len(vals),size=(B,len(vals)))
    boots=vals[idx].mean(axis=1)
    return tuple(float(x) for x in np.quantile(boots,[.025,.975]))

def selectors(gate,x):
    if gate=='H1_PREV_LOW_HELD':
        return x[gate],~x[gate],'NON_H1'
    if gate in ('H2_HELD_MA20_UP_ABOVE_20_60','H3_HELD_ACCUM20_DRY5'):
        # Compare incremental rule against the other H1 records; not against all exclusions.
        return x[gate],x['H1_PREV_LOW_HELD'] & ~x[gate],'H1_WITHOUT_'+gate[:2]
    if gate=='NEG_OVERHEAD_HEAVY':
        return x[gate],~x[gate],'NON_HEAVY_OR_UNKNOWN'
    raise ValueError('INVALID_CONTRAST')

def robust_audit(known):
    rows=[]; composition=[];daily=[]
    for scope,g in known.groupby('scope',sort=True):
        dates=int(g.signal_date.nunique())
        tickers=int(g.code.nunique())
        repeated=int(len(g)-tickers)
        signal_counts=g.signal_date.value_counts()
        for gate in GATES[1:]:
            tr,co,control_name=selectors(gate,g)
            good=g.loc[tr]; bad=g.loc[co]
            pairs=[]
            for day,d in g.groupby('signal_date',sort=True):
                dt,dc,_=selectors(gate,d)
                t=d.loc[dt];c=d.loc[dc]
                if len(t)>0 and len(c)>0:
                    pairs.append({'scope':scope,'gate':gate,'date':day,'n_gate':len(t),'n_control':len(c),
                                  'delta_win_pp':100*(t.bucket.eq('TP_FIRST').mean()-c.bucket.eq('TP_FIRST').mean()),
                                  'delta_net_budget_pp':t.r122_baseline_budget_net_pct.mean()-c.r122_baseline_budget_net_pct.mean()})
            matched=pd.DataFrame(pairs)
            ci=date_bootstrap_ci(matched,seed=124124+list(GATES).index(gate))
            matched_mean=float(matched.delta_win_pp.mean()) if len(matched) else float('nan')
            net_matched=float(matched.delta_net_budget_pp.mean()) if len(matched) else float('nan')
            # No winner claims for filtered / missing / unobserved D0 records.
            rows.append({'scope':scope,'gate':gate,'control':control_name,'n_all_mature':len(g),
              'n_gate':len(good),'n_control':len(bad),'n_neither':len(g)-len(good)-len(bad),
              'n_signal_dates':dates,'n_unique_tickers':tickers,'repeated_ticker_events':repeated,
              'top_one_date_share_pct':100*signal_counts.iloc[0]/len(g),
              'top_two_date_share_pct':100*signal_counts.head(2).sum()/len(g),
              'gate_tp_first_n':int(good.bucket.eq('TP_FIRST').sum()),
              'gate_tp_first_pct':100*good.bucket.eq('TP_FIRST').mean() if len(good) else float('nan'),
              'control_tp_first_pct':100*bad.bucket.eq('TP_FIRST').mean() if len(bad) else float('nan'),
              'raw_win_delta_pp':100*(good.bucket.eq('TP_FIRST').mean()-bad.bucket.eq('TP_FIRST').mean()) if len(good) and len(bad) else float('nan'),
              'gate_stop_no_tp_n':int(good.bucket.eq('STOP_NO_TP').sum()),
              'gate_stop_after_then_target_n':int(good.bucket.eq('STOP_THEN_LATER_TP').sum()),
              'gate_ambiguous_n':int(good.bucket.eq('AMBIGUOUS').sum()),
              'gate_net_budget_mean_pct':float(good.r122_baseline_budget_net_pct.mean()) if len(good) else float('nan'),
              'control_net_budget_mean_pct':float(bad.r122_baseline_budget_net_pct.mean()) if len(bad) else float('nan'),
              'matched_dates':len(matched),'matched_date_win_delta_mean_pp':matched_mean,
              'matched_date_win_delta_bootstrap_low_pp':ci[0],
              'matched_date_win_delta_bootstrap_high_pp':ci[1],
              'matched_date_net_delta_mean_pp':net_matched,
              'scope_is_historical_oos':False,'real_fill_verified':False})
            daily.extend(pairs)
            for outcome in OUTCOMES:
                composition.append({'scope':scope,'gate':gate,'outcome':outcome,
                  'kept':int(good.bucket.eq(outcome).sum()),'comparison':int(bad.bucket.eq(outcome).sum()),
                  'neither':int(g.loc[~tr & ~co].bucket.eq(outcome).sum())})
    return pd.DataFrame(rows),pd.DataFrame(daily),pd.DataFrame(composition)

def emit(r123,r124,out):
    d0,known,original,r124m=load_pinned(r123,r124)
    contrasts,paired,composition=robust_audit(known)
    p=Path(out);p.mkdir(parents=True,exist_ok=True)
    tables={'r1241_frozen_gate_robustness_by_scope_NOT_OOS.csv':contrasts,
            'r1241_within_date_pair_deltas_NOT_OOS.csv':paired,
            'r1241_outcome_composition_NO_FILTER_TUNING.csv':composition}
    for name,data in tables.items():data.to_csv(p/name,index=False,encoding='utf-8-sig')
    top=contrasts[(contrasts.scope=='V4976_SELECTED') & (contrasts.gate=='H1_PREV_LOW_HELD')].iloc[0]
    all_base=known.bucket.value_counts()
    note=f'''# Stock-Hunter R1.24.1: Frozen gate robustness diagnostic\n\n'''+f'''- Authority: **RESEARCH_ONLY; retrospective, NOT OOS, NOT trade-approved**.\n- Original GitHub R1.23 run `{R123_RUN}`, R1.24 run `{R124_RUN}`, both member-level SHA256 verified.\n- Saved universe: hindsight 907-code source; not complete market point-in-time reconstruction.\n- D0 records: **{len(d0)}**, fully labeled D+20: **{len(known)}**; without D+20 label **{len(d0)-len(known)}**.\n- Fully labelled signal dates: **{known.signal_date.nunique()}**, not 1,907 independent evaluation days.\n- All mature outcomes: TP before stop={all_base.get('TP_FIRST',0)}, stop then later TP={all_base.get('STOP_THEN_LATER_TP',0)}, stop without later TP={all_base.get('STOP_NO_TP',0)}, no touch={all_base.get('NO_TOUCH',0)}, ambiguous={all_base.get('AMBIGUOUS',0)}.\n- TOP5: n={int(top.n_all_mature)}, H1 retained={int(top.n_gate)}, H1 TP-first={int(top.gate_tp_first_n)}, H1 stop-no-TP={int(top.gate_stop_no_tp_n)}, excluded control n={int(top.n_control)}.\n- Critically H2 and H3 are **compared incrementally to other H1-qualified** records. Original R1.24 compared against all non-gate records; this changes the comparator, not the frozen gate.\n- Date-block bootstrap CI resamples dates, not ticker rows, and is not a predictive performance guarantee.\n- Same-day TP/stop remains ambiguous, +5% after a -5% stop is not a realized trade, nominal 0.50% fee estimate is not a verified fill.\n- The 43 TOP5 post-stop rises vs 51 post-stop failures are ex-post labels; no agent intention can be inferred.\n- No scoring, selection, or threshold tuning, and no modifications to LIVE, Telegram, shadow or orders.\n- **Next blocker**: verified prospective day-by-day all-market PIT capture and full D+20 outcome collection.\n'''
    (p/'r1241_korean_evidence_summary.md').write_text(note,encoding='utf-8')
    selected=contrasts[contrasts.scope.eq('V4976_SELECTED')].set_index('gate')
    h1=selected.loc['H1_PREV_LOW_HELD']
    h2=selected.loc['H2_HELD_MA20_UP_ABOVE_20_60']
    h3=selected.loc['H3_HELD_ACCUM20_DRY5']
    held=known[(known.scope=='V4976_SELECTED')&known.H1_PREV_LOW_HELD]
    excluded=known[(known.scope=='V4976_SELECTED')&~known.H1_PREV_LOW_HELD]
    top_all=known[known.scope=='V4976_SELECTED']
    brief=f"""# Stock-Hunter 종가매매 R1.24.1 결과 보고서 — 사후 진단 전용

## 1. GitHub 원본 확인

- 실제 GitHub Actions R1.23 실행: `{R123_RUN}` 성공, R1.24 실행: `{R124_RUN}` 성공.
- R1.23 및 R1.24 결과물의 기준 manifest SHA256과 각 구성 파일 SHA256을 모두 검증했다.
- R1.24.1은 원본 R1.24 매매 가설을 수정하지 않고 날짜 편중과 대조군 구성만 재점검했다.
- 이번 추가 분석은 로컬 실행 결과이며 **R1.24.1 GitHub Actions 실행 성공을 주장하지 않는다.**

## 2. 데이터 범위 및 중요한 한계

- 사후 선정된 약 907종목 자료가 출처다. 당시의 전체 KRX 종목을 복원한 PIT 전체 시장 자료가 아니다.
- D0 확인 가능 사건 {len(d0):,}건 중 D+20 경로 평가 가능 사건은 {len(known):,}건이고, 나머지 {len(d0)-len(known):,}건에는 미래 라벨을 확정할 수 없다.
- 결과 평가에 사용한 검색 기준일은 **{known.signal_date.nunique()}개**뿐이다. 과거 KRX 데이터 {original['krx_day_sha_verified']:,}일치의 SHA256 검증이 {original['krx_day_sha_verified']:,}개의 독립 실험 날짜를 의미하지 않는다.
- D+1 공식 시가 진입 가정, +5% vs -5% 최초 터치 D+20 비교다. 장중 동일 봉의 목표·손절 경로가 모호한 30건은 승리로 처리하지 않았다.
- 참고 순손익은 기존 R1.22의 **명목 왕복 수수료·비용 0.50% 가정** 결과이며, 실제 체결·슬리피지·호가 잔량을 검증한 값은 아니다.

## 3. 기존 TOP5 208건: 지지선 유지 H1이 무엇을 남기고 제외하는가

| 구분 | 검색 사건 | +5% 목표 먼저 | 손절 후 +5% | 손절 후 목표 미도달 | 기존 모형 평균 예산대비 순손익 |
|---|---:|---:|---:|---:|---:|
| 전체 TOP5 | {len(top_all)} | {int(top_all.bucket.eq('TP_FIRST').sum())} | {int(top_all.bucket.eq('STOP_THEN_LATER_TP').sum())} | {int(top_all.bucket.eq('STOP_NO_TP').sum())} | {top_all.r122_baseline_budget_net_pct.mean():+.2f}% |
| H1 전일 저가 유지 | {len(held)} | {int(held.bucket.eq('TP_FIRST').sum())} | {int(held.bucket.eq('STOP_THEN_LATER_TP').sum())} | {int(held.bucket.eq('STOP_NO_TP').sum())} | {held.r122_baseline_budget_net_pct.mean():+.2f}% |
| H1 미충족 | {len(excluded)} | {int(excluded.bucket.eq('TP_FIRST').sum())} | {int(excluded.bucket.eq('STOP_THEN_LATER_TP').sum())} | {int(excluded.bucket.eq('STOP_NO_TP').sum())} | {excluded.r122_baseline_budget_net_pct.mean():+.2f}% |

H1을 적용하면 **122건(58.7%)**을 남기지만, 승리 110건 중 38건도 함께 제외한다. 지속적인 하락이 의심되는 종목을 줄이는 면과 상승 기회를 놓치는 면을 동시에 살펴야 한다. H1의 TP 먼저 터치 비율은 {h1.gate_tp_first_pct:.1f}%, 미충족 대조군은 {h1.control_tp_first_pct:.1f}%로 사후 차이는 {h1.raw_win_delta_pp:+.1f}%p이다.

날짜별로 H1/비H1을 모두 포함한 **{int(h1.matched_dates)}거래일**만 동일 날짜 비교하면 평균 차이 {h1.matched_date_win_delta_mean_pp:+.1f}%p, 날짜 블록 부트스트랩 95% 기술적 구간 [{h1.matched_date_win_delta_bootstrap_low_pp:+.1f}, {h1.matched_date_win_delta_bootstrap_high_pp:+.1f}]%p였다. 이 구간은 동일 자료를 사용한 **사후 확인**이며 신뢰할 수 있는 미래 성공률 구간이 아니다.

## 4. 이평선 및 거래량 조건을 더 엄격하게 만들면 좋은가?

H2(전일 저가 유지+20일선 상승+20·60일선 위)는 TOP5 **{int(h2.n_gate)}건**으로 줄인다. H2 내부 TP 우선은 {h2.gate_tp_first_pct:.1f}%이며 H1이지만 H2 미충족인 {int(h2.n_control)}건은 {h2.control_tp_first_pct:.1f}%다. 따라서 H2 조건을 추가하는 것만으로 개선됐다고 보기 어렵다. 날짜 매칭 평균은 {h2.matched_date_win_delta_mean_pp:+.1f}%p이고 부트스트랩 구간은 [{h2.matched_date_win_delta_bootstrap_low_pp:+.1f}, {h2.matched_date_win_delta_bootstrap_high_pp:+.1f}]%p다.

H3(전일 저가 유지+20일간 거래량 유입+최근 5일 감소)는 TOP5에 **{int(h3.n_gate)}건**뿐이어서 정량 평가에 부적합하다. R1.24의 가설이 맞다는 증거도, 틀렸다는 결론도 내릴 수 없다. 새 임계치를 계속 탐색해 과거 종목에 맞춰서는 안 된다.

## 5. 날짜·종목 의존성과 매매상 의미

D+20 라벨은 26개 검색일에서 왔고, 시장 전체가 강했던 특정 날짜에 과도하게 집중될 수 있다. 일부 범위에서 단순 사건 비율의 효과와 동일 날짜 비교 효과의 방향까지 달라진다. 같은 종목의 다른 날짜 관찰도 완전한 독립 표본이 아니다. 따라서 사건 단위 승률이나 95% 구간만으로 실전 진입 조건을 승인하지 않는다.

D0 전일 저가 보존은 **관찰 후보 가설**로 유지하되, 첫 상승파동·224일선 아래 매집·양봉 몸통 지지·상단 매물대·구조적 손절·분할매수까지 통합한 규칙이 검증됐다고 주장하지 않는다. 또 손절 후 가격이 회복한 사건은 기존 고정 손절 규칙의 실제 수익이 아니다.

## 6. 다음 연구의 선결 조건

동결된 H1/H2/H3를 변경하지 않은 상태에서 전체 시장의 매일 기준일별 저장 스냅샷·시각·종목 자격을 남기고, 공식 KRX D+1~D+20 데이터를 완성한 뒤 날짜 블록·종목군·시장 국면을 분리한 미래 OOS를 해야 한다. 그다음에야 진입, 구조적 손절, 제한적인 분할매수의 실제 효과를 별도 실험으로 검증한다.

**판정: R1.24.1 날짜 편중 재검사 완료(로컬), 독립 미래 OOS 미완료, LIVE 적용 불가.**
"""
    (p/'r1241_한국어_연구결과.md').write_text(brief,encoding='utf-8')
    files={f.name:sha(f) for f in sorted(p.iterdir()) if f.is_file()}
    manifest={'revision':REV,'status':'PASS_RETROSPECTIVE_DATE_BLOCK_DIAGNOSTIC_NOT_OOS_NOT_TRADE_APPROVED',
       'r123_run':R123_RUN,'r124_run':R124_RUN,'source_manifests_sha256':{'r123':R123_SHA,'r124':R124_SHA},
       'source_r124_policy_sha256':r124m['policy_sha256'],'d0_count':len(d0),'d20_count':len(known),
       'd20_signal_dates':int(known.signal_date.nunique()),'full_market_pit':False,'oos':False,
       'live_unchanged':True,'shadow_unchanged':True,'telegram_unchanged':True,'orders_unchanged':True,
       'output_sha256':files}
    (p/'r1241_integrity_manifest.json').write_text(json.dumps(manifest,indent=2,ensure_ascii=False)+'\n',encoding='utf-8')
    print('R1241_LOCAL_RESEARCH_ONLY_PASS '+json.dumps({'dates':manifest['d20_signal_dates'],'mature':len(known),'top5_h1_kept':int(top.n_gate)},ensure_ascii=False))
    return manifest

def main():
    a=argparse.ArgumentParser(description=__doc__)
    a.add_argument('--r123-root',required=True)
    a.add_argument('--r124-root',required=True)
    a.add_argument('--output-dir',required=True)
    p=a.parse_args();emit(p.r123_root,p.r124_root,p.output_dir)

if __name__=='__main__':main()
