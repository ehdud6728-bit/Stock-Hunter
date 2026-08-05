from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys

import pandas as pd
import Closing_bet_scanner_v2 as s


def file_sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def f(v):
    try:
        if pd.isna(v):
            return '평가없음'
        return f'{float(v):+.2f}%'
    except Exception:
        return '평가없음'


def main() -> int:
    ap = argparse.ArgumentParser(description='v49.61 global shard merge')
    ap.add_argument('--input-root', default='v49_61_downloads')
    ap.add_argument('--prepare-dir', default='v49_61_prepare_output')
    ap.add_argument('--output-dir', default='reports')
    ap.add_argument('--start-date', default='')
    ap.add_argument('--end-date', default='')
    ap.add_argument('--top-per-strategy', type=int, default=5)
    ap.add_argument('--shard-count', type=int, default=8)
    ap.add_argument('--send-telegram', action='store_true')
    args = ap.parse_args()

    root = Path(args.input_root)
    prep_root = Path(args.prepare_dir)
    if not prep_root.exists():
        # actions/download-artifact with merge-multiple may place files under input root.
        prep_root = root
    preflight_path = next(iter(prep_root.rglob('v49_61_preflight.json')), None)
    universe_path = next(iter(prep_root.rglob('v49_61_universe.json')), None)
    if preflight_path is None or universe_path is None:
        raise RuntimeError('prepare artifacts missing')
    preflight = json.loads(preflight_path.read_text(encoding='utf-8'))
    universe = json.loads(universe_path.read_text(encoding='utf-8'))
    if preflight.get('status') != 'VALID':
        raise RuntimeError(f"preflight invalid: {preflight}")

    manifests = sorted(root.rglob('shard_*_manifest.json'))
    raws = sorted(root.rglob('shard_*_raw.csv'))
    if len(manifests) != args.shard_count or len(raws) != args.shard_count:
        raise RuntimeError(f'shard artifacts incomplete: manifests {len(manifests)}/{args.shard_count}, raw {len(raws)}/{args.shard_count}')

    manifest_docs = [json.loads(p.read_text(encoding='utf-8')) for p in manifests]
    start_dates={str(x.get('start_date','')) for x in manifest_docs}; end_dates={str(x.get('end_date','')) for x in manifest_docs}
    if len(start_dates)!=1 or len(end_dates)!=1:
        raise RuntimeError(f'shard date consensus failed: start={start_dates}, end={end_dates}')
    actual_start=next(iter(start_dates)); actual_end=next(iter(end_dates))
    if args.start_date and args.start_date!=actual_start:
        raise RuntimeError(f'input start mismatch {args.start_date}!={actual_start}')
    if args.end_date and args.end_date!=actual_end:
        raise RuntimeError(f'input end mismatch {args.end_date}!={actual_end}')
    args.start_date=actual_start; args.end_date=actual_end
    shard_ids = sorted(int(x.get('shard_index', -1)) for x in manifest_docs)
    expected_ids = list(range(args.shard_count))
    if shard_ids != expected_ids:
        raise RuntimeError(f'shard ids mismatch: {shard_ids} != {expected_ids}')
    global_counts = {int(x.get('global_count', 0) or 0) for x in manifest_docs}
    global_fps = {str(x.get('global_fingerprint', '')) for x in manifest_docs}
    if global_counts != {int(preflight.get('universe_count', 0))} or global_fps != {str(preflight.get('universe_fingerprint', ''))}:
        raise RuntimeError(f'global universe consensus failed: counts={global_counts}, fps={global_fps}')
    for p, m in zip(raws, sorted(manifest_docs, key=lambda x: int(x.get('shard_index', -1)))):
        if file_sha(p) != str(m.get('raw_sha256', '')):
            raise RuntimeError(f'raw sha mismatch: {p}')
        if str(m.get('engine_status', '')).upper() != 'VALID':
            raise RuntimeError(f'shard engine invalid: {m}')

    frames = []
    for p in raws:
        try:
            df = pd.read_csv(p, dtype={'code': str})
        except pd.errors.EmptyDataError:
            df = pd.DataFrame()
        if not df.empty:
            if 'code' in df.columns:
                df['code'] = df['code'].astype(str).str.zfill(6)
            frames.append(df)
    raw_df = pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()
    if raw_df.empty:
        raise RuntimeError('all shard raw signals empty')
    # Defensive schema normalization. Real shard rows include these fields, but merge remains
    # fail-readable when a strategy emits a sparse payload.
    defaults={'grade':'','score':0.0,'amount_b':0.0,'vol_ratio':0.0,'rule35_pnl':float('nan'),'ret_next_close':float('nan'),'hit3_before_stop':0,'band_type':''}
    for col,val in defaults.items():
        if col not in raw_df.columns: raw_df[col]=val
    canonical = s._v4940_canonical_df(raw_df)
    selected = s._select_backtest_top(canonical, top_per_strategy=max(1, args.top_per_strategy), all_candidates=False)
    if selected.empty:
        raise RuntimeError('global selected population empty')

    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    raw_path = out / 'v49_61_global_raw.csv'
    canonical_path = out / 'v49_61_global_canonical.csv'
    selected_path = out / 'v49_61_global_selected_top5.csv'
    raw_df.to_csv(raw_path, index=False, encoding='utf-8-sig')
    canonical.to_csv(canonical_path, index=False, encoding='utf-8-sig')
    selected.to_csv(selected_path, index=False, encoding='utf-8-sig')

    # Restore prepared metadata for M5/strategy reporting.
    s.INDEX_MAP = {str(k).zfill(6): str(v) for k, v in dict(universe.get('index_map') or {}).items()}
    s.MARCAP_MAP = {str(k).zfill(6): float(v or 0) for k, v in dict(universe.get('marcap_map') or {}).items()}
    s._V4959_STRATEGY_ENGINE_AUDIT = {'status': 'VALID', 'detail': {'shard_count': args.shard_count, 'global_fingerprint': preflight.get('universe_fingerprint')}}
    perf = s._v4959_build_common_performance_audit(raw_df, selected, args.start_date, args.end_date, {'source_codes': preflight.get('universe_count', 0)})
    perf_status = str(perf.get('status', 'INVALID'))
    generated_root=Path('reports')
    if out.resolve()!=generated_root.resolve():
        import shutil
        for gp in generated_root.glob('v49_61_common_strategy_*'):
            shutil.copy2(gp,out/gp.name)

    performance_csv = out / 'v49_61_common_strategy_performance.csv'
    portfolio_csv = out / 'v49_61_common_strategy_portfolio.csv'
    ptab = pd.read_csv(performance_csv) if performance_csv.exists() else pd.DataFrame()
    port = pd.read_csv(portfolio_csv) if portfolio_csv.exists() else pd.DataFrame()
    allrow = ptab[ptab['strategy'].eq('ALL')].iloc[0].to_dict() if not ptab.empty and (ptab['strategy'] == 'ALL').any() else {}
    p1_50 = port[(port.strategy == 'ALL') & (port.day_limit == 1) & (port.cost_bps == 50)].iloc[0].to_dict() if not port.empty and len(port[(port.strategy == 'ALL') & (port.day_limit == 1) & (port.cost_bps == 50)]) else {}
    p1_20 = port[(port.strategy == 'ALL') & (port.day_limit == 1) & (port.cost_bps == 20)].iloc[0].to_dict() if not port.empty and len(port[(port.strategy == 'ALL') & (port.day_limit == 1) & (port.cost_bps == 20)]) else {}

    tech_ok = preflight.get('status') == 'VALID' and perf_status == 'VALID'
    lines = [
        '(1/2)',
        f"⚙️ 공통 검색식 성과검증 | v49.61",
        '──────────',
        f"버전: {s.CLOSING_BET_SCANNER_VERSION}",
        f"기간: {args.start_date} ~ {args.end_date} | prepared universe {preflight.get('universe_count')} · shards {args.shard_count}",
        '[기술 검증]',
        f"- PREPARED UNIVERSE: VALID ✅ · fp {preflight.get('universe_fingerprint')}",
        f"- SHARD CONSENSUS: {len(manifests)}/{args.shard_count} VALID ✅ · global merge before TOP selection",
        f"- SEARCH CONTRACT: {'VALID ✅' if preflight.get('contract_valid') else 'INVALID ⛔'} · explicit {preflight.get('explicit_hist_failures')} · boundary {preflight.get('boundary_failures')} · thread {preflight.get('thread_isolation_failures')} · determinism {preflight.get('determinism_failures')}",
        f"- GLOBAL PIPELINE: raw {len(raw_df)} → canonical {len(canonical)} → date×strategy TOP{args.top_per_strategy} {len(selected)}",
        f"- PERFORMANCE ENGINE: {perf_status} {'✅' if perf_status == 'VALID' else '⛔'}",
        '',
        '[전체 성과]',
        f"- 거래평균 gross/net20/net50: {f(allrow.get('gross_mean_pct'))} / {f(allrow.get('net20_mean_pct'))} / {f(allrow.get('net50_mean_pct'))}",
        f"- OOS 평균 net20/net50: {f(allrow.get('oos_net20_mean_pct'))} / {f(allrow.get('oos_net50_mean_pct'))} · OOS n {int(allrow.get('oos_n', 0) or 0)}",
        f"- 하루1종목 20bp: 누적 {f(p1_20.get('total'))} · MDD {f(p1_20.get('mdd'))}",
        f"- 하루1종목 50bp: 누적 {f(p1_50.get('total'))} · MDD {f(p1_50.get('mdd'))} · 양수월 {f(p1_50.get('positive_month'))}",
        '',
        '[운용 잠금]',
        '- PAPER 유지 · 실제주문 0건',
        '- 50bp OOS·하루1종목·MDD·양수월·대박제거를 함께 통과하기 전 LIVE 자동전환 금지',
    ]
    lines2 = ['(2/2)', '📊 전략별 OOS | v49.61', '──────────']
    if not ptab.empty:
        strat = ptab[ptab.strategy.ne('ALL')].sort_values(['oos_net50_mean_pct', 'oos_n'], ascending=[False, False])
        for _, r in strat.iterrows():
            lines2.append(f"- {r['strategy']}: n {int(r['n'])} · OOS {int(r['oos_n'])} · net20/50 {f(r['oos_net20_mean_pct'])}/{f(r['oos_net50_mean_pct'])} · 전체50 {f(r['net50_mean_pct'])}")
    lines2 += ['', '- I/IT proxy 수급 표본은 승격 근거에서 제외', '- Lifecycle/Runner/FAIL/BIG/Cluster는 이번 성과 전용 실행과 분리']
    report = '\n'.join(lines + [''] + lines2)
    (out / 'v49_61_global_summary.txt').write_text(report, encoding='utf-8')
    manifest = {
        'version': s.CLOSING_BET_SCANNER_VERSION,
        'status': 'VALID' if tech_ok else 'INVALID',
        'preflight': preflight,
        'shards': manifest_docs,
        'global_raw_rows': len(raw_df),
        'global_canonical_rows': len(canonical),
        'global_selected_rows': len(selected),
        'performance_status': perf_status,
        'files': {p.name: file_sha(p) for p in [raw_path, canonical_path, selected_path]},
        'paper_only': True,
        'real_orders': 0,
    }
    (out / 'v49_61_global_manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    print(report)
    if args.send_telegram and s._telegram_route_ready():
        for part in (lines, lines2):
            s.send_telegram_photo('\n'.join(part), [])
    return 0 if tech_ok else 3


if __name__ == '__main__':
    sys.exit(main())
