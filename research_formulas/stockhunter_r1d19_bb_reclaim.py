#!/usr/bin/env python3
"""R1-D19 / RESEARCH_ONLY.

BB20/BB30/BB40 close-reclaim ASOF on ORIGINAL R1D6 frozen 5,247 event keys.
ASOF data is written and sha-sealed before D7 retrospective outcomes are read.
This is an exploratory conditional study of the 224-wave PROXY, not full market,
not native 66-formula parity, not OOS, not trade instructions.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import re
from collections import defaultdict, deque
from pathlib import Path

import numpy as np
import pandas as pd

KRX_ORIGINAL_RUN = 39295295197

VERSION = 'STOCKHUNTER_R1D19_BB203040_RECLAIM_HISTORICAL_V1'
HORIZONS = (5, 10, 15, 20)
RESEARCH_ONLY = True
DONE = 'PRICE_PATH_COMPLETE_NOT_EXECUTION_CERTIFIED'
BANDS = (20, 30, 40)
FLAGS = ('bb20_reclaim_today', 'bb30_reclaim_today', 'bb40_reclaim_today',
         'bb20_reclaim_recent5', 'bb30_reclaim_recent5', 'bb40_reclaim_recent5',
         'bb_all3_today', 'bb_all3_recent5', 'bb_any_today')


def stop(reason):
    raise RuntimeError('R1D19_FAIL_CLOSED_' + reason)


def source_protocol(path):
    p = json.loads(Path(path).read_text(encoding='utf-8'))
    assert p['research_id'] == 'STOCKHUNTER_R1D19_BB203040_RECLAIM_ON_FROZEN_R1D6_COHORT'
    assert p['research_only'] and all(p['constraints'].values())
    assert p['historical_population_expected'] == 5247
    assert p['bands'] == [20, 30, 40] and p['rolling_std_ddof'] == 0
    assert p['outcome_horizons'] == list(HORIZONS)
    assert p['historical_signal_end'] < p['training_cutoff'] < p['prospective_start']
    assert p['historical_source']['krx_run'] == KRX_ORIGINAL_RUN
    assert p['historical_source']['krx_daily_days'] == 1907
    assert p['historical_source']['r1d6_asof_csv_sha256'] == '5062180dbb9d1299773d61074c687e8f7bf7f7ec2fe06b8622b9ed9cc4de1d29'
    return p


def sha(data):
    return hashlib.sha256(data).hexdigest()


def read_frozen_asof(root, p):
    f = Path(root) / 'R1D6_ASOF_ONLY_FROZEN_FEATURES_NO_OUTCOME.csv.gz'
    if not f.is_file() or sha(f.read_bytes()) != p['historical_source']['r1d6_asof_csv_sha256']:
        stop('R1D6_FROZEN_ASOF_BYTES_DRIFT')
    x = pd.read_csv(f, compression='gzip', dtype={'date': str, 'code': str}, low_memory=False)
    if len(x) != 5247 or x[['date', 'code']].duplicated().any():
        stop('R1D6_UNCHANGED_5247_KEYS_REQUIRED')
    if not x.date.between(p['historical_signal_start'], p['historical_signal_end']).all():
        stop('R1D6_SIGNAL_DATES_NOT_FROZEN')
    if not x.date.eq(x.reference_data_last_bar_date).all() or not x.status_feature.eq('ASOF_OK').all():
        stop('R1D6_ASOF_SOURCE_TIME_OR_QUALITY')
    if not x.code.str.fullmatch('[0-9A-Z]{6}').all():
        stop('R1D6_EXACT_SECURITY_IDENTITY')
    if any(q in x for q in ('entry_open', 'exit_close', 'net_20bps_pct', 'gross_pct', 'status_out')):
        stop('FUTURE_LEAK_IN_ORIGINAL_ASOF')
    return x


def lower(close, n, ddof=0):
    """Only trailing values including each contemporaneous day; no future bars."""
    x = pd.Series(np.asarray(close, dtype=float))
    avg = x.rolling(n, min_periods=n).mean()
    sd = x.rolling(n, min_periods=n).std(ddof=ddof)
    return (avg - 2.0 * sd).to_numpy(dtype=float)


def compute_flags(bars, current_session, p=None):
    """bars: (global session_index, close), ending at current_session.

    Fail closed on noncontiguous 224 KRX sessions or unadjusted extreme gaps.
    No ffill, no historical ticker aliases, and no outcome price data.
    """
    if len(bars) < 224:
        return {'asof_status': 'INSUFFICIENT_224_HISTORY'}
    tail = list(bars)[-224:]
    if [int(v[0]) for v in tail] != list(range(current_session - 223, current_session + 1)):
        return {'asof_status': 'NONCONTIGUOUS_SESSION_HISTORY'}
    c = np.asarray([x[1] for x in tail], dtype=np.float64)
    if not np.isfinite(c).all() or (c <= 0).any():
        return {'asof_status': 'INVALID_RAW_CLOSE'}
    if (np.abs(c[1:] / c[:-1] - 1) > 0.40).any():
        return {'asof_status': 'UNADJUSTED_EXTREME_GAP_GUARD'}
    out = {'asof_status': 'ASOF_OK', 'signal_close': float(c[-1]),
           'ma224_below': int(c[-1] < float(c.mean()))}
    today = []
    within5 = []
    for n in BANDS:
        lev = lower(c, n, 0)
        below = c < lev
        # Each band uses its OWN contemporaneous previous-session boundary.
        crossed = below[:-1] & (c[1:] >= lev[1:])
        crossed = np.concatenate(([False], crossed))
        out[f'bb{n}_lower_signal'] = float(lev[-1])
        out[f'bb{n}_lower_prev'] = float(lev[-2])
        out[f'bb{n}_below_prev_close'] = int(below[-2])
        out[f'bb{n}_reclaim_today'] = int(crossed[-1])
        out[f'bb{n}_reclaim_recent5'] = int(crossed[-5:].any())
        out[f'bb{n}_below_in_prior20'] = int(below[-21:-1].sum())
        out[f'bb{n}_below_streak_before_signal'] = int(next((j for j in range(0, 21) if j == 20 or not below[-2-j]), 20))
        # Wick touches are a different phenomenon; this module only uses CLOSE.
        today.append(bool(crossed[-1])); within5.append(bool(crossed[-5:].any()))
    out['bb_all3_today'] = int(all(today))
    out['bb_all3_recent5'] = int(all(within5))
    out['bb_any_today'] = int(any(today))
    return out


def write_csv(df, path):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(p, index=False, encoding='utf-8-sig', compression='gzip' if p.suffix == '.gz' else None)


def save_json(obj, path):
    f = Path(path)
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_text(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True, default=str) + '\n', encoding='utf-8')


def seal_features(krx_root, asof, p, out):
    """Full 1907-day SHA verification and exact frozen-event asof extraction.

    Do NOT read R1D7 prices in this phase.
    """
    from research_formulas import formula66_r1d6_oos_holdout as d6
    src = d6.source_manifest(krx_root)
    cal, _ = d6.index_asof(krx_root)
    if set(src) != set(cal) or d6.verify_all_daily_raw_sha(src) != 1907:
        stop('ORIGINAL_KRX_DAILY_SOURCE_NOT_VERIFIED')
    wanted = defaultdict(set)
    for date, code in asof[['date', 'code']].itertuples(index=False, name=None):
        wanted[date].add(code)
    seen = set()
    result = []
    histories = defaultdict(lambda: deque(maxlen=224))
    warm = cal.index('2022-01-03')
    end = cal.index(p['historical_signal_end'])
    for session in range(warm, end + 1):
        date = cal[session]
        df = d6.priceday(src[date][0], date)
        targets = wanted.get(date, set())
        day_seen = set()
        for code, close in zip(df['code'].to_numpy(), df['close'].to_numpy()):
            code = str(code).upper()
            histories[code].append((session, float(close)))
            if code not in targets:
                continue
            key = (date, code)
            if key in seen:
                stop('DUPLICATE_CAPTURE_EVENT')
            seen.add(key)
            day_seen.add(code)
            row = {'date': date, 'code': code}
            row.update(compute_flags(histories[code], session))
            result.append(row)
        if targets - day_seen:
            stop('FROZEN_SIGNAL_TICKER_MISSING_ON_SOURCE_DAY_' + date)
        if session % 90 == 0:
            print('R1D19_FEATURE_CAPTURE', date, 'captured', len(result), flush=True)
    keys = set(zip(asof.date, asof.code))
    if seen != keys or len(result) != 5247:
        stop('MISSING_OR_CHANGED_FROZEN_SIGNAL_POPULATION')
    x = pd.DataFrame(result).sort_values(['date', 'code']).reset_index(drop=True)
    if any(k.startswith(('net_', 'gross_', 'exit_', 'future_', 'fwd_')) for k in x.columns):
        stop('SEALED_FEATURES_CONTAIN_FUTURE')
    path = Path(out) / 'R1D19_ASOF_ONLY_BB_RECLAIM_NO_OUTCOME.csv.gz'
    write_csv(x, path)
    evidence = {'source_days_sha_verified': 1907, 'asof_rows': len(x),
                'asof_ok': int(x.asof_status.eq('ASOF_OK').sum()),
                'blocked_by_quality': x.loc[x.asof_status.ne('ASOF_OK'), 'asof_status'].value_counts().to_dict(),
                'asof_file_sha256': sha(path.read_bytes()),
                'historical_only': True, 'oos_confirmed': False,
                'production_logic_changed': False, 'orders_placed': 0}
    save_json(evidence, Path(out) / 'R1D19_ASOF_MANIFEST.json')
    return x, evidence


def read_r1d7_paths(root, frozen_keys):
    f = Path(root) / 'result/R1D7_D5_D10_D15_D20_FROZEN_FORWARD_PRICE_PATHS.csv.gz'
    if not f.is_file():
        stop('FROZEN_R1D7_OUTCOME_FILE_MISSING')
    x = pd.read_csv(f, compression='gzip', dtype={'date': str, 'code': str}, low_memory=False)
    if len(x) != 5247 * len(HORIZONS) or x[['date', 'code', 'horizon']].duplicated().any():
        stop('FROZEN_R1D7_OUTCOME_KEYS_OR_COUNT_CHANGED')
    if set(x.horizon) != set(HORIZONS) or set(zip(x.date, x.code)) != frozen_keys:
        stop('FROZEN_R1D7_COHORT_OR_HORIZON_CHANGED')
    for col in ('status_out', 'net_20bps_pct', 'mae_intraday_pct', 'mfe_intraday_pct'):
        if col not in x:
            stop('FROZEN_R1D7_OUTCOME_SCHEMA_' + col)
    if int(x.loc[x.horizon.eq(20), 'status_out'].eq(DONE).sum()) != 5214:
        stop('FROZEN_R1D7_D20_COMPLETION_CHANGED')
    return x


def one_score(g, flag, val, segment):
    v = g[g[flag].eq(val)] if flag != '__ALL__' else g
    ok = v[v.status_out.eq(DONE)]
    n = len(ok)
    return {'segment': segment, 'factor': flag, 'value': val,
            'horizon': int(g.horizon.iloc[0]), 'events': len(v), 'complete': n,
            'blocked': len(v)-n, 'codes': ok.code.nunique(), 'dates': ok.date.nunique(),
            'win_net_positive_pct': 100 * ok.net_20bps_pct.gt(0).mean() if n else np.nan,
            'median_net_pct': ok.net_20bps_pct.median() if n else np.nan,
            'mean_net_pct': ok.net_20bps_pct.mean() if n else np.nan,
            'failure_minus5_pct': 100 * ok.net_20bps_pct.le(-5).mean() if n else np.nan,
            'strong_plus15_pct': 100 * ok.net_20bps_pct.ge(15).mean() if n else np.nan,
            'median_mae_pct': ok.mae_intraday_pct.median() if n else np.nan,
            'median_mfe_pct': ok.mfe_intraday_pct.median() if n else np.nan,
            # Entry-basis percentage-points of best observed intraday high minus
            # final D+h close. Not an executable peak exit and not causal order.
            'median_peak_to_exit_giveback_pp': (ok.mfe_intraday_pct - (ok.net_20bps_pct + 0.20)).median() if n else np.nan,
            'mfe15_then_exit_nonpositive_pct': 100 * ((ok.mfe_intraday_pct >= 15) & (ok.net_20bps_pct <= 0)).mean() if n else np.nan,
            'low_n_warning': int(n < 30 or ok.date.nunique() < 8),
            'not_new_oos': 1}


def compare(features, paths, original_asof, out):
    if set(zip(features.date, features.code)) != set(zip(original_asof.date, original_asof.code)):
        stop('ASOF_FEATURES_VS_SOURCE_KEYS_DRIFT')
    # Reject metric contaminated ASOF before joining ANY outcomes.
    if any(q in features for q in ('status_out', 'gross_pct', 'net_20bps_pct', 'exit_close')):
        stop('OUTCOME_IN_FEATURES_BEFORE_JOIN')
    # Retain all 5247 events, including blocked ASOF and censored outcomes.
    src = original_asof[['date', 'code', 'amount3d_above_prior10', 'joint_index_20d_regime']].copy()
    a = features.merge(src, on=['date', 'code'], validate='one_to_one')
    j = paths.merge(a, on=['date', 'code'], validate='many_to_one')
    if len(j) != 5247 * 4:
        stop('JOIN_CARDINALITY_MISMATCH')
    valid = j[j.asof_status.eq('ASOF_OK')].copy()
    valid['ma224_phase'] = np.where(valid.ma224_below.eq(1), 'BELOW', 'ABOVE_OR_EQUAL')
    valid['amount_state'] = np.where(valid.amount3d_above_prior10.eq(1), 'AMOUNT_UP', 'AMOUNT_NOT_UP')
    scores = []
    for h, g in valid.groupby('horizon', sort=True):
        scores.append(one_score(g, '__ALL__', 'ALL', 'ALL_VALID_BB'))
        for fld in FLAGS:
            for val in (0, 1):
                scores.append(one_score(g, fld, val, 'ALL_VALID_BB'))
                for ma in ('BELOW', 'ABOVE_OR_EQUAL'):
                    subset = g[g.ma224_phase.eq(ma)]
                    if not subset.empty:
                        scores.append(one_score(subset, fld, val, 'MA224_' + ma))
                for amt in ('AMOUNT_UP', 'AMOUNT_NOT_UP'):
                    subset = g[g.amount_state.eq(amt)]
                    if not subset.empty:
                        scores.append(one_score(subset, fld, val, amt))
                # Original frozen 3d-vs-prior10 amount state, crossed with signal-day MA224.
                for ma in ('BELOW', 'ABOVE_OR_EQUAL'):
                    for amt in ('AMOUNT_UP', 'AMOUNT_NOT_UP'):
                        subset = g[g.ma224_phase.eq(ma) & g.amount_state.eq(amt)]
                        if not subset.empty:
                            scores.append(one_score(subset, fld, val, 'MA224_' + ma + '__' + amt))
        for ma in ('BELOW', 'ABOVE_OR_EQUAL'):
            if g.ma224_phase.eq(ma).any():
                scores.append(one_score(g[g.ma224_phase.eq(ma)], '__ALL__', 'ALL', 'MA224_' + ma))
    # Descriptive equal-date contrasts, no stock-level independence assumption.
    pairs = []
    for h, hg in valid[valid.status_out.eq(DONE)].groupby('horizon'):
        for flag in FLAGS:
            vals = []
            for day, g in hg.groupby('date'):
                yes = g[g[flag].eq(1)]
                no = g[g[flag].eq(0)]
                if len(yes) and len(no):
                    vals.append((day,
                                 100 * (yes.net_20bps_pct.gt(0).mean() - no.net_20bps_pct.gt(0).mean()),
                                 float(yes.net_20bps_pct.median() - no.net_20bps_pct.median()),
                                 100 * (yes.net_20bps_pct.le(-5).mean() - no.net_20bps_pct.le(-5).mean())))
            pair = np.asarray([[v[1], v[2], v[3]] for v in vals], dtype=float).reshape(-1,3)
            pairs.append({'horizon': int(h), 'flag': flag, 'matched_dates': len(vals),
                          'equal_date_win_diff_pp': pair[:,0].mean() if len(pair) else np.nan,
                          'equal_date_median_net_diff_pp': pair[:,1].mean() if len(pair) else np.nan,
                          'equal_date_failure5_diff_pp': pair[:,2].mean() if len(pair) else np.nan,
                          'weak_match_warning': int(len(vals) < 8), 'not_causal': True})
    out = Path(out)
    write_csv(pd.DataFrame(scores), out / 'R1D19_HISTORICAL_BB_FLAG_SCORECARDS_NOT_OOS.csv')
    write_csv(pd.DataFrame(pairs), out / 'R1D19_SAME_DATE_DESCRIPTIVE_CONTRASTS_NOT_OOS.csv')
    # Data audit: do not hide errors or treat blocked ASOF features as zero flags.
    quality = j[['date', 'code', 'horizon', 'asof_status', 'status_out']].copy()
    write_csv(quality, out / 'R1D19_QUALITY_ALL_EVENTS_NO_FILL.csv.gz')
    manifest = {'research_id': VERSION, 'historical_signals': len(features),
                'unmodified_r1d6_population': len(original_asof),
                'd7_original_path_rows': len(paths), 'matched_horizon_rows': len(j),
                'bb_feature_eligible_events': int(features.asof_status.eq('ASOF_OK').sum()),
                'bb_feature_blocked_events': int(features.asof_status.ne('ASOF_OK').sum()),
                'complete_d20_original': 5214,
                'scope': 'R1D6_224_PROXY_ONLY_NOT_ALL_KRX_RECLAIMS',
                'future_independent_oos': False, 'same_sample_tuning': False,
                'signal_outcome_joint_rank_not_authorized': True,
                'new_signal_captures': 0, 'orders_placed': 0,
                'production_unchanged': True, 'strategy_ready': False,
                'signal_entry_not_actual_execution': True, 'corporate_actions_not_certified': True}
    save_json(manifest, out / 'R1D19_MANIFEST.json')
    return manifest


def run(args):
    p = source_protocol(args.protocol)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    if args.mode == 'protocol':
        save_json({'protocol': VERSION, 'source_refs_checked_in_protocol': True,
                   'historical_backtest_executed': False, 'new_oos': False,
                   'orders_placed': 0, 'production_unchanged': True},
                  out / 'R1D19_PROTOCOL_LOCK.json')
        return
    if args.mode != 'historical':
        stop('MODE_NOT_ALLOWED')
    asof = read_frozen_asof(args.d6_asof, p)
    features, evidence = seal_features(args.krx_root, asof, p, out)
    # R1D7 outcome is read ONLY after the ASOF file is on disk, sha-sealed.
    if not (out / 'R1D19_ASOF_ONLY_BB_RECLAIM_NO_OUTCOME.csv.gz').is_file():
        stop('ASOF_NOT_SEALED_BEFORE_OUTCOMES')
    paths = read_r1d7_paths(args.d7, set(zip(asof.date, asof.code)))
    manifest = compare(features, paths, asof, out)
    manifest['feature_csv_sha256'] = evidence['asof_file_sha256']
    manifest['krx_daily_sha_verified'] = evidence['source_days_sha_verified']
    save_json(manifest, out / 'R1D19_MANIFEST.json')
    print('R1D19_HISTORICAL_EXPLORATION_ONLY', json.dumps(manifest, ensure_ascii=False), flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=('protocol','historical'), required=True)
    parser.add_argument('--protocol', default='R1D19_PREDECLARED_PROTOCOL.json')
    parser.add_argument('--d6-asof', default='')
    parser.add_argument('--d7', default='')
    parser.add_argument('--krx-root', default='')
    parser.add_argument('--out', required=True)
    run(parser.parse_args())


if __name__ == '__main__':
    main()
