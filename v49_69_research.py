from __future__ import annotations

import itertools
import json
import math
import os
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

FORBIDDEN_FEATURE_TOKENS = (
    'path_', 'rule35', 'ret_next', 'ret_close', 'ret_max', 'ret_min',
    'hit3', 'hit5', 'hit10', 'stop_before', 'first_event', 'eval_',
    'outcome', '_net', 'pnl', 'future', 'forward',
)
TARGET_STRATEGIES = ('L', 'G', 'S')
DOWN_MARKET_TARGETS = ('ALL', 'LP', 'L', 'G', 'S')


def _num(df: pd.DataFrame, col: str, default=np.nan) -> pd.Series:
    if col in df.columns:
        return pd.to_numeric(df[col], errors='coerce')
    return pd.Series(default, index=df.index, dtype=float)


def _date(df: pd.DataFrame) -> pd.Series:
    return pd.to_datetime(df.get('signal_date', pd.Series('', index=df.index)), errors='coerce').dt.normalize()


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)) or default)
    except Exception:
        return float(default)


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(os.environ.get(name, str(default)) or default))
    except Exception:
        return int(default)


def stage_boundaries(start_date: str, end_date: str) -> dict[str, Any]:
    start = pd.Timestamp(start_date).normalize()
    end = pd.Timestamp(end_date).normalize()
    if end <= start:
        raise ValueError(f'invalid date range: {start_date}..{end_date}')
    train_ratio = min(0.70, max(0.35, _env_float('CLOSING_BET_V4967_TRAIN_RATIO', 0.50)))
    validation_ratio = min(0.35, max(0.15, _env_float('CLOSING_BET_V4967_VALIDATION_RATIO', 0.25)))
    if train_ratio + validation_ratio >= 0.90:
        validation_ratio = max(0.10, 0.90 - train_ratio)
    train_override = str(os.environ.get('CLOSING_BET_V4967_TRAIN_END', '') or '').strip()
    val_override = str(os.environ.get('CLOSING_BET_V4967_VALIDATION_END', '') or '').strip()
    span = end - start
    train_end = pd.Timestamp(train_override).normalize() if train_override else (start + span * train_ratio).normalize()
    validation_end = pd.Timestamp(val_override).normalize() if val_override else (start + span * (train_ratio + validation_ratio)).normalize()
    min_gap = pd.Timedelta(days=14)
    if train_end <= start + min_gap:
        train_end = start + min_gap
    if validation_end <= train_end + min_gap:
        validation_end = train_end + min_gap
    if validation_end >= end:
        validation_end = end - min_gap
    if not (start < train_end < validation_end < end):
        raise ValueError(f'triple split invalid: {start=} {train_end=} {validation_end=} {end=}')
    return {
        'start': start,
        'train_end': train_end,
        'validation_start': train_end + pd.Timedelta(days=1),
        'validation_end': validation_end,
        'test_start': validation_end + pd.Timedelta(days=1),
        'test_end': end,
        'train_ratio_requested': train_ratio,
        'validation_ratio_requested': validation_ratio,
        'authority': 'CALENDAR_DATE_LOCK_BEFORE_FEATURE_SELECTION',
        'test_purity': 'LOCKED_WITHIN_V49_69_RUN_NOT_HISTORICALLY_UNSEEN_BECAUSE_PRIOR_VERSIONS_EXPOSED_PERIOD',
    }


def attach_stage(df: pd.DataFrame, boundaries: dict[str, Any]) -> pd.DataFrame:
    z = df.copy()
    z['_date'] = _date(z)
    z['sample_stage'] = np.select(
        [z['_date'].le(boundaries['train_end']), z['_date'].le(boundaries['validation_end'])],
        ['TRAIN', 'VALIDATION'],
        default='LOCKED_TEST',
    )
    z.loc[z['_date'].isna(), 'sample_stage'] = 'INVALID_DATE'
    return z


def _top_removed_mean(values: pd.Series, k: int) -> float:
    v = pd.to_numeric(values, errors='coerce').dropna().sort_values(ascending=False)
    if len(v) <= k:
        return np.nan
    return float(v.iloc[k:].mean())


def _equity_metrics(df: pd.DataFrame, return_col: str) -> dict[str, float]:
    if df.empty:
        return {'portfolio_n': 0, 'portfolio_total_pct': np.nan, 'portfolio_mdd_pct': np.nan, 'positive_month_pct': np.nan}
    z = df.copy()
    z['_date'] = _date(z)
    z['_ret'] = pd.to_numeric(z.get(return_col), errors='coerce')
    z['_score'] = pd.to_numeric(z.get('score', 0), errors='coerce').fillna(0)
    z['_amount'] = pd.to_numeric(z.get('entry_amount_b', z.get('amount_b', 0)), errors='coerce').fillna(0)
    z = z[z['_date'].notna() & z['_ret'].notna()].sort_values(['_date', '_score', '_amount'], ascending=[True, False, False])
    z = z.drop_duplicates('_date', keep='first')
    if z.empty:
        return {'portfolio_n': 0, 'portfolio_total_pct': np.nan, 'portfolio_mdd_pct': np.nan, 'positive_month_pct': np.nan}
    equity = (1.0 + z['_ret'].clip(lower=-99.0) / 100.0).cumprod()
    dd = equity / equity.cummax() - 1.0
    monthly = z.set_index('_date')['_ret'].resample('ME').sum()
    return {
        'portfolio_n': int(len(z)),
        'portfolio_total_pct': float((equity.iloc[-1] - 1.0) * 100.0),
        'portfolio_mdd_pct': float(dd.min() * 100.0),
        'positive_month_pct': float((monthly > 0).mean() * 100.0) if len(monthly) else np.nan,
    }


def metric_summary(df: pd.DataFrame, return_col: str = '_net50') -> dict[str, Any]:
    if df.empty:
        return {
            'n': 0, 'mean_pct': np.nan, 'median_pct': np.nan, 'win_pct': np.nan,
            'top1_removed_pct': np.nan, 'top3_removed_pct': np.nan,
            'top5_removed_pct': np.nan, 'top10_removed_pct': np.nan,
            'stop_rate_pct': np.nan, 'big_rate_pct': np.nan,
            'early_n': 0, 'early_mean_pct': np.nan, 'late_n': 0, 'late_mean_pct': np.nan,
            **_equity_metrics(df, return_col),
        }
    z = df.copy()
    r = pd.to_numeric(z.get(return_col), errors='coerce')
    z = z[r.notna()].copy()
    z['_metric_ret'] = r[r.notna()].astype(float)
    if z.empty:
        return metric_summary(pd.DataFrame(), return_col)
    dates = _date(z)
    valid_dates = dates.dropna().sort_values()
    half = valid_dates.iloc[len(valid_dates) // 2] if len(valid_dates) else pd.NaT
    early = z[dates.le(half)] if pd.notna(half) else z.iloc[0:0]
    late = z[dates.gt(half)] if pd.notna(half) else z.iloc[0:0]
    out = {
        'n': int(len(z)),
        'mean_pct': float(z['_metric_ret'].mean()),
        'median_pct': float(z['_metric_ret'].median()),
        'win_pct': float(z['_metric_ret'].gt(0).mean() * 100.0),
        'top1_removed_pct': _top_removed_mean(z['_metric_ret'], 1),
        'top3_removed_pct': _top_removed_mean(z['_metric_ret'], 3),
        'top5_removed_pct': _top_removed_mean(z['_metric_ret'], 5),
        'top10_removed_pct': _top_removed_mean(z['_metric_ret'], 10),
        'stop_rate_pct': float(_num(z, 'stop_first_flag', 0).fillna(0).gt(0).mean() * 100.0),
        'big_rate_pct': float(_num(z, 'big_before_stop', 0).fillna(0).gt(0).mean() * 100.0),
        'early_n': int(len(early)),
        'early_mean_pct': float(pd.to_numeric(early['_metric_ret'], errors='coerce').mean()) if len(early) else np.nan,
        'late_n': int(len(late)),
        'late_mean_pct': float(pd.to_numeric(late['_metric_ret'], errors='coerce').mean()) if len(late) else np.nan,
    }
    out.update(_equity_metrics(z, '_metric_ret'))
    return out


def _rule_mask(df: pd.DataFrame, rule: dict[str, Any]) -> pd.Series:
    mask = pd.Series(True, index=df.index)
    for part in rule.get('parts', []):
        vals = _num(df, str(part['feature']))
        if part['direction'] == 'HIGH_GOOD':
            part_mask = vals.ge(float(part['threshold']))
        else:
            part_mask = vals.le(float(part['threshold']))
        mask &= part_mask.fillna(False)
    return mask


def _rule_text(rule: dict[str, Any], feature_meta: dict[str, tuple[str, str]]) -> str:
    pieces = []
    for p in rule.get('parts', []):
        label = feature_meta.get(p['feature'], (p['feature'], ''))[0]
        op = '≥' if p['direction'] == 'HIGH_GOOD' else '≤'
        pieces.append(f'{label}{op}{float(p["threshold"]):.4f}')
    return ' AND '.join(pieces)


def _validation_score(metrics: dict[str, Any], baseline: dict[str, Any]) -> float:
    n = int(metrics.get('n', 0) or 0)
    mean = float(metrics.get('mean_pct', np.nan))
    top3 = float(metrics.get('top3_removed_pct', np.nan))
    if not np.isfinite(mean):
        return -1e9
    lift = mean - float(baseline.get('mean_pct', np.nan))
    robust = top3 if np.isfinite(top3) else -9.0
    return mean + 0.60 * lift + 0.25 * robust + min(n, 150) / 1000.0


def _pairwise_redundancy(df: pd.DataFrame, candidates: list[dict[str, Any]], scope: str, strategy: str) -> pd.DataFrame:
    rows = []
    for a, b in itertools.combinations(candidates, 2):
        ma = _rule_mask(df, a)
        mb = _rule_mask(df, b)
        inter = int((ma & mb).sum())
        union = int((ma | mb).sum())
        jaccard = inter / union if union else np.nan
        av = ma.astype(int).to_numpy(); bv = mb.astype(int).to_numpy()
        phi = float(np.corrcoef(av, bv)[0, 1]) if av.std() > 0 and bv.std() > 0 else np.nan
        rows.append({
            'scope': scope, 'strategy': strategy,
            'rule_a': a['rule_id'], 'rule_b': b['rule_id'],
            'intersection_n': inter, 'union_n': union,
            'jaccard': jaccard, 'phi': phi,
            'high_redundancy': int(np.isfinite(jaccard) and jaccard >= _env_float('CLOSING_BET_V4967_REDUNDANCY_JACCARD', 0.80)),
        })
    return pd.DataFrame(rows)


def _feature_rule_library(target: pd.DataFrame, strategy: str, feature_meta: dict[str, tuple[str, str]]) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any] | None]:
    train = target[target.sample_stage.eq('TRAIN')]
    validation = target[target.sample_stage.eq('VALIDATION')]
    test = target[target.sample_stage.eq('LOCKED_TEST')]
    min_train = _env_int('CLOSING_BET_V4967_FEATURE_MIN_TRAIN_N', 40)
    min_val = _env_int('CLOSING_BET_V4967_FEATURE_MIN_VALIDATION_N', 15)
    min_test = _env_int('CLOSING_BET_V4967_FEATURE_MIN_TEST_N', 15)
    min_lift = _env_float('CLOSING_BET_V4967_VALIDATION_MIN_LIFT_PCTP', 0.30)
    base_val = metric_summary(validation)
    candidates: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []

    for feat, (label, causal) in feature_meta.items():
        if causal == 'AUDIT_PROXY' or any(tok in feat.lower() for tok in FORBIDDEN_FEATURE_TOKENS):
            continue
        t = _num(train, feat)
        valid = t.notna()
        win = train.get('net50_group', pd.Series('', index=train.index)).astype(str).eq('WIN_NET50') & valid
        loss = train.get('net50_group', pd.Series('', index=train.index)).astype(str).eq('LOSS_NET50') & valid
        if int(valid.sum()) < min_train or int(win.sum()) < 12 or int(loss.sum()) < 12:
            continue
        winner_median = float(t[win].median())
        loser_median = float(t[loss].median())
        threshold = float(t[valid].median())
        direction = 'HIGH_GOOD' if winner_median >= loser_median else 'LOW_GOOD'
        rule = {
            'rule_id': f'{strategy}:SINGLE:{feat}', 'model_type': 'SINGLE',
            'parts': [{'feature': feat, 'direction': direction, 'threshold': threshold}],
        }
        vm = metric_summary(validation[_rule_mask(validation, rule)])
        val_lift = vm['mean_pct'] - base_val['mean_pct'] if np.isfinite(vm['mean_pct']) and np.isfinite(base_val['mean_pct']) else np.nan
        eligible = bool(vm['n'] >= min_val and np.isfinite(vm['mean_pct']) and vm['mean_pct'] > 0 and np.isfinite(val_lift) and val_lift >= min_lift and np.isfinite(vm['top3_removed_pct']) and vm['top3_removed_pct'] > 0)
        row = {
            'strategy': strategy, 'rule_id': rule['rule_id'], 'model_type': 'SINGLE',
            'rule_text': _rule_text(rule, feature_meta), 'feature_1': feat, 'feature_2': '',
            'train_threshold_1': threshold, 'direction_1': direction,
            'train_winner_median_1': winner_median, 'train_loser_median_1': loser_median,
            'validation_base_n': base_val['n'], 'validation_base_net50': base_val['mean_pct'],
            'validation_n': vm['n'], 'validation_net50': vm['mean_pct'], 'validation_lift_pctp': val_lift,
            'validation_top3_removed_net50': vm['top3_removed_pct'], 'validation_stop_rate_pct': vm['stop_rate_pct'],
            'validation_score': _validation_score(vm, base_val), 'validation_eligible': int(eligible),
            'test_base_n': np.nan, 'test_base_net50': np.nan,
            'test_n': np.nan, 'test_net50': np.nan, 'test_lift_pctp': np.nan,
            'test_top3_removed_net50': np.nan, 'test_stop_rate_pct': np.nan,
            'selected_on_validation': 0, 'locked_test_status': 'NOT_SELECTED', 'auto_apply': 0,
        }
        rows.append(row)
        rule['row'] = row
        candidates.append(rule)

    eligible_rules = [r for r in candidates if r['row']['validation_eligible'] == 1]
    eligible_rules.sort(key=lambda r: r['row']['validation_score'], reverse=True)
    redundancy = _pairwise_redundancy(validation, eligible_rules, 'VALIDATION', strategy)
    # Collapse highly overlapping signals before pair search.
    representatives: list[dict[str, Any]] = []
    red_cut = _env_float('CLOSING_BET_V4967_REDUNDANCY_JACCARD', 0.80)
    for rule in eligible_rules:
        mask = _rule_mask(validation, rule)
        redundant = False
        for rep in representatives:
            rm = _rule_mask(validation, rep)
            union = int((mask | rm).sum())
            jac = int((mask & rm).sum()) / union if union else 0.0
            if jac >= red_cut:
                redundant = True
                rule['row']['redundant_with'] = rep['rule_id']
                break
        if not redundant:
            representatives.append(rule)
        if len(representatives) >= _env_int('CLOSING_BET_V4967_MAX_NONREDUNDANT_FEATURES', 5):
            break

    pair_rows = []
    for a, b in itertools.combinations(representatives, 2):
        rule = {
            'rule_id': f'{strategy}:PAIR:{a["parts"][0]["feature"]}+{b["parts"][0]["feature"]}',
            'model_type': 'PAIR', 'parts': a['parts'] + b['parts'],
        }
        vm = metric_summary(validation[_rule_mask(validation, rule)])
        val_lift = vm['mean_pct'] - base_val['mean_pct'] if np.isfinite(vm['mean_pct']) and np.isfinite(base_val['mean_pct']) else np.nan
        eligible = bool(vm['n'] >= min_val and np.isfinite(vm['mean_pct']) and vm['mean_pct'] > 0 and np.isfinite(val_lift) and val_lift >= min_lift and np.isfinite(vm['top3_removed_pct']) and vm['top3_removed_pct'] > 0)
        row = {
            'strategy': strategy, 'rule_id': rule['rule_id'], 'model_type': 'PAIR', 'rule_text': _rule_text(rule, feature_meta),
            'feature_1': a['parts'][0]['feature'], 'feature_2': b['parts'][0]['feature'],
            'train_threshold_1': a['parts'][0]['threshold'], 'direction_1': a['parts'][0]['direction'],
            'train_threshold_2': b['parts'][0]['threshold'], 'direction_2': b['parts'][0]['direction'],
            'validation_base_n': base_val['n'], 'validation_base_net50': base_val['mean_pct'],
            'validation_n': vm['n'], 'validation_net50': vm['mean_pct'], 'validation_lift_pctp': val_lift,
            'validation_top3_removed_net50': vm['top3_removed_pct'], 'validation_stop_rate_pct': vm['stop_rate_pct'],
            'validation_score': _validation_score(vm, base_val), 'validation_eligible': int(eligible),
            'test_base_n': np.nan, 'test_base_net50': np.nan,
            'test_n': np.nan, 'test_net50': np.nan, 'test_lift_pctp': np.nan,
            'test_top3_removed_net50': np.nan, 'test_stop_rate_pct': np.nan,
            'selected_on_validation': 0, 'locked_test_status': 'NOT_SELECTED', 'auto_apply': 0,
        }
        rows.append(row); pair_rows.append((rule, row))

    all_selectable = [(r, r['row']) for r in eligible_rules] + pair_rows
    all_selectable = [x for x in all_selectable if x[1]['validation_eligible'] == 1]
    selected_rule = None
    if all_selectable:
        selected_rule, selected_row = max(all_selectable, key=lambda x: float(x[1]['validation_score']))
        # The selection is finalized before any locked-test acceptance check.
        selected_row['selected_on_validation'] = 1
        # LOCKED_TEST is accessed only after VALIDATION has finalized exactly one model.
        base = metric_summary(test)
        tm = metric_summary(test[_rule_mask(test, selected_rule)])
        lift = tm['mean_pct'] - base['mean_pct'] if np.isfinite(tm['mean_pct']) and np.isfinite(base['mean_pct']) else np.nan
        selected_row['test_base_n'] = base['n']
        selected_row['test_base_net50'] = base['mean_pct']
        selected_row['test_n'] = tm['n']
        selected_row['test_net50'] = tm['mean_pct']
        selected_row['test_lift_pctp'] = lift
        selected_row['test_top3_removed_net50'] = tm['top3_removed_pct']
        selected_row['test_stop_rate_pct'] = tm['stop_rate_pct']
        half_min = max(5, min_test // 2)
        stable = bool(tm['early_n'] >= half_min and tm['late_n'] >= half_min and tm['early_mean_pct'] > 0 and tm['late_mean_pct'] > 0)
        passed = bool(tm['n'] >= min_test and np.isfinite(tm['mean_pct']) and tm['mean_pct'] > 0 and np.isfinite(lift) and lift > 0 and np.isfinite(tm['top3_removed_pct']) and tm['top3_removed_pct'] > 0 and stable)
        selected_row['locked_test_status'] = 'LOCKED_TEST_PASS' if passed else 'LOCKED_TEST_FAIL'
        selected_row['test_halves_stable'] = int(stable)
        selected_row['selection_authority'] = 'VALIDATION_ONLY'
        selected_row['test_access_count'] = 1
        selected_rule['selected_row'] = selected_row

    library = pd.DataFrame(rows)
    return library, redundancy, selected_rule


def _policy_gross(df: pd.DataFrame, policy: str) -> pd.Series:
    idx = df.index
    close = {d: _num(df, f'ret_close_{d}d') for d in (1, 3, 5, 10)}
    p3 = _num(df, 'path_first_plus3_day', 0).fillna(0)
    p5 = _num(df, 'path_first_plus5_day', 0).fillna(0)
    stop = _num(df, 'path_first_stop_day', 0).fillna(0)
    if policy == 'CURRENT_35':
        return _num(df, 'rule35_pnl')
    if policy.startswith('CLOSE_D'):
        day = int(policy.replace('CLOSE_D', ''))
        return close[day]
    if policy == 'FULL_TP3_STOP3_D10':
        target_first = p3.gt(0) & ((stop.le(0)) | p3.lt(stop))
        stop_first = stop.gt(0) & ((p3.le(0)) | stop.le(p3))
        return pd.Series(np.select([target_first, stop_first], [3.0, -3.0], default=close[10]), index=idx, dtype=float)
    if policy == 'FULL_TP5_STOP3_D10':
        target_first = p5.gt(0) & ((stop.le(0)) | p5.lt(stop))
        stop_first = stop.gt(0) & ((p5.le(0)) | stop.le(p5))
        return pd.Series(np.select([target_first, stop_first], [5.0, -3.0], default=close[10]), index=idx, dtype=float)
    specs = {
        'EXIT70_TP3_RUN30_D5': (0.70, 5),
        'EXIT70_TP3_RUN30_D10': (0.70, 10),
        'EXIT50_TP3_RUN50_D5': (0.50, 5),
        'EXIT50_TP3_RUN50_D10': (0.50, 10),
    }
    if policy in specs:
        exit_weight, horizon = specs[policy]
        remain = 1.0 - exit_weight
        plus3_first = p3.gt(0) & ((stop.le(0)) | p3.lt(stop))
        stop_before = stop.gt(0) & ((p3.le(0)) | stop.le(p3))
        stop_after_before_horizon = plus3_first & stop.gt(p3) & stop.le(horizon)
        runner_ret = close[horizon].where(~stop_after_before_horizon, -3.0)
        no_plus_ret = close[horizon].where(~(stop.gt(0) & stop.le(horizon)), -3.0)
        out = no_plus_ret.copy()
        out = out.where(~plus3_first, exit_weight * 3.0 + remain * runner_ret)
        out = out.where(~stop_before, -3.0)
        return out
    raise KeyError(policy)


def lp_exit_audit(z: pd.DataFrame, out: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    lp = z[z.get('mode', pd.Series('', index=z.index)).astype(str).eq('LP')].copy()
    policies = [
        'CURRENT_35', 'CLOSE_D1', 'CLOSE_D3', 'CLOSE_D5', 'CLOSE_D10',
        'FULL_TP3_STOP3_D10', 'FULL_TP5_STOP3_D10',
        'EXIT70_TP3_RUN30_D5', 'EXIT70_TP3_RUN30_D10',
        'EXIT50_TP3_RUN50_D5', 'EXIT50_TP3_RUN50_D10',
    ]
    rows = []
    cost = _env_float('CLOSING_BET_V4967_EXIT_COST_PCT', 0.50)
    # TRAIN and VALIDATION may compare every policy. LOCKED_TEST is not touched here.
    for policy in policies:
        gross = _policy_gross(lp, policy)
        lp[f'_policy_{policy}'] = gross - cost
        for stage in ('TRAIN', 'VALIDATION'):
            q = lp[lp.sample_stage.eq(stage)].copy()
            q['_policy_ret'] = pd.to_numeric(q[f'_policy_{policy}'], errors='coerce')
            rows.append({'policy': policy, 'stage': stage, **metric_summary(q, '_policy_ret'), 'test_accessed': 0})
    audit = pd.DataFrame(rows)
    val = audit[audit.stage.eq('VALIDATION')].copy()
    baseline_val = val[val.policy.eq('CURRENT_35')].iloc[0].to_dict() if len(val[val.policy.eq('CURRENT_35')]) else {}
    val['lift_vs_current_pctp'] = pd.to_numeric(val['mean_pct'], errors='coerce') - float(baseline_val.get('mean_pct', np.nan))
    min_val = _env_int('CLOSING_BET_V4967_LP_EXIT_MIN_VALIDATION_N', 15)
    eligible = val[(val['n'] >= min_val) & val['mean_pct'].gt(0) & val['top3_removed_pct'].gt(0)].copy()
    if eligible.empty:
        selected_policy = 'CURRENT_35'
    else:
        eligible['selection_score'] = eligible['mean_pct'] + .5 * eligible['lift_vs_current_pctp'] + .25 * eligible['top3_removed_pct'] + eligible['n'].clip(upper=150) / 1000.0
        selected_policy = str(eligible.sort_values('selection_score', ascending=False).iloc[0]['policy'])

    # Only the frozen current-policy baseline and the one VALIDATION-selected policy are opened on LOCKED_TEST.
    test = lp[lp.sample_stage.eq('LOCKED_TEST')].copy()
    test_policies = ['CURRENT_35'] if selected_policy == 'CURRENT_35' else ['CURRENT_35', selected_policy]
    test_docs = {}
    for policy in test_policies:
        q = test.copy(); q['_policy_ret'] = pd.to_numeric(q[f'_policy_{policy}'], errors='coerce')
        m = metric_summary(q, '_policy_ret'); test_docs[policy] = m
        rows.append({'policy': policy, 'stage': 'LOCKED_TEST', **m, 'test_accessed': 1})
    audit = pd.DataFrame(rows)
    selected = test_docs.get(selected_policy, {})
    base_test = test_docs.get('CURRENT_35', {})
    lift = float(selected.get('mean_pct', np.nan)) - float(base_test.get('mean_pct', np.nan))
    min_test = _env_int('CLOSING_BET_V4967_LP_EXIT_MIN_TEST_N', 15)
    stable = bool(int(selected.get('early_n', 0) or 0) >= 5 and int(selected.get('late_n', 0) or 0) >= 5 and float(selected.get('early_mean_pct', np.nan)) > 0 and float(selected.get('late_mean_pct', np.nan)) > 0)
    passed = bool(int(selected.get('n', 0) or 0) >= min_test and float(selected.get('mean_pct', np.nan)) > 0 and float(selected.get('top3_removed_pct', np.nan)) > 0 and stable and (selected_policy == 'CURRENT_35' or lift > 0))
    selected_doc = pd.DataFrame([{
        'selected_policy': selected_policy,
        'selection_authority': 'VALIDATION_ONLY',
        'locked_test_status': 'BASELINE_RETAIN' if selected_policy == 'CURRENT_35' and passed else ('LOCKED_TEST_PASS' if passed else 'LOCKED_TEST_FAIL'),
        'test_n': int(selected.get('n', 0) or 0), 'test_net50': selected.get('mean_pct', np.nan),
        'test_lift_vs_current_pctp': lift, 'test_top3_removed_net50': selected.get('top3_removed_pct', np.nan),
        'test_mdd_pct': selected.get('portfolio_mdd_pct', np.nan), 'test_halves_stable': int(stable),
        'locked_test_policies_opened': ','.join(test_policies), 'test_access_count': len(test_policies),
        'auto_apply': 0,
    }])
    stop_cases = test[test.get('outcome_class', pd.Series('', index=test.index)).astype(str).eq('STOP_FIRST')].copy()
    keep = [c for c in ['signal_date','code','name','mode','outcome_class','rule35_pnl','path_first_plus3_day','path_first_stop_day','path_min_low_ret','entry_close_loc_pct','entry_upper_wick_pct','entry_amount_b','entry_ma20_dist_pct','entry_ma60_dist_pct','entry_high20_dist_pct','market_m5_t1','market_ret_5d_t1','stock_excess_5d'] if c in stop_cases.columns]
    stop_cases = stop_cases[keep]
    audit.to_csv(out / 'v49_69_lp_exit_policy_audit.csv', index=False, encoding='utf-8-sig')
    selected_doc.to_csv(out / 'v49_69_lp_exit_selected.csv', index=False, encoding='utf-8-sig')
    stop_cases.to_csv(out / 'v49_69_lp_locked_test_stop_cases.csv', index=False, encoding='utf-8-sig')
    return audit, selected_doc, stop_cases


def run_research(selected_ctx: pd.DataFrame, out: Path, start_date: str, end_date: str, feature_meta: dict[str, tuple[str, str]]) -> dict[str, Any]:
    out = Path(out); out.mkdir(parents=True, exist_ok=True)
    boundaries = stage_boundaries(start_date, end_date)
    z = attach_stage(selected_ctx, boundaries)
    if z.sample_stage.eq('INVALID_DATE').any():
        raise RuntimeError('V49_69_TRIPLE_SPLIT_INVALID_DATE')
    # Reassert leakage protection for the entire candidate dictionary.
    for feat in feature_meta:
        if any(tok in feat.lower() for tok in FORBIDDEN_FEATURE_TOKENS):
            raise RuntimeError(f'V49_69_LEAKAGE_GUARD:{feat}')

    libraries = []; redundancies = []; selected_rules = []
    for strategy in TARGET_STRATEGIES:
        target = z[z.get('mode', pd.Series('', index=z.index)).astype(str).eq(strategy)].copy()
        lib, red, selected = _feature_rule_library(target, strategy, feature_meta)
        libraries.append(lib); redundancies.append(red)
        if selected is not None:
            selected_rules.append(selected['selected_row'])

    # Down-market independent router: selection still uses net50 outcomes, but all input features are entry-time only.
    down_libraries = []; down_redundancies = []; down_selected = []
    down = z[z.get('market_down_context', pd.Series(0, index=z.index)).fillna(0).astype(int).eq(1)].copy()
    for target_name in DOWN_MARKET_TARGETS:
        target = down if target_name == 'ALL' else down[down.get('mode', pd.Series('', index=down.index)).astype(str).eq(target_name)]
        lib, red, selected = _feature_rule_library(target, f'DOWN_{target_name}', feature_meta)
        down_libraries.append(lib); down_redundancies.append(red)
        if selected is not None:
            down_selected.append(selected['selected_row'])

    library = pd.concat([x for x in libraries if not x.empty], ignore_index=True) if any(not x.empty for x in libraries) else pd.DataFrame()
    redundancy = pd.concat([x for x in redundancies if not x.empty], ignore_index=True) if any(not x.empty for x in redundancies) else pd.DataFrame()
    locked = pd.DataFrame(selected_rules)
    down_library = pd.concat([x for x in down_libraries if not x.empty], ignore_index=True) if any(not x.empty for x in down_libraries) else pd.DataFrame()
    down_redundancy = pd.concat([x for x in down_redundancies if not x.empty], ignore_index=True) if any(not x.empty for x in down_redundancies) else pd.DataFrame()
    down_locked = pd.DataFrame(down_selected)

    library.to_csv(out / 'v49_69_lgs_candidate_library.csv', index=False, encoding='utf-8-sig')
    redundancy.to_csv(out / 'v49_69_lgs_redundancy.csv', index=False, encoding='utf-8-sig')
    locked.to_csv(out / 'v49_69_lgs_locked_test.csv', index=False, encoding='utf-8-sig')
    down_library.to_csv(out / 'v49_69_down_market_candidate_library.csv', index=False, encoding='utf-8-sig')
    down_redundancy.to_csv(out / 'v49_69_down_market_redundancy.csv', index=False, encoding='utf-8-sig')
    down_locked.to_csv(out / 'v49_69_down_market_locked_test.csv', index=False, encoding='utf-8-sig')

    lp_audit, lp_selected, lp_stop = lp_exit_audit(z, out)
    stage_counts = z['sample_stage'].value_counts().to_dict()
    manifest = {
        'status': 'FULL-VALID',
        'boundaries': {k: (v.strftime('%Y-%m-%d') if isinstance(v, pd.Timestamp) else v) for k, v in boundaries.items()},
        'stage_counts': {str(k): int(v) for k, v in stage_counts.items()},
        'selection_discipline': {
            'threshold_discovery': 'TRAIN_ONLY',
            'model_selection': 'VALIDATION_ONLY',
            'locked_test_use': 'FINAL_ACCEPTANCE_ONCE',
            'test_access_count_per_selected_model': 1,
            'test_reselection_forbidden': True,
        },
        'historical_purity_warning': boundaries['test_purity'],
        'lgs_selected_models': int(len(locked)),
        'lgs_locked_test_pass': int(locked.get('locked_test_status', pd.Series(dtype=str)).eq('LOCKED_TEST_PASS').sum()) if not locked.empty else 0,
        'down_market_selected_models': int(len(down_locked)),
        'down_market_locked_test_pass': int(down_locked.get('locked_test_status', pd.Series(dtype=str)).eq('LOCKED_TEST_PASS').sum()) if not down_locked.empty else 0,
        'lp_selected_policy': str(lp_selected.iloc[0].get('selected_policy', '')) if len(lp_selected) else '',
        'lp_locked_test_status': str(lp_selected.iloc[0].get('locked_test_status', '')) if len(lp_selected) else '',
        'auto_apply': 0,
    }
    (out / 'v49_69_triple_split_manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    return {
        'enriched': z, 'boundaries': boundaries, 'manifest': manifest,
        'library': library, 'redundancy': redundancy, 'locked': locked,
        'down_library': down_library, 'down_redundancy': down_redundancy, 'down_locked': down_locked,
        'lp_audit': lp_audit, 'lp_selected': lp_selected, 'lp_stop': lp_stop,
    }
