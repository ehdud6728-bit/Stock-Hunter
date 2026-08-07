from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys

import numpy as np
import pandas as pd
import Closing_bet_scanner_v2 as s
import v49_70_research as r70
import v49_70_quality as q70



V4970_SECTOR_VALID_MIN_COVERAGE_PCT = float(os.environ.get('CLOSING_BET_V4970_SECTOR_MIN_COVERAGE_PCT','20') or 20)
V4970_REQUIRE_SECTOR_CONTEXT = str(os.environ.get('CLOSING_BET_V4970_REQUIRE_SECTOR_CONTEXT','0') or '0').lower() in ('1','true','yes','y','on')


def _v4970_context_status(coverage_pct: float, mapped_rows: int = 0, *, kind: str = 'SECTOR') -> str:
    cov=float(coverage_pct or 0.0)
    if kind == 'EVENT':
        return 'VALID' if int(mapped_rows or 0) > 0 else 'UNAVAILABLE'
    if cov >= float(V4970_SECTOR_VALID_MIN_COVERAGE_PCT): return 'VALID'
    if cov > 0.0: return 'DEGRADED'
    return 'UNAVAILABLE'


def file_sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def f(v) -> str:
    try:
        if pd.isna(v):
            return '평가없음'
        return f'{float(v):+.2f}%'
    except Exception:
        return '평가없음'


def _read_csvs(paths, **kwargs) -> pd.DataFrame:
    kwargs.setdefault('low_memory', False)
    frames = []
    for p in paths:
        try:
            x = pd.read_csv(p, **kwargs)
        except pd.errors.EmptyDataError:
            x = pd.DataFrame()
        if not x.empty:
            frames.append(x)
    return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()


def _split_date(start_date: str, end_date: str) -> pd.Timestamp:
    split = pd.Timestamp(getattr(s, 'CLOSING_BET_V4940_OOS_SPLIT_DATE', '2026-01-01'))
    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)
    if split <= start or split > end:
        split = start + (end - start) * .70
    return split.normalize()


def _deliver_telegram(parts: list[list[str]], out: Path, requested: bool) -> dict:
    delivery = {
        'requested': bool(requested),
        'route_validated': str(os.environ.get('TELEGRAM_ROUTE_VALIDATED', '') or ''),
        'pair_source': str(os.environ.get('TELEGRAM_ROUTE_PAIR_SOURCE', '') or ''),
        'alias_source': str(os.environ.get('TELEGRAM_ROUTE_ALIAS_SOURCE', '') or ''),
        'chat_id_masked': str(os.environ.get('TELEGRAM_ROUTE_CHAT_MASKED', '') or ''),
        'parts': [],
        'status': 'SKIPPED',
    }
    path = out / 'v49_70_telegram_delivery_manifest.json'
    try:
        if requested:
            if delivery['route_validated'] != '1':
                raise RuntimeError('v49.70 Telegram requested but unified route preflight was not validated')
            if not s._telegram_route_ready():
                raise RuntimeError('v49.70 Telegram requested but scanner route is not ready after validated preflight')
            for part_no, part in enumerate(parts, start=1):
                result = s.send_telegram_photo('\n'.join(part), [])
                delivery['parts'].append({'part': part_no, **result})
                if int(result.get('success_count', 0) or 0) < 1:
                    raise RuntimeError(f'v49.70 Telegram delivery failed for part {part_no}: {result.get("errors", [])}')
            delivery['status'] = 'DELIVERED'
            delivery['success_count'] = sum(int(x.get('success_count', 0) or 0) for x in delivery['parts'])
            print(
                f"TELEGRAM DELIVERY ACK ✅ · parts {len(delivery['parts'])}/{len(parts)} · "
                f"success {delivery['success_count']} · pair {delivery['pair_source']} · chat {delivery['chat_id_masked']}"
            )
        path.write_text(json.dumps(delivery, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
        return delivery
    except Exception as exc:
        delivery['status'] = 'FAILED'
        delivery['error'] = f'{type(exc).__name__}: {exc}'
        path.write_text(json.dumps(delivery, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
        raise


def _normalize_identity(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    x = df.copy()
    if 'code' not in x:
        x['code'] = ''
    if 'mode' not in x:
        x['mode'] = ''
    if 'signal_date' not in x:
        x['signal_date'] = ''
    x['code'] = x['code'].fillna('').astype(str).str.replace(r'\.0$', '', regex=True).str.zfill(6)
    x['mode'] = x['mode'].fillna('').astype(str).str.strip()
    parsed = pd.to_datetime(x['signal_date'], errors='coerce')
    x['_identity_date_valid'] = parsed.notna().astype(int)
    x['_identity_mode_valid'] = x['mode'].isin(list(s.CLOSING_BET_V4958_PRIMARY_PRIORITY)).astype(int)
    x['signal_date'] = parsed.dt.strftime('%Y-%m-%d').fillna('')
    audit = (
        x.groupby('mode', dropna=False)
        .agg(
            raw_input_n=('code', 'size'),
            missing_signal_date=('_identity_date_valid', lambda q: int((q == 0).sum())),
            invalid_mode=('_identity_mode_valid', lambda q: int((q == 0).sum())),
        )
        .reset_index()
        .rename(columns={'mode': 'strategy'})
    )
    valid = x[(x['_identity_date_valid'] == 1) & (x['_identity_mode_valid'] == 1)].copy()
    return valid.drop(columns=['_identity_date_valid', '_identity_mode_valid']), audit


def _performance_table(populations: dict[str, pd.DataFrame], start_date: str, end_date: str) -> pd.DataFrame:
    split = _split_date(start_date, end_date)
    rows = []
    modes = list(s.CLOSING_BET_V4958_PRIMARY_PRIORITY)
    for attribution, frame in populations.items():
        x = frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()
        if x.empty:
            for mode in modes:
                rows.append({'attribution': attribution, 'strategy': mode, 'n': 0, 'oos_n': 0})
            continue
        x['_date'] = pd.to_datetime(x.get('signal_date'), errors='coerce')
        x['_gross'] = pd.to_numeric(x.get('rule35_pnl'), errors='coerce')
        x = x[x['_date'].notna() & x['_gross'].notna()].copy()
        x['_net20'] = x['_gross'] - .20
        x['_net50'] = x['_gross'] - .50
        for mode in modes:
            z = x[x.get('mode', pd.Series('', index=x.index)).astype(str).eq(mode)]
            oo = z[z['_date'] >= split]
            rows.append({
                'attribution': attribution,
                'strategy': mode,
                'n': len(z),
                'oos_n': len(oo),
                'net20_mean_pct': float(z['_net20'].mean()) if len(z) else np.nan,
                'net50_mean_pct': float(z['_net50'].mean()) if len(z) else np.nan,
                'oos_net20_mean_pct': float(oo['_net20'].mean()) if len(oo) else np.nan,
                'oos_net50_mean_pct': float(oo['_net50'].mean()) if len(oo) else np.nan,
            })
    return pd.DataFrame(rows)


def _attribution_populations(canonical: pd.DataFrame, top_n: int) -> dict[str, pd.DataFrame]:
    predicate = s._select_backtest_top(canonical, top_per_strategy=top_n, all_candidates=False)
    z = canonical.copy()
    z['primary_strategy'] = z.get('primary_strategy', z.get('mode', pd.Series('', index=z.index))).fillna('').astype(str)
    primary = z[z['mode'].astype(str).eq(z['primary_strategy'])].copy()
    if primary.empty:
        primary = z.sort_values(['signal_date', 'code']).drop_duplicates(['signal_date', 'code'], keep='first').copy()
        primary['mode'] = primary['primary_strategy'].where(primary['primary_strategy'].ne(''), primary['mode'])
    primary = s._select_backtest_top(primary, top_per_strategy=top_n, all_candidates=False)

    base = z[z['mode'].astype(str).eq(z['primary_strategy'])].copy()
    if base.empty:
        base = z.sort_values(['signal_date', 'code']).drop_duplicates(['signal_date', 'code'], keep='first').copy()
    else:
        base = base.sort_values(['signal_date', 'code']).drop_duplicates(['signal_date', 'code'], keep='first').copy()
    base['_matched'] = base.get('all_matched_strategies', base.get('mode', pd.Series('', index=base.index))).fillna('').astype(str).str.split(',')
    any_match = base.explode('_matched').copy()
    any_match['mode'] = any_match['_matched'].fillna('').astype(str).str.strip()
    any_match = any_match[any_match['mode'].isin(list(s.CLOSING_BET_V4958_PRIMARY_PRIORITY))].drop(columns=['_matched'])
    any_match = any_match.drop_duplicates(['signal_date', 'code', 'mode'], keep='first')
    any_match = s._select_backtest_top(any_match, top_per_strategy=top_n, all_candidates=False)
    return {'PREDICATE_MODE': predicate, 'PRIMARY': primary, 'ALL_MATCHED': any_match}


def _lp_validation(selected: pd.DataFrame, out: Path, start_date: str, end_date: str) -> pd.DataFrame:
    z = selected[selected.get('mode', pd.Series('', index=selected.index)).astype(str).eq('LP')].copy()
    cols = ['fold', 'start', 'end', 'n', 'net20_mean_pct', 'net50_mean_pct', 'positive_month50_pct', 'status']
    if z.empty:
        r = pd.DataFrame(columns=cols)
        r.to_csv(out / 'v49_70_lp_walk_forward.csv', index=False, encoding='utf-8-sig')
        return r
    z['_date'] = pd.to_datetime(z.get('signal_date'), errors='coerce')
    z['_gross'] = pd.to_numeric(z.get('rule35_pnl'), errors='coerce')
    z = z[z['_date'].notna() & z['_gross'].notna()].copy()
    z['_net20'] = z['_gross'] - .20
    z['_net50'] = z['_gross'] - .50
    split = _split_date(start_date, end_date)
    rows = []
    cur = split
    fold = 1
    months = int(getattr(s, 'CLOSING_BET_V4964_LP_WALK_MONTHS', 3))
    min_n = int(getattr(s, 'CLOSING_BET_V4964_LP_MIN_FOLD_N', 15))
    data_end = pd.Timestamp(end_date)
    while cur <= data_end:
        nxt = min(data_end, cur + pd.DateOffset(months=months) - pd.Timedelta(days=1))
        q = z[z['_date'].between(cur, nxt)]
        if len(q):
            monthly = q.set_index('_date')['_net50'].resample('ME').sum()
            rows.append({
                'fold': fold,
                'start': cur.strftime('%Y-%m-%d'),
                'end': nxt.strftime('%Y-%m-%d'),
                'n': len(q),
                'net20_mean_pct': float(q['_net20'].mean()),
                'net50_mean_pct': float(q['_net50'].mean()),
                'positive_month50_pct': float((monthly > 0).mean() * 100) if len(monthly) else np.nan,
                'status': 'VALID' if len(q) >= min_n else 'LOW-N',
            })
        cur = nxt + pd.Timedelta(days=1)
        fold += 1
    r = pd.DataFrame(rows, columns=cols)
    r.to_csv(out / 'v49_70_lp_walk_forward.csv', index=False, encoding='utf-8-sig')
    return r


def _iit_split(selected: pd.DataFrame, out: Path, start_date: str, end_date: str) -> pd.DataFrame:
    x = selected.copy()
    x['_date'] = pd.to_datetime(x.get('signal_date'), errors='coerce')
    x['_gross'] = pd.to_numeric(x.get('rule35_pnl'), errors='coerce')
    x = x[x['_date'].notna() & x['_gross'].notna()].copy()
    x['_net20'] = x['_gross'] - .20
    x['_net50'] = x['_gross'] - .50
    split = _split_date(start_date, end_date)
    src = x.get('i_flow_source', x.get('flow_source', pd.Series('', index=x.index))).astype(str)
    x['_flow_group'] = np.where(src.str.contains('proxy', case=False, na=False), 'PROXY', 'REAL_OR_CACHE')
    rows = []
    for mode in ('I', 'IT'):
        for grp in ('REAL_OR_CACHE', 'PROXY'):
            z = x[(x.get('mode', pd.Series('', index=x.index)).astype(str) == mode) & (x['_flow_group'] == grp)]
            oo = z[z['_date'] >= split]
            rows.append({
                'strategy': mode,
                'flow_group': grp,
                'n': len(z),
                'oos_n': len(oo),
                'net20_mean_pct': float(z['_net20'].mean()) if len(z) else np.nan,
                'net50_mean_pct': float(z['_net50'].mean()) if len(z) else np.nan,
                'oos_net20_mean_pct': float(oo['_net20'].mean()) if len(oo) else np.nan,
                'oos_net50_mean_pct': float(oo['_net50'].mean()) if len(oo) else np.nan,
                'promotion_eligible': int(grp == 'REAL_OR_CACHE' and len(oo) > 0),
            })
    r = pd.DataFrame(rows)
    r.to_csv(out / 'v49_70_iit_flow_split.csv', index=False, encoding='utf-8-sig')
    return r


# ---- v49.70 winner/loser, market, sector and causal feature audit ----
V4967_ENTRY_FEATURE_META = {
    'entry_stock_ret_1d': ('종목 당일수익률', 'CAUSAL_CLOSE'),
    'entry_stock_ret_5d': ('종목 5일수익률', 'CAUSAL_CLOSE'),
    'entry_stock_ret_20d': ('종목 20일수익률', 'CAUSAL_CLOSE'),
    'entry_close_loc_pct': ('종가위치', 'CAUSAL_CLOSE'),
    'entry_upper_wick_pct': ('윗꼬리', 'CAUSAL_CLOSE'),
    'entry_gap_pct': ('시가갭', 'CAUSAL_CLOSE'),
    'entry_vol20_ratio': ('거래량20일배수', 'CAUSAL_CLOSE'),
    'entry_vol50_ratio': ('거래량50일배수', 'CAUSAL_CLOSE'),
    'entry_amount_b': ('거래대금억원', 'CAUSAL_CLOSE'),
    'entry_amount20_ratio': ('거래대금20일배수', 'CAUSAL_CLOSE'),
    'entry_ma20_dist_pct': ('20일선이격', 'CAUSAL_CLOSE'),
    'entry_ma60_dist_pct': ('60일선이격', 'CAUSAL_CLOSE'),
    'entry_high20_dist_pct': ('20일고점이격', 'CAUSAL_CLOSE'),
    'entry_range_pct': ('당일변동폭', 'CAUSAL_CLOSE'),
    'score': ('검색식점수', 'CAUSAL_CLOSE'),
    'matched_strategy_count': ('동시충족전략수', 'CAUSAL_CLOSE'),
    'market_ret_1d_t1': ('전일시장1일수익률', 'CAUSAL_T1'),
    'market_ret_5d_t1': ('전일시장5일수익률', 'CAUSAL_T1'),
    'market_ret_20d_t1': ('전일시장20일수익률', 'CAUSAL_T1'),
    'market_m5_dist_t1': ('전일시장5일선이격', 'CAUSAL_T1'),
    'market_ret_1d': ('당일시장수익률', 'CAUSAL_CLOSE'),
    'market_ret_5d': ('당일시장5일수익률', 'CAUSAL_CLOSE'),
    'market_ret_20d': ('당일시장20일수익률', 'CAUSAL_CLOSE'),
    'stock_excess_1d': ('시장대비당일초과', 'CAUSAL_CLOSE'),
    'stock_excess_5d': ('시장대비5일초과', 'CAUSAL_CLOSE'),
    'stock_excess_20d': ('시장대비20일초과', 'CAUSAL_CLOSE'),
    'sector_peer_count': ('동일섹터신호동료수', 'AUDIT_PROXY'),
    'sector_peer_mean_ret_1d': ('동일섹터동료평균수익률', 'AUDIT_PROXY'),
    'sector_peer_positive_pct': ('동일섹터동료상승비율', 'AUDIT_PROXY'),
}
V4967_FORBIDDEN_FEATURE_TOKENS = ('path_', 'rule35', 'ret_next', 'ret_close', 'hit3', 'stop_before', 'first_event', 'eval_')
V4967_MARKET_MIN_COVERAGE_PCT = float(os.environ.get('CLOSING_BET_V4967_MARKET_MIN_COVERAGE_PCT', '80') or 80)
V4967_FEATURE_MIN_OOS_N = max(10, int(float(os.environ.get('CLOSING_BET_V4967_FEATURE_MIN_OOS_N', '30') or 30)))
V4967_FEATURE_MIN_LIFT_PCTP = float(os.environ.get('CLOSING_BET_V4967_FEATURE_MIN_LIFT_PCTP', '0.30') or 0.30)


def _v4967_num_series(df: pd.DataFrame, col: str, default=np.nan) -> pd.Series:
    if col in df.columns:
        return pd.to_numeric(df[col], errors='coerce')
    return pd.Series(default, index=df.index, dtype=float)


def _v4967_first_text(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    out = pd.Series('', index=df.index, dtype=object)
    for c in cols:
        if c in df.columns:
            z = df[c].fillna('').astype(str).str.strip()
            out = out.where(out.ne(''), z)
    return out


def _v4967_index_frame(symbol: str, code: str, label: str, start_date: str, end_date: str) -> pd.DataFrame:
    try:
        fn = getattr(s, '_v538419_index_history', None)
        if callable(fn):
            z = fn(symbol, code, label, start_date, end_date)
        else:
            z = s._load_market_index_df_cached_for_i_core(symbol, code, label)
        if z is None or z.empty:
            return pd.DataFrame()
        z = z.copy()
        z['Date'] = pd.to_datetime(z.get('Date'), errors='coerce').dt.normalize()
        z['Close'] = pd.to_numeric(z.get('Close'), errors='coerce')
        z = z[z.Date.notna() & z.Close.notna()].sort_values('Date').drop_duplicates('Date', keep='last').reset_index(drop=True)
        c = z['Close']
        z['market_ret_1d'] = c.pct_change(1) * 100.0
        z['market_ret_5d'] = c.pct_change(5) * 100.0
        z['market_ret_20d'] = c.pct_change(20) * 100.0
        ma5 = c.rolling(5).mean(); ma20 = c.rolling(20).mean()
        z['market_m5_dist'] = (c / ma5 - 1.0) * 100.0
        z['market_ma20_dist'] = (c / ma20 - 1.0) * 100.0
        z['market_m5_slope5'] = ma5.pct_change(5) * 100.0
        z['market_m5_state_calc'] = np.where((z.market_m5_dist >= 0) & (z.market_m5_slope5 >= 0), 'M5-우호', np.where((z.market_m5_dist < 0) & (z.market_ret_5d < 0), 'M5-비우호', 'M5-중립'))
        for col in ['market_ret_1d','market_ret_5d','market_ret_20d','market_m5_dist','market_ma20_dist','market_m5_slope5','market_m5_state_calc']:
            z[col + '_t1'] = z[col].shift(1)
        z['market_source'] = str(z.get('market_source', pd.Series(label, index=z.index)).iloc[-1] if len(z) else label)
        return z
    except Exception:
        return pd.DataFrame()


def _v4967_event_bucket_row(row: pd.Series) -> str:
    text = ' '.join(str(row.get(c, '') or '') for c in ['sector_label','sector','sector_name','theme','theme_name','tags','material_hint','news_hint','reason','issue']).lower()
    groups = [
        ('ENERGY_OIL', ['석유','원유','유가','정유','가스','lng','lpg','호르무즈','유조선']),
        ('DEFENSE_WAR', ['방산','무기','미사일','군사','전쟁','드론','탄약']),
        ('RECONSTRUCTION', ['재건','우크라','건설기계','시멘트','굴삭기']),
        ('POWER_GRID_NUCLEAR', ['전력','전선','변압기','원전','원자력','송전','배전']),
        ('SHIPPING_SHIPBUILDING', ['해운','조선','선박','운임','물류']),
        ('SEMICONDUCTOR', ['반도체','hbm','메모리','파운드리','후공정']),
        ('ROBOT_AI', ['로봇','인공지능','ai ','자동화']),
        ('BATTERY_EV', ['2차전지','이차전지','배터리','전기차','리튬']),
        ('BIO_HEALTH', ['바이오','제약','신약','의료','헬스']),
    ]
    for label, kws in groups:
        if any(k in text for k in kws): return label
    return 'OTHER_OR_UNKNOWN'


def _v4967_attach_context(selected: pd.DataFrame, canonical: pd.DataFrame, universe: dict, start_date: str, end_date: str) -> tuple[pd.DataFrame, dict]:
    x = selected.copy()
    x['code'] = x.get('code', pd.Series('', index=x.index)).fillna('').astype(str).str.zfill(6)
    x['_date'] = pd.to_datetime(x.get('signal_date'), errors='coerce').dt.normalize()
    market_map = {str(k).zfill(6): str(v or '') for k,v in dict(universe.get('market_map') or {}).items()}
    sector_map = {str(k).zfill(6): str(v or '') for k,v in dict(universe.get('sector_map') or {}).items()}
    payload_market = _v4967_first_text(x, ['market','market_name','exchange','index_label','universe_tag'])
    payload_sector = _v4967_first_text(x, ['sector_name','sector','industry','theme_name','theme'])
    x['market_label'] = x['code'].map(market_map).fillna('').where(x['code'].map(market_map).fillna('').ne(''), payload_market)
    x['sector_label'] = x['code'].map(sector_map).fillna('').where(x['code'].map(sector_map).fillna('').ne(''), payload_sector)
    x['benchmark_name'] = np.where(x['market_label'].str.upper().str.contains('KOSDAQ|KQ|코스닥', regex=True, na=False), 'KOSDAQ', 'KOSPI')

    request_start = (pd.Timestamp(start_date) - pd.Timedelta(days=60)).strftime('%Y-%m-%d')
    request_end = (pd.Timestamp(end_date) + pd.Timedelta(days=5)).strftime('%Y-%m-%d')
    ks = _v4967_index_frame('KS11','1001','KOSPI',request_start,request_end)
    kq = _v4967_index_frame('KQ11','2001','KOSDAQ',request_start,request_end)
    cols = ['Date','market_ret_1d','market_ret_5d','market_ret_20d','market_m5_dist','market_ma20_dist','market_m5_slope5','market_m5_state_calc',
            'market_ret_1d_t1','market_ret_5d_t1','market_ret_20d_t1','market_m5_dist_t1','market_ma20_dist_t1','market_m5_slope5_t1','market_m5_state_calc_t1']
    def _lookup(frame: pd.DataFrame, dates: pd.Series, prefix: str) -> pd.DataFrame:
        if frame.empty:
            return pd.DataFrame(index=dates.index)
        z = frame[[c for c in cols if c in frame.columns]].copy().rename(columns={'Date':'_date'})
        base = pd.DataFrame({'_date':dates}, index=dates.index).reset_index().rename(columns={'index':'_row'})
        q = base.merge(z,on='_date',how='left').set_index('_row').reindex(dates.index)
        return q
    qks = _lookup(ks,x['_date'],'KS'); qkq = _lookup(kq,x['_date'],'KQ')
    use_kq = x['benchmark_name'].eq('KOSDAQ')
    for c in [c for c in cols if c != 'Date']:
        a = qks[c] if c in qks else pd.Series(np.nan,index=x.index)
        b = qkq[c] if c in qkq else pd.Series(np.nan,index=x.index)
        x[c] = a.where(~use_kq, b)
    x['market_context_source'] = np.where(x['market_ret_1d'].notna(), 'BENCHMARK_INDEX', 'MISSING')
    x['market_m5'] = x.get('market_m5_state_calc', pd.Series('M5-확인필요',index=x.index)).fillna('M5-확인필요')
    x['market_m5_t1'] = x.get('market_m5_state_calc_t1', pd.Series('M5-확인필요',index=x.index)).fillna('M5-확인필요')
    for n in (1,5,20):
        x[f'stock_excess_{n}d'] = _v4967_num_series(x,f'entry_stock_ret_{n}d') - _v4967_num_series(x,f'market_ret_{n}d')
    x['market_down_context'] = (
        x['market_m5_t1'].eq('M5-비우호') |
        _v4967_num_series(x,'market_ret_1d').lt(0) |
        _v4967_num_series(x,'market_ret_5d').lt(0)
    ).astype(int)

    # Candidate-sector peer breadth proxy. It is same-day and causal, but it is not a full-sector universe.
    base = canonical.copy()
    base['code'] = base.get('code',pd.Series('',index=base.index)).fillna('').astype(str).str.zfill(6)
    base['signal_date'] = base.get('signal_date',pd.Series('',index=base.index)).fillna('').astype(str)
    bsec = base['code'].map(sector_map).fillna('')
    psec = _v4967_first_text(base,['sector_name','sector','industry','theme_name','theme'])
    base['sector_label'] = bsec.where(bsec.ne(''),psec)
    base['_ret1'] = _v4967_num_series(base,'entry_stock_ret_1d')
    base['_amount'] = _v4967_num_series(base,'entry_amount_b',0).fillna(0)
    base = base.sort_values(['signal_date','code']).drop_duplicates(['signal_date','code'],keep='first')
    good = base[base.sector_label.ne('') & base._ret1.notna()].copy()
    if not good.empty:
        g = good.groupby(['signal_date','sector_label'],dropna=False).agg(
            _sector_n=('code','size'), _sector_sum_ret=('_ret1','sum'), _sector_pos=('_ret1',lambda z:int((z>0).sum())), _sector_amount=('_amount','sum')
        ).reset_index()
        x = x.merge(g,on=['signal_date','sector_label'],how='left')
        n = pd.to_numeric(x.get('_sector_n'),errors='coerce')
        r = _v4967_num_series(x,'entry_stock_ret_1d')
        pos = pd.to_numeric(x.get('_sector_pos'),errors='coerce')
        x['sector_peer_count'] = (n-1).where(n>1)
        x['sector_peer_mean_ret_1d'] = ((pd.to_numeric(x.get('_sector_sum_ret'),errors='coerce')-r)/(n-1)).where(n>1)
        x['sector_peer_positive_pct'] = ((pos-r.gt(0).astype(int))/(n-1)*100.0).where(n>1)
        x['sector_peer_amount_b'] = (pd.to_numeric(x.get('_sector_amount'),errors='coerce')-_v4967_num_series(x,'entry_amount_b',0).fillna(0)).where(n>1)
        x.drop(columns=['_sector_n','_sector_sum_ret','_sector_pos','_sector_amount'],errors='ignore',inplace=True)
    else:
        x['sector_peer_count']=np.nan; x['sector_peer_mean_ret_1d']=np.nan; x['sector_peer_positive_pct']=np.nan; x['sector_peer_amount_b']=np.nan
    x['sector_context_source'] = np.where(x['sector_peer_count'].notna(), 'SIGNAL_CANDIDATE_PROXY', 'MISSING')
    x['event_theme_bucket'] = x.apply(_v4967_event_bucket_row,axis=1)
    diag = {
        'market_rows': int(x.market_context_source.eq('BENCHMARK_INDEX').sum()),
        'market_coverage_pct': float(x.market_context_source.eq('BENCHMARK_INDEX').mean()*100.0) if len(x) else 0.0,
        'sector_rows': int(x.sector_context_source.eq('SIGNAL_CANDIDATE_PROXY').sum()),
        'sector_coverage_pct': float(x.sector_context_source.eq('SIGNAL_CANDIDATE_PROXY').mean()*100.0) if len(x) else 0.0,
        'event_mapped_rows': int(x.event_theme_bucket.ne('OTHER_OR_UNKNOWN').sum()),
        'ks_rows': len(ks), 'kq_rows': len(kq),
    }
    return x, diag


def _v4967_classify_outcomes(x: pd.DataFrame) -> pd.DataFrame:
    z=x.copy()
    z['_gross']=_v4967_num_series(z,'rule35_pnl')
    z['_net20']=z['_gross']-.20; z['_net50']=z['_gross']-.50
    p3=_v4967_num_series(z,'path_first_plus3_day',0).fillna(0)
    p10=_v4967_num_series(z,'path_first_plus10_day',0).fillna(0)
    stopd=_v4967_num_series(z,'path_first_stop_day',0).fillna(0)
    stop=_v4967_num_series(z,'stop_before_3',0).fillna(0).gt(0) | _v4967_num_series(z,'rule35_stop',0).fillna(0).gt(0) | stopd.gt(0)
    hit3=_v4967_num_series(z,'hit3_before_stop',0).fillna(0).gt(0) | _v4967_num_series(z,'rule35_hit3',0).fillna(0).gt(0) | p3.gt(0)
    big=(p10>0)&((stopd<=0)|(p10<stopd))
    pre3=_v4967_num_series(z,'path_pre_plus3_min_low_ret')
    conditions=[big, hit3 & pre3.le(-1.0) & pre3.gt(-3.0), hit3 & p3.gt(0) & p3.le(3), hit3, stop, z['_net50']>0]
    labels=['BIG_CAPTURABLE','SHAKEOUT_WIN','QUICK_WIN','SLOW_WIN','STOP_FIRST','POLICY_WIN']
    z['outcome_class']=np.select(conditions,labels,default='WEAK_OR_LOSS')
    z['net50_group']=np.where(z['_net50']>0,'WIN_NET50','LOSS_NET50')
    z['big_before_stop']=big.astype(int); z['stop_first_flag']=stop.astype(int); z['plus3_before_stop_flag']=hit3.astype(int)
    z['down_market_independent_win']=((z.get('market_down_context',0)==1)&(z['_net50']>0)&(_v4967_num_series(z,'stock_excess_1d')>0)).astype(int)
    return z


def _v4967_anatomy_tables(x: pd.DataFrame, out: Path, start_date: str, end_date: str) -> tuple[pd.DataFrame,pd.DataFrame,pd.DataFrame,pd.DataFrame,pd.DataFrame]:
    split=_split_date(start_date,end_date)
    z=x.copy(); z['_date']=pd.to_datetime(z.get('signal_date'),errors='coerce')
    z['sample_scope']=np.where(z['_date']>=split,'OOS','TRAIN')
    modes=['ALL']+list(s.CLOSING_BET_V4958_PRIMARY_PRIORITY)
    outcome_rows=[]; feature_rows=[]
    for scope in ('ALL','TRAIN','OOS'):
        q0=z if scope=='ALL' else z[z.sample_scope.eq(scope)]
        for mode in modes:
            q=q0 if mode=='ALL' else q0[q0.get('mode',pd.Series('',index=q0.index)).astype(str).eq(mode)]
            if q.empty: continue
            for oc,g in q.groupby('outcome_class'):
                outcome_rows.append({'scope':scope,'strategy':mode,'outcome_class':oc,'n':len(g),'share_pct':len(g)/len(q)*100.0,'net20_mean_pct':g['_net20'].mean(),'net50_mean_pct':g['_net50'].mean(),'max_high_mean_pct':_v4967_num_series(g,'path_max_high_ret').mean(),'mae_mean_pct':_v4967_num_series(g,'path_min_low_ret').mean()})
            win=q[q.net50_group.eq('WIN_NET50')]; loss=q[q.net50_group.eq('LOSS_NET50')]
            for feat,(label,causal) in V4967_ENTRY_FEATURE_META.items():
                a=_v4967_num_series(win,feat).dropna(); b=_v4967_num_series(loss,feat).dropna()
                feature_rows.append({'scope':scope,'strategy':mode,'feature':feat,'feature_label':label,'causal_scope':causal,'n_win':len(a),'n_loss':len(b),'winner_median':a.median() if len(a) else np.nan,'loser_median':b.median() if len(b) else np.nan,'median_delta':(a.median()-b.median()) if len(a) and len(b) else np.nan,'winner_mean':a.mean() if len(a) else np.nan,'loser_mean':b.mean() if len(b) else np.nan})
    outcomes=pd.DataFrame(outcome_rows); features=pd.DataFrame(feature_rows)

    # Train-derived, frozen univariate thresholds. No future/outcome column is permitted as an input feature.
    for feat in V4967_ENTRY_FEATURE_META:
        low=feat.lower()
        if any(tok in low for tok in V4967_FORBIDDEN_FEATURE_TOKENS):
            raise RuntimeError(f'LEAKAGE_GUARD_FORBIDDEN_FEATURE:{feat}')
    lift_rows=[]
    train=z[z.sample_scope.eq('TRAIN')]; oos=z[z.sample_scope.eq('OOS')]
    for mode in modes:
        tr=train if mode=='ALL' else train[train.get('mode',pd.Series('',index=train.index)).astype(str).eq(mode)]
        oo=oos if mode=='ALL' else oos[oos.get('mode',pd.Series('',index=oos.index)).astype(str).eq(mode)]
        if len(tr)<40 or len(oo)<20: continue
        base=float(oo['_net50'].mean()); base_stop=float(oo.stop_first_flag.mean()*100); base_big=float(oo.big_before_stop.mean()*100)
        for feat,(label,causal) in V4967_ENTRY_FEATURE_META.items():
            t=_v4967_num_series(tr,feat); o=_v4967_num_series(oo,feat)
            valid=t.notna(); win=tr.net50_group.eq('WIN_NET50')&valid; loss=tr.net50_group.eq('LOSS_NET50')&valid
            if valid.sum()<40 or win.sum()<15 or loss.sum()<15 or o.notna().sum()<20: continue
            wm=float(t[win].median()); lm=float(t[loss].median()); threshold=float(t[valid].median())
            direction='HIGH_GOOD' if wm>=lm else 'LOW_GOOD'
            mask=(o>=threshold) if direction=='HIGH_GOOD' else (o<=threshold)
            fq=oo[mask.fillna(False)]
            if len(fq)<10: continue
            sorted_net=fq['_net50'].sort_values(ascending=False)
            stress1=float(sorted_net.iloc[1:].mean()) if len(sorted_net)>1 else np.nan
            stress3=float(sorted_net.iloc[3:].mean()) if len(sorted_net)>3 else np.nan
            stress10=float(sorted_net.iloc[10:].mean()) if len(sorted_net)>10 else np.nan
            fmean=float(fq['_net50'].mean()); lift=fmean-base
            # Stability is evaluated after the TRAIN-derived threshold is frozen. The OOS halves do not retune it.
            odates=oo['_date'].dropna().sort_values()
            half_date=odates.iloc[len(odates)//2] if len(odates) else pd.NaT
            early_oo=oo[oo['_date']<=half_date] if pd.notna(half_date) else oo.iloc[0:0]
            late_oo=oo[oo['_date']>half_date] if pd.notna(half_date) else oo.iloc[0:0]
            early_fq=fq[fq['_date']<=half_date] if pd.notna(half_date) else fq.iloc[0:0]
            late_fq=fq[fq['_date']>half_date] if pd.notna(half_date) else fq.iloc[0:0]
            early_base=float(early_oo['_net50'].mean()) if len(early_oo) else np.nan
            late_base=float(late_oo['_net50'].mean()) if len(late_oo) else np.nan
            early_net=float(early_fq['_net50'].mean()) if len(early_fq) else np.nan
            late_net=float(late_fq['_net50'].mean()) if len(late_fq) else np.nan
            early_lift=early_net-early_base if pd.notna(early_net) and pd.notna(early_base) else np.nan
            late_lift=late_net-late_base if pd.notna(late_net) and pd.notna(late_base) else np.nan
            stable_halves=(len(early_fq)>=10 and len(late_fq)>=10 and early_net>0 and late_net>0 and early_lift>0 and late_lift>0)
            if causal!='AUDIT_PROXY' and len(fq)>=V4967_FEATURE_MIN_OOS_N and fmean>0 and lift>=V4967_FEATURE_MIN_LIFT_PCTP and pd.notna(stress3) and stress3>0 and stable_halves:
                status='TWO_STAGE_DISCOVERY_ONLY'
            elif fmean>base and len(fq)>=20:
                status='RESEARCH_ONLY'
            else:
                status='REJECT'
            lift_rows.append({'strategy':mode,'feature':feat,'feature_label':label,'causal_scope':causal,'direction':direction,'train_threshold':threshold,'train_winner_median':wm,'train_loser_median':lm,'train_n':int(valid.sum()),'oos_base_n':len(oo),'oos_base_net50':base,'oos_filtered_n':len(fq),'oos_filtered_net50':fmean,'oos_lift_pctp':lift,'oos_half_split_date':str(pd.Timestamp(half_date).date()) if pd.notna(half_date) else '', 'oos_early_n':len(early_fq),'oos_early_net50':early_net,'oos_early_lift_pctp':early_lift,'oos_late_n':len(late_fq),'oos_late_net50':late_net,'oos_late_lift_pctp':late_lift,'oos_halves_stable':int(stable_halves),'oos_stop_rate_pct':float(fq.stop_first_flag.mean()*100),'oos_stop_rate_delta_pctp':float(fq.stop_first_flag.mean()*100-base_stop),'oos_big_rate_pct':float(fq.big_before_stop.mean()*100),'oos_big_rate_delta_pctp':float(fq.big_before_stop.mean()*100-base_big),'top1_removed_net50':stress1,'top3_removed_net50':stress3,'top10_removed_net50':stress10,'status':status,'auto_apply':0})
    lifts=pd.DataFrame(lift_rows)

    market_rows=[]
    for mode in modes:
        q=z if mode=='ALL' else z[z.get('mode',pd.Series('',index=z.index)).astype(str).eq(mode)]
        for scope in ('TRAIN','OOS'):
            qq=q[q.sample_scope.eq(scope)]
            for lab,g in qq.groupby(qq.get('market_m5_t1',pd.Series('M5-확인필요',index=qq.index)).fillna('M5-확인필요')):
                market_rows.append({'strategy':mode,'scope':scope,'market_bucket':lab,'n':len(g),'net20_mean_pct':g['_net20'].mean(),'net50_mean_pct':g['_net50'].mean(),'win50_pct':g.net50_group.eq('WIN_NET50').mean()*100,'plus3_first_pct':g.plus3_before_stop_flag.mean()*100,'stop_first_pct':g.stop_first_flag.mean()*100,'big_pct':g.big_before_stop.mean()*100})
    market=pd.DataFrame(market_rows)

    event_rows=[]
    for (mode,bucket),g in z.groupby([z.get('mode',pd.Series('',index=z.index)).astype(str),z.event_theme_bucket]):
        oo=g[g.sample_scope.eq('OOS')]
        event_rows.append({'strategy':mode,'event_theme_bucket':bucket,'n':len(g),'oos_n':len(oo),'net50_mean_pct':g['_net50'].mean(),'oos_net50_mean_pct':oo['_net50'].mean() if len(oo) else np.nan,'down_market_winners':int(g.down_market_independent_win.sum()),'causality_claimed':0})
    events=pd.DataFrame(event_rows)

    down=z[(z.market_down_context==1)].copy()
    down=down.sort_values(['down_market_independent_win','_net50','path_max_high_ret'],ascending=[False,False,False])
    cols=[c for c in ['signal_date','code','name','mode','outcome_class','_net50','rule35_pnl','path_max_high_ret','path_min_low_ret','market_m5_t1','market_ret_1d','market_ret_5d','entry_stock_ret_1d','entry_stock_ret_5d','stock_excess_1d','stock_excess_5d','sector_label','sector_peer_count','sector_peer_mean_ret_1d','event_theme_bucket','amount_b','entry_amount_b','entry_close_loc_pct','entry_upper_wick_pct','down_market_independent_win'] if c in down.columns]
    down=down[cols]

    z.to_csv(out/'v49_70_selected_enriched_outcomes.csv',index=False,encoding='utf-8-sig')
    outcomes.to_csv(out/'v49_70_outcome_distribution.csv',index=False,encoding='utf-8-sig')
    features.to_csv(out/'v49_70_winner_loser_feature_anatomy.csv',index=False,encoding='utf-8-sig')
    lifts.to_csv(out/'v49_70_train_oos_feature_lift.csv',index=False,encoding='utf-8-sig')
    market.to_csv(out/'v49_70_market_regime_performance.csv',index=False,encoding='utf-8-sig')
    events.to_csv(out/'v49_70_event_theme_context.csv',index=False,encoding='utf-8-sig')
    down.to_csv(out/'v49_70_down_market_cases.csv',index=False,encoding='utf-8-sig')
    return outcomes,features,lifts,market,events


def main() -> int:
    ap = argparse.ArgumentParser(description='v49.70 global merge + identity/pipeline/attribution audit')
    ap.add_argument('--input-root', default='v49_70_downloads')
    ap.add_argument('--prepare-dir', default='v49_70_prepare_output')
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
        prep_root = root
    preflight_path = next(iter(prep_root.rglob('v49_70_preflight.json')), None)
    universe_path = next(iter(prep_root.rglob('v49_70_universe.json')), None)
    if preflight_path is None or universe_path is None:
        raise RuntimeError('prepare artifacts missing')
    preflight = json.loads(preflight_path.read_text(encoding='utf-8'))
    universe = json.loads(universe_path.read_text(encoding='utf-8'))
    if preflight.get('status') != 'VALID':
        raise RuntimeError(f'preflight invalid: {preflight}')

    manifests = sorted(root.rglob('shard_*_manifest.json'))
    raws = sorted(root.rglob('shard_*_raw.csv'))
    completes = sorted(root.rglob('shard_*_complete.json'))
    funnels = sorted(root.rglob('shard_*_strategy_funnel.csv'))
    zero_files = sorted(root.rglob('shard_*_zero_mode_audit.csv'))
    exc_files = sorted(root.rglob('shard_*_predicate_exceptions.csv'))
    opportunity_files = sorted(root.rglob('shard_*_opportunity_census.csv'))
    counts = {'manifest': len(manifests), 'raw': len(raws), 'complete': len(completes), 'funnel': len(funnels), 'zero': len(zero_files), 'exceptions': len(exc_files), 'opportunity': len(opportunity_files)}
    if any(v != args.shard_count for v in counts.values()):
        raise RuntimeError(f'shard artifacts incomplete: {counts}, expected {args.shard_count}')

    manifest_docs = [json.loads(p.read_text(encoding='utf-8')) for p in manifests]
    complete_docs = [json.loads(p.read_text(encoding='utf-8')) for p in completes]
    start_dates = {str(x.get('start_date', '')) for x in manifest_docs}
    end_dates = {str(x.get('end_date', '')) for x in manifest_docs}
    if len(start_dates) != 1 or len(end_dates) != 1:
        raise RuntimeError(f'shard date consensus failed: start={start_dates}, end={end_dates}')
    args.start_date = next(iter(start_dates))
    args.end_date = next(iter(end_dates))
    expected_ids = list(range(args.shard_count))
    shard_ids = sorted(int(x.get('shard_index', -1)) for x in manifest_docs)
    if shard_ids != expected_ids:
        raise RuntimeError(f'shard ids mismatch: {shard_ids} != {expected_ids}')
    if {int(x.get('global_count', 0) or 0) for x in manifest_docs} != {int(preflight.get('universe_count', 0))}:
        raise RuntimeError('global universe count consensus failed')
    if {str(x.get('global_fingerprint', '')) for x in manifest_docs} != {str(preflight.get('universe_fingerprint', ''))}:
        raise RuntimeError('global universe fingerprint consensus failed')

    manifest_by_id = {int(x.get('shard_index', -1)): x for x in manifest_docs}
    manifest_path_by_id = {int(d.get('shard_index', -1)): p for p, d in zip(manifests, manifest_docs)}
    complete_by_id = {int(x.get('shard_index', -1)): x for x in complete_docs}

    def idmap(paths):
        return {int(p.stem.split('_')[1]): p for p in paths}

    raw_by, funnel_by, zero_by, exc_by, opportunity_by = map(idmap, (raws, funnels, zero_files, exc_files, opportunity_files))
    for sid in expected_ids:
        p = raw_by[sid]
        m = manifest_by_id[sid]
        c = complete_by_id[sid]
        d = m.get('diagnostics', {}) or {}
        if file_sha(p) != str(m.get('raw_sha256', '')) or file_sha(p) != str(c.get('raw_sha256', '')):
            raise RuntimeError(f'raw sha mismatch shard {sid}')
        op=opportunity_by[sid]
        if file_sha(op) != str(m.get('opportunity_sha256','')) or file_sha(op) != str(c.get('opportunity_sha256','')):
            raise RuntimeError(f'opportunity sha mismatch shard {sid}')
        if str(m.get('engine_status', '')).upper() != 'VALID':
            raise RuntimeError(f'shard engine invalid {sid}')
        if str(m.get('control_flow', '')) != 'V49_70_SHARD_EXPORT_RETURN_MAIN_EXIT':
            raise RuntimeError(f'shard control flow invalid {sid}')
        if str(m.get('research_lane', '')) != 'SKIPPED' or str(m.get('telegram', '')) != 'SKIPPED' or str(m.get('post_export_network', '')) != 'FORBIDDEN':
            raise RuntimeError(f'shard isolation invalid {sid}')
        if str(c.get('status', '')) != 'SHARD_ONLY_COMPLETE' or file_sha(manifest_path_by_id[sid]) != str(c.get('manifest_sha256', '')):
            raise RuntimeError(f'shard completion invalid {sid}')
        for key, path in [('funnel_sha256', funnel_by[sid]), ('zero_audit_sha256', zero_by[sid]), ('exceptions_sha256', exc_by[sid])]:
            if file_sha(path) != str(d.get(key, '')):
                raise RuntimeError(f'diagnostic sha mismatch shard={sid} key={key}')

    raw_input = _read_csvs(raws, dtype={'code': str})
    funnel_raw = _read_csvs(funnels)
    zero_df = _read_csvs(zero_files, dtype={'code': str})
    exc_df = _read_csvs(exc_files, dtype={'code': str})
    opportunity_df = _read_csvs(opportunity_files, dtype={'code': str})
    if raw_input.empty:
        raise RuntimeError('all shard raw signals empty')
    raw_df, identity_audit = _normalize_identity(raw_input)
    if raw_df.empty:
        raise RuntimeError('all raw rows failed identity validation')
    defaults = {'grade': '', 'score': 0.0, 'amount_b': 0.0, 'vol_ratio': 0.0, 'rule35_pnl': float('nan'), 'ret_next_close': float('nan'), 'hit3_before_stop': 0, 'band_type': ''}
    for col, val in defaults.items():
        if col not in raw_df.columns:
            raw_df[col] = val

    canonical = s._v4940_canonical_df(raw_df)
    populations = _attribution_populations(canonical, max(1, args.top_per_strategy))
    selected = populations['PREDICATE_MODE']
    if selected.empty:
        raise RuntimeError('global selected population empty')

    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    raw_path = out / 'v49_70_global_raw.csv'
    canonical_path = out / 'v49_70_global_canonical.csv'
    selected_path = out / 'v49_70_global_selected_top5.csv'
    raw_df.to_csv(raw_path, index=False, encoding='utf-8-sig')
    canonical.to_csv(canonical_path, index=False, encoding='utf-8-sig')
    selected.to_csv(selected_path, index=False, encoding='utf-8-sig')
    populations['PRIMARY'].to_csv(out / 'v49_70_primary_selected_top5.csv', index=False, encoding='utf-8-sig')
    populations['ALL_MATCHED'].to_csv(out / 'v49_70_all_matched_selected_top5.csv', index=False, encoding='utf-8-sig')

    metrics = [
        'eligible_dates', 'gate_admitted', 'predicate_called', 'predicate_hit_dates', 'predicate_hit_records',
        'eval_success', 'eval_fail', 'raw_emitted', 'payload_missing_signal_date_before',
        'payload_signal_date_injected', 'payload_mode_mismatch', 'exceptions', 'zero_audit_tested', 'zero_audit_hits',
    ]
    for col in metrics:
        if col not in funnel_raw:
            funnel_raw[col] = 0
        funnel_raw[col] = pd.to_numeric(funnel_raw[col], errors='coerce').fillna(0).astype(int)
    funnel = funnel_raw.groupby('strategy', as_index=False)[metrics].sum()

    raw_counts = raw_df['mode'].astype(str).value_counts()
    canonical_counts = canonical.get('mode', pd.Series(dtype=str)).astype(str).value_counts()
    selected_counts = selected.get('mode', pd.Series(dtype=str)).astype(str).value_counts()
    identity_by = identity_audit.set_index('strategy').to_dict('index') if not identity_audit.empty else {}
    identity_global_missing = int(pd.to_numeric(identity_audit.get('missing_signal_date', pd.Series(dtype=float)), errors='coerce').fillna(0).sum()) if not identity_audit.empty else 0
    identity_global_invalid_mode = int(pd.to_numeric(identity_audit.get('invalid_mode', pd.Series(dtype=float)), errors='coerce').fillna(0).sum()) if not identity_audit.empty else 0
    zero_file_hits = (
        pd.to_numeric(zero_df.get('full_predicate_hit', pd.Series(0, index=zero_df.index)), errors='coerce')
        .fillna(0)
        .groupby(zero_df.get('strategy', pd.Series('', index=zero_df.index)).astype(str))
        .sum()
        .to_dict()
        if not zero_df.empty else {}
    )
    exc_file_counts = exc_df.get('strategy', pd.Series('', index=exc_df.index)).astype(str).value_counts().to_dict() if not exc_df.empty else {}
    min_audit = int(getattr(s, 'CLOSING_BET_V4964_ZERO_AUDIT_MIN_GLOBAL', 100))
    status_rows = []
    for mode in s.CLOSING_BET_V4958_PRIMARY_PRIORITY:
        rr = funnel[funnel.strategy.eq(mode)]
        vals = {k: int(rr.iloc[0][k]) if len(rr) else 0 for k in metrics}
        raw_n = int(raw_counts.get(mode, 0))
        canonical_n = int(canonical_counts.get(mode, 0))
        selected_n = int(selected_counts.get(mode, 0))
        ident = identity_by.get(mode, {})
        missing_date = int(ident.get('missing_signal_date', 0) or 0)
        invalid_mode = int(ident.get('invalid_mode', 0) or 0)
        funnel_zero = int(vals.get('zero_audit_hits', 0))
        file_zero = int(zero_file_hits.get(mode, 0) or 0)
        funnel_exc = int(vals.get('exceptions', 0))
        file_exc = int(exc_file_counts.get(mode, 0) or 0)
        vals['zero_audit_hits'] = max(funnel_zero, file_zero)
        vals['exceptions'] = max(funnel_exc, file_exc)
        diag_ok = int(funnel_zero == file_zero and funnel_exc == file_exc)
        hit_account_ok = int(vals['predicate_hit_records'] == vals['eval_success'] + vals['eval_fail'])
        raw_account_ok = int(vals['eval_success'] == vals['raw_emitted'] == raw_n)
        stage_order_ok = int(raw_n >= canonical_n >= selected_n)
        pipeline_ok = int(diag_ok and hit_account_ok and raw_account_ok and stage_order_ok and missing_date == 0 and invalid_mode == 0 and vals['payload_mode_mismatch'] == 0)
        gate_hit_pct = float(vals['predicate_hit_dates'] / vals['gate_admitted'] * 100.0) if vals['gate_admitted'] else np.nan

        if vals['zero_audit_hits'] > 0:
            status, reason = 'INVALID-FALSE-NEGATIVE', 'vector gate rejected an authoritative predicate hit'
        elif vals['exceptions'] > 0:
            status, reason = 'INVALID-PREDICATE-EXCEPTION', 'predicate exception rows present'
        elif missing_date > 0 or invalid_mode > 0:
            status, reason = 'INVALID-IDENTITY', f'missing date {missing_date}, invalid mode {invalid_mode}'
        elif not diag_ok:
            status, reason = 'INVALID-DIAGNOSTIC-MISMATCH', f'funnel/file mismatch zero {funnel_zero}/{file_zero}, exceptions {funnel_exc}/{file_exc}'
        elif not hit_account_ok or not raw_account_ok:
            status, reason = 'INVALID-RAW-ACCOUNTING', f'hit {vals["predicate_hit_records"]}, eval {vals["eval_success"]}+{vals["eval_fail"]}, emitted {vals["raw_emitted"]}, raw {raw_n}'
        elif raw_n > 0 and canonical_n == 0:
            status, reason = 'INVALID-CANONICAL-DROP', 'raw rows existed but canonical population became zero'
        elif canonical_n > 0 and selected_n == 0:
            status, reason = 'INVALID-TOP-DROP', 'canonical rows existed but date×strategy TOP selection became zero'
        elif selected_n > 0:
            status, reason = 'VALID', 'predicate identity and all pipeline stages reconciled'
        elif vals['predicate_hit_records'] == 0:
            if vals['predicate_called'] <= 0:
                status, reason = 'ZERO-HIT-UNEXPLAINED', 'predicate was never called'
            elif vals['zero_audit_tested'] >= min_audit or vals['gate_admitted'] >= vals['eligible_dates']:
                status, reason = 'ZERO-HIT-EXPLAINED', 'predicate returned no hit and rejected-date audit found no hit'
            else:
                status, reason = 'ZERO-HIT-UNEXPLAINED', f'zero audit {vals["zero_audit_tested"]} < required {min_audit}'
        else:
            status, reason = 'INVALID-PIPELINE-DROP', 'predicate hit existed but no selected population remained'

        status_rows.append({
            'strategy': mode,
            'raw_n': raw_n,
            'canonical_n': canonical_n,
            'selected_n': selected_n,
            'raw_missing_signal_date': missing_date,
            'raw_invalid_mode': invalid_mode,
            'diagnostic_consistent': diag_ok,
            'hit_account_ok': hit_account_ok,
            'raw_account_ok': raw_account_ok,
            'stage_order_ok': stage_order_ok,
            'pipeline_ok': pipeline_ok,
            'gate_hit_pct': gate_hit_pct,
            **vals,
            'status': status,
            'reason': reason,
        })
    strategy_status = pd.DataFrame(status_rows)
    invalid = bool(strategy_status.status.str.startswith('INVALID').any())
    partial = bool(strategy_status.status.eq('ZERO-HIT-UNEXPLAINED').any())
    funnel_status = 'INVALID' if invalid else ('PARTIAL-VALID' if partial else 'FULL-VALID')

    if zero_df.empty:
        zero_df = pd.DataFrame(columns=['strategy', 'code', 'name', 'signal_date', 'index', 'gate_admitted', 'full_predicate_hit', 'hit_records', 'error', 'status'])
    if exc_df.empty:
        exc_df = pd.DataFrame(columns=['strategy', 'code', 'name', 'signal_date', 'exception_type', 'exception_message', 'context'])
    funnel.to_csv(out / 'v49_70_strategy_funnel.csv', index=False, encoding='utf-8-sig')
    strategy_status.to_csv(out / 'v49_70_strategy_stage_audit.csv', index=False, encoding='utf-8-sig')
    identity_audit.to_csv(out / 'v49_70_identity_audit.csv', index=False, encoding='utf-8-sig')
    zero_df.to_csv(out / 'v49_70_zero_mode_audit.csv', index=False, encoding='utf-8-sig')
    exc_df.to_csv(out / 'v49_70_predicate_exceptions.csv', index=False, encoding='utf-8-sig')

    s.INDEX_MAP = {str(k).zfill(6): str(v) for k, v in dict(universe.get('index_map') or {}).items()}
    s.MARCAP_MAP = {str(k).zfill(6): float(v or 0) for k, v in dict(universe.get('marcap_map') or {}).items()}
    s._V4959_STRATEGY_ENGINE_AUDIT = {'status': 'VALID', 'detail': {'shard_count': args.shard_count, 'global_fingerprint': preflight.get('universe_fingerprint')}}
    perf = s._v4959_build_common_performance_audit(raw_df, selected, args.start_date, args.end_date, {'source_codes': preflight.get('universe_count', 0)})
    perf_status = str(perf.get('status', 'INVALID'))
    generated_root = Path('reports')
    if out.resolve() != generated_root.resolve():
        import shutil
        for gp in generated_root.glob('v49_70_common_strategy_*'):
            shutil.copy2(gp, out / gp.name)
    performance_csv = out / 'v49_70_common_strategy_performance.csv'
    portfolio_csv = out / 'v49_70_common_strategy_portfolio.csv'
    ptab = pd.read_csv(performance_csv) if performance_csv.exists() else pd.DataFrame()
    port = pd.read_csv(portfolio_csv) if portfolio_csv.exists() else pd.DataFrame()
    allrow = ptab[ptab.strategy.eq('ALL')].iloc[0].to_dict() if not ptab.empty and ptab.strategy.eq('ALL').any() else {}
    p1_50q = port[(port.strategy == 'ALL') & (port.day_limit == 1) & (port.cost_bps == 50)] if not port.empty else pd.DataFrame()
    p1_20q = port[(port.strategy == 'ALL') & (port.day_limit == 1) & (port.cost_bps == 20)] if not port.empty else pd.DataFrame()
    p1_50 = p1_50q.iloc[0].to_dict() if len(p1_50q) else {}
    p1_20 = p1_20q.iloc[0].to_dict() if len(p1_20q) else {}

    attribution = _performance_table(populations, args.start_date, args.end_date)
    attribution.to_csv(out / 'v49_70_strategy_attribution_performance.csv', index=False, encoding='utf-8-sig')
    lpwf = _lp_validation(selected, out, args.start_date, args.end_date)
    iit = _iit_split(selected, out, args.start_date, args.end_date)

    selected_ctx, anatomy_diag = _v4967_attach_context(selected, canonical, universe, args.start_date, args.end_date)
    selected_ctx = _v4967_classify_outcomes(selected_ctx)
    outcomes, anatomy_features, feature_lifts, market_perf, event_perf = _v4967_anatomy_tables(selected_ctx, out, args.start_date, args.end_date)
    anatomy_status = 'FULL-VALID' if anatomy_diag.get('market_coverage_pct', 0) >= V4967_MARKET_MIN_COVERAGE_PCT and not outcomes.empty else ('PARTIAL-VALID' if not outcomes.empty else 'INVALID')
    sector_context_status = _v4970_context_status(anatomy_diag.get('sector_coverage_pct',0.0), kind='SECTOR')
    event_context_status = _v4970_context_status(0.0, anatomy_diag.get('event_mapped_rows',0), kind='EVENT')
    context_guard_status = 'INVALID' if (V4970_REQUIRE_SECTOR_CONTEXT and sector_context_status != 'VALID') else ('DEGRADED' if sector_context_status != 'VALID' or event_context_status != 'VALID' else 'VALID')
    feature_candidates = feature_lifts[feature_lifts.status.eq('TWO_STAGE_DISCOVERY_ONLY')].copy() if not feature_lifts.empty else pd.DataFrame()
    down_cases_path = out / 'v49_70_down_market_cases.csv'
    down_cases = pd.read_csv(down_cases_path, dtype={'code':str}) if down_cases_path.exists() else pd.DataFrame()

    # v49.70: TRAIN -> VALIDATION -> LOCKED_TEST research. Thresholds are discovered on
    # TRAIN, a single model/policy is selected on VALIDATION, and LOCKED_TEST is read once
    # for final acceptance. Nothing is auto-applied to LIVE/search conditions.
    triple = r70.run_research(selected_ctx, out, args.start_date, args.end_date, V4967_ENTRY_FEATURE_META)
    triple_manifest = triple['manifest']
    triple_locked = triple['locked']
    down_locked = triple['down_locked']
    lp_exit_selected = triple['lp_selected']

    # v49.70 independent SEARCH QUALITY AUDIT: machine intent proxy, winner/loser casebook,
    # material missed-winner census, rank-stage attribution and distribution drift.
    quality = q70.run_quality_audit(raw_df, triple['enriched'], selected, opportunity_df, out, V4967_ENTRY_FEATURE_META)
    quality_manifest = quality['manifest']
    quality_status = str(quality_manifest.get('status','INVALID'))

    technical_status = 'INVALID' if preflight.get('status') != 'VALID' or perf_status != 'VALID' or funnel_status == 'INVALID' or quality_status == 'INVALID' or identity_global_missing > 0 or identity_global_invalid_mode > 0 or (V4970_REQUIRE_SECTOR_CONTEXT and sector_context_status != 'VALID') else ('PARTIAL-VALID' if funnel_status == 'PARTIAL-VALID' else 'FULL-VALID')
    lines1 = [
        '(1/12)', '⚙️ 공통 검색식 성과검증 | v49.70', '──────────',
        f'버전: {s.CLOSING_BET_SCANNER_VERSION}',
        f'기간: {args.start_date} ~ {args.end_date} | prepared universe {preflight.get("universe_count")} · shards {args.shard_count}',
        '[기술 검증]',
        f'- PREPARED UNIVERSE: VALID ✅ · fp {preflight.get("universe_fingerprint")}',
        f'- SHARD CONSENSUS: {len(manifests)}/{args.shard_count} VALID ✅ · global merge before TOP selection',
        f'- SEARCH CONTRACT: {"VALID ✅" if preflight.get("contract_valid") else "INVALID ⛔"} · explicit {preflight.get("explicit_hist_failures")} · boundary {preflight.get("boundary_failures")} · thread {preflight.get("thread_isolation_failures")} · determinism {preflight.get("determinism_failures")}',
        f'- STRATEGY POPULATION: {technical_status} · funnel {funnel_status} · false-negative {int(strategy_status.zero_audit_hits.sum())} · predicate exceptions {int(strategy_status.exceptions.sum())}',
        f'- IDENTITY: raw input {len(raw_input)} · valid {len(raw_df)} · missing date {identity_global_missing} · invalid mode {identity_global_invalid_mode}',
        f'- GLOBAL PIPELINE: raw {len(raw_df)} → canonical {len(canonical)} → date×strategy TOP{args.top_per_strategy} {len(selected)}',
        f'- PERFORMANCE ENGINE: {perf_status} {"✅" if perf_status == "VALID" else "⛔"}',
        f'- WIN/LOSS ANATOMY: {anatomy_status} · 진입시점 feature whitelist · TRAIN→OOS 고정',
        f'- CONTEXT GUARD: {context_guard_status} · sector {sector_context_status} {anatomy_diag.get("sector_coverage_pct",0):.1f}% · event {event_context_status} {anatomy_diag.get("event_mapped_rows",0)}건',
        f'- SEARCH QUALITY AUDIT: {quality_status} · opportunity {quality_manifest.get("opportunity_rows",0)} · deep-audit {quality_manifest.get("deep_audit_rows",0)} · technical FN {quality_manifest.get("technical_false_negative_rows",0)}', '',
        '[전체 성과]',
        f'- 거래평균 gross/net20/net50: {f(allrow.get("gross_mean_pct"))} / {f(allrow.get("net20_mean_pct"))} / {f(allrow.get("net50_mean_pct"))}',
        f'- OOS 평균 net20/net50: {f(allrow.get("oos_net20_mean_pct"))} / {f(allrow.get("oos_net50_mean_pct"))} · OOS n {int(allrow.get("oos_n", 0) or 0)}',
        f'- 하루1종목 20bp: 누적 {f(p1_20.get("total"))} · MDD {f(p1_20.get("mdd"))}',
        f'- 하루1종목 50bp: 누적 {f(p1_50.get("total"))} · MDD {f(p1_50.get("mdd"))} · 양수월 {f(p1_50.get("positive_month"))}', '',
        '[운용 잠금]', '- PAPER 유지 · 실제주문 0건', '- FULL-VALID와 50bp OOS·MDD·대박제거를 함께 통과하기 전 LIVE 자동전환 금지',
    ]
    lines2 = ['(2/12)', '📊 전략별 OOS · Predicate Mode | v49.70', '──────────']
    if not ptab.empty:
        for _, r in ptab[ptab.strategy.ne('ALL')].sort_values(['oos_net50_mean_pct', 'oos_n'], ascending=[False, False]).iterrows():
            st = strategy_status[strategy_status.strategy.eq(r['strategy'])]
            label = str(st.iloc[0].status) if len(st) else 'UNKNOWN'
            lines2.append(f'- {r["strategy"]}: {label} · n {int(r["n"])} · OOS {int(r["oos_n"])} · net20/50 {f(r["oos_net20_mean_pct"])}/{f(r["oos_net50_mean_pct"])} · 전체50 {f(r["net50_mean_pct"])}')
    lines2 += ['', '- I/IT는 REAL_OR_CACHE 성과만 승격 검토 · proxy는 별도 표본', '- Lifecycle/Runner/FAIL/BIG/Cluster는 이번 성과 전용 실행과 분리']

    lines3 = ['(3/12)', '🔬 전략 Funnel·Pipeline | v49.70', '──────────']
    for _, r in strategy_status.iterrows():
        lines3.append(
            f'- {r.strategy}: {r.status} · gate {int(r.gate_admitted)} → hit {int(r.predicate_hit_records)}({f(r.gate_hit_pct)}) → eval {int(r.eval_success)} '
            f'→ raw {int(r.raw_n)} → canon {int(r.canonical_n)} → TOP {int(r.selected_n)} · dateFix {int(r.payload_signal_date_injected)} '
            f'· zeroAudit {int(r.zero_audit_tested)}/{int(r.zero_audit_hits)} · exc {int(r.exceptions)}'
        )

    lines4 = ['(4/12)', '🧭 전략 귀속·LP·I/IT | v49.70', '──────────']
    for mode in s.CLOSING_BET_V4958_PRIMARY_PRIORITY:
        q = attribution[(attribution.strategy == mode) & (attribution.attribution.isin(['PREDICATE_MODE', 'PRIMARY', 'ALL_MATCHED']))]
        vals = {r.attribution: r for _, r in q.iterrows()}
        pm = vals.get('PREDICATE_MODE'); pr = vals.get('PRIMARY'); am = vals.get('ALL_MATCHED')
        lines4.append(
            f'- {mode}: Predicate OOS50 {f(pm.oos_net50_mean_pct) if pm is not None else "평가없음"} n{int(pm.oos_n) if pm is not None else 0} '
            f'| Primary {f(pr.oos_net50_mean_pct) if pr is not None else "평가없음"} n{int(pr.oos_n) if pr is not None else 0} '
            f'| AnyMatch {f(am.oos_net50_mean_pct) if am is not None else "평가없음"} n{int(am.oos_n) if am is not None else 0}'
        )
    if not lpwf.empty:
        valid = lpwf[lpwf.status.eq('VALID')]
        lines4 += ['', f'- LP WALK: folds {len(lpwf)} · VALID {len(valid)} · net50 양수 {int((valid.net50_mean_pct > 0).sum())}/{len(valid)}' if len(valid) else '- LP WALK: 유효 fold 없음']
    real_iit = iit[iit.flow_group.eq('REAL_OR_CACHE')]
    proxy_iit = iit[iit.flow_group.eq('PROXY')]
    lines4.append('- I/IT REAL: ' + (' · '.join(f'{r.strategy} OOS n{int(r.oos_n)} net50 {f(r.oos_net50_mean_pct)}' for _, r in real_iit.iterrows()) or '없음'))
    lines4.append('- I/IT PROXY(승격제외): ' + (' · '.join(f'{r.strategy} n{int(r.n)}' for _, r in proxy_iit.iterrows()) or '없음'))

    lines5 = ['(5/12)', '🧬 상승·하락 경로 해부 | v49.70', '──────────']
    lines5.append(f'- ANATOMY STATUS: {anatomy_status} · 시장커버리지 {anatomy_diag.get("market_coverage_pct",0):.1f}%')
    lines5.append(f'- SECTOR CONTEXT: {sector_context_status} · 신호후보 프록시 {anatomy_diag.get("sector_coverage_pct",0):.1f}% · VALID 기준 ≥{V4970_SECTOR_VALID_MIN_COVERAGE_PCT:.1f}%')
    lines5.append(f'- EVENT CONTEXT: {event_context_status} · 이벤트매핑 {anatomy_diag.get("event_mapped_rows",0)}건')
    oo_out = outcomes[outcomes.scope.eq('OOS')] if not outcomes.empty else pd.DataFrame()
    all_oo = oo_out[oo_out.strategy.eq('ALL')] if not oo_out.empty else pd.DataFrame()
    if not all_oo.empty:
        for _,r in all_oo.sort_values('n',ascending=False).iterrows():
            lines5.append(f'- 전체 OOS {r.outcome_class}: n {int(r.n)} ({r.share_pct:.1f}%) · net50 {f(r.net50_mean_pct)} · 최대 {f(r.max_high_mean_pct)} · MAE {f(r.mae_mean_pct)}')
    for mode in ['LP','SLOCK','S','L','G','A','B1','B2','C','H']:
        q=oo_out[oo_out.strategy.eq(mode)] if not oo_out.empty else pd.DataFrame()
        if q.empty: continue
        wins=int(q[q.outcome_class.isin(['BIG_CAPTURABLE','SHAKEOUT_WIN','QUICK_WIN','SLOW_WIN','POLICY_WIN'])].n.sum())
        stops=int(q[q.outcome_class.eq('STOP_FIRST')].n.sum())
        big=int(q[q.outcome_class.eq('BIG_CAPTURABLE')].n.sum())
        lines5.append(f'- {mode}: OOS {int(q.n.sum())} · 상승경로 {wins} · BIG {big} · 손절선행 {stops}')
    lines5 += ['', '- 경로분류는 진입 후 결과 해부용이며 선별특징은 반드시 TRAIN 발견→OOS 고정검증으로만 평가합니다.']

    lines6 = ['(6/12)', '🌧️ 하락장 독립상승·개선후보 | v49.70', '──────────']
    if not down_cases.empty:
        dc=down_cases.copy(); independent=pd.to_numeric(dc.get('down_market_independent_win',0),errors='coerce').fillna(0).eq(1)
        lines6.append(f'- 하락시장 사례 {len(dc)}건 · 시장대비 독립상승 {int(independent.sum())}건')
        for mode,g in dc[independent].groupby('mode'):
            lines6.append(f'- 하락장 독립상승 {mode}: {len(g)}건 · net50 평균 {f(pd.to_numeric(g.get("_net50"),errors="coerce").mean())} · 시장초과1일 {f(pd.to_numeric(g.get("stock_excess_1d"),errors="coerce").mean())}')
    else:
        lines6.append('- 하락시장 사례: 시장 데이터 부족으로 평가없음')
    if not feature_candidates.empty:
        lines6.append('[구형 2단계 탐색 · v49.70 최종 승격근거 아님]')
        for _,r in feature_candidates.sort_values(['oos_lift_pctp','oos_filtered_n'],ascending=[False,False]).head(10).iterrows():
            op='≥' if r.direction=='HIGH_GOOD' else '≤'
            lines6.append(f'- {r.strategy} · {r.feature_label} {op}{r.train_threshold:.2f}: OOS n{int(r.oos_filtered_n)} · net50 {f(r.oos_filtered_net50)} · lift {r.oos_lift_pctp:+.2f}%p · Top3제거 {f(r.top3_removed_net50)}')
    else:
        lines6.append('- 구형 2단계 탐색후보 없음')
    if not event_perf.empty:
        ev=event_perf[(event_perf.oos_n>=10)&event_perf.oos_net50_mean_pct.notna()].sort_values('oos_net50_mean_pct',ascending=False).head(6)
        if len(ev):
            lines6.append('[재료/테마 문구 컨텍스트 · 인과 주장 아님]')
            for _,r in ev.iterrows():
                lines6.append(f'- {r.strategy}/{r.event_theme_bucket}: OOS n{int(r.oos_n)} · net50 {f(r.oos_net50_mean_pct)} · 하락장승자 {int(r.down_market_winners)}')
    lines6 += ['', '- 섹터동행은 전체 업종 유니버스가 아닌 신호후보 프록시이므로 자동필터 승격 금지.', '- 위 항목은 2단계 탐색 참고자료이며 v49.70 LOCKED TEST 판정과 분리합니다.']

    lines7 = ['(7/12)', '🏁 LP 청산정책 3단계 검증 | v49.70', '──────────']
    b = triple['boundaries']
    lines7.append(f'- 분할: TRAIN ~{b["train_end"].strftime("%Y-%m-%d")} · VALIDATION {b["validation_start"].strftime("%Y-%m-%d")}~{b["validation_end"].strftime("%Y-%m-%d")} · LOCKED TEST {b["test_start"].strftime("%Y-%m-%d")}~')
    if not lp_exit_selected.empty:
        rr=lp_exit_selected.iloc[0]
        lines7.append(f'- VALIDATION 선택: {rr.selected_policy} · LOCKED TEST {rr.locked_test_status} · n{int(rr.test_n)} · net50 {f(rr.test_net50)} · 현재3/5대비 {float(rr.test_lift_vs_current_pctp):+.2f}%p · Top3제거 {f(rr.test_top3_removed_net50)} · MDD {f(rr.test_mdd_pct)}')
    else:
        lines7.append('- LP 청산정책: 평가없음')
    lp_test=triple['lp_audit'][triple['lp_audit'].stage.eq('LOCKED_TEST')] if not triple['lp_audit'].empty else pd.DataFrame()
    if not lp_test.empty:
        for _,rr in lp_test.sort_values(['mean_pct','n'],ascending=[False,False]).head(6).iterrows():
            lines7.append(f'- {rr.policy}: n{int(rr.n)} · net50 {f(rr.mean_pct)} · Top3제거 {f(rr.top3_removed_pct)} · MDD {f(rr.portfolio_mdd_pct)}')
    lines7 += ['', '- 청산후보도 PAPER 연구만 허용하며 기존 +3% 우선익절을 자동변경하지 않습니다.']

    lines8 = ['(8/12)', '🧪 L·G·S·하락장 LOCKED TEST | v49.70', '──────────']
    lines8.append(f'- 3단계 상태: {triple_manifest.get("status")} · TEST 접근 selected model당 1회 · 역사적 순수 미관측은 아님')
    if not triple_locked.empty:
        for _,rr in triple_locked.sort_values(['locked_test_status','test_net50'],ascending=[True,False]).iterrows():
            lines8.append(f'- {rr.strategy}: {rr.rule_text} · {rr.locked_test_status} · TEST n{int(rr.test_n)} · net50 {f(rr.test_net50)} · lift {float(rr.test_lift_pctp):+.2f}%p · Top3제거 {f(rr.test_top3_removed_net50)}')
    else:
        lines8.append('- L·G·S VALIDATION 선택모델 없음')
    if not down_locked.empty:
        lines8.append('[하락장 Router · 진입시점 정보만 사용]')
        for _,rr in down_locked.sort_values(['locked_test_status','test_net50'],ascending=[True,False]).head(6).iterrows():
            lines8.append(f'- {rr.strategy}: {rr.rule_text} · {rr.locked_test_status} · TEST n{int(rr.test_n)} · net50 {f(rr.test_net50)} · lift {float(rr.test_lift_pctp):+.2f}%p')
    else:
        lines8.append('- 하락장 VALIDATION 선택모델 없음')
    lines8 += ['', '- TEST 결과로 기준·조건을 재선택하지 않으며 auto_apply=0 · 검색식/LIVE/자동주문 변경 금지']

    lines9 = ['(9/12)', '🎯 검색 의도 적합성 감사 | v49.70', '──────────']
    intent=quality['intent']
    if not intent.empty:
        for mode in s.CLOSING_BET_V4958_PRIMARY_PRIORITY:
            q=intent[intent.strategy.eq(mode)]
            if q.empty: continue
            assessed=q[~q.intent_status.eq('UNASSESSED')]
            total=int(q.n.sum()); match=int(q[q.intent_status.eq('INTENT_MATCH')].n.sum()); partial_n=int(q[q.intent_status.eq('PARTIAL_MATCH')].n.sum()); mismatch=int(q[q.intent_status.eq('INTENT_MISMATCH')].n.sum()); unassessed=int(q[q.intent_status.eq('UNASSESSED')].n.sum())
            mm=q[q.intent_status.eq('INTENT_MATCH')]; mx=q[q.intent_status.eq('INTENT_MISMATCH')]
            lines9.append(f'- {mode}: total {total} · MATCH {match} · PARTIAL {partial_n} · MISMATCH {mismatch} · 미평가 {unassessed} | MATCH net50 {f(mm.net50_mean_pct.iloc[0]) if len(mm) else "평가없음"} · MISMATCH {f(mx.net50_mean_pct.iloc[0]) if len(mx) else "평가없음"}')
    else:
        lines9.append('- 의도 프록시 평가없음')
    lines9 += ['', '- INTENT는 search_spec의 사람 언어 의도를 독립 수치 프록시로 점검한 것이며 권위 검색식 자체를 대체하지 않습니다.', '- MISMATCH인데 상승한 사례와 MATCH인데 하락한 사례는 manual casebook에서 우선 검토합니다.']

    lines10 = ['(10/12)', '🕵️ 놓친 상승종목·단계별 누락 | v49.70', '──────────']
    opp=quality['opp_summary']
    if not opp.empty:
        lines10.append(f'- Material opportunity census: {quality_manifest.get("opportunity_rows",0)}건 · TOP5 미포착 {quality_manifest.get("missed_opportunity_rows",0)}건 · full-predicate deep audit {quality_manifest.get("deep_audit_rows",0)}건')
        for _,r in opp.sort_values('n',ascending=False).iterrows():
            lines10.append(f'- {r.miss_stage}: n {int(r.n)} ({float(r.share_pct):.1f}%) · 익일종가 {f(r.next_close_mean_pct)} · 3일최대 {f(r.max3_mean_pct)} · 5일최대 {f(r.max5_mean_pct)}')
    else:
        lines10.append('- Opportunity census 평가없음')
    path_summary = quality.get('path_summary', pd.DataFrame())
    if path_summary is not None and not path_summary.empty:
        lines10.append('[상승기회 경로 구분 · 사후 해부]')
        agg=(path_summary.groupby('opportunity_path_class',dropna=False)['n'].sum().sort_values(ascending=False))
        for cls,n0 in agg.items():
            lines10.append(f'- {cls}: {int(n0)}건')
    lines10 += ['', f'- 기술적 false negative: {quality_manifest.get("technical_false_negative_rows",0)}건 · 1건이라도 있으면 SEARCH QUALITY INVALID', '- 미감사 Gate 누락은 VECTOR_GATE_REJECTED_UNVERIFIED로 표시하며 전략적 누락으로 단정하지 않습니다.']

    lines11 = ['(11/12)', '📐 랭킹·분포변화·상시감사 | v49.70', '──────────']
    rank=quality['rank_summary']
    if not rank.empty:
        for mode in ['LP','L','G','S','SLOCK']:
            q=rank[rank.strategy.eq(mode)]
            if q.empty: continue
            vals={r.rank_bucket:r for _,r in q.iterrows()}
            r1=vals.get('RANK1'); r25=vals.get('RANK2_5'); out5=vals.get('OUTSIDE_TOP5')
            lines11.append(f'- {mode}: 1위 {f(r1.net50_mean_pct) if r1 is not None else "평가없음"} n{int(r1.n) if r1 is not None else 0} | 2~5위 {f(r25.net50_mean_pct) if r25 is not None else "평가없음"} n{int(r25.n) if r25 is not None else 0} | TOP5밖 {f(out5.net50_mean_pct) if out5 is not None else "평가없음"} n{int(out5.n) if out5 is not None else 0}')
    drift=quality['drift']
    if not drift.empty:
        alerts=drift[drift.status.eq('DRIFT_ALERT')].sort_values('standardized_median_shift',key=lambda x:x.abs(),ascending=False).head(6)
        lines11.append(f'- 최근분포 DRIFT_ALERT {int(drift.status.eq("DRIFT_ALERT").sum())} · WATCH {int(drift.status.eq("WATCH").sum())}')
        for _,r in alerts.iterrows(): lines11.append(f'  · {r.strategy}/{r.feature}: shift {float(r.standardized_median_shift):+.2f}σ · recent n{int(r.recent_n)}')
    else:
        lines11.append('- 분포변화 평가 표본 부족')
    lines11 += ['', '- 검색식 변경은 하지 않으며 사례→반복성→가설등록→별도 검증→미래 PAPER 순서를 유지합니다.', '- auto_apply=0 · PAPER 유지 · 실제주문 0건']

    lines12 = ['(12/12)', '🚨 기술적 누락 포렌식 | v49.70', '──────────']
    tech_fn = quality.get('technical_fn', pd.DataFrame())
    lines12.append('- A Gate: SAFE SUPERSET · 공통자격 통과 시 A 권위식을 항상 호출 · 6조건/4점 판정은 권위식 단일 소스')
    if tech_fn is None or tech_fn.empty:
        lines12.append('- TECHNICAL_FALSE_NEGATIVE 0건 ✅')
        lines12.append('- 전용 CSV/JSON은 0건이어도 생성되며 별도 artifact로 업로드됩니다.')
    else:
        lines12.append(f'- TECHNICAL_FALSE_NEGATIVE {len(tech_fn)}건 ⛔ · SEARCH QUALITY INVALID')
        for _, r in tech_fn.head(20).iterrows():
            lines12.append(
                f'- {str(r.get("signal_date", ""))[:10]} {r.get("name", "")}({str(r.get("code", "")).zfill(6)}) '
                f'| 원인 {r.get("technical_fn_root_cause", "UNRESOLVED")} | Gate누락 {r.get("gate_missing_modes", "-") or "-"} '
                f'| 계획 {r.get("planned_modes", "-") or "-"} | 재검사 {r.get("deep_audit_hit_modes", "-") or "-"} '
                f'| A-safe {int(float(r.get("a_safe_superset_expected",0) or 0))} / A-trace {int(float(r.get("a_trace_score",0) or 0))} / A-auth {int(float(r.get("a_authority_replay_score",0) or 0))} '
                f'| 경로 {r.get("opportunity_path_class", "-")} | D1 {f(r.get("op_ret_next_close"))} '
                f'| +3일 {int(float(r.get("op_first_plus3_day", 0) or 0))} / -3일 {int(float(r.get("op_first_minus3_day", 0) or 0))} '
                f'| 5일최대 {f(r.get("op_ret_max_high_5d"))} | 고정3/5 {f(r.get("op_fixed35_pnl"))}'
            )
        if len(tech_fn) > 20:
            lines12.append(f'- 나머지 {len(tech_fn)-20}건은 v49_70_technical_false_negative.csv에 전부 저장')
    lines12 += ['', '- 권위 검색식 조건은 변경하지 않았으며 A Gate를 권위식보다 넓은 SAFE SUPERSET으로 변경했습니다.', '- A 6조건 trace와 권위 replay score를 함께 저장해 향후 차이를 즉시 진단합니다.', '- 1건이라도 발생하면 종료코드 3 · PAPER/실제주문 0 유지']

    report = '\n'.join(lines1 + [''] + lines2 + [''] + lines3 + [''] + lines4 + [''] + lines5 + [''] + lines6 + [''] + lines7 + [''] + lines8 + [''] + lines9 + [''] + lines10 + [''] + lines11 + [''] + lines12)
    (out / 'v49_70_global_summary.txt').write_text(report, encoding='utf-8')
    files = [
        raw_path, canonical_path, selected_path,
        out / 'v49_70_primary_selected_top5.csv', out / 'v49_70_all_matched_selected_top5.csv',
        out / 'v49_70_strategy_funnel.csv', out / 'v49_70_strategy_stage_audit.csv', out / 'v49_70_identity_audit.csv',
        out / 'v49_70_zero_mode_audit.csv', out / 'v49_70_predicate_exceptions.csv',
        out / 'v49_70_strategy_attribution_performance.csv', out / 'v49_70_lp_walk_forward.csv', out / 'v49_70_iit_flow_split.csv',
        out / 'v49_70_selected_enriched_outcomes.csv', out / 'v49_70_outcome_distribution.csv',
        out / 'v49_70_winner_loser_feature_anatomy.csv', out / 'v49_70_train_oos_feature_lift.csv',
        out / 'v49_70_market_regime_performance.csv', out / 'v49_70_event_theme_context.csv',
        out / 'v49_70_down_market_cases.csv',
        out / 'v49_70_lgs_candidate_library.csv', out / 'v49_70_lgs_redundancy.csv', out / 'v49_70_lgs_locked_test.csv',
        out / 'v49_70_down_market_candidate_library.csv', out / 'v49_70_down_market_redundancy.csv', out / 'v49_70_down_market_locked_test.csv',
        out / 'v49_70_lp_exit_policy_audit.csv', out / 'v49_70_lp_exit_selected.csv', out / 'v49_70_lp_locked_test_stop_cases.csv',
        out / 'v49_70_triple_split_manifest.json',
        out / 'v49_70_search_intent_enriched.csv', out / 'v49_70_search_intent_summary.csv',
        out / 'v49_70_winner_loser_effect_size.csv', out / 'v49_70_manual_casebook.csv',
        out / 'v49_70_ranking_summary.csv', out / 'v49_70_ranking_detail.csv',
        out / 'v49_70_opportunity_miss_summary.csv', out / 'v49_70_opportunity_census_global.csv',
        out / 'v49_70_missed_winner_casebook.csv', out / 'v49_70_technical_false_negative.csv',
        out / 'v49_70_technical_false_negative.json', out / 'v49_70_opportunity_path_summary.csv',
        out / 'v49_70_distribution_drift.csv', out / 'v49_70_search_quality_manifest.json',
    ]
    manifest = {
        'version': s.CLOSING_BET_SCANNER_VERSION,
        'status': technical_status,
        'funnel_status': funnel_status,
        'preflight': preflight,
        'shards': manifest_docs,
        'shard_completions': complete_docs,
        'global_raw_input_rows': len(raw_input),
        'global_raw_valid_rows': len(raw_df),
        'global_canonical_rows': len(canonical),
        'global_selected_rows': len(selected),
        'performance_status': perf_status,
        'false_negative_hits': int(strategy_status.zero_audit_hits.sum()),
        'predicate_exceptions': int(strategy_status.exceptions.sum()),
        'identity_missing_dates': identity_global_missing,
        'identity_invalid_modes': identity_global_invalid_mode,
        'pipeline_invalid_strategies': strategy_status[strategy_status.status.str.startswith('INVALID')]['strategy'].tolist(),
        'anatomy_status': anatomy_status,
        'anatomy_context': anatomy_diag,
        'sector_context_status': sector_context_status,
        'sector_context_valid_min_coverage_pct': V4970_SECTOR_VALID_MIN_COVERAGE_PCT,
        'event_context_status': event_context_status,
        'context_guard_status': context_guard_status,
        'require_sector_context': V4970_REQUIRE_SECTOR_CONTEXT,
        'paper_priority_feature_candidates': int(len(feature_candidates)),
        'feature_leakage_guard': 'PASS',
        'feature_threshold_authority': 'TRAIN_ONLY_FROZEN_TO_OOS',
        'sector_context_limit': 'SIGNAL_CANDIDATE_PROXY_NOT_FULL_SECTOR',
        'event_context_limit': 'KEYWORD_CONTEXT_NO_CAUSALITY_CLAIM',
        'triple_split_status': triple_manifest.get('status'),
        'triple_split_boundaries': triple_manifest.get('boundaries'),
        'test_purity_warning': triple_manifest.get('historical_purity_warning'),
        'lgs_locked_test_pass': triple_manifest.get('lgs_locked_test_pass'),
        'down_market_locked_test_pass': triple_manifest.get('down_market_locked_test_pass'),
        'lp_selected_policy': triple_manifest.get('lp_selected_policy'),
        'lp_locked_test_status': triple_manifest.get('lp_locked_test_status'),
        'search_quality_status': quality_status,
        'search_quality_manifest': quality_manifest,
        'opportunity_census_scope': 'ALL_MATERIAL_OPPORTUNITIES_WITH_DETERMINISTIC_DEEP_AUDIT_SUBSET',
        'intent_proxy_scope': 'INDEPENDENT_MACHINE_PROXY_NOT_AUTHORITATIVE_PREDICATE',
        'selection_authority': 'TRAIN_THRESHOLD_VALIDATION_SELECTION_LOCKED_TEST_ACCEPTANCE',
        'test_reselection_forbidden': True,
        'files': {p.name: file_sha(p) for p in files if p.exists()},
        'paper_only': True,
        'real_orders': 0,
    }
    (out / 'v49_70_global_manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    print(report)
    _deliver_telegram([lines1, lines2, lines3, lines4, lines5, lines6, lines7, lines8, lines9, lines10, lines11, lines12], out, bool(args.send_telegram))
    return 3 if technical_status == 'INVALID' else 0


if __name__ == '__main__':
    sys.exit(main())
