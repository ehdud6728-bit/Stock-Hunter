# -*- coding: utf-8 -*-
"""
Closing_bet_scanner_v4_3_9_COMPACT_OPERATION_SUMMARY_complete.py

v4.3.9 COMPACT OPERATION SUMMARY overlay runner.

Purpose
-------
- Reuse the existing v4.3.8 Stock Feature Risk Analysis scanner without changing
  its core signal/backtest logic.
- Inject compact Telegram/report behavior just before the original CLI entrypoint.
- Keep artifact CSV/HTML generation, but make the Telegram summary useful by itself.

How to use
----------
Recommended:
    python Closing_bet_scanner_v4_3_9_COMPACT_OPERATION_SUMMARY_complete.py --backtest-months 18 --backtest-hold-days 60 --backtest-i-core-only --backtest-all-candidates --send-backtest-summary --force

This file expects one of these base files to exist in the same folder:
    1) Closing_bet_scanner_v4_3_8_STOCK_FEATURE_RISK_ANALYSIS_complete.py
    2) Closing_bet_scanner_v2.py  (currently v4.3.8 in the repo)

If you want to replace Closing_bet_scanner_v2.py directly, first keep a copy of the
old v4.3.8 file under the exact v4.3.8 filename above, then run this v4.3.9 file.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

V439_VERSION = "G_MORALES_V4_3_9_COMPACT_OPERATION_SUMMARY_20260517"
V438_VERSION = "G_MORALES_V4_3_8_STOCK_FEATURE_RISK_ANALYSIS_20260516"


# -----------------------------------------------------------------------------
# v4.3.9 default environment — can be overridden by GitHub Actions inputs/env.
# -----------------------------------------------------------------------------
def _env_default(key: str, value: str) -> None:
    if os.environ.get(key, "") == "":
        os.environ[key] = value


_env_default("CLOSING_BET_COMPACT_OPERATION_SUMMARY", "1")
_env_default("CLOSING_BET_COMPACT_MAX_CHARS", "9000")
_env_default("CLOSING_BET_COMPACT_I_MAIN_SAMPLE_TOP_N", "5")
_env_default("CLOSING_BET_BACKTEST_DETAIL_TOP_N", "5")
_env_default("CLOSING_BET_BACKTEST_DETAIL_MAX_ROWS", "20000")

# Hide broad/diagnostic sections by default. Turn back on with env if needed.
_env_default("CLOSING_BET_SHOW_LEGACY_SECTIONS", "0")
_env_default("CLOSING_BET_SHOW_RISK_DETAILS", "0")
_env_default("CLOSING_BET_SHOW_C_DIAG", "0")
_env_default("CLOSING_BET_SHOW_H_DIAG", "0")
_env_default("CLOSING_BET_SHOW_FULL_I_CORE_REPORT", "0")
_env_default("CLOSING_BET_SHOW_FULL_STOCK_FEATURE_REPORT", "0")
_env_default("CLOSING_BET_SHOW_FULL_BROAD_DIAG", "0")

# Candidate count compression for live output.
_env_default("CLOSING_BET_PRACTICAL_A_TOP_N", "1")
_env_default("CLOSING_BET_PRACTICAL_C_SWING_TOP_N", "1")
_env_default("CLOSING_BET_PRACTICAL_C_PULLBACK_TOP_N", "1")
_env_default("CLOSING_BET_PRACTICAL_H_TRIANGLE_TOP_N", "2")
_env_default("CLOSING_BET_PRACTICAL_H_CORE_TOP_N", "3")
_env_default("CLOSING_BET_PRACTICAL_H_FAST_TOP_N", "1")
_env_default("CLOSING_BET_PRACTICAL_I_MAIN_CORE_TOP_N", "3")
_env_default("CLOSING_BET_PRACTICAL_I_MAIN_ACCEL_TOP_N", "3")
_env_default("CLOSING_BET_PRACTICAL_I_MAIN_WATCH_TOP_N", "2")
_env_default("CLOSING_BET_PRACTICAL_I_MAIN_ADD_TOP_N", "2")
_env_default("CLOSING_BET_PRACTICAL_I_MAIN_CONFIRM_TOP_N", "1")

# v4.3.8 결과 기준으로 I-MAIN 검증은 MAIN 후보를 기본으로 압축.
_env_default("I_CORE_MAIN_ONLY", "1")
_env_default("I_CORE_MAIN_MIN_MATERIAL", "3")
_env_default("I_CORE_MAIN_REQUIRE_OBV_AMOUNT", "1")
_env_default("CLOSING_BET_STOCK_FEATURE_REPORT", "1")
_env_default("CLOSING_BET_STOCK_FEATURE_MIN_N", "5")


INJECT_PATCH = r'''
# =============================================================================
# v4.3.9 COMPACT OPERATION SUMMARY — injected patch
# =============================================================================
try:
    CLOSING_BET_SCANNER_VERSION = "G_MORALES_V4_3_9_COMPACT_OPERATION_SUMMARY_20260517"
except Exception:
    pass

try:
    _v439_scan_universe = str(os.environ.get('CLOSING_BET_SCAN_UNIVERSE', '')).strip()
    if _v439_scan_universe:
        SCAN_UNIVERSE = _v439_scan_universe
except Exception:
    pass

try:
    import os as _v439_os
    import re as _v439_re
except Exception:
    _v439_os = os


def _v439_bool_env(key, default='0'):
    try:
        return str(_v439_os.environ.get(key, default)).strip().lower() in ('1', 'true', 'yes', 'y', 'on')
    except Exception:
        return False


def _v439_int_env(key, default=0):
    try:
        return int(float(_v439_os.environ.get(key, str(default))))
    except Exception:
        return int(default)


CLOSING_BET_COMPACT_OPERATION_SUMMARY = _v439_bool_env('CLOSING_BET_COMPACT_OPERATION_SUMMARY', '1')
CLOSING_BET_COMPACT_MAX_CHARS = _v439_int_env('CLOSING_BET_COMPACT_MAX_CHARS', 9000)
CLOSING_BET_COMPACT_I_MAIN_SAMPLE_TOP_N = max(1, _v439_int_env('CLOSING_BET_COMPACT_I_MAIN_SAMPLE_TOP_N', 5))
CLOSING_BET_SHOW_FULL_I_CORE_REPORT = _v439_bool_env('CLOSING_BET_SHOW_FULL_I_CORE_REPORT', '0')
CLOSING_BET_SHOW_FULL_STOCK_FEATURE_REPORT = _v439_bool_env('CLOSING_BET_SHOW_FULL_STOCK_FEATURE_REPORT', '0')
CLOSING_BET_SHOW_FULL_BROAD_DIAG = _v439_bool_env('CLOSING_BET_SHOW_FULL_BROAD_DIAG', '0')

try:
    BACKTEST_DETAIL_TOP_N = min(int(BACKTEST_DETAIL_TOP_N), CLOSING_BET_COMPACT_I_MAIN_SAMPLE_TOP_N)
except Exception:
    BACKTEST_DETAIL_TOP_N = CLOSING_BET_COMPACT_I_MAIN_SAMPLE_TOP_N


def _v439_num(series_or_value, default=0.0):
    try:
        if 'pd' in globals() and isinstance(series_or_value, pd.Series):
            return pd.to_numeric(series_or_value, errors='coerce').fillna(default)
        return float(series_or_value)
    except Exception:
        return default


def _v439_flag_series(df, col):
    try:
        if df is None or df.empty or col not in df.columns:
            return pd.Series(False, index=df.index if df is not None else [])
        return pd.to_numeric(df.get(col), errors='coerce').fillna(0).astype(float).ge(1)
    except Exception:
        return pd.Series(False, index=df.index if df is not None else [])


def _v439_text_series(df, col, default=''):
    try:
        if df is None or df.empty or col not in df.columns:
            return pd.Series(default, index=df.index if df is not None else [])
        return df.get(col).astype(str)
    except Exception:
        return pd.Series(default, index=df.index if df is not None else [])


def _v439_code_col(df):
    for c in ['code', '종목코드', 'ticker', 'Code']:
        if df is not None and c in df.columns:
            return c
    return ''


def _v439_signal_date_col(df):
    for c in ['signal_date', '발생일자', 'date', 'Date']:
        if df is not None and c in df.columns:
            return c
    return ''


def _v439_dedupe_by_stock(df, sort_col='', ascending=False, recent=False):
    try:
        if df is None or df.empty:
            return df
        view = df.copy()
        code_col = _v439_code_col(view)
        date_col = _v439_signal_date_col(view)
        if code_col:
            view['_v439_code_key'] = view[code_col].astype(str).str.replace('.0', '', regex=False).str.extract(r'(\d+)')[0].fillna(view[code_col].astype(str)).str.zfill(6)
        else:
            view['_v439_code_key'] = view.index.astype(str)
        if date_col:
            view['_v439_date_key'] = pd.to_datetime(view[date_col], errors='coerce')
        else:
            view['_v439_date_key'] = pd.Timestamp.min
        if recent:
            view = view.sort_values(['_v439_date_key'], ascending=[False], na_position='last')
        elif sort_col and sort_col in view.columns:
            view['_v439_sort_key'] = pd.to_numeric(view[sort_col], errors='coerce')
            view = view.sort_values(['_v439_sort_key', '_v439_date_key'], ascending=[ascending, False], na_position='last')
        else:
            view = view.sort_values(['_v439_date_key'], ascending=[False], na_position='last')
        view = view.drop_duplicates('_v439_code_key', keep='first')
        drop_cols = [c for c in ['_v439_code_key', '_v439_date_key', '_v439_sort_key'] if c in view.columns]
        return view.drop(columns=drop_cols, errors='ignore')
    except Exception:
        return df


def _v439_pct(x, suffix='%'):
    try:
        v = pd.to_numeric(pd.Series([x]), errors='coerce').iloc[0]
        if pd.isna(v):
            return '-'
        return f"{v:+.1f}{suffix}"
    except Exception:
        return '-'


def _v439_price(x):
    try:
        v = float(x)
        return f"{int(round(v)):,}원" if v > 0 else '-'
    except Exception:
        return '-'


def _v439_normalize_code(x):
    try:
        if '_normalize_code' in globals():
            return _normalize_code(str(x))
        s = ''.join(ch for ch in str(x).replace('.0', '') if ch.isdigit())
        return s.zfill(6)
    except Exception:
        return str(x)


def _v439_rule35_stat(sub, label):
    try:
        if sub is None or sub.empty:
            return f"- {label}: 해당 없음"
        n = len(sub)
        pnl = pd.to_numeric(sub.get('rule35_pnl', pd.Series(dtype=float)), errors='coerce').mean()
        win = pd.to_numeric(sub.get('rule35_win', pd.Series(dtype=float)), errors='coerce').mean() * 100
        stop = pd.to_numeric(sub.get('rule35_stop', pd.Series(dtype=float)), errors='coerce').mean() * 100
        hit3 = pd.to_numeric(sub.get('rule35_hit3', pd.Series(dtype=float)), errors='coerce').mean() * 100
        hit5 = pd.to_numeric(sub.get('rule35_hit5', pd.Series(dtype=float)), errors='coerce').mean() * 100
        return f"- {label}: {n}건 | 3/5 {_v439_pct(pnl)} | 승률 {win:.1f}% | +3/+5 {hit3:.1f}/{hit5:.1f}% | 손절 {stop:.1f}%"
    except Exception as e:
        return f"- {label}: 통계 오류 {type(e).__name__}"


def _v439_i_stat(sub, label):
    try:
        if sub is None or sub.empty:
            return f"- {label}: 해당 없음"
        n = len(sub)
        r20 = pd.to_numeric(sub.get('i_ret_close_20d', pd.Series(dtype=float)), errors='coerce').mean()
        r40 = pd.to_numeric(sub.get('i_ret_close_40d', pd.Series(dtype=float)), errors='coerce').mean()
        r60 = pd.to_numeric(sub.get('i_ret_close_60d', sub.get('ret_close_hd', pd.Series(dtype=float))), errors='coerce').mean()
        h10 = pd.to_numeric(sub.get('i_hit10_60d', pd.Series(dtype=float)), errors='coerce').mean() * 100
        h20 = pd.to_numeric(sub.get('i_hit20_60d', pd.Series(dtype=float)), errors='coerce').mean() * 100
        h30 = pd.to_numeric(sub.get('i_hit30_60d', pd.Series(dtype=float)), errors='coerce').mean() * 100
        h50 = pd.to_numeric(sub.get('i_hit50_60d', pd.Series(dtype=float)), errors='coerce').mean() * 100
        box = pd.to_numeric(sub.get('i_box_fail_close', pd.Series(dtype=float)), errors='coerce').mean() * 100
        bench = pd.to_numeric(sub.get('i_bench_excess_close_60d', pd.Series(dtype=float)), errors='coerce').mean()
        tail = f" | BENCH초과 {_v439_pct(bench)}" if not pd.isna(bench) else ''
        return f"- {label}: {n}건 | 20/40/60일 {_v439_pct(r20)}/{_v439_pct(r40)}/{_v439_pct(r60)} | +10/+20/+30/+50 {h10:.1f}/{h20:.1f}/{h30:.1f}/{h50:.1f}% | 박스실패 {box:.1f}%{tail}"
    except Exception as e:
        return f"- {label}: 통계 오류 {type(e).__name__}"


def _v439_i_df(df):
    try:
        if df is None or df.empty:
            return pd.DataFrame()
        if '_bt_mask_i_core_all' in globals() and callable(_bt_mask_i_core_all):
            _mask = _bt_mask_i_core_all(df)
            if isinstance(_mask, pd.Series):
                _mask = _mask.reindex(df.index).fillna(False).astype(bool)
            i = df[_mask].copy()
        elif 'mode' in df.columns:
            i = df[df['mode'].astype(str).eq('I')].copy()
        else:
            i = pd.DataFrame()
        if '_i_main_enriched_df' in globals() and callable(_i_main_enriched_df):
            i = _i_main_enriched_df(i)
        return i
    except Exception:
        return pd.DataFrame()


def _format_i_main_signal_samples(sub: pd.DataFrame, label: str, max_rows: int | None = None, sort_col: str = 'i_ret_close_60d', ascending: bool = False) -> str:
    """v4.3.9: I-MAIN Telegram sample — one representative signal per stock."""
    try:
        if sub is None or sub.empty:
            return f"- {label}: 해당 없음"
        n = CLOSING_BET_COMPACT_I_MAIN_SAMPLE_TOP_N if max_rows is None else min(int(max_rows), CLOSING_BET_COMPACT_I_MAIN_SAMPLE_TOP_N)
        recent = str(sort_col).lower() in ('signal_date', '발생일자', 'date')
        view = _v439_dedupe_by_stock(sub, sort_col=sort_col, ascending=ascending, recent=recent).head(max(1, n))
        lines = [f"- {label}: 종목별 중복제거 {len(view)}개"]
        for _, r in view.iterrows():
            date = str(r.get('signal_date', r.get('발생일자', '')))[:10]
            name = str(r.get('name', r.get('종목명', ''))).strip() or '종목명확인필요'
            code = _v439_normalize_code(str(r.get('code', r.get('종목코드', ''))))
            phase = str(r.get('i_phase', r.get('I타점', r.get('mode', ''))))
            cls = str(r.get('imain_primary_class', r.get('I-MAIN_대표분류', r.get('mode_label', ''))))
            ret20 = _v439_pct(r.get('i_ret_close_20d', r.get('20일종가수익_pct', float('nan'))))
            ret40 = _v439_pct(r.get('i_ret_close_40d', r.get('40일종가수익_pct', float('nan'))))
            ret60 = _v439_pct(r.get('i_ret_close_60d', r.get('60일종가수익_pct', r.get('ret_close_hd', float('nan')))))
            max60 = _v439_pct(r.get('i_ret_max_high_60d', r.get('60일최대상승_pct', r.get('ret_max_high_hd', float('nan')))))
            h10 = 'O' if float(pd.to_numeric(pd.Series([r.get('i_hit10_60d', r.get('+10도달', 0))]), errors='coerce').fillna(0).iloc[0]) >= 1 else 'X'
            h20 = 'O' if float(pd.to_numeric(pd.Series([r.get('i_hit20_60d', r.get('+20도달', 0))]), errors='coerce').fillna(0).iloc[0]) >= 1 else 'X'
            h30 = 'O' if float(pd.to_numeric(pd.Series([r.get('i_hit30_60d', r.get('+30도달', 0))]), errors='coerce').fillna(0).iloc[0]) >= 1 else 'X'
            h50 = 'O' if float(pd.to_numeric(pd.Series([r.get('i_hit50_60d', r.get('+50도달', 0))]), errors='coerce').fillna(0).iloc[0]) >= 1 else 'X'
            bench_ex = _v439_pct(r.get('i_bench_excess_close_60d', r.get('선택벤치60일초과_pct', float('nan'))))
            tags = str(r.get('imain_detail_tags', r.get('I-MAIN_세부태그', ''))).strip()
            tag_txt = f" | {tags}" if tags else ''
            lines.append(
                f" · {date} | {name}({code}) | {cls}/{phase} | 20/40/60 {ret20}/{ret40}/{ret60} | 최대 {max60} | +10/20/30/50 {h10}/{h20}/{h30}/{h50} | BENCH {bench_ex}{tag_txt}"
            )
        return "\n".join(lines)
    except Exception as e:
        return f"- {label}: 샘플 생성 오류 {type(e).__name__}"


def _format_i_main_signal_detail_report(df: pd.DataFrame) -> str:
    """v4.3.9: compact, de-duplicated I-MAIN sample report."""
    try:
        i = _v439_i_df(df)
        lines = ["\n[📋 I-MAIN 발생 샘플 — 종목별 중복제거 v4.3.9]"]
        if i.empty:
            lines.append('- I-MAIN/I-CORE 발생 상세 후보 없음')
            return "\n".join(lines)
        core = i[_v439_flag_series(i, 'imain_core')]
        accel = i[_v439_flag_series(i, 'imain_accel')]
        main = i[_v439_flag_series(i, 'imain_is_main')]
        weak = i[pd.to_numeric(i.get('i_ret_close_60d', i.get('ret_close_hd', pd.Series(index=i.index, dtype=float))), errors='coerce').fillna(999).lt(0)].copy()
        lines.append(_format_i_main_signal_samples(accel, '🚀 ACCEL 성과상위', max_rows=CLOSING_BET_COMPACT_I_MAIN_SAMPLE_TOP_N, sort_col='i_ret_close_60d', ascending=False))
        lines.append(_format_i_main_signal_samples(core, '✅ CORE 성과상위', max_rows=CLOSING_BET_COMPACT_I_MAIN_SAMPLE_TOP_N, sort_col='i_ret_close_40d', ascending=False))
        lines.append(_format_i_main_signal_samples(main, '🕒 MAIN 최근발생', max_rows=CLOSING_BET_COMPACT_I_MAIN_SAMPLE_TOP_N, sort_col='signal_date', ascending=False))
        lines.append(_format_i_main_signal_samples(weak, '⚠️ 음수/실패 대표샘플', max_rows=min(3, CLOSING_BET_COMPACT_I_MAIN_SAMPLE_TOP_N), sort_col='i_ret_close_60d', ascending=True))
        lines.append('- 전체 상세 CSV/HTML은 closing_bet_logs artifact에 저장됩니다.')
        return "\n".join(lines)
    except Exception as e:
        return f"\n[📋 I-MAIN 발생 샘플]\n- 생성 오류: {type(e).__name__}: {e}"


def _v439_operation_filter_conclusion() -> str:
    return "\n".join([
        "\n[🎯 v4.3.9 실전 운용 자동 결론]",
        "- 단기 1순위: L 리더갭 5000억+ · 갭 3~12% · 종가위치 70%+.",
        "- 단기 1-1순위: L 5000억~1조 / 갭 6~12% / 종가위치 70%+는 가장 강한 실전 후보로 우선 검토.",
        "- 단기 2순위: S-CORE NEUTRAL / S2 실행형. KOSPI200·5조+·거래대금 3000억+일수록 우선.",
        "- 중기 1순위: I-MAIN ACCEL. 20/40/60거래일 시세분출 관점, 단기 종가배팅으로 해석 금지.",
        "- 중기 2순위: I-MAIN CORE. 안정형 고확률 누적관찰 후보.",
        "- 관찰: H 핵심셀, A/C 고거래대금 후보만 제한적으로 확인.",
        "- 제외/강등: 저거래대금 A/B/C, B1/B2 즉시매수, C 넓은 후보, H-RISK, 종가위치 70% 미만 S 후보.",
        "- 텔레그램은 결론 중심으로 압축하고, 전체 CSV/HTML은 artifact에서 복기합니다.",
    ])


def _format_stock_feature_report(df: pd.DataFrame) -> str:
    """v4.3.9: compact stock-feature success/risk report."""
    if CLOSING_BET_SHOW_FULL_STOCK_FEATURE_REPORT:
        try:
            return _v439_original_format_stock_feature_report(df)
        except Exception:
            pass
    lines = ["\n[🧬 종목특성 핵심 요약 — 차트 외 요인 v4.3.9 COMPACT]"]
    try:
        if df is None or df.empty:
            lines.append('- 데이터 없음')
            lines.append(_v439_operation_filter_conclusion())
            return "\n".join(lines)
        work = _stock_feature_bucket_columns(df) if '_stock_feature_bucket_columns' in globals() else df.copy()
        lines.append('- 한줄 결론: 시총 단독보다 거래대금·대표성·재료/거래대금 프록시가 더 실전적이었습니다.')
        # Liquidity summary
        amt = pd.to_numeric(work.get('amount_b', pd.Series(0, index=work.index)), errors='coerce').fillna(0)
        lines.append(_v439_rule35_stat(work[amt >= 5000], '전체 거래대금 5000억+'))
        lines.append(_v439_rule35_stat(work[(amt >= 3000) & (amt < 5000)], '전체 거래대금 3000~5000억'))
        low_amt = work[amt < 300]
        if not low_amt.empty:
            lines.append(_v439_rule35_stat(low_amt, '저유동성 300억 미만'))
        # L leader gap
        if '_bt_mask_leader_gap_all' in globals():
            l_all = work[_bt_mask_leader_gap_all(work)].copy()
        else:
            l_all = work[_v439_text_series(work, 'mode').eq('L') | _v439_text_series(work, 'strategy').eq('L')]
        if not l_all.empty:
            l_amt = pd.to_numeric(l_all.get('amount_b', pd.Series(0, index=l_all.index)), errors='coerce').fillna(0)
            close_loc = pd.to_numeric(l_all.get('close_loc_pct', l_all.get('close_loc', pd.Series(0, index=l_all.index))), errors='coerce').fillna(0)
            gap = pd.to_numeric(l_all.get('gap_pct', l_all.get('gap_rate', pd.Series(0, index=l_all.index))), errors='coerce').fillna(0)
            lines.append('\n[단기 L 리더갭]')
            lines.append(_v439_rule35_stat(l_all, 'L READY'))
            lines.append(_v439_rule35_stat(l_all[l_amt >= 5000], 'L 5000억+'))
            lines.append(_v439_rule35_stat(l_all[(l_amt >= 5000) & (l_amt < 10000)], 'L 5000억~1조'))
            strong_l = l_all[(l_amt >= 5000) & (gap >= 6) & (gap <= 12) & (close_loc >= 70)]
            lines.append(_v439_rule35_stat(strong_l, 'L 갭6~12%×5000억+×종가위치70%+'))
        # S core
        s_neu = pd.DataFrame()
        s2 = pd.DataFrame()
        try:
            if '_bt_mask_s_core_neutral' in globals():
                s_neu = work[_bt_mask_s_core_neutral(work)].copy()
        except Exception:
            pass
        try:
            s_type = _v439_text_series(work, 's_type') + ' ' + _v439_text_series(work, 's_quality') + ' ' + _v439_text_series(work, 'mode_label')
            s2 = work[s_type.str.contains('S2|실행', regex=True, na=False)].copy()
        except Exception:
            pass
        if not s_neu.empty or not s2.empty:
            lines.append('\n[단기 S-CORE]')
            lines.append(_v439_rule35_stat(s_neu, 'S-CORE NEUTRAL'))
            lines.append(_v439_rule35_stat(s2, 'S2 실행형'))
            lines.append('- S는 KOSPI200·5조+·거래대금 3000억+·종가위치 70%+ 조건을 우선하고, 종가위치 70% 미만은 강등합니다.')
        # I main
        i = _v439_i_df(work)
        if not i.empty:
            core = i[_v439_flag_series(i, 'imain_core')]
            accel = i[_v439_flag_series(i, 'imain_accel')]
            main = i[_v439_flag_series(i, 'imain_is_main')]
            lines.append('\n[중기 I-MAIN]')
            lines.append(_v439_i_stat(main, 'I-CORE MAIN'))
            lines.append(_v439_i_stat(core, 'I-MAIN CORE'))
            lines.append(_v439_i_stat(accel, 'I-MAIN ACCEL'))
            material = pd.to_numeric(i.get('i_material_proxy_score', pd.Series(0, index=i.index)), errors='coerce').fillna(0)
            idx = _v439_text_series(i, 'index_label')
            marcap = pd.to_numeric(i.get('marcap', pd.Series(0, index=i.index)), errors='coerce').fillna(0)
            amt_i = pd.to_numeric(i.get('amount_b', pd.Series(0, index=i.index)), errors='coerce').fillna(0)
            lines.append(_v439_i_stat(core[idx.str.contains('코스피200|KOSPI200|K200', regex=True, na=False)], 'CORE KOSPI200'))
            lines.append(_v439_i_stat(accel[marcap >= 5000000000000], 'ACCEL 5조+'))
            lines.append(_v439_i_stat(i[material >= 4], 'I-MAIN 재료/대금 4점+'))
            lines.append(_v439_i_stat(core[(amt_i >= 300) & (amt_i < 1000)], 'CORE 거래대금 300~1000억'))
        lines.append(_v439_operation_filter_conclusion())
        return "\n".join(lines)
    except Exception as e:
        lines.append(f'- 종목특성 요약 생성 오류: {type(e).__name__}: {e}')
        lines.append(_v439_operation_filter_conclusion())
        return "\n".join(lines)


def _format_i_core_report(df: pd.DataFrame) -> str:
    """v4.3.9: compact I-MAIN report. Full v4.3.8 report remains available by env."""
    if CLOSING_BET_SHOW_FULL_I_CORE_REPORT:
        try:
            return _v439_original_format_i_core_report(df)
        except Exception:
            pass
    try:
        i = _v439_i_df(df)
        lines = ["\n[📈 I-MAIN 150/200일 시세분출 — v4.3.9 COMPACT]"]
        if i.empty:
            lines.append('- I-MAIN/I-CORE 후보 없음')
            return "\n".join(lines)
        core = i[_v439_flag_series(i, 'imain_core')]
        accel = i[_v439_flag_series(i, 'imain_accel')]
        main = i[_v439_flag_series(i, 'imain_is_main')]
        watch = i[_v439_flag_series(i, 'imain_watch')]
        confirm = i[_v439_flag_series(i, 'imain_confirm')]
        add = i[_v439_flag_series(i, 'imain_add')]
        phase = _v439_text_series(i, 'i_phase')
        lines.append('- 해석: I-MAIN은 단기 종가배팅이 아니라 20/40/60거래일 누적관찰·분할매집 후보입니다.')
        lines.append(_v439_i_stat(main if not main.empty else i, 'I-CORE MAIN'))
        lines.append(_v439_i_stat(accel, '🚀 I-MAIN ACCEL: 고수익 시세분출형'))
        lines.append(_v439_i_stat(core, '✅ I-MAIN CORE: 안정형 고확률'))
        lines.append(_v439_i_stat(watch, '🟡 I-MAIN WATCH: I-4 관찰'))
        lines.append(_v439_i_stat(confirm, '🔎 I-MAIN CONFIRM: I-5 돌파확인'))
        lines.append(_v439_i_stat(add, '➕ I-MAIN ADD: I-6 첫눌림'))
        lines.append('\n[타점 해석]')
        lines.append('- I-4: 5MA가 150/200MA 회복, 1차 매집 핵심.')
        lines.append('- I-5: 박스/120일 고점 돌파 확인. 단독 신규진입보다 확인 신호로 사용.')
        lines.append('- I-6: 돌파 후 첫 눌림 재지지, 추가매수 후보.')
        lines.append(_format_i_main_signal_detail_report(i))
        return "\n".join(lines)
    except Exception as e:
        return f"\n[📈 I-MAIN 150/200일 시세분출]\n- 리포트 생성 오류: {type(e).__name__}: {e}"


try:
    _v439_original_format_stock_feature_report = globals().get('_format_stock_feature_report')
    _v439_original_format_i_core_report = globals().get('_format_i_core_report')
except Exception:
    pass


def _v439_block_header(line):
    s = str(line).strip()
    return (s.startswith('[') and s.endswith(']')) or (s.startswith('🧪') or s.startswith('📈') or s.startswith('🧬'))


def _v439_compact_report_text(report: str) -> str:
    """Last-resort text compressor for broad A/B/C/H diagnostic overflow."""
    try:
        if not CLOSING_BET_COMPACT_OPERATION_SUMMARY:
            return report
        if CLOSING_BET_SHOW_FULL_BROAD_DIAG:
            return report
        txt = str(report)
        # Drop selected broad diagnostic blocks while preserving core L/S/I/stock-feature conclusions.
        lines = txt.splitlines()
        blocks = []
        cur = []
        for line in lines:
            if line.strip().startswith('[') and cur:
                blocks.append(cur)
                cur = [line]
            else:
                cur.append(line)
        if cur:
            blocks.append(cur)
        drop_keywords = [
            'A 보조', 'A 전체', 'B1', 'B2', 'C 역매', 'C-SWING', '역매공파',
            'H 신고가거자름 전체', 'H-WATCH', 'H v4.2', 'H 직전 구조', 'H 돌파봉',
            'H-OVERHEAT', 'H-AGGRESSIVE', '완화형', '진단 후보'
        ]
        keep_keywords = [
            '전체', '실전형', 'L ', '리더갭', 'S-CORE', 'S2', 'I-MAIN', 'I-CORE',
            '종목특성', '실전 운용', '백테스트 발생', '상세 파일', '진단', '0건'
        ]
        kept = []
        for b in blocks:
            header = b[0].strip() if b else ''
            if any(k in header for k in drop_keywords) and not any(k in header for k in ['H-CORE', '핵심']):
                continue
            kept.append('\n'.join(b))
        out = '\n'.join(kept).strip()
        if '[🎯 v4.3.9 실전 운용 자동 결론]' not in out:
            out += '\n' + _v439_operation_filter_conclusion()
        if len(out) > CLOSING_BET_COMPACT_MAX_CHARS:
            # Keep front overview plus the v4.3.9 conclusion and detail paths.
            front = out[:max(2500, CLOSING_BET_COMPACT_MAX_CHARS // 2)].rstrip()
            important = []
            for marker in ['[🧬 종목특성 핵심 요약', '[📈 I-MAIN', '[📋 I-MAIN', '[🎯 v4.3.9', '[ 백테스트 발생 종목 상세 파일]']:
                idx = out.find(marker)
                if idx >= 0:
                    important.append(out[idx: idx + 2500])
            out = front + "\n\n...\n[중간 세부진단 압축 생략: CSV/HTML artifact 확인]\n" + "\n".join(important)
        return out
    except Exception:
        return report


try:
    _v439_original_build_backtest_summary = _build_backtest_summary
    def _build_backtest_summary(*args, **kwargs):
        report = _v439_original_build_backtest_summary(*args, **kwargs)
        return _v439_compact_report_text(report)
except Exception:
    pass

try:
    log_info(f"✅ V4.3.9_COMPACT_OPERATION_SUMMARY_PATCH_ACTIVE | compact={CLOSING_BET_COMPACT_OPERATION_SUMMARY}")
except Exception:
    print(f"✅ V4.3.9_COMPACT_OPERATION_SUMMARY_PATCH_ACTIVE | compact={CLOSING_BET_COMPACT_OPERATION_SUMMARY}")
# =============================================================================
# end v4.3.9 patch
# =============================================================================
'''


def _read_text_any(path: Path) -> str:
    for enc in ("utf-8", "utf-8-sig", "cp949"):
        try:
            return path.read_text(encoding=enc)
        except UnicodeDecodeError:
            continue
    return path.read_text(errors="replace")


def _find_base_file() -> Path:
    self_path = Path(__file__).resolve()
    root = self_path.parent
    candidates: list[Path] = []

    env_base = os.environ.get("CLOSING_BET_V438_BASE", "").strip()
    if env_base:
        candidates.append(Path(env_base))

    candidates.extend([
        root / "Closing_bet_scanner_v4_3_8_STOCK_FEATURE_RISK_ANALYSIS_complete.py",
        root / "Closing_bet_scanner_v2.py",
        root / "Closing_bet_scanner.py",
    ])

    checked = []
    for p in candidates:
        try:
            p = p.expanduser().resolve()
            if p == self_path:
                checked.append(f"{p} (self-skip)")
                continue
            if not p.exists() or not p.is_file():
                checked.append(f"{p} (missing)")
                continue
            src_head = _read_text_any(p)[:12000]
            if ("G_MORALES_V4_3_8" in src_head or "v4.3.8" in src_head or "_build_backtest_summary" in src_head) and "종가배팅" in src_head:
                return p
            checked.append(f"{p} (not v4.3.8-like)")
        except Exception as e:
            checked.append(f"{p} ({type(e).__name__}: {e})")

    msg = "\n".join(checked)
    raise FileNotFoundError(
        "v4.3.9 overlay가 사용할 v4.3.8 base 파일을 찾지 못했습니다.\n"
        "같은 폴더에 Closing_bet_scanner_v4_3_8_STOCK_FEATURE_RISK_ANALYSIS_complete.py "
        "또는 기존 Closing_bet_scanner_v2.py를 두세요.\n\n확인한 경로:\n" + msg
    )


def _inject_patch(source: str) -> str:
    if "V4.3.9_COMPACT_OPERATION_SUMMARY_PATCH_ACTIVE" in source:
        return source
    source = source.replace(V438_VERSION, V439_VERSION)
    marker = "if __name__ == '__main__':"
    if marker in source:
        return source.replace(marker, INJECT_PATCH + "\n" + marker, 1)
    return source + "\n\n" + INJECT_PATCH + "\n"


def main() -> None:
    base_path = _find_base_file()
    source = _read_text_any(base_path)
    patched_source = _inject_patch(source)

    # Execute the patched source as the original script, preserving __file__ so all
    # relative paths/log paths behave exactly like the base scanner.
    globs = {
        "__name__": "__main__",
        "__file__": str(base_path),
        "__package__": None,
        "__cached__": None,
    }
    code = compile(patched_source, str(base_path), "exec")
    exec(code, globs, globs)


if __name__ == "__main__":
    main()
