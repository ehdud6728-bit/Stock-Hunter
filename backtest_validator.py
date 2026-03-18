# =============================================================
# 📊 backtest_validator.py
# 기간 지정 백테스트 — 패턴별 승률 + 최고/최저점 추적
# =============================================================
# 사용법:
#   python backtest_validator.py --start 2024-09-01 --end 2025-01-31
#   python backtest_validator.py --start 2024-06-01 --end 2024-12-31 --hold_days 10
# =============================================================

import argparse
import os
import json
import traceback
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import FinanceDataReader as fdr
from pykrx import stock

# ─── 로컬 모듈 (main7.py 와 같은 폴더에서 실행)
from tactics_engine import (
    get_global_and_leader_status,
    analyze_all_narratives,
    get_dynamic_sector_leaders,
    calculate_dante_symmetry,
    watermelon_indicator_complete,
    judge_yeok_break_sequence_v2,
)
from triangle_combo_analyzer import jongbe_triangle_combo_v3

# get_indicators, calculate_combination_score 등은 main7.py에서 import
# main7.py를 직접 import 하거나 아래처럼 sys.path 활용
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# scan_logger 없으면 print로 폴백
try:
    from scan_logger import set_log_level, log_error, log_info, log_debug, log_scan_date
    set_log_level('NORMAL')
except ImportError:
    def log_info(msg):  print(msg)
    def log_error(msg): print(msg)
    def log_debug(msg): pass
    def log_scan_date(date, n): print(f"🗓️  {date} → 히트: {n}개")
from main7 import (
    get_indicators,
    calculate_combination_score,
    build_default_signals,
    inject_tri_result,
    classify_style,
    STYLE_WEIGHTS,
    stage_rank_value,
)
import main7 as _main7
# v2 함수 우선, 없으면 기존 함수 사용
evaluate_stage_sequence_v2 = getattr(
    _main7, 'evaluate_stage_sequence_v2',
    _main7.evaluate_stage_sequence
)


# =============================================================
# ⚙️ 설정
# =============================================================

HOLD_DAYS_LIST  = [3, 5, 10, 15, 20]  # ✅ FIX-E: 3일 추가 (초단기 급등 포착)
MIN_SCORE       = 150                # 최소 N점수 필터
MIN_AMOUNT      = 50                 # 최소 평균 거래대금 (억)
MAX_WORKERS     = 12                 # 병렬 스레드 수
PROFIT_TARGET   = 5.0                # 승리 기준 수익률 (%)
STOP_LOSS       = -5.0               # 손절 기준 (%)

# 패턴별 집계 대상 컬럼
PATTERN_COL     = 'N조합'


# =============================================================
# 📅 백테스트용 날짜 시퀀스 생성
# 매주 월요일만 스캔 (속도 조절), 또는 매일
# =============================================================

def get_trading_dates(start_str: str, end_str: str, freq: str = 'weekly') -> list:
    """
    start_str ~ end_str 사이 거래일(월~금) 목록 반환.
    freq='weekly' 이면 매주 월요일만, 'daily' 이면 매일.
    """
    try:
        # KRX 거래일 목록 (pykrx)
        all_dates = stock.get_market_ohlcv_by_date(start_str.replace('-',''), end_str.replace('-',''), 'KOSPI')
        trading_days = [d.strftime('%Y-%m-%d') for d in all_dates.index]
    except Exception:
        # fallback: 평일만
        s = datetime.strptime(start_str, '%Y-%m-%d')
        e = datetime.strptime(end_str, '%Y-%m-%d')
        trading_days = []
        d = s
        while d <= e:
            if d.weekday() < 5:
                trading_days.append(d.strftime('%Y-%m-%d'))
            d += timedelta(days=1)

    if freq == 'weekly':
        # 각 주에서 첫 번째 거래일
        weeks = {}
        for d in trading_days:
            dt = datetime.strptime(d, '%Y-%m-%d')
            week_key = dt.strftime('%Y-W%U')
            if week_key not in weeks:
                weeks[week_key] = d
        return sorted(weeks.values())
    return trading_days


# =============================================================
# 🔍 과거 시점에서 단일 종목 신호 판별
# =============================================================

def analyze_on_date(ticker: str, name: str, scan_date: str,
                    forward_df: pd.DataFrame = None) -> dict | None:
    """
    scan_date 기준으로 신호 여부 판별 + 이후 수익률 계산.

    Parameters
    ----------
    ticker     : 종목 코드
    name       : 종목명
    scan_date  : 스캔 기준일 (YYYY-MM-DD)
    forward_df : 미리 로드한 전체 OHLCV (없으면 직접 조회)

    Returns
    -------
    dict or None
    """
    try:
        scan_dt  = datetime.strptime(scan_date, '%Y-%m-%d')
        start_dt = scan_dt - timedelta(days=400)

        # 전체 데이터 로드 (scan_date 포함, 이후 최대 30일 포함)
        end_dt = scan_dt + timedelta(days=60)

        if forward_df is not None:
            full_df = forward_df
        else:
            full_df = fdr.DataReader(ticker,
                                     start=start_dt.strftime('%Y-%m-%d'),
                                     end=end_dt.strftime('%Y-%m-%d'))

        if full_df.empty or len(full_df) < 60:
            return None

        # ── scan_date까지의 히스토리 슬라이스
        hist_df = full_df[full_df.index <= scan_date].copy()
        if len(hist_df) < 60:
            return None

        # ── 지표 계산
        hist_df = get_indicators(hist_df)
        if hist_df is None or hist_df.empty:
            return None

        row  = hist_df.iloc[-1]
        prev = hist_df.iloc[-2]

        # ── 최소 거래대금 필터
        recent_avg_amount = (hist_df['Close'] * hist_df['Volume']).tail(5).mean() / 1e8
        if recent_avg_amount < MIN_AMOUNT:
            return None

        # ── 신호 계산
        signals, new_tags = build_default_signals(row, row['Close'], prev), []
        try:
            tri_result = jongbe_triangle_combo_v3(hist_df) or {}
        except Exception:
            tri_result = {}

        signals, new_tags = inject_tri_result(signals, tri_result, new_tags)

        result = calculate_combination_score(signals)

        if result['score'] < MIN_SCORE:
            return None

        # ── Stage 판정
        try:
            stage_eval = evaluate_stage_sequence_v2(hist_df)
        except Exception:
            from main7 import evaluate_stage_sequence
            stage_eval = evaluate_stage_sequence(hist_df)

        entry_price = float(row['Close'])

        # ── 이후 N일 수익률 추적
        future_df = full_df[full_df.index > scan_date].head(max(HOLD_DAYS_LIST) + 5)
        forward_returns = {}
        max_high_pct    = 0.0
        max_low_pct     = 0.0
        stop_triggered_day = None

        if not future_df.empty and entry_price > 0:
            highs  = future_df['High'].values
            lows   = future_df['Low'].values
            closes = future_df['Close'].values
            dates  = [d.strftime('%Y-%m-%d') for d in future_df.index]

            for hold in HOLD_DAYS_LIST:
                if hold <= len(closes):
                    exit_price = closes[hold - 1]
                    ret = (exit_price - entry_price) / entry_price * 100
                    forward_returns[f'수익률_{hold}일'] = round(ret, 2)
                else:
                    forward_returns[f'수익률_{hold}일'] = None

            # 보유 기간 중 최고/최저
            n = min(max(HOLD_DAYS_LIST), len(highs))
            if n > 0:
                max_high_pct = round((highs[:n].max() - entry_price) / entry_price * 100, 2)
                max_low_pct  = round((lows[:n].min()  - entry_price) / entry_price * 100, 2)

            # 손절 발동일 탐색
            for i, (low_p, date_s) in enumerate(zip(lows, dates)):
                if i >= max(HOLD_DAYS_LIST):
                    break
                if (low_p - entry_price) / entry_price * 100 <= STOP_LOSS:
                    stop_triggered_day = i + 1
                    break

        record = {
            '스캔일':       scan_date,
            '종목명':       name,
            'code':         ticker,
            '진입가':       int(entry_price),
            'N등급':        f"{result['type']}{result['grade']}",
            PATTERN_COL:    result['combination'],
            'N점수':        result['score'],
            'N구분':        " ".join(new_tags),
            '단계상태':     stage_eval.get('stage_status', 'DROP'),
            '단계랭크':     stage_rank_value(stage_eval.get('stage_status', 'DROP')),
            'S1날짜':       stage_eval.get('s1_date'),
            'S2날짜':       stage_eval.get('s2_date'),
            'S3날짜':       stage_eval.get('s3_date'),
            'RSI':          round(float(row.get('RSI', 0)), 1),
            'BB40폭':       round(float(row.get('BB40_Width', 0)), 1),
            'MA수렴':       round(float(row.get('MA_Convergence', 0)), 1),
            'OBV기울기':    int(row.get('OBV_Slope', 0)),
            '이격':         int(row.get('Disparity', 0)),
            '최고점%':      max_high_pct,
            '최저점%':      max_low_pct,
            '손절발동일':   stop_triggered_day,
            **forward_returns,
        }

        # ── 승/패 판정 (각 보유기간별)
        for hold in HOLD_DAYS_LIST:
            key = f'수익률_{hold}일'
            ret = record.get(key)
            if ret is not None:
                record[f'승패_{hold}일'] = '승' if ret >= PROFIT_TARGET else ('손절' if ret <= STOP_LOSS else '패')
            else:
                record[f'승패_{hold}일'] = 'N/A'

        return record

    except Exception:
        return None


# =============================================================
# 🏭 KRX 종목 로드
# =============================================================

def load_krx_tickers(min_marcap: int = 1_000_000_000) -> dict:
    """
    KRX 코스피/코스닥 전체 종목 코드→이름 딕셔너리 반환.
    ETF/ETN/스팩 등 제외.
    """
    try:
        from main7 import load_krx_listing_safe
        df = load_krx_listing_safe()
    except Exception:
        df = stock.get_market_ticker_list(market='ALL')
        df = pd.DataFrame({'Code': df})
        df['Name'] = df['Code'].apply(lambda c: stock.get_market_ticker_name(c))
        df['Market'] = 'KOSPI'

    df['Code'] = df['Code'].fillna('').astype(str).str.replace('.0', '', regex=False).str.zfill(6)
    df = df[df['Market'].isin(['KOSPI', 'KOSDAQ', '코스닥', '유가'])]
    df['Name'] = df['Name'].astype(str)
    df = df[~df['Name'].str.contains('ETF|ETN|스팩|제[0-9]+호|우$|우A|우B|우C', regex=True)]

    # 시가총액 필터 (가능한 경우)
    if 'Marcap' in df.columns:
        df = df[df['Marcap'] >= min_marcap]
    elif 'Amount' in df.columns:
        df = df.sort_values('Amount', ascending=False).head(600)
    else:
        df = df.head(600)

    return dict(zip(df['Code'], df['Name']))


# =============================================================
# 🚀 메인 백테스트 실행
# =============================================================

def run_backtest(start_date: str, end_date: str,
                 hold_days: int = None,
                 freq: str = 'weekly',
                 top_n: int = 500,
                 output_csv: str = 'backtest_result.csv') -> pd.DataFrame:
    """
    start_date ~ end_date 기간 동안 매 스캔일에 신호 탐지 후
    forward return 추적.

    Parameters
    ----------
    start_date : '2024-09-01'
    end_date   : '2025-01-31'
    hold_days  : 특정 보유일만 분석할 경우 (기본: HOLD_DAYS_LIST 전체)
    freq       : 'weekly' or 'daily'
    top_n      : 거래대금 상위 N종목으로 제한
    """
    log_info(f"\n{'='*60}")
    log_info(f"📊 백테스트 시작: {start_date} ~ {end_date}")
    log_info(f"   스캔 주기: {freq} | 보유일: {HOLD_DAYS_LIST} | 종목수: {top_n}")
    log_info(f"{'='*60}")

    # 스캔 날짜 목록
    scan_dates = get_trading_dates(start_date, end_date, freq=freq)
    log_info(f"📅 총 스캔일: {len(scan_dates)}개")

    # 종목 목록
    ticker_dict = load_krx_tickers()
    # 거래대금 상위 top_n으로 제한 (시간 절약)
    tickers_list = list(ticker_dict.items())[:top_n]
    log_info(f"🔭 대상 종목: {len(tickers_list)}개")

    all_records = []

    for scan_date in scan_dates:
        # 날짜별 진행은 히트 결과와 함께 출력
        date_records = []

        # 병렬 처리
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_map = {
                executor.submit(analyze_on_date, code, name, scan_date): (code, name)
                for code, name in tickers_list
            }
            for future in as_completed(future_map, timeout=30 * len(tickers_list)):
                try:
                    rec = future.result(timeout=20)
                    if rec:
                        date_records.append(rec)
                except Exception:
                    pass

        log_scan_date(scan_date, len(date_records))
        all_records.extend(date_records)

    if not all_records:
        log_error("⚠️ 결과 없음")
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    df.to_csv(output_csv, index=False, encoding='utf-8-sig')
    log_info(f"✅ 원본 결과 저장: {output_csv}  ({len(df)}건)")
    return df


# =============================================================
# 📈 패턴별 승률 집계
# =============================================================

def analyze_pattern_winrate(df: pd.DataFrame) -> pd.DataFrame:
    """
    N조합 패턴별 보유일별 승률, 평균수익률, MFE/MAE 집계.
    """
    rows = []

    for pattern, grp in df.groupby(PATTERN_COL):
        n_total = len(grp)
        if n_total < 3:   # 샘플 너무 적으면 제외
            continue

        row_data = {
            '패턴':   pattern,
            '총건수': n_total,
            '평균N점수': round(grp['N점수'].mean(), 1),
            'MFE평균%':  round(grp['최고점%'].mean(), 2),   # Max Favorable Excursion
            'MAE평균%':  round(grp['최저점%'].mean(), 2),   # Max Adverse Excursion
            '손절발동률': round(
                grp['손절발동일'].notna().sum() / n_total * 100, 1
            ),
        }

        for hold in HOLD_DAYS_LIST:
            ret_col  = f'수익률_{hold}일'
            win_col  = f'승패_{hold}일'
            valid    = grp[grp[win_col] != 'N/A']
            if valid.empty:
                row_data[f'승률_{hold}일%']    = None
                row_data[f'평균수익_{hold}일%'] = None
                continue

            win_rate  = (valid[win_col] == '승').sum() / len(valid) * 100
            avg_ret   = valid[ret_col].mean()
            row_data[f'승률_{hold}일%']    = round(win_rate, 1)
            row_data[f'평균수익_{hold}일%'] = round(avg_ret, 2)

        rows.append(row_data)

    summary = pd.DataFrame(rows)
    if summary.empty:
        return summary

    # 10일 승률 기준으로 정렬 (없으면 5일)
    sort_col = '승률_10일%' if '승률_10일%' in summary.columns else '승률_5일%'
    summary = summary.sort_values(sort_col, ascending=False).reset_index(drop=True)
    return summary


# =============================================================
# 📊 Stage 별 승률 집계
# =============================================================

def analyze_stage_winrate(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for stage, grp in df.groupby('단계상태'):
        n_total = len(grp)
        if n_total < 2:
            continue
        row_data = {'단계': stage, '건수': n_total}
        for hold in HOLD_DAYS_LIST:
            win_col = f'승패_{hold}일'
            ret_col = f'수익률_{hold}일'
            valid = grp[grp[win_col] != 'N/A']
            if valid.empty:
                row_data[f'승률_{hold}일%'] = None
                continue
            row_data[f'승률_{hold}일%'] = round(
                (valid[win_col] == '승').sum() / len(valid) * 100, 1
            )
            row_data[f'평균수익_{hold}일%'] = round(valid[ret_col].mean(), 2)
        rows.append(row_data)
    return pd.DataFrame(rows)


# =============================================================
# 📅 시간대별 승률 (월별)
# =============================================================

def analyze_monthly_winrate(df: pd.DataFrame, hold_col: str = '수익률_10일') -> pd.DataFrame:
    if '스캔일' not in df.columns:
        return pd.DataFrame()
    df = df.copy()
    df['월'] = pd.to_datetime(df['스캔일']).dt.to_period('M').astype(str)
    rows = []
    hold_win_col = hold_col.replace('수익률_', '승패_')
    for month, grp in df.groupby('월'):
        valid = grp[grp[hold_win_col] != 'N/A']
        if valid.empty:
            continue
        rows.append({
            '월': month,
            '건수': len(grp),
            '승률%': round((valid[hold_win_col] == '승').sum() / len(valid) * 100, 1),
            '평균수익%': round(valid[hold_col].mean(), 2),
        })
    return pd.DataFrame(rows)


# =============================================================
# 🖥️ 콘솔 출력 리포트
# =============================================================

def print_report(df: pd.DataFrame):
    log_info("\n" + "="*60)
    log_info("📊 [백테스트 결과 요약]")
    log_info("="*60)
    log_info(f"총 신호 건수: {len(df)}")
    log_info(f"기간: {df['스캔일'].min()} ~ {df['스캔일'].max()}")
    log_info(f"평균 최고점(MFE): {df['최고점%'].mean():.2f}%")
    log_info(f"평균 최저점(MAE): {df['최저점%'].mean():.2f}%")

    for hold in HOLD_DAYS_LIST:
        ret_col = f'수익률_{hold}일'
        win_col = f'승패_{hold}일'
        valid = df[df[win_col] != 'N/A']
        if valid.empty:
            continue
        wr = (valid[win_col] == '승').sum() / len(valid) * 100
        avg = valid[ret_col].mean()
        log_info(f"  {hold}일 보유: 승률 {wr:.1f}% | 평균수익 {avg:.2f}%")

    log_info("\n📌 [패턴별 Top10 승률 — 10일 기준]")
    summary = analyze_pattern_winrate(df)
    if not summary.empty:
        cols = ['패턴', '총건수', '승률_10일%', '평균수익_10일%', 'MFE평균%', 'MAE평균%']
        cols = [c for c in cols if c in summary.columns]
        log_info(summary[cols].head(10).to_string(index=False))

    log_info("\n📌 [Stage별 승률]")
    stage_summary = analyze_stage_winrate(df)
    if not stage_summary.empty:
        log_info(stage_summary.to_string(index=False))


# =============================================================
# 🚀 CLI 엔트리포인트
# =============================================================

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='backtest_validator')
    parser.add_argument('--start',     default='2024-09-01', help='시작일 YYYY-MM-DD')
    parser.add_argument('--end',       default='2025-01-31', help='종료일 YYYY-MM-DD')
    parser.add_argument('--freq',      default='daily',      help='weekly or daily  (default: daily — 급등 초동 포착)')
    parser.add_argument('--top_n',     default=500, type=int,help='종목 수')
    parser.add_argument('--no_sheet',  action='store_true',  help='구글시트 업로드 생략')
    args = parser.parse_args()

    # 1) 백테스트 실행
    result_df = run_backtest(
        start_date=args.start,
        end_date=args.end,
        freq=args.freq,
        top_n=args.top_n,
        output_csv='backtest_result.csv',
    )

    if result_df.empty:
        log_error("결과 없음 종료")
        raise SystemExit(0)

    # 2) 집계 CSV 저장
    pattern_df  = analyze_pattern_winrate(result_df)
    stage_df    = analyze_stage_winrate(result_df)
    monthly_df  = analyze_monthly_winrate(result_df)

    pattern_df.to_csv('backtest_pattern_winrate.csv', index=False, encoding='utf-8-sig')
    stage_df.to_csv('backtest_stage_winrate.csv',     index=False, encoding='utf-8-sig')
    monthly_df.to_csv('backtest_monthly.csv',         index=False, encoding='utf-8-sig')

    # 3) 콘솔 리포트
    print_report(result_df)

    # 4) 구글시트 업로드
    if not args.no_sheet:
        from backtest_sheet_uploader import upload_backtest_to_sheet
        upload_backtest_to_sheet(result_df, pattern_df, stage_df, monthly_df,
                                  start=args.start, end=args.end)
