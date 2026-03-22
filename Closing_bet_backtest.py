# =============================================================
# 📊 closing_bet_backtest.py — 종가배팅 백테스트
# =============================================================
# 전략 A (돌파형) / 전략 B (반등형) 과거 성과 검증
#
# 방법:
#   과거 날짜의 일봉 데이터에 종가배팅 조건 적용
#   → 다음날 시가 진입, N일 후 결과 측정
#
# 구글시트 탭:
#   CB_요약    : 전략별 승률/수익률 요약
#   CB_패턴    : 조건별 상세 성과
#   CB_월별    : 월별 승률 추이
#   CB_원본    : 전체 백테스트 레코드
#
# 실행:
#   python closing_bet_backtest.py --start 2024-01-01 --end 2024-12-31
#   python closing_bet_backtest.py --start 2024-01-01 --end 2024-12-31 --top 200
# =============================================================

import os
import sys
import json
import argparse
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import numpy as np
import FinanceDataReader as fdr

try:
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    HAS_GSPREAD = True
except ImportError:
    HAS_GSPREAD = False

try:
    from scan_logger import set_log_level, log_info, log_error, log_debug
    set_log_level('NORMAL')
except ImportError:
    def log_info(m):  print(m)
    def log_error(m): print(m)
    def log_debug(m): pass

# main7 / closing_bet_scanner import
from main7_bugfix import get_indicators
from Closing_bet_scanner import (
    _calc_envelope,
    _check_envelope_bottom,
    _calc_upper_wick_ratio,
    MIN_PRICE,
    MIN_AMOUNT,
    NEAR_HIGH20_MIN, NEAR_HIGH20_MAX,
    UPPER_WICK_MAX, VOL_MULT,
    DISPARITY_MIN, DISPARITY_MAX,
)


# =============================================================
# ⚙️ 설정
# =============================================================
HOLD_DAYS_LIST  = [1, 3, 5, 10]   # 보유 기간 (일)
PROFIT_TARGET   = 3.0              # 수익 목표 % (승 판정)
STOP_LOSS       = -3.0             # 손절 기준 %
MAX_WORKERS     = 15
TOP_N           = 300              # 분석 종목 수

JSON_KEY_PATH   = 'stock-key.json'
SHEET_NAME      = '주식자동매매일지'
SCOPE = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive'
]


# =============================================================
# 📋 종목 리스트
# =============================================================

def _get_ticker_list(top_n: int) -> list:
    log_info("📋 종목 리스트 수집...")
    try:
        df_k = fdr.StockListing('KOSPI')
        df_q = fdr.StockListing('KOSDAQ')
        df   = pd.concat([df_k, df_q], ignore_index=True)
        if df is not None and not df.empty:
            mcap_col = next((c for c in df.columns if 'cap' in c.lower()), None)
            sym_col  = next((c for c in df.columns
                             if c in ('Code','Symbol','종목코드')), None)
            if mcap_col and sym_col:
                df = df.nlargest(top_n, mcap_col)
                codes = [str(c).zfill(6) for c in df[sym_col].tolist()]
                log_info(f"  FDR: {len(codes)}개")
                return codes
    except Exception as e:
        log_error(f"FDR 실패: {e}")

    # pykrx 폴백
    try:
        from pykrx import stock as _pk
        codes = _pk.get_market_ticker_list(market='ALL')
        log_info(f"  pykrx: {len(codes[:top_n])}개")
        return list(codes[:top_n])
    except Exception as e:
        log_error(f"pykrx 실패: {e}")

    return []


# =============================================================
# 🔍 단일 날짜/종목 종가배팅 조건 체크
# =============================================================

def _check_conditions_on_date(df: pd.DataFrame, date_idx: int) -> dict | None:
    """
    특정 날짜(date_idx)의 일봉 데이터로 종가배팅 조건 체크.
    Returns: 조건 충족 시 dict, 미충족 시 None
    """
    if date_idx < 60:
        return None

    sub_df = df.iloc[:date_idx + 1].copy()
    row    = sub_df.iloc[-1]

    close  = float(row['Close'])
    open_p = float(row['Open'])
    high   = float(row['High'])
    low    = float(row['Low'])
    vol    = float(row['Volume'])

    if close < MIN_PRICE:
        return None

    amount = close * vol
    if amount < MIN_AMOUNT:
        return None

    try:
        vma20  = float(row.get('VMA20', sub_df['Volume'].rolling(20).mean().iloc[-1]) or 0)
        ma20   = float(row.get('MA20', sub_df['Close'].rolling(20).mean().iloc[-1]) or 0)
        disp   = (close / ma20 * 100) if ma20 > 0 else 100
        high20 = float(sub_df['High'].rolling(20).max().iloc[-1])
        near20 = (close / high20 * 100) if high20 > 0 else 0
        rsi    = float(row.get('RSI', 50) or 50)
        upper_wick = _calc_upper_wick_ratio(row)

        # ── 전략 A 조건
        a_cond = {
            '①전고점85~100%': NEAR_HIGH20_MIN <= near20 <= NEAR_HIGH20_MAX,
            '②윗꼬리20%이하':  upper_wick <= UPPER_WICK_MAX,
            '③거래량2배폭발':  vma20 > 0 and vol >= vma20 * VOL_MULT,
            '④양봉마감':       close >= open_p,
            '⑤이격도98~112':   DISPARITY_MIN <= disp <= DISPARITY_MAX,
            '⑥MA20위마감':     ma20 > 0 and close >= ma20,
        }
        a_passed = [k for k, v in a_cond.items() if v]
        a_score  = len(a_passed)

        # ── 전략 B 조건
        env   = _check_envelope_bottom(row, sub_df)
        env_ok = env['env20_near'] or env['env40_near']

        b_score  = 0
        b_passed = []
        if env_ok:
            obv = (sub_df['Close'].diff().apply(
                lambda x: 1 if x > 0 else (-1 if x < 0 else 0)
            ) * sub_df['Volume']).cumsum()
            obv_ma5  = obv.rolling(5).mean()
            obv_ma10 = obv.rolling(10).mean()
            obv_rising = float(obv_ma5.iloc[-1]) > float(obv_ma10.iloc[-1])

            recent5   = sub_df.tail(5)
            vma10_val = float(sub_df['Volume'].rolling(10).mean().iloc[-1])
            maejip_5d = int(((recent5['Volume'] > vma10_val) &
                             (recent5['Close'] > recent5['Open'])).sum())

            b_cond = {
                '①Env20하한2%이내':   env['env20_near'],
                '②Env40하한10%이내':  env['env40_near'],
                '③RSI40이하':         rsi <= 40,
                '④OBV매수세유입':     obv_rising,
                '⑤5일내매집봉1회↑':   maejip_5d >= 1,
            }
            b_passed = [k for k, v in b_cond.items() if v]
            b_score  = len(b_passed)

        # 유효 전략 판단
        mode = None
        if a_score >= 4 and b_score >= 3:
            mode = 'A' if a_score >= b_score else 'B'
        elif a_score >= 4:
            mode = 'A'
        elif b_score >= 3:
            mode = 'B'

        if mode is None:
            return None

        return {
            'mode':    mode,
            'a_score': a_score,
            'b_score': b_score,
            'passed':  a_passed if mode == 'A' else b_passed,
            'close':   close,
            'near20':  round(near20, 1),
            'disp':    round(disp, 1),
            'vol_ratio': round(vol / vma20, 1) if vma20 > 0 else 0,
            'upper_wick_pct': round(upper_wick * 100, 1),
            'rsi':     round(rsi, 1),
            'env20_pct': env.get('env20_pct', 0),
            'env40_pct': env.get('env40_pct', 0),
            'amount_b': round(amount / 1e8, 1),
        }
    except Exception:
        return None


# =============================================================
# 📈 단일 종목 백테스트
# =============================================================

def backtest_ticker(code: str, start: str, end: str) -> list:
    """종목 코드의 기간 내 종가배팅 신호 발생일 + 결과 계산"""
    records = []
    try:
        # 여유 기간 포함 로드
        load_start = (datetime.strptime(start, '%Y-%m-%d') - timedelta(days=120)).strftime('%Y-%m-%d')
        df_raw = fdr.DataReader(code, start=load_start, end=end)
        if df_raw is None or len(df_raw) < 80:
            return []

        # 지표 계산
        df = get_indicators(df_raw.copy())
        if df is None or df.empty:
            return []

        df = df.reset_index()
        date_col = df.columns[0] if 'Date' not in df.columns else 'Date'

        # 분석 기간 인덱스 범위
        start_dt = datetime.strptime(start, '%Y-%m-%d')
        end_dt   = datetime.strptime(end,   '%Y-%m-%d')

        for i in range(60, len(df) - max(HOLD_DAYS_LIST)):
            row_date = pd.to_datetime(df[date_col].iloc[i])
            if not (start_dt <= row_date.to_pydatetime().replace(tzinfo=None) <= end_dt):
                continue

            # 종가배팅 조건 체크
            cond = _check_conditions_on_date(df, i)
            if cond is None:
                continue

            # 진입: 다음날 시가
            entry_idx = i + 1
            entry_price = float(df['Open'].iloc[entry_idx])

            # 보유 기간별 수익률
            forward_returns = {}
            stop_day = None
            max_high_pct = 0.0
            max_low_pct  = 0.0

            for hold in HOLD_DAYS_LIST:
                future_idx = entry_idx + hold
                if future_idx >= len(df):
                    forward_returns[f'수익률_{hold}일'] = None
                    forward_returns[f'승패_{hold}일']   = 'N/A'
                    continue

                future_close = float(df['Close'].iloc[future_idx])
                ret = (future_close - entry_price) / entry_price * 100
                forward_returns[f'수익률_{hold}일'] = round(ret, 2)
                forward_returns[f'승패_{hold}일'] = (
                    '승' if ret >= PROFIT_TARGET
                    else ('손절' if ret <= STOP_LOSS else '보합')
                )

            # 최고/최저 추적 (최대 10일)
            future_range = df.iloc[entry_idx:entry_idx + 11]
            if not future_range.empty:
                max_high = future_range['High'].max()
                min_low  = future_range['Low'].min()
                max_high_pct = round((max_high - entry_price) / entry_price * 100, 2)
                max_low_pct  = round((min_low  - entry_price) / entry_price * 100, 2)

            # 손절 발동일
            for d in range(1, min(11, len(df) - entry_idx)):
                daily_low = float(df['Low'].iloc[entry_idx + d])
                if (daily_low - entry_price) / entry_price * 100 <= STOP_LOSS:
                    stop_day = d
                    break

            records.append({
                '스캔일':      row_date.strftime('%Y-%m-%d'),
                'code':        code,
                '전략':        cond['mode'],
                '충족조건':    ' '.join(cond['passed']),
                'A점수':       cond['a_score'],
                'B점수':       cond['b_score'],
                '종가':        int(cond['close']),
                '진입가':      int(entry_price),
                '거래량배율':  cond['vol_ratio'],
                '전고점%':     cond['near20'],
                '이격도':      cond['disp'],
                '윗꼬리%':     cond['upper_wick_pct'],
                'RSI':         cond['rsi'],
                'Env20%':      cond['env20_pct'],
                'Env40%':      cond['env40_pct'],
                '거래대금억':  cond['amount_b'],
                '최고점%':     max_high_pct,
                '최저점%':     max_low_pct,
                '손절발동일':  stop_day,
                **forward_returns,
            })

    except Exception as e:
        log_debug(f"  [{code}] 오류: {e}")

    return records


# =============================================================
# 📊 통계 집계
# =============================================================

def summarize(df: pd.DataFrame) -> dict:
    """전략별 / 조건별 / 월별 승률 집계"""
    results = {}

    # ── 전략별 요약
    rows = []
    for strategy in ['A', 'B']:
        grp = df[df['전략'] == strategy]
        if grp.empty:
            continue
        n = len(grp)
        row = {
            '전략': f"{'📈돌파형(A)' if strategy=='A' else '📉반등형(B)'}",
            '총건수': n,
            '평균A점수': round(grp['A점수'].mean(), 1),
            '평균B점수': round(grp['B점수'].mean(), 1),
            'MFE평균%': round(grp['최고점%'].mean(), 2),
            'MAE평균%': round(grp['최저점%'].mean(), 2),
            '손절발동률%': round(grp['손절발동일'].notna().sum() / n * 100, 1),
        }
        for hold in HOLD_DAYS_LIST:
            key = f'승패_{hold}일'
            if key in grp.columns:
                valid = grp[grp[key] != 'N/A']
                if not valid.empty:
                    win_rate = (valid[key] == '승').mean() * 100
                    avg_ret  = grp[f'수익률_{hold}일'].dropna().mean()
                    row[f'{hold}일_승률%'] = round(win_rate, 1)
                    row[f'{hold}일_평균수익%'] = round(avg_ret, 2)
        rows.append(row)
    results['전략별'] = pd.DataFrame(rows)

    # ── 월별 승률
    df['년월'] = df['스캔일'].str[:7]
    monthly = []
    for ym, grp in df.groupby('년월'):
        n = len(grp)
        row = {'년월': ym, '총건수': n,
               'A건수': (grp['전략']=='A').sum(),
               'B건수': (grp['전략']=='B').sum()}
        for hold in HOLD_DAYS_LIST:
            key = f'승패_{hold}일'
            if key in grp.columns:
                valid = grp[grp[key] != 'N/A']
                if not valid.empty:
                    row[f'{hold}일승률%'] = round((valid[key]=='승').mean()*100, 1)
        monthly.append(row)
    results['월별'] = pd.DataFrame(monthly)

    # ── 조건별 승률 (passed 조건 텍스트 기준)
    cond_rows = []
    for strategy in ['A', 'B']:
        grp = df[df['전략'] == strategy]
        for hold in HOLD_DAYS_LIST:
            key = f'승패_{hold}일'
            if key not in grp.columns: continue
            valid = grp[grp[key] != 'N/A']
            if valid.empty: continue
            cond_rows.append({
                '전략': strategy,
                '보유일': hold,
                '건수': len(valid),
                '승률%': round((valid[key]=='승').mean()*100, 1),
                '평균수익%': round(valid[f'수익률_{hold}일'].mean(), 2),
                '최대수익%': round(valid[f'수익률_{hold}일'].max(), 2),
                '최대손실%': round(valid[f'수익률_{hold}일'].min(), 2),
            })
    results['보유기간별'] = pd.DataFrame(cond_rows)

    return results


# =============================================================
# 📤 구글시트 저장
# =============================================================

def _get_gspread_client():
    if not HAS_GSPREAD:
        return None, None
    try:
        import json as _json
        if os.path.exists(JSON_KEY_PATH):
            creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEY_PATH, SCOPE)
        elif os.environ.get('GOOGLE_JSON_KEY'):
            creds = ServiceAccountCredentials.from_json_keyfile_dict(
                _json.loads(os.environ['GOOGLE_JSON_KEY']), SCOPE
            )
        else:
            log_info("⚠️ 구글시트 인증 없음")
            return None, None
        gc  = gspread.authorize(creds)
        doc = gc.open(SHEET_NAME)
        return gc, doc
    except Exception as e:
        log_error(f"구글시트 연결 실패: {e}")
        return None, None


def _upsert_tab(doc, tab_name: str, df: pd.DataFrame):
    """탭 없으면 생성, 있으면 초기화 후 업로드"""
    try:
        try:
            ws = doc.worksheet(tab_name)
            ws.clear()
        except Exception:
            ws = doc.add_worksheet(title=tab_name, rows=5000, cols=df.shape[1]+5)

        data = [df.columns.tolist()] + df.fillna('').astype(str).values.tolist()
        chunk = 500
        for i in range(0, len(data), chunk):
            ws.append_rows(data[i:i+chunk], value_input_option='RAW')
            if i + chunk < len(data):
                time.sleep(1)

        log_info(f"  ✅ [{tab_name}] {len(df)}행 저장")
    except Exception as e:
        log_error(f"  ❌ [{tab_name}] 저장 실패: {e}")


def save_to_gsheet(raw_df: pd.DataFrame, summary: dict, start: str, end: str):
    gc, doc = _get_gspread_client()
    if doc is None:
        log_info("⚠️ 구글시트 저장 생략")
        return

    log_info(f"\n📤 구글시트 저장 중... [{SHEET_NAME}]")

    # CB_요약
    meta_df = pd.DataFrame([{
        '분석기간': f"{start}~{end}",
        '저장시각': datetime.now().strftime('%Y-%m-%d %H:%M'),
        '총건수':   len(raw_df),
        'A(돌파형)': (raw_df['전략']=='A').sum(),
        'B(반등형)': (raw_df['전략']=='B').sum(),
    }])
    _upsert_tab(doc, 'CB_요약', pd.concat([meta_df, summary['전략별']], ignore_index=True))

    # CB_보유기간
    _upsert_tab(doc, 'CB_보유기간', summary['보유기간별'])

    # CB_월별
    _upsert_tab(doc, 'CB_월별', summary['월별'])

    # CB_원본
    _upsert_tab(doc, 'CB_원본', raw_df.head(5000))

    log_info("✅ 구글시트 저장 완료")
    log_info("  탭: CB_요약 | CB_보유기간 | CB_월별 | CB_원본")


# =============================================================
# 🖨️ 터미널 출력
# =============================================================

def print_results(raw_df: pd.DataFrame, summary: dict, start: str, end: str):
    print(f"\n{'='*60}")
    print(f"📊 종가배팅 백테스트 결과 ({start} ~ {end})")
    print(f"{'='*60}")
    print(f"총 신호 발생: {len(raw_df):,}건")
    print(f"  📈 돌파형(A): {(raw_df['전략']=='A').sum()}건")
    print(f"  📉 반등형(B): {(raw_df['전략']=='B').sum()}건")

    print(f"\n[전략별 성과]")
    for _, row in summary['전략별'].iterrows():
        print(f"\n  {row['전략']} (n={row['총건수']})")
        print(f"  MFE평균(최고): {row['MFE평균%']:+.2f}% | MAE평균(최저): {row['MAE평균%']:+.2f}%")
        print(f"  손절발동률: {row['손절발동률%']}%")
        for hold in HOLD_DAYS_LIST:
            wr_key  = f'{hold}일_승률%'
            ret_key = f'{hold}일_평균수익%'
            if wr_key in row:
                print(f"  {hold}일 승률: {row[wr_key]}% | 평균수익: {row[ret_key]:+.2f}%")

    print(f"\n[보유 기간별]")
    print(summary['보유기간별'].to_string(index=False))


# =============================================================
# 🚀 메인
# =============================================================

def main():
    parser = argparse.ArgumentParser(description='종가배팅 백테스트')
    parser.add_argument('--start',  default='2024-01-01', help='시작일')
    parser.add_argument('--end',    default='2024-12-31', help='종료일')
    parser.add_argument('--top',    default=TOP_N, type=int, help='분석 종목 수')
    parser.add_argument('--output', default='closing_bet_bt.csv', help='결과 CSV')
    args = parser.parse_args()

    log_info(f"\n🕯️ 종가배팅 백테스트 시작")
    log_info(f"   기간: {args.start} ~ {args.end}")
    log_info(f"   종목: {args.top}개")

    codes = _get_ticker_list(args.top)
    if not codes:
        log_error("❌ 종목 없음")
        sys.exit(1)

    log_info(f"📊 {len(codes)}개 종목 분석 중...")
    all_records = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {
            ex.submit(backtest_ticker, code, args.start, args.end): code
            for code in codes
        }
        done = 0
        for future in as_completed(futures):
            done += 1
            if done % 50 == 0:
                log_info(f"  진행: {done}/{len(codes)} | 신호: {len(all_records)}건")
            try:
                recs = future.result(timeout=60)
                all_records.extend(recs)
            except Exception:
                pass

    if not all_records:
        log_error("❌ 신호 없음")
        sys.exit(1)

    raw_df  = pd.DataFrame(all_records)
    summary = summarize(raw_df)

    # 터미널 출력
    print_results(raw_df, summary, args.start, args.end)

    # CSV 저장
    raw_df.to_csv(args.output, index=False, encoding='utf-8-sig')
    log_info(f"\n💾 CSV: {args.output}")

    # 구글시트 저장
    save_to_gsheet(raw_df, summary, args.start, args.end)


if __name__ == '__main__':
    main()
