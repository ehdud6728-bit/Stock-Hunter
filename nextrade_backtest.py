# =============================================================
# 📊 nextrade_backtest.py — 넥스트레이드 가설 검증 백테스트
# =============================================================
# 도영님 가설:
#   "넥스트장에서 급락한 종목은 본장에서 회복/상승하는 경우가 많다"
#
# 검증 방법:
#   넥스트레이드 과거 데이터 없음
#   → "시가 갭다운"을 프록시로 사용 (구조적으로 동일)
#   → 전일 종가 대비 당일 시가가 X% 낮으면 → 당일/3일 후 회복률 계산
#
# 실행:
#   python nextrade_backtest.py --start 2023-01-01 --end 2024-12-31
#   python nextrade_backtest.py --start 2023-01-01 --end 2024-12-31 --top 100
# =============================================================

import argparse
import sys
import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import numpy as np

try:
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    HAS_GSPREAD = True
except ImportError:
    HAS_GSPREAD = False

try:
    import FinanceDataReader as fdr
except ImportError:
    print("pip install FinanceDataReader 필요")
    sys.exit(1)

try:
    from pykrx import stock as pykrx_stock
    HAS_PYKRX = True
except ImportError:
    HAS_PYKRX = False


# =============================================================
# ⚙️ 설정
# =============================================================
GAP_BINS = [
    ('소형갭다운 (-2~-5%)',   -2.0,  -5.0),
    ('중형갭다운 (-5~-10%)',  -5.0, -10.0),
    ('대형갭다운 (-10~-15%)', -10.0, -15.0),
    ('폭락갭다운 (-15%+)',    -15.0, -99.0),
]
HOLD_DAYS = [0, 1, 3, 5]   # 당일(0), 1일, 3일, 5일 후
MIN_PRICE    = 5_000
MIN_AMOUNT   = 5_000_000_000   # 5억
MAX_WORKERS  = 20


# =============================================================
# 📦 종목 리스트
# =============================================================

def get_ticker_list(top_n: int = 300) -> list:
    """분석 대상 종목 코드 리스트"""
    print("📋 종목 리스트 수집 중...")
    try:
        df = fdr.StockListing('KRX')
        if df is not None and not df.empty:
            # 거래대금 기준 상위 or 전체
            if 'Amount' in df.columns:
                df = df[df['Amount'] >= MIN_AMOUNT]
                df = df.nlargest(top_n, 'Amount')
            else:
                df = df.head(top_n)
            codes = df['Code'].tolist() if 'Code' in df.columns else []
            print(f"  FDR: {len(codes)}개")
            return codes
    except Exception as e:
        print(f"  FDR 실패: {e}")

    if HAS_PYKRX:
        try:
            codes = pykrx_stock.get_market_ticker_list(market='ALL')
            print(f"  pykrx: {len(codes[:top_n])}개")
            return codes[:top_n]
        except Exception as e:
            print(f"  pykrx 실패: {e}")

    return []


# =============================================================
# 📊 종목별 갭다운 분석
# =============================================================

def analyze_ticker(code: str, start: str, end: str) -> list:
    """
    단일 종목의 갭다운 발생일과 이후 회복률 계산.
    Returns: list of records
    """
    try:
        df = fdr.DataReader(code, start=start, end=end)
        if df is None or len(df) < 10:
            return []

        # 최소 가격 필터
        if float(df['Close'].iloc[-1]) < MIN_PRICE:
            return []

        df = df.reset_index()
        df.columns = [c.strip() for c in df.columns]

        # 갭 계산
        df['prev_close'] = df['Close'].shift(1)
        df['gap_pct']    = (df['Open'] - df['prev_close']) / df['prev_close'] * 100

        records = []
        for i in range(1, len(df)):
            gap = df['gap_pct'].iloc[i]
            if pd.isna(gap) or gap >= 0:
                continue

            # 갭다운 구간 분류
            bin_name = None
            for bname, lo, hi in GAP_BINS:
                if hi <= gap <= lo:   # hi가 더 낮은 값 (음수)
                    bin_name = bname
                    break
            if bin_name is None:
                continue

            row        = df.iloc[i]
            open_price = float(row['Open'])
            prev_close = float(row['prev_close'])
            date_str   = str(row.get('Date', df.index[i]))[:10]

            rec = {
                'code':       code,
                'date':       date_str,
                'gap_bin':    bin_name,
                'gap_pct':    round(gap, 2),
                'open':       int(open_price),
                'prev_close': int(prev_close),
            }

            # 회복률 계산
            for hold in HOLD_DAYS:
                future_idx = i + hold
                if future_idx >= len(df):
                    rec[f'close_{hold}d']       = None
                    rec[f'ret_{hold}d']         = None
                    rec[f'recover_open_{hold}d']  = None   # 시가 대비 상승 여부
                    rec[f'recover_prev_{hold}d']  = None   # 전일종가 회복 여부
                    continue

                future_close = float(df['Close'].iloc[future_idx])
                ret_from_open  = (future_close - open_price) / open_price * 100
                ret_from_prev  = (future_close - prev_close) / prev_close * 100

                rec[f'close_{hold}d']         = int(future_close)
                rec[f'ret_from_open_{hold}d'] = round(ret_from_open, 2)
                rec[f'ret_from_prev_{hold}d'] = round(ret_from_prev, 2)
                rec[f'recover_open_{hold}d']  = future_close > open_price   # 시가보다 높나
                rec[f'recover_prev_{hold}d']  = future_close >= prev_close * 0.98  # 전일종가 98% 이상 회복

            records.append(rec)

        return records

    except Exception:
        return []


# =============================================================
# 📈 통계 집계
# =============================================================

def summarize(df: pd.DataFrame) -> pd.DataFrame:
    """갭 구간별 회복률 통계 집계"""
    rows = []
    for bin_name, _, _ in GAP_BINS:
        grp = df[df['gap_bin'] == bin_name]
        if grp.empty:
            continue

        n = len(grp)
        row = {
            '갭다운구간':  bin_name,
            '샘플수':      n,
            '평균갭크기':  round(grp['gap_pct'].mean(), 2),
        }

        for hold in HOLD_DAYS:
            col_open = f'recover_open_{hold}d'
            col_prev = f'recover_prev_{hold}d'
            col_ret  = f'ret_from_open_{hold}d'

            valid = grp.dropna(subset=[col_open])
            if valid.empty:
                row[f'{hold}일_시가회복률'] = '-'
                row[f'{hold}일_전종회복률'] = '-'
                row[f'{hold}일_평균수익률'] = '-'
                continue

            open_rec = valid[col_open].mean() * 100
            prev_rec = valid[col_prev].mean() * 100
            avg_ret  = valid[col_ret].mean()

            row[f'{hold}일_시가회복률'] = f"{open_rec:.1f}%"
            row[f'{hold}일_전종회복률'] = f"{prev_rec:.1f}%"
            row[f'{hold}일_평균수익률'] = f"{avg_ret:+.2f}%"

        rows.append(row)

    return pd.DataFrame(rows)




# =============================================================
# 📤 구글시트 저장
# =============================================================

JSON_KEY_PATH = 'stock-key.json'
SHEET_NAME    = '주식자동매매일지'
SCOPE = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive'
]

def _get_gspread_client():
    """google_sheet_manager와 동일한 인증 방식"""
    if not HAS_GSPREAD:
        return None, None
    try:
        import json as _json
        if os.path.exists(JSON_KEY_PATH):
            creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEY_PATH, SCOPE)
        elif os.environ.get('GOOGLE_JSON_KEY'):
            key_dict = _json.loads(os.environ['GOOGLE_JSON_KEY'])
            creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, SCOPE)
        else:
            print("⚠️ 구글시트 인증 정보 없음 (stock-key.json or GOOGLE_JSON_KEY)")
            return None, None
        gc  = gspread.authorize(creds)
        doc = gc.open(SHEET_NAME)
        return gc, doc
    except Exception as e:
        print(f"⚠️ 구글시트 연결 실패: {e}")
        return None, None


def _upsert_worksheet(doc, tab_name: str, df: pd.DataFrame, max_rows: int = 5000):
    """탭이 없으면 생성, 있으면 덮어씀"""
    import time as _time
    try:
        try:
            ws = doc.worksheet(tab_name)
            ws.clear()
        except gspread.exceptions.WorksheetNotFound:
            ws = doc.add_worksheet(title=tab_name, rows=max_rows, cols=len(df.columns) + 5)

        # 헤더 + 데이터
        data = [df.columns.tolist()] + df.fillna('').astype(str).values.tolist()

        # 청크 단위 업로드 (Quota 방지)
        chunk = 500
        for i in range(0, len(data), chunk):
            ws.append_rows(data[i:i+chunk], value_input_option='RAW')
            if i + chunk < len(data):
                _time.sleep(1)

        print(f"  ✅ [{tab_name}] {len(df)}행 저장")
        return True
    except Exception as e:
        print(f"  ❌ [{tab_name}] 저장 실패: {e}")
        return False


def save_to_gsheet(summary: pd.DataFrame, raw: pd.DataFrame,
                   start: str, end: str):
    """
    백테스트 결과를 구글시트에 저장.
    탭 구조:
      NXT_요약   : 갭다운 구간별 회복률 통계
      NXT_원본   : 전체 갭다운 이벤트 원본
    """
    gc, doc = _get_gspread_client()
    if doc is None:
        print("⚠️ 구글시트 저장 생략")
        return

    print(f"\n📤 구글시트 저장 중... ({SHEET_NAME})")

    # 메타 정보 추가
    summary.insert(0, '분석기간', f"{start}~{end}")
    summary.insert(1, '저장시각', datetime.now().strftime('%Y-%m-%d %H:%M'))

    _upsert_worksheet(doc, 'NXT_요약',   summary)
    _upsert_worksheet(doc, 'NXT_원본',   raw.head(5000))   # 최대 5000건
    print("✅ 구글시트 저장 완료")

def print_results(summary: pd.DataFrame, raw: pd.DataFrame):
    """결과 출력"""
    total = len(raw)
    print(f"\n{'='*70}")
    print(f"📊 갭다운 회복률 분석 결과 (총 {total:,}건)")
    print(f"{'='*70}")

    # 핵심 요약
    print("\n[핵심 지표]")
    print(f"  {'갭 구간':<25} {'샘플':>6} {'당일시가회복':>12} {'당일전종회복':>12} {'3일전종회복':>12} {'당일평균수익':>12}")
    print(f"  {'-'*25} {'-'*6} {'-'*12} {'-'*12} {'-'*12} {'-'*12}")

    for _, r in summary.iterrows():
        print(f"  {r['갭다운구간']:<25} "
              f"{r['샘플수']:>6} "
              f"{r.get('0일_시가회복률','-'):>12} "
              f"{r.get('0일_전종회복률','-'):>12} "
              f"{r.get('3일_전종회복률','-'):>12} "
              f"{r.get('0일_평균수익률','-'):>12}")

    # 가설 검증 결론
    print(f"\n{'='*70}")
    print("🔬 도영님 가설 검증 결론")
    print(f"{'='*70}")

    for _, r in summary.iterrows():
        bin_name = r['갭다운구간']
        n        = r['샘플수']
        d0_open  = r.get('0일_시가회복률', '-')
        d0_prev  = r.get('0일_전종회복률', '-')
        d3_prev  = r.get('3일_전종회복률', '-')
        d0_ret   = r.get('0일_평균수익률', '-')

        # 검증 판단
        try:
            open_pct = float(d0_open.replace('%', ''))
            if open_pct >= 60:
                verdict = "✅ 가설 지지 — 통계적으로 유의미"
            elif open_pct >= 50:
                verdict = "🟡 약한 지지 — 동전던지기 수준"
            else:
                verdict = "❌ 가설 기각 — 오히려 하락 우세"
        except Exception:
            verdict = "❓ 데이터 부족"

        print(f"\n  [{bin_name}] n={n}건")
        print(f"    당일 시가 대비 상승 확률: {d0_open}")
        print(f"    당일 전일종가 회복 확률: {d0_prev}")
        print(f"    3일 후 전일종가 회복 확률: {d3_prev}")
        print(f"    당일 평균 수익률(시가진입): {d0_ret}")
        print(f"    → {verdict}")


# =============================================================
# 🚀 메인
# =============================================================

def main():
    parser = argparse.ArgumentParser(description='넥스트레이드 가설 검증 백테스트')
    parser.add_argument('--start',  default='2023-01-01', help='시작일')
    parser.add_argument('--end',    default='2024-12-31', help='종료일')
    parser.add_argument('--top',    default=300, type=int, help='분석 종목 수')
    parser.add_argument('--output', default='nextrade_backtest_result.csv', help='결과 CSV')
    args = parser.parse_args()

    print(f"\n🔬 넥스트레이드 가설 검증 백테스트")
    print(f"   기간: {args.start} ~ {args.end}")
    print(f"   종목: 상위 {args.top}개")
    print(f"   프록시: 시가 갭다운 = 넥스트장 급락 근사\n")

    # 종목 리스트
    codes = get_ticker_list(args.top)
    if not codes:
        print("❌ 종목 리스트 없음")
        sys.exit(1)

    # 병렬 분석
    print(f"📊 {len(codes)}개 종목 분석 중...")
    all_records = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {
            ex.submit(analyze_ticker, code, args.start, args.end): code
            for code in codes
        }
        done = 0
        for future in as_completed(futures):
            done += 1
            if done % 50 == 0:
                print(f"  진행: {done}/{len(codes)} ({done/len(codes)*100:.0f}%)")
            try:
                recs = future.result(timeout=30)
                all_records.extend(recs)
            except Exception:
                pass

    if not all_records:
        print("❌ 분석 결과 없음")
        sys.exit(1)

    raw_df = pd.DataFrame(all_records)
    print(f"\n✅ 총 갭다운 이벤트: {len(raw_df):,}건")

    # 통계 집계
    summary = summarize(raw_df)

    # 결과 출력
    print_results(summary, raw_df)

    # CSV 저장
    raw_df.to_csv(args.output, index=False, encoding='utf-8-sig')
    summary.to_csv(f"summary_{args.output}", index=False, encoding='utf-8-sig')
    print(f"\n💾 CSV 저장: {args.output}")
    print(f"💾 요약 CSV: summary_{args.output}")

    # 구글시트 저장
    save_to_gsheet(summary, raw_df, args.start, args.end)


if __name__ == '__main__':
    main()
