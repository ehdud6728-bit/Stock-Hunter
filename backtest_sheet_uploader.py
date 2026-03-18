# =============================================================
# 📤 backtest_sheet_uploader.py  (Ver 2.0)
# 백테스트 결과 → 구글시트 업로드
#
# ✅ 연결 방식: google_sheet_manager.py 와 동일
#    (oauth2client + ServiceAccountCredentials)
#
# 생성되는 탭:
#   BT_Meta      : 실행 요약 (기간, 전체 승률)
#   BT_Pattern   : 패턴(N조합)별 승률 집계
#   BT_Stage     : Stage별 승률 집계
#   BT_Monthly   : 월별 승률 추이
#   BT_Raw       : 원본 전체 레코드
# =============================================================

import os
import json
import time
import traceback
from datetime import datetime

import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials


# ─────────────────────────────────────────────
# ⚙️ 설정 (google_sheet_manager.py 와 동일)
# ─────────────────────────────────────────────
JSON_KEY_PATH = 'stock-key.json'
SHEET_NAME    = '주식자동매매일지'
SCOPE = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive'
]

HOLD_DAYS_LIST = [5, 10, 15, 20]
PROFIT_TARGET  = 5.0
STOP_LOSS      = -5.0


# =============================================================
# 🔑 인증 (google_sheet_manager._get_client_and_doc 동일)
# =============================================================

def _get_client_and_doc():
    if os.path.exists(JSON_KEY_PATH):
        creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEY_PATH, SCOPE)
    elif os.environ.get('GOOGLE_JSON_KEY'):
        key_dict = json.loads(os.environ.get('GOOGLE_JSON_KEY'))
        creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, SCOPE)
    else:
        raise Exception("❌ [Google] 인증 키를 찾을 수 없습니다.")
    client = gspread.authorize(creds)
    doc    = client.open(SHEET_NAME)
    return client, doc


def _get_or_create_worksheet(doc, tab_name, rows=5000, cols=40):
    try:
        return doc.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = doc.add_worksheet(title=tab_name, rows=rows, cols=cols)
        print(f"✅ 새 시트 생성: {tab_name}")
        return ws


# =============================================================
# 📝 DataFrame → 시트 쓰기 (google_sheet_manager._write_to_sheet 동일)
# =============================================================

def _write_to_sheet(ws, df: pd.DataFrame):
    df = df.copy().fillna('')
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].astype(str).replace('None', '').replace('nan', '')

    data = [df.columns.values.tolist()] + df.values.tolist()

    for attempt in range(3):
        try:
            ws.clear()
            time.sleep(0.3)
            ws.update('A1', data)
            return
        except gspread.exceptions.APIError as e:
            if '429' in str(e) or 'quota' in str(e).lower():
                wait = (attempt + 1) * 30
                print(f"⏳ Quota 초과, {wait}초 대기...")
                time.sleep(wait)
            else:
                raise
    raise RuntimeError(f"[{ws.title}] 업로드 3회 실패")


def _apply_header_bold(ws, n_cols: int):
    try:
        end_col = chr(64 + min(n_cols, 26))
        ws.format(f'A1:{end_col}1', {
            'textFormat':          {'bold': True},
            'backgroundColor':     {'red': 0.20, 'green': 0.40, 'blue': 0.70},
            'horizontalAlignment': 'CENTER',
        })
    except Exception:
        pass


# =============================================================
# 🏗️ 탭별 업로드
# =============================================================

def _upload_meta(doc, start, end, raw_df):
    ws   = _get_or_create_worksheet(doc, 'BT_Meta', rows=100, cols=5)
    rows = [
        ['항목', '값'],
        ['실행시각',      datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
        ['백테스트 기간', f'{start} ~ {end}'],
        ['총 신호 건수',  len(raw_df)],
        ['대상 종목 수',  raw_df['code'].nunique() if 'code' in raw_df.columns else '-'],
        ['스캔 기준일 수',raw_df['스캔일'].nunique() if '스캔일' in raw_df.columns else '-'],
        ['수익 기준(%)',  PROFIT_TARGET],
        ['손절 기준(%)',  STOP_LOSS],
        ['평균 최고점(MFE)%', round(raw_df['최고점%'].mean(), 2) if '최고점%' in raw_df.columns else '-'],
        ['평균 최저점(MAE)%', round(raw_df['최저점%'].mean(), 2) if '최저점%' in raw_df.columns else '-'],
        [],
        ['보유기간', '승률%', '평균수익%', '승리건수', '총건수'],
    ]
    for hold in HOLD_DAYS_LIST:
        win_col = f'승패_{hold}일'
        ret_col = f'수익률_{hold}일'
        if win_col not in raw_df.columns:
            continue
        valid = raw_df[raw_df[win_col] != 'N/A']
        if valid.empty:
            continue
        wins    = int((valid[win_col] == '승').sum())
        wr      = round(wins / len(valid) * 100, 1)
        avg_ret = round(valid[ret_col].mean(), 2)
        rows.append([f'{hold}일', wr, avg_ret, wins, len(valid)])

    ws.clear()
    ws.update('A1', rows)
    _apply_header_bold(ws, 5)
    print(f"  ✅ BT_Meta 업로드 완료")
    time.sleep(0.5)


def _upload_pattern(doc, pattern_df, raw_df):
    ws = _get_or_create_worksheet(doc, 'BT_Pattern')
    if pattern_df.empty:
        print("  ⚠️ BT_Pattern: 데이터 없음")
        return

    _write_to_sheet(ws, pattern_df)
    _apply_header_bold(ws, len(pattern_df.columns))

    # 오른쪽에 사례 샘플 추가
    if not raw_df.empty and 'N조합' in raw_df.columns:
        sample_rows = [['패턴', '사례일', '종목', '10일수익%']]
        for pattern in pattern_df['패턴'].head(15):
            for _, s in raw_df[raw_df['N조합'] == pattern].head(3).iterrows():
                sample_rows.append([
                    pattern,
                    str(s.get('스캔일', '')),
                    str(s.get('종목명', '')),
                    s.get('수익률_10일', ''),
                ])
        try:
            start_col = len(pattern_df.columns) + 2
            cell = gspread.utils.rowcol_to_a1(1, start_col)
            ws.update(cell, sample_rows)
        except Exception:
            pass

    print(f"  ✅ BT_Pattern 업로드: {len(pattern_df)}패턴")
    time.sleep(0.5)


def _upload_stage(doc, stage_df):
    ws = _get_or_create_worksheet(doc, 'BT_Stage', rows=20, cols=20)
    if stage_df.empty:
        print("  ⚠️ BT_Stage: 데이터 없음")
        return
    _write_to_sheet(ws, stage_df)
    _apply_header_bold(ws, len(stage_df.columns))
    print(f"  ✅ BT_Stage 업로드: {len(stage_df)}행")
    time.sleep(0.5)


def _upload_monthly(doc, monthly_df):
    ws = _get_or_create_worksheet(doc, 'BT_Monthly', rows=100, cols=10)
    if monthly_df.empty:
        print("  ⚠️ BT_Monthly: 데이터 없음")
        return
    _write_to_sheet(ws, monthly_df)
    _apply_header_bold(ws, len(monthly_df.columns))
    print(f"  ✅ BT_Monthly 업로드: {len(monthly_df)}행")
    time.sleep(0.5)


def _upload_raw(doc, raw_df):
    ws = _get_or_create_worksheet(doc, 'BT_Raw',
                                   rows=max(5000, len(raw_df) + 10), cols=45)
    if raw_df.empty:
        print("  ⚠️ BT_Raw: 데이터 없음")
        return

    priority = [
        '스캔일', '종목명', 'code', '진입가',
        'N등급', 'N조합', 'N점수', '단계상태',
        'RSI', 'BB40폭', 'MA수렴', 'OBV기울기', '이격',
        '최고점%', '최저점%', '손절발동일',
        '수익률_5일',  '수익률_10일',  '수익률_15일',  '수익률_20일',
        '승패_5일',    '승패_10일',    '승패_15일',    '승패_20일',
        'N구분', 'S1날짜', 'S2날짜', 'S3날짜', '단계랭크',
    ]
    other   = [c for c in raw_df.columns if c not in priority]
    ordered = raw_df.reindex(columns=[c for c in priority if c in raw_df.columns] + other)

    _write_to_sheet(ws, ordered)
    _apply_header_bold(ws, len(ordered.columns))
    print(f"  ✅ BT_Raw 업로드: {len(ordered)}행")


# =============================================================
# 🚀 통합 업로드 엔트리포인트
# =============================================================

def upload_backtest_to_sheet(raw_df, pattern_df, stage_df, monthly_df,
                              start='', end=''):
    """
    백테스트 결과 4종 → 기존 '주식자동매매일지' 에 BT_ 탭들로 업로드.
    google_sheet_manager 와 동일한 인증 방식 사용.
    """
    print(f"\n📤 구글시트 업로드 시작 → [{SHEET_NAME}]")
    print(f"   기간: {start} ~ {end}  |  총 {len(raw_df)}건\n")

    try:
        _, doc = _get_client_and_doc()
    except Exception as e:
        print(f"🚨 구글 인증 실패: {e}")
        return

    try:
        _upload_meta(doc, start, end, raw_df)
        _upload_pattern(doc, pattern_df, raw_df)
        _upload_stage(doc, stage_df)
        _upload_monthly(doc, monthly_df)
        _upload_raw(doc, raw_df)
    except Exception as e:
        print(f"🚨 업로드 중 오류:\n{traceback.format_exc()}")
        return

    print(f"\n✅ 백테스트 시트 업로드 완료!")
    print(f"   탭: BT_Meta | BT_Pattern | BT_Stage | BT_Monthly | BT_Raw")


# =============================================================
# 📋 CSV → 구글시트 재업로드 (단독 실행용)
# =============================================================

def upload_from_csv(raw_csv='backtest_result.csv',
                    pattern_csv='backtest_pattern_winrate.csv',
                    stage_csv='backtest_stage_winrate.csv',
                    monthly_csv='backtest_monthly.csv',
                    start='', end=''):
    def _load(path):
        if os.path.exists(path):
            return pd.read_csv(path, encoding='utf-8-sig')
        print(f"⚠️ {path} 없음 → 빈 DataFrame 사용")
        return pd.DataFrame()

    upload_backtest_to_sheet(
        raw_df=_load(raw_csv), pattern_df=_load(pattern_csv),
        stage_df=_load(stage_csv), monthly_df=_load(monthly_csv),
        start=start, end=end,
    )


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', default='', help='백테스트 시작일')
    parser.add_argument('--end',   default='', help='백테스트 종료일')
    args = parser.parse_args()
    upload_from_csv(start=args.start, end=args.end)
