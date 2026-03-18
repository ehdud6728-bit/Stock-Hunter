# =============================================================
# 📤 backtest_sheet_uploader.py
# 백테스트 결과 → 구글시트 4탭 업로드
#
# 탭 구성:
#   BT_Raw          : 원본 전체 레코드 (스캔일·종목·수익률·승패)
#   BT_Pattern      : 패턴별 승률 집계
#   BT_Stage        : Stage별 승률 집계
#   BT_Monthly      : 월별 승률 추이
# =============================================================

import os
import time
import traceback
from datetime import datetime

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe


# ─── 인증
SCOPES = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive',
]

def _get_client() -> gspread.Client:
    """서비스 계정 JSON 또는 환경변수 JSON으로 인증."""
    creds_path = os.environ.get('GOOGLE_CREDENTIALS_PATH', 'credentials.json')

    if os.path.exists(creds_path):
        creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    else:
        import json
        creds_json = os.environ.get('GOOGLE_CREDENTIALS_JSON', '')
        if not creds_json:
            raise RuntimeError("구글 인증 정보 없음: GOOGLE_CREDENTIALS_JSON 환경변수 설정 필요")
        creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=SCOPES)

    return gspread.authorize(creds)


def _get_or_create_sheet(spreadsheet, tab_name: str):
    """탭이 없으면 생성, 있으면 그대로 반환."""
    try:
        return spreadsheet.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=tab_name, rows=5000, cols=40)


def _safe_upload(ws, df: pd.DataFrame, batch_size: int = 500):
    """
    대용량 데이터프레임을 배치로 나눠서 업로드.
    Quota 에러 방지용 재시도 포함.
    """
    # NaN → 빈 문자열 처리 (구글시트 JSON 직렬화 오류 방지)
    df = df.fillna('').copy()

    # None 타입 컬럼 문자열 변환
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str).replace('None', '').replace('nan', '')

    # 헤더 포함 전체 업로드 (gspread_dataframe)
    for attempt in range(3):
        try:
            ws.clear()
            time.sleep(0.5)
            set_with_dataframe(ws, df, include_index=False, resize=True)
            return
        except Exception as e:
            if 'quota' in str(e).lower() or '429' in str(e):
                wait = (attempt + 1) * 30
                print(f"⏳ Quota 초과, {wait}초 대기...")
                time.sleep(wait)
            else:
                raise
    raise RuntimeError("업로드 3회 실패")


def _apply_header_format(ws, n_cols: int):
    """헤더 행 굵게 + 배경색."""
    try:
        ws.format(f'A1:{chr(64 + min(n_cols, 26))}1', {
            'textFormat': {'bold': True},
            'backgroundColor': {'red': 0.27, 'green': 0.51, 'blue': 0.71},
            'horizontalAlignment': 'CENTER',
        })
    except Exception:
        pass  # 포맷 실패해도 데이터는 유지


def _add_winrate_conditional(ws, start_row: int, col_letter: str, n_rows: int):
    """승률 컬럼에 조건부 색상 (60% 이상 녹색, 40% 이하 빨강)."""
    try:
        ws.format(f'{col_letter}{start_row}:{col_letter}{start_row + n_rows}', {
            'numberFormat': {'type': 'NUMBER', 'pattern': '0.0"%"'}
        })
    except Exception:
        pass


# =============================================================
# 🏗️ 탭별 업로드 함수
# =============================================================

def _upload_raw(spreadsheet, df: pd.DataFrame):
    ws = _get_or_create_sheet(spreadsheet, 'BT_Raw')

    # 보기 좋게 컬럼 순서 재정렬
    priority_cols = [
        '스캔일', '종목명', 'code', '진입가',
        'N등급', 'N조합', 'N점수', '단계상태',
        'RSI', 'BB40폭', 'MA수렴', 'OBV기울기', '이격',
        '최고점%', '최저점%', '손절발동일',
        '수익률_5일', '수익률_10일', '수익률_15일', '수익률_20일',
        '승패_5일',   '승패_10일',   '승패_15일',   '승패_20일',
        'N구분', 'S1날짜', 'S2날짜', 'S3날짜', '단계랭크',
    ]
    other_cols = [c for c in df.columns if c not in priority_cols]
    ordered_df = df.reindex(columns=[c for c in priority_cols if c in df.columns] + other_cols)

    _safe_upload(ws, ordered_df)
    _apply_header_format(ws, len(ordered_df.columns))
    print(f"  ✅ BT_Raw 업로드: {len(ordered_df)}행")


def _upload_pattern(spreadsheet, pattern_df: pd.DataFrame, raw_df: pd.DataFrame):
    ws = _get_or_create_sheet(spreadsheet, 'BT_Pattern')

    if pattern_df.empty:
        print("  ⚠️ BT_Pattern: 데이터 없음")
        return

    # 최근 10개 히트 샘플 패턴별로 추가
    sample_rows = []
    for pattern in pattern_df['패턴'].head(20):
        samples = raw_df[raw_df['N조합'] == pattern].head(3)
        for _, s in samples.iterrows():
            sample_rows.append({
                '패턴':   pattern,
                '사례일': s.get('스캔일', ''),
                '종목':   s.get('종목명', ''),
                '수익_10일%': s.get('수익률_10일', ''),
            })

    _safe_upload(ws, pattern_df)
    _apply_header_format(ws, len(pattern_df.columns))

    # 샘플 테이블을 패턴 집계 오른쪽에 추가
    if sample_rows:
        sample_df = pd.DataFrame(sample_rows)
        start_col = len(pattern_df.columns) + 2
        try:
            set_with_dataframe(ws, sample_df,
                               row=1, col=start_col,
                               include_index=False)
        except Exception:
            pass

    print(f"  ✅ BT_Pattern 업로드: {len(pattern_df)}패턴")


def _upload_stage(spreadsheet, stage_df: pd.DataFrame):
    ws = _get_or_create_sheet(spreadsheet, 'BT_Stage')
    if stage_df.empty:
        print("  ⚠️ BT_Stage: 데이터 없음")
        return
    _safe_upload(ws, stage_df)
    _apply_header_format(ws, len(stage_df.columns))
    print(f"  ✅ BT_Stage 업로드: {len(stage_df)}행")


def _upload_monthly(spreadsheet, monthly_df: pd.DataFrame):
    ws = _get_or_create_sheet(spreadsheet, 'BT_Monthly')
    if monthly_df.empty:
        print("  ⚠️ BT_Monthly: 데이터 없음")
        return
    _safe_upload(ws, monthly_df)
    _apply_header_format(ws, len(monthly_df.columns))
    print(f"  ✅ BT_Monthly 업로드: {len(monthly_df)}행")


def _upload_meta(spreadsheet, start: str, end: str, raw_df: pd.DataFrame):
    """메타 탭: 실행 정보 + 전체 요약 통계."""
    ws = _get_or_create_sheet(spreadsheet, 'BT_Meta')

    meta_rows = [
        ['항목', '값'],
        ['실행시각', datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
        ['백테스트 기간', f'{start} ~ {end}'],
        ['총 신호 건수', len(raw_df)],
        ['대상 종목 수', raw_df['code'].nunique() if 'code' in raw_df.columns else '-'],
        ['스캔 기준일 수', raw_df['스캔일'].nunique() if '스캔일' in raw_df.columns else '-'],
        ['평균 최고점(MFE)%', round(raw_df['최고점%'].mean(), 2) if '최고점%' in raw_df.columns else '-'],
        ['평균 최저점(MAE)%', round(raw_df['최저점%'].mean(), 2) if '최저점%' in raw_df.columns else '-'],
    ]

    # 보유일별 전체 승률
    from backtest_validator import HOLD_DAYS_LIST, PROFIT_TARGET, STOP_LOSS
    meta_rows.append(['수익 기준 (%)', PROFIT_TARGET])
    meta_rows.append(['손절 기준 (%)', STOP_LOSS])
    for hold in HOLD_DAYS_LIST:
        win_col = f'승패_{hold}일'
        ret_col = f'수익률_{hold}일'
        if win_col in raw_df.columns:
            valid = raw_df[raw_df[win_col] != 'N/A']
            if not valid.empty:
                wr  = (valid[win_col] == '승').sum() / len(valid) * 100
                avg = valid[ret_col].mean()
                meta_rows.append([f'{hold}일 보유 승률%', round(wr, 1)])
                meta_rows.append([f'{hold}일 보유 평균수익%', round(avg, 2)])

    ws.clear()
    ws.update('A1', meta_rows)
    _apply_header_format(ws, 2)
    print(f"  ✅ BT_Meta 업로드 완료")


# =============================================================
# 🚀 통합 업로드 엔트리포인트
# =============================================================

def upload_backtest_to_sheet(
    raw_df:     pd.DataFrame,
    pattern_df: pd.DataFrame,
    stage_df:   pd.DataFrame,
    monthly_df: pd.DataFrame,
    start:      str = '',
    end:        str = '',
    spreadsheet_name: str = 'BackTest_결과',
):
    """
    백테스트 4개 DataFrame → 구글시트 업로드.

    Parameters
    ----------
    raw_df         : backtest_validator.run_backtest() 결과
    pattern_df     : analyze_pattern_winrate() 결과
    stage_df       : analyze_stage_winrate() 결과
    monthly_df     : analyze_monthly_winrate() 결과
    start / end    : 기간 문자열 (메타 탭 표시용)
    spreadsheet_name : 구글시트 파일명
    """
    print(f"\n📤 구글시트 업로드 시작: [{spreadsheet_name}]")

    try:
        gc = _get_client()
    except Exception as e:
        print(f"🚨 구글 인증 실패: {e}")
        return

    # 스프레드시트 열기 (없으면 생성)
    try:
        ss = gc.open(spreadsheet_name)
        print(f"  📂 기존 시트 열기: {spreadsheet_name}")
    except gspread.exceptions.SpreadsheetNotFound:
        ss = gc.create(spreadsheet_name)
        # 서비스 계정 소유가 아닌 개인 계정으로 공유
        owner_email = os.environ.get('SHEET_OWNER_EMAIL', '')
        if owner_email:
            ss.share(owner_email, perm_type='user', role='writer')
        print(f"  📂 새 시트 생성: {spreadsheet_name}")

    try:
        _upload_meta(ss, start, end, raw_df)
        time.sleep(1)
        _upload_pattern(ss, pattern_df, raw_df)
        time.sleep(1)
        _upload_stage(ss, stage_df)
        time.sleep(1)
        _upload_monthly(ss, monthly_df)
        time.sleep(1)
        _upload_raw(ss, raw_df)    # Raw는 가장 마지막 (가장 크므로)
    except Exception as e:
        print(f"🚨 업로드 중 오류:\n{traceback.format_exc()}")
        return

    sheet_url = f"https://docs.google.com/spreadsheets/d/{ss.id}"
    print(f"\n✅ 업로드 완료!")
    print(f"🔗 시트 URL: {sheet_url}")
    return sheet_url


# =============================================================
# 📋 기존 google_sheet_manager.py 와 병행하는 래퍼
# (main7.py의 update_google_sheet 호출 방식과 동일하게 사용 가능)
# =============================================================

def update_backtest_sheet_from_csv(
    raw_csv:     str = 'backtest_result.csv',
    pattern_csv: str = 'backtest_pattern_winrate.csv',
    stage_csv:   str = 'backtest_stage_winrate.csv',
    monthly_csv: str = 'backtest_monthly.csv',
    start: str = '', end: str = '',
):
    """CSV 파일에서 읽어서 바로 구글시트 업로드."""
    import os

    dfs = {}
    for name, path in [
        ('raw', raw_csv), ('pattern', pattern_csv),
        ('stage', stage_csv), ('monthly', monthly_csv)
    ]:
        if os.path.exists(path):
            dfs[name] = pd.read_csv(path, encoding='utf-8-sig')
        else:
            print(f"⚠️ {path} 없음, 빈 DataFrame 사용")
            dfs[name] = pd.DataFrame()

    upload_backtest_to_sheet(
        raw_df=dfs['raw'],
        pattern_df=dfs['pattern'],
        stage_df=dfs['stage'],
        monthly_df=dfs['monthly'],
        start=start, end=end,
    )


if __name__ == '__main__':
    # CSV 파일 이미 있을 때 재업로드용
    update_backtest_sheet_from_csv()
