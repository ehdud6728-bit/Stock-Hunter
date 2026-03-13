import os
import json
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import FinanceDataReader as fdr


def _get_client_and_doc():
    """인증 공통 함수"""
    json_key_path = 'stock-key.json'
    sheet_name = "주식자동매매일지"
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
    if os.path.exists(json_key_path):
        creds = ServiceAccountCredentials.from_json_keyfile_name(json_key_path, scope)
    elif os.environ.get('GOOGLE_JSON_KEY'):
        key_dict = json.loads(os.environ.get('GOOGLE_JSON_KEY'))
        creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
    else:
        raise Exception("❌ [Google] 인증 키를 찾을 수 없습니다.")
    client = gspread.authorize(creds)
    doc = client.open(sheet_name)
    return client, doc


def _get_or_create_worksheet(doc, worksheet_name, rows=2000, cols=40):
    """시트 없으면 자동 생성"""
    try:
        return doc.worksheet(worksheet_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = doc.add_worksheet(title=worksheet_name, rows=rows, cols=cols)
        print(f"✅ 새 시트 생성: {worksheet_name}")
        return ws


# 컬럼 정의
def _build_new_rows(new_picks, today_str, is_us):
    new_rows = []
    for pick in new_picks:
        row = dict(pick)  # ✅ pick의 모든 키/값 그대로 복사

        # 추천일 / 상태 / 매수가는 없을 수 있으니 보정만
        row['추천일'] = today_str
        row['상태']   = row.get('상태', '진행중')
        row['매수가'] = row.get('매수가', row.get('현재가', 0))
        row['현재수익'] = row.get('현재수익', 0.0)
        row['AI한줄평'] = row.get('ai_tip', row.get('AI한줄평', '분석전'))

        # 종목코드 포맷 보정
        if not is_us:
            code = str(row.get('code', '000000'))
            row['종목코드'] = f"'{code.zfill(6)}"
        else:
            row['종목코드'] = row.get('code', '')

        new_rows.append(row)
    return new_rows


def _write_to_sheet(worksheet, df):
    """DataFrame → 시트에 업로드 (컬럼 자동)"""
    df = df.fillna('')

    # ✅ object 타입 컬럼만 문자열 변환 (숫자는 유지)
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].astype(str)

    data = [df.columns.values.tolist()] + df.values.tolist()
    worksheet.clear()
    worksheet.update('A1', data)


def _sync_profit(df_log, today_str, is_us):
    """누적 시트 전용: 과거 종목 수익률 동기화"""
    print(f"🔄 과거 추천주 수익률 동기화 중...")
    for idx, row in df_log.iterrows():
        if str(row['상태']) == '완료' or str(row['추천일']) == today_str:
            continue
        try:
            code = str(row['종목코드']).replace("'", "")
            if not is_us:
                code = code.zfill(6)
            rec_date = pd.to_datetime(row['추천일'])
            df_curr = fdr.DataReader(code, start=rec_date)
            if not df_curr.empty:
                buy_price = float(row['매수가'])
                if buy_price == 0:
                    continue
                high_p = float(df_curr['High'].max())
                curr_p = float(df_curr['Close'].iloc[-1])
                df_log.at[idx, '현재가']   = round(curr_p, 2) if is_us else int(curr_p)
                df_log.at[idx, '최고수익'] = round(((high_p - buy_price) / buy_price) * 100, 2)
                df_log.at[idx, '현재수익'] = round(((curr_p - buy_price) / buy_price) * 100, 2)
        except Exception:
            continue
    return df_log


# ================================================================
# ✅ update_google_sheet
#    당일 시트: 오늘 결과만 덮어쓰기
#    누적 시트: 기존 데이터 + 오늘 추가 + 수익률 동기화
# ================================================================

def update_google_sheet(new_picks, today_str, tournament_report=None, sheet_target='KR'):
    """
    sheet_target: 'KR' 또는 'US'

    생성되는 시트 탭:
      KR_당일, KR_누적  /  US_당일, US_누적
    """
    is_us = (sheet_target == 'US')
    tab_daily = f'{sheet_target}_당일'
    tab_acc   = f'{sheet_target}_누적'

    try:
        _, doc = _get_client_and_doc()

        new_rows = _build_new_rows(new_picks, today_str, is_us) if new_picks else []
        df_new   = pd.DataFrame(new_rows, columns=COLS) if new_rows else pd.DataFrame(columns=COLS)

        # ────────────────────────────────
        # [1] 당일 시트 — 오늘 결과만 덮어쓰기
        # ────────────────────────────────
        ws_daily = _get_or_create_worksheet(doc, tab_daily)
        _write_to_sheet(ws_daily, df_new)
        print(f"📅 [{tab_daily}] 당일 {len(df_new)}개 저장 완료")

        # ────────────────────────────────
        # [2] 누적 시트 — 오늘 데이터 추가 + 수익률 동기화
        # ────────────────────────────────
        ws_acc = _get_or_create_worksheet(doc, tab_acc)

        existing = ws_acc.get_all_records()
        df_acc = pd.DataFrame(existing) if existing else pd.DataFrame(columns=COLS)

        if df_acc.empty:
            df_acc = pd.DataFrame(columns=COLS)
        else:
            df_acc = pd.concat([df_acc, df_new], ignore_index=True)
# → concat이 알아서 컬럼 union 처리
            df_acc['추천일'] = df_acc['추천일'].astype(str)

        # 오늘 날짜 중복이면 덮어쓰기, 없으면 추가
        df_acc = df_acc[df_acc['추천일'] != today_str]  # 오늘 기존 행 제거
        df_acc = pd.concat([df_acc, df_new], ignore_index=True)

        # 수익률 동기화
        df_acc = _sync_profit(df_acc, today_str, is_us)

        _write_to_sheet(ws_acc, df_acc)
        print(f"📚 [{tab_acc}] 누적 총 {len(df_acc)}개 저장 완료")

        # ────────────────────────────────
        # [3] AI_Report 탭
        # ────────────────────────────────
        if tournament_report:
            try:
                report_tab = f'AI_Report_{sheet_target}'
                try:
                    report_sheet = doc.worksheet(report_tab)
                except:
                    report_sheet = doc.add_worksheet(title=report_tab, rows="1000", cols="5")
                report_sheet.append_row([today_str, tournament_report])
                print(f"✅ {report_tab} 기록 완료")
            except Exception as e:
                print(f"⚠️ AI 리포트 기록 실패: {e}")

    except Exception as e:
        print(f"🚨 [{sheet_target}] 시트 연동 중 오류: {e}")


# ================================================================
# update_ai_briefing_sheet — 기존과 동일
# ================================================================

def update_ai_briefing_sheet(briefing_result, today_str):
    json_key_path = 'stock-key.json'
    sheet_name = "주식자동매매일지"
    worksheet_name = "AI_Briefing"
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]

    try:
        if os.path.exists(json_key_path):
            creds = ServiceAccountCredentials.from_json_keyfile_name(json_key_path, scope)
        elif os.environ.get('GOOGLE_JSON_KEY'):
            key_dict = json.loads(os.environ.get('GOOGLE_JSON_KEY'))
            creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
        else:
            print("❌ [Google] 인증 키를 찾을 수 없습니다.")
            return
    except Exception as e:
        print(f"❌ [Google] 인증 객체 생성 실패: {e}")
        return

    try:
        client = gspread.authorize(creds)
        sh = client.open(sheet_name)
    except Exception as e:
        print(f"❌ [Google] 시트 열기 실패: {e}")
        return

    try:
        ws = sh.worksheet(worksheet_name)
    except Exception:
        try:
            ws = sh.add_worksheet(title=worksheet_name, rows=100, cols=20)
        except Exception as e:
            print(f"❌ [Google] 워크시트 생성 실패: {e}")
            return

    try:
        ws.clear()
    except Exception as e:
        print(f"❌ [Google] 워크시트 초기화 실패: {e}")
        return

    if "error" in briefing_result:
        try:
            ws.update("A1", [
                ["날짜", "상태", "메시지"],
                [today_str, "ERROR", briefing_result["error"]]
            ])
        except Exception as e:
            print(f"❌ [Google] 에러 내용 기록 실패: {e}")
        return

    try:
        mb = briefing_result.get("market_briefing", {})
        sv = briefing_result.get("sector_view", {})
        tp = briefing_result.get("top_pick", {})
        av = briefing_result.get("avoid_first", {})
        ck = briefing_result.get("today_checkpoints", [])
    except Exception as e:
        print(f"❌ [Google] 브리핑 결과 파싱 실패: {e}")
        return

    rows = [
        ["날짜",       today_str],
        ["시장위험도",  mb.get("market_risk_score", "")],
        ["시장상태",    mb.get("market_state", "")],
        ["한국장",      mb.get("korea_bias", "")],
        ["매매태도",    mb.get("trading_stance", "")],
        ["요약",        mb.get("summary", "")],
        ["유가영향",    mb.get("oil_impact", "")],
        ["유리섹터",    ", ".join(sv.get("favorable_sectors", []))],
        ["불리섹터",    ", ".join(sv.get("unfavorable_sectors", []))],
        ["최우선",      f"{tp.get('name','')}({tp.get('code','')}) / {tp.get('reason','')}"],
        ["후순위주의",  f"{av.get('name','')}({av.get('code','')}) / {av.get('reason','')}"],
        ["체크포인트1", ck[0] if len(ck) > 0 else ""],
        ["체크포인트2", ck[1] if len(ck) > 1 else ""],
        ["체크포인트3", ck[2] if len(ck) > 2 else ""],
    ]

    try:
        ws.update("A1", rows)
    except Exception as e:
        print(f"❌ [Google] 상단 요약 저장 실패: {e}")
        return

    ranking_header_row = 16
    ranking_rows = [["rank", "name", "code", "fit_score", "action_type", "why", "risk"]]
    try:
        for item in briefing_result.get("candidate_ranking", []):
            ranking_rows.append([
                item.get("rank", ""),
                item.get("name", ""),
                item.get("code", ""),
                item.get("fit_score", ""),
                item.get("action_type", ""),
                item.get("why", ""),
                item.get("risk", ""),
            ])
        ws.update(f"A{ranking_header_row}", ranking_rows)
    except Exception as e:
        print(f"❌ [Google] 후보 랭킹 저장 실패: {e}")
        return

    print("✅ [Google] AI_Briefing 시트 업데이트 완료")