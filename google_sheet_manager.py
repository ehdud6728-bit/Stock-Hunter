import os
import json
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import FinanceDataReader as fdr

def update_google_sheet(new_picks, today_str, tournament_report=None):
    json_key_path = 'stock-key.json'
    sheet_name = "주식자동매매일지"
    
    # 컬럼 정의 (이 순서대로 시트에 기록됩니다)
    # 💡 컬럼 추가: 'AI한줄평', 'AI토너먼트'
    cols = [
        '추천일', '기상', '종목명', '종목코드', '에너지', '안전', '점수', '매수가', 
        '현재가', '최고수익', '현재수익', '구분','N구분','N조합', '이격', '수급', 'AI한줄평', '상태', '👑등급', 'N등급', '📜서사히스토리'
    ]

    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        
        # 1. 인증 로직
        if os.path.exists(json_key_path):
            creds = ServiceAccountCredentials.from_json_keyfile_name(json_key_path, scope)
        elif os.environ.get('GOOGLE_JSON_KEY'):
            key_dict = json.loads(os.environ.get('GOOGLE_JSON_KEY'))
            creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
        else:
            print("❌ [Google] 인증 키를 찾을 수 없습니다.")
            return

        client = gspread.authorize(creds)
        doc = client.open(sheet_name)
        worksheet = doc.sheet1
        
        # 2. 기존 데이터 로드 및 전처리
        existing_data = worksheet.get_all_records()
        df_log = pd.DataFrame(existing_data)

        if df_log.empty:
            df_log = pd.DataFrame(columns=cols)
        else:
            # 기존 데이터의 컬럼 순서 및 타입 보정
            df_log = df_log.reindex(columns=cols)
            df_log['추천일'] = df_log['추천일'].astype(str)
            df_log['종목코드'] = df_log['종목코드'].astype(str).apply(lambda x: x.zfill(6))

        # 3. 신규 종목 추가 (중복 체크 포함)
        if new_picks:
            new_rows = []
            for pick in new_picks:
                new_row = {
                    '추천일': today_str,
                    '기상': pick.get('기상', '☀️'),
                    '종목명': pick.get('종목명', 'N/A'),
                    '종목코드': f"'{pick.get('code', '000000')}", 
                    '에너지': pick.get('에너지', '🔋'),
                    '안전': pick.get('안전', 0),
                    '점수': pick.get('점수', 0),
                    '매수가': pick.get('현재가', 0),
                    '현재가': pick.get('현재가', 0),
                    '현재수익': 0.0,
                    '구분': pick.get('구분', ''),
                    'N구분': pick.get('N구분', ''),
                    'N조합': pick.get('N조합', ''),
                    '이격': pick.get('이격', 0),
                    '수급': pick.get('수급', ''),
                    # 💡 AI 분석 결과 매핑
                    'AI한줄평': pick.get('ai_tip', '분석전'), 
                    '상태': '진행중',
                    '👑등급': pick.get('👑등급', ''),
                    'N등급': pick.get('N등급', ''),
                    '📜서사히스토리': pick.get('📜서사히스토리', '')
                }
                new_rows.append(new_row)
            
            if new_rows:
                df_log = pd.concat([df_log, pd.DataFrame(new_rows)], ignore_index=True)
                print(f"📝 [Google] 신규 {len(new_rows)}개 종목 리스트 추가")

        # 4. 수익률 자동 업데이트
        print("🔄 [Google] 과거 추천주 수익률 동기화 중...")
        for idx, row in df_log.iterrows():
            if str(row['상태']) == '완료' or str(row['추천일']) == today_str:
                continue

            try:
                code = str(row['종목코드']).replace("'", "").zfill(6)
                rec_date = pd.to_datetime(row['추천일'])
                
                # FDR 데이터 호출 (안정성을 위해 최근 데이터 확보)
                df_curr = fdr.DataReader(code, start=rec_date)
                
                if not df_curr.empty:
                    buy_price = float(row['매수가'])
                    if buy_price == 0: continue
                    
                    high_p = float(df_curr['High'].max())
                    low_p  = float(df_curr['Low'].min())
                    curr_p = float(df_curr['Close'].iloc[-1])
                    
                    df_log.at[idx, '현재가'] = int(curr_p)
                    df_log.at[idx, '최고수익'] = round(((high_p - buy_price) / buy_price) * 100, 2)
                    df_log.at[idx, '최저수익'] = round(((low_p - buy_price) / buy_price) * 100, 2)
                    df_log.at[idx, '현재수익'] = round(((curr_p - buy_price) / buy_price) * 100, 2)
            except Exception:
                continue

        # 5. 시트 반영 (Overwrite)
        df_log = df_log.fillna('')
        
        # 🌟 화면 확인용 깔끔한 출력!
        print("=== 📊 [사령부 최종 판독 결과] ===")
        print(df_log.to_string()) # .to_string()을 쓰면 중간에 생략 없이 표 전체를 예쁘게 보여줍니다.
        
        data_to_upload = [df_log.columns.values.tolist()] + df_log.values.tolist()

        worksheet.clear()
        worksheet.update('A1', data_to_upload) # 💡 최신 gspread 규격 적용
        print("💾 [Google] 시트 저장 및 동기화 완료!")

        # --- [탭 2: AI 전략실 (AI_Report)] ---
        if tournament_report:
            try:
                # AI_Report 탭이 있으면 가져오고, 없으면 생성
                try:
                    report_sheet = doc.worksheet("AI_Report")
                except:
                    report_sheet = doc.add_worksheet(title="AI_Report", rows="1000", cols="5")
                
                # 날짜와 리포트 내용을 새 행으로 추가 (최신 리포트가 위로 오게 하거나 아래로 쌓음)
                report_sheet.append_row([today_str, tournament_report])
                print("✅ AI_Report 탭에 분석 보고서 기록 완료")
            except Exception as e:
                print(f"⚠️ AI 리포트 기록 실패: {e}")
    except Exception as e:
        print(f"🚨 [Google] 시트 연동 중 치명적 오류: {e}")

def update_ai_briefing_sheet(briefing_result, today_str):
    import os
    import json
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials

    json_key_path = 'stock-key.json'
    sheet_name = "주식자동매매일지"
    worksheet_name = "AI_Briefing"

    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]

    # 1) 인증
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

    # 2) 클라이언트 연결
    try:
        client = gspread.authorize(creds)
        sh = client.open(sheet_name)
    except Exception as e:
        print(f"❌ [Google] 시트 열기 실패: {e}")
        return

    # 3) 워크시트 열기 / 없으면 생성
    try:
        ws = sh.worksheet(worksheet_name)
    except Exception:
        try:
            ws = sh.add_worksheet(title=worksheet_name, rows=100, cols=20)
        except Exception as e:
            print(f"❌ [Google] 워크시트 생성 실패: {e}")
            return

    # 4) 기존 내용 초기화
    try:
        ws.clear()
    except Exception as e:
        print(f"❌ [Google] 워크시트 초기화 실패: {e}")
        return

    # 5) 에러 응답 저장
    if "error" in briefing_result:
        try:
            ws.update("A1", [
                ["날짜", "상태", "메시지"],
                [today_str, "ERROR", briefing_result["error"]]
            ])
            print("⚠️ [Google] AI 브리핑 에러 내용을 시트에 기록했습니다.")
        except Exception as e:
            print(f"❌ [Google] 에러 내용 기록 실패: {e}")
        return

    # 6) 정상 응답 파싱
    try:
        mb = briefing_result.get("market_briefing", {})
        sv = briefing_result.get("sector_view", {})
        tp = briefing_result.get("top_pick", {})
        av = briefing_result.get("avoid_first", {})
        ck = briefing_result.get("today_checkpoints", [])
    except Exception as e:
        print(f"❌ [Google] 브리핑 결과 파싱 실패: {e}")
        return

    # 7) 상단 요약 저장
    rows = [
        ["날짜", today_str],
        ["시장위험도", mb.get("market_risk_score", "")],
        ["시장상태", mb.get("market_state", "")],
        ["한국장", mb.get("korea_bias", "")],
        ["매매태도", mb.get("trading_stance", "")],
        ["요약", mb.get("summary", "")],
        ["유리섹터", ", ".join(sv.get("favorable_sectors", []))],
        ["불리섹터", ", ".join(sv.get("unfavorable_sectors", []))],
        ["최우선", f"{tp.get('name','')}({tp.get('code','')}) / {tp.get('reason','')}"],
        ["후순위주의", f"{av.get('name','')}({av.get('code','')}) / {av.get('reason','')}"],
        ["체크포인트1", ck[0] if len(ck) > 0 else ""],
        ["체크포인트2", ck[1] if len(ck) > 1 else ""],
        ["체크포인트3", ck[2] if len(ck) > 2 else ""],
    ]

    try:
        ws.update("A1", rows)
    except Exception as e:
        print(f"❌ [Google] 상단 요약 저장 실패: {e}")
        return

    # 8) 하단 랭킹 저장
    ranking_header_row = 16
    ranking_rows = [[
        "rank", "name", "code", "fit_score", "action_type", "why", "risk"
    ]]

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