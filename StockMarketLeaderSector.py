import os
import io
import json
import warnings
import requests
import pandas as pd
import numpy as np
import FinanceDataReader as fdr
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from openai import OpenAI
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import pytz

warnings.filterwarnings('ignore', category=FutureWarning)

# =================================================
# ⚙️ [1. 환경 설정]
# =================================================
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID_LIST = os.environ.get('TELEGRAM_CHAT_ID', '').split(',')
GOOGLE_JSON_KEY = os.environ.get('GOOGLE_JSON_KEY')
SHEET_NAME = '나의_주식_스캐너_리포트'

KST = pytz.timezone('Asia/Seoul')
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}

# ---------------------------------------------------------
# 🔍 [2] 주도 섹터 및 테마 정보 스크래핑
# ---------------------------------------------------------
def get_leading_themes():
    """네이버 금융에서 현재 가장 핫한 상위 3개 테마와 특징을 가져옵니다."""
    try:
        url = "https://finance.naver.com/sise/theme.naver"
        res = requests.get(url, headers=HEADERS)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        themes = []
        table = soup.select('table.type_1 tr')
        for tr in table[3:8]: # 광고 제외 상위 테마
            tds = tr.select('td')
            if len(tds) > 1:
                name = tds[0].text.strip()
                change = tds[1].text.strip()
                themes.append(f"🔥 {name}({change})")
        return "\n".join(themes)
    except:
        return "테마 정보 분석 지연"

# ---------------------------------------------------------
# 🧠 [3] 종목별 재료 및 주도주 확인 AI 브리핑
# ---------------------------------------------------------
def get_stock_material_briefing(stock_list_df):
    """상위 20개 종목에 대해 AI가 상승 재료와 주도주 성격을 분석합니다."""
    if not OPENAI_API_KEY: return "AI 분석 기능을 사용할 수 없습니다."
    
    # AI에게 전달할 데이터 정리 (종목명, 거래대금, 시총)
    summary_data = ""
    for _, row in stock_list_df.iterrows():
        summary_data += f"- {row['Name']}: 거래대금 {row['Amount']//100000000:,.0f}억, 시총 {row['Marcap']//100000000:,.0f}억\n"

    client = OpenAI(api_key=OPENAI_API_KEY)
    prompt = f"""
    오늘 한국 시장 거래대금 상위 종목 데이터야:
    {summary_data}
    
    다음 정보를 바탕으로 리포트를 작성해줘:
    1. 각 종목이 오늘 왜 주목받았는지 '상승 재료(뉴스/테마)'를 한 줄로 요약해 (반말).
    2. 이 중에서 오늘 시장을 이끈 '진짜 주도주 섹터'가 무엇인지 정의해줘.
    3. 주도주 섹터 내에서 '대장주'를 선정하고 그 이유(거래대금, 시총 비중 등)를 설명해줘.
    """
    try:
        res = client.chat.completions.create(model="gpt-4o-mini", messages=[{"role":"user", "content":prompt}])
        return res.choices[0].message.content.strip()
    except:
        return "AI 주도주 분석 중 오류 발생"

# ---------------------------------------------------------
# 🚀 [4] 메인 실행부 (상위 20개 주도주 집중 분석)
# ---------------------------------------------------------
if __name__ == "__main__":
    print("🚀 주도주 및 섹터 정밀 분석 시작...")
    
    # 1. 전 종목 리스트 가져오기 및 거래대금 순 정렬
    df_krx = fdr.StockListing('KRX')
    # 거래대금(Amount) 기준 상위 20개 추출
    top_20 = df_krx.sort_values(by='Amount', ascending=False).head(20)
    
    # 2. 테마 섹터 정보 수집
    hot_themes = get_leading_themes()
    
    # 3. AI 주도주/재료 정밀 분석
    market_leader_report = get_stock_material_briefing(top_20)
    
    # 4. 텔레그램 메시지 조립
    final_report = f"📅 {datetime.now(KST).strftime('%Y-%m-%d')} 주도주 사령부 리포트\n\n"
    final_report += f"✅ [실시간 급등 테마]\n{hot_themes}\n\n"
    final_report += f"📊 [거래대금 Top 20 및 AI 재료 분석]\n{market_leader_report}"

    # 5. 텔레그램 분할 전송 (이전 로직 활용)
    MAX_CHAR = 3800
    url_t = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    
    if len(final_report) > MAX_CHAR:
        # 메시지가 길면 문단 단위로 잘라서 전송
        chunks = final_report.split('\n\n')
        current_msg = ""
        for chunk in chunks:
            if len(current_msg) + len(chunk) > MAX_CHAR:
                for chat_id in CHAT_ID_LIST:
                    requests.post(url_t, data={'chat_id': chat_id.strip(), 'text': current_msg})
                current_msg = chunk + "\n\n"
            else:
                current_msg += chunk + "\n\n"
        for chat_id in CHAT_ID_LIST:
            requests.post(url_t, data={'chat_id': chat_id.strip(), 'text': current_msg})
    else:
        for chat_id in CHAT_ID_LIST:
            requests.post(url_t, data={'chat_id': chat_id.strip(), 'text': final_report})

    print("✅ 주도주 리포트 전송 완료!")
