import FinanceDataReader as fdr
import requests
import os
from datetime import datetime, timedelta

# --- [환경변수] ---
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID_LIST = os.environ.get('TELEGRAM_CHAT_ID', '').split(',') 

# ---------------------------------------------------------
# 📨 전송 함수
# ---------------------------------------------------------
def send_telegram(message):
    print(f"📩 텔레그램 전송 시도: {message[:20]}...")
    if not TELEGRAM_TOKEN or not CHAT_ID_LIST: 
        print("❌ 토큰이나 챗ID가 없습니다.")
        return
    
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    for chat_id in CHAT_ID_LIST:
        if chat_id.strip():
            try: 
                requests.post(url, data={'chat_id': chat_id, 'text': message})
                print("✅ 전송 성공")
            except Exception as e:
                print(f"❌ 전송 실패: {e}")

# ---------------------------------------------------------
# 🏥 진단 시작
# ---------------------------------------------------------
print("🏥 [시스템 긴급 진단] 시작합니다...")
report = "🏥 [진단 리포트]\n"

# 1. 삼성전자(005930) 데이터 강제 조회
target_code = '005930'
target_name = '삼성전자'

try:
    print(f"🔍 1. {target_name} 데이터 요청 중...")
    # 최근 10일치만 요청
    df = fdr.DataReader(target_code, start=(datetime.now() - timedelta(days=20)))
    
    if df is None or df.empty:
        msg = "❌ 데이터가 비어있습니다 (Empty DataFrame). 외부 통신 차단 의심."
        print(msg)
        report += msg
    else:
        # 데이터가 잘 왔다면 내용 확인
        last_date = df.index[-1].strftime('%Y-%m-%d')
        last_price = df['Close'].iloc[-1]
        data_count = len(df)
        
        msg = f"✅ 데이터 수신 성공!\n- 마지막 날짜: {last_date}\n- 종가: {int(last_price):,}원\n- 데이터 개수: {data_count}개\n"
        print(msg)
        report += msg
        
        # 2. 지표 계산 테스트 (여기서 에러나나 확인)
        print("🧮 2. 지표 계산 테스트...")
        try:
            ma5 = df['Close'].rolling(5).mean().iloc[-1]
            report += f"✅ 이동평균선 계산 성공 (MA5: {int(ma5):,})\n"
        except Exception as e:
            report += f"❌ 지표 계산 실패: {e}\n"

except Exception as e:
    # 여기가 제일 중요합니다! 에러 내용을 그대로 봅니다.
    msg = f"❌ [치명적 에러] 데이터 요청 실패:\n{str(e)}"
    print(msg)
    report += msg

# 3. pykrx 수급 데이터 테스트 (여기가 문제일 확률 높음)
print("⚡ 3. 수급 데이터(pykrx) 테스트...")
try:
    from pykrx import stock
    # 가장 최근 평일 찾기 (오늘 or 어제)
    today = datetime.now().strftime("%Y%m%d")
    df_supply = stock.get_market_net_purchases_of_equities_by_ticker(today, "ALL", "value")
    
    if df_supply.empty:
        # 주말이거나 장 시작 전이면 비어있을 수 있음 -> 하루 전으로 재시도
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        df_supply = stock.get_market_net_purchases_of_equities_by_ticker(yesterday, "ALL", "value")
    
    if not df_supply.empty:
        samsung_net = df_supply.loc[target_code]['기관합계'] if target_code in df_supply.index else 0
        report += f"✅ 수급 데이터 수신 성공 (삼성전자 기관수급: {samsung_net:,})\n"
    else:
        report += "⚠️ 수급 데이터가 비어있습니다 (휴일 가능성)\n"

except Exception as e:
    report += f"❌ 수급 데이터(pykrx) 에러: {str(e)}\n"


# 4. 최종 보고
print("📤 결과 전송 중...")
send_telegram(report)
print("🏁 진단 종료")