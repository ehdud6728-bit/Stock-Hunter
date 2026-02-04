import FinanceDataReader as fdr
import pandas as pd
from datetime import datetime, timedelta

# ---------------------------------------------------------
# 🚑 단테 기법 탈락 원인 분석기 (Debug Mode)
# ---------------------------------------------------------

# 분석하고 싶은 종목들 (코드, 이름)
TARGETS = {
    '008350': '남선알미늄',
    '294630': '서남',
    '307160': '테라뷰' # (테라사이언스 등 실제 이름 확인 필요)
}

# 우리가 설정한 기준값 (main_dante.py와 동일하게)
DROP_RATE = 0.30      # 30% 하락
MA_MARGIN = 0.15      # 이평선 근처 범위 (여기를 10% -> 15%로 늘려볼 예정)
STOP_LOSS_RANGE = 40  # 40일 최저가

def diagnose_stock(code, name):
    print(f"\n💉 [진단 시작] {name} ({code})")
    
    # 2년치 데이터 가져오기
    try:
        df = fdr.DataReader(code, start=(datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d'))
    except:
        print("❌ 데이터 가져오기 실패 (상장폐지? 코드오류?)")
        return

    if len(df) < 250:
        print("❌ 데이터 부족 (신규 상장주?)")
        return
        
    row = df.iloc[-1]
    price = row['Close']
    print(f"💰 현재가: {int(price):,}원")

    # 1. 📉 고점 대비 하락률 체크
    past_high = df['High'].iloc[:-120].max() # 6개월 전 ~ 2년 전 고점
    drop_pct = (past_high - price) / past_high
    
    print(f"📉 고점({int(past_high):,}원) 대비 하락률: -{drop_pct*100:.2f}%")
    if drop_pct < DROP_RATE:
        print(f"   👉 [탈락 사유] 하락폭 부족! (기준: {DROP_RATE*100}% 이상이어야 함)")
    else:
        print(f"   ✅ 하락폭 조건 통과")

    # 2. 📊 이평선 거리 체크
    ma112 = df['Close'].rolling(112).mean().iloc[-1]
    ma224 = df['Close'].rolling(224).mean().iloc[-1]
    
    print(f"📊 112일선: {int(ma112):,}원 / 224일선: {int(ma224):,}원")
    
    # 224일선과의 거리 계산
    dist_224 = abs(price - ma224) / ma224
    print(f"   📏 224일선과의 거리: {dist_224*100:.2f}%")
    
    if dist_224 > MA_MARGIN: # 15%보다 멀면
        print(f"   👉 [탈락 사유] 이평선과 너무 멉니다. (기준: {MA_MARGIN*100}% 이내)")
    else:
        print(f"   ✅ 이평선 거리 통과")
        
    # 3. 🛡️ 손절가(공구리) 체크
    recent_low = df['Low'].iloc[-STOP_LOSS_RANGE:].min()
    risk_pct = (price - recent_low) / price * 100
    
    print(f"🛡️ 바닥 지지선(손절가): {int(recent_low):,}원 (Risk: -{risk_pct:.1f}%)")
    if risk_pct > 15.0:
        print(f"   👉 [탈락 사유] 손절가가 너무 멉니다. (손익비 나쁨, 기준 15% 이내)")
    else:
        print(f"   ✅ 손익비 조건 통과")

    # 4. 📈 거래량(Amount) 체크 (Top N에 드는지)
    amount = row['Close'] * row['Volume']
    print(f"💵 오늘 거래대금: {int(amount/100000000):,}억원")
    if amount < 1000000000: # 10억 미만이면
        print(f"   ⚠️ [주의] 거래대금이 너무 적어 TOP 600 검색에서 제외됐을 수 있음.")

if __name__ == "__main__":
    for code, name in TARGETS.items():
        diagnose_stock(code, name)