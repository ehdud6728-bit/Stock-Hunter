import FinanceDataReader as fdr
import pandas as pd
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import feedparser
import re

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

def get_dynamic_analysis(stock_name, stock_code):
    # 1. 네이버 증권에서 실시간 테마/업종 긁어오기
    url = f"https://finance.naver.com/item/main.naver?code={stock_code}"
    headers = {'User-Agent': 'Mozilla/5.0'}
    res = requests.get(url, headers=headers)
    soup = BeautifulSoup(res.text, 'html.parser')

    # 현재 시장이 부여한 테마 키워드 추출
    themes = [a.text.strip() for a in soup.select(".u_gstree_v2 .item a")]
    # 업종 키워드 추출 (예: 반도체와반도체장비 -> 반도체)
    industry = soup.select_one(".gray .p11")
    ind_keyword = industry.text.replace("와", " ").split()[0] if industry else ""
    
    # 최종 검색 키워드 조합 (종목명 + 상위 테마 2개 + 업종)
    base_keywords = [stock_name] + themes[:2] + [ind_keyword]
    query = "+".join(base_keywords)
    
    print(f"🎯 실시간 타겟 키워드: {base_keywords}")
    print(f"🚀 생성된 RSS 검색어: {query}\n")

    # 2. 구글 뉴스 RSS 분석
    rss_url = f"https://news.google.com/rss/search?q={query}&hl=ko&gl=KR&ceid=KR:ko"
    feed = feedparser.parse(rss_url)
    
    # 3. 긍정/부정 스코어링 사전
    pos_words = ['상승', '돌파', '수주', '계약', '흑자', '최대', '강세', '공급', 'HBM', 'AI', '국산화']
    neg_words = ['하락', '급락', '적자', '취소', '감소', '부진', '유상증자', '소송']

    score = 0
    signals = {'positive': False, 'negative': False, 'hbm_tagged': False}
    
    print("--- [최신 뉴스 헤드라인] ---")
    for entry in feed.entries[:8]:  # 최신 뉴스 8개 확인
        title = entry.title
        print(f"- {title}")
        
        # 점수 계산 루틴
        for pw in pos_words:
            if pw in title: 
                score += 15
                signals['positive'] = True
        for nw in neg_words:
            if nw in title: 
                score -= 20
                signals['negative'] = True
        if 'HBM' in title: signals['hbm_tagged'] = True

    # 4. 최종 결과 리턴
    grade = 'S' if score >= 80 else 'A' if score >= 40 else 'B' if score >= 0 else 'C'
    
    return {
        'stock': stock_name,
        'score': score,
        'themes': themes,
        'grade': grade,
        'signals': signals,
        'keywords_used': base_keywords
    }

def load_krx_listing_safe():
    try:
        print("📡 FDR KRX 시도...")
        df = fdr.StockListing('KRX')
        if df is None or df.empty:
            raise ValueError("빈 데이터")
        print("✅ FDR 성공")
        return df
    except Exception as e:
        print(f"⚠️ FDR 실패 → pykrx 대체 사용 ({e})")
        SHEET_ID = "13Esd11iwgzLN7opMYobQ3ee6huHs1FDEbyeb3Djnu6o"
        URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit?usp=drivesdk"

        df_krx = pd.read_csv(URL)

        df_krx.rename(columns={
               '종목코드': 'Code',
               '회사명': 'Name',
               '시장구분': 'Market'
               }, inplace=True)

        return df_krx

def diagnose_stock(code, name):
    print(f"\n💉 [진단 시작] {name} ({code})")
    
    try:
        print("📡 KRX 종목 리스트 보급 시도 중...")
        df_krx = load_krx_listing_safe()
        
        # 데이터가 정상적으로 들어왔는지 최종 검문
        if df_krx is None or df_krx.empty:
            raise ValueError("데이터가 텅 비어있습니다.")
        else:
            print("✅ [성공] KRX 종목 리스트 로드 완료.")        
    except Exception as e:
        print(f"⚠️ [보급 차단] KRX 서버 응답 없음 ({e})")
        
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
        # 실행 테스트 (덕산하이메탈)
        result = get_dynamic_analysis(name, code)

        print("\n" + "="*30)
        print(f"📊 {result['stock']} {result['themes']} 분석 리포트")
        print(f"점수: {result['score']}점 | 등급: {result['grade']}")
        print(f"상태: {result['signals']}")
        print("="*30)
