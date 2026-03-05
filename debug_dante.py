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
    print(f"🔍 {stock_name}({stock_code}) 분석 시작...")
    
    # --- [Step 1] 네이버 금융 실시간 업종/테마 추출 ---
    url = f"https://finance.naver.com/item/main.naver?code={stock_code}"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    
    try:
        res = requests.get(url, headers=headers)
        res.encoding = res.apparent_encoding # 한글 깨짐 방지 핵심!
        soup = BeautifulSoup(res.text, 'html.parser')

        # 업종 추출 (th 태그에서 '업종' 글자 찾기)
        industry_tag = soup.find('th', string=re.compile("업종"))
        industry = industry_tag.find_next_sibling('td').text.strip() if industry_tag else "반도체"
        clean_ind = industry.replace("와", " ").replace("및", " ").split()[0]

        # 테마 추출 (링크 구조 분석)
        theme_links = soup.find_all('a', href=re.compile(r"sise_group_detail\.naver\?type=theme"))
        themes = [link.text.strip() for link in theme_links[:2]] # 상위 2개 테마
        
        # 검색어 조합 (종목명 + 테마 + 업종)
        base_keywords = [stock_name] + themes + [clean_ind]
        query = "+".join(list(set(base_keywords)))
        print(f"🎯 실시간 타겟 키워드: {base_keywords}")

    except Exception as e:
        print(f"⚠️ 테마 추출 중 오류 발생(기본값 사용): {e}")
        base_keywords = [stock_name, "주식", "반도체"]
        query = "+".join(base_keywords)

    # --- [Step 2] 구글 뉴스 RSS 수집 ---
    rss_url = f"https://news.google.com/rss/search?q={query}&hl=ko&gl=KR&ceid=KR:ko"
    feed = feedparser.parse(rss_url)
    
    # --- [Step 3] 진짜 뉴스 판별 및 스코어링 ---
    pos_words = ['상승', '돌파', '수주', '계약', '흑자', '최대', '강세', '공급', 'HBM', 'AI', '독점', '신기술']
    neg_words = ['하락', '급락', '적자', '취소', '감소', '부진', '유상증자', '소송', '논란']
    noise_words = ['카톡', '리딩', '무료체험', '종목추천', '찌라시']

    score = 0
    valid_news_count = 0
    signals = {'positive': False, 'negative': False, 'is_main_subject': False}
    matched_tags = []

    print(f"\n--- [{stock_name}] 검증된 최신 뉴스 ---")
    
    for entry in feed.entries[:10]:
        title = entry.title
        
        # [검증 A] 제목에 종목명이 없으면 '남의 뉴스'일 확률 큼 (필터링)
        if stock_name not in title:
            continue
            
        # [검증 B] 광고/스팸 키워드 제거
        if any(nw in title for nw in noise_words):
            continue

        valid_news_count += 1
        signals['is_main_subject'] = True
        print(f"✅ {title}")

        # [검증 C] 문맥 기반 점수 부여
        for pw in pos_words:
            if pw in title: 
                score += 15
                if pw not in matched_tags: matched_tags.append(pw)
                signals['positive'] = True
        for nw in neg_words:
            if nw in title: 
                score -= 20
                signals['negative'] = True

    # --- [Step 4] 결과 등급 산정 ---
    # 뉴스가 하나도 없으면 C, 긍정 뉴스가 많으면 S/A
    if valid_news_count == 0:
        grade = 'C (무소식)'
    else:
        grade = 'S' if score >= 60 else 'A' if score >= 30 else 'B' if score >= 0 else 'D'

    return {
        'stock': stock_name,
        'code': stock_code,
        'score': score,
        'grade': grade,
        'themes': base_keywords,
        'tags': matched_tags,
        'news_count': valid_news_count,
        'signals': signals
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
        print(f"📊 종목 : {result['stock']}   테마 : {result['themes']} 분석 리포트")
        print(f"점수: {result['score']}점 | 등급: {result['grade']}")
        print(f"상태: {result['signals']}")
        print("="*30)
