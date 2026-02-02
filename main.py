import FinanceDataReader as fdr
import pandas as pd
import requests
import os
import time
from datetime import datetime, timedelta
import concurrent.futures
import pytz
import numpy as np

# ---------------------------------------------------------
# 🌍 설정
# ---------------------------------------------------------
KST = pytz.timezone('Asia/Seoul')
NOW = datetime.now(KST)
TODAY_STR = NOW.strftime('%Y-%m-%d')

TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID_LIST = os.environ.get('TELEGRAM_CHAT_ID', '').split(',') 
GROQ_API_KEY = os.environ.get('GROQ_API_KEY', '').strip() 

# 📊 전역 변수
FUNDAMENTALS = {} 

# ---------------------------------------------------------
# 📨 텔레그램
# ---------------------------------------------------------
def send_telegram(message):
    if not TELEGRAM_TOKEN or not CHAT_ID_LIST: return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    chunks = [message[i:i+4000] for i in range(0, len(message), 4000)]

    print(f"📨 전송 시작... 대상: {len(CHAT_ID_LIST)}명")

    # 1. 혹시 뭉쳐있는 아이디가 있다면 콤마(,)로 쪼개서 리스트를 다시 만듭니다.
    real_id_list = []
    if isinstance(CHAT_ID_LIST, list):
        for item in CHAT_ID_LIST:
            # 콤마로 쪼개고, 공백 제거해서 하나씩 추가
            real_id_list.extend([x.strip() for x in item.split(',') if x.strip()])
    else:
        # 리스트가 아니라 문자열로 넣었을 경우 대비
        real_id_list = [x.strip() for x in str(CHAT_ID_LIST).split(',') if x.strip()]

    # 2. 정리된 리스트로 전송 시작
    for chat_id in real_id_list:
        if chat_id.strip():
            print(f"✅ 전송 ! ({chat_id})")
            for chunk in chunks:
                try: 
                    response = requests.post(url, data={'chat_id': chat_id, 'text': chunk})

                    # 결과 확인
                    if res.status_code == 200:
                        print(f"✅ 성공! ({chat_id} 님에게 전송됨)")
                    else:
                        print(f"❌ 실패! (에러코드: {res.status_code})")
                        print(f"👉 텔레그램 답변: {res.json()}") # 여기가 핵심입니다!
                        
                    time.sleep(0.5) 
                except Exception as e:
                    print(f"🚨 에러 발생 ({chat_id}): {e}")
                time.sleep(0.5)
# ---------------------------------------------------------
# 🤖 AI 코멘트
# ---------------------------------------------------------
def get_ai_summary(ticker, name, score, reason):
    if not GROQ_API_KEY: return ""
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    prompt = f"종목: {name}\n점수: {score}\n특징: {reason}\n이 종목의 수급과 차트 흐름을 1줄로 분석해."
    payload = {
        "model": "llama-3.3-70b-versatile", 
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.3
    }
    try:
        res = requests.post(url, json=payload, headers=headers, timeout=3)
        return "\n💡 " + res.json()['choices'][0]['message']['content'].strip()
    except: return ""

# ---------------------------------------------------------
# ⚡ 시장 데이터 (기본)
# ---------------------------------------------------------
def get_market_data():
    print("⚡ 시장 데이터 수집 중...")
    try:
        df_krx = fdr.StockListing('KRX')
        global FUNDAMENTALS
        try:
            FUNDAMENTALS = df_krx.set_index('Code')[['Name', 'PER', 'PBR', 'EPS', 'Amount']].to_dict('index')
        except: FUNDAMENTALS = {}
        
        df_leaders = df_krx.sort_values(by='Amount', ascending=False).head(300)
        return dict(zip(df_leaders['Code'].astype(str), df_leaders['Name']))
    except: return {}

# ---------------------------------------------------------
# 🕵️ [NEW] 외인/기관 수급 크롤링 (네이버 금융)
# ---------------------------------------------------------
def get_investor_trend(code):
    """
    네이버 금융 수급 확인 (제목 줄 제거 필터 추가)
    """
    try:
        url = f"https://finance.naver.com/item/frgn.naver?code={code}"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': f'https://finance.naver.com/item/main.naver?code={code}'
        }
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        response.encoding = 'euc-kr' 
        
        dfs = pd.read_html(response.text, header=0)
        
        target_df = None
        for df in dfs:
            if '날짜' in df.columns and '외국인' in df.columns and '기관' in df.columns:
                target_df = df
                break
        
        if target_df is None:
            return False, False, "테이블못찾음"

        # 1. 결측치(NaN) 제거
        target_df = target_df.dropna()
        
        # ⭐️ [핵심 수정] "날짜" 컬럼에 "날짜"라고 적힌 제목 줄(Garbage) 제거!
        # 이 코드가 없어서 아까 '순매매량' 에러가 났던 겁니다.
        target_df = target_df[target_df['날짜'].str.contains('날짜') == False]
        
        # 2. 데이터가 없는지 재확인
        if len(target_df) < 1: 
            return False, False, "데이터없음"
            
        # 3. 가장 최근 데이터 가져오기
        latest = target_df.iloc[0]
        
        # 4. 숫자 변환 (이제 안전합니다)
        foreigner = int(str(latest['외국인']).replace(',', ''))
        institution = int(str(latest['기관']).replace(',', ''))
        
        is_for_buy = foreigner > 0
        is_ins_buy = institution > 0
        
        trend_str = ""
        if is_for_buy and is_ins_buy: trend_str = "🚀쌍끌이매수"
        elif is_for_buy: trend_str = "👨🏼‍🦰외인매수"
        elif is_ins_buy: trend_str = "🏢기관매수"
        else: trend_str = "💧개인매수"
        
        return is_for_buy, is_ins_buy, trend_str
        
    except Exception as e:
        print(f"⚠️ [{code}] 에러: {e}")
        return False, False, "크롤링실패"

# ---------------------------------------------------------
# 🏢 재무 크롤링 (실적 추세)
# ---------------------------------------------------------
def get_financial_info(code):
    """
    네이버 금융 '기업실적분석' 표에서 
    1. 이익 추이 (흑자전환, 급증 등)
    2. 기업 등급 (저평가, 성장주, 자산주 등 - PER/PBR/EPS 기반)
    두 가지 정보를 모두 가져옵니다.
    """
    # 기본값 설정
    result = {
        "trend": "보통",          # 추이 (기세)
        "badge": "⚖️보통",        # 등급 (가치)
        "eps": 0, "per": 0, "pbr": 0
    }
    
    try:
        url = f"https://finance.naver.com/item/main.naver?code={code}"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
        
        response = requests.get(url, headers=headers)
        response.encoding = 'euc-kr'
        
        # 테이블 읽기
        dfs = pd.read_html(response.text, header=0)
        
        # '기업실적분석' (또는 최근 연간 실적) 표 찾기
        fin_df = None
        for df in dfs:
            # 컬럼이나 내용에 '영업이익'이나 'PER' 등이 있는지 확인
            if '최근 연간 실적' in str(df.columns) or '주요재무제표' in str(df.columns):
                fin_df = df
                break
        
        if fin_df is None: return result

        # 인덱스 설정 (항목명으로 접근하기 위해)
        if len(fin_df.columns) > 0: 
            fin_df = fin_df.set_index(fin_df.columns[0])

        # -------------------------------------------------------
        # 1. 📈 이익 추이 분석 (기존 로직)
        # -------------------------------------------------------
        if '영업이익' in fin_df.index:
            op_row = fin_df.loc['영업이익']
            vals = []
            # 문자열을 숫자로 변환 (결측치 제외)
            for v in op_row.values:
                try: vals.append(float(str(v).replace(',', '').strip()))
                except: pass
            
            # 최근 2개 데이터 비교
            if len(vals) >= 2:
                prev = vals[-2] # 직전
                last = vals[-1] # 최근
                
                if prev < 0 and last > 0: result['trend'] = "🐢흑자전환"
                elif last > prev * 1.3: result['trend'] = "📈이익급증"
                elif last > prev: result['trend'] = "🔺이익증가"
                elif last < prev: result['trend'] = "📉이익감소"

        # -------------------------------------------------------
        # 2. 💎 기업 등급(Badge) 분석 (선생님 로직)
        # -------------------------------------------------------
        # 가장 최근 결산 데이터(보통 맨 오른쪽이나 그 앞)를 가져옵니다.
        # 안전하게 유효한 값이 있는 가장 최근 컬럼을 찾습니다.
        
        per = 0; pbr = 0; eps = 0
        
        # PER 파싱
        if 'PER(배)' in fin_df.index:
            row = fin_df.loc['PER(배)']
            for v in reversed(row.values): # 뒤에서부터 찾음
                try: 
                    per = float(str(v).replace(',', ''))
                    if not np.isnan(per): break
                except: pass
                
        # PBR 파싱
        if 'PBR(배)' in fin_df.index:
            row = fin_df.loc['PBR(배)']
            for v in reversed(row.values):
                try: 
                    pbr = float(str(v).replace(',', ''))
                    if not np.isnan(pbr): break
                except: pass
                
        # EPS 파싱 (우선순위: 지배주주순이익 -> 그냥 EPS)
        target_idx = 'EPS(원)' if 'EPS(원)' in fin_df.index else ('주당순이익' if '주당순이익' in fin_df.index else None)
        if target_idx:
            row = fin_df.loc[target_idx]
            for v in reversed(row.values):
                try: 
                    eps = float(str(v).replace(',', ''))
                    if not np.isnan(eps): break
                except: pass

        # 값 저장
        result['eps'] = eps
        result['per'] = per
        result['pbr'] = pbr

        # 🎖️ 뱃지 부여 로직 (선생님 요청 사항)
        badge = "⚖️보통"
        
        if eps < 0: 
            badge = "⚠️적자기업(주의)"
        elif (eps > 0) and (0 < per < 12) and (pbr < 1.5): 
            badge = "💎저평가우량주"
        elif (eps > 0) and (per >= 12): 
            badge = "💰고수익성장주"
        elif (pbr < 0.6) and (eps >= 0): 
            badge = "🧱헐값자산주"
            
        result['badge'] = badge

    except Exception as e:
        # print(f"재무 분석 에러: {e}")
        pass
        
    return result

# ---------------------------------------------------------
# ⚖️ 재무 등급(Badge) 판독기
# ---------------------------------------------------------
def get_financial_badge(ticker):
    info = FUNDAMENTALS.get(ticker, {})
    per = info.get('PER', 0); pbr = info.get('PBR', 0); eps = info.get('EPS', 0)
    
    if pd.isna(per): per = 0
    if pd.isna(pbr): pbr = 0
    if pd.isna(eps): eps = 0
    
    roe = 0
    if per > 0 and pbr > 0: roe = (pbr / per) * 100
        
    badge = "⚖️ 보통"
    if eps < 0: badge = "⚠️ 적자기업 (주의)"
    elif (eps > 0) and (0 < per < 12) and (pbr < 1.5): badge = "💎 저평가 우량주"
    elif (eps > 0) and (per >= 12): badge = "💰 고수익 성장주"
    elif (pbr < 0.6) and (eps >= 0): badge = "🧱 헐값 자산주"
        
    return badge, roe

# ---------------------------------------------------------
# 🧮 6대 지표
# ---------------------------------------------------------
def get_indicators(df):
    ma5 = df['Close'].rolling(5).mean()
    ma20 = df['Close'].rolling(20).mean()
    ma60 = df['Close'].rolling(60).mean()
    disparity = (df['Close'] / ma20) * 100
    
    delta = df['Close'].diff(1)
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rsi = 100 - (100 / (1 + (gain / loss)))

    high = df['High'].rolling(9).max()
    low = df['Low'].rolling(9).min()
    fast_k = ((df['Close'] - low) / (high - low)) * 100
    slow_k = fast_k.rolling(3).mean()
    slow_d = slow_k.rolling(3).mean()

    direction = df['Close'].diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    obv = (direction * df['Volume']).cumsum()
    obv_rising = obv.iloc[-1] > obv.iloc[-2]
    
    return ma5, ma20, ma60, disparity, rsi, slow_k, slow_d, obv_rising

# ---------------------------------------------------------
# 💯 점수 계산 (수급 포함!)
# ---------------------------------------------------------
def calculate_score(row, ticker, pattern_name, is_for_buy, is_ins_buy, fin_trend):
    score = 50 
    details = [] 
    
    trend = fin_info.get('trend', '보통')
    badge = fin_info.get('badge', '⚖️보통')
    
    # 1. 💰 재무 점수 (Trend + Badge)
    
    # [A] 추이 점수 (기세)
    if "흑자전환" in trend: score += 15; details.append(f"{trend}(15)")
    elif "이익급증" in trend: score += 10; details.append(f"{trend}(10)")
    elif "이익증가" in trend: score += 5; details.append(f"{trend}(5)")
    elif "이익감소" in trend: score -= 5; details.append(f"{trend}(-5)")
    
    # [B] 뱃지 점수 (가치)
    if "저평가" in badge: score += 15; details.append(f"💎저평가(15)")
    elif "성장주" in badge: score += 10; details.append(f"💰성장주(10)")
    elif "자산주" in badge: score += 10; details.append(f"🧱자산주(10)")
    elif "적자" in badge: score -= 15; details.append(f"⚠️적자(-15)")
    
    # [1] 수급 (30점)
    if is_for_buy and is_ins_buy: 
        score += 30; details.append("🚀수급(30/30)")
    elif is_for_buy: 
        score += 10; details.append("👨🏼‍🦰수급(10/30)")
    elif is_ins_buy: 
        score += 10; details.append("🏢수급(10/30)")
    else:
        details.append("수급(0/30)")

    # [2] 패턴 (30점)
    if "골파기" in pattern_name: 
        score += 30; details.append("⛏️패턴(30/30)")
    elif "숨고르기" in pattern_name: 
        score += 30; details.append("🏳️패턴(30/30)")
    elif "돌파" in pattern_name or "눌림" in pattern_name: 
        score += 15; details.append("🦁패턴(15/30)")
    else:
        details.append("패턴(0/30)")

    # [3] 지표 (40점) - row 안에 있는 데이터 사용
    # 이격도
    if 100 <= row['Disp'] <= 105: 
        score += 20; details.append("⚡이격(20/20)")
    elif row['Disp'] <= 110: 
        score += 10; details.append("⚡이격(10/20)")
    else:
        details.append("이격(0/20)")
    
    # RSI
    if row['RSI'] <= 40: 
        score += 15; details.append("📉RSI(15/15)")
    elif 40 < row['RSI'] <= 65: 
        score += 10; details.append("📉RSI(10/15)")
    else:
        details.append("RSI(0/15)")
        
    # 스토캐스틱
    if row['Stoch_K'] > row['Stoch_D']: 
        score += 5; details.append("🌊Stoch(5/5)")
    else:
        details.append("Stoch(0/5)")
    
    return score, ", ".join(details)

# ---------------------------------------------------------
# 🔍 통합 분석
# ---------------------------------------------------------
def analyze_stock(ticker, name):
    try:
        # 1. 데이터 가져오기 (200일치)
        df = fdr.DataReader(ticker, start=(NOW - timedelta(days=200)).strftime('%Y-%m-%d'))
        if len(df) < 60: return None
        
        # 2. 지표 계산 및 '통합 데이터(df)' 만들기
        # (기존 get_indicators 결과를 df에 합쳐야 'row'를 만들 수 있습니다)
        ma5, ma20, ma60, disparity, rsi, k, d, obv_rising = get_indicators(df)
        
        # ⭐️ [중요] 점수 계산기가 읽을 수 있게 df에 담아줍니다.
        df['MA20'] = ma20
        df['Disp'] = disparity
        df['RSI'] = rsi
        df['Stoch_K'] = k
        df['Stoch_D'] = d
        df['OBV_Rising'] = obv_rising
        
        # 현재봉과 전봉 정의
        curr = df.iloc[-1]   
        prev = df.iloc[-2]
        
        # 동전주 제외 (1000원 미만)
        if curr['Close'] < 1000: return None
        
        # 🛑 공통 필터 (OBV 상승 & RSI 정상범위 & 스토캐스틱 정배열)
        if not (curr['OBV_Rising'] and (30 <= curr['RSI'] <= 75) and (curr['Stoch_K'] >= curr['Stoch_D'])):
            return None

        # 3. 🎯 전략 패턴 감지
        pct = curr['Change'] * 100
        # 거래량 비율 (전일 거래량이 0이면 0 처리)
        vol_ratio = (curr['Volume'] / prev['Volume']) if prev['Volume'] > 0 else 0
        
        strategy = "" # 패턴 이름
        
        # [패턴 1] 골파기 (20일선 깼다가 다시 복귀)
        if ((prev['Close'] < prev['MA20']) and (df['Close'].iloc[-3] > df['MA20'].iloc[-3]) and \
            (curr['Close'] > curr['MA20']) and pct > 0):
            strategy = "⛏️골파기"
            
        # [패턴 2] 숨고르기 (장대양봉 후 거래량 줄며 횡보)
        elif (prev['Change'] >= 0.10) and (curr['Volume'] < prev['Volume'] * 0.5) and (-2.0 <= pct <= 2.0):
            strategy = "🏳️숨고르기"
            
        # [패턴 3] 상승초입 (이격도 낮은 상태에서 돌파)
        elif (curr['Disp'] <= 110):
            if (vol_ratio >= 1.5) and (pct >= 1.0): strategy = "🦁돌파"
            elif (-3.0 <= pct <= 1.0) and (curr['Disp'] <= 105): strategy = "🦁눌림"
            
        # 4. 🕵️ 패턴이 발견된 놈만 '수급' 확인하러 감 (속도 향상)
        if strategy:
            is_for_buy, is_ins_buy, trend_str = get_investor_trend(ticker)
            
            # ⭐️ [핵심] 점수 계산 (row와 strategy를 넘겨줍니다!)
            # 2. ⭐️ 재무 확인 (선생님 로직 함수 호출!)
            fin_trend = get_naver_financials(ticker)
            score, score_detail = calculate_score(curr, ticker, strategy, is_for_buy, is_ins_buy,fin_trend)
            
            # 60점 미만은 과락
            if score < 60: return None
            
            # 5. 💬 결과 메시지 포장
            rank = "🥉B급"
            if score >= 90: rank = "🏆SS급"
            elif score >= 80: rank = "🥇S급"
            elif score >= 70: rank = "🥈A급"

            # AI 코멘트 (선택사항 - 기존 코드에 있다면 유지)
            ai_comment = ""
            try:
                # 80점 이상인 우등생만 AI에게 물어봐서 비용 절약
                if score >= 80: 
                    # 상세 채점표(score_detail)를 AI에게 넘겨줘서 분석하게 함
                    ai_comment = get_ai_summary(ticker, name, score, score_detail)
            except Exception as e:
                print(f"AI 에러: {e}")
                ai_comment = "" # 에러나면 그냥 빈칸으로

            price_str = format(int(curr['Close']), ',')
            
            # 최종 리턴 데이터
            return {
                "score": score,
                "msg": f"[{rank} {name} ({ticker})]\n"
                       f"📊 총점: {score}점\n"
                       f"🔎 패턴: {strategy}\n"
                       f"💰 수급: {trend_str}\n"
                       f"📝 상세: {score_detail}\n" # 👈 (30/30) 상세 점수
                       f"💵 현재가: {price_str}원 ({pct:+.2f}%)\n"
                       f"🤖 AI평: {ai_comment}" 
            }
            
    except Exception as e:
        # 에러 나면 넘어가기 (로그 찍어보면 좋음)
        # print(f"Err {name}: {e}") 
        return None
        
    return None

# ---------------------------------------------------------
# 🚨 비상용
# ---------------------------------------------------------
def get_fallback_stocks(target_dict):
    print("🚨 [비상] 결과 없음 -> 단순 상승주 추출")
    results = []
    top_tickers = list(target_dict.keys())[:50]
    for t in top_tickers:
        try:
            df = fdr.DataReader(t, start=(NOW - timedelta(days=5)).strftime('%Y-%m-%d'))
            curr = df.iloc[-1]
            pct = curr['Change'] * 100
            if pct > 0:
                name = target_dict[t]
                price_str = format(int(curr['Close']),',')
                results.append({"score": pct, "msg": f"🆘 [비상] {name} (+{pct:.2f}%)"}) 
        except: pass
    return sorted(results, key=lambda x: x['score'], reverse=True)[:10]

# ---------------------------------------------------------
# 🚀 메인 실행
# ---------------------------------------------------------
if __name__ == "__main__":
    print(f"🚀 [시스템 가동] 외인/기관 수급 분석 기능 추가")
    send_telegram(f"🚀 [기능 탑재] 이제 '외인/기관'이 샀는지까지 확인합니다!\n'🚀쌍끌이매수' 종목을 주목하세요.")

    target_dict = get_market_data()
    target_tickers = list(target_dict.keys())
    print(f"⚡ {len(target_tickers)}개 종목 정밀 분석 중...")
    
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        futures = {executor.submit(analyze_stock, t, target_dict[t]): t for t in target_tickers}
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res: results.append(res)

    if not results:
        results = get_fallback_stocks(target_dict)

    if results:
        results.sort(key=lambda x: x['score'], reverse=True)
        final_msgs = [r['msg'] for r in results[:30]]
        full_text = f"🤖 [오늘의 추천주] {TODAY_STR}\n(총 {len(results)}개 포착)\n\n" + "\n\n".join(final_msgs)
        send_telegram(full_text)
    else:
        send_telegram("💀 시장 관망 필요.")
