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

import sys
import subprocess
import re
from datetime import datetime, timezone
import time

# 1. 필수 라이브러리 자동 설치 및 임포트
def install_and_import():
    required = ['feedparser', 'requests', 'beautifulsoup4']
    for lib in required:
        try:
            __import__(lib if lib != 'beautifulsoup4' else 'bs4')
        except ImportError:
            print(f"🚀 라이브러리 설치 중: {lib}...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", lib])

install_and_import()

import unicodedata
import requests
from bs4 import BeautifulSoup
import feedparser


# ───────────────────────────────────────────────
# 텍스트 정제 유틸
# ───────────────────────────────────────────────
def clean_keyword(text: str) -> str:
    """
    테마/업종 텍스트에서 깨진 문자·숫자·특수문자 제거.
    예) 'HBM(���뿪����..' → 'HBM'  /  '63.67배' → ''
    """
    # NFC 정규화
    text = unicodedata.normalize('NFC', text.strip())
    # 숫자로 시작하거나 숫자+단위(배, %, 원)만 있는 토큰 → 빈 문자열
    if re.match(r'^[\d.,]+', text):
        return ''
    # 깨진 바이트 대체문자(U+FFFD) 및 특수문자 제거
    text = re.sub(r'[\uFFFD\x00-\x1F]', '', text)
    # 괄호 뒤 깨진 부분 제거: 'HBM(깨짐..' → 'HBM'
    text = re.sub(r'[(\[].{0,10}$', '', text).strip()
    # 한글·영문·숫자·공백만 남기기
    text = re.sub(r'[^\w\s가-힣a-zA-Z0-9]', '', text).strip()
    # 2글자 미만 단어는 의미 없음
    return text if len(text) >= 2 else ''


# ───────────────────────────────────────────────
# [Step 1] 네이버 금융 → 업종 / 테마 동적 추출
# ───────────────────────────────────────────────
def fetch_naver_themes(stock_name: str, stock_code: str) -> tuple[str, list[str]]:
    """네이버 금융에서 업종명과 테마 리스트를 반환."""
    url = f"https://finance.naver.com/item/main.naver?code={stock_code}"
    headers = {
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/120.0.0.0 Safari/537.36'
        )
    }

    try:
        res = requests.get(url, headers=headers, timeout=10)
        # bytes를 직접 넘겨 BS4가 HTML meta charset 기준으로 디코딩
        soup = BeautifulSoup(res.content, 'html.parser')
        

        # ── 업종 추출 ──
        # td.td_industry > a 구조가 가장 안전 (PER 등 숫자 셀 회피)
        industry = "기타"
        industry_tag = soup.find('th', string=re.compile(r'^\s*업종\s*$'))
        if industry_tag:
            td = industry_tag.find_next_sibling('td')
            if td:
                a_tag = td.find('a')
                raw = a_tag.text.strip() if a_tag else td.text.strip()
                cleaned = clean_keyword(raw)
                print(f"  [디버그] 업종 raw값: '{raw}' → 정제: '{cleaned}'")

                if cleaned:
                    industry = cleaned

        # ── 테마 추출 (상위 3개 후보 → 정제 후 유효한 것만) ──
        theme_links = soup.find_all(
            'a', href=re.compile(r"sise_group_detail\.naver\?type=theme")
        )
        themes = []
        for link in theme_links[:5]:
            t = clean_keyword(link.text)
            if t and t not in themes:
                themes.append(t)
            if len(themes) == 2:
                break

        print(f"🏭 업종: {industry}  |  🎯 테마: {themes}")
        return industry, themes

    except Exception as e:
        print(f"⚠️ 테마 추출 실패(기본값 사용): {e}")
        return "기타", []


# ───────────────────────────────────────────────
# [Step 2] 구글 뉴스 RSS 수집
# ───────────────────────────────────────────────
def build_query(stock_name: str, themes: list[str]) -> str:
    """
    종목명을 필수 조건(따옴표)으로, 테마는 OR 확장 조건으로 구성.
    → AND 과잉 필터로 뉴스가 0건 되는 문제 방지.
    """
    if themes:
        theme_part = " OR ".join(themes)
        return f'"{stock_name}" ({theme_part})'
    return f'"{stock_name}"'


def fetch_rss(query: str, max_entries: int = 20) -> list:
    rss_url = (
        f"https://news.google.com/rss/search"
        f"?q={requests.utils.quote(query)}&hl=ko&gl=KR&ceid=KR:ko"
    )
    feed = feedparser.parse(rss_url)
    return feed.entries[:max_entries]


# ───────────────────────────────────────────────
# [Step 3] 뉴스 검증 + 스코어링
# ───────────────────────────────────────────────
POS_WORDS = ['상승', '돌파', '수주', '계약', '흑자', '최대', '강세',
             '공급', 'HBM', 'AI', '독점', '신기술', '수출', '호실적']
NEG_WORDS = ['하락', '급락', '적자', '취소', '감소', '부진',
             '유상증자', '소송', '논란', '경고', '위기']
NOISE_WORDS = ['카톡', '리딩', '무료체험', '종목추천', '찌라시', '광고']

CUTOFF_DAYS = 3  # 이 일수 이상 된 뉴스는 제외


def is_recent(entry) -> bool:
    """published_parsed 기준 CUTOFF_DAYS 이내인지 확인."""
    try:
        pub = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
        age = (datetime.now(timezone.utc) - pub).days
        return age <= CUTOFF_DAYS
    except Exception:
        return True  # 날짜 파싱 실패 시 통과


def score_title(title: str) -> tuple[int, list[str]]:
    """
    긍정/부정 키워드가 공존하면 상쇄(0점) 처리.
    단독 긍정 → +15, 단독 부정 → -20.
    """
    pos_hits = [pw for pw in POS_WORDS if pw in title]
    neg_hits = [nw for nw in NEG_WORDS if nw in title]

    if pos_hits and not neg_hits:
        return 15, pos_hits          # 순수 긍정
    elif neg_hits and not pos_hits:
        return -20, []               # 순수 부정
    elif pos_hits and neg_hits:
        return 0, []                 # 혼재 → 중립
    return 0, []


def deduplicate(entries: list) -> list:
    """유사 제목 중복 제거 (앞 20자 기준)."""
    seen_titles = set()
    unique = []
    for e in entries:
        key = e.title[:20]
        if key not in seen_titles:
            seen_titles.add(key)
            unique.append(e)
    return unique


# ───────────────────────────────────────────────
# [메인] 통합 분석 함수
# ───────────────────────────────────────────────
def get_dynamic_analysis(stock_name: str, stock_code: str) -> dict:
    print(f"\n🔍 {stock_name}({stock_code}) 분석 시작...")

    # Step 1 — 테마/업종
    clean_ind, themes = fetch_naver_themes(stock_name, stock_code)

    # 순서 유지 dedup
    raw_keywords = [stock_name] + themes + [clean_ind]
    seen = set()
    base_keywords = [x for x in raw_keywords if not (x in seen or seen.add(x))]

    # Step 2 — RSS 수집
    query = build_query(stock_name, themes)
    print(f"🔎 검색 쿼리: {query}")
    entries = fetch_rss(query, max_entries=20)
    entries = deduplicate(entries)

    # Step 3 — 필터링 + 스코어링
    score = 0
    valid_news_count = 0
    filtered_noise = 0
    filtered_old = 0
    filtered_irrelevant = 0
    matched_tags: list[str] = []
    signals = {'positive': False, 'negative': False, 'is_main_subject': False}

    print(f"\n--- [{stock_name}] 검증된 최신 뉴스 ---")

    for entry in entries:
        title = entry.title

        # [A] 종목명 미포함 → 제외
        # 2글자 이하(서남 등)는 다른 단어 안에 포함될 수 있어 단어 경계 체크
        if len(stock_name) <= 2:
            pattern = r'(?<![가-힣a-zA-Z])' + re.escape(stock_name) + r'(?![가-힣a-zA-Z])'
            if not re.search(pattern, title):
                filtered_irrelevant += 1
                continue
        else:
            if stock_name not in title:
                filtered_irrelevant += 1
                continue

        # [B] 광고/스팸 → 제외
        if any(nw in title for nw in NOISE_WORDS):
            filtered_noise += 1
            continue

        # [C] 오래된 뉴스 → 제외
        if not is_recent(entry):
            filtered_old += 1
            continue

        valid_news_count += 1
        signals['is_main_subject'] = True

        s, tags = score_title(title)
        score += s
        if s > 0:
            signals['positive'] = True
            for t in tags:
                if t not in matched_tags:
                    matched_tags.append(t)
        elif s < 0:
            signals['negative'] = True

        # 날짜 표시
        try:
            pub_str = time.strftime("%m/%d", entry.published_parsed)
        except Exception:
            pub_str = "??/??"

        mark = "📈" if s > 0 else ("📉" if s < 0 else "➖")
        print(f"  {mark} [{pub_str}] {title}  ({s:+d}점)")

    # 필터링 통계 출력
    print(f"\n  [필터 통계] 무관: {filtered_irrelevant}건 | "
          f"스팸: {filtered_noise}건 | 오래됨({CUTOFF_DAYS}일↑): {filtered_old}건")

    # Step 4 — 등급 산정
    if valid_news_count == 0:
        grade = 'C (무소식)'
    elif score >= 60:
        grade = 'S'
    elif score >= 30:
        grade = 'A'
    elif score >= 0:
        grade = 'B'
    else:
        grade = 'D'

    return {
        'stock': stock_name,
        'code': stock_code,
        'score': score,
        'grade': grade,
        'themes': base_keywords,
        'tags': matched_tags,
        'news_count': valid_news_count,
        'signals': signals,
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
        print("\n" + "=" * 50)
    print(f"📊 {result['stock']} 수박 통합 분석 보고")
    print(f"▶ 최종 등급 : {result['grade']} ({result['score']}점)")
    print(f"▶ 핵심 테마 : {', '.join(result['themes'])}")
    print(f"▶ 감지 호재 : {result['tags'] if result['tags'] else '없음'}")
    print(f"▶ 유효 뉴스 : {result['news_count']}건")
    print(f"▶ 시그널    : 긍정={result['signals']['positive']} / "
          f"부정={result['signals']['negative']}")
    print("=" * 50)
