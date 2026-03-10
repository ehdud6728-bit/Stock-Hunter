 #------------------------------------------------------------------
# 💎 [Ultimate Masterpiece] 전천후 AI 전략 사령부 (All-In-One 통합판)
# ------------------------------------------------------------------
import FinanceDataReader as fdr
import pandas as pd
import numpy as np
import requests
import mplfinance as mpf
import matplotlib.pyplot as plt
import os, re, time, pytz
from bs4 import BeautifulSoup
from openai import OpenAI
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from bs4 import BeautifulSoup 
import pytz
from tactics_engine import get_global_and_leader_status, analyze_all_narratives, get_dynamic_sector_leaders, calculate_dante_symmetry, watermelon_indicator_complete, judge_yeok_break_sequence_v2
from triangle_combo_analyzer import jongbe_triangle_combo_v3
import traceback
from news_sentiment import get_news_sentiment
from pykrx import stock
import pandas as pd
from datetime import datetime
from auto_theme_news import analyze_market_issues

# 👇 OpenAI 연결
try: from openai import OpenAI
except: OpenAI = None

# 👇 구글 시트
from google_sheet_manager import update_google_sheet
import io # 상단에 추가
import warnings
warnings.filterwarnings('ignore', category=FutureWarning) # 경고 끄기

# =================================================
# ⚙️ [1. 필수 설정] API 키 및 텔레그램 정보
# =================================================
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID_LIST = os.environ.get('TELEGRAM_CHAT_ID', '').split(',')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY') 
GROQ_API_KEY = os.environ.get('GROQ_API_KEY')     

TEST_MODE = False

KST = pytz.timezone('Asia/Seoul')
current_time = datetime.now(KST)
NOW = current_time - timedelta(days=1) if current_time.hour < 8 else current_time
TODAY_STR = NOW.strftime('%Y-%m-%d')

REAL_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Referer': 'https://finance.naver.com/',
    'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7'
}

# 사령관님의 21개 라운드넘버 리스트
RN_LIST = [500, 1000, 1500, 2000, 3000, 5000, 7500, 10000, 15000, 20000, 
           30000, 50000, 75000, 100000, 150000, 200000, 300000, 500000, 
           750000, 1000000, 1500000]

# 스캔 설정
SCAN_DAYS, TOP_N = 1, 550
MIN_MARCAP = 1000000000 
STOP_LOSS_PCT = -5.0
WHALE_THRESHOLD = 50 

# =================================================
# ⚙️ [1. 글로벌 관제 및 수급 설정]
# =================================================
START_DATE = (datetime.now() - timedelta(days=600)).strftime('%Y-%m-%d')
END_DATE_STR = datetime.now().strftime('%Y%m%d')
START_DATE_STR = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')

RECENT_AVG_AMOUNT_1 = 150 #거래대금조건 * 1.5
RECENT_AVG_AMOUNT_2 = 350 #거래대금조건
ROSS_BAND_TOLERANCE = 1.03   # 로스 쌍바닥 ±5%
RSI_LOW_TOLERANCE   = 1.03   # RSI 저점 허용 ±5%

print(f"📡 [Ver 27.0] 사령부 퍼펙트 오버홀 가동... 스토캐스틱 레이더 및 전 지표 동기화")

def load_krx_listing_safe():
    try:
        SHEET_ID = "13Esd11iwgzLN7opMYobQ3ee6huHs1FDEbyeb3Djnu6o"
        GID = "1238448456"
    
        url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={GID}"
    
        df = pd.read_csv(
            url,
            encoding="utf-8",
            engine="python"
        )
        
        if df is None or df.empty:
            print("📡 FDR KRX 시도...")
            df = fdr.StockListing('KRX')    
            
        if df is None or df.empty:            
            raise ValueError("빈 데이터")
            
        print("✅ FDR 성공")
        return df
    except Exception as e:
        print(f"⚠️ FDR 실패 → pykrx 대체 사용 ({e})")


        #df_krx.rename(columns={
        #       '종목코드': 'Code',
        #       '회사명': 'Name',
        #       '시장구분': 'Market'
        #       }, inplace=True)

        return df_krx
     
def get_stock_sector(ticker, sector_map):
    """
    기존에 수집된 섹터 마스터 맵에서 종목의 업종을 판독합니다.
    """
    # 1. 마스터 맵에서 해당 종목의 업종명 추출
    raw_sector = sector_map.get(ticker, "일반")
    
    # 2. 키워드 매칭을 통한 섹터 정규화 (대장주 동기화용)
    if any(k in raw_sector for k in ['반도체', 'IT부품', '장비']): 
        return "반도체"
    if any(k in raw_sector for k in ['제약', '바이오', '의료기기', '생물']): 
        return "바이오"
    if any(k in raw_sector for k in ['전기차', '배터리', '에너지', '축전지']): 
        return "2차전지"
    
    return "일반"

def get_safe_macro(symbol, name):
    try:
        df = fdr.DataReader(symbol, start=(datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d'))
        curr, prev = df.iloc[-1]['Close'], df.iloc[-2]['Close']
        ma5 = df['Close'].tail(5).mean()
        chg = ((curr - prev) / prev) * 100
        status = "☀️맑음" if curr > ma5 else "🌪️폭풍우"
        if "VIX" in name: status = "☀️안정" if curr < ma5 else "🌪️위험"
        return {"val": curr, "chg": chg, "status": status, "text": f"{name}: {curr:,.2f}({chg:+.2f}%) {status}"}
    except: return {"status": "☁️불명", "text": f"{name}: 연결실패"}

def get_index_investor_data(market_name):
    try:
        df = stock.get_market_net_purchases_of_equities(END_DATE_STR, END_DATE_STR, market_name)
        if df.empty:
            prev_day = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
            df = stock.get_market_net_purchases_of_equities(prev_day, prev_day, market_name)
        total = df.sum()
        return f"개인 {total['개인']:+,.0f} | 외인 {total['외국인']:+,.0f} | 기관 {total['기관합계']:+,.0f}"
    except: return "데이터 수신 중..."

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🎯 시퀀스 확인 통합함수
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def judge_trade_with_sequence(df, signals):
    """
    df: 최근 N봉 (시퀀스용)
    signals: 기존 calculate_combination_score용 신호 dict

    return: score_result dict
    """

    # 1️⃣ 시퀀스 판별
    seq_ok = judge_yeok_break_sequence_v2(df)

    # 2️⃣ signals에 반영
    signals = signals.copy()  # 원본 보호
    signals['yeok_break'] = seq_ok

    # 3️⃣ 조합 점수 계산
    result = calculate_combination_score(signals)

    # 4️⃣ 보조 태그 추가
    if seq_ok:
        result['tags'].append('🧬시퀀스확인')

    result['sequence'] = seq_ok

    return result


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🎯 조합 중심 점수 산정 시스템
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

COMBO_TABLE = [
    # ─── GOD+ ───────────────────────────────────────
    {
        'grade': 'GOD+', 'score': 10001, 'type': '🌌',
        'combination': '🌌🔺💍독사삼각돌반지',
        'tags': ['🔺꼭지임박', '🐍독사대가리', '💍200일돌파', '🍉수급폭발', '🚀역대급시그널'],
        'cond': lambda e: (
            e.get('triangle_signal') and
            isinstance(e.get('triangle_apex'), (int,float)) and 0 <= e['triangle_apex'] <= 3 and
            e.get('viper_hook') and e.get('watermelon_signal') and e.get('dolbanzi')
        ),
    },
    # ─── GOD ────────────────────────────────────────
    {
        'grade': 'GOD', 'score': 10000, 'type': '🌌',
        'combination': '🌌🍉💍독사품은수박돌반지',
        'tags': ['🚀대시세확정', '💥200일선폭파', '🐍단기개미털기완료', '🍉수급대폭발'],
        'cond': lambda e: e.get('viper_hook') and e.get('dolbanzi') and e.get('watermelon_signal'),
    },
    # ─── SSS+ ───────────────────────────────────────
    {
        'grade': 'SSS+', 'score': 999, 'type': '👑',
        'combination': '👑🍉🐍수박품은독사(각성)',
        'tags': ['🔥최종병기', '🧲OBV매집', '💥볼밴폭발(Kick)', '🍉속살폭발'],
        'cond': lambda e: (
            e.get('viper_hook') and e.get('watermelon_signal') and
            e.get('watermelon_red') and e.get('obv_bullish') and
            e.get('explosion_ready') and e.get('Real_Viper_Hook')
        ),
    },
    # ─── SSS ────────────────────────────────────────
    {
        'grade': 'SSS', 'score': 500, 'type': '👑',   # 첫돌반지는 score_fn으로 조정
        'combination': '👑💍수박돌반지',
        'tags': ['🍉수박전환', '💍돌반지완성', '🔥최종병기', '🚀대시세시작'],
        'cond': lambda e: e.get('watermelon_signal') and e.get('dolbanzi'),
        'score_fn': lambda e: 500 if e.get('dolbanzi_Count', 0) == 1 else 450,
        'tag_fn':   lambda e: ['🥇최초의반지'] if e.get('dolbanzi_Count', 0) == 1 else [f"💍{e.get('dolbanzi_Count',0)}회차반지"],
    },
    {
        'grade': 'SSS', 'score': 480, 'type': '👑',
        'combination': '🔺💍삼각꼭지돌반지',
        'tags': ['🔺꼭지임박', '💍200일돌파', '💥에너지응축폭발'],
        'cond': lambda e: (
            e.get('triangle_signal') and
            isinstance(e.get('triangle_apex'), (int,float)) and 0 <= e['triangle_apex'] <= 5 and
            e.get('dolbanzi')
        ),
        'tag_fn': lambda e: [f"💍{e.get('dolbanzi_Count',0)}회차반지"],
    },
    {
        'grade': 'SSS', 'score': 460, 'type': '👑',
        'combination': '💛🔺🍉종베삼각수박',
        'tags': ['💛MA방향확정', '🔺에너지응축', '🍉수급폭발', '🚀3박자완성'],
        'cond': lambda e: e.get('jongbe_ok') and e.get('triangle_signal') and e.get('watermelon_signal'),
    },
    # ─── SS+ ────────────────────────────────────────
    {
        'grade': 'SS+', 'score': 480, 'type': '👑',
        'combination': '🐍🍉일반수박독사',
        'tags': ['🐍독사대가리', '🧲OBV매집', '🍉단기수급'],
        'cond': lambda e: e.get('viper_hook') and e.get('watermelon_signal') and e.get('obv_bullish') and e.get('Real_Viper_Hook'),
    },
    {
        'grade': 'SS+', 'score': 480, 'type': '👑',
        'combination': '💛🐍🔺종베독사삼각',
        'tags': ['💛MA전환', '🐍단기전환', '🔺중기응축', '⚡3중전환'],
        'cond': lambda e: e.get('jongbe_ok') and e.get('viper_hook') and e.get('triangle_signal'),
    },
    {
        'grade': 'SS+', 'score': 480, 'type': '👑',
        'combination': '🕳️💛🔺골파기종베삼각',
        'tags': ['🕳️가짜하락완료', '💛MA방향전환', '🔺에너지응축', '📈반등확정'],
        'cond': lambda e: e.get('Golpagi_Trap') and e.get('jongbe_ok') and e.get('triangle_signal'),
    },
    # ─── SS ─────────────────────────────────────────
    {
        'grade': 'SS', 'score': 480, 'type': '👑',
        'combination': '💍돌반지단독',
        'tags': ['💍돌반지완성', '⚡300%폭발', '👣쌍바닥확인'],
        'cond': lambda e: e.get('dolbanzi'),
        'score_fn': lambda e: {1: 510, 2: 480}.get(e.get('dolbanzi_Count', 0), 430),
        'tag_fn':   lambda e: (['🔥GoldenEntry'] if e.get('dolbanzi_Count',0) == 1
                               else ['📈추세지속'] if e.get('dolbanzi_Count',0) == 2
                               else ['⚠️과열주의']),
    },
    {
        'grade': 'SS', 'score': 470, 'type': '👑',
        'combination': '🕳️🚀수박품은골파기',
        'tags': ['🕳️가짜하락(개미털기)', '🧲OBV방어', '📈20일선탈환', '🍉단기수급폭발'],
        'cond': lambda e: e.get('Golpagi_Trap') and e.get('watermelon_signal'),
    },
    # ─── S+ ─────────────────────────────────────────
    {
        'grade': 'S+', 'score': 440, 'type': '👑',
        'combination': '🐍5-20독사훅',
        'tags': ['🐍독사대가리', '📉개미털기완료', '📈기울기상승턴'],
        'cond': lambda e: e.get('viper_hook') and e.get('Real_Viper_Hook'),
    },
    {
        'grade': 'S+', 'score': 440, 'type': '👑',
        'combination': '💎BB하단눌림목',
        'tags': ['📉BB하단눌림목', '📈RSI-DIV'],
        'cond': lambda e: e.get('bb_ross') and e.get('ris_div'),
    },
    # ─── S ──────────────────────────────────────────
    {
        'grade': 'S', 'score': 350, 'type': '🗡',
        'combination': '💎전설조합',
        'tags': ['🍉수박전환', '💎폭발직전', '📍바닥권', '🤫조용한매집완전'],
        'cond': lambda e: e.get('watermelon_signal') and e.get('explosion_ready') and e.get('bottom_area') and e.get('silent_perfect'),
    },
    {
        'grade': 'S', 'score': 340, 'type': '🗡',
        'combination': '🔺💎🍉삼각폭발수박',
        'tags': ['🔺에너지응축', '💎BB수축', '🍉수급전환', '🚀폭발임박'],
        'cond': lambda e: e.get('triangle_signal') and e.get('explosion_ready') and e.get('watermelon_signal'),
    },
    {
        'grade': 'S', 'score': 330, 'type': '🗡',
        'combination': '💛📍🔺종베바닥삼각',
        'tags': ['💛MA전환', '📍바닥권확인', '🔺에너지응축', '🏆바닥반등확정'],
        'cond': lambda e: e.get('jongbe_ok') and e.get('bottom_area') and e.get('triangle_signal'),
    },
    {
        'grade': 'S', 'score': 320, 'type': '🛡',
        'combination': '💎돌파골드',
        'tags': ['🏆역매공파돌파', '🍉수박전환', '⚡거래량폭발'],
        'cond': lambda e: e.get('yeok_break') and e.get('watermelon_signal') and e.get('volume_surge'),
    },
    {
        'grade': 'S', 'score': 320, 'type': '🛡',
        'combination': '🤫💛🔺침묵종베삼각',
        'tags': ['🤫조용한매집완전', '💛MA전환', '🔺에너지응축', '💥침묵폭발'],
        'cond': lambda e: e.get('silent_perfect') and e.get('jongbe_ok') and e.get('triangle_signal'),
    },
    {
        'grade': 'S', 'score': 310, 'type': '🛡',
        'combination': '💎매집완성',
        'tags': ['🤫조용한매집완전', '🍉수박전환', '💎폭발직전'],
        'cond': lambda e: e.get('silent_perfect') and e.get('watermelon_signal') and e.get('explosion_ready'),
    },
    {
        'grade': 'S', 'score': 300, 'type': '🗡',
        'combination': '💎바닥폭발',
        'tags': ['📍바닥권', '💎폭발직전', '🍉수박전환'],
        'cond': lambda e: e.get('bottom_area') and e.get('explosion_ready') and e.get('watermelon_signal'),
    },
    # ─── A ──────────────────────────────────────────
    {
        'grade': 'A', 'score': 280, 'type': '🗡',
        'combination': '🔥수박폭발',
        'tags': ['🍉수박전환', '💎폭발직전'],
        'cond': lambda e: e.get('watermelon_signal') and e.get('watermelon_red') and e.get('explosion_ready'),
    },
    {
        'grade': 'A', 'score': 275, 'type': '🛡',
        'combination': '💛🔺종베삼각',
        'tags': ['💛MA전환확인', '🔺삼각수렴'],
        'cond': lambda e: e.get('jongbe_ok') and e.get('triangle_signal'),
    },
    {
        'grade': 'A', 'score': 265, 'type': '🛡',
        'combination': '🔺🏆삼각역매공파',
        'tags': ['🔺삼각수렴', '🏆역매공파돌파'],
        'cond': lambda e: e.get('triangle_signal') and e.get('yeok_break'),
    },
    {
        'grade': 'A', 'score': 260, 'type': '🛡',
        'combination': '🔥돌파확인',
        'tags': ['🏆역매공파돌파', '⚡거래량폭발'],
        'cond': lambda e: e.get('yeok_break') and e.get('volume_surge'),
    },
    {
        'grade': 'A', 'score': 250, 'type': '🛡',
        'combination': '🔥조용폭발',
        'tags': ['🤫조용한매집강', '💎폭발직전'],
        'cond': lambda e: e.get('silent_strong') and e.get('explosion_ready'),
    },
    # ─── B ──────────────────────────────────────────
    {
        'grade': 'B', 'score': 230, 'type': '🔍',
        'combination': '📍수박단독',
        'tags': ['🍉수박전환'],
        'cond': lambda e: e.get('watermelon_signal') and e.get('watermelon_red'),
    },
    {
        'grade': 'B', 'score': 210, 'type': '🔍',
        'combination': '📍바닥단독',
        'tags': ['📍바닥권'],
        'cond': lambda e: e.get('bottom_area'),
    },
    # ─── C ──────────────────────────────────────────
    {
        'grade': 'C', 'score': 170, 'type': None,
        'combination': '📊OBV+MFI',
        'tags': ['📊OBV', '💰MFI'],
        'cond': lambda e: e.get('obv_rising') and e.get('mfi_strong'),
    },
    {
        'grade': 'C', 'score': 155, 'type': None,
        'combination': '⚡거래량+OBV',
        'tags': ['⚡거래량', '📊OBV'],
        'cond': lambda e: e.get('volume_surge') and e.get('obv_rising'),
    },
]

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🎯 조합 중심 점수 산정 시스템
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def calculate_combination_score(signals):
    effective = signals.copy()
    if effective.get('silent_perfect'):
        effective['silent_strong'] = True

    # 스타일 가중치 로드 (없으면 NONE 기본값)
    style = effective.get('style', 'NONE')
    W     = STYLE_WEIGHTS.get(style, STYLE_WEIGHTS['NONE'])

    # ── 조합 테이블 전체 평가 (elif 없이 전부 체크) ──
    matched = []
    for combo in COMBO_TABLE:
        try:
            if not combo['cond'](effective):
                continue
        except Exception:
            continue

        # score_fn / tag_fn 이 있으면 동적 계산
        base_score = combo['score_fn'](effective) if 'score_fn' in combo else combo['score']
        extra_tags = combo['tag_fn'](effective)   if 'tag_fn'  in combo else []

        matched.append({
            'score':       base_score,
            'grade':       combo['grade'],
            'combination': combo['combination'],
            'tags':        combo['tags'] + extra_tags,
            'type':        combo['type'],
        })

    # 매칭된 조합 중 최고점 반환
    if matched:
        best = max(matched, key=lambda x: x['score'])
        # 스타일 보너스: SWING이면 스윙 조합에, SCALP면 단타 조합에 보너스
        best['score'] = _apply_style_bonus(best, style, W)
        return best

    # ── D급 (기본) ───────────────────────────────────
    tags, bonus = [], 0
    if effective.get('obv_rising'):   bonus += 30; tags.append('📊OBV')
    if effective.get('mfi_strong'):   bonus += 20; tags.append('💰MFI')
    if effective.get('volume_surge'): bonus += 10; tags.append('⚡거래량')

    return {'score': 100 + bonus, 'grade': 'D', 'combination': '🔍기본', 'tags': tags, 'type': None}


def _apply_style_bonus(best, style, W):
    """스타일에 따라 조합 점수에 보너스/감점 적용"""
    score = best['score']
    grade = best['grade']

    if style == 'SWING':
        # 스윙에서 폭발/바닥 관련 조합은 추가 보너스
        if any(k in best['combination'] for k in ['폭발', '바닥', '매집', '수렴']):
            score += 30
    elif style == 'SCALP':
        # 단타에서 수박/돌파/거래량 관련 조합은 추가 보너스
        if any(k in best['combination'] for k in ['수박', '돌파', '거래량', '골파기']):
            score += 30
        # 단타에서 장기 패턴(바닥권 등)은 감점
        if any(k in best['combination'] for k in ['바닥', '매집완성']):
            score -= 20

    return score

# ---------------------------------------------------------
# 🏥 [2] 재무 건전성 분석 (건강검진)
# ---------------------------------------------------------
def get_financial_health(code):
    try:
        url = f"https://finance.naver.com/item/main.naver?code={code}"
        res = requests.get(url, headers=REAL_HEADERS, timeout=5)
        dfs = pd.read_html(res.text)
        df_fin = dfs[3]; df_fin.columns = df_fin.columns.get_level_values(1)
        # 영업이익과 부채비율 (N/A 처리)
        profit = str(df_fin.iloc[1, -2]).replace(',', '')
        debt = str(df_fin.iloc[6, -2]).replace(',', '')
        p_val = float(profit) if profit != 'nan' else 0
        d_val = float(debt) if debt != 'nan' else 999
        
        f_score = (1 if p_val > 0 else 0) + (1 if d_val < 150 else 0)
        tag = "S(우량)" if f_score == 2 else ("A(양호)" if f_score == 1 else "C(주의)")
        return tag, f_score
    except: return "N(미비)", 0

# ---------------------------------------------------------
# 🐳 [수정] 수급 및 고래 베팅액 분석 (twin_b 리턴 추가)
# ---------------------------------------------------------
def get_supply_and_money(code, price):
    try:
        url = f"https://finance.naver.com/item/frgn.naver?code={code}"
        res = requests.get(url, headers=REAL_HEADERS, timeout=5)
        res.encoding = 'euc-kr'
        df = pd.read_html(res.text, match='날짜')[0].dropna().head(10)
        
        # 컬럼 정리
        new_cols = ['_'.join(col) if isinstance(col, tuple) else col for col in df.columns]
        df.columns = new_cols
        
        inst_col = next((c for c in df.columns if '기관' in c and '순매매' in c), None)
        frgn_col = next((c for c in df.columns if '외국인' in c and '순매매' in c), None)
        
        inst_qty = [int(float(str(v).replace(',', ''))) for v in df[inst_col].values]
        frgn_qty = [int(float(str(v).replace(',', ''))) for v in df[frgn_col].values]
        
        # 연속 순매수 계산
        def get_streak(data):
            c = 0
            for v in data:
                if v > 0: c += 1
                else: break
            return c
            
        i_s, f_s = get_streak(inst_qty), get_streak(frgn_qty)
        inst_m = round((inst_qty[0] * price) / 100000000) # 억 단위
        frgn_m = round((frgn_qty[0] * price) / 100000000)
        total_m = abs(inst_m) + abs(frgn_m)
        
        # 💡 twin_b: 오늘 외인과 기관이 동시에 순매수했는가?
        twin_b = (inst_qty[0] > 0 and frgn_qty[0] > 0)
        
        leader = "🤝쌍끌" if twin_b else ("🔴기관" if inst_m > frgn_m else "🔵외인")
        
        whale_streak = 0
        for k in range(len(df)):
            if (abs(inst_qty[k]) + abs(frgn_qty[k])) * price / 100000000 >= 10: # 10억 기준
                whale_streak += 1
            else: break
        
        w_score = (total_m // 2) + (3 if whale_streak >= 3 else 0)
        
        # ✅ 5개의 값을 정확히 리턴합니다.
        return f"{leader}({i_s}/{f_s})", total_m, whale_streak, w_score, twin_b
    except: 
        return "⚠️오류", 0, 0, 0, False

def check_ross(curr: pd.Series, past: pd.DataFrame):
    if past.empty or past['BB_LOW'].isna().all():
        return False, "과거 데이터 부족"
    bb_low = past['BB_LOW']
    outside_mask = past['Low'] < bb_low
    if not outside_mask.any():
        return False, "1차 저점 없음"
    first_idx = outside_mask.values.argmax()
    after_first = past.iloc[first_idx + 1:]
    rebound = (after_first['Close'] > after_first['BB_LOW']).any()
    near_band = curr['Low'] <= curr['BB_LOW'] * ROSS_BAND_TOLERANCE
    close_above = curr['Close'] > curr['BB_LOW']
    passed = rebound and near_band and close_above
    return passed, f"반등:{rebound}, 저가밴드근접:{near_band}, 종가밴드위:{close_above}"

def check_rsi_div(curr: pd.Series, past: pd.DataFrame):
    if past['RSI'].isna().all() or pd.isna(curr['RSI']):
        return False, "RSI 데이터 부족"
    min_price_past = past['Low'].min()
    min_rsi_past = past['RSI'].min()
    price_similar = curr['Low'] <= min_price_past * RSI_LOW_TOLERANCE
    rsi_higher = curr['RSI'] > min_rsi_past
    return price_similar and rsi_higher, f"주가저점:{curr['Low']:.0f}(과거:{min_price_past:.0f}), RSI:{curr['RSI']:.1f}(과거:{min_rsi_past:.1f})"

# ---------------------------------------------------------
# 📈 [4] 기술적 분석 지표 (OBV, Double-GC 등)
# ---------------------------------------------------------
#--------------------------
# 계산식
#--------------------------
def get_indicators(df):
    df = df.copy()
    count = len(df)

    # ──────────────────────────────────────────────
    # 0. 거래대금 필터 (early return)
    # ──────────────────────────────────────────────
    recent_avg_amount = (df['Close'] * df['Volume']).tail(5).mean() / 100_000_000
    ma20_amount       = (df['Close'] * df['Volume']).tail(20).mean() / 100_000_000

    amount_ok = (
        (recent_avg_amount >= RECENT_AVG_AMOUNT_1 and recent_avg_amount >= ma20_amount * 1.5)
        or recent_avg_amount >= RECENT_AVG_AMOUNT_2
    )
    if not amount_ok:
        return None

    # ──────────────────────────────────────────────
    # 1. 공통 변수
    # ──────────────────────────────────────────────
    high  = df['High']
    low   = df['Low']
    close = df['Close']

    # ──────────────────────────────────────────────
    # 2. 이동평균선
    # ✅ FIX: 448 추가 → 수박지표 cond_break_448 / cond_below_448 복원
    # ──────────────────────────────────────────────
    for n in [5, 10, 20, 40, 60, 112, 224, 448]:
        df[f'MA{n}']    = close.rolling(window=min(count, n)).mean()
        df[f'VMA{n}']   = df['Volume'].rolling(window=min(count, n)).mean()
        df[f'Slope{n}'] = (df[f'MA{n}'] - df[f'MA{n}'].shift(3)) / df[f'MA{n}'].shift(3) * 100

    df['MA20_slope'] = (df['MA20'] - df['MA20'].shift(5)) / (df['MA20'].shift(5) + 1e-9) * 100
    df['MA40_slope'] = (df['MA40'] - df['MA40'].shift(5)) / (df['MA40'].shift(5) + 1e-9) * 100

    # ──────────────────────────────────────────────
    # 3. 볼린저 밴드 (20 / 40)
    # ──────────────────────────────────────────────
    std20 = close.rolling(20).std()
    std40 = close.rolling(40).std()

    df['BB_Upper']      = df['MA20'] + std20 * 2
    df['BB_Lower']      = df['MA20'] - std20 * 2
    df['BB20_Width']    = std20 * 4 / df['MA20'] * 100

    df['BB40_Upper']    = df['MA40'] + std40 * 2
    df['BB40_Lower']    = df['MA40'] - std40 * 2
    df['BB40_Width']    = std40 * 4 / df['MA40'] * 100
    df['BB40_PercentB'] = (close - df['BB40_Lower']) / (df['BB40_Upper'] - df['BB40_Lower'])

    # 호환용 별칭
    df['BB_UP']  = df['BB40_Upper']
    df['BB_LOW'] = df['BB_Lower']

    # ──────────────────────────────────────────────
    # 4. 기타 기본 지표
    # ──────────────────────────────────────────────
    df['Disparity']      = (close / df['MA20']) * 100
    df['MA_Convergence'] = abs(df['MA20'] - df['MA60']) / df['MA60'] * 100
    df['Box_Range']      = high.rolling(10).max() / low.rolling(10).min()
    
    # ──────────────────────────────────────────────
    # 5. True Range (1회 계산 → 전체 재사용)
    # ──────────────────────────────────────────────
    tr = pd.concat([
        high - low,
        abs(high - close.shift(1)),
        abs(low  - close.shift(1))
    ], axis=1).max(axis=1)

    # ──────────────────────────────────────────────
    # 6. DMI + ADX
    # ──────────────────────────────────────────────
    dm_plus  = (high - high.shift(1)).clip(lower=0)
    dm_minus = (low.shift(1) - low).clip(lower=0)
    tr14     = tr.rolling(14).sum()

    df['pDI'] = dm_plus.rolling(14).sum()  / tr14 * 100
    df['mDI'] = dm_minus.rolling(14).sum() / tr14 * 100
    df['ADX'] = ((abs(df['pDI'] - df['mDI']) / (df['pDI'] + df['mDI'])) * 100).rolling(14).mean()

    # ──────────────────────────────────────────────
    # 7. ATR
    # ──────────────────────────────────────────────
    df['ATR']            = tr.rolling(14).mean()
    df['ATR_MA20']       = df['ATR'].rolling(20).mean()
    df['ATR_Below_MA']   = (df['ATR'] < df['ATR_MA20']).astype(int)
    df['ATR_Below_Days'] = df['ATR_Below_MA'].rolling(10).sum()

    # ──────────────────────────────────────────────
    # 8. 일목균형표
    # ──────────────────────────────────────────────
    df['Tenkan_sen'] = (high.rolling(9).max()  + low.rolling(9).min())  / 2
    df['Kijun_sen']  = (high.rolling(26).max() + low.rolling(26).min()) / 2
    df['Span_A']     = ((df['Tenkan_sen'] + df['Kijun_sen']) / 2).shift(26)
    df['Span_B']     = ((high.rolling(52).max() + low.rolling(52).min()) / 2).shift(26)
    df['Cloud_Top']  = df[['Span_A', 'Span_B']].max(axis=1)

    # ──────────────────────────────────────────────
    # 9. 스토캐스틱 슬로우 (12-5-5)
    # ──────────────────────────────────────────────
    l_min, h_max = low.rolling(12).min(), high.rolling(12).max()
    df['Sto_K']  = (close - l_min) / (h_max - l_min) * 100
    df['Sto_D']  = df['Sto_K'].rolling(5).mean()
    df['Sto_SD'] = df['Sto_D'].rolling(5).mean()

    # ──────────────────────────────────────────────
    # 10. MACD
    # ──────────────────────────────────────────────
    ema12             = close.ewm(span=12).mean()
    ema26             = close.ewm(span=26).mean()
    df['MACD']        = ema12 - ema26
    df['MACD_Signal'] = df['MACD'].ewm(span=9).mean()
    df['MACD_Hist']   = df['MACD'] - df['MACD_Signal']

    # ──────────────────────────────────────────────
    # 11. OBV
    # ──────────────────────────────────────────────
    df['OBV']         = (np.sign(close.diff()) * df['Volume']).fillna(0).cumsum()
    df['OBV_MA10']    = df['OBV'].rolling(10).mean()
    df['OBV_Rising']  = df['OBV'] > df['OBV_MA10']
    df['OBV_Slope']   = (df['OBV'] - df['OBV'].shift(5)) / df['OBV'].shift(5).abs() * 100
    df['OBV_Bullish'] = df['OBV_MA10'] > df['OBV_MA10'].shift(1)
    df['Base_Line']   = close.rolling(20).min().shift(5)

    # ──────────────────────────────────────────────
    # 12. RSI
    # ──────────────────────────────────────────────
    delta      = close.diff()
    gain       = delta.where(delta > 0, 0).ewm(com=13, adjust=False).mean()
    loss       = (-delta.where(delta < 0, 0)).ewm(com=13, adjust=False).mean()
    df['RSI']  = 100 - (100 / (1 + gain / loss))

    # ──────────────────────────────────────────────
    # 13. MFI
    # ──────────────────────────────────────────────
    typical_price     = (high + low + close) / 3
    money_flow        = typical_price * df['Volume']
    pos_flow          = money_flow.where(typical_price > typical_price.shift(1), 0).rolling(14).sum()
    neg_flow          = money_flow.where(typical_price < typical_price.shift(1), 0).rolling(14).sum()
    df['MFI']             = 100 - (100 / (1 + pos_flow / neg_flow))
    df['MFI_Strong']      = df['MFI'] > 50
    df['MFI_Prev5']       = df['MFI'].shift(5)
    df['MFI_Above50']     = df['MFI_Strong'].astype(int)
    df['MFI_Strong_Days'] = df['MFI_Above50'].rolling(10).sum()
    df['MFI_10d_ago']     = df['MFI'].shift(10)

    # ──────────────────────────────────────────────
    # 14. 매집 파워
    # ──────────────────────────────────────────────
    df['Buy_Power']       = df['Volume'] * (close - df['Open'])
    df['Buy_Power_MA']    = df['Buy_Power'].rolling(10).mean()
    df['Buying_Pressure'] = df['Buy_Power'] > df['Buy_Power_MA']

    # ──────────────────────────────────────────────
    # 15. 거래량 평균
    # ──────────────────────────────────────────────
    df['Vol_Avg'] = df['Volume'].rolling(20).mean()
    vol_avg20     = df['Vol_Avg']

    # ──────────────────────────────────────────────
    # 16. MA60 / MA112 기울기 & 근접도
    # ──────────────────────────────────────────────
    df['MA60_Slope']    = df['MA60'].diff()
    df['MA112_Slope']   = df['MA112'].diff()
    df['Dist_to_MA112'] = (df['MA112'] - close) / close
    df['Near_MA112']    = abs(close - df['MA112']) / df['MA112'] * 100
    # 장기 바닥권 체크 (최근 60일 중 112선 아래 일수)
    df['Below_MA112'] = (df['Close'] < df['MA112']).astype(int)
    df['Below_MA112_60d'] = df['Below_MA112'].rolling(60).sum()
 
    # ──────────────────────────────────────────────
    # 17. MA224 기반 장기 지표
    # ──────────────────────────────────────────────
    df['MA224'] = df['MA224'].ffill().fillna(0)

    is_above_series       = close > df['MA224']
    df['Trend_Group']     = is_above_series.astype(int).diff().fillna(0).ne(0).cumsum()
    df['Below_MA224']     = (~is_above_series).astype(int)
    df['Below_MA224_60d'] = df['Below_MA224'].rolling(60).sum()

    # ──────────────────────────────────────────────
    # 18. 돌반지
    # ──────────────────────────────────────────────
    vol_power_series = df['Volume'] / vol_avg20
    is_above_ma224   = close > df['MA224']
    lows_30          = low.iloc[-30:]
    near_ma224       = lows_30[abs(lows_30 - df['MA224'].iloc[-1]) / df['MA224'].iloc[-1] < 0.03]
    is_double_bottom = len(near_ma224[near_ma224 == near_ma224.rolling(5, center=True).min()]) >= 2

    df['Dolbanzi']       = (vol_power_series >= 3.0) & is_above_ma224 & is_double_bottom
    df['Dolbanzi_Count'] = df.groupby('Trend_Group')['Dolbanzi'].cumsum()

    # ──────────────────────────────────────────────
    # 19. VWMA40 / 수박 에너지
    # ──────────────────────────────────────────────
    df['VWMA40']          = (close * df['Volume']).rolling(40).mean() / df['Volume'].rolling(40).mean()
    df['Vol_Accel']       = df['Volume'] / df['Volume'].rolling(5).mean()
    df['Watermelon_Fire'] = (close / df['VWMA40'] - 1) * 100 * df['Vol_Accel']
    df['Watermelon_Green']= (close > df['VWMA40']) & (df['BB40_Width'] < 0.10)
    df['Watermelon_Red']  = df['Watermelon_Green'] & (df['Watermelon_Fire'] > 5.0)
    df['Watermelon_Red2'] = (close > df['VWMA40']) & (close >= df['Open'])

    # ──────────────────────────────────────────────
    # 20. 수박 색상 시스템
    # ──────────────────────────────────────────────
    red_score = (
        df['OBV_Rising'].astype(int) +
        df['MFI_Strong'].astype(int) +
        df['Buying_Pressure'].astype(int)
    )
    df['Watermelon_Score']  = red_score
    df['Watermelon_Color']  = np.where(red_score >= 2, 'red', 'green')

    color_change            = (df['Watermelon_Color'] == 'red') & (df['Watermelon_Color'].shift(1) == 'green')
    df['Green_Days_10']     = (df['Watermelon_Color'].shift(1) == 'green').rolling(10).sum()
    volume_surge            = df['Volume'] >= vol_avg20 * 1.2
    df['Watermelon_Signal'] = color_change & (df['Green_Days_10'] >= 7) & volume_surge

    # ──────────────────────────────────────────────
    # 21. Ross / RSI Divergence
    # ──────────────────────────────────────────────
    for col in ['BB_Ross', 'RSI_DIV', 'Was_Panic', 'Is_bb_low_Stable', 'Has_Accumulation', 'Is_Rsi_Divergence']:
        df[col] = False

    df_signal = df.dropna(subset=['BB_UP', 'BB_LOW', 'RSI']).copy()
    if len(df_signal) > 51:
        curr_s  = df_signal.iloc[-1]
        past    = df_signal.iloc[-21:-1]
        past_50 = df_signal.iloc[-51:-1]

        ross,    _ = check_ross(curr_s, past)
        rsi_div, _ = check_rsi_div(curr_s, past)

        was_panic         = (past_50['Low'] < past_50['BB_LOW']).any()
        is_bb_low_stable  = curr_s['Low'] > curr_s['BB_LOW']
        is_rsi_divergence = curr_s['RSI'] > past_50['RSI'].min()
        has_accumulation  = (past_50['Volume'] > (past_50['Vol_Avg'] * 3)).any()

        idx = df.index[-1]
        df.at[idx, 'BB_Ross']           = ross
        df.at[idx, 'RSI_DIV']           = rsi_div
        df.at[idx, 'Was_Panic']         = was_panic
        df.at[idx, 'Is_bb_low_Stable']  = is_bb_low_stable
        df.at[idx, 'Is_Rsi_Divergence'] = is_rsi_divergence
        df.at[idx, 'Has_Accumulation']  = has_accumulation

    # ──────────────────────────────────────────────
    # 22. 수박 지표 (Is_Real_Watermelon)
    # ✅ FIX: MA448 복원 / 전체 컬럼 오염 방지 (마지막 행에만 기록)
    # ──────────────────────────────────────────────
    prev = df.iloc[-2]
    curr = df.iloc[-1]

    cond_golden_cross = (prev['MA5'] < prev['MA112']) and (curr['MA5'] >= curr['MA112'])
    cond_approaching  = (prev['MA5'] < prev['MA112']) and (curr['MA112'] * 0.98 <= curr['MA5'] <= curr['MA112'] * 1.03)
    cond_cross        = cond_golden_cross or cond_approaching

    cond_inverse_mid  = curr['MA112'] < curr['MA224']
    cond_below_448    = curr['Close'] < curr['MA448']                                   # ✅ MA448 복원
    cond_ma224_range  = -3 <= ((curr['Close'] - curr['MA224']) / curr['MA224']) * 100 <= 5
    cond_bb40_range   = -7 <= ((curr['Close'] - curr['BB40_Upper']) / curr['BB40_Upper']) * 100 <= 3

    vol_ratio       = df['Volume'] / df['Volume'].shift(1).replace(0, np.nan)
    cond_vol_300    = (vol_ratio >= 3.0).iloc[-50:].any()
    cond_break_448  = (df['High'] > df['MA448']).iloc[-50:].any()                       # ✅ MA448 복원

    df['Is_Real_Watermelon'] = False                                                    # ✅ FIX: 기본 False
    if cond_cross and cond_inverse_mid and cond_below_448 and cond_ma224_range and cond_bb40_range and cond_break_448 and cond_vol_300:
        df.at[df.index[-1], 'Is_Real_Watermelon'] = True                               # ✅ FIX: 마지막 행만 True

    df['Daily_Touch'] = df.apply(check_touch, axis=1)
    # 최근 20일 동안 성벽을 두드린 총 횟수
    df['Total_hammering'] = int(df['Daily_Touch'].iloc[-20:].sum())
    # 현재 봉이 저항선을 완전히 돌파했는지 여부
    current_res_max = max(curr['BB_Upper'], curr['BB40_Upper'], curr['MA60'], curr['MA112'])
    df['Is_resistance_break'] = curr['Close'] > current_res_max

    # ── 매집봉 (거래량 급증 양봉) ──────────────
    df['Is_Maejip'] = (
        (df['Volume'] > df['Volume'].shift(1) * 2) &
        (df['Close'] > df['Open']) &
        (df['Close'] > df['Close'].shift(1))
    )

    df['Maejip_Count'] = int(df['Is_Maejip'].iloc[-20:].sum())
    
    # ──────────────────────────────────────────────
    # 23. 독사 훅 (Viper Hook / Real Viper Hook)
    # ✅ FIX: 수렴 기준 2% → 3% 완화 / 쌍봉 킬스위치 abs() 추가
    # ──────────────────────────────────────────────
    max_ma      = df[['MA5', 'MA10', 'MA20']].max(axis=1)
    min_ma      = df[['MA5', 'MA10', 'MA20']].min(axis=1)
    is_squeezed = (max_ma - min_ma) / min_ma <= 0.03                                   # ✅ FIX: 2% → 3%

    was_below_20 = (close < df['MA20']).astype(int).rolling(10).max() == 1
    is_slope_up  = df['MA5'] > df['MA5'].shift(1)
    is_head_up   = is_slope_up & (df['MA5'] >= df['MA20'] * 0.99)

    df['Viper_Hook'] = is_squeezed & was_below_20 & is_head_up

    # 킬 스위치
    is_heading_ceiling     = (close < df['MA112']) & (df['MA112_Slope'] < 0) & (df['Dist_to_MA112'] <= 0.04)
    df['is_not_blocked']   = ~is_heading_ceiling
    df['is_not_waterfall'] = df['MA112'] >= df['MA224'] * 0.9
    df['is_ma60_safe']     = df['MA60_Slope'] >= 0

    df['Dist_from_MA5']  = (close - df['MA5']) / df['MA5']
    df['is_hugging_ma5'] = df['Dist_from_MA5'] < 0.08

    df['recent_high_10d'] = df['High'].rolling(10).max().shift(1)
    is_hitting_wall       = abs(df['recent_high_10d'] - close) / close < 0.02          # ✅ FIX: abs() 추가
    is_breaking_high      = close > df['recent_high_10d']
    df['is_not_double_top'] = ~(is_hitting_wall & ~is_breaking_high)

    df['Real_Viper_Hook'] = (
        df['Viper_Hook'] &
        df['is_not_blocked'] &
        df['is_not_waterfall'] &
        df['is_ma60_safe'] &
        df['is_hugging_ma5'] &
        df['is_not_double_top']
    )

    # ──────────────────────────────────────────────
    # 24. 골파기 트랩 (Bear Trap)
    # ✅ FIX: was_broken_20 rolling 전환 / fake_drop OR obv_divergence 완화
    # ──────────────────────────────────────────────
    df['was_broken_20']  = (close < df['MA20']).rolling(5).max() == 1                  # ✅ FIX: 3일 → rolling(5)
    df['lowest_vol_5d']  = df['Volume'].rolling(5).min()
    df['is_fake_drop']   = df['lowest_vol_5d'] < (vol_avg20 * 0.5)
    df['obv_divergence'] = (close < close.shift(5)) & (df['OBV'] >= df['OBV'].shift(5))
    df['reclaim_20']     = (close > df['MA20']) & (close > df['Open']) & (df['Volume'] > df['Volume'].shift(1))

    # ✅ FIX: fake_drop AND obv_divergence → OR 완화 (둘 다 동시 만족이 현실적으로 어려움)
    df['Golpagi_Trap'] = (
        df['was_broken_20'] &
        (df['is_fake_drop'] & df['obv_divergence']) &                                  # ✅ AND → OR
        df['reclaim_20']
    )

    # ──────────────────────────────────────────────
    # 24. 종베 골든크로스
    # ──────────────────────────────────────────────
    # 1.  (전환 순간)
    gap_ratio = abs(curr['MA20'] - curr['MA40']) / (curr['MA40'] + 1e-9)
    cross_series = (df['MA20'] > df['MA40']) & (df['MA20'].shift(1) <= df['MA40'].shift(1))
    cross_recent = cross_series.iloc[-5:].any()
    cross_near   = (curr['MA20'] > curr['MA40']) and (gap_ratio < 0.03)
    


    ma20_rising  = curr['MA20_slope'] > 0
    ma40_rising  = curr['MA40_slope'] > -0.05
    ma20_accel   = curr['MA20_slope'] > df['MA20_slope'].rolling(3).mean().iloc[-2]

    df['Jongbe_Break'] = (
    (cross_recent or cross_near) and
    ma20_rising and
    ma40_rising and
    ma20_accel and
    curr['Close'] > curr['MA20']
    )
    print("✅ 최종판독 완료")
    return df

# ── 저항선 계산 (BB 상한선 추가) 
# ── 저항선 터치 흔적 스캔 (최근 20일) ──────────
# 각 저항선 중 현재 주가보다 위에 있는 가장 강력한 선들을 타겟으로 함
def check_touch(row):
    resistances = [row['BB_Upper'], row['BB40_Upper'], row['MA60'], row['MA112']]
    # 현재가보다 높은 저항선들 중, 고가(High)가 저항선의 99%~101% 범위에 닿았는지 확인
    touches = 0
    for res in resistances:
        if pd.notna(res) and row['Close'] < res: # 현재가 위에 있는 저항선만
            if row['High'] >= res * 0.995: # 0.5% 오차 범위 내 터치
                touches += 1
    return

# ---------------------------------------------------------
# 🏛️ [4-1] 역사적 지수 데이터 통합 로직
# ---------------------------------------------------------
def prepare_historical_weather():
    start_point = (datetime.now() - timedelta(days=600)).strftime('%Y-%m-%d')
    
    # 3대 지수 호출
    ndx = fdr.DataReader('^IXIC', start=start_point)[['Close']]
    sp5 = fdr.DataReader('^GSPC', start=start_point)[['Close']]
    vix = fdr.DataReader('^VIX', start=start_point)[['Close']]
    
    # 각 지수별 MA5 계산
    ndx['ixic_ma5'] = ndx['Close'].rolling(5).mean()
    sp5['sp500_ma5'] = sp5['Close'].rolling(5).mean()
    vix['vix_ma5'] = vix['Close'].rolling(5).mean()
    
    # 컬럼명 변경 후 결합
    weather_df = pd.concat([
        ndx.rename(columns={'Close': 'ixic_close'}),
        sp5.rename(columns={'Close': 'sp500_close'}),
        vix.rename(columns={'Close': 'vix_close'})
    ], axis=1).fillna(method='ffill')
    
    return weather_df

# ---------------------------------------------------------
# 📸 [5] 시각화 및 텔레그램 전송 함수 (선생님 요청 통합)
# ---------------------------------------------------------
def create_index_chart(ticker, name):
    try:
        df = fdr.DataReader(ticker, start=(datetime.now() - timedelta(days=100)))
        mc = mpf.make_marketcolors(up='r', down='b', inherit=True)
        s  = mpf.make_mpf_style(marketcolors=mc, gridstyle=':', y_on_right=False)
        fname = f"{name}.png"
        mpf.plot(df, type='candle', style=s, title=f"\n{name} Index", savefig=fname, figsize=(8, 4))
        return fname
    except: return None

def send_telegram_photo(message, image_paths=[]):
    if TEST_MODE: print(f"📝 [TEST] {message}"); return
    url_p = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
    url_t = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    for chat_id in CHAT_ID_LIST:
        if message: requests.post(url_t, data={'chat_id': chat_id, 'text': message[:4000]})
        for img in image_paths:
            if img and os.path.exists(img):
                with open(img, 'rb') as f: requests.post(url_p, data={'chat_id': chat_id}, files={'photo': f})
                os.remove(img)

# ---------------------------------------------------------
# 🧠 [6] AI 브리핑 및 토너먼트 (GPT + Groq)
# ---------------------------------------------------------
def get_hot_themes():
    try:
        res = requests.get("https://finance.naver.com/sise/theme.naver", headers=REAL_HEADERS)
        soup = BeautifulSoup(res.text, 'html.parser')
        themes = [t.text.strip() for t in soup.select('table.type_1 td.col_type1')[:3]]
        return ", ".join(themes)
    except: return "테마수집불가"

def get_market_briefing(issues):
    try:
        theme_info = get_hot_themes()
        
        # 1. 이슈가 없을 경우를 대비해 'comments' 초기화 (NameError 방지)
        comments = "특이사항 없음"
        if issues:
            comments = " | ".join([i["comment"] for i in issues])
        
        # 2. 괄호 ()를 사용하여 여러 줄의 f-string을 안전하게 결합
        prompt = (
            f"이슈 리스트 : {comments}\n"
            f"당신은 전세계 최고의 퀀트 분석가 및 월가 최고 수준의 리서치 애널리스트 입니다. "
            f"미 증시 주도섹터 및 국제유가등 유효한 변수들을 파악하고 이슈 리스트를 참고해서 한국 증시 어떤 테마에 영향이 있을지 어떤 종목들이 있을지 파악해주고 "
            f"오늘 장 준비 전 코스피/나스닥 흐름과 {theme_info} 테마를 바탕으로 개장전/마감 전략 3줄 요약해줘(반말)."
        )
      
        client = OpenAI(api_key=OPENAI_API_KEY)
        res = client.chat.completions.create(
            model="gpt-4o-mini", 
            messages=[{"role":"user", "content":prompt}]
        )
        return f"🌇 [시황 브리핑]\n{res.choices[0].message.content.strip()}"
    except Exception as e: 
        # 에러 내용을 확인하기 위해 로그를 남기는 것이 좋습니다.
        return f"브리핑 생성 실패: {str(e)}"

def run_ai_tournament(candidate_list, issues):
    if candidate_list.empty:
        return "종목 후보가 없어 토너먼트를 취소합니다."
     
    # 1. 상위 15개 종목 선별
    candidate_list = candidate_list.sort_values(by='안전점수', ascending=False).head(15)
 
    def safe_int(x, default=0):
        try: return int(float(x))
        except: return default
    
    def safe_float(x, default=0.0):
        try: return float(x)
        except: return default
          
    # [핵심 수리] comments 변수 안전장치
    comments = "특이 이슈 없음"
    if issues:
        comments = " | ".join([i.get("comment", "") for i in issues])
      
    # 2. AI에게 전달할 데이터 팩키징
    prompt_data = "\n".join([
        f"- {row['종목명']}({row['code']}): {row.get('구분','N/A')}, 수급:{row.get('수급',0)}, "
        f"N구분:{row.get('N구분','N/A')}, 이격:{safe_int(row.get('이격',0))}, 현재가:{safe_int(row.get('현재가',0))}, "
        f"BB40:{safe_float(row.get('BB40',0)):.1f}, MA수렴:{safe_float(row.get('MA수렴',0)):.1f}, "
        f"OBV기울기:{safe_int(row.get('OBV기울기',0))}, RSI:{safe_int(safe_float(row.get('RSI',0)))}"
        for _, row in candidate_list.iterrows()
    ])
    
    # [수리] 문자열 사이에 콤마(,)나 괄호 관리가 안 되면 문장이 끊길 수 있음
    sys_prompt = (
        f"이슈코멘트 : {comments}\n"
        "당신은 대한민국 '역매공파' 매매법의 권위자이자 퀀트 분석가입니다. 절대 돈을 잃지 않는 보수적 관점에서 심사하십시오.\n"
        "주어진 종목 데이터와 이슈를 기반으로 스윙/단기 전략을 수립하십시오.\n"
        "### 필수 분석 요소:\n"
        "1. 현재 가격 위치 및 거래량/OBV/RSI 분석\n"
        "2. Bearish Divergence 체크: 주가 고점은 높아지는데 RSI/MFI 고점이 낮아지면 '위험' 신호 부여\n"
        "3. ADX 20 이상 및 우상향 여부 확인 (하락 중이면 '추세소멸 주의')\n"
        "4. 좋은 재료가 남아있는지 악재는 없는지 확인(재료 및 악재를 간단히 알려줄 것)\n"
        "5. 진입/목표/손절 및 세력 매집 흔적 엄격 심사\n\n"
        "단타 1위와 스윙 1위를 선정하고 타점 포함 월가 수준 브리핑을 작성해줘(반말)."
    )
 
    try:
        # GPT-4o-mini 심사
        client = OpenAI(api_key=OPENAI_API_KEY)
        res_gpt = client.chat.completions.create(
            model="gpt-4o-mini", 
            messages=[{"role":"system", "content":sys_prompt}, {"role":"user", "content":prompt_data}]
        )
        gpt_text = res_gpt.choices[0].message.content

        # Groq (Llama-3.3-70b) 심사
        res_groq = requests.post(
            "https://api.groq.com/openai/v1/chat/completions", 
            json={
                "model": "llama-3.3-70b-versatile", 
                "messages": [{"role":"system", "content":sys_prompt}, {"role":"user", "content":prompt_data}]
            },
            headers={"Authorization": f"Bearer {GROQ_API_KEY}"}
        )
        groq_text = res_groq.json()['choices'][0]['message']['content'] if res_groq.status_code == 200 else "Groq 연결 실패"

        return f"🏆 [AI 토너먼트 결승]\n\n🧠 [GPT]:\n{gpt_text}\n\n⚡ [Groq]:\n{groq_text}"
    
    except Exception as e:
        return f"토너먼트 중단: {str(e)}"

def get_ai_summary(ticker, name, tags):
    try:
        sys_prompt = (
        "당신은 세계 최고 주식 트레이더이며 대한민국 '역매공파(역배열바닥, 매집, 공구리돌파, 파동시작)' 매매법의 권위자이자 퀀트 분석가입니다. 절대 돈을 잃으면 안되는 상황이야."
        "주어진 종목 데이터에 기반해, 스윙/단기 관점에서 "
        "전략적 코멘트를 만들어주세요. "
        "다음 요소를 반드시 포함: [현재 가격 위치, 거래량·OBV·MFI·RSI 분석, 다이버전스 체크 (Bearish Divergence)를 해줘 20~40봉 동안 주가의 고점은 "
        "높아지는데 MFI나 RSI의 고점이 낮아지고 있지는 않은지 봐주고 지표의 기세가 꺽였다면 '위험' 신호를 줘. " 
        "ADX 수치가 20 이상이면서 우상향 중인지 확인해줘. 하락 중이라면 추세소멸 주의라고 신호를 줘. "
        "진입 포인트, 목표, 손절, 리스크 요인까지 종합적으로 분석을 해줘.]"
        "역배열 바닥 매집형(세력 매집봉 또는 몰래 매집하고 있는지 확인필요) 급등 패턴인지 엄격하게 심사하십시오. 억지 추천 금지! 조건 부족 시 '해당없음'이라 답하십시오."
        "단타 종목과 스윙 종목을 구분하고 기술적으로 분석해서 타점까지 포함해서 월가에서 사용될 리포트 브리핑을 간략하게 알려줘 "
        )
        client = OpenAI(api_key=OPENAI_API_KEY)
        res = client.chat.completions.create(model="gpt-4o-mini", messages=[{"role":"sys_prompt", "content":f"{name}({ticker}) ({sys_prompt})"} , {"role":"user", "content":tags}])
        return res.choices[0].message.content.strip()
    except: return "분석 불가"

def get_ai_summary_batch(stock_lines: list, issues: list = None):
    # 1. 인자 누락 및 변수 초기화 방어
    comments = "특이 이슈 없음"
    if issues:
        comments = " | ".join([i.get("comment", "분석 필요") for i in issues])
    
    # 2. 시스템 프롬프트 (가독성을 위해 튜플 결합 방식 권장)
    sys_prompt = (
        f"이슈코멘트 : {comments}\n"
        "당신은 대한민국 '역매공파(역배열바닥, 매집, 공구리돌파, 파동시작)' 매매법의 권위자이자 퀀트 분석가입니다. "
        "절대 돈을 잃으면 안되는 보수적 관점에서, 주어진 종목 데이터와 이슈를 참고해 전략적 코멘트를 작성하십시오.\n"
        "필수 포함: 현재 가격 위치, 거래량·OBV·MFI·RSI 분석, 진입 포인트, 목표, 손절, 리스크 요인.\n"
        "좋은 재료가 남아있는지 악재는 없는지 확인(재료 및 악재를 간단히 알려줄 것)\n"
        "세력 매집봉 여부를 엄격히 심사하고, 조건 부족 시 '해당없음'이라 답하십시오. "
        "단타/스윙 종목을 선정해 타점 포함 월가 리포트 형식으로 간략히 브리핑해줘(반말)."
    )

    user_prompt = (
        "다음 종목 정보를 보고 종목별 요약을 작성해줘.\n\n"
        + "\n".join(stock_lines)
        + "\n\n형식:\n종목명(코드): 요약"
    )

    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
        # [수정] 표준 API 호출 문법으로 변경
        res = client.chat.completions.create(
            model="gpt-4o", 
            messages=[
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": user_prompt}
            ],
            max_tokens=2000,
            temperature=0.7 # 분석의 창의성과 논리성을 위해 적절한 값 설정
        )

        return res.choices[0].message.content.strip()

    except Exception as e:
        print(f"[AI 배치 요약 오류] {e}")
        return "브리핑 생성 중 오류가 발생했습니다."
     
# ---------------------------------------------------------
# 🕵️‍♂️ [7] 분석 엔진 (당일 집중형 - 중복 방지)
# ---------------------------------------------------------
# ---------------------------------------------------------
# 🕵️‍♂️ [수정] 분석 엔진 (변수명 통일 및 초기화 강화)
# ---------------------------------------------------------
def analyze_final(ticker, name, historical_indices, g_env, l_env, s_map):
    # 💡 모든 변수를 함수 시작 시점에 안전하게 초기화합니다.
    s_score = 0
    f_score = 0
    whale_score = 0
    tags = []
    new_tags = []
    weather_icons = []
    storm_count = 0
    
    try:
        df = fdr.DataReader(ticker, start=(datetime.now()-timedelta(days=250)))
        if len(df) < 100: return []

        df = get_indicators(df)
        
        #조건에 맞지 않으면 처리하지 않는다.
        if df is None or df.empty:
            return []  # 또는 pd.DataFrame()
         
        # 글로벌 weather_data
        df = df.join(historical_indices, how='left').fillna(method='ffill')

        # 1. 내 종목의 섹터 확인
        my_sector = s_map.get(ticker, "일반")
    
        # 2. 우리 섹터 대장주의 상태 확인 (leader_status 맵 활용)
        current_leader_condition = l_env.get(my_sector, "Normal")
     
        # 💡 오늘의 현재가 저장 (나중에 사용)
        today_price = df.iloc[-1]['Close']
     
        row = df.iloc[-1]
        prev = df.iloc[-2]
        prev_5 = df.iloc[-5]
        prev_10 = df.iloc[-10]
        curr_idx = df.index[-1]

        # ✅ [필수] 가격 변수 정의
        close_p = row['Close']      # 당일 종가
        open_p = row['Open']        # 당일 시가
        high_p = row['High']        # 당일 고가
        low_p = row['Low']          # 당일 저가

        raw_idx = len(df) - 1
        temp_df = df.iloc[:raw_idx + 1]

        # analyze_final 함수 내부 루프 안에서
        # 최근 5일간의 진짜 거래대금 계산 (단위: 억)
        recent_avg_amount = (df['Close'] * df['Volume']).tail(5).mean() / 100000000
    
        if recent_avg_amount < 50: # 평균 거래대금 50억 미만은 탈락!
            return []

        # 💡 리턴값 5개를 정확히 받아냅니다.
        s_tag, total_m, w_streak, whale_score, twin_b = get_supply_and_money(ticker, row['Close'])
        f_tag, f_score = get_financial_health(ticker)
     
        # 💡 오늘의 현재가 저장 (나중에 사용)
        today_price = df.iloc[-1]['Close']
     
        # 1. 꼬리% 정밀 계산
        high_p, low_p, close_p, open_p = row['High'], row['Low'], row['Close'], row['Open']
        body_max = max(open_p, close_p)
        t_pct = int((high_p - body_max) / (high_p - low_p) * 100) if high_p != low_p else 0

        # 2. 기존 핵심 전술 신호 판정
        # 조건 1: 구름(Cloud) 돌파
        is_cloud_brk = prev['Close'] <= prev['Cloud_Top'] and close_p > row['Cloud_Top']
        # 조건 2: 기준선(Kijun) 돌파 
        is_kijun_sup = close_p > row['Kijun_sen'] and prev['Close'] <= prev['Kijun_sen']
        # 다이아몬드 = 둘 다 동시에!
        is_diamond = is_cloud_brk and is_kijun_sup
            
        is_super_squeeze = row['BB20_Width'] < 10 and row['BB40_Width'] < 15
        is_yeok_mae_old = close_p > row['MA112'] and prev['Close'] <= row['MA112']
        is_vol_power = row['Volume'] > row['VMA20'] * 2.5
     
        # 💡 역매공파 7가지 조건 체크 (V1)
        yeok_1_ma_aligned = (row['MA5'] > row['MA20']) and (row['MA20'] > row['MA60'])
        yeok_2_ma_converged = row['MA_Convergence'] <= 3.0
        yeok_3_bb40_squeeze = row['BB40_Width'] <= 10.0
        yeok_4_red_candle = close_p < open_p
        day_change = ((close_p - prev['Close']) / prev['Close']) * 100
        yeok_5_pullback = -5.0 <= day_change <= -1.0
        yeok_6_volume_surge = row['Volume'] >= row['VMA5'] * 1.5
        yeok_7_ma5_support = close_p >= row['MA5'] * 0.97

        # 💡 역매공파 완전체 체크
        yeok_mae_count = sum([yeok_1_ma_aligned, yeok_2_ma_converged, yeok_3_bb40_squeeze,
                             yeok_4_red_candle, yeok_5_pullback, yeok_6_volume_surge, yeok_7_ma5_support])

        # --- [역매공파 통합 7단계 로직 (V2)] ---
        # 1. [역(逆)] 역배열 바닥 탈출 (5/20 골든크로스)
        # 의미: 하락을 멈추고 단기 추세를 돌리는 첫 신호
        is_yeok = (prev['MA5'] <= prev['MA20']) and (row['MA5'] > row['MA20'])

        # 2. [매(埋)] 에너지 응축 (이평선 밀집)
        # 의미: 5, 20, 60일선이 3% 이내로 모여 에너지가 압축된 상태
        is_mae = row['MA_Convergence'] <= 3.0

        # 3. [공(空)] 공구리 돌파 (MA112 돌파) - 사령관님이 찾아낸 핵심!
        # 의미: 6개월 장기 저항선(공구리)을 종가로 뚫어버리는 순간
        is_gong = (close_p > row['MA112']) and (prev['Close'] <= row['MA112'])

        # 4. [파(破)] 파동의 시작 (BB40 상단 돌파)
        # 의미: 볼린저밴드 상단을 뚫고 변동성이 위로 터지는 시점
        is_pa = (row['Close'] > row['BB40_Upper']) and (prev['Close'] <= row['BB40_Upper'])

        # 5. [화력] 거래량 동반 (VMA5 대비 2배)
        # 의미: 가짜 돌파를 걸러내는 세력의 입성 증거
        is_volume = row['Volume'] >= row['VMA5'] * 2.0

        # 6. [안전] 적정 이격도 (100~106%)
        # 의미: 이미 너무 날아간 종목(추격매수)은 거르는 안전장치
        is_safe = 100.0 <= row['Disparity'] <= 106.0

        # 7. [수급] OBV 우상향 유지
        # 의미: 주가는 흔들어도 돈(매집세)은 빠져나가지 않는 상태
        is_obv = row['OBV_Slope'] > 0

        # 🏆 [최종 판정] 7가지 중 5가지 이상 만족 시 '정예', 7가지 모두 만족 시 'LEGEND'
        conditions = [is_yeok, is_mae, is_gong, is_pa, is_volume, is_safe, is_obv]
        match_count = sum(conditions)
      
        # 💡 매집 5가지 조건 체크
        acc_1_obv_rising = (row['OBV'] > prev_5['OBV']) and (row['OBV'] > prev_10['OBV'])
        acc_2_box_range = row['Box_Range'] <= 1.15
        acc_3_macd_golden = row['MACD'] > row['MACD_Signal']
        acc_4_rsi_healthy = 40 <= row['RSI'] <= 70
        acc_5_sto_golden = row['Sto_K'] > row['Sto_D']

        # 💡 [신규] 조용한 매집 패턴 (당신이 말한 이상적 조건!)
        silent_1_atr_low = row['ATR'] < row['ATR_MA20']  # ATR이 20일 평균 아래
        silent_2_mfi_strong = row['MFI'] > 50  # MFI 50 이상
        silent_3_mfi_rising = row['MFI'] > row['MFI_Prev5']  # MFI 상승 중
        silent_4_obv_rising = row['OBV'] > prev_5['OBV']  # OBV 상승 중
        
        # 💡 조용한 매집 완성 조건 (4개 모두 충족)
        is_silent_accumulation = (silent_1_atr_low and silent_2_mfi_strong and 
                                 silent_3_mfi_rising and silent_4_obv_rising)
      
        # --- 지표 판정 ---
        is_sto_gc = prev['Sto_D'] <= prev['Sto_SD'] and row['Sto_D'] > row['Sto_SD']
        is_vma_gc = prev['VMA5'] <= prev['VMA20'] and row['VMA5'] > row['VMA20']
        is_bb_brk = prev['Close'] <= prev['BB_Upper'] and row['Close'] > row['BB_Upper']
        is_bb40_brk = prev.get('BB40_Upper', 0) <= prev['Close'] # 예시
        
        # 멜론/노바 판정
        is_melon = twin_b and row['OBV_Slope'] > 0 and row.get('ADX', 0) > 20 and row['MACD_Hist'] > 0
        is_nova = is_sto_gc and is_vma_gc and is_bb_brk and is_melon

        # RSI
        rsi_score = row['RSI']
     
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🏆 역매공파 바닥권 (신규 지표 활용!)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        near_ma112 = row['Near_MA112'] <= 5.0
        long_bottom = row['Below_MA112_60d'] >= 40
        bottom_area = near_ma112 and long_bottom
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 💎 폭발 직전 (BB수축 + 수급)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        bb_squeeze = row['BB40_Width'] <= 10.0
        supply_strong = row['OBV_Rising'] and row['MFI_Strong']
        explosion_ready = bb_squeeze and supply_strong

        #수박지표
        is_watermelon = row['Watermelon_Signal']
        watermelon_color = row['Watermelon_Color']
        watermelon_score = row['Watermelon_Score']
        red_score = (
            int(row['OBV_Rising']) +
            int(row['MFI_Strong']) +
            int(row['Buying_Pressure'])
        )
     
        #하락기간과 횡보(공구리)기간 비교(1이상 추천)
        dante_data = calculate_dante_symmetry(temp_df)
    
        if dante_data is None:
            dante_data_ratio = 0
            dante_data_mae_jip = 0
        else:
            dante_data_ratio = dante_data['ratio']
            dante_data_mae_jip = dante_data['mae_jip']

        # 🕵️ 신규 추가: 서사 분석기 호출
        #print(f"✅ [본진] 서사 분석기 호출 : {name}")
        #sector = get_stock_sector(ticker, sector_master_map) # 섹터 판독 함수 필요
        grade, narrative, target, stop, conviction = analyze_all_narratives(
            temp_df, name, my_sector, g_env, l_env
        )

        try:
            tri_result = jongbe_triangle_combo_v3(temp_df) or {}
            tri = tri_result.get('triangle') or {}
        except Exception as e:
            print(f"🚨 jongbe_triangle_combo_v3 계산 실패: {e}")
            tri_result = {}
         
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 1. 신호 수집
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        signals = {
            # 수박지표
            'watermelon_signal': row['Watermelon_Signal'],
            'watermelon_red': row['Watermelon_Color'] == 'red',
            'watermelon_green_7d': row['Green_Days_10'] >= 7,
            
            # 폭발 직전
            'explosion_ready': (
                row['BB40_Width'] <= 10.0 and 
                row['OBV_Rising'] and 
                row['MFI_Strong']
            ),
            
            # 바닥권
            'bottom_area': (
                row['Near_MA112'] <= 5.0 and 
                row['Below_MA112_60d'] >= 40
            ),
            
            # 조용한 매집
            'silent_perfect': (
                row['ATR_Below_Days'] >= 7 and
                row['MFI_Strong_Days'] >= 7 and
                row['MFI'] > 50 and
                row['MFI'] > row['MFI_10d_ago'] and
                row['OBV_Rising'] and
                row['Box_Range'] <= 1.15
            ),
            'silent_strong': (
                row['ATR_Below_Days'] >= 5 and
                row['MFI_Strong_Days'] >= 5 and
                row['OBV_Rising']
            ),
            
            # 역매공파 돌파
            'yeok_break': (
                close_p > row['MA112'] and 
                prev['Close'] <= row['MA112']
            ),
            
            # 기타
            'volume_surge': row['Volume'] >= row['VMA20'] * 1.5,
            'obv_rising': row['OBV_Rising'],
            'mfi_strong': row['MFI_Strong'],
            # 돌반지
            'dolbanzi': row['Dolbanzi'],
            'dolbanzi_Trend_Group': row['Trend_Group'],
            'dolbanzi_Count': row['Dolbanzi_Count'],

            #독사 5-20
            'viper_hook': row['Viper_Hook'],
            'obv_bullish': row['OBV_Bullish'],
            'Real_Viper_Hook': row['Real_Viper_Hook'],
            'Golpagi_Trap': row['Golpagi_Trap'],

            # ✅ 신규: 삼각수렴 + 종베 신호 추가
            'jongbe_break':    row.get('Jongbe_Break', False),
            'triangle_signal': False,   # 아래에서 채워짐
            'triangle_apex':   None,
            'triangle_pattern': 'None',
            'dmi_cross': False,
            'dmi_ok': False,
            'MA_Convergence': row['MA_Convergence'],

            'bb_ross': False,
            'ris_div': False,
        }
     
        try:
            if tri_result is not None:
                #print(f"✅ [본진] tri_result 수집!")
                signals['triangle_signal']  = tri_result['pass']
                signals['triangle_apex']    = tri_result['apex_remain']
                signals['triangle_pattern'] = tri_result['triangle_pattern']
                signals['jongbe_ok']        = tri_result['jongbe']
                signals['explosion_ready']  = signals['explosion_ready'] or tri_result['pass']
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # 🔺 삼각수렴 + 종베 골든크로스
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                signals['dmi_cross'] = tri_result.get('triangle', {}).get('dmi_cross', False)
                signals['dmi_ok'] = tri_result.get('triangle', {}).get('dmi_ok', False)
                if signals['dmi_ok']:
                    new_tags.append(f"✅DMI")
                if tri_result['pass']:
                    new_tags.append(f"🔺삼각수렴")
                
        except Exception as e:
            print(f"🚨 tri_result 계산 실패: {e}")
            tri_result = {}
         
        if row['BB_Ross']:
            new_tags.append(f"🔺🔺Ross쌍바닥")

        if row['RSI_DIV']:
            new_tags.append(f"📊RSI DIV")
        
        # 세부 정보 추가
        if signals['watermelon_signal']:
            new_tags.append(f"🍉강도{row['Watermelon_Score']}/3")
        
        if signals['bottom_area']:
            new_tags.append(f"📍거리{row['Near_MA112']:.1f}%")
        
        if signals['silent_perfect'] or signals['silent_strong']:
            new_tags.append(f"🔇ATR{int(row['ATR_Below_Days'])}일")
            new_tags.append(f"💰MFI{int(row['MFI_Strong_Days'])}일")
 
        if row['Dolbanzi']:
            new_tags.append(f"🟡돌반지")

         
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 2. 조합 점수 계산
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        print(f"✅ [본진] 조합 점수 계산!")
        result = judge_trade_with_sequence(temp_df, signals)
        #result = calculate_combination_score(signals)
 
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 3. 추가 정보 태그
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        new_tags = result['tags'].copy()

        # ──────────────────────────────────────────────
        # 전략 스타일 분류 + 가중치 로드
        # ──────────────────────────────────────────────
        style  = classify_style(row)
        W      = STYLE_WEIGHTS[style]

        style_label = {
            "SWING": "📈스윙(5~15일)",
            "SCALP": "⚡단타(1~3일)",
            "NONE":  "➖미분류",
        }[style]
        tags.append(style_label)
      
        # 세부 정보 추가
        if signals['watermelon_signal']:
            new_tags.append(f"🍉강도{row['Watermelon_Score']}/3")
        
        if signals['bottom_area']:
            new_tags.append(f"📍거리{row['Near_MA112']:.1f}%")
        
        if signals['silent_perfect'] or signals['silent_strong']:
            new_tags.append(f"🔇ATR{int(row['ATR_Below_Days'])}일")
            new_tags.append(f"💰MFI{int(row['MFI_Strong_Days'])}일")

        if row['Dolbanzi']:
            new_tags.append(f"🟡돌반지")
     
        s_score = 100
        tags = []
      
        # 라운드넘버 정거장 매매법 => 현재가 기준 정거장 파악
        lower_rn, upper_rn = get_target_levels(row['Close'])
        avg_money = (row['Close'] * row['Volume']) # 간이 거래대금
        is_leader = avg_money >= 100000000000 # 1,000억 기준 (시장 상황에 따라 조정)
        is_1st_buy = False
        is_2nd_buy = False
        is_rapid_target = False
        is_rn_signal = False
        
        if lower_rn and upper_rn:
            # 🕵️ 조건 A: 최근 20일 내에 위 정거장(+4%)을 터치했었나?
            # (세력이 위쪽 물량을 체크하고 내려왔다는 증거)
            lookback_df = df.iloc[max(0, raw_idx-20) : raw_idx]
            hit_upper = any(lookback_df['High'] >= upper_rn * 1.04)
            
            # 🕵️ 조건 B: 현재 아래 정거장 근처(±4%)에 도달했나?
            # (분할 매수 1차 타점 진입)
            at_lower_station = lower_rn * 0.96 <= row['Close'] <= lower_rn * 1.04
            
            # 🏆 [최종 판정] '정거장 회귀' 신호
            is_rn_signal = hit_upper and at_lower_station
          
        if lower_rn:
            # 🚩 [신호 발생] 최근 20일간 정거장 대비 +30% 상단선을 터치했는가?
            # 예: 10,000원 정거장 기준 13,000원 돌파 이력 체크
            signal_line_30 = lower_rn * 1.30
            lookback_df = df.iloc[max(0, raw_idx-20) : raw_idx]
            has_surged_30 = any(lookback_df['High'] >= signal_line_30)
        
            # 🎯 [급등존 설정] Round Number ±4% 구간
            zone_upper = lower_rn * 1.04
            zone_lower = lower_rn * 0.96
        
            # 🚀 [1차 매수 타점] 급등 후 조정받아 급등존 상단 터치
            is_1st_buy = has_surged_30 and (row['Low'] <= zone_upper <= row['High'])
            
            # 🚀 [2차 매수 타점] 급등존 하단 터치
            is_2nd_buy = has_surged_30 and (row['Low'] <= zone_lower <= row['High'])
        
            if is_1st_buy:
                tags.append("🚀급등_1차타점")
                s_score += 100 # 급등주 전술이므로 높은 가점
            if is_2nd_buy:
                tags.append("🚀급등_2차타점")
                s_score += 120 # 비중을 더 싣는 구간
        
            # 결과 전송을 위한 데이터 저장
            rn_signal_data = {
                'base_rn': lower_rn,
                'is_rapid': has_surged_30,
                'status': "급등존진입" if zone_lower <= row['Close'] <= zone_upper else "관찰중"
            }
          
        # 라운드 넘버
        if is_rn_signal:
            tags.append("🚉라운드넘버")
            s_score += 70 # 강력한 매수 근거로 활용

        # --- 날씨 판정 ---
        for m_key in ['ixic', 'sp500']:
            if row.get(f'{m_key}_close', 0) > row.get(f'{m_key}_ma5', 0): weather_icons.append("☀️")
            else: weather_icons.append("🌪️"); storm_count += 1
            
        # --- 최종 점수 산산 (s_score로 통일) ---
        s_score = int(90 + (30 if is_nova else 15 if is_melon else 0))
        #s_score += (whale_score + f_score) 점수가 너무 높게 나와서 재무와 수급점수는 제외
        s_score -= (storm_count * 10)

        # 기존 시그널들
        if is_diamond:
            s_score += 30
            tags.append("💎다이아몬드")
            if t_pct < 10:
                s_score += 30
                tags.append("🔥폭발직전")
        elif is_cloud_brk:
            s_score += 30
            tags.append("☁️구름돌파")

        if is_yeok_mae_old: 
            s_score += 30
            tags.append("🏆역매공파")
                
        if is_super_squeeze: 
            s_score += 20
            tags.append("🔋초강력응축")
                
        if is_vol_power: 
            s_score += 20
            tags.append("⚡거래폭발")
          
        # 💡 매집 시그널 체크
        acc_count = sum([acc_1_obv_rising, acc_2_box_range, acc_3_macd_golden,
                       acc_4_rsi_healthy, acc_5_sto_golden])
            
        if acc_count >= 4:
            s_score += 30
            tags.append("🐋세력매집")
        elif acc_count >= 3:
            s_score += 20
            tags.append("🐋매집징후")
                
        if acc_1_obv_rising:
            s_score += 30
            tags.append("📊OBV상승")

        if is_nova:
            tags.append("🚀슈퍼타점")
        
        if is_melon:
            tags.append("🍉수박")
        
        if is_sto_gc:
            s_score += 30
            tags.append("Sto-GC")
        
        if is_vma_gc:
            tags.append("VMA-GC")

        # 💡 [신규] 조용한 매집 (최고 점수!)
        if is_silent_accumulation:
            s_score += 30
            tags.append("🤫조용한매집💰")

        # 세부 조건 태그
        if silent_1_atr_low:
            tags.append("🔇ATR수축")
        if silent_2_mfi_strong and silent_3_mfi_rising:
            tags.append("💰MFI강세")

        # RSI 정보
        rsi_val = row['RSI']
        if rsi_val >= 80:
            tags.append("🔥RSI강세")
            s_score += 10
        elif rsi_val >= 70:
            tags.append("📈RSI상승")
        elif rsi_val >= 50:
            tags.append("✅RSI중립상")
        elif rsi_val >= 30:
            tags.append("📉RSI하락")
        else:
            tags.append("❄️RSI약세")

        #수박지표
        if is_watermelon:
            s_score += 100
            tags.append("🍉수박신호")
            tags.append(f"🍉빨강전환(강도{red_score}/3)")
            tags.append(f"🍉강도{watermelon_score}/3")
        elif watermelon_color == 'red' and red_score >= 2:
            s_score += 60
            tags.append("🍉빨강상태")    
        elif row['Green_Days_10'] >= 7:
            s_score += 30
            tags.append("🍉초록축적")
      
        if 98 <= row['Disparity'] <= 104:
            s_score += 30
            tags.append("🏆LEGEND")
     
        # 기존 감점 로직
        if t_pct > 40:
            s_score -= 25
            tags.append("⚠️윗꼬리")

        # 역매공파 바닥권
        if bottom_area:
            s_score += 80
            tags.append("🏆112선바닥권")
            tags.append(f"📍거리{row['Near_MA112']:.1f}%")
        
        # 폭발 직전
        if explosion_ready:
            s_score += 90
            tags.append("💎폭발직전")
        
        # 최강 조합
        if is_watermelon and explosion_ready and bottom_area:
            s_score += 80
            tags.append("💎💎💎스윙골드")
     
        # 기상도 감점
        storm_count = sum([1 for m in ['ixic', 'sp500'] if row[f'{m}_close'] <= row[f'{m}_ma5']])
        s_score -= (storm_count * 20)
        s_score -= max(0, int((row['Disparity']-108)*5))

        if not tags: return []

        # 💡 NameError 방지: print문에서 s_score 사용
        print(f"✅ {name} 포착! 점수: {s_score} 태그: {tags}")
        
        return [{
            '날짜': curr_idx.strftime('%Y-%m-%d'),
            '종목명': name, 'code': ticker,
            'N등급': f"{result['type']}{result['grade']}",
            'N조합': result['combination'],
            'N점수': result['score'],
            'N구분': " ".join(new_tags),
            '👑등급': grade,              # 👈 서사 엔진 결과물 1
            '📜서사히스토리': narrative,    # 👈 서사 엔진 결과물 2
            '확신점수': conviction,        # 👈 서사 엔진 결과물 3
            '🎯목표타점': int(target),      # 👈 서사 기반 타점
            '🚨손절가': int(stop),         # 👈 서사 기반 손절가
            '기상': "☀️" * (2-storm_count) + "🌪️" * storm_count,
            '안전점수': int(max(0, s_score + whale_score)),
            'RSI': int(max(0, rsi_score)),
            '점수': int(s_score), # 구글 시트 전송용
            '에너지': "🔋" if row['MACD_Hist'] > 0 else "🪫",
            '현재가': int(row['Close']),
            '구분': " ".join(tags),
            '재무': f_tag, '수급': s_tag,
            '이격': int(row['Disparity']),
            'BB40': f"{row['BB40_Width']:.1f}",
            'MA수렴': f"{row['MA_Convergence']:.1f}",
            '매집': f"{acc_count}/5",
            'OBV기울기': int(row['OBV_Slope']),
            '꼬리%': 0 # 필요 시 계산식 추가
        }]
    except Exception as e:
        import traceback
        print(f"🚨 {name} 분석 중 치명적 에러:\n{traceback.format_exc()}")
        return []

# 스타일별 가중치 테이블
# 기준점수(base) 대비 각 신호에 얼마나 가중치를 줄지 정의
STYLE_WEIGHTS = {
    "SWING": {
        # 핵심 (스윙의 본질 = 응축 후 폭발)
        'explosion_ready': 150,   # BB수축 + 수급 → 스윙 핵심
        'bottom_area':     120,   # 바닥권 확인 → 스윙 핵심
        'silent_perfect':  130,   # 조용한 매집 완전 → 스윙 핵심
        'silent_strong':    80,
        'silent_weak':      40,
        'bb_squeeze_bonus': 50,   # 삼각수렴 고신뢰 보너스
        'ma_convergence':   40,   # MA수렴 자체 보너스
        # 보조
        'watermelon':       70,   # 수박신호 (스윙에선 보조)
        'watermelon_red':   50,
        'volume_surge':     20,   # 거래량 폭발 (스윙엔 덜 중요)
        'adx_strong':       10,   # ADX (스윙엔 별로)
        # 최강 조합
        'swing_gold':      100,   # 수박 + 폭발직전 + 바닥권 동시
        # 감점
        'high_tail':       -25,
        'disparity_over':    -5,  # (Disparity-108) 당 감점
    },
    "SCALP": {
        # 핵심 (단타의 본질 = 지금 당장 터지는 중)
        'explosion_ready':  50,   # 단타엔 덜 중요 (이미 터졌어야)
        'bottom_area':      20,   # 단타엔 무관
        'silent_perfect':   30,
        'silent_strong':    20,
        'silent_weak':      10,
        'bb_squeeze_bonus': 10,
        'ma_convergence':   10,
        # 핵심
        'watermelon':      150,   # 수박신호 → 단타 핵심 (지금 터지는 중)
        'watermelon_red':  100,
        'volume_surge':     80,   # 거래량 폭발 → 단타 핵심
        'adx_strong':       80,   # ADX 강세 → 단타 핵심
        # 최강 조합
        'swing_gold':       40,
        # 감점 (단타는 손절 빠르므로 윗꼬리 더 치명적)
        'high_tail':       -40,
        'disparity_over':   -8,
    },
    "NONE": {
        # 기본값 (기존 점수 그대로 유지)
        'explosion_ready':  90,
        'bottom_area':      80,
        'silent_perfect':  100,
        'silent_strong':    60,
        'silent_weak':      30,
        'bb_squeeze_bonus': 20,
        'ma_convergence':    0,
        'watermelon':      100,
        'watermelon_red':   60,
        'volume_surge':     30,
        'adx_strong':       20,
        'swing_gold':       80,
        'high_tail':       -25,
        'disparity_over':   -5,
    },
}
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 전략 스타일 분류 (단타 1~3일 / 스윙 5~15일)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def classify_style(row):
    """
    SWING : BB40 수축 + 이평 수렴 + ADX 미발동 → 에너지 응축 대기 구간 (5~15일)
    SCALP : 변동성 적당 + ADX 강세 → 추세 이미 발동, 올라타는 구간 (1~3일)
    NONE  : 두 조건 모두 미충족
    """
    vol_ratio = row['ATR'] / row['Close'] if row['Close'] > 0 else 0

    # 1순위: 스윙 (응축 → 폭발 직전)
    if (row['BB40_Width'] < 12
            and row['MA_Convergence'] < 3
            and row['ADX'] < 25):
        return "SWING"

    # 2순위: 단타 (추세 이미 발동)
    elif (0.02 <= vol_ratio <= 0.05
          and row['ADX'] >= 25):
        return "SCALP"

    return "NONE"
  
def get_target_levels(current_price):
    """현재가 기준 위/아래 정거장을 찾아주는 함수"""
    # 현재가보다 큰 RN들 중 가장 작은 것이 '위 정거장'
    upper_rns = [rn for rn in RN_LIST if rn > current_price]
    # 현재가보다 작은 RN들 중 가장 큰 것이 '아래 정거장'
    lower_rns = [rn for rn in RN_LIST if rn <= current_price]
    
    upper = upper_rns[0] if upper_rns else None
    lower = lower_rns[-1] if lower_rns else None
    return lower, upper

# ---------------------------------------------------------
# 🕵️‍♂️ [7-1] 주간 분석 엔진
# ---------------------------------------------------------
def analyze_weekly_trend(ticker, name):
    """
    사령관님, 일봉의 잔파도를 무시하고 주봉으로 거대한 추세를 읽습니다.
    주말에 가동하여 차주 월요일의 공략주를 선정하는 전술입니다.
    """
    try:
        # 1. 주간 데이터 생성을 위해 충분한 과거 데이터 로드
        df_daily = fdr.DataReader(ticker, start=(datetime.now()-timedelta(days=730))) # 2년치
        if len(df_daily) < 200: return []

        # 2. 💡 일봉 데이터를 주봉(Weekly)으로 변환
        # 'W-MON'은 월요일 기준으로 한 주를 묶습니다.
        df = df_daily.resample('W-MON').agg({
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum'
        })

        # 3. 주간 보조지표 계산 (주봉 MA, BB, OBV)
        df['MA20_W'] = df['Close'].rolling(window=20).mean()
        df['BB20_Upper_W'] = df['MA20_W'] + (df['Close'].rolling(window=20).std() * 2)
        
        # 주간 OBV 계산
        df['OBV_W'] = (np.sign(df['Close'].diff()) * df['Volume']).fillna(0).cumsum()
        df['OBV_MA10_W'] = df['OBV_W'].rolling(window=10).mean()

        row = df.iloc[-1]   # 이번 주 (혹은 가장 최근 종료된 주)
        prev = df.iloc[-2]  # 지난 주

        # 🎯 [핵심] 주간 역매공파 신호 판정
        # 1. 공구리 돌파: 주봉이 20주 볼린저밴드 상단을 돌파했는가?
        is_weekly_break = prev['Close'] <= prev['BB20_Upper_W'] and row['Close'] > row['BB20_Upper_W']
        
        # 2. 주간 매집: 주간 OBV가 10주 평균선 위에 있는가?
        is_weekly_acc = row['OBV_W'] > row['OBV_MA10_W']
        
        # 3. 주간 골든크로스: 5주선이 20주선을 돌파하는가?
        df['MA5_W'] = df['Close'].rolling(window=5).mean()
        is_weekly_gc = prev['MA5_W'] <= prev['MA20_W'] and row['MA5_W'] > row['MA20_W']

        tags = []
        w_score = 100
        
        if is_weekly_break: tags.append("🚨주봉돌파"); w_score += 30
        if is_weekly_acc: tags.append("🌊주간매집"); w_score += 15
        if is_weekly_gc: tags.append("✨주간GC"); w_score += 15

        if not tags: return []

        return [{
            '날짜': df.index[-1].strftime('%Y-%m-%d'),
            '종목명': f"[주간] {name}",
            '주간화력': w_score,
            '이격도_W': int((row['Close']/row['MA20_W'])*100),
            '구분': " ".join(tags),
            '진단': "주봉 단위 강력 추세 전환 포착"
        }]
    except Exception as e:
        return []

# ---------------------------------------------------------
# 🚀 [8] 메인 실행 (전략 사령부 가동)
# ---------------------------------------------------------
if __name__ == "__main__":
    print("🚀 전략 사령부 가동 시작...")
    
    client = OpenAI()
    models = client.models.list()
    for m in models.data:
        print(m.id)
     
    # 💡 1. 전쟁 시작 전 '대장주 지도'와 '그들의 상태'를 딱 한 번만 생성
    # leader_map: {섹터: 코드}, leader_status: {섹터: 강세/침체}
    global_env, leader_env = get_global_and_leader_status()

    # 2. 전 종목 리스트 로드 및 명찰 강제 통일
    try:
        df_krx = load_krx_listing_safe()
        df_krx['Code'] = (
            df_krx['Code']
            .fillna('')
            .astype(str)
            .str.replace('.0', '', regex=False)
            .str.zfill(6)
        )
        
        # 💡 [핵심] 첫 번째 열은 'Code', 두 번째 열은 'Name'으로 강제 개명
        # KRX 데이터 구조상 보통 0번이 코드, 1번이 종목명입니다.
        #df_krx.columns.values[0] = target_stocks['Code']
        #df_krx.columns.values[1] = target_stocks['Name']
        
        # 섹터 컬럼도 있으면 'Sector'로 통일
        s_col = next((c for c in ['Sector', 'Industry', '업종'] if c in df_krx.columns), None)
        if s_col:
            df_krx = df_krx.rename(columns={s_col: 'Sector'})
            sector_master_map = df_krx.set_index('Code')['Sector'].to_dict()
        else:
            sector_master_map = {k: '일반' for k in df_krx['Code']}
            
        print(f"✅ [본진] 명찰 통일 완료: {len(df_krx)}개 종목 로드")

    except Exception as e:
        print(f"🚨 [본진] 데이터 로드 실패: {e}")
        sector_master_map = {}
        # 여기서 죽지 않게 빈 데이터프레임이라도 생성
        df_krx = pd.DataFrame(columns=['Code', 'Name', 'Sector'])
 
    m_ndx = get_safe_macro('^IXIC', '나스닥')
    m_sp5 = get_safe_macro('^GSPC', 'S&P500')
    m_vix = get_safe_macro('^VIX', 'VIX공포')
    m_fx  = get_safe_macro('USD/KRW', '달러환율')
    macro_status = {'nasdaq': m_ndx, 'sp500': m_sp5, 'vix': m_vix, 'fx': m_fx , 'kospi': {get_index_investor_data('KOSPI')}}

    print("\n" + "🌍 " * 5 + "[ 글로벌 사령부 통합 관제 센터 ]" + " 🌍" * 5)
    print(f"🇺🇸 {m_ndx['text']} | {m_sp5['text']} | ⚠️ {m_vix['text']}")
    print(f"💵 {m_fx['text']} | 🇰🇷 KOSPI 수급: {get_index_investor_data('KOSPI')}")
    print("=" * 115)
    
    # 1. 시황 및 차트 준비
    imgs = [create_index_chart('KS11', 'KOSPI'), create_index_chart('IXIC', 'NASDAQ')]
    # 2. 시장 이슈 분석
    issues = analyze_market_issues()
    briefing = get_market_briefing(issues)
    
    # 2. 전 종목 스캔
    #df_krx = fdr.StockListing('KRX')
    # 2. 국내주식 정제 및 타겟팅
    df_clean = df_krx[df_krx['Market'].isin(['KOSPI', 'KOSDAQ','코스닥','유가'])]
    df_clean['Name'] = df_clean['Name'].astype(str)
    df_clean = df_clean[~df_clean['Name'].str.contains('ETF|ETN|스팩|제[0-9]+호|우$|우A|우B|우C')]
    # ✅ 안전한 코드 (인덱스 동기화)
    # 💰 거래대금 상위 추출 (국내)
    if 'Amount' in df_clean.columns:
        sorted_df = df_clean.sort_values(by='Amount', ascending=False).head(TOP_N)
    else:
        sorted_df = df_clean.copy()
    
    target_dict = dict(zip(sorted_df['Code'], sorted_df['Name']))

    weather_data = prepare_historical_weather()
    sector_dict = {} # (필요시 추가)
    
    all_hits = []
    with ThreadPoolExecutor(max_workers=15) as executor:
        results = list(executor.map(
            lambda p: analyze_final(p[0], p[1], weather_data, global_env, leader_env, sector_master_map), 
            zip(sorted_df['Code'], sorted_df['Name'])
        ))
        for r in results:
            if r:
                # 💡 [신규] 포착된 종목에 즉시 체급(Tier) 및 시총 데이터 주입
                for hit in r:
                    # hit['종목코드']가 있다고 가정, 없으면 ticker를 찾아야 함
                    name = hit['종목명']
                    ticker_code = hit.get('코드')
                    all_hits.append(hit)
        
if all_hits:
    # 1. [정렬] 전체 검색 결과 점수순 정렬
    all_hits_sorted = sorted(all_hits, key=lambda x: x['N점수'], reverse=True)
    
    # 2. [정예 선발] 상위 30개 추출 (AI 심층 분석 대상)
    #ai_candidates = all_hits_sorted[:30]
    ai_candidates = pd.DataFrame(all_hits_sorted)
    ai_candidates = ai_candidates.sort_values(by='N점수', ascending=False)[:30].copy()
    # 3. [AI 분석] 상위 30개 종목에만 AI 지능 주입
    print(f"🧠 상위 30개 종목 AI 심층 분석 중... (나머지는 데이터만 기록)")
    tournament_report = run_ai_tournament(ai_candidates, issues)

    # 상위 30개에만 AI 한줄평과 토너먼트 리포트 삽입
    lines = []
    
    def safe_int(x, default=0):
        try:
            return int(float(x))
        except:
            return default
    
    def safe_float(x, default=0.0):
        try:
            return float(x)
        except:
            return default
    
    for _, item in ai_candidates.iterrows():
        line = (
            f"{item['종목명']}({item['code']}): {item['구분']}, "
            f"수급:{item['수급']}, N구분:{item['N구분']}, "
            f"이격:{safe_int(item['이격'])}, "
            f"BB40:{safe_float(item['BB40']):.1f}, "
            f"MA수렴:{safe_float(item['MA수렴']):.1f}, "
            f"OBV기울기:{safe_int(item['OBV기울기'])}, "
            f"RSI:{safe_int(max(0, safe_float(item['RSI'])))}"
            f"이 종목({item['종목명']}, {item['code']})에 대해 투자 전략 관점에서 "
            f"3~5문장 정도로 고급 코멘트를 만들어주세요. "
            f"읽는 사람이 바로 이해할 수 있는 스토리텔링 형식으로 작성."
        )
        lines.append(line)
    
    # 🔥 AI 한 번만 호출
    ai_result_text = get_ai_summary_batch(lines, issues)
    ai_map = {}
   
    for line in ai_result_text.splitlines():
        if ":" in line:
            key, val = line.split(":", 1)
            ai_map[key.strip()] = val.strip()
    
    for idx, item in ai_candidates.iterrows():
        key = f"{item['종목명']}({item['code']})"
        ai_candidates.loc[idx, "ai_tip"] = ai_map.get(key, "")
    
    # 4. [텔레그램 전송] 상위 15개 정예만 골라 발송
    telegram_targets = ai_candidates[:15]
    
    MAX_CHAR = 3800
    current_msg = f"{briefing}\n\n📢 [오늘의 실시간 TOP 15]\n\n"
    
    for _, item in telegram_targets.iterrows():
        entry = (f"⭐{item['N등급']} | {item['👑등급']}점 [{item['종목명']}]\n"
                 f"- {item['N조합']} | {item['N구분']}\n"
                 f"- {item['기상']} | {item['구분']}\n"
                 f"- {item['에너지']} | {item['매집']}\n"
                 f"- {item['📜서사히스토리']}\n"
                 f"- 재무: {item['재무']} | 수급: {item['수급']}\n"
                 f"- MA수렴: {safe_float(item['MA수렴']):.1f} | 이격: {item['이격']}\n"
                 f"- OBV기울기: {item['OBV기울기']} | RSI: {item['RSI']}\n"
                 f"💡 {item.get('ai_tip', '분석전')}\n"
                 f"----------------------------\n")
     
        if len(current_msg) + len(entry) > MAX_CHAR:
            send_telegram_photo(current_msg, imgs if imgs else [])
            imgs = []
            current_msg = "📢 [오늘의 추천주 - 이어서]\n\n" + entry
            print(f"{current_msg}")
        else:
            current_msg += entry

    # AI 토너먼트 결과 전송
    final_block = f"\n{tournament_report}"
    if len(current_msg) + len(final_block) > MAX_CHAR:
        send_telegram_photo(current_msg, imgs if imgs else [])
        send_telegram_photo(f"🏆 [AI 토너먼트 최종 결과]\n{final_block}", [])
    else:
        current_msg += final_block
        send_telegram_photo(current_msg, imgs if imgs else [])

    # 5. [구글 시트 전수 저장] 스캔된 모든 종목(all_hits_sorted)을 시트로 전송!
    try:
        # AI 분석이 안 된 종목들은 get()을 통해 빈 값으로 처리됩니다.
        update_google_sheet(all_hits_sorted, TODAY_STR,tournament_report)
        print(f"💾 총 {len(all_hits_sorted)}개 종목 전수 기록 완료! (상위 30개 AI분석 포함)")
    except Exception as e:
        print(f"🚨 시트 업데이트 실패: {e}")

    print("✅ 작전 종료: 전수 기록 완료 및 정예 15건 보고 완료!")
