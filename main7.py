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
from functools import lru_cache  # ✅ FIX 1: 캐시용

try: from openai import OpenAI
except: OpenAI = None

from google_sheet_manager import update_google_sheet, update_ai_briefing_sheet
import io
import warnings
warnings.filterwarnings('ignore', category=FutureWarning)

# =================================================
# ⚙️ [1. 필수 설정]
# =================================================
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID_LIST = os.environ.get('TELEGRAM_CHAT_ID', '').split(',')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY') 
GROQ_API_KEY = os.environ.get('GROQ_API_KEY')     

TEST_MODE = True

KST = pytz.timezone('Asia/Seoul')
current_time = datetime.now(KST)
NOW = current_time - timedelta(days=1) if current_time.hour < 8 else current_time
TODAY_STR = NOW.strftime('%Y-%m-%d')

REAL_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Referer': 'https://finance.naver.com/',
    'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7'
}

RN_LIST = [500, 1000, 1500, 2000, 3000, 5000, 7500, 10000, 15000, 20000, 
           30000, 50000, 75000, 100000, 150000, 200000, 300000, 500000, 
           750000, 1000000, 1500000]

SCAN_DAYS, TOP_N = 1, 550
MIN_MARCAP = 1000000000 
STOP_LOSS_PCT = -5.0
WHALE_THRESHOLD = 50 

START_DATE = (datetime.now() - timedelta(days=600)).strftime('%Y-%m-%d')
END_DATE_STR = datetime.now().strftime('%Y%m%d')
START_DATE_STR = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')

RECENT_AVG_AMOUNT_1 = 150
RECENT_AVG_AMOUNT_2 = 350
ROSS_BAND_TOLERANCE = 1.03
RSI_LOW_TOLERANCE   = 1.03

print(f"📡 [Ver 27.1] 성능 최적화 오버홀 가동...")

# =================================================
# ✅ FIX 1: HTTP 요청 함수에 딕셔너리 캐시 적용
# 종목당 2번 × 550종목 = 1,100번 요청 → 중복 요청 제거
# =================================================
_supply_cache = {}
_financial_cache = {}

def get_supply_and_money(code, price):
    if code in _supply_cache:
        return _supply_cache[code]
    try:
        url = f"https://finance.naver.com/item/frgn.naver?code={code}"
        res = requests.get(url, headers=REAL_HEADERS, timeout=5)
        res.encoding = 'euc-kr'
        df = pd.read_html(res.text, match='날짜')[0].dropna().head(10)
        
        new_cols = ['_'.join(col) if isinstance(col, tuple) else col for col in df.columns]
        df.columns = new_cols
        
        inst_col = next((c for c in df.columns if '기관' in c and '순매매' in c), None)
        frgn_col = next((c for c in df.columns if '외국인' in c and '순매매' in c), None)
        
        inst_qty = [int(float(str(v).replace(',', ''))) for v in df[inst_col].values]
        frgn_qty = [int(float(str(v).replace(',', ''))) for v in df[frgn_col].values]
        
        def get_streak(data):
            c = 0
            for v in data:
                if v > 0: c += 1
                else: break
            return c
            
        i_s, f_s = get_streak(inst_qty), get_streak(frgn_qty)
        inst_m = round((inst_qty[0] * price) / 100000000)
        frgn_m = round((frgn_qty[0] * price) / 100000000)
        total_m = abs(inst_m) + abs(frgn_m)
        
        twin_b = (inst_qty[0] > 0 and frgn_qty[0] > 0)
        leader = "🤝쌍끌" if twin_b else ("🔴기관" if inst_m > frgn_m else "🔵외인")
        
        whale_streak = 0
        for k in range(len(df)):
            if (abs(inst_qty[k]) + abs(frgn_qty[k])) * price / 100000000 >= 10:
                whale_streak += 1
            else: break
        
        w_score = (total_m // 2) + (3 if whale_streak >= 3 else 0)
        result = f"{leader}({i_s}/{f_s})", total_m, whale_streak, w_score, twin_b
        _supply_cache[code] = result
        return result
    except: 
        result = "⚠️오류", 0, 0, 0, False
        _supply_cache[code] = result
        return result

def get_financial_health(code):
    if code in _financial_cache:
        return _financial_cache[code]
    try:
        url = f"https://finance.naver.com/item/main.naver?code={code}"
        res = requests.get(url, headers=REAL_HEADERS, timeout=5)
        dfs = pd.read_html(res.text)
        df_fin = dfs[3]; df_fin.columns = df_fin.columns.get_level_values(1)
        profit = str(df_fin.iloc[1, -2]).replace(',', '')
        debt = str(df_fin.iloc[6, -2]).replace(',', '')
        p_val = float(profit) if profit != 'nan' else 0
        d_val = float(debt) if debt != 'nan' else 999
        
        f_score = (1 if p_val > 0 else 0) + (1 if d_val < 150 else 0)
        tag = "S(우량)" if f_score == 2 else ("A(양호)" if f_score == 1 else "C(주의)")
        result = tag, f_score
        _financial_cache[code] = result
        return result
    except:
        result = "N(미비)", 0
        _financial_cache[code] = result
        return result

# =================================================
# ✅ FIX 5: load_krx_listing_safe NameError 수정
# except 블록에서 df_krx가 미정의 상태로 반환되던 버그
# =================================================
def load_krx_listing_safe():
    try:
        SHEET_ID = "13Esd11iwgzLN7opMYobQ3ee6huHs1FDEbyeb3Djnu6o"
        GID = "1238448456"
        url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={GID}"
        df = pd.read_csv(url, encoding="utf-8", engine="python")
        
        if df is None or df.empty:
            print("📡 FDR KRX 시도...")
            df = fdr.StockListing('KRX')    
            
        if df is None or df.empty:
            raise ValueError("빈 데이터")
            
        print("✅ FDR 성공")
        return df
    except Exception as e:
        print(f"⚠️ FDR 실패 → pykrx 대체 사용 ({e})")
        try:
            # ✅ FIX: except 블록 안에서 df_krx를 직접 생성
            df_krx = pd.DataFrame(
                stock.get_market_ticker_list(market="ALL"),
                columns=['Code']
            )
            df_krx['Name'] = df_krx['Code'].apply(
                lambda c: stock.get_market_ticker_name(c)
            )
            df_krx['Market'] = 'KOSPI'
            return df_krx
        except Exception as e2:
            print(f"🚨 pykrx도 실패: {e2}")
            return pd.DataFrame(columns=['Code', 'Name', 'Market'])

def get_stock_sector(ticker, sector_map):
    raw_sector = sector_map.get(ticker, "일반")
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
    seq_ok = judge_yeok_break_sequence_v2(df)
    signals = signals.copy()
    signals['yeok_break'] = seq_ok
    result = calculate_combination_score(signals)
    if seq_ok:
        result['tags'].append('🧬시퀀스확인')
    result['sequence'] = seq_ok
    return result

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 🎯 조합 중심 점수 산정 시스템
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
COMBO_TABLE = [
    {
        'grade': 'GOD+', 'score': 10001, 'type': '🌌',
        'combination': '🌌🔺💍독사삼각돌반지',
        'tags': ['🔺꼭지임박', '🐍독사대가리', '💍200일돌파', '🍉수급폭발', '🚀역대급시그널'],
        'cond': lambda e: (
            e.get('triangle_signal') and
            isinstance(e.get('triangle_apex'), (int,float)) and 0 <= e['triangle_apex'] <= 3 and
            e.get('viper_hook') and e.get('Real_Viper_Hook') and e.get('watermelon_signal') and e.get('dolbanzi')
        ),
    },
    {
        'grade': 'GOD', 'score': 10000, 'type': '🌌',
        'combination': '🌌🍉💍독사품은수박돌반지',
        'tags': ['🚀대시세확정', '💥200일선폭파', '🐍단기개미털기완료', '🍉수급대폭발'],
        'cond': lambda e: e.get('viper_hook') and e.get('dolbanzi') and e.get('watermelon_signal'),
    },
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
    {
        'grade': 'SSS', 'score': 500, 'type': '👑',
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
    {
        'grade': 'SS', 'score': 470, 'type': '👑',
        'combination': '🟣BB40재안착눌림목',
        'tags': ['🟣BB40하단재안착', '📈RSI-DIV', '🏹중기눌림핵심'],
        'cond': lambda e: e.get('bb40_reclaim_rsi_div'),
    },
    {
        'grade': 'S+', 'score': 445, 'type': '👑',
        'combination': '🟣BB40재안착',
        'tags': ['🟣BB40하단재안착', '📉중기눌림목'],
        'cond': lambda e: e.get('bb40_ross'),
    },
    {
        'grade': 'S+', 'score': 435, 'type': '👑',
        'combination': '🟣BB40 RSI-DIV',
        'tags': ['🟣BB40구간', '📈RSI-DIV'],
        'cond': lambda e: e.get('bb40_rsi_div'),
    },
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

def calculate_combination_score(signals):
    effective = signals.copy()
    if effective.get('silent_perfect'):
        effective['silent_strong'] = True

    style = effective.get('style', 'NONE')
    W     = STYLE_WEIGHTS.get(style, STYLE_WEIGHTS['NONE'])

    matched = []
    for combo in COMBO_TABLE:
        try:
            if not combo['cond'](effective):
                continue
        except Exception:
            continue

        base_score = combo['score_fn'](effective) if 'score_fn' in combo else combo['score']
        extra_tags = combo['tag_fn'](effective)   if 'tag_fn'  in combo else []

        matched.append({
            'score':       base_score,
            'grade':       combo['grade'],
            'combination': combo['combination'],
            'tags':        combo['tags'] + extra_tags,
            'type':        combo['type'],
        })

    if matched:
        best = max(matched, key=lambda x: x['score'])
        best['score'] = _apply_style_bonus(best, style, W)
        return best

    tags, bonus = [], 0
    if effective.get('obv_rising'):   bonus += 30; tags.append('📊OBV')
    if effective.get('mfi_strong'):   bonus += 20; tags.append('💰MFI')
    if effective.get('volume_surge'): bonus += 10; tags.append('⚡거래량')

    return {'score': 100 + bonus, 'grade': 'D', 'combination': '🔍기본', 'tags': tags, 'type': None}

def _apply_style_bonus(best, style, W):
    score = best['score']
    if style == 'SWING':
        if any(k in best['combination'] for k in ['폭발', '바닥', '매집', '수렴']):
            score += 30
    elif style == 'SCALP':
        if any(k in best['combination'] for k in ['수박', '돌파', '거래량', '골파기']):
            score += 30
        if any(k in best['combination'] for k in ['바닥', '매집완성']):
            score -= 20
    return score

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

def check_bb40_ross(curr: pd.Series, past: pd.DataFrame):
    """
    BB40 하단 이탈 후 재안착 판단
    - 과거 구간에서 BB40_Lower 하향 이탈이 있었는지
    - 이후 다시 BB40_Lower 위로 복귀한 적이 있는지
    - 현재봉이 BB40_Lower 근처에서 종가 기준 위에 안착했는지
    """
    if past.empty or 'BB40_Lower' not in past.columns or past['BB40_Lower'].isna().all():
        return False, "BB40 데이터 부족"

    bb40_low = past['BB40_Lower']
    outside_mask = past['Low'] < bb40_low

    if not outside_mask.any():
        return False, "BB40 1차 저점 없음"

    first_idx = outside_mask.values.argmax()
    after_first = past.iloc[first_idx + 1:]

    if after_first.empty:
        return False, "BB40 반등 확인 구간 부족"

    rebound = (after_first['Close'] > after_first['BB40_Lower']).any()
    near_band = curr['Low'] <= curr['BB40_Lower'] * ROSS_BAND_TOLERANCE
    close_above = curr['Close'] > curr['BB40_Lower']

    passed = rebound and near_band and close_above
    return passed, f"BB40반등:{rebound}, 저가밴드근접:{near_band}, 종가밴드위:{close_above}"


def check_bb40_rsi_div(curr: pd.Series, past: pd.DataFrame):
    """
    BB40 관점 RSI 다이버전스
    - 과거 BB40 하단 이탈 봉들만 후보로 봄
    - 현재 저점이 과거 저점 부근이거나 더 낮고
    - RSI는 과거보다 높으면 다이버전스로 판단
    """
    if past.empty or 'BB40_Lower' not in past.columns or past['RSI'].isna().all() or pd.isna(curr['RSI']):
        return False, "RSI 데이터 부족"

    bb40_break_df = past[past['Low'] < past['BB40_Lower']].copy()

    if bb40_break_df.empty:
        return False, "BB40 하단 이탈 이력 없음"

    min_price_idx = bb40_break_df['Low'].idxmin()
    min_price_past = bb40_break_df.loc[min_price_idx, 'Low']
    min_rsi_past = bb40_break_df.loc[min_price_idx, 'RSI']

    if pd.isna(min_rsi_past):
        min_rsi_past = bb40_break_df['RSI'].min()

    price_similar = curr['Low'] <= min_price_past * RSI_LOW_TOLERANCE
    rsi_higher = curr['RSI'] > min_rsi_past

    passed = price_similar and rsi_higher
    return passed, f"BB40저점:{curr['Low']:.0f}(과거:{min_price_past:.0f}), RSI:{curr['RSI']:.1f}(과거:{min_rsi_past:.1f})"


def check_bb40_reclaim_rsi_div(curr: pd.Series, past: pd.DataFrame):
    """
    최종 결합형:
    BB40 하단 이탈 후 재안착 + RSI DIV
    """
    bb40_ross, ross_msg = check_bb40_ross(curr, past)
    bb40_div, div_msg = check_bb40_rsi_div(curr, past)

    passed = bb40_ross and bb40_div
    return passed, f"[BB40_Ross] {ross_msg} | [BB40_RSI_DIV] {div_msg}"

# ---------------------------------------------------------
# 📈 [4] 기술적 분석 지표
# ---------------------------------------------------------
def get_indicators(df):
    df = df.copy()
    count = len(df)

    recent_avg_amount = (df['Close'] * df['Volume']).tail(5).mean() / 100_000_000
    ma20_amount       = (df['Close'] * df['Volume']).tail(20).mean() / 100_000_000

    amount_ok = (
        (recent_avg_amount >= RECENT_AVG_AMOUNT_1 and recent_avg_amount >= ma20_amount * 1.5)
        or recent_avg_amount >= RECENT_AVG_AMOUNT_2
    )
    if not amount_ok:
        return None

    high  = df['High']
    low   = df['Low']
    close = df['Close']

    for n in [5, 10, 20, 40, 60, 112, 224, 448]:
        df[f'MA{n}']    = close.rolling(window=min(count, n)).mean()
        df[f'VMA{n}']   = df['Volume'].rolling(window=min(count, n)).mean()
        df[f'Slope{n}'] = (df[f'MA{n}'] - df[f'MA{n}'].shift(3)) / df[f'MA{n}'].shift(3) * 100

    df['MA20_slope'] = (df['MA20'] - df['MA20'].shift(5)) / (df['MA20'].shift(5) + 1e-9) * 100
    df['MA40_slope'] = (df['MA40'] - df['MA40'].shift(5)) / (df['MA40'].shift(5) + 1e-9) * 100

    std20 = close.rolling(20).std()
    std40 = close.rolling(40).std()

    df['BB_Upper']      = df['MA20'] + std20 * 2
    df['BB_Lower']      = df['MA20'] - std20 * 2
    df['BB20_Width']    = std20 * 4 / df['MA20'] * 100
    df['BB40_Upper']    = df['MA40'] + std40 * 2
    df['BB40_Lower']    = df['MA40'] - std40 * 2
    df['BB40_Width']    = std40 * 4 / df['MA40'] * 100
    df['BB40_PercentB'] = (close - df['BB40_Lower']) / (df['BB40_Upper'] - df['BB40_Lower'])
    df['BB_UP']  = df['BB40_Upper']
    df['BB_LOW'] = df['BB_Lower']

    df['Disparity']      = (close / df['MA20']) * 100
    df['MA_Convergence'] = abs(df['MA20'] - df['MA60']) / df['MA60'] * 100
    df['Box_Range']      = high.rolling(10).max() / low.rolling(10).min()
    
    # ✅ TR 1회 계산 후 전체 재사용
    tr = pd.concat([
        high - low,
        abs(high - close.shift(1)),
        abs(low  - close.shift(1))
    ], axis=1).max(axis=1)

    dm_plus  = (high - high.shift(1)).clip(lower=0)
    dm_minus = (low.shift(1) - low).clip(lower=0)
    tr14     = tr.rolling(14).sum()

    df['pDI'] = dm_plus.rolling(14).sum()  / tr14 * 100
    df['mDI'] = dm_minus.rolling(14).sum() / tr14 * 100
    df['ADX'] = ((abs(df['pDI'] - df['mDI']) / (df['pDI'] + df['mDI'])) * 100).rolling(14).mean()

    df['ATR']            = tr.rolling(14).mean()
    df['ATR_MA20']       = df['ATR'].rolling(20).mean()
    df['ATR_Below_MA']   = (df['ATR'] < df['ATR_MA20']).astype(int)
    df['ATR_Below_Days'] = df['ATR_Below_MA'].rolling(10).sum()

    df['Tenkan_sen'] = (high.rolling(9).max()  + low.rolling(9).min())  / 2
    df['Kijun_sen']  = (high.rolling(26).max() + low.rolling(26).min()) / 2
    df['Span_A']     = ((df['Tenkan_sen'] + df['Kijun_sen']) / 2).shift(26)
    df['Span_B']     = ((high.rolling(52).max() + low.rolling(52).min()) / 2).shift(26)
    df['Cloud_Top']  = df[['Span_A', 'Span_B']].max(axis=1)

    l_min, h_max = low.rolling(12).min(), high.rolling(12).max()
    df['Sto_K']  = (close - l_min) / (h_max - l_min) * 100
    df['Sto_D']  = df['Sto_K'].rolling(5).mean()
    df['Sto_SD'] = df['Sto_D'].rolling(5).mean()

    ema12             = close.ewm(span=12).mean()
    ema26             = close.ewm(span=26).mean()
    df['MACD']        = ema12 - ema26
    df['MACD_Signal'] = df['MACD'].ewm(span=9).mean()
    df['MACD_Hist']   = df['MACD'] - df['MACD_Signal']

    df['OBV']         = (np.sign(close.diff()) * df['Volume']).fillna(0).cumsum()
    df['OBV_MA10']    = df['OBV'].rolling(10).mean()
    df['OBV_Rising']  = df['OBV'] > df['OBV_MA10']
    df['OBV_Slope']   = (df['OBV'] - df['OBV'].shift(5)) / df['OBV'].shift(5).abs() * 100
    df['OBV_Bullish'] = df['OBV_MA10'] > df['OBV_MA10'].shift(1)
    df['Base_Line']   = close.rolling(20).min().shift(5)

    delta      = close.diff()
    gain       = delta.where(delta > 0, 0).ewm(com=13, adjust=False).mean()
    loss       = (-delta.where(delta < 0, 0)).ewm(com=13, adjust=False).mean()
    df['RSI']  = 100 - (100 / (1 + gain / loss))

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

    df['Buy_Power']       = df['Volume'] * (close - df['Open'])
    df['Buy_Power_MA']    = df['Buy_Power'].rolling(10).mean()
    df['Buying_Pressure'] = df['Buy_Power'] > df['Buy_Power_MA']

    df['Vol_Avg'] = df['Volume'].rolling(20).mean()
    vol_avg20     = df['Vol_Avg']

    df['MA60_Slope']    = df['MA60'].diff()
    df['MA112_Slope']   = df['MA112'].diff()
    df['Dist_to_MA112'] = (df['MA112'] - close) / close
    df['Near_MA112']    = abs(close - df['MA112']) / df['MA112'] * 100
    df['Below_MA112']   = (df['Close'] < df['MA112']).astype(int)
    df['Below_MA112_60d'] = df['Below_MA112'].rolling(60).sum()
 
    df['MA224'] = df['MA224'].ffill().fillna(0)

    is_above_series       = close > df['MA224']
    df['Trend_Group']     = is_above_series.astype(int).diff().fillna(0).ne(0).cumsum()
    df['Below_MA224']     = (~is_above_series).astype(int)
    df['Below_MA224_60d'] = df['Below_MA224'].rolling(60).sum()

    # ✅ FIX 4: Dolbanzi - is_double_bottom 스칼라 오염 수정
    # 전체 df에 적용되는 벡터화 방식으로 교체
    vol_power_series = df['Volume'] / vol_avg20
    is_above_ma224   = close > df['MA224']
    ma224_last       = df['MA224'].iloc[-1]

    # 최근 30일치 low에서 MA224 ±3% 내 저점 2개 이상 = 쌍바닥
    # → 전체 Series 기준 rolling 방식으로 대체
    near_band_low = (low - df['MA224']).abs() / df['MA224'] < 0.03
    # 5봉 rolling min과 같은 경우 = 로컬 저점
    local_min     = low == low.rolling(5, center=True, min_periods=1).min()
    double_bottom_series = (near_band_low & local_min).rolling(30).sum() >= 2

    df['Dolbanzi']       = (vol_power_series >= 3.0) & is_above_ma224 & double_bottom_series
    df['Dolbanzi_Count'] = df.groupby('Trend_Group')['Dolbanzi'].cumsum()

    df['VWMA40']          = (close * df['Volume']).rolling(40).mean() / df['Volume'].rolling(40).mean()
    df['Vol_Accel']       = df['Volume'] / df['Volume'].rolling(5).mean()
    df['Watermelon_Fire'] = (close / df['VWMA40'] - 1) * 100 * df['Vol_Accel']
    df['Watermelon_Green']= (close > df['VWMA40']) & (df['BB40_Width'] < 0.10)
    df['Watermelon_Red']  = df['Watermelon_Green'] & (df['Watermelon_Fire'] > 5.0)
    df['Watermelon_Red2'] = (close > df['VWMA40']) & (close >= df['Open'])

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

    for col in [
    'BB_Ross', 'RSI_DIV',
    'BB40_Ross', 'BB40_RSI_DIV', 'BB40_Reclaim_RSI_DIV',
    'Was_Panic', 'Is_bb_low_Stable', 'Has_Accumulation', 'Is_Rsi_Divergence'
]:
    df[col] = False

    df_signal = df.dropna(subset=['BB_UP', 'BB_LOW', 'BB40_Lower', 'RSI']).copy()
if len(df_signal) > 51:
    curr_s  = df_signal.iloc[-1]
    past    = df_signal.iloc[-21:-1]
    past_50 = df_signal.iloc[-51:-1]

    # 기존 BB20 기준 유지
    ross, _ = check_ross(curr_s, past)
    rsi_div, _ = check_rsi_div(curr_s, past)

    # 추가 BB40 기준
    bb40_ross, _ = check_bb40_ross(curr_s, past)
    bb40_rsi_div, _ = check_bb40_rsi_div(curr_s, past)
    bb40_combo, _ = check_bb40_reclaim_rsi_div(curr_s, past)

    was_panic         = (past_50['Low'] < past_50['BB_LOW']).any()
    is_bb_low_stable  = curr_s['Low'] > curr_s['BB_LOW']
    is_rsi_divergence = curr_s['RSI'] > past_50['RSI'].min()
    has_accumulation  = (past_50['Volume'] > (past_50['Vol_Avg'] * 3)).any()

    idx = df.index[-1]
    df.at[idx, 'BB_Ross']              = ross
    df.at[idx, 'RSI_DIV']              = rsi_div
    df.at[idx, 'BB40_Ross']            = bb40_ross
    df.at[idx, 'BB40_RSI_DIV']         = bb40_rsi_div
    df.at[idx, 'BB40_Reclaim_RSI_DIV'] = bb40_combo
    df.at[idx, 'Was_Panic']            = was_panic
    df.at[idx, 'Is_bb_low_Stable']     = is_bb_low_stable
    df.at[idx, 'Is_Rsi_Divergence']    = is_rsi_divergence
    df.at[idx, 'Has_Accumulation']     = has_accumulation

    prev = df.iloc[-2]
    curr = df.iloc[-1]

    cond_golden_cross = (prev['MA5'] < prev['MA112']) and (curr['MA5'] >= curr['MA112'])
    cond_approaching  = (prev['MA5'] < prev['MA112']) and (curr['MA112'] * 0.98 <= curr['MA5'] <= curr['MA112'] * 1.03)
    cond_cross        = cond_golden_cross or cond_approaching

    cond_inverse_mid  = curr['MA112'] < curr['MA224']
    cond_below_448    = curr['Close'] < curr['MA448']
    cond_ma224_range  = -3 <= ((curr['Close'] - curr['MA224']) / curr['MA224']) * 100 <= 5
    cond_bb40_range   = -7 <= ((curr['Close'] - curr['BB40_Upper']) / curr['BB40_Upper']) * 100 <= 3

    vol_ratio       = df['Volume'] / df['Volume'].shift(1).replace(0, np.nan)
    cond_vol_300    = (vol_ratio >= 3.0).iloc[-50:].any()
    cond_break_448  = (df['High'] > df['MA448']).iloc[-50:].any()

    df['Is_Real_Watermelon'] = False
    if cond_cross and cond_inverse_mid and cond_below_448 and cond_ma224_range and cond_bb40_range and cond_break_448 and cond_vol_300:
        df.at[df.index[-1], 'Is_Real_Watermelon'] = True

    # ✅ FIX 2: check_touch → 반환값 없는 apply 제거, 벡터화로 교체
    # 각 저항선보다 종가가 낮고 고가가 저항선의 99.5% 이상이면 터치로 간주
    resistances = df[['BB_Upper', 'BB40_Upper', 'MA60', 'MA112']]
    touch_count = pd.DataFrame({
        col: (close < df[col]) & (high >= df[col] * 0.995)
        for col in ['BB_Upper', 'BB40_Upper', 'MA60', 'MA112']
        if col in df.columns
    }).sum(axis=1)
    df['Daily_Touch']     = touch_count
    df['Total_hammering'] = df['Daily_Touch'].rolling(20).sum().fillna(0).astype(int)

    current_res_max = max(curr['BB_Upper'], curr['BB40_Upper'], curr['MA60'], curr['MA112'])
    df['Is_resistance_break'] = curr['Close'] > current_res_max

    df['Is_Maejip'] = (
        (df['Volume'] > df['Volume'].shift(1) * 2) &
        (df['Close'] > df['Open']) &
        (df['Close'] > df['Close'].shift(1))
    )
    df['Maejip_Count'] = df['Is_Maejip'].rolling(20).sum().fillna(0).astype(int)
    
    max_ma      = df[['MA5', 'MA10', 'MA20']].max(axis=1)
    min_ma      = df[['MA5', 'MA10', 'MA20']].min(axis=1)
    is_squeezed = (max_ma - min_ma) / min_ma <= 0.03

    was_below_20 = (close < df['MA20']).astype(int).rolling(10).max() == 1
    is_slope_up  = df['MA5'] > df['MA5'].shift(1)
    is_head_up   = is_slope_up & (df['MA5'] >= df['MA20'] * 0.99)

    df['Viper_Hook'] = is_squeezed & was_below_20 & is_head_up

    is_heading_ceiling     = (close < df['MA112']) & (df['MA112_Slope'] < 0) & (df['Dist_to_MA112'] <= 0.04)
    df['is_not_blocked']   = ~is_heading_ceiling
    df['is_not_waterfall'] = df['MA112'] >= df['MA224'] * 0.9
    df['is_ma60_safe']     = df['MA60_Slope'] >= 0

    df['Dist_from_MA5']  = (close - df['MA5']) / df['MA5']
    df['is_hugging_ma5'] = df['Dist_from_MA5'] < 0.08

    df['recent_high_10d'] = df['High'].rolling(10).max().shift(1)
    is_hitting_wall       = abs(df['recent_high_10d'] - close) / close < 0.02
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

    df['was_broken_20']  = (close < df['MA20']).rolling(5).max() == 1
    df['lowest_vol_5d']  = df['Volume'].rolling(5).min()
    df['is_fake_drop']   = df['lowest_vol_5d'] < (vol_avg20 * 0.5)
    df['obv_divergence'] = (close < close.shift(5)) & (df['OBV'] >= df['OBV'].shift(5))
    df['reclaim_20']     = (close > df['MA20']) & (close > df['Open']) & (df['Volume'] > df['Volume'].shift(1))

    df['Golpagi_Trap'] = (
        df['was_broken_20'] &
        (df['is_fake_drop'] & df['obv_divergence']) &
        df['reclaim_20']
    )

    # ✅ FIX 3: Jongbe_Break 스칼라 전체 컬럼 오염 수정
    # → 전체 컬럼 False 초기화 후 마지막 행에만 True 할당
    gap_ratio    = abs(curr['MA20'] - curr['MA40']) / (curr['MA40'] + 1e-9)
    cross_series = (df['MA20'] > df['MA40']) & (df['MA20'].shift(1) <= df['MA40'].shift(1))
    cross_recent = cross_series.iloc[-5:].any()
    cross_near   = (curr['MA20'] > curr['MA40']) and (gap_ratio < 0.03)

    ma20_rising  = curr['MA20_slope'] > 0
    ma40_rising  = curr['MA40_slope'] > -0.05
    ma20_accel   = curr['MA20_slope'] > df['MA20_slope'].rolling(3).mean().iloc[-2]

    jongbe_value = (
        (cross_recent or cross_near) and
        ma20_rising and
        ma40_rising and
        ma20_accel and
        curr['Close'] > curr['MA20']
    )
    df['Jongbe_Break'] = False                              # ✅ 전체 False 초기화
    df.at[df.index[-1], 'Jongbe_Break'] = jongbe_value     # ✅ 마지막 행만 설정

    print("✅ 최종판독 완료")
    return df

# ---------------------------------------------------------
# 🏛️ [4-1] 역사적 지수 데이터 (캐시 적용)
# ---------------------------------------------------------
_weather_cache = {}  # ✅ FIX 6: 매 실행마다 재다운로드 방지

def prepare_historical_weather():
    cache_key = datetime.now().strftime('%Y-%m-%d')
    if cache_key in _weather_cache:
        print("✅ 날씨 데이터 캐시 사용")
        return _weather_cache[cache_key]

    start_point = (datetime.now() - timedelta(days=600)).strftime('%Y-%m-%d')
    
    ndx = fdr.DataReader('^IXIC', start=start_point)[['Close']]
    sp5 = fdr.DataReader('^GSPC', start=start_point)[['Close']]
    vix = fdr.DataReader('^VIX', start=start_point)[['Close']]
    
    ndx['ixic_ma5']   = ndx['Close'].rolling(5).mean()
    sp5['sp500_ma5']  = sp5['Close'].rolling(5).mean()
    vix['vix_ma5']    = vix['Close'].rolling(5).mean()
    
    weather_df = pd.concat([
        ndx.rename(columns={'Close': 'ixic_close'}),
        sp5.rename(columns={'Close': 'sp500_close'}),
        vix.rename(columns={'Close': 'vix_close'})
    ], axis=1).fillna(method='ffill')
    
    _weather_cache[cache_key] = weather_df
    return weather_df

# ---------------------------------------------------------
# 📸 [5] 시각화 및 텔레그램
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
# 🧠 [6] AI 브리핑 및 토너먼트
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
        comments = "특이사항 없음"
        if issues:
            comments = " | ".join([i["comment"] for i in issues])
        
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
        return f"브리핑 생성 실패: {str(e)}"

def run_ai_tournament(candidate_list, issues):
    if candidate_list.empty:
        return "종목 후보가 없어 토너먼트를 취소합니다."
     
    candidate_list = candidate_list.sort_values(by='안전점수', ascending=False).head(15)
 
    def safe_int(x, default=0):
        try: return int(float(x))
        except: return default
    
    def safe_float(x, default=0.0):
        try: return float(x)
        except: return default
          
    comments = "특이 이슈 없음"
    if issues:
        comments = " | ".join([i.get("comment", "") for i in issues])
      
    prompt_data = "\n".join([
        f"- {row['종목명']}({row['code']}): {row.get('구분','N/A')}, 수급:{row.get('수급',0)}, "
        f"N구분:{row.get('N구분','N/A')}, 이격:{safe_int(row.get('이격',0))}, 현재가:{safe_int(row.get('현재가',0))}, "
        f"BB40:{safe_float(row.get('BB40',0)):.1f}, MA수렴:{safe_float(row.get('MA수렴',0)):.1f}, "
        f"OBV기울기:{safe_int(row.get('OBV기울기',0))}, RSI:{safe_int(safe_float(row.get('RSI',0)))}"
        for _, row in candidate_list.iterrows()
    ])
    
    sys_prompt = (
        f"이슈코멘트 : {comments}\n"
        "당신은 대한민국 '역매공파' 매매법의 권위자이자 퀀트 분석가입니다. 절대 돈을 잃지 않는 보수적 관점에서 심사하십시오.\n"
        "주어진 종목 데이터와 이슈를 기반으로 스윙/단기 전략을 수립하십시오.\n"
        "### 필수 분석 요소:\n"
        "1. 현재 가격 위치 및 거래량/OBV/RSI 분석\n"
        "2. Bearish Divergence 체크\n"
        "3. ADX 20 이상 및 우상향 여부 확인\n"
        "4. 좋은 재료가 남아있는지 악재는 없는지 확인\n"
        "5. 진입/목표/손절 및 세력 매집 흔적 엄격 심사\n\n"
        "단타 1위와 스윙 1위를 선정하고 타점 포함 월가 수준 브리핑을 작성해줘(반말)."
    )
 
    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
        res_gpt = client.chat.completions.create(
            model="gpt-4o-mini", 
            messages=[{"role":"system", "content":sys_prompt}, {"role":"user", "content":prompt_data}]
        )
        gpt_text = res_gpt.choices[0].message.content

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

def get_ai_summary_batch(stock_lines: list, issues: list = None):
    comments = "특이 이슈 없음"
    if issues:
        comments = " | ".join([i.get("comment", "분석 필요") for i in issues])
    
    sys_prompt = (
        f"이슈코멘트 : {comments}\n"
        "당신은 대한민국 '역매공파' 매매법의 권위자이자 퀀트 분석가입니다. "
        "절대 돈을 잃으면 안되는 보수적 관점에서, 주어진 종목 데이터와 이슈를 참고해 전략적 코멘트를 작성하십시오.\n"
        "필수 포함: 현재 가격 위치, 거래량·OBV·MFI·RSI 분석, 진입 포인트, 목표, 손절, 리스크 요인.\n"
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
        res = client.chat.completions.create(
            model="gpt-4o", 
            messages=[
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": user_prompt}
            ],
            max_tokens=2000,
            temperature=0.7
        )
        return res.choices[0].message.content.strip()

    except Exception as e:
        print(f"[AI 배치 요약 오류] {e}")
        return "브리핑 생성 중 오류가 발생했습니다."

def build_ai_candidates_for_macro(ai_candidates: pd.DataFrame):
    result = []

    for _, row in ai_candidates.iterrows():
        result.append({
            "name": row.get("종목명", ""),
            "code": row.get("code", ""),
            "sector": row.get("구분", ""),
            "n_grade": row.get("N등급", ""),
            "n_combo": row.get("N조합", ""),
            "n_score": row.get("N점수", 0),
            "safe_score": row.get("안전점수", 0),
            "current_price": row.get("현재가", 0),
            "disparity": row.get("이격", 0),
            "bb40": row.get("BB40", ""),
            "ma_conv": row.get("MA수렴", ""),
            "obv_slope": row.get("OBV기울기", 0),
            "rsi": row.get("RSI", 0),
            "supply": row.get("수급", ""),
            "finance": row.get("재무", ""),
            "story": row.get("📜서사히스토리", ""),
        })
    return result


def build_macro_snapshot(m_ndx, m_sp5, m_vix, m_fx, issues):
    comments = "특이 이슈 없음"
    if issues:
        comments = " | ".join([i.get("comment", "") for i in issues])

    return {
        "nasdaq": {
            "value": m_ndx.get("val"),
            "change_pct": round(m_ndx.get("chg", 0), 2) if m_ndx.get("chg") is not None else None,
            "status": m_ndx.get("status", "")
        },
        "sp500": {
            "value": m_sp5.get("val"),
            "change_pct": round(m_sp5.get("chg", 0), 2) if m_sp5.get("chg") is not None else None,
            "status": m_sp5.get("status", "")
        },
        "vix": {
            "value": m_vix.get("val"),
            "change_pct": round(m_vix.get("chg", 0), 2) if m_vix.get("chg") is not None else None,
            "status": m_vix.get("status", "")
        },
        "usdkrw": {
            "value": m_fx.get("val"),
            "change_pct": round(m_fx.get("chg", 0), 2) if m_fx.get("chg") is not None else None,
            "status": m_fx.get("status", "")
        },
        "issues": comments
    }


def run_macro_candidate_briefing(ai_candidates: pd.DataFrame, m_ndx, m_sp5, m_vix, m_fx, issues):
    if ai_candidates is None or ai_candidates.empty:
        return {"error": "후보 종목 없음"}

    macro_data = build_macro_snapshot(m_ndx, m_sp5, m_vix, m_fx, issues)
    candidate_data = build_ai_candidates_for_macro(ai_candidates.head(15))

    prompt = f"""
당신은 한국 주식시장 단기/스윙 트레이딩 보조 AI입니다.
사용자는 자동매매를 하지 않고 직접 매매합니다.
목표는 글로벌 시장 상황과 오늘 후보 종목의 궁합을 평가해 우선 검토 순서를 정하는 것입니다.

반드시 JSON만 출력하세요.
마크다운, 코드블록, 설명문 없이 JSON만 출력하세요.

출력 형식:
{{
  "market_briefing": {{
    "market_risk_score": 0,
    "market_state": "Risk On | Neutral | Risk Off",
    "korea_bias": "강세 | 강보합 | 혼조 | 약세 | 약세주의",
    "trading_stance": "공격적 | 선별적 | 방어적",
    "summary": ""
  }},
  "sector_view": {{
    "favorable_sectors": ["", "", ""],
    "unfavorable_sectors": ["", "", ""]
  }},
  "candidate_ranking": [
    {{
      "rank": 1,
      "name": "",
      "code": "",
      "fit_score": 0,
      "action_type": "돌파형 | 눌림목형 | 관망형",
      "why": "",
      "risk": ""
    }}
  ],
  "top_pick": {{
    "name": "",
    "code": "",
    "reason": ""
  }},
  "avoid_first": {{
    "name": "",
    "code": "",
    "reason": ""
  }},
  "today_checkpoints": ["", "", ""]
}}

판단 원칙:
- VIX 상승, 나스닥 약세, S&P500 약세면 Risk Off 성향 강화
- 환율 상승은 한국 성장주/외국인 수급에 부담 요인이 될 수 있음
- N점수, 안전점수는 참고하되 시장 궁합을 더 중요하게 판단
- 추격보다 실전 대응 관점으로 작성
- candidate_ranking에는 반드시 모든 후보 종목을 포함
- action_type은 반드시 돌파형 / 눌림목형 / 관망형 중 하나
- summary는 2~4문장
- today_checkpoints는 장중 체크할 핵심 3개

글로벌 시장 데이터:
{json.dumps(macro_data, ensure_ascii=False, indent=2)}

후보 종목 데이터:
{json.dumps(candidate_data, ensure_ascii=False, indent=2)}
"""

    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
        res = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "너는 보수적이고 실전적인 한국 주식 트레이딩 보조 AI다."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=0.3
        )

        text = res.choices[0].message.content.strip()

        try:
            return json.loads(text)
        except Exception:
            return {
                "error": "JSON 파싱 실패",
                "raw_response": text
            }

    except Exception as e:
        return {
            "error": f"OpenAI 호출 실패: {str(e)}"
        }


def format_macro_briefing_for_telegram(result):
    if "error" in result:
        return f"🌍 [시장 통합 브리핑 실패]\n{result['error']}"

    mb = result["market_briefing"]
    sv = result["sector_view"]
    tp = result["top_pick"]
    av = result["avoid_first"]
    ck = result["today_checkpoints"]

    text = (
        f"🌍 [시장 통합 브리핑]\n"
        f"- 위험도: {mb.get('market_risk_score','')}\n"
        f"- 상태: {mb.get('market_state','')}\n"
        f"- 한국장: {mb.get('korea_bias','')}\n"
        f"- 태도: {mb.get('trading_stance','')}\n"
        f"- 유리섹터: {', '.join(sv.get('favorable_sectors', []))}\n"
        f"- 불리섹터: {', '.join(sv.get('unfavorable_sectors', []))}\n"
        f"- 최우선: {tp.get('name','')}({tp.get('code','')}) / {tp.get('reason','')}\n"
        f"- 주의종목: {av.get('name','')}({av.get('code','')}) / {av.get('reason','')}\n"
        f"- 요약: {mb.get('summary','')}\n"
        f"- 체크: {', '.join(ck)}"
    )
    return text     
# ---------------------------------------------------------
# 🕵️ [7] 분석 엔진
# ---------------------------------------------------------
def analyze_final(ticker, name, historical_indices, g_env, l_env, s_map):
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
        
        if df is None or df.empty:
            return []
         
        df = df.join(historical_indices, how='left').fillna(method='ffill')

        my_sector = s_map.get(ticker, "일반")
        current_leader_condition = l_env.get(my_sector, "Normal")
        today_price = df.iloc[-1]['Close']
     
        row = df.iloc[-1]
        prev = df.iloc[-2]
        prev_5 = df.iloc[-5]
        prev_10 = df.iloc[-10]
        curr_idx = df.index[-1]

        close_p = row['Close']
        open_p = row['Open']
        high_p = row['High']
        low_p = row['Low']

        raw_idx = len(df) - 1
        temp_df = df.iloc[:raw_idx + 1]

        recent_avg_amount = (df['Close'] * df['Volume']).tail(5).mean() / 100000000
        if recent_avg_amount < 50:
            return []

        s_tag, total_m, w_streak, whale_score, twin_b = get_supply_and_money(ticker, row['Close'])
        f_tag, f_score = get_financial_health(ticker)
     
        high_p, low_p, close_p, open_p = row['High'], row['Low'], row['Close'], row['Open']
        body_max = max(open_p, close_p)
        t_pct = int((high_p - body_max) / (high_p - low_p) * 100) if high_p != low_p else 0

        is_cloud_brk = prev['Close'] <= prev['Cloud_Top'] and close_p > row['Cloud_Top']
        is_kijun_sup = close_p > row['Kijun_sen'] and prev['Close'] <= prev['Kijun_sen']
        is_diamond = is_cloud_brk and is_kijun_sup
        is_super_squeeze = row['BB20_Width'] < 10 and row['BB40_Width'] < 15
        is_yeok_mae_old = close_p > row['MA112'] and prev['Close'] <= row['MA112']
        is_vol_power = row['Volume'] > row['VMA20'] * 2.5
     
        yeok_1_ma_aligned = (row['MA5'] > row['MA20']) and (row['MA20'] > row['MA60'])
        yeok_2_ma_converged = row['MA_Convergence'] <= 3.0
        yeok_3_bb40_squeeze = row['BB40_Width'] <= 10.0
        yeok_4_red_candle = close_p < open_p
        day_change = ((close_p - prev['Close']) / prev['Close']) * 100
        yeok_5_pullback = -5.0 <= day_change <= -1.0
        yeok_6_volume_surge = row['Volume'] >= row['VMA5'] * 1.5
        yeok_7_ma5_support = close_p >= row['MA5'] * 0.97

        yeok_mae_count = sum([yeok_1_ma_aligned, yeok_2_ma_converged, yeok_3_bb40_squeeze,
                             yeok_4_red_candle, yeok_5_pullback, yeok_6_volume_surge, yeok_7_ma5_support])

        is_yeok = (prev['MA5'] <= prev['MA20']) and (row['MA5'] > row['MA20'])
        is_mae = row['MA_Convergence'] <= 3.0
        is_gong = (close_p > row['MA112']) and (prev['Close'] <= row['MA112'])
        is_pa = (row['Close'] > row['BB40_Upper']) and (prev['Close'] <= row['BB40_Upper'])
        is_volume = row['Volume'] >= row['VMA5'] * 2.0
        is_safe = 100.0 <= row['Disparity'] <= 106.0
        is_obv = row['OBV_Slope'] > 0

        conditions = [is_yeok, is_mae, is_gong, is_pa, is_volume, is_safe, is_obv]
        match_count = sum(conditions)
      
        acc_1_obv_rising = (row['OBV'] > prev_5['OBV']) and (row['OBV'] > prev_10['OBV'])
        acc_2_box_range = row['Box_Range'] <= 1.15
        acc_3_macd_golden = row['MACD'] > row['MACD_Signal']
        acc_4_rsi_healthy = 40 <= row['RSI'] <= 70
        acc_5_sto_golden = row['Sto_K'] > row['Sto_D']

        silent_1_atr_low = row['ATR'] < row['ATR_MA20']
        silent_2_mfi_strong = row['MFI'] > 50
        silent_3_mfi_rising = row['MFI'] > row['MFI_Prev5']
        silent_4_obv_rising = row['OBV'] > prev_5['OBV']
        
        is_silent_accumulation = (silent_1_atr_low and silent_2_mfi_strong and 
                                 silent_3_mfi_rising and silent_4_obv_rising)
      
        is_sto_gc = prev['Sto_D'] <= prev['Sto_SD'] and row['Sto_D'] > row['Sto_SD']
        is_vma_gc = prev['VMA5'] <= prev['VMA20'] and row['VMA5'] > row['VMA20']
        is_bb_brk = prev['Close'] <= prev['BB_Upper'] and row['Close'] > row['BB_Upper']
        is_bb40_brk = prev.get('BB40_Upper', 0) <= prev['Close']
        
        is_melon = twin_b and row['OBV_Slope'] > 0 and row.get('ADX', 0) > 20 and row['MACD_Hist'] > 0
        is_nova = is_sto_gc and is_vma_gc and is_bb_brk and is_melon

        rsi_score = row['RSI']
     
        near_ma112 = row['Near_MA112'] <= 5.0
        long_bottom = row['Below_MA112_60d'] >= 40
        bottom_area = near_ma112 and long_bottom
        
        bb_squeeze = row['BB40_Width'] <= 10.0
        supply_strong = row['OBV_Rising'] and row['MFI_Strong']
        explosion_ready = bb_squeeze and supply_strong

        is_watermelon = row['Watermelon_Signal']
        watermelon_color = row['Watermelon_Color']
        watermelon_score = row['Watermelon_Score']
        red_score = (
            int(row['OBV_Rising']) +
            int(row['MFI_Strong']) +
            int(row['Buying_Pressure'])
        )
     
        dante_data = calculate_dante_symmetry(temp_df)
        if dante_data is None:
            dante_data_ratio = 0
            dante_data_mae_jip = 0
        else:
            dante_data_ratio = dante_data['ratio']
            dante_data_mae_jip = dante_data['mae_jip']

        grade, narrative, target, stop, conviction = analyze_all_narratives(
            temp_df, name, my_sector, g_env, l_env
        )

        try:
            tri_result = jongbe_triangle_combo_v3(temp_df) or {}
            tri = tri_result.get('triangle') or {}
        except Exception as e:
            print(f"🚨 jongbe_triangle_combo_v3 계산 실패: {e}")
            tri_result = {}
         
        signals = {
            'watermelon_signal': row['Watermelon_Signal'],
            'watermelon_red': row['Watermelon_Color'] == 'red',
            'watermelon_green_7d': row['Green_Days_10'] >= 7,
            'explosion_ready': (
                row['BB40_Width'] <= 10.0 and 
                row['OBV_Rising'] and 
                row['MFI_Strong']
            ),
            'bottom_area': (
                row['Near_MA112'] <= 5.0 and 
                row['Below_MA112_60d'] >= 40
            ),
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
            'yeok_break': (
                close_p > row['MA112'] and 
                prev['Close'] <= row['MA112']
            ),
            'volume_surge': row['Volume'] >= row['VMA20'] * 1.5,
            'obv_rising': row['OBV_Rising'],
            'mfi_strong': row['MFI_Strong'],
            'dolbanzi': row['Dolbanzi'],
            'dolbanzi_Trend_Group': row['Trend_Group'],
            'dolbanzi_Count': row['Dolbanzi_Count'],
            'viper_hook': row['Viper_Hook'],
            'obv_bullish': row['OBV_Bullish'],
            'Real_Viper_Hook': row['Real_Viper_Hook'],
            'Golpagi_Trap': row['Golpagi_Trap'],
            'jongbe_break':    row.get('Jongbe_Break', False),
            'triangle_signal': False,
            'triangle_apex':   None,
            'triangle_pattern': 'None',
            'dmi_cross': False,
            'dmi_ok': False,
            'MA_Convergence': row['MA_Convergence'],
            'bb_ross': row.get('BB_Ross', False),
            'ris_div': row.get('RSI_DIV', False),
            'bb40_ross': row.get('BB40_Ross', False),
            'bb40_rsi_div': row.get('BB40_RSI_DIV', False),
            'bb40_reclaim_rsi_div': row.get('BB40_Reclaim_RSI_DIV', False),
        }
     
        try:
            if tri_result is not None:
                signals['triangle_signal']  = tri_result['pass']
                signals['triangle_apex']    = tri_result['apex_remain']
                signals['triangle_pattern'] = tri_result['triangle_pattern']
                signals['jongbe_ok']        = tri_result['jongbe']
                signals['explosion_ready']  = signals['explosion_ready'] or tri_result['pass']
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
            new_tags.append("🔺🔺Ross쌍바닥")

        if row['RSI_DIV']:
            new_tags.append("📊RSI DIV")

        if row.get('BB40_Ross', False):
            new_tags.append("🟣BB40재안착")

        if row.get('BB40_RSI_DIV', False):
            new_tags.append("🟣BB40 RSI-DIV")

        if row.get('BB40_Reclaim_RSI_DIV', False):
            new_tags.append("🟣BB40재안착+RSI")

        print(f"✅ [본진] 조합 점수 계산!")
        result = judge_trade_with_sequence(temp_df, signals)

        # ✅ new_tags extend (덮어쓰기 제거)
        new_tags.extend(result['tags'])

        if signals['watermelon_signal']:
            new_tags.append(f"🍉강도{row['Watermelon_Score']}/3")
        if signals['bottom_area']:
            new_tags.append(f"📍거리{row['Near_MA112']:.1f}%")
        if signals['silent_perfect'] or signals['silent_strong']:
            new_tags.append(f"🔇ATR{int(row['ATR_Below_Days'])}일")
            new_tags.append(f"💰MFI{int(row['MFI_Strong_Days'])}일")
        if row['Dolbanzi']:
            new_tags.append(f"🟡돌반지")

        style  = classify_style(row)
        W      = STYLE_WEIGHTS[style]
        style_label = {
            "SWING": "📈스윙(5~15일)",
            "SCALP": "⚡단타(1~3일)",
            "NONE":  "➖미분류",
        }[style]
        tags.append(style_label)

        # ✅ s_score 한 번만 설정 (중간 리셋 제거)
        s_score = int(90 + (30 if is_nova else 15 if is_melon else 0))
      
        lower_rn, upper_rn = get_target_levels(row['Close'])
        avg_money = (row['Close'] * row['Volume'])
        is_leader = avg_money >= 100000000000
        is_1st_buy = False
        is_2nd_buy = False
        is_rn_signal = False
        
        if lower_rn and upper_rn:
            lookback_df = df.iloc[max(0, raw_idx-20) : raw_idx]
            hit_upper = any(lookback_df['High'] >= upper_rn * 1.04)
            at_lower_station = lower_rn * 0.96 <= row['Close'] <= lower_rn * 1.04
            is_rn_signal = hit_upper and at_lower_station
          
        if lower_rn:
            signal_line_30 = lower_rn * 1.30
            lookback_df = df.iloc[max(0, raw_idx-20) : raw_idx]
            has_surged_30 = any(lookback_df['High'] >= signal_line_30)
            zone_upper = lower_rn * 1.04
            zone_lower = lower_rn * 0.96
            is_1st_buy = has_surged_30 and (row['Low'] <= zone_upper <= row['High'])
            is_2nd_buy = has_surged_30 and (row['Low'] <= zone_lower <= row['High'])
        
            if is_1st_buy:
                tags.append("🚀급등_1차타점")
                s_score += 100
            if is_2nd_buy:
                tags.append("🚀급등_2차타점")
                s_score += 120
        
            rn_signal_data = {
                'base_rn': lower_rn,
                'is_rapid': has_surged_30,
                'status': "급등존진입" if zone_lower <= row['Close'] <= zone_upper else "관찰중"
            }
          
        if is_rn_signal:
            tags.append("🚉라운드넘버")
            s_score += 70

        # ✅ storm_count 루프 한 번으로 통일
        for m_key in ['ixic', 'sp500']:
            if row.get(f'{m_key}_close', 0) > row.get(f'{m_key}_ma5', 0):
                weather_icons.append("☀️")
            else:
                weather_icons.append("🌪️")
                storm_count += 1

        s_score -= (storm_count * 10)

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
        if is_silent_accumulation:
            s_score += 30
            tags.append("🤫조용한매집💰")
        if silent_1_atr_low:
            tags.append("🔇ATR수축")
        if silent_2_mfi_strong and silent_3_mfi_rising:
            tags.append("💰MFI강세")

        rsi_val = row['RSI']
        if rsi_val >= 80:
            tags.append("🔥RSI강세"); s_score += 10
        elif rsi_val >= 70:
            tags.append("📈RSI상승")
        elif rsi_val >= 50:
            tags.append("✅RSI중립상")
        elif rsi_val >= 30:
            tags.append("📉RSI하락")
        else:
            tags.append("❄️RSI약세")

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
        if t_pct > 40:
            s_score -= 25
            tags.append("⚠️윗꼬리")
        if bottom_area:
            s_score += 80
            tags.append("🏆112선바닥권")
            tags.append(f"📍거리{row['Near_MA112']:.1f}%")
        if row.get('BB40_Ross', False):
            s_score += 35

        if row.get('BB40_RSI_DIV', False):
            s_score += 25

        if row.get('BB40_Reclaim_RSI_DIV', False):
            s_score += 50
        if explosion_ready:
            s_score += 90
            tags.append("💎폭발직전")
        if is_watermelon and explosion_ready and bottom_area:
            s_score += 80
            tags.append("💎💎💎스윙골드")
     
        s_score -= max(0, int((row['Disparity']-108)*5))

        if not tags: return []

        print(f"✅ {name} 포착! 점수: {s_score} 태그: {tags}")
        
        return [{
            '날짜': curr_idx.strftime('%Y-%m-%d'),
            '종목명': name, 'code': ticker,
            'N등급': f"{result['type']}{result['grade']}",
            'N조합': result['combination'],
            'N점수': result['score'],
            'N구분': " ".join(new_tags),
            '👑등급': grade,
            '📜서사히스토리': narrative,
            '확신점수': conviction,
            '🎯목표타점': int(target),
            '🚨손절가': int(stop),
            '기상': "☀️" * (2-storm_count) + "🌪️" * storm_count,
            '안전점수': int(max(0, s_score + whale_score)),
            'RSI': int(max(0, rsi_score)),
            '점수': int(s_score),
            '에너지': "🔋" if row['MACD_Hist'] > 0 else "🪫",
            '현재가': int(row['Close']),
            '구분': " ".join(tags),
            '재무': f_tag, '수급': s_tag,
            '이격': int(row['Disparity']),
            'BB40': f"{row['BB40_Width']:.1f}",
            'MA수렴': f"{row['MA_Convergence']:.1f}",
            '매집': f"{acc_count}/5",
            'OBV기울기': int(row['OBV_Slope']),
            'BB20로스': bool(row.get('BB_Ross', False)),
            'RSI다이버': bool(row.get('RSI_DIV', False)),
            'BB40로스': bool(row.get('BB40_Ross', False)),
            'BB40_RSI_DIV': bool(row.get('BB40_RSI_DIV', False)),
            'BB40재안착조합': bool(row.get('BB40_Reclaim_RSI_DIV', False)),
            '꼬리%': 0
        }]
    except Exception as e:
        import traceback
        print(f"🚨 {name} 분석 중 치명적 에러:\n{traceback.format_exc()}")
        return []

# 스타일별 가중치
STYLE_WEIGHTS = {
    "SWING": {
        'explosion_ready': 150, 'bottom_area': 120, 'silent_perfect': 130,
        'silent_strong': 80, 'silent_weak': 40, 'bb_squeeze_bonus': 50,
        'ma_convergence': 40, 'watermelon': 70, 'watermelon_red': 50,
        'volume_surge': 20, 'adx_strong': 10, 'swing_gold': 100,
        'high_tail': -25, 'disparity_over': -5,
    },
    "SCALP": {
        'explosion_ready': 50, 'bottom_area': 20, 'silent_perfect': 30,
        'silent_strong': 20, 'silent_weak': 10, 'bb_squeeze_bonus': 10,
        'ma_convergence': 10, 'watermelon': 150, 'watermelon_red': 100,
        'volume_surge': 80, 'adx_strong': 80, 'swing_gold': 40,
        'high_tail': -40, 'disparity_over': -8,
    },
    "NONE": {
        'explosion_ready': 90, 'bottom_area': 80, 'silent_perfect': 100,
        'silent_strong': 60, 'silent_weak': 30, 'bb_squeeze_bonus': 20,
        'ma_convergence': 0, 'watermelon': 100, 'watermelon_red': 60,
        'volume_surge': 30, 'adx_strong': 20, 'swing_gold': 80,
        'high_tail': -25, 'disparity_over': -5,
    },
}

def classify_style(row):
    vol_ratio = row['ATR'] / row['Close'] if row['Close'] > 0 else 0
    if (row['BB40_Width'] < 12 and row['MA_Convergence'] < 3 and row['ADX'] < 25):
        return "SWING"
    elif (0.02 <= vol_ratio <= 0.05 and row['ADX'] >= 25):
        return "SCALP"
    return "NONE"
  
def get_target_levels(current_price):
    upper_rns = [rn for rn in RN_LIST if rn > current_price]
    lower_rns = [rn for rn in RN_LIST if rn <= current_price]
    upper = upper_rns[0] if upper_rns else None
    lower = lower_rns[-1] if lower_rns else None
    return lower, upper

def analyze_weekly_trend(ticker, name):
    try:
        df_daily = fdr.DataReader(ticker, start=(datetime.now()-timedelta(days=730)))
        if len(df_daily) < 200: return []

        df = df_daily.resample('W-MON').agg({
            'Open': 'first', 'High': 'max',
            'Low': 'min', 'Close': 'last', 'Volume': 'sum'
        })

        df['MA20_W'] = df['Close'].rolling(window=20).mean()
        df['BB20_Upper_W'] = df['MA20_W'] + (df['Close'].rolling(window=20).std() * 2)
        df['OBV_W'] = (np.sign(df['Close'].diff()) * df['Volume']).fillna(0).cumsum()
        df['OBV_MA10_W'] = df['OBV_W'].rolling(window=10).mean()

        row = df.iloc[-1]
        prev = df.iloc[-2]

        is_weekly_break = prev['Close'] <= prev['BB20_Upper_W'] and row['Close'] > row['BB20_Upper_W']
        is_weekly_acc = row['OBV_W'] > row['OBV_MA10_W']
        
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
# 🚀 [8] 메인 실행
# ---------------------------------------------------------
if __name__ == "__main__":
    print("🚀 전략 사령부 가동 시작...")
    
    client = OpenAI()
    models = client.models.list()
    for m in models.data:
        print(m.id)
     
    global_env, leader_env = get_global_and_leader_status()

    try:
        df_krx = load_krx_listing_safe()
        df_krx['Code'] = (
            df_krx['Code']
            .fillna('')
            .astype(str)
            .str.replace('.0', '', regex=False)
            .str.zfill(6)
        )
        
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
        df_krx = pd.DataFrame(columns=['Code', 'Name', 'Sector'])
 
    m_ndx = get_safe_macro('^IXIC', '나스닥')
    m_sp5 = get_safe_macro('^GSPC', 'S&P500')
    m_vix = get_safe_macro('^VIX', 'VIX공포')
    m_fx  = get_safe_macro('USD/KRW', '달러환율')
    macro_status = {'nasdaq': m_ndx, 'sp500': m_sp5, 'vix': m_vix, 'fx': m_fx, 'kospi': {get_index_investor_data('KOSPI')}}

    print("\n" + "🌍 " * 5 + "[ 글로벌 사령부 통합 관제 센터 ]" + " 🌍" * 5)
    print(f"🇺🇸 {m_ndx['text']} | {m_sp5['text']} | ⚠️ {m_vix['text']}")
    print(f"💵 {m_fx['text']} | 🇰🇷 KOSPI 수급: {get_index_investor_data('KOSPI')}")
    print("=" * 115)
    
    imgs = [create_index_chart('KS11', 'KOSPI'), create_index_chart('IXIC', 'NASDAQ')]
    issues = analyze_market_issues()
    briefing = get_market_briefing(issues)
    
    df_clean = df_krx[df_krx['Market'].isin(['KOSPI', 'KOSDAQ','코스닥','유가'])]
    df_clean['Name'] = df_clean['Name'].astype(str)
    df_clean = df_clean[~df_clean['Name'].str.contains('ETF|ETN|스팩|제[0-9]+호|우$|우A|우B|우C')]

    if 'Amount' in df_clean.columns:
        sorted_df = df_clean.sort_values(by='Amount', ascending=False).head(TOP_N)
    else:
        sorted_df = df_clean.copy()
    
    target_dict = dict(zip(sorted_df['Code'], sorted_df['Name']))

    weather_data = prepare_historical_weather()
    
    all_hits = []
    with ThreadPoolExecutor(max_workers=15) as executor:
        results = list(executor.map(
            lambda p: analyze_final(p[0], p[1], weather_data, global_env, leader_env, sector_master_map), 
            zip(sorted_df['Code'], sorted_df['Name'])
        ))
        for r in results:
            if r:
                for hit in r:
                    all_hits.append(hit)
        
if all_hits:
    all_hits_sorted = sorted(all_hits, key=lambda x: x['N점수'], reverse=True)

    ai_candidates = pd.DataFrame(all_hits_sorted)
    ai_candidates = ai_candidates.sort_values(by='N점수', ascending=False)[:30].copy()

    print(f"🌍 시장 + 후보종목 통합 AI 브리핑 생성 중...")
    macro_briefing_result = run_macro_candidate_briefing(
        ai_candidates=ai_candidates,
        m_ndx=m_ndx,
        m_sp5=m_sp5,
        m_vix=m_vix,
        m_fx=m_fx,
        issues=issues
    )

    print("✅ 통합 AI 브리핑 결과:")
    print(json.dumps(macro_briefing_result, ensure_ascii=False, indent=2))

    try:
        update_ai_briefing_sheet(macro_briefing_result, TODAY_STR)
        print("💾 AI_Briefing 시트 저장 완료")
    except Exception as e:
        print(f"🚨 AI_Briefing 저장 실패: {e}")

    macro_briefing_text = format_macro_briefing_for_telegram(macro_briefing_result)

    print(f"🧠 상위 30개 종목 AI 심층 분석 중...")
    tournament_report = run_ai_tournament(ai_candidates, issues)

    lines = []
    
    def safe_int(x, default=0):
        try: return int(float(x))
        except: return default
    
    def safe_float(x, default=0.0):
        try: return float(x)
        except: return default
    
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
    
    ai_result_text = get_ai_summary_batch(lines, issues)
    ai_map = {}
   
    for line in ai_result_text.splitlines():
        if ":" in line:
            key, val = line.split(":", 1)
            ai_map[key.strip()] = val.strip()
    
    for idx, item in ai_candidates.iterrows():
        key = f"{item['종목명']}({item['code']})"
        ai_candidates.loc[idx, "ai_tip"] = ai_map.get(key, "")
    
    telegram_targets = ai_candidates[:15]
    
    MAX_CHAR = 3800
    current_msg = f"{briefing}\n\n{macro_briefing_text}\n\n📢 [오늘의 실시간 TOP 15]\n\n"
    
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

    final_block = f"\n{tournament_report}"
    if len(current_msg) + len(final_block) > MAX_CHAR:
        send_telegram_photo(current_msg, imgs if imgs else [])
        send_telegram_photo(f"🏆 [AI 토너먼트 최종 결과]\n{final_block}", [])
    else:
        current_msg += final_block
        send_telegram_photo(current_msg, imgs if imgs else [])

    try:
        update_google_sheet(all_hits_sorted, TODAY_STR, tournament_report)
        print(f"💾 총 {len(all_hits_sorted)}개 종목 전수 기록 완료!")
    except Exception as e:
        print(f"🚨 시트 업데이트 실패: {e}")

    print("✅ 작전 종료: 전수 기록 완료 및 정예 15건 보고 완료!")
