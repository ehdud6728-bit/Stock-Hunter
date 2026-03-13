# ================================================================
# us_ticker_loader.py
# 나스닥100 / S&P500 종목 리스트 동적으로 가져오기
# 방법 우선순위:
#   1. fdr.StockListing (가장 간단)
#   2. Wikipedia 크롤링 (안정적)
#   3. 하드코딩 fallback (최후 수단)
# ================================================================

import pandas as pd
import requests
import FinanceDataReader as fdr
from bs4 import BeautifulSoup

# ────────────────────────────────────────────────────────────────
# ✅ 방법 1: FinanceDataReader (기존에 이미 쓰고 있음)
# ────────────────────────────────────────────────────────────────

def get_nasdaq100_fdr():
    """
    fdr.StockListing('S&P500') 로 S&P500 전체 가져온 뒤
    나스닥 상장 종목만 필터링
    """
    try:
        df = fdr.StockListing('S&P500')
        print(f"✅ FDR S&P500: {len(df)}개")
        # 시가총액 상위 100개 (나스닥100 근사치)
        if 'MarketCap' in df.columns:
            df = df.sort_values('MarketCap', ascending=False).head(100)
        tickers = df['Symbol'].tolist() if 'Symbol' in df.columns else df['Code'].tolist()
        return tickers
    except Exception as e:
        print(f"⚠️ FDR 실패: {e}")
        return []


def get_nyse_nasdaq_all_fdr(min_marcap=None):
    """
    나스닥/NYSE 전체 종목 가져오기
    min_marcap: 최소 시가총액 필터 (달러, 예: 10_000_000_000 = 100억달러)
    """
    try:
        dfs = []
        for market in ['NASDAQ', 'NYSE']:
            df = fdr.StockListing(market)
            df['Market'] = market
            dfs.append(df)

        all_df = pd.concat(dfs, ignore_index=True)
        print(f"✅ FDR 전체: {len(all_df)}개")

        # 시가총액 필터
        if min_marcap and 'MarketCap' in all_df.columns:
            all_df = all_df[all_df['MarketCap'] >= min_marcap]
            print(f"✅ 시가총액 필터 후: {len(all_df)}개")

        # ETF / 우선주 제거
        sym_col = 'Symbol' if 'Symbol' in all_df.columns else 'Code'
        all_df = all_df[~all_df[sym_col].str.contains(r'\^|\.|-', regex=True, na=False)]

        return all_df[sym_col].tolist()
    except Exception as e:
        print(f"⚠️ FDR 전체 실패: {e}")
        return []


# ────────────────────────────────────────────────────────────────
# ✅ 방법 2: Wikipedia 크롤링 (가장 정확한 나스닥100/S&P500)
# ────────────────────────────────────────────────────────────────

def get_nasdaq100_wikipedia():
    """Wikipedia에서 나스닥100 정확한 구성 종목 가져오기"""
    try:
        url = "https://en.wikipedia.org/wiki/Nasdaq-100"
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers, timeout=10)
        tables = pd.read_html(res.text)

        # 티커 컬럼이 있는 테이블 찾기
        for table in tables:
            cols = [str(c).lower() for c in table.columns]
            if any('ticker' in c or 'symbol' in c for c in cols):
                col = next(c for c in table.columns if 'ticker' in str(c).lower() or 'symbol' in str(c).lower())
                tickers = table[col].dropna().tolist()
                tickers = [t.strip() for t in tickers if isinstance(t, str) and t.strip()]
                print(f"✅ Wikipedia 나스닥100: {len(tickers)}개")
                return tickers

        print("⚠️ Wikipedia 테이블 파싱 실패")
        return []
    except Exception as e:
        print(f"⚠️ Wikipedia 나스닥100 실패: {e}")
        return []


def get_sp500_wikipedia():
    """Wikipedia에서 S&P500 구성 종목 가져오기"""
    try:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers, timeout=10)
        df = pd.read_html(res.text)[0]

        sym_col = next(c for c in df.columns if 'symbol' in str(c).lower() or 'ticker' in str(c).lower())
        tickers = df[sym_col].dropna().tolist()
        tickers = [t.strip().replace('.', '-') for t in tickers]  # BRK.B → BRK-B
        print(f"✅ Wikipedia S&P500: {len(tickers)}개")
        return tickers
    except Exception as e:
        print(f"⚠️ Wikipedia S&P500 실패: {e}")
        return []


# ────────────────────────────────────────────────────────────────
# ✅ 방법 3: 하드코딩 fallback
# ────────────────────────────────────────────────────────────────

NASDAQ_100_FALLBACK = [
    'AAPL', 'MSFT', 'NVDA', 'AMZN', 'META', 'GOOGL', 'TSLA',
    'AVGO', 'COST', 'NFLX', 'AMD', 'PEP', 'CSCO', 'ADBE',
    'QCOM', 'TXN', 'INTU', 'AMGN', 'AMAT', 'BKNG', 'ISRG',
    'VRTX', 'ADP', 'REGN', 'PANW', 'SBUX', 'LRCX', 'MU',
    'KLAC', 'SNPS', 'CDNS', 'MELI', 'FTNT', 'ABNB', 'ORLY',
    'CTAS', 'MNST', 'MRVL', 'PYPL', 'CRWD', 'PCAR', 'WDAY',
    'ROST', 'ODFL', 'IDXX', 'FAST', 'MRNA', 'ON', 'DDOG',
    'ZS', 'TEAM', 'TTD', 'PLTR', 'ARM', 'SMCI', 'ORCL',
    'CRM', 'NOW', 'SNOW', 'UBER', 'NET', 'COIN',
]


# ────────────────────────────────────────────────────────────────
# ✅ 통합 로더 (우선순위 자동 fallback)
# ────────────────────────────────────────────────────────────────

def load_us_tickers(
    mode='nasdaq100',       # 'nasdaq100' / 'sp500' / 'all'
    min_marcap_b=10,        # 최소 시가총액 (단위: 십억달러), all 모드에서만 사용
    max_count=None,         # 최대 종목 수 제한 (None = 전체)
):
    """
    미국주식 종목 리스트 동적 로딩
    
    mode:
      'nasdaq100' → 나스닥100 구성 종목
      'sp500'     → S&P500 구성 종목
      'all'       → 나스닥+NYSE 전체 (시가총액 필터 적용)
    
    반환: 티커 리스트
    """
    tickers = []

    if mode == 'nasdaq100':
        # 1순위: Wikipedia
        tickers = get_nasdaq100_wikipedia()
        # 2순위: FDR
        if not tickers:
            tickers = get_nasdaq100_fdr()
        # 3순위: 하드코딩
        if not tickers:
            tickers = NASDAQ_100_FALLBACK
            print("⚠️ fallback 사용: 하드코딩 나스닥100")

    elif mode == 'sp500':
        # 1순위: Wikipedia
        tickers = get_sp500_wikipedia()
        # 2순위: FDR
        if not tickers:
            tickers = get_nasdaq100_fdr()
        if not tickers:
            tickers = NASDAQ_100_FALLBACK

    elif mode == 'all':
        min_marcap = min_marcap_b * 1_000_000_000
        tickers = get_nyse_nasdaq_all_fdr(min_marcap=min_marcap)
        if not tickers:
            tickers = NASDAQ_100_FALLBACK

    # ETF / 특수문자 제거
    tickers = [
        t for t in tickers
        if isinstance(t, str) and t.strip()
        and not any(c in t for c in ['^', '.', '/', ' '])
    ]

    # 최대 수 제한
    if max_count:
        tickers = tickers[:max_count]

    print(f"✅ 최종 스캔 대상: {len(tickers)}개 ({mode})")
    return tickers


# ────────────────────────────────────────────────────────────────
# ✅ us_scanner.py 적용 가이드
# ────────────────────────────────────────────────────────────────
"""
기존 us_scanner.py 상단의 하드코딩 리스트:
    NASDAQ_100 = ['AAPL', 'MSFT', ...]

아래로 교체:
    from us_ticker_loader import load_us_tickers

그리고 run_us_scanner() 안에서:
    # 기존
    tickers = NASDAQ_100

    # 교체 (원하는 모드 선택)
    tickers = load_us_tickers(mode='nasdaq100')          # 나스닥100
    tickers = load_us_tickers(mode='sp500')              # S&P500 500개
    tickers = load_us_tickers(mode='all', min_marcap_b=50)  # 시총 500억달러 이상 전체

---
모드별 예상 종목 수:
  nasdaq100  → 약 100개
  sp500      → 약 500개
  all (50B)  → 약 200~300개
  all (10B)  → 약 500~800개
"""
