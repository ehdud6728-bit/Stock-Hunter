# =============================================================
# 🚨 surge_scanner_fixed_complete.py — 실시간 급등 포착 스캐너 (수정완성형)
# =============================================================
# 주요 수정:
# 1) _get_market_snapshot() 를 "오늘 시세 전용"으로 재구성
# 2) _normalize_ohlcv() 실제 사용
# 3) 시총(MIN_MARCAP) 필터 실제 반영
# 4) 전일 거래량 조회 구간 확대 (10일)
# 5) KST aware 중복알림 시간 처리
# 6) 장 초반 동적 임계값 적용
# 7) amount / change_pct 계산 보강
#
# 실행 방법:
#   python surge_scanner_fixed_complete.py
#   python surge_scanner_fixed_complete.py --loop
#   python surge_scanner_fixed_complete.py --test
# =============================================================

import os
import sys
import json
import time
import argparse
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
import pytz
from pykrx import stock

try:
    import FinanceDataReader as fdr
except Exception:
    fdr = None

try:
    from scan_logger import set_log_level, log_info, log_error, log_debug
    set_log_level('NORMAL')
except ImportError:
    def log_info(m):  print(m)
    def log_error(m): print(m)
    def log_debug(m): pass


# =============================================================
# ⚙️ 설정
# =============================================================
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN', '')
CHAT_ID_LIST = os.environ.get('TELEGRAM_CHAT_ID', '').split(',')
KST = pytz.timezone('Asia/Seoul')

# 기본 급등 탐지 조건
MIN_CHANGE_PCT = 3.0
MIN_VOL_RATIO = 2.5
MIN_AMOUNT = 5            # 억
MIN_PRICE = 5_000
MIN_MARCAP = 30_000_000_000  # 300억

TOP_N = 300
MAX_WORKERS = 20

ALERTED_FILE = '/tmp/surge_alerted.json'
ALERT_COOLDOWN = 60  # 분

SHEET_ID = "13Esd11iwgzLN7opMYobQ3ee6huHs1FDEbyeb3Djnu6o"
SHEET_GID = "1238448456"


# =============================================================
# 🧰 공통 유틸
# =============================================================
def safe_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default


def safe_int(v, default=0):
    try:
        return int(float(v))
    except Exception:
        return default


def now_kst() -> datetime:
    return datetime.now(KST)


def _get_today_str() -> str:
    now = now_kst()
    if now.hour < 9:
        now = now - timedelta(days=1)
    return now.strftime('%Y%m%d')


def _find_prev_trading_day(today_str: str, lookback_days: int = 10) -> str:
    """가장 가까운 이전 거래일 찾기"""
    today_dt = datetime.strptime(today_str, '%Y%m%d')
    for i in range(1, lookback_days + 1):
        cand = (today_dt - timedelta(days=i)).strftime('%Y%m%d')
        try:
            df = stock.get_market_ohlcv(cand, market='KOSPI')
            if df is not None and not df.empty:
                return cand
        except Exception:
            pass
    return (today_dt - timedelta(days=1)).strftime('%Y%m%d')


def _get_dynamic_thresholds(ts: datetime):
    """
    장 초반에는 기준을 완화해서 초동을 더 잘 잡도록 함.
    반환: (min_change_pct, min_vol_ratio, min_amount_억)
    """
    hhmm = ts.hour * 100 + ts.minute

    if hhmm < 930:
        return 2.0, 1.5, 2.0
    if hhmm < 1000:
        return 2.5, 2.0, 3.0
    return MIN_CHANGE_PCT, MIN_VOL_RATIO, float(MIN_AMOUNT)


# =============================================================
# 📋 알림 기록 관리
# =============================================================
def _load_alerted() -> dict:
    try:
        if os.path.exists(ALERTED_FILE):
            with open(ALERTED_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def _save_alerted(alerted: dict):
    try:
        with open(ALERTED_FILE, 'w', encoding='utf-8') as f:
            json.dump(alerted, f, ensure_ascii=False)
    except Exception:
        pass


def _parse_alert_time(ts: str):
    try:
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            return KST.localize(dt)
        return dt.astimezone(KST)
    except Exception:
        return None


def _is_already_alerted(code: str, alerted: dict) -> bool:
    if code not in alerted:
        return False
    last_time = _parse_alert_time(alerted[code])
    if last_time is None:
        return False
    elapsed = (now_kst() - last_time).total_seconds() / 60
    return elapsed < ALERT_COOLDOWN


def _mark_alerted(code: str, alerted: dict):
    alerted[code] = now_kst().isoformat()


# =============================================================
# 📋 KRX 종목 로드
# =============================================================
def _load_krx_listing() -> pd.DataFrame:
    """KRX 전종목 리스트 로드 — 구글시트 → FDR → pykrx"""
    # ① 구글시트
    try:
        url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={SHEET_GID}"
        df = pd.read_csv(url, encoding='utf-8', engine='python')
        if df is not None and not df.empty and len(df) > 100:
            log_info(f"✅ 구글시트 종목 로드: {len(df)}개")
            return df
    except Exception as e:
        log_info(f"⚠️ 구글시트 실패: {e}")

    # ② FDR
    if fdr is not None:
        try:
            df = fdr.StockListing('KRX')
            if df is not None and not df.empty and len(df) > 100:
                log_info(f"✅ FDR 종목 로드: {len(df)}개")
                return df
        except Exception as e:
            log_info(f"⚠️ FDR 실패: {e}")

    # ③ pykrx fallback
    log_info("📡 pykrx로 종목 리스트 구성 중...")
    try:
        today = _get_today_str()
        frames = []

        for mkt in ['KOSPI', 'KOSDAQ']:
            try:
                df_m = stock.get_market_ohlcv(today, market=mkt)
                if df_m is None or df_m.empty:
                    continue

                df_m = df_m.reset_index()
                rename = {}
                for c in df_m.columns:
                    cs = str(c).strip()
                    if cs in ('티커', 'Ticker', '종목코드'):
                        rename[c] = 'Code'
                    elif cs in ('종가', 'Close', '현재가'):
                        rename[c] = 'Close'
                    elif cs in ('거래량', 'Volume'):
                        rename[c] = 'Volume'
                    elif cs in ('거래대금', 'Amount', 'Turnover'):
                        rename[c] = 'Amount'
                    elif cs in ('시가총액', 'Marcap', 'MarCap'):
                        rename[c] = 'Marcap'
                df_m = df_m.rename(columns=rename)
                if 'Code' in df_m.columns:
                    codes = df_m['Code'].astype(str).str.zfill(6).tolist()
                    name_map = {}
                    for t in codes[:2000]:
                        try:
                            name_map[t] = stock.get_market_ticker_name(t)
                        except Exception:
                            name_map[t] = t
                    df_m['Code'] = codes
                    df_m['Name'] = df_m['Code'].map(name_map).fillna(df_m['Code'])
                df_m['Market'] = mkt
                frames.append(df_m)
                log_info(f"  {mkt}: {len(df_m)}개")
            except Exception as e:
                log_error(f"  {mkt} 실패: {e}")

        if frames:
            result = pd.concat(frames, ignore_index=True)
            log_info(f"✅ pykrx 종목 구성 완료: {len(result)}개")
            return result
    except Exception as e:
        log_error(f"🚨 pykrx 실패: {e}")

    log_error("🚨 종목 리스트 로드 실패")
    return pd.DataFrame(columns=['Code', 'Name', 'Market'])


def _normalize_listing(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=['code', 'name_meta', 'marcap', 'market_meta'])

    df = df.copy()
    rename = {}

    for c in df.columns:
        cs = str(c).strip()
        if cs in ('Code', 'code', '종목코드', '티커', 'Ticker'):
            rename[c] = 'code'
        elif cs in ('Name', 'name', '종목명'):
            rename[c] = 'name_meta'
        elif cs in ('Marcap', 'MarCap', '시가총액'):
            rename[c] = 'marcap'
        elif cs in ('Market', 'market'):
            rename[c] = 'market_meta'

    df = df.rename(columns=rename)

    if 'code' not in df.columns:
        return pd.DataFrame(columns=['code', 'name_meta', 'marcap', 'market_meta'])

    df['code'] = df['code'].fillna('').astype(str).str.replace('.0', '', regex=False).str.zfill(6)

    if 'name_meta' not in df.columns:
        df['name_meta'] = df['code']
    if 'marcap' not in df.columns:
        df['marcap'] = None
    if 'market_meta' not in df.columns:
        df['market_meta'] = ''

    keep_cols = ['code', 'name_meta', 'marcap', 'market_meta']
    return df[keep_cols].drop_duplicates(subset='code').copy()


# =============================================================
# 📊 시세 스냅샷 수집
# =============================================================
def _normalize_ohlcv(df: pd.DataFrame, market: str) -> pd.DataFrame:
    """
    pykrx 컬럼명을 내부 표준 컬럼으로 정규화.
    """
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy().reset_index()
    df.columns = [str(c).strip() for c in df.columns]

    KR_TO_EN = {
        '티커': 'code', 'Ticker': 'code', '종목코드': 'code',
        '시가': 'open', 'Open': 'open',
        '고가': 'high', 'High': 'high',
        '저가': 'low', 'Low': 'low',
        '종가': 'close', 'Close': 'close', '현재가': 'close',
        '거래량': 'volume', 'Volume': 'volume',
        '거래대금': 'amount', 'Amount': 'amount', 'Turnover': 'amount',
        '등락률': 'change_pct', '변동률': 'change_pct', 'ChangeRate': 'change_pct', 'Change': 'change_pct',
        '시가총액': 'marcap', 'Marcap': 'marcap', 'MarCap': 'marcap',
        '종목명': 'name', 'Name': 'name',
    }

    rename = {}
    for c in df.columns:
        cs = str(c).strip()
        if cs in KR_TO_EN:
            rename[c] = KR_TO_EN[cs]
        elif '티커' in cs or cs.lower() == 'ticker':
            rename[c] = 'code'
        elif '종가' in cs or '현재가' in cs:
            rename[c] = 'close'
        elif '시가' in cs and '총' not in cs:
            rename[c] = 'open'
        elif '고가' in cs:
            rename[c] = 'high'
        elif '저가' in cs:
            rename[c] = 'low'
        elif '거래량' in cs:
            rename[c] = 'volume'
        elif '거래대금' in cs:
            rename[c] = 'amount'
        elif '등락률' in cs or '변동률' in cs:
            rename[c] = 'change_pct'
        elif '시가총액' in cs:
            rename[c] = 'marcap'

    df = df.rename(columns=rename)
    df['market'] = market

    if 'code' in df.columns:
        df['code'] = df['code'].fillna('').astype(str).str.replace('.0', '', regex=False).str.zfill(6)

    if 'name' not in df.columns and 'code' in df.columns:
        try:
            name_map = {}
            for t in df['code'].tolist()[:500]:
                try:
                    name_map[t] = stock.get_market_ticker_name(t)
                except Exception:
                    name_map[t] = t
            df['name'] = df['code'].map(name_map).fillna(df['code'])
        except Exception:
            df['name'] = df.get('code', '')

    return df


def _get_prev_close_map(today: str, prev_day: str, market: str) -> pd.DataFrame:
    """
    전일 종가 맵 생성.
    """
    try:
        df_prev = stock.get_market_ohlcv(prev_day, market=market)
        if df_prev is None or df_prev.empty:
            return pd.DataFrame(columns=['code', 'prev_close'])

        df_prev = _normalize_ohlcv(df_prev, market)
        if 'code' not in df_prev.columns or 'close' not in df_prev.columns:
            return pd.DataFrame(columns=['code', 'prev_close'])

        out = df_prev[['code', 'close']].copy()
        out = out.rename(columns={'close': 'prev_close'})
        return out
    except Exception:
        return pd.DataFrame(columns=['code', 'prev_close'])


def _get_market_snapshot() -> pd.DataFrame:
    """
    오늘 실시간 누적 시세를 pykrx에서 가져오고,
    종목명/시총 같은 메타데이터는 listing과 merge.
    """
    today = _get_today_str()
    prev_day = _find_prev_trading_day(today)

    frames = []
    prev_frames = []

    for market in ['KOSPI', 'KOSDAQ']:
        try:
            df_m = stock.get_market_ohlcv(today, market=market)
            df_m = _normalize_ohlcv(df_m, market)
            if df_m is not None and not df_m.empty:
                frames.append(df_m)
                prev_frames.append(_get_prev_close_map(today, prev_day, market))
        except Exception as e:
            log_error(f"⚠️ {market} 시세 수집 실패: {e}")

    if not frames:
        return pd.DataFrame()

    snapshot = pd.concat(frames, ignore_index=True)

    # prev_close merge
    if prev_frames:
        prev_df = pd.concat(prev_frames, ignore_index=True)
        prev_df = prev_df.drop_duplicates(subset='code')
        snapshot = snapshot.merge(prev_df, on='code', how='left')

    # change_pct 계산 보강
    if 'change_pct' not in snapshot.columns:
        snapshot['change_pct'] = None

    if 'prev_close' in snapshot.columns and 'close' in snapshot.columns:
        mask = snapshot['change_pct'].isna() | (snapshot['change_pct'] == 0)
        snapshot.loc[mask, 'change_pct'] = (
            (snapshot.loc[mask, 'close'] - snapshot.loc[mask, 'prev_close']) /
            snapshot.loc[mask, 'prev_close'].replace(0, pd.NA)
        ) * 100

    # amount 계산 보강
    if 'amount' not in snapshot.columns:
        snapshot['amount'] = 0.0

    amt_mask = snapshot['amount'].isna() | (snapshot['amount'] <= 0)
    if 'close' in snapshot.columns and 'volume' in snapshot.columns:
        snapshot.loc[amt_mask, 'amount'] = snapshot.loc[amt_mask, 'close'] * snapshot.loc[amt_mask, 'volume']

    # listing merge
    listing = _normalize_listing(_load_krx_listing())
    if listing is not None and not listing.empty:
        snapshot = snapshot.merge(listing, on='code', how='left')

        if 'name' not in snapshot.columns:
            snapshot['name'] = snapshot.get('name_meta', snapshot['code'])
        else:
            snapshot['name'] = snapshot['name'].fillna(snapshot.get('name_meta'))

        if 'market' not in snapshot.columns and 'market_meta' in snapshot.columns:
            snapshot['market'] = snapshot['market_meta']

        if 'marcap' not in snapshot.columns:
            snapshot['marcap'] = snapshot.get('marcap')
        else:
            snapshot['marcap'] = snapshot['marcap'].fillna(snapshot.get('marcap'))

    # 필수 컬럼 보장
    for col in ['open', 'high', 'low', 'close', 'volume', 'amount', 'change_pct', 'marcap']:
        if col not in snapshot.columns:
            snapshot[col] = 0.0

    snapshot['change_pct'] = pd.to_numeric(snapshot['change_pct'], errors='coerce').fillna(0.0)
    snapshot['close'] = pd.to_numeric(snapshot['close'], errors='coerce').fillna(0.0)
    snapshot['amount'] = pd.to_numeric(snapshot['amount'], errors='coerce').fillna(0.0)
    snapshot['volume'] = pd.to_numeric(snapshot['volume'], errors='coerce').fillna(0.0)
    snapshot['marcap'] = pd.to_numeric(snapshot['marcap'], errors='coerce')

    log_info(f"✅ 시세 로드: {len(snapshot)}개 종목")
    return snapshot


def _get_prev_volume(code: str, today: str) -> float:
    """
    전일 거래량 조회.
    연휴/공휴일 대응을 위해 10일 구간 조회 후 직전 거래일 사용.
    """
    try:
        start = (datetime.strptime(today, '%Y%m%d') - timedelta(days=10)).strftime('%Y%m%d')
        df = stock.get_market_ohlcv(start, today, code)
        if df is None or len(df) < 2:
            return 0.0

        vol_col = next((c for c in df.columns if c in ('거래량', 'Volume', 'volume')), None)
        if vol_col is None:
            return 0.0

        return float(df[vol_col].iloc[-2])
    except Exception:
        return 0.0


# =============================================================
# 🔍 급등 조건 판별
# =============================================================
def _get_signal_tags(row: pd.Series, vol_ratio: float) -> list:
    tags = []

    change = safe_float(row.get('change_pct', 0))
    close = safe_float(row.get('close', 0))
    open_p = safe_float(row.get('open', 0))
    high = safe_float(row.get('high', 0))
    low = safe_float(row.get('low', 0))

    if change >= 10:
        tags.append(f"🔥상한가근접(+{change:.1f}%)")
    elif change >= 7:
        tags.append(f"🚀급등(+{change:.1f}%)")
    elif change >= 5:
        tags.append(f"📈강세(+{change:.1f}%)")
    else:
        tags.append(f"📈상승(+{change:.1f}%)")

    if vol_ratio >= 5:
        tags.append(f"💥거래량{vol_ratio:.1f}배")
    elif vol_ratio >= 3:
        tags.append(f"⚡거래량{vol_ratio:.1f}배")
    else:
        tags.append(f"📊거래량{vol_ratio:.1f}배")

    if open_p > 0 and close > open_p:
        body_pct = (close - open_p) / open_p * 100
        if body_pct >= 3:
            tags.append("🕯️장대양봉")
        else:
            tags.append("🕯️양봉")

    if high > 0 and close >= high * 0.99:
        tags.append("🏆장중신고가")

    if high > low > 0:
        upper_wick_ratio = (high - max(open_p, close)) / max(high - low, 1e-9)
        if upper_wick_ratio >= 0.4:
            tags.append("⚠️윗꼬리김")

    return tags


def check_surge(row: pd.Series, vol_ratio: float, min_change_pct: float, min_vol_ratio: float, min_amount: float) -> dict | None:
    try:
        code = str(row.get('code', ''))
        close = safe_float(row.get('close', 0))
        change_pct = safe_float(row.get('change_pct', 0))
        amount_raw = safe_float(row.get('amount', 0))

        if amount_raw <= 0:
            amount_raw = safe_float(row.get('close', 0)) * safe_float(row.get('volume', 0))
        amount = amount_raw / 1e8

        if close < MIN_PRICE:
            return None
        if change_pct < min_change_pct:
            return None
        if vol_ratio < min_vol_ratio:
            return None
        if amount < min_amount:
            return None

        tags = _get_signal_tags(row, vol_ratio)
        score = (
            min(change_pct * 5, 50) +
            min(vol_ratio * 5, 30) +
            min(amount * 2, 20)
        )

        return {
            'code': code,
            'name': str(row.get('name', code)),
            'close': safe_int(close),
            'change_pct': round(change_pct, 1),
            'vol_ratio': round(vol_ratio, 1),
            'amount': round(amount, 1),
            'signal': ' '.join(tags),
            'score': round(score, 1),
        }
    except Exception:
        return None


# =============================================================
# 📡 텔레그램
# =============================================================
def send_telegram(message: str):
    if not TELEGRAM_TOKEN or not CHAT_ID_LIST:
        log_info(f"[텔레그램 미설정] {message[:120]}")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    for chat_id in CHAT_ID_LIST:
        chat_id = chat_id.strip()
        if not chat_id:
            continue
        try:
            requests.post(
                url,
                data={
                    'chat_id': chat_id,
                    'text': message[:4000],
                    'parse_mode': 'HTML',
                },
                timeout=5
            )
        except Exception as e:
            log_error(f"텔레그램 전송 실패: {e}")


def format_surge_message(hit: dict, scan_time: str) -> str:
    return (
        f"🚨 <b>[급등포착]</b> {hit['name']}({hit['code']})\n"
        f"📈 <b>{hit['change_pct']:+.1f}%</b> | 거래량 <b>{hit['vol_ratio']}배</b> | 거래대금 <b>{hit['amount']:.0f}억</b>\n"
        f"💰 현재가: <b>{hit['close']:,}원</b>\n"
        f"💡 {hit['signal']}\n"
        f"⏰ {scan_time} 포착 | 점수 {hit['score']}"
    )


# =============================================================
# 🚀 메인 스캔
# =============================================================
def run_surge_scan(force_run: bool = False) -> list:
    ts = now_kst()
    scan_time = ts.strftime('%H:%M')
    today = ts.strftime('%Y%m%d')

    market_open = ts.replace(hour=9, minute=5, second=0, microsecond=0)
    market_close = ts.replace(hour=15, minute=25, second=0, microsecond=0)

    if not force_run and not (market_open <= ts <= market_close):
        log_info(f"⏸️ 장외 시간 ({scan_time}) — 스캔 생략")
        return []

    min_change_pct, min_vol_ratio, min_amount = _get_dynamic_thresholds(ts)

    log_info(f"\n{'='*50}")
    log_info(f"🔍 급등 스캔 시작: {scan_time}")
    log_info(f"   조건: +{min_change_pct:.1f}% | 거래량 {min_vol_ratio:.1f}배 | 거래대금 {min_amount:.1f}억+")

    snapshot = _get_market_snapshot()
    if snapshot.empty:
        log_error("⚠️ 시세 수집 실패")
        return []

    # 기본 필터
    snapshot = snapshot[snapshot['close'] >= MIN_PRICE]

    if 'marcap' in snapshot.columns:
        snapshot = snapshot[(snapshot['marcap'].isna()) | (snapshot['marcap'] >= MIN_MARCAP)]

    snapshot = snapshot[snapshot['amount'] >= min_amount * 1e8]
    snapshot = snapshot[snapshot['change_pct'] >= min_change_pct]

    # 거래대금 상위 TOP_N
    if 'amount' in snapshot.columns and not snapshot.empty:
        snapshot = snapshot.nlargest(TOP_N, 'amount')

    log_info(f"📊 필터 후 대상: {len(snapshot)}개")

    if snapshot.empty:
        log_info("✅ 급등 후보 없음")
        return []

    codes = list(snapshot['code'].astype(str).values)

    prev_vols = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_get_prev_volume, code, today): code for code in codes}
        for future in as_completed(futures, timeout=60):
            code = futures[future]
            try:
                prev_vols[code] = future.result(timeout=10)
            except Exception:
                prev_vols[code] = 0.0

    hits = []
    for _, row in snapshot.iterrows():
        code = str(row.get('code', ''))
        curr_vol = safe_float(row.get('volume', 0))
        prev_vol = prev_vols.get(code, 0.0)
        vol_ratio = curr_vol / prev_vol if prev_vol > 0 else 0.0

        result = check_surge(row, vol_ratio, min_change_pct, min_vol_ratio, min_amount)
        if result:
            hits.append(result)

    hits.sort(key=lambda x: x['score'], reverse=True)
    log_info(f"🎯 급등 포착: {len(hits)}개")

    alerted = _load_alerted()
    new_alerts = []

    for hit in hits[:10]:
        code = hit['code']
        if _is_already_alerted(code, alerted):
            log_debug(f"  [{hit['name']}] 중복 알림 생략 ({ALERT_COOLDOWN}분 내)")
            continue

        msg = format_surge_message(hit, scan_time)
        send_telegram(msg)
        log_info(f"  📨 알림: {hit['name']} {hit['change_pct']:+.1f}% (거래량 {hit['vol_ratio']}배)")
        _mark_alerted(code, alerted)
        new_alerts.append(hit)

    _save_alerted(alerted)

    if len(new_alerts) >= 3:
        summary = f"📊 <b>[급등 요약] {scan_time}</b>\n총 {len(new_alerts)}종목 포착\n\n"
        for i, h in enumerate(new_alerts[:5], 1):
            summary += f"{i}. {h['name']} {h['change_pct']:+.1f}% ({h['vol_ratio']}배)\n"
        send_telegram(summary)

    if not new_alerts:
        log_info(f"✅ 신규 알림 없음 (기존 {len(hits)}개는 중복 또는 미충족)")

    return new_alerts


# =============================================================
# 🔄 장중 루프
# =============================================================
def run_loop(interval_minutes: int = 10):
    log_info(f"🔄 자동 반복 모드: {interval_minutes}분 간격")
    log_info("  종료: Ctrl+C\n")

    send_telegram(
        f"🤖 <b>급등 스캐너 시작</b>\n"
        f"⏱ {interval_minutes}분 간격 | 09:05~15:25\n"
        f"조건: 동적 기준 적용 (장 초반 완화)"
    )

    while True:
        try:
            ts = now_kst()

            if ts.weekday() < 5:
                run_surge_scan(force_run=False)

            next_run = ts + timedelta(minutes=interval_minutes)
            wait_sec = (next_run - now_kst()).total_seconds()
            log_info(f"⏳ 다음 스캔: {next_run.strftime('%H:%M')} ({wait_sec:.0f}초 후)")
            time.sleep(max(0, wait_sec))

        except KeyboardInterrupt:
            log_info("\n🛑 스캐너 종료")
            send_telegram("🛑 급등 스캐너 종료")
            break
        except Exception as e:
            log_error(f"⚠️ 스캔 오류: {e}")
            time.sleep(60)


# =============================================================
# 🚀 엔트리포인트
# =============================================================
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='실시간 급등 스캐너 (수정완성형)')
    parser.add_argument('--loop', action='store_true', help='장중 자동 반복')
    parser.add_argument('--interval', default=10, type=int, help='반복 간격(분)')
    parser.add_argument('--test', action='store_true', help='장외에도 강제 실행')
    args = parser.parse_args()

    if args.loop:
        run_loop(args.interval)
    else:
        hits = run_surge_scan(force_run=args.test)
        if not hits:
            log_info("✅ 급등 종목 없음")
        sys.exit(0)
