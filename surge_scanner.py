# =============================================================
# 🚨 surge_scanner.py — 실시간 급등 포착 스캐너
# =============================================================
# 10분마다 실행 → 급등 조건 충족 시 텔레그램 즉시 알림
#
# 실행 방법:
#   python surge_scanner.py              # 1회 실행
#   python surge_scanner.py --loop       # 장중 자동 반복 (로컬)
#
# GitHub Actions (scan.yml):
#   schedule: '*/10 0,1,2,3,4,5,6 * * 1-5'  # UTC 9:00~15:30
# =============================================================

import os
import sys
import json
import time
import argparse
import requests
import pytz
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import numpy as np
from pykrx import stock

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
TELEGRAM_TOKEN   = os.environ.get('TELEGRAM_TOKEN', '')
CHAT_ID_LIST     = os.environ.get('TELEGRAM_CHAT_ID', '').split(',')
KST              = pytz.timezone('Asia/Seoul')

# 급등 탐지 조건
MIN_CHANGE_PCT   = 3.0     # 당일 등락률 최소 +3%
MIN_VOL_RATIO    = 2.5     # 거래량 배율 (전일 대비) 최소 2.5배
MIN_AMOUNT       = 5       # 최소 거래대금 5억
MIN_PRICE        = 5_000   # 동전주 제외 5,000원 이상
MIN_MARCAP       = 30_000_000_000  # 시총 300억 이상

# 알림 중복 방지 (파일 기반 — GitHub Actions 비영구)
ALERTED_FILE     = '/tmp/surge_alerted.json'
ALERT_COOLDOWN   = 60      # 분 단위 (같은 종목 60분 내 재알림 금지)

# 스캔 대상 (코스피+코스닥 거래대금 상위)
TOP_N            = 300     # 상위 300종목만 (속도)
MAX_WORKERS      = 20      # 병렬 스레드


# =============================================================
# 📋 알림 기록 관리
# =============================================================

def _load_alerted() -> dict:
    """이미 알린 종목 목록 로드"""
    try:
        if os.path.exists(ALERTED_FILE):
            with open(ALERTED_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def _save_alerted(alerted: dict):
    """알린 종목 목록 저장"""
    try:
        with open(ALERTED_FILE, 'w') as f:
            json.dump(alerted, f)
    except Exception:
        pass


def _is_already_alerted(code: str, alerted: dict) -> bool:
    """최근 ALERT_COOLDOWN분 내 이미 알렸는지 확인"""
    if code not in alerted:
        return False
    last_time = datetime.fromisoformat(alerted[code])
    elapsed   = (datetime.now() - last_time).total_seconds() / 60
    return elapsed < ALERT_COOLDOWN


def _mark_alerted(code: str, alerted: dict):
    alerted[code] = datetime.now().isoformat()


# =============================================================
# 📊 당일 시세 수집 — pykrx 기반
# =============================================================

def _normalize_ohlcv(df: pd.DataFrame, market: str) -> pd.DataFrame:
    """
    pykrx 버전에 따라 컬럼명이 달라지는 문제 해결.
    
    구버전 pykrx: '시가','고가','저가','종가','거래량','거래대금','등락률'
    신버전 pykrx: 'Open','High','Low','Close','Volume','Amount','Change' 또는
                  '시가','고가','저가','종가' (한글 유지 버전도 있음)
    인덱스: '티커' 또는 'Ticker' 또는 '종목코드'
    """
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    df = df.reset_index()
    # 공백 제거
    df.columns = [str(c).strip() for c in df.columns]

    # ── 한글 → 영문 매핑 (모든 알려진 패턴 포함)
    KR_TO_EN = {
        # 코드/티커
        '티커': 'code', 'Ticker': 'code', '종목코드': 'code',
        # 가격
        '시가': 'open',   'Open': 'open',
        '고가': 'high',   'High': 'high',
        '저가': 'low',    'Low': 'low',
        '종가': 'close',  'Close': 'close', '현재가': 'close',
        # 거래량/대금
        '거래량': 'volume',  'Volume': 'volume',
        '거래대금': 'amount', 'Amount': 'amount', 'Turnover': 'amount',
        # 등락
        '등락률': 'change_pct', '변동률': 'change_pct',
        'Change': 'change_pct', 'ChangeRate': 'change_pct',
        # 시총
        '시가총액': 'marcap', 'Marcap': 'marcap', 'MarCap': 'marcap',
        # 종목명
        '종목명': 'name', 'Name': 'name',
    }

    rename = {}
    for c in df.columns:
        c_clean = str(c).strip()
        if c_clean in KR_TO_EN:
            rename[c] = KR_TO_EN[c_clean]
        # 부분 매칭 (컬럼명에 키워드 포함)
        elif '티커' in c_clean or c_clean.lower() == 'ticker':
            rename[c] = 'code'
        elif '종가' in c_clean or '현재가' in c_clean:
            rename[c] = 'close'
        elif '시가' in c_clean and '총' not in c_clean:
            rename[c] = 'open'
        elif '고가' in c_clean:
            rename[c] = 'high'
        elif '저가' in c_clean:
            rename[c] = 'low'
        elif '거래량' in c_clean:
            rename[c] = 'volume'
        elif '등락률' in c_clean or '변동률' in c_clean:
            rename[c] = 'change_pct'
        elif '거래대금' in c_clean:
            rename[c] = 'amount'
        elif '시가총액' in c_clean:
            rename[c] = 'marcap'

    df = df.rename(columns=rename)
    df['market'] = market

    # ── 종목명 보강 (없으면 pykrx로 조회)
    if 'name' not in df.columns and 'code' in df.columns:
        try:
            today = datetime.now(KST).strftime('%Y%m%d')
            name_map = {t: stock.get_market_ticker_name(t)
                        for t in df['code'].tolist()[:500]}
            df['name'] = df['code'].map(name_map).fillna(df['code'])
        except Exception:
            df['name'] = df.get('code', '')

    return df


def _get_today_str() -> str:
    now = datetime.now(KST)
    # 장 시작 전이면 전 거래일
    if now.hour < 9:
        return (now - timedelta(days=1)).strftime('%Y%m%d')
    return now.strftime('%Y%m%d')


def _get_market_snapshot() -> pd.DataFrame:
    """
    코스피 + 코스닥 전체 당일 시세 한 번에 수집.
    컬럼: 종목코드, 종목명, 현재가, 등락률, 거래량, 거래대금, 시가총액
    """
    today = _get_today_str()
    dfs = []

    for market in ['KOSPI', 'KOSDAQ']:
        try:
            df = stock.get_market_ohlcv(today, market=market)
            if df is None or df.empty:
                continue
            # ✅ BUGFIX: 한글/영문 컬럼명 통합 정규화
            df = _normalize_ohlcv(df, market)
            if df.empty:
                continue
            dfs.append(df)
        except Exception as e:
            log_error(f"⚠️ {market} 시세 수집 실패: {e}")

    if not dfs:
        return pd.DataFrame()

    result = pd.concat(dfs, ignore_index=True)
    return result


def _get_prev_volume(code: str, today: str) -> float:
    """전일 거래량 조회 (비율 계산용)"""
    try:
        prev = (datetime.strptime(today, '%Y%m%d') - timedelta(days=3)).strftime('%Y%m%d')
        df   = stock.get_market_ohlcv(prev, today, code)
        if df is None or len(df) < 2:
            return 0
        return float(df['거래량'].iloc[-2])   # 전일
    except Exception:
        return 0


# =============================================================
# 🔍 급등 조건 판별
# =============================================================

def _get_signal_tags(row: pd.Series, vol_ratio: float) -> list:
    """보조 신호 태그 생성"""
    tags = []

    change = float(row.get('change_pct', 0))
    close  = float(row.get('close', 0))
    open_p = float(row.get('open', 0))
    high   = float(row.get('high', 0))

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

    # 양봉 여부
    if open_p > 0 and close > open_p:
        body_pct = (close - open_p) / open_p * 100
        if body_pct >= 3:
            tags.append("🕯️장대양봉")
        else:
            tags.append("🕯️양봉")

    # 장중 신고가 (고가 = 현재가 근처)
    if high > 0 and close >= high * 0.99:
        tags.append("🏆장중신고가")

    return tags


def check_surge(row: pd.Series, vol_ratio: float) -> dict | None:
    """
    급등 조건 체크.
    충족 시 결과 dict 반환, 미충족 시 None.
    """
    try:
        code       = str(row.get('code', ''))
        close      = float(row.get('close', 0))
        change_pct = float(row.get('change_pct', 0))
        volume     = float(row.get('volume', 0))
        amount     = float(row.get('amount', 0)) / 1e8  # 억

        # ── 필수 조건 (AND)
        if close < MIN_PRICE:           return None   # 동전주
        if change_pct < MIN_CHANGE_PCT: return None   # 등락률 미달
        if vol_ratio < MIN_VOL_RATIO:   return None   # 거래량 배율 미달
        if amount < MIN_AMOUNT:         return None   # 거래대금 미달

        tags      = _get_signal_tags(row, vol_ratio)
        signal_str = ' '.join(tags)
        score     = (
            min(change_pct * 5, 50) +        # 등락률 점수 (최대 50)
            min(vol_ratio * 5, 30) +          # 거래량 배율 점수 (최대 30)
            min(amount * 2, 20)               # 거래대금 점수 (최대 20)
        )

        return {
            'code':       code,
            'name':       str(row.get('name', code)),
            'close':      int(close),
            'change_pct': round(change_pct, 1),
            'vol_ratio':  round(vol_ratio, 1),
            'amount':     round(amount, 1),
            'signal':     signal_str,
            'score':      round(score, 1),
        }
    except Exception:
        return None


# =============================================================
# 📡 텔레그램 전송
# =============================================================

def send_telegram(message: str):
    """텔레그램 메시지 전송"""
    if not TELEGRAM_TOKEN or not CHAT_ID_LIST:
        log_info(f"[텔레그램 미설정] {message[:100]}")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    for chat_id in CHAT_ID_LIST:
        chat_id = chat_id.strip()
        if not chat_id:
            continue
        try:
            requests.post(url, data={
                'chat_id':    chat_id,
                'text':       message[:4000],
                'parse_mode': 'HTML',
            }, timeout=5)
        except Exception as e:
            log_error(f"텔레그램 전송 실패: {e}")


def format_surge_message(hit: dict, scan_time: str) -> str:
    """급등 알림 메시지 포맷"""
    return (
        f"🚨 <b>[급등포착]</b> {hit['name']}({hit['code']})\n"
        f"📈 <b>{hit['change_pct']:+.1f}%</b> | "
        f"거래량 <b>{hit['vol_ratio']}배</b> | "
        f"거래대금 <b>{hit['amount']:.0f}억</b>\n"
        f"💰 현재가: <b>{hit['close']:,}원</b>\n"
        f"💡 {hit['signal']}\n"
        f"⏰ {scan_time} 포착 | 점수 {hit['score']}"
    )


# =============================================================
# 🚀 메인 스캔 함수
# =============================================================

def run_surge_scan() -> list:
    """
    1회 급등 스캔 실행.
    Returns: 포착된 급등 종목 리스트
    """
    now_kst   = datetime.now(KST)
    scan_time = now_kst.strftime('%H:%M')
    today     = now_kst.strftime('%Y%m%d')

    # 장 시간 체크 (09:05 ~ 15:25)
    market_open  = now_kst.replace(hour=9,  minute=5,  second=0, microsecond=0)
    market_close = now_kst.replace(hour=15, minute=25, second=0, microsecond=0)
    if not (market_open <= now_kst <= market_close):
        log_info(f"⏸️ 장외 시간 ({scan_time}) — 스캔 생략")
        return []

    log_info(f"\n{'='*50}")
    log_info(f"🔍 급등 스캔 시작: {scan_time}")

    # 1. 전체 시세 수집
    snapshot = _get_market_snapshot()
    if snapshot.empty:
        log_error("⚠️ 시세 수집 실패")
        return []

    # 2. 기본 필터 (가격/거래대금)
    if 'close' in snapshot.columns:
        snapshot = snapshot[snapshot['close'] >= MIN_PRICE]
    if 'amount' in snapshot.columns:
        snapshot = snapshot[snapshot['amount'] >= MIN_AMOUNT * 1e8]
    if 'change_pct' in snapshot.columns:
        snapshot = snapshot[snapshot['change_pct'] >= MIN_CHANGE_PCT]

    # 3. 거래대금 상위 TOP_N으로 제한
    if 'amount' in snapshot.columns:
        snapshot = snapshot.nlargest(TOP_N, 'amount')

    log_info(f"📊 필터 후 대상: {len(snapshot)}개")

    if snapshot.empty:
        log_info("✅ 급등 후보 없음")
        return []

    # 4. 전일 거래량 병렬 조회 → 배율 계산
    codes = list(snapshot['code'].values) if 'code' in snapshot.columns else []

    prev_vols = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_get_prev_volume, code, today): code
            for code in codes
        }
        for future in as_completed(futures, timeout=60):
            code = futures[future]
            try:
                prev_vols[code] = future.result(timeout=10)
            except Exception:
                prev_vols[code] = 0

    # 5. 급등 조건 체크
    hits = []
    for _, row in snapshot.iterrows():
        code     = str(row.get('code', ''))
        curr_vol = float(row.get('volume', 0))
        prev_vol = prev_vols.get(code, 0)

        vol_ratio = curr_vol / prev_vol if prev_vol > 0 else 0
        result    = check_surge(row, vol_ratio)
        if result:
            hits.append(result)

    # 점수 순 정렬
    hits.sort(key=lambda x: x['score'], reverse=True)
    log_info(f"🎯 급등 포착: {len(hits)}개")

    # 6. 알림 전송 (중복 제거)
    alerted = _load_alerted()
    new_alerts = []

    for hit in hits[:10]:  # 최대 10개
        code = hit['code']
        if _is_already_alerted(code, alerted):
            log_debug(f"  [{hit['name']}] 중복 알림 생략 (60분 내)")
            continue

        msg = format_surge_message(hit, scan_time)
        send_telegram(msg)
        log_info(f"  📨 알림: {hit['name']} {hit['change_pct']:+.1f}% (거래량{hit['vol_ratio']}배)")
        _mark_alerted(code, alerted)
        new_alerts.append(hit)

    _save_alerted(alerted)

    # 7. 배치 요약 (알림이 3개 이상이면 요약도 전송)
    if len(new_alerts) >= 3:
        summary = (
            f"📊 <b>[급등 요약] {scan_time}</b>\n"
            f"총 {len(new_alerts)}종목 포착\n\n"
        )
        for i, h in enumerate(new_alerts[:5], 1):
            summary += f"{i}. {h['name']} {h['change_pct']:+.1f}% ({h['vol_ratio']}배)\n"
        send_telegram(summary)

    if not new_alerts:
        log_info(f"  ✅ 신규 알림 없음 (기존 {len(hits)}개는 중복)")

    return new_alerts


# =============================================================
# 🔄 장중 자동 반복 (로컬 실행용)
# =============================================================

def run_loop(interval_minutes: int = 10):
    """장중 N분마다 자동 스캔"""
    log_info(f"🔄 자동 반복 모드: {interval_minutes}분 간격")
    log_info("  종료: Ctrl+C\n")

    send_telegram(
        f"🤖 <b>급등 스캐너 시작</b>\n"
        f"⏱ {interval_minutes}분 간격 | 09:05~15:25\n"
        f"조건: +{MIN_CHANGE_PCT}% | 거래량{MIN_VOL_RATIO}배 | 거래대금{MIN_AMOUNT}억+"
    )

    while True:
        try:
            now = datetime.now(KST)

            # 장 시간이면 스캔
            if now.weekday() < 5:  # 월~금
                run_surge_scan()

            # 다음 실행까지 대기
            next_run = now + timedelta(minutes=interval_minutes)
            wait_sec = (next_run - datetime.now(KST)).total_seconds()
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
    parser = argparse.ArgumentParser(description='실시간 급등 스캐너')
    parser.add_argument('--loop',     action='store_true', help='장중 자동 반복 (로컬용)')
    parser.add_argument('--interval', default=10, type=int, help='반복 간격 (분)')
    parser.add_argument('--test',     action='store_true', help='장외에도 강제 실행 (테스트용)')
    args = parser.parse_args()

    if args.test:
        # 장 시간 체크 우회
        MIN_CHANGE_PCT = 0.0
        MIN_VOL_RATIO  = 0.0
        log_info("⚠️ 테스트 모드 — 조건 완화")

    if args.loop:
        run_loop(args.interval)
    else:
        hits = run_surge_scan()
        if not hits:
            log_info("✅ 급등 종목 없음")
        sys.exit(0)
