# =============================================================
# 🌙 nextrade_scanner.py — 넥스트레이드 기회 탐지 스캐너
# =============================================================
# 넥스트레이드(Nextrade) 장외 거래에서 본장 대비 괴리 발생 시 알림
#
# 실행 시간:
#   장전 넥스트장: 08:00~08:50 (10분마다)
#   장후 넥스트장: 15:40~23:30 (10분마다)
#
# 탐지 패턴:
#   A. 본장 종가 대비 넥스트장 급락 → 본장 시초 회복 기대 (역추세)
#   B. 장후 공시/뉴스 후 넥스트장 급등 → 본장 갭업 추종
#   C. 평소 없던 넥스트장 거래 이상 감지
#
# 주의:
#   넥스트레이드는 유동성이 극히 낮음
#   소량 주문으로 가격이 크게 튀는 경우 많음
#   패턴 A(급락 후 회복)가 가장 실전 활용도 높음
# =============================================================

import os, sys, json, time, argparse, requests, pytz
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import numpy as np

try:
    from pykrx import stock
except ImportError:
    stock = None

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
TELEGRAM_TOKEN  = os.environ.get('TELEGRAM_TOKEN', '')
CHAT_ID_LIST    = os.environ.get('TELEGRAM_CHAT_ID', '').split(',')
KST             = pytz.timezone('Asia/Seoul')

# 괴리율 임계값 (도영님 관찰 기반 튜닝)
GAP_DOWN_ALERT    = -2.0   # 급락 알림 시작 (-2%부터 탐지)
GAP_UP_ALERT      =  3.0   # 급등 알림
# 역추세 3단계 분류
REVERSAL_A        = (-2.0,  -5.0)   # A등급: -2~-5%  → 경미 눌림, 회복 일반적
REVERSAL_B        = (-5.0, -15.0)   # B등급: -5~-15% → 중간 급락, 회복 빈번
REVERSAL_C        = (-15.0, -50.0)  # C등급: -15%+   → 폭락권, 양전 케이스 존재 (주의)
MIN_PRICE         = 5_000
MIN_BASE_AMOUNT   = 5      # 본장 기준 최소 거래대금 5억 (유동성 있는 종목만)
ALERTED_FILE      = '/tmp/nextrade_alerted.json'
ALERT_COOLDOWN    = 120    # 2시간 내 재알림 금지
MAX_WORKERS       = 10     # 넥스트장은 종목 수 제한

# 분석 대상 종목 수 (본장 거래대금 상위)
TOP_N = 200

# 넥스트레이드 데이터 소스 (네이버 금융 시간외 현재가)
NAVER_NXTRD_URL = "https://finance.naver.com/item/sise.naver?code={code}"
NAVER_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Referer': 'https://finance.naver.com/',
}

SHEET_ID  = "13Esd11iwgzLN7opMYobQ3ee6huHs1FDEbyeb3Djnu6o"
SHEET_GID = "1238448456"


# =============================================================
# 🛠️ 유틸
# =============================================================

def _now() -> datetime:
    return datetime.now(KST)

def _today() -> str:
    return _now().strftime('%Y%m%d')

def _is_nextrade_hours() -> bool:
    """넥스트레이드 운영 시간인지 확인"""
    now = _now()
    if now.weekday() >= 5:
        return False

    h, m = now.hour, now.minute
    # 장전: 08:00~08:50
    if 8 == h and m <= 50:
        return True
    # 장후: 15:40~23:30
    if (h == 15 and m >= 40) or (16 <= h <= 23) or (h == 23 and m <= 30):
        return True
    return False

def _is_premarket() -> bool:
    """장전 넥스트장 (08:00~08:50)"""
    now = _now()
    return now.weekday() < 5 and now.hour == 8 and now.minute <= 50

def _is_aftermarket() -> bool:
    """장후 넥스트장 (15:40~23:30)"""
    now = _now()
    if now.weekday() >= 5:
        return False
    h, m = now.hour, now.minute
    return (h == 15 and m >= 40) or (16 <= h) or (h == 23 and m <= 30)

def _load_alerted() -> dict:
    try:
        if os.path.exists(ALERTED_FILE):
            with open(ALERTED_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return {}

def _save_alerted(alerted: dict):
    try:
        with open(ALERTED_FILE, 'w') as f:
            json.dump(alerted, f)
    except Exception:
        pass

def _is_alerted(code: str, pattern: str, alerted: dict) -> bool:
    key = f"{code}_{pattern}"
    if key not in alerted:
        return False
    elapsed = (datetime.now() - datetime.fromisoformat(alerted[key])).total_seconds() / 60
    return elapsed < ALERT_COOLDOWN

def _mark_alerted(code: str, pattern: str, alerted: dict):
    alerted[f"{code}_{pattern}"] = datetime.now().isoformat()


# =============================================================
# 📊 데이터 수집
# =============================================================

def _load_base_data() -> pd.DataFrame:
    """
    본장 종가 기준 데이터 로드 (비교 기준값).
    구글시트 → FDR → pykrx 순서.
    """
    try:
        import FinanceDataReader as fdr
    except ImportError:
        fdr = None

    # ① 구글시트
    try:
        url = (f"https://docs.google.com/spreadsheets/d/{SHEET_ID}"
               f"/export?format=csv&gid={SHEET_GID}")
        df = pd.read_csv(url, encoding="utf-8", engine="python")
        if df is not None and not df.empty and len(df) > 100:
            return df
    except Exception:
        pass

    # ② FDR
    if fdr:
        try:
            df = fdr.StockListing('KRX')
            if df is not None and not df.empty:
                return df
        except Exception:
            pass

    # ③ pykrx
    if stock:
        try:
            today = _today()
            dfs = []
            for mkt in ['KOSPI', 'KOSDAQ']:
                df_m = stock.get_market_ohlcv(today, market=mkt)
                if df_m is not None and not df_m.empty:
                    df_m = df_m.reset_index()
                    col_map = {}
                    for c in df_m.columns:
                        cs = str(c).strip()
                        if   cs in ('티커','Ticker'):        col_map[c] = 'Code'
                        elif cs in ('종가','Close'):         col_map[c] = 'Close'
                        elif cs in ('거래대금','Amount'):    col_map[c] = 'Amount'
                        elif cs in ('등락률','ChangeRate'):  col_map[c] = 'ChangeRate'
                    df_m = df_m.rename(columns=col_map)
                    df_m['Market'] = mkt
                    dfs.append(df_m)
            if dfs:
                return pd.concat(dfs, ignore_index=True)
        except Exception:
            pass

    return pd.DataFrame()


def _get_nextrade_price(code: str, base_close: float) -> dict:
    """
    넥스트레이드 현재가 조회.
    네이버 금융 시간외 단일가 페이지 스크래핑.

    반환:
        {'price': 현재가, 'volume': 거래량, 'gap_pct': 괴리율}
    """
    try:
        from bs4 import BeautifulSoup

        # 네이버 금융 종목 페이지에서 시간외 현재가 파싱
        url = f"https://finance.naver.com/item/main.naver?code={code}"
        res = requests.get(url, headers=NAVER_HEADERS, timeout=5)
        res.encoding = 'euc-kr'
        soup = BeautifulSoup(res.text, 'html.parser')

        # 시간외 현재가 (class: after_info)
        after_div = soup.select_one('.after_info')
        if not after_div:
            # 시간외 거래 없음
            return {'price': 0, 'volume': 0, 'gap_pct': 0, 'has_data': False}

        # 가격 파싱
        price_tag = after_div.select_one('.num')
        if not price_tag:
            return {'price': 0, 'volume': 0, 'gap_pct': 0, 'has_data': False}

        price_str = price_tag.text.strip().replace(',', '')
        price = float(price_str) if price_str.replace('.', '').isdigit() else 0

        if price <= 0 or base_close <= 0:
            return {'price': 0, 'volume': 0, 'gap_pct': 0, 'has_data': False}

        # 거래량
        vol = 0
        vol_tags = after_div.select('.num')
        if len(vol_tags) > 1:
            vol_str = vol_tags[1].text.strip().replace(',', '')
            vol = int(vol_str) if vol_str.isdigit() else 0

        gap_pct = (price - base_close) / base_close * 100

        return {
            'price':    price,
            'volume':   vol,
            'gap_pct':  round(gap_pct, 2),
            'has_data': True,
        }

    except Exception as e:
        log_debug(f"  [{code}] 넥스트레이드 조회 실패: {e}")
        return {'price': 0, 'volume': 0, 'gap_pct': 0, 'has_data': False}


# =============================================================
# 🎯 패턴 탐지
# =============================================================

def _classify_pattern(gap_pct: float, has_dart: bool = False) -> tuple:
    """
    괴리율로 패턴 분류 — 도영님 관찰 기반 3단계 역추세 체계.

    관찰:
      -2~3%:  넥스트장 후반 or 본장에서 회복/상승 빈번
      -15%+:  폭락처럼 보여도 양전 케이스 多

    Returns: (pattern_code, pattern_name, description, score)
    """
    # ── 하락 패턴 (역추세 3단계)
    if gap_pct < 0:
        abs_gap = abs(gap_pct)

        # A등급: -2~-5% — 경미 눌림 → 회복 일반적
        if 2.0 <= abs_gap < 5.0:
            score = 40 + abs_gap * 4   # 40~60
            return (
                'A_light', '📉A등급(-2~5%) 경미눌림',
                f"본장 종가 대비 {gap_pct:.1f}% | 넥스트장 후반 or 본장 회복 일반적",
                round(score, 1)
            )

        # B등급: -5~-15% — 중간 급락 → 회복 빈번
        elif 5.0 <= abs_gap < 15.0:
            score = 60 + abs_gap * 2   # 70~90
            dart_str = " | ⚠️공시확인권장" if has_dart else ""
            return (
                'B_mid', f'📉B등급(-5~15%) 중간급락',
                f"본장 종가 대비 {gap_pct:.1f}% | 회복 빈번 (유동성 오버슈팅){dart_str}",
                round(score, 1)
            )

        # C등급: -15%+ — 폭락권 → 양전 케이스 존재, 공시 확인 필수
        elif abs_gap >= 15.0:
            score = 55 if not has_dart else 30  # 공시 없으면 오히려 회복 기대
            dart_str = " 🚨악재공시가능" if has_dart else " (악재없으면 양전 기대)"
            return (
                'C_crash', f'📉C등급(-15%+) 폭락권',
                f"본장 종가 대비 {gap_pct:.1f}% | 양전 케이스 多{dart_str}",
                round(score, 1)
            )

    # ── 상승 패턴
    if gap_pct >= GAP_UP_ALERT:
        score = min(gap_pct * 6, 70)
        if has_dart:
            score += 20
        return (
            'UP_gap', '🚀장후급등',
            f"넥스트장 {gap_pct:.1f}% 급등 → 본장 갭업 기대" + (" (공시확인)" if has_dart else ""),
            round(score, 1)
        )

    # ── 소폭 괴리 (참고)
    if abs(gap_pct) >= 1.0:
        return ('minor', '📊소폭괴리', f"넥스트장 {gap_pct:+.1f}%", 15)

    return ('none', '', '', 0)


def _has_recent_dart(code: str) -> bool:
    """최근 DART 공시 여부 간단 체크"""
    try:
        dart_key = os.environ.get('DART_API_KEY', '')
        if not dart_key:
            return False
        # corp_code 조회
        res = requests.get(
            f"https://opendart.fss.or.kr/api/company.json",
            params={'crtfc_key': dart_key, 'stock_code': code},
            timeout=3
        )
        data = res.json()
        if data.get('status') != '000':
            return False
        corp_code = data.get('corp_code', '')
        # 오늘 공시 조회
        today = _today()
        res2 = requests.get(
            f"https://opendart.fss.or.kr/api/list.json",
            params={
                'crtfc_key': dart_key, 'corp_code': corp_code,
                'bgn_de': today, 'end_de': today, 'page_count': 5
            },
            timeout=3
        )
        data2 = res2.json()
        return data2.get('status') == '000' and len(data2.get('list', [])) > 0
    except Exception:
        return False


# =============================================================
# 🚀 메인 스캔
# =============================================================

def run_nextrade_scan() -> list:
    """넥스트레이드 기회 탐지 1회 실행"""
    now      = _now()
    now_str  = now.strftime('%H:%M')
    is_pre   = _is_premarket()
    is_after = _is_aftermarket()

    if not (is_pre or is_after):
        log_info(f"⏸️ 넥스트레이드 비활성 시간 ({now_str})")
        return []

    session = "장전" if is_pre else "장후"
    log_info(f"\n{'='*55}")
    log_info(f"🌙 넥스트레이드 {session} 스캔: {now_str}")
    log_info(f"{'='*55}")

    # 본장 기준 데이터
    base_df = _load_base_data()
    if base_df is None or base_df.empty:
        log_error("⚠️ 본장 기준 데이터 로드 실패")
        return []

    # 컬럼 정규화
    col_map = {}
    for c in base_df.columns:
        cs = str(c).strip()
        if   cs in ('Code','code','종목코드','티커'):     col_map[c] = 'code'
        elif cs in ('Name','name','종목명'):               col_map[c] = 'name'
        elif cs in ('Close','close','종가','현재가'):      col_map[c] = 'close'
        elif cs in ('Amount','amount','거래대금'):         col_map[c] = 'amount'
        elif cs in ('Market','market'):                    col_map[c] = 'market'
    base_df = base_df.rename(columns=col_map)

    # 필터: 본장 거래대금 상위 + 최소 가격
    if 'close' in base_df.columns:
        base_df = base_df[base_df['close'] >= MIN_PRICE]
    if 'amount' in base_df.columns:
        base_df = base_df[base_df['amount'] >= MIN_BASE_AMOUNT * 1e8]
        base_df = base_df.nlargest(TOP_N, 'amount')

    log_info(f"📊 분석 대상: {len(base_df)}개")

    # 넥스트레이드 현재가 병렬 조회
    def _check(row):
        try:
            code      = str(row.get('code', ''))
            name      = str(row.get('name', code))
            base_close = float(row.get('close', 0))

            if not code or base_close <= 0:
                return None

            nt = _get_nextrade_price(code, base_close)
            if not nt.get('has_data') or nt['price'] <= 0:
                return None

            gap_pct   = nt['gap_pct']
            nt_price  = nt['price']
            nt_vol    = nt['volume']

            # 최소 괴리율 필터 (-2% or +3% 이상부터 탐지)
            if gap_pct > -2.0 and gap_pct < GAP_UP_ALERT:
                return None

            has_dart = _has_recent_dart(code) if abs(gap_pct) >= 3 else False
            pattern_code, pattern_name, desc, score = _classify_pattern(gap_pct, has_dart)

            if pattern_code == 'none' or score < 20:
                return None

            return {
                'code':         code,
                'name':         name,
                'base_close':   int(base_close),
                'nt_price':     int(nt_price),
                'nt_volume':    nt_vol,
                'gap_pct':      gap_pct,
                'pattern_code': pattern_code,
                'pattern_name': pattern_name,
                'desc':         desc,
                'score':        score,
                'has_dart':     has_dart,
                'session':      session,
            }
        except Exception:
            return None

    hits = []
    rows = [row for _, row in base_df.iterrows()]
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for res in ex.map(_check, rows):
            if res:
                hits.append(res)

    hits.sort(key=lambda x: x['score'], reverse=True)
    log_info(f"🎯 넥스트레이드 기회 포착: {len(hits)}개")

    # 알림 전송
    alerted  = _load_alerted()
    new_hits = []

    for hit in hits[:8]:
        code    = hit['code']
        pattern = hit['pattern_code']

        if _is_alerted(code, pattern, alerted):
            continue

        msg = _format_message(hit, now_str)
        _send_telegram(msg)
        log_info(f"  📨 [{hit['name']}] {hit['pattern_name']} "
                 f"(본장:{hit['base_close']:,} → 넥스트:{hit['nt_price']:,} / {hit['gap_pct']:+.1f}%)")
        _mark_alerted(code, pattern, alerted)
        new_hits.append(hit)

    _save_alerted(alerted)

    if not new_hits and hits:
        log_info(f"  (신규 알림 없음 — 중복 {len(hits)}건)")
    elif not hits:
        log_info("  ✅ 유의미한 괴리 없음")

    return new_hits


def _format_message(hit: dict, scan_time: str) -> str:
    """텔레그램 메시지 포맷"""
    gap_pct  = hit['gap_pct']
    # 등급별 이모지
    pcode = hit['pattern_code']
    if pcode == 'A_light':   arrow = "📉→"
    elif pcode == 'B_mid':   arrow = "📉📉→"
    elif pcode == 'C_crash': arrow = "🔥→"
    elif pcode == 'UP_gap':  arrow = "🚀"
    else:                    arrow = "📊"
    dart_str = " 🚨공시있음" if hit.get('has_dart') else ""

    # 패턴별 전략 힌트
    strategy = {
        'A_light': "💡 전략: 본장 시초가 매수 → 전일 종가 회복 목표 | 손절 -2% 추가 이탈 시",
        'B_mid':   "💡 전략: 본장 시초가 눌림 확인 후 매수 | 공시 없으면 강한 반등 기대",
        'C_crash': "💡 전략: 공시 없으면 양전 기대 | 본장 시초 -3% 이내면 매수 고려",
        'UP_gap':  "💡 전략: 본장 시초가 갭업 확인 후 초동 단타 | 갭 크면 시초 매도 고려",
        'minor':   "💡 전략: 참고 수준 — 본장 시초 방향 확인",
    }.get(hit['pattern_code'], "")

    return (
        f"🌙 <b>[넥스트레이드 {hit['session']}]</b> "
        f"{hit['name']}({hit['code']}){dart_str}\n"
        f"📌 <b>{hit['pattern_name']}</b>\n"
        f"💡 {hit['desc']}\n"
        f"💰 본장종가: <b>{hit['base_close']:,}원</b> "
        f"{arrow} 넥스트: <b>{hit['nt_price']:,}원</b> "
        f"(<b>{gap_pct:+.1f}%</b>)\n"
        f"📊 넥스트 거래량: {hit['nt_volume']:,}주\n"
        f"{strategy}\n"
        f"⏰ {scan_time} | 점수 {hit['score']:.0f}"
    )


def _send_telegram(msg: str):
    if not TELEGRAM_TOKEN:
        log_info(f"[텔레그램 미설정]\n{msg[:200]}")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    for chat_id in CHAT_ID_LIST:
        chat_id = chat_id.strip()
        if not chat_id:
            continue
        try:
            requests.post(url, data={
                'chat_id': chat_id, 'text': msg[:4000], 'parse_mode': 'HTML'
            }, timeout=5)
        except Exception as e:
            log_error(f"텔레그램 실패: {e}")


# =============================================================
# 🔄 자동 반복
# =============================================================

def run_loop(interval: int = 10):
    log_info(f"🌙 넥스트레이드 스캐너 시작: {interval}분 간격")
    _send_telegram(
        f"🌙 <b>넥스트레이드 스캐너 시작</b>\n"
        f"⏱ {interval}분 간격\n"
        f"패턴: 역추세회복 | 장후급등 | 괴리감지"
    )
    while True:
        try:
            if _is_nextrade_hours():
                run_nextrade_scan()
            else:
                log_info(f"⏸️ 비활성 시간 ({_now().strftime('%H:%M')})")

            next_run = _now() + timedelta(minutes=interval)
            wait = (next_run - _now()).total_seconds()
            log_info(f"⏳ 다음 스캔: {next_run.strftime('%H:%M')}")
            time.sleep(max(0, wait))

        except KeyboardInterrupt:
            log_info("\n🛑 종료")
            _send_telegram("🛑 넥스트레이드 스캐너 종료")
            break
        except Exception as e:
            log_error(f"⚠️ 오류: {e}")
            time.sleep(60)


# =============================================================
# 🚀 엔트리포인트
# =============================================================

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='넥스트레이드 기회 탐지 스캐너')
    parser.add_argument('--loop',     action='store_true', help='자동 반복')
    parser.add_argument('--interval', default=10, type=int, help='반복 간격 (분)')
    parser.add_argument('--test',     action='store_true', help='강제 실행 (시간 무관)')
    args = parser.parse_args()

    if args.test:
        log_info("⚠️ 테스트 모드 — 시간 체크 무시")

    if args.loop:
        run_loop(args.interval)
    else:
        if args.test or _is_nextrade_hours():
            hits = run_nextrade_scan()
            if not hits:
                log_info("✅ 유의미한 기회 없음")
        else:
            log_info(f"⏸️ 넥스트레이드 비활성 시간 ({_now().strftime('%H:%M')})")
            log_info("  장전: 08:00~08:50 / 장후: 15:40~23:30")
        sys.exit(0)
