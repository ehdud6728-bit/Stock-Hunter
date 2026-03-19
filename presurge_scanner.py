# =============================================================
# 🎯 presurge_scanner.py — 선제적 급등 징후 감지 스캐너
# =============================================================
# "이미 올랐다" 가 아니라 "곧 터질 것 같다"를 먼저 포착
#
# 5가지 선제 신호:
#   ① 거래량 누적 이상 — 오전 30분에 전일 하루치 50%+ 이미 소화
#   ② 분봉 에너지 압축  — BB폭 최근 5일 최소값 (방향 결정 직전)
#   ③ 섹터 선행주 동반  — 대장주 오르면 후행주 예측
#   ④ 시간외 이상 징후  — 장전 시간외 거래에서 이미 움직임
#   ⑤ 전일 종가 세력매집 — 전날 종가 기준 5가지 매집 흔적 복합 판단
#
# 실행:
#   python presurge_scanner.py              # 1회 실행
#   python presurge_scanner.py --loop       # 장중 자동 반복
# =============================================================

import os, sys, json, time, argparse, requests, pytz
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import numpy as np
import FinanceDataReader as fdr
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
TELEGRAM_TOKEN  = os.environ.get('TELEGRAM_TOKEN', '')
CHAT_ID_LIST    = os.environ.get('TELEGRAM_CHAT_ID', '').split(',')
KST             = pytz.timezone('Asia/Seoul')

# 필터 기준
MIN_PRICE       = 5_000           # 5,000원 미만 제외
MIN_MARCAP      = 30_000_000_000  # 시총 300억 미만 제외
MIN_AMOUNT_PREV = 3               # 전일 거래대금 최소 3억 (아주 얇은 종목 제외)
TOP_N           = 400             # 거래대금 상위 N종목

# 선제 신호 임계값
VOL_ACCUM_RATIO  = 0.4   # ① 오전 N분에 전일 거래량의 40%+ 이미 소화
BB_COMPRESS_PCT  = 0.3   # ② BB폭이 최근 20일 하위 30% 이하
SECTOR_LEAD_PCT  = 2.0   # ③ 대장주 2% 이상 상승 시 추종주 탐색
PRE_MARKET_PCT   = 1.5   # ④ 시간외 거래 1.5% 이상 변동

# 알림 중복 방지
ALERTED_FILE    = '/tmp/presurge_alerted.json'
ALERT_COOLDOWN  = 90    # 90분 내 재알림 금지
MAX_WORKERS     = 15


# =============================================================
# 📋 섹터 대장주 맵 (선제 신호 ③용)
# =============================================================
SECTOR_LEADERS = {
    '반도체':   {'leader': '000660', 'leader_name': 'SK하이닉스',
                 'followers': ['042700', '240810', '336370', '058470']},
    '2차전지':  {'leader': '373220', 'leader_name': 'LG에너지솔루션',
                 'followers': ['051910', '006400', '247540', '096770']},
    '바이오':   {'leader': '068270', 'leader_name': '셀트리온',
                 'followers': ['207940', '326030', '145020', '091990']},
    '조선':     {'leader': '009540', 'leader_name': 'HD한국조선해양',
                 'followers': ['010140', '042660', '003570', '007570']},
    '방산':     {'leader': '012450', 'leader_name': '한화에어로스페이스',
                 'followers': ['047810', '064350', '079550', '272210']},
    '자동차':   {'leader': '005380', 'leader_name': '현대차',
                 'followers': ['000270', '012330', '204320', '003620']},
}


# =============================================================
# 🛠️ 공통 유틸
# =============================================================

def _today() -> str:
    now = datetime.now(KST)
    return now.strftime('%Y%m%d')

def _now_str() -> str:
    return datetime.now(KST).strftime('%H:%M')

def _is_market_hours() -> bool:
    now = datetime.now(KST)
    return now.weekday() < 5 and \
           now.replace(hour=9, minute=0) <= now <= now.replace(hour=15, minute=30)

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

def _is_alerted(code: str, alerted: dict) -> bool:
    if code not in alerted:
        return False
    elapsed = (datetime.now() - datetime.fromisoformat(alerted[code])).total_seconds() / 60
    return elapsed < ALERT_COOLDOWN

def _mark_alerted(code: str, alerted: dict):
    alerted[code] = datetime.now().isoformat()


# =============================================================
# 📊 시세 스냅샷
# =============================================================

def _get_snapshot() -> pd.DataFrame:
    """오늘 전 종목 시세 (pykrx)"""
    today = _today()
    dfs = []
    for market in ['KOSPI', 'KOSDAQ']:
        try:
            df = stock.get_market_ohlcv(today, market=market)
            if df is not None and not df.empty:
                df = df.reset_index()
                df['market'] = market
                dfs.append(df)
        except Exception as e:
            log_error(f"⚠️ {market} 시세 실패: {e}")

    if not dfs:
        return pd.DataFrame()

    result = pd.concat(dfs, ignore_index=True)

    # 컬럼 정규화
    col_map = {}
    for c in result.columns:
        cl = c.replace(' ', '')
        if '티커' in cl or cl == '종목코드': col_map[c] = 'code'
        elif '종가' in cl:                   col_map[c] = 'close'
        elif '시가' in cl and '총' not in cl: col_map[c] = 'open'
        elif '고가' in cl:                   col_map[c] = 'high'
        elif '저가' in cl:                   col_map[c] = 'low'
        elif '거래량' in cl:                 col_map[c] = 'volume'
        elif '등락률' in cl:                 col_map[c] = 'change_pct'
        elif '거래대금' in cl:               col_map[c] = 'amount'
    result = result.rename(columns=col_map)

    # 종목명 보강
    try:
        name_map = {}
        for market in ['KOSPI', 'KOSDAQ']:
            tickers = stock.get_market_ticker_list(today, market=market)
            for t in tickers:
                name_map[t] = stock.get_market_ticker_name(t)
        if 'code' in result.columns:
            result['name'] = result['code'].map(name_map).fillna(result.get('code', ''))
    except Exception:
        if 'name' not in result.columns:
            result['name'] = result.get('code', '')

    return result


# =============================================================
# 🎯 선제 신호 ① — 오전 거래량 누적 이상
# =============================================================

def signal_vol_accumulation(snapshot: pd.DataFrame) -> list:
    """
    오전 장 시작 후 경과 시간 대비 거래량이 비정상적으로 많은 종목.

    로직:
      expected_vol = 전일 거래량 × (경과분 / 390분)   ← 하루 6.5시간
      actual_vol   = 오늘 거래량 (현재까지)
      ratio        = actual_vol / expected_vol
      → ratio >= VOL_ACCUM_RATIO(0.4) 이상이면 이상 거래량
    """
    now = datetime.now(KST)
    open_time = now.replace(hour=9, minute=0, second=0, microsecond=0)
    elapsed_min = max(1, (now - open_time).total_seconds() / 60)
    today = _today()

    results = []
    if 'code' not in snapshot.columns or 'volume' not in snapshot.columns:
        return results

    # 전일 거래량 일괄 조회 (상위 종목만)
    top_codes = snapshot.nlargest(TOP_N, 'amount')['code'].tolist() \
                if 'amount' in snapshot.columns else snapshot['code'].tolist()[:TOP_N]

    def _get_prev_vol(code):
        try:
            prev_date = (datetime.strptime(today, '%Y%m%d') - timedelta(days=5)).strftime('%Y%m%d')
            df = stock.get_market_ohlcv(prev_date, today, code)
            if df is None or len(df) < 2:
                return code, 0
            return code, float(df['거래량'].iloc[-2])
        except Exception:
            return code, 0

    prev_vols = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for code, vol in ex.map(_get_prev_vol, top_codes):
            prev_vols[code] = vol

    for _, row in snapshot.iterrows():
        code     = str(row.get('code', ''))
        if code not in top_codes:
            continue
        curr_vol = float(row.get('volume', 0))
        close    = float(row.get('close', 0))
        amount   = float(row.get('amount', 0)) / 1e8
        change   = float(row.get('change_pct', 0))
        prev_vol = prev_vols.get(code, 0)

        if close < MIN_PRICE or amount < MIN_AMOUNT_PREV:
            continue
        if prev_vol <= 0:
            continue

        # 이미 많이 오른 건 제외 (이미 터진 것 → 선제가 아님)
        if change >= 5:
            continue

        # 경과 시간 기준 기대 거래량 대비 실제 배율
        expected = prev_vol * (elapsed_min / 390)
        ratio    = curr_vol / expected if expected > 0 else 0

        if ratio >= (1 / VOL_ACCUM_RATIO):  # 기대치의 2.5배 이상
            results.append({
                'code':    code,
                'name':    str(row.get('name', code)),
                'close':   int(close),
                'change':  round(change, 1),
                'amount':  round(amount, 1),
                'signal_type': '① 거래량누적이상',
                'signal_detail': f"현재까지 거래량이 기대치의 {ratio:.1f}배 (경과 {elapsed_min:.0f}분)",
                'score': min(ratio * 10, 50),
            })
            log_debug(f"  ① [{row.get('name',code)}] 거래량 {ratio:.1f}배 누적")

    return results


# =============================================================
# 🎯 선제 신호 ② — 분봉 BB 에너지 압축
# =============================================================

def signal_bb_compression(top_codes: list, snapshot_map: dict) -> list:
    """
    일봉 기준 BB40폭이 최근 20일 중 최저 수준 → 에너지 응축 완료.
    (분봉 API 없으므로 일봉 BB폭 근사)
    """
    results = []
    today = _today()
    start = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')

    def _check_bb(code):
        try:
            row = snapshot_map.get(code, {})
            close = float(row.get('close', 0))
            change = float(row.get('change_pct', 0))
            amount = float(row.get('amount', 0)) / 1e8

            if close < MIN_PRICE or amount < MIN_AMOUNT_PREV:
                return None
            if change >= 5 or change <= -3:  # 이미 터졌거나 급락 중이면 제외
                return None

            df = fdr.DataReader(code, start=start)
            if df is None or len(df) < 25:
                return None

            # BB40 폭 계산
            std40 = df['Close'].rolling(40).std()
            ma40  = df['Close'].rolling(40).mean()
            bb40w = (std40 * 4 / ma40 * 100).dropna()

            if len(bb40w) < 20:
                return None

            curr_bb  = float(bb40w.iloc[-1])
            min_20d  = float(bb40w.tail(20).min())
            pct_rank = float((bb40w.tail(20) <= curr_bb).mean())  # 하위 몇 %인지

            if pct_rank > BB_COMPRESS_PCT:
                return None

            # OBV 방향 확인 (하락 압축이 아닌 상승 응축인지)
            obv   = (np.sign(df['Close'].diff()) * df['Volume']).fillna(0).cumsum()
            obv_rising = float(obv.iloc[-1]) > float(obv.iloc[-5])

            return {
                'code':    code,
                'name':    str(row.get('name', code)),
                'close':   int(close),
                'change':  round(change, 1),
                'amount':  round(amount, 1),
                'signal_type': '② BB압축응축',
                'signal_detail': (
                    f"BB40폭 {curr_bb:.1f} (20일 하위 {pct_rank*100:.0f}%) | "
                    f"OBV{'상승↑' if obv_rising else '하락↓'}"
                ),
                'score': (1 - pct_rank) * 40 + (10 if obv_rising else 0),
            }
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for res in ex.map(_check_bb, top_codes[:200]):  # 상위 200개만
            if res:
                results.append(res)
                log_debug(f"  ② [{res['name']}] BB압축 {res['signal_detail'][:40]}")

    return results


# =============================================================
# 🎯 선제 신호 ③ — 섹터 선행주 동반 감지
# =============================================================

def signal_sector_follower(snapshot_map: dict) -> list:
    """
    섹터 대장주가 오르면 아직 안 오른 후행주를 선제 포착.
    """
    results = []
    now_str = _now_str()

    for sector, info in SECTOR_LEADERS.items():
        leader_code = info['leader']
        leader_name = info['leader_name']
        followers   = info['followers']

        leader_row = snapshot_map.get(leader_code, {})
        leader_chg = float(leader_row.get('change_pct', 0))

        if leader_chg < SECTOR_LEAD_PCT:
            continue

        log_debug(f"  ③ 섹터 [{sector}] 대장주 {leader_name} +{leader_chg:.1f}% 상승 → 후행주 탐색")

        for fcode in followers:
            frow   = snapshot_map.get(fcode, {})
            fchg   = float(frow.get('change_pct', 0))
            fclose = float(frow.get('close', 0))
            famount = float(frow.get('amount', 0)) / 1e8

            if fclose < MIN_PRICE or famount < MIN_AMOUNT_PREV:
                continue

            # 아직 덜 오른 후행주 (대장주보다 2%p 이상 낮음)
            lag = leader_chg - fchg
            if lag < 2.0:
                continue

            results.append({
                'code':    fcode,
                'name':    str(frow.get('name', fcode)),
                'close':   int(fclose),
                'change':  round(fchg, 1),
                'amount':  round(famount, 1),
                'signal_type': f'③ 섹터후행({sector})',
                'signal_detail': (
                    f"대장주 {leader_name} +{leader_chg:.1f}% | "
                    f"현재 +{fchg:.1f}% (격차 {lag:.1f}%p)"
                ),
                'score': min(lag * 5 + leader_chg * 3, 50),
            })
            log_debug(f"    └→ [{frow.get('name', fcode)}] +{fchg:.1f}% (격차 {lag:.1f}%p)")

    return results


# =============================================================
# 🎯 선제 신호 ④ — 시간외 이상 징후
# =============================================================

def signal_premarket(snapshot: pd.DataFrame) -> list:
    """
    장 시작 직후 (09:00~09:15) 첫 봉에서 이미 시간외 대비 갭 확인.
    - 시가 > 전일 종가 × (1 + PRE_MARKET_PCT/100)
    - 아직 등락률이 낮으면 = 이제 막 올라가기 시작
    """
    results = []
    now = datetime.now(KST)

    # 09:00~09:30 구간에서만 의미 있음
    if not (9 <= now.hour < 10):
        return results

    for _, row in snapshot.iterrows():
        code   = str(row.get('code', ''))
        close  = float(row.get('close', 0))
        open_p = float(row.get('open', 0))
        change = float(row.get('change_pct', 0))
        amount = float(row.get('amount', 0)) / 1e8

        if close < MIN_PRICE or amount < MIN_AMOUNT_PREV:
            continue

        # 갭 상승 시작했지만 아직 많이 안 오른 상태
        if open_p <= 0:
            continue
        gap_pct = (open_p / close - 1) * 100  # 시가 갭 (근사: 전일 종가 대신 현재가 활용)

        # 시가가 높게 시작했고 (갭업) 아직 등락률이 PRE_MARKET_PCT 미만
        if open_p > close * (1 + PRE_MARKET_PCT / 100) and 0 <= change < PRE_MARKET_PCT:
            results.append({
                'code':    code,
                'name':    str(row.get('name', code)),
                'close':   int(close),
                'change':  round(change, 1),
                'amount':  round(amount, 1),
                'signal_type': '④ 갭업출발',
                'signal_detail': (
                    f"시가 갭업 {gap_pct:.1f}% | 아직 +{change:.1f}%만 반영"
                ),
                'score': min(gap_pct * 8, 40),
            })
            log_debug(f"  ④ [{row.get('name', code)}] 갭업 {gap_pct:.1f}% 출발")

    return results


# =============================================================
# 🎯 선제 신호 ⑤ — 전일 종가 세력 매집 징후
# =============================================================

def signal_prev_accumulation(top_codes: list, snapshot_map: dict) -> list:
    """
    전날 종가 기준으로 세력 매집 흔적 5가지를 복합 판단.

    [5가지 체크 항목]
    A. 종가 > 시가 (양봉 마감) — 세력이 장 내내 받쳐줌
    B. 거래량 > 20일 평균 × 1.5 — 평소보다 거래 활발
    C. OBV 5일 연속 상승 — 거래량이 꾸준히 매수 우위
    D. 저가 > 전일 저가 (고저점 상승) — 하락 없이 올라오는 구조
    E. 종가가 당일 고가의 90% 이상 — 윗꼬리 없이 강하게 마감

    → 5개 중 4개 이상 충족 = 강한 매집 징후
    → 3개 충족 = 중간 수준 매집 징후
    """
    results = []
    today = _today()
    start = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')

    def _check_prev_acc(code):
        try:
            row    = snapshot_map.get(code, {})
            close  = float(row.get('close', 0))
            change = float(row.get('change_pct', 0))
            amount = float(row.get('amount', 0)) / 1e8

            if close < MIN_PRICE or amount < MIN_AMOUNT_PREV:
                return None

            # 이미 크게 오르고 있는 종목은 제외 (선제가 아님)
            if change >= 3:
                return None

            # 일봉 데이터 조회
            df = fdr.DataReader(code, start=start)
            if df is None or len(df) < 22:
                return None

            # 전일 봉 데이터
            prev     = df.iloc[-2]    # 어제
            prev2    = df.iloc[-3]    # 그저께
            vol_ma20 = df['Volume'].tail(20).mean()

            p_open   = float(prev['Open'])
            p_high   = float(prev['High'])
            p_low    = float(prev['Low'])
            p_close  = float(prev['Close'])
            p_vol    = float(prev['Volume'])

            # ─── 5가지 매집 조건
            cond_a = p_close > p_open                                      # A. 양봉 마감
            cond_b = p_vol   > vol_ma20 * 1.5                              # B. 거래량 1.5배+
            cond_d = p_low   > float(prev2['Low'])                         # D. 고저점 상승

            # E. 종가가 고가 90% 이상 (윗꼬리 작음)
            cond_e = p_high  > 0 and p_close >= p_high * 0.90

            # C. OBV 5일 연속 상승 방향 (종가 기준 누적 방향)
            obv = (df['Close'].diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
                   * df['Volume']).cumsum()
            obv_5d_rising = all(obv.iloc[-i-1] > obv.iloc[-i-2] for i in range(5))
            cond_c = obv_5d_rising

            score_items = [cond_a, cond_b, cond_c, cond_d, cond_e]
            score_count = sum(score_items)
            labels = ['양봉마감', '거래량1.5배', 'OBV5일상승', '고저점상승', '윗꼬리없음']
            passed = [l for l, c in zip(labels, score_items) if c]

            if score_count < 3:
                return None

            # 이격도 체크 — 너무 올라있으면 제외
            ma20 = float(df['Close'].rolling(20).mean().iloc[-1])
            disparity = p_close / ma20 * 100 if ma20 > 0 else 100
            if disparity > 115:
                return None

            grade = '🏆A급' if score_count == 5 else ('✅B급' if score_count == 4 else '📋C급')

            return {
                'code':    code,
                'name':    str(row.get('name', code)),
                'close':   int(close),
                'change':  round(change, 1),
                'amount':  round(amount, 1),
                'signal_type': '⑤ 전일매집징후',
                'signal_detail': (
                    f"{grade} {score_count}/5개 충족 | {' + '.join(passed)} | "
                    f"이격도:{disparity:.0f} | 전일거래:{p_vol/vol_ma20:.1f}배"
                ),
                'score': score_count * 12 + (5 if disparity < 105 else 0),
            }
        except Exception as e:
            log_debug(f"⑤ {code} 실패: {e}")
            return None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for res in ex.map(_check_prev_acc, top_codes[:250]):
            if res:
                results.append(res)
                log_debug(f"  ⑤ [{res['name']}] {res['signal_detail'][:60]}")

    # 점수 높은 순 (A급 우선)
    results.sort(key=lambda x: x['score'], reverse=True)
    return results



# =============================================================
# 📡 텔레그램
# =============================================================

def send_telegram(msg: str):
    if not TELEGRAM_TOKEN:
        log_info(f"[텔레그램 미설정]\n{msg[:200]}")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    for chat_id in CHAT_ID_LIST:
        chat_id = chat_id.strip()
        if not chat_id:
            continue
        try:
            requests.post(url, data={'chat_id': chat_id, 'text': msg[:4000], 'parse_mode': 'HTML'}, timeout=5)
        except Exception as e:
            log_error(f"텔레그램 실패: {e}")


def format_presurge_message(hit: dict, scan_time: str) -> str:
    signal_emoji = {
        '① 거래량누적이상': '📦',
        '② BB압축응축':     '🔋',
        '③ 섹터후행':       '🔗',
        '④ 갭업출발':       '🚀',
        '⑤ 전일매집징후':   '🐋',
    }
    # signal_type에서 이모지 찾기
    emoji = '🎯'
    for k, v in signal_emoji.items():
        if k[:2] in hit['signal_type']:
            emoji = v
            break

    return (
        f"{emoji} <b>[선제포착]</b> {hit['name']}({hit['code']})\n"
        f"📌 <b>{hit['signal_type']}</b>\n"
        f"💡 {hit['signal_detail']}\n"
        f"💰 현재가: <b>{hit['close']:,}원</b> ({hit['change']:+.1f}%) | "
        f"거래대금 {hit['amount']:.0f}억\n"
        f"⏰ {scan_time} 포착 | 점수 {hit['score']:.0f}"
    )


# =============================================================
# 🚀 메인 스캔
# =============================================================

def run_presurge_scan() -> list:
    now_kst   = datetime.now(KST)
    scan_time = now_kst.strftime('%H:%M')

    market_open  = now_kst.replace(hour=9,  minute=0)
    market_close = now_kst.replace(hour=15, minute=30)
    if not (market_open <= now_kst <= market_close):
        log_info(f"⏸️ 장외 시간 ({scan_time})")
        return []

    log_info(f"\n{'='*55}")
    log_info(f"🎯 선제 급등 스캔: {scan_time}")
    log_info(f"{'='*55}")

    # 시세 스냅샷
    snapshot = _get_snapshot()
    if snapshot.empty:
        log_error("⚠️ 시세 수집 실패")
        return []

    # 기본 필터
    if 'close'  in snapshot.columns: snapshot = snapshot[snapshot['close']  >= MIN_PRICE]
    if 'amount' in snapshot.columns: snapshot = snapshot[snapshot['amount'] >= MIN_AMOUNT_PREV * 1e8]

    # snapshot_map (code → row dict) — 빠른 조회용
    snapshot_map = {}
    if 'code' in snapshot.columns:
        for _, row in snapshot.iterrows():
            snapshot_map[str(row['code'])] = row.to_dict()

    top_codes = snapshot.nlargest(TOP_N, 'amount')['code'].tolist() \
                if 'amount' in snapshot.columns else list(snapshot_map.keys())[:TOP_N]

    log_info(f"📊 대상 종목: {len(top_codes)}개")

    # 4가지 신호 병렬 수집
    all_hits = []

    log_info("  📦 ① 거래량 누적 이상 탐지...")
    hits1 = signal_vol_accumulation(snapshot)
    all_hits.extend(hits1)
    log_info(f"     → {len(hits1)}개")

    log_info("  🔋 ② BB 에너지 압축 탐지...")
    hits2 = signal_bb_compression(top_codes, snapshot_map)
    all_hits.extend(hits2)
    log_info(f"     → {len(hits2)}개")

    log_info("  🔗 ③ 섹터 선행주 동반 탐지...")
    hits3 = signal_sector_follower(snapshot_map)
    all_hits.extend(hits3)
    log_info(f"     → {len(hits3)}개")

    log_info("  🚀 ④ 갭업 출발 탐지...")
    hits4 = signal_premarket(snapshot)
    all_hits.extend(hits4)
    log_info(f"     → {len(hits4)}개")

    log_info("  🐋 ⑤ 전일 세력매집 징후 탐지...")
    hits5 = signal_prev_accumulation(top_codes, snapshot_map)
    all_hits.extend(hits5)
    log_info(f"     → {len(hits5)}개")

    if not all_hits:
        log_info("✅ 선제 신호 없음")
        return []

    # 점수 정렬 + 중복 제거 (같은 종목이 여러 신호에 걸리면 합산)
    merged = {}
    for h in all_hits:
        code = h['code']
        if code not in merged:
            merged[code] = h.copy()
            merged[code]['signals'] = [h['signal_type']]
        else:
            merged[code]['score']  += h['score'] * 0.5  # 중복 신호 보너스
            merged[code]['signals'].append(h['signal_type'])
            merged[code]['signal_detail'] += f" | {h['signal_type']}"

    hits_final = sorted(merged.values(), key=lambda x: x['score'], reverse=True)
    log_info(f"\n🎯 최종 선제 후보: {len(hits_final)}개")

    # 알림 전송
    alerted = _load_alerted()
    new_alerts = []

    for hit in hits_final[:8]:
        code = hit['code']
        if _is_alerted(code, alerted):
            continue

        # 중복 신호가 있으면 메시지에 표시
        if len(hit.get('signals', [])) > 1:
            hit['signal_type'] = ' + '.join(hit['signals'])

        msg = format_presurge_message(hit, scan_time)
        send_telegram(msg)
        log_info(f"  📨 [{hit['name']}] {hit['signal_type']} (점수:{hit['score']:.0f})")
        _mark_alerted(code, alerted)
        new_alerts.append(hit)

    _save_alerted(alerted)

    if not new_alerts:
        log_info(f"  (신규 알림 없음, 중복 {len(hits_final)}개)")

    return new_alerts


# =============================================================
# 🔄 자동 반복
# =============================================================

def run_loop(interval: int = 10):
    log_info(f"🔄 선제 스캐너 시작: {interval}분 간격")

    send_telegram(
        f"🎯 <b>선제 급등 스캐너 시작</b>\n"
        f"⏱ {interval}분 간격 | 09:00~15:30\n"
        f"신호: 거래량누적 | BB압축 | 섹터후행 | 갭업출발 | 전일매집"
    )

    while True:
        try:
            if datetime.now(KST).weekday() < 5:
                run_presurge_scan()

            next_run = datetime.now(KST) + timedelta(minutes=interval)
            wait = (next_run - datetime.now(KST)).total_seconds()
            log_info(f"⏳ 다음 스캔: {next_run.strftime('%H:%M')}")
            time.sleep(max(0, wait))

        except KeyboardInterrupt:
            log_info("\n🛑 종료")
            send_telegram("🛑 선제 스캐너 종료")
            break
        except Exception as e:
            log_error(f"⚠️ 오류: {e}")
            time.sleep(60)


# =============================================================
# 🚀 엔트리포인트
# =============================================================

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--loop',     action='store_true')
    parser.add_argument('--interval', default=10, type=int)
    parser.add_argument('--test',     action='store_true', help='장외 강제 실행')
    args = parser.parse_args()

    if args.test:
        log_info("⚠️ 테스트 모드")
        # 장 시간 체크 우회용 패치
        import unittest.mock as mock
        now_mock = datetime.now(KST).replace(hour=10, minute=0)

    if args.loop:
        run_loop(args.interval)
    else:
        hits = run_presurge_scan()
        if not hits:
            log_info("✅ 선제 신호 없음")
        sys.exit(0)
