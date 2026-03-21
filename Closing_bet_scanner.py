# =============================================================
# 🕯️ closing_bet_scanner.py — 종가배팅 타점 스캐너
# =============================================================
# 실행 시간: 15:00 ~ 15:20 (이 시간대에만 의미 있음)
#
# 왜 이 시간인가:
#   15:00 이전 → 아직 장 중, 종가 확정 안 됨
#   15:20 이후 → 동시호가 진입 타이밍 지남
#   15:00~15:20 → 오늘 종가 흐름 확인 + 진입 결정 가능
#
# 종가배팅 조건 6가지:
#   ① 전고점(20일) 대비 85~100% 구간
#   ② 윗꼬리 비율 20% 이하 (강봉 마감 중)
#   ③ 거래량 VMA20 × 2배 이상 폭발
#   ④ 양봉 마감 (현재가 ≥ 시가)
#   ⑤ 이격도 98~112 (MA20 적정 위치)
#   ⑥ MA20 위 마감
#
# 실행:
#   python closing_bet_scanner.py          # 1회 실행
#   python closing_bet_scanner.py --force  # 시간 무관 강제 실행 (테스트)
# =============================================================

import os
import sys
import argparse
import pytz
import requests
import pandas as pd
import numpy as np
import FinanceDataReader as fdr
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── main7.py에서 필요한 것만 import
from main7 import (
    get_indicators,
    _calc_upper_wick_ratio,
    load_krx_listing_safe,
    send_telegram_photo,
    send_telegram_chunks,
    TELEGRAM_TOKEN,
    CHAT_ID_LIST,
    ANTHROPIC_API_KEY,
    OPENAI_API_KEY,
    GROQ_API_KEY,
    TODAY_STR,
    KST,
)

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
MIN_PRICE        = 5_000
MIN_AMOUNT       = 5_000_000_000    # 거래대금 50억 이상
MIN_MARCAP       = 50_000_000_000   # 시총 500억 이상
TOP_N            = 400              # 거래대금 상위 N종목
MAX_WORKERS      = 20

# 종가배팅 조건 임계값
NEAR_HIGH20_MIN  = 85.0   # 전고점 대비 최소 85%
NEAR_HIGH20_MAX  = 100.0  # 전고점 대비 최대 100% (신고가 직전)
UPPER_WICK_MAX   = 0.20   # 윗꼬리 비율 최대 20%
VOL_MULT         = 2.0    # 거래량 VMA20 대비 배율
DISPARITY_MIN    = 98.0   # 이격도 최소
DISPARITY_MAX    = 112.0  # 이격도 최대

# 실행 가능 시간대
SCAN_START_HOUR  = 14
SCAN_START_MIN   = 50    # 14:50부터 (5분 여유)
SCAN_END_HOUR    = 15
SCAN_END_MIN     = 25    # 15:25까지

ALERTED_FILE     = '/tmp/closing_bet_alerted.json'


# =============================================================
# 🕐 시간 체크
# =============================================================

def _is_closing_time(force: bool = False) -> bool:
    """종가배팅 유효 시간 (14:50~15:25)"""
    if force:
        return True
    now = datetime.now(KST)
    if now.weekday() >= 5:
        return False
    t = now.hour * 60 + now.minute
    start = SCAN_START_HOUR * 60 + SCAN_START_MIN
    end   = SCAN_END_HOUR   * 60 + SCAN_END_MIN
    return start <= t <= end


def _time_to_close() -> int:
    """마감까지 남은 분"""
    now   = datetime.now(KST)
    close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return max(0, int((close - now).total_seconds() / 60))


# =============================================================
# 📊 종가배팅 조건 체크
# =============================================================

def _check_closing_bet(code: str, name: str) -> dict | None:
    """
    단일 종목에 대해 종가배팅 6가지 조건 체크.
    조건 4개 이상 충족 시 후보로 반환.
    """
    try:
        start_date = (datetime.now() - timedelta(days=300)).strftime('%Y-%m-%d')
        df = fdr.DataReader(code, start=start_date)

        if df is None or len(df) < 60:
            return None

        # 지표 계산
        df = get_indicators(df)
        if df is None or df.empty:
            return None

        # 전고점 컬럼 (get_indicators에 추가됨)
        if 'NearHigh20_Pct' not in df.columns:
            df['High20']         = df['High'].rolling(20).max()
            df['NearHigh20_Pct'] = (df['Close'] / df['High20'] * 100)

        row    = df.iloc[-1]
        close  = float(row['Close'])
        open_p = float(row['Open'])
        high   = float(row['High'])
        vol    = float(row['Volume'])
        vma20  = float(row.get('VMA20', row.get('Vol_Avg', 0)) or 0)
        ma20   = float(row.get('MA20', 0) or 0)
        disp   = float(row.get('Disparity', 100) or 100)
        near20 = float(row.get('NearHigh20_Pct', 0) or 0)
        amount = close * vol

        # 기본 필터
        if close < MIN_PRICE or amount < MIN_AMOUNT:
            return None

        # ── 6가지 조건 체크
        upper_wick = _calc_upper_wick_ratio(row)

        cond = {
            '①전고점85~100%': NEAR_HIGH20_MIN <= near20 <= NEAR_HIGH20_MAX,
            '②윗꼬리20%이하':  upper_wick <= UPPER_WICK_MAX,
            '③거래량2배폭발':  vma20 > 0 and vol >= vma20 * VOL_MULT,
            '④양봉마감':       close >= open_p,
            '⑤이격도98~112':   DISPARITY_MIN <= disp <= DISPARITY_MAX,
            '⑥MA20위마감':     ma20 > 0 and close >= ma20,
        }

        passed = [k for k, v in cond.items() if v]
        score  = len(passed)

        if score < 4:
            return None

        # 등급 부여
        if score == 6:
            grade = '🏆완전체'
        elif score == 5:
            grade = '✅A급'
        else:
            grade = '📋B급'

        # 추가 정보
        vol_ratio  = round(vol / vma20, 1) if vma20 > 0 else 0
        wick_pct   = round(upper_wick * 100, 1)
        amount_b   = round(amount / 1e8, 1)

        # 목표가/손절 (ATR 기반)
        atr = float(row.get('ATR', 0) or 0)
        target1 = round(close + atr * 2)    if atr > 0 else round(close * 1.05)
        stoploss = round(close - atr * 1.5) if atr > 0 else round(close * 0.97)
        rr = round((target1 - close) / (close - stoploss), 1) if close > stoploss else 0

        return {
            'code':       code,
            'name':       name,
            'close':      int(close),
            'open':       int(open_p),
            'high':       int(high),
            'vol_ratio':  vol_ratio,
            'wick_pct':   wick_pct,
            'near20':     round(near20, 1),
            'disp':       round(disp, 1),
            'amount_b':   amount_b,
            'score':      score,
            'grade':      grade,
            'passed':     passed,
            'target1':    target1,
            'stoploss':   stoploss,
            'rr':         rr,
            'atr':        int(atr),
        }

    except Exception as e:
        log_debug(f"  [{name}] 오류: {e}")
        return None


# =============================================================
# 📱 텔레그램 포맷
# =============================================================

def _format_hit(hit: dict, rank: int, mins_left: int) -> str:
    passed_str = ' '.join(hit['passed'])
    failed = [k for k in ['①전고점85~100%','②윗꼬리20%이하','③거래량2배폭발',
                           '④양봉마감','⑤이격도98~112','⑥MA20위마감']
              if k not in hit['passed']]
    failed_str = (' | ❌' + ' '.join(failed)) if failed else ''

    return (
        f"{'─'*28}\n"
        f"🕯️ {hit['grade']}  [{hit['name']}({hit['code']})]  {hit['close']:,}원\n"
        f"✅ {passed_str}{failed_str}\n"
        f"📊 거래량:{hit['vol_ratio']}배 | 윗꼬리:{hit['wick_pct']}% | "
        f"전고점:{hit['near20']}% | 이격:{hit['disp']}\n"
        f"💰 거래대금:{hit['amount_b']}억 | ATR:{hit['atr']:,}원\n"
        f"📌 목표:{hit['target1']:,} | 손절:{hit['stoploss']:,} (RR {hit['rr']})\n"
        f"⏰ 마감까지 {mins_left}분\n"
    )


def _send_results(hits: list, mins_left: int):
    """결과 텔레그램 전송"""
    if not hits:
        send_telegram_photo(
            f"🕯️ [{TODAY_STR}] 종가배팅 후보 없음\n"
            f"(조건 4개 이상 충족 종목 없음)",
            []
        )
        return

    # 헤더
    header = (
        f"🕯️ <b>종가배팅 후보 {len(hits)}종목</b> ({TODAY_STR})\n"
        f"⏰ 마감까지 {mins_left}분\n"
        f"조건: 전고점85~100% | 윗꼬리↓ | 거래량2배↑ | 양봉 | 이격98~112 | MA20위\n"
    )
    send_telegram_photo(header, [])

    # 종목별 (완전체 우선)
    full  = [h for h in hits if h['score'] == 6]
    a_cls = [h for h in hits if h['score'] == 5]
    b_cls = [h for h in hits if h['score'] == 4]

    current_msg = ''
    MAX_CHAR = 3800

    for hit in full + a_cls + b_cls:
        entry = _format_hit(hit, 0, mins_left)
        if len(current_msg) + len(entry) > MAX_CHAR:
            send_telegram_photo(current_msg, [])
            current_msg = entry
        else:
            current_msg += entry

    if current_msg.strip():
        send_telegram_photo(current_msg, [])

    # AI 코멘트 (완전체 있을 때만)
    if full and (ANTHROPIC_API_KEY or OPENAI_API_KEY):
        _send_ai_comment(full[:5], mins_left)


def _send_ai_comment(hits: list, mins_left: int):
    """완전체 종목에 대한 AI 간단 코멘트"""
    try:
        lines = '\n'.join([
            f"- {h['name']}({h['code']}): 현재가={h['close']:,}원 | "
            f"거래량={h['vol_ratio']}배 | 전고점={h['near20']}% | "
            f"이격={h['disp']} | 윗꼬리={h['wick_pct']}% | "
            f"목표={h['target1']:,} 손절={h['stoploss']:,}"
            for h in hits
        ])

        system_msg = (
            "너는 단테 역매공파 매매법 전문가야. "
            "종가배팅 타점 분석을 해줘. 각 종목당 2~3문장으로 간결하게."
        )
        user_msg = (
            f"오늘 15시 종가배팅 후보 종목들이야. "
            f"마감까지 {mins_left}분 남았어. "
            f"각 종목별로 진입 여부와 이유를 알려줘:\n\n{lines}"
        )

        comment = ''

        # Claude 우선
        if ANTHROPIC_API_KEY:
            try:
                res = requests.post(
                    'https://api.anthropic.com/v1/messages',
                    headers={
                        'Content-Type': 'application/json',
                        'x-api-key': ANTHROPIC_API_KEY,
                        'anthropic-version': '2023-06-01',
                    },
                    json={
                        'model': 'claude-sonnet-4-20250514',
                        'max_tokens': 800,
                        'system': system_msg,
                        'messages': [{'role': 'user', 'content': user_msg}],
                    },
                    timeout=30
                )
                data = res.json()
                if 'content' in data and data['content']:
                    comment = data['content'][0].get('text', '').strip()
                    log_info("✅ Claude 코멘트 완료")
            except Exception as e:
                log_error(f"⚠️ Claude 실패: {e}")

        # OpenAI 폴백
        if not comment and OPENAI_API_KEY:
            try:
                from openai import OpenAI as _OAI
                client = _OAI(api_key=OPENAI_API_KEY)
                res = client.chat.completions.create(
                    model='gpt-4o-mini',
                    messages=[
                        {'role': 'system', 'content': system_msg},
                        {'role': 'user',   'content': user_msg},
                    ],
                    max_tokens=800,
                )
                comment = res.choices[0].message.content.strip()
                log_info("✅ GPT 코멘트 완료")
            except Exception as e:
                log_error(f"⚠️ GPT 실패: {e}")

        if comment:
            send_telegram_chunks(
                f"🤖 종가배팅 AI 코멘트\n\n{comment}",
                max_len=3500
            )

    except Exception as e:
        log_error(f"⚠️ AI 코멘트 실패: {e}")


# =============================================================
# 🚀 메인 스캔
# =============================================================

def run_closing_bet_scan(force: bool = False) -> list:
    """종가배팅 스캔 실행"""
    now      = datetime.now(KST)
    now_str  = now.strftime('%H:%M')
    mins_left = _time_to_close()

    if not _is_closing_time(force):
        log_info(
            f"⏸️ 종가배팅 스캐너는 14:50~15:25에만 실행 (현재 {now_str})\n"
            f"   테스트: python closing_bet_scanner.py --force"
        )
        return []

    log_info(f"\n{'='*55}")
    log_info(f"🕯️ 종가배팅 스캔 시작: {now_str} (마감 {mins_left}분 전)")
    log_info(f"{'='*55}")

    # 종목 리스트
    df_krx = load_krx_listing_safe()
    if df_krx is None or df_krx.empty:
        log_error("⚠️ 종목 리스트 로드 실패")
        return []

    # 컬럼 정규화
    col_map = {}
    for c in df_krx.columns:
        cs = str(c).strip()
        if   cs in ('Code','code','티커','종목코드'): col_map[c] = 'Code'
        elif cs in ('Name','name','종목명'):          col_map[c] = 'Name'
        elif cs in ('Amount','amount','거래대금'):    col_map[c] = 'Amount'
        elif cs in ('Market','market'):               col_map[c] = 'Market'
    df_krx = df_krx.rename(columns=col_map)

    # 필터
    if 'Market' in df_krx.columns:
        df_krx = df_krx[df_krx['Market'].isin(['KOSPI','KOSDAQ','코스피','코스닥','유가'])]
    if 'Name' in df_krx.columns:
        df_krx = df_krx[~df_krx['Name'].astype(str).str.contains(
            'ETF|ETN|스팩|제[0-9]+호|우$|우A|우B', na=False
        )]

    # 거래대금 상위 TOP_N
    if 'Amount' in df_krx.columns:
        df_krx = df_krx.nlargest(TOP_N, 'Amount')

    codes = df_krx['Code'].tolist() if 'Code' in df_krx.columns else []
    names = df_krx['Name'].tolist() if 'Name' in df_krx.columns else codes

    log_info(f"📊 대상: {len(codes)}개 종목")

    # 병렬 분석
    hits = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {
            ex.submit(_check_closing_bet, code, name): (code, name)
            for code, name in zip(codes, names)
        }
        done = 0
        for future in as_completed(futures, timeout=300):
            done += 1
            try:
                result = future.result(timeout=20)
                if result:
                    hits.append(result)
            except Exception:
                pass
            if done % 100 == 0:
                log_info(f"  진행: {done}/{len(codes)} | 후보: {len(hits)}개")

    # 점수 → 거래량 순 정렬
    hits.sort(key=lambda x: (x['score'], x['vol_ratio']), reverse=True)

    log_info(f"\n🕯️ 종가배팅 후보: {len(hits)}개")
    log_info(f"  🏆완전체: {sum(1 for h in hits if h['score']==6)}개")
    log_info(f"  ✅A급:    {sum(1 for h in hits if h['score']==5)}개")
    log_info(f"  📋B급:    {sum(1 for h in hits if h['score']==4)}개")

    # 텔레그램 전송
    _send_results(hits[:10], mins_left)

    return hits


# =============================================================
# 🚀 엔트리포인트
# =============================================================

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='종가배팅 타점 스캐너')
    parser.add_argument('--force', action='store_true', help='시간 무관 강제 실행')
    args = parser.parse_args()

    hits = run_closing_bet_scan(force=args.force)
    if not hits:
        log_info("✅ 종가배팅 후보 없음")
    sys.exit(0)
