# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# calculate_combination_score_v2.py
# 조합 테이블 기반 점수 산출 + 스타일 가중치 + 수익률 학습 점수
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
import os
import json
import pandas as pd
from datetime import datetime

# ──────────────────────────────────────────────────
# 0. 수익률 실적 파일 경로
# ──────────────────────────────────────────────────
PERF_FILE = "combo_performance.json"   # 패턴별 수익률 누적 기록
SCORE_OVERRIDE_FILE = "combo_score_override.json"  # 학습된 보정 점수


def _load_json(path, default):
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return default


def _save_json(path, data):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ──────────────────────────────────────────────────
# 1. 조합 정의 테이블 (COMBO_TABLE)
#    cond     : 충족 조건 (lambda)
#    score    : 기본 점수
#    score_fn : 동적 점수 (선택)
#    tag_fn   : 동적 태그 (선택)
# ──────────────────────────────────────────────────
COMBO_TABLE = [
    # ─── GOD+ ───────────────────────────────────────
    {
        'grade': 'GOD+', 'score': 10001, 'type': '🌌',
        'combination': '🌌🔺💍독사삼각돌반지',
        'tags': ['🔺꼭지임박', '🐍독사대가리', '💍200일돌파', '🍉수급폭발', '🚀역대급시그널'],
        'cond': lambda e: (
            e.get('triangle_signal') and
            isinstance(e.get('triangle_apex'), (int, float)) and
            0 <= e['triangle_apex'] <= 3 and
            e.get('viper_hook') and
            e.get('watermelon_signal') and
            e.get('dolbanzi')
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
        'grade': 'SSS', 'score': 500, 'type': '👑',
        'combination': '👑💍수박돌반지',
        'tags': ['🍉수박전환', '💍돌반지완성', '🔥최종병기', '🚀대시세시작'],
        'cond': lambda e: e.get('watermelon_signal') and e.get('dolbanzi'),
        'score_fn': lambda e: 500 if e.get('dolbanzi_Count', 0) == 1 else 450,
        'tag_fn':   lambda e: (['🥇최초의반지'] if e.get('dolbanzi_Count', 0) == 1
                               else [f"💍{e.get('dolbanzi_Count', 0)}회차반지"]),
    },
    {
        'grade': 'SSS', 'score': 480, 'type': '👑',
        'combination': '🔺💍삼각꼭지돌반지',
        'tags': ['🔺꼭지임박', '💍200일돌파', '💥에너지응축폭발'],
        'cond': lambda e: (
            e.get('triangle_signal') and
            isinstance(e.get('triangle_apex'), (int, float)) and
            0 <= e['triangle_apex'] <= 5 and
            e.get('dolbanzi')
        ),
        'tag_fn': lambda e: [f"💍{e.get('dolbanzi_Count', 0)}회차반지"],
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
        'cond': lambda e: (
            e.get('viper_hook') and e.get('watermelon_signal') and
            e.get('obv_bullish') and e.get('Real_Viper_Hook')
        ),
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
        'tag_fn':   lambda e: (
            ['🔥GoldenEntry'] if e.get('dolbanzi_Count', 0) == 1 else
            ['📈추세지속']     if e.get('dolbanzi_Count', 0) == 2 else
            ['⚠️과열주의']
        ),
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
    # ─── S ──────────────────────────────────────────
    {
        'grade': 'S', 'score': 350, 'type': '🗡',
        'combination': '💎전설조합',
        'tags': ['🍉수박전환', '💎폭발직전', '📍바닥권', '🤫조용한매집완전'],
        'cond': lambda e: (
            e.get('watermelon_signal') and e.get('explosion_ready') and
            e.get('bottom_area') and e.get('silent_perfect')
        ),
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


# ──────────────────────────────────────────────────
# 2. 수익률 기록 함수
#    analyze_final 루프에서 hits.append() 직후 호출
# ──────────────────────────────────────────────────
def record_combo_performance(combination: str, max_return: float,
                              min_return: float, days_to_max: int,
                              style: str = 'NONE'):
    """
    패턴 조합의 실제 수익률을 누적 기록
    combination : result['combination'] 값 (예: '🌌🍉💍독사품은수박돌반지')
    max_return  : 최고수익률_raw
    min_return  : 최저수익률_raw
    days_to_max : 최고점 도달 소요일
    style       : SWING / SCALP / NONE
    """
    perf = _load_json(PERF_FILE, {})
    key  = combination

    if key not in perf:
        perf[key] = {
            'combination': combination,
            'style':       style,
            'count':       0,
            'win':         0,       # 수익률 > 0 횟수
            'total_max_r': 0.0,     # 최고수익률 누적
            'total_min_r': 0.0,     # 최저수익률 누적
            'total_days':  0,       # 소요일 누적
            'history':     [],      # 최근 20개 기록
        }

    rec  = perf[key]
    rec['count']       += 1
    rec['total_max_r'] += max_return
    rec['total_min_r'] += min_return
    rec['total_days']  += days_to_max
    if max_return > 3.0:           # 3% 이상이면 승리로 카운트
        rec['win'] += 1

    # 최근 20개 히스토리만 유지
    rec['history'].append({
        'date':    datetime.now().strftime('%Y-%m-%d'),
        'max_r':   round(max_return, 2),
        'min_r':   round(min_return, 2),
        'days':    days_to_max,
        'style':   style,
    })
    rec['history'] = rec['history'][-20:]

    _save_json(PERF_FILE, perf)


# ──────────────────────────────────────────────────
# 3. 수익률 기반 점수 보정 생성
#    일정 횟수(MIN_SAMPLE) 이상 쌓이면 점수를 자동 조정
# ──────────────────────────────────────────────────
MIN_SAMPLE   = 5     # 최소 샘플 수 (이 이하면 보정 안 함)
MAX_BONUS    = 150   # 최대 보너스 점수
MAX_PENALTY  = 100   # 최대 감점 점수

def rebuild_score_overrides():
    """
    누적된 수익률 데이터로 combo_score_override.json 재생성
    주기적으로 (또는 스캔 시작 전에) 호출
    """
    perf      = _load_json(PERF_FILE, {})
    overrides = {}

    for key, rec in perf.items():
        n = rec['count']
        if n < MIN_SAMPLE:
            continue

        win_rate  = rec['win'] / n                          # 승률 (0~1)
        avg_max_r = rec['total_max_r'] / n                  # 평균 최고수익률
        avg_min_r = rec['total_min_r'] / n                  # 평균 최저수익률 (손실)
        avg_days  = rec['total_days']  / n                  # 평균 소요일

        # 기대값 점수 = 평균 최고수익 * 승률 - 평균 손실 * (1-승률)
        expected  = avg_max_r * win_rate + avg_min_r * (1 - win_rate)

        # 보정 점수 계산
        # expected > 10% 이상이면 보너스, < 0이면 감점
        if expected >= 20:
            bonus = min(MAX_BONUS, int(expected * 5))
        elif expected >= 10:
            bonus = min(MAX_BONUS, int(expected * 3))
        elif expected >= 0:
            bonus = int(expected * 1)
        else:
            bonus = max(-MAX_PENALTY, int(expected * 3))    # 음수 expected → 감점

        overrides[key] = {
            'combination': key,
            'count':       n,
            'win_rate':    round(win_rate * 100, 1),
            'avg_max_r':   round(avg_max_r, 2),
            'avg_min_r':   round(avg_min_r, 2),
            'avg_days':    round(avg_days, 1),
            'expected':    round(expected, 2),
            'bonus':       bonus,
            'updated':     datetime.now().strftime('%Y-%m-%d %H:%M'),
        }

    _save_json(SCORE_OVERRIDE_FILE, overrides)
    print(f"✅ 점수 보정 재계산 완료: {len(overrides)}개 조합 반영")
    return overrides


def _load_score_overrides():
    return _load_json(SCORE_OVERRIDE_FILE, {})


# ──────────────────────────────────────────────────
# 4. 스타일 보너스 적용
# ──────────────────────────────────────────────────
def _apply_style_bonus(best: dict, style: str) -> int:
    score = best['score']
    combo = best['combination']

    if style == 'SWING':
        if any(k in combo for k in ['폭발', '바닥', '매집', '수렴', '삼각', '독사']):
            score += 30
    elif style == 'SCALP':
        if any(k in combo for k in ['수박', '돌파', '거래량', '골파기']):
            score += 30
        if any(k in combo for k in ['바닥', '매집완성']):
            score -= 20

    return score


# ──────────────────────────────────────────────────
# 5. 메인 함수
# ──────────────────────────────────────────────────
def calculate_combination_score(signals: dict) -> dict:
    """
    signals 딕셔너리를 받아 최고 조합 점수를 반환
    signals에 'style' 키 포함 시 가중치 자동 적용
    """
    effective = signals.copy()
    if effective.get('silent_perfect'):
        effective['silent_strong'] = True

    style     = effective.get('style', 'NONE')
    overrides = _load_score_overrides()   # 수익률 학습 보정값

    # ── 전체 조합 평가 ────────────────────────────
    matched = []
    for combo in COMBO_TABLE:
        try:
            if not combo['cond'](effective):
                continue
        except Exception:
            continue

        base_score = combo['score_fn'](effective) if 'score_fn' in combo else combo['score']
        extra_tags = combo['tag_fn'](effective)   if 'tag_fn'  in combo else []
        combo_name = combo['combination']

        # 수익률 학습 보정 적용
        learn_bonus = 0
        if combo_name in overrides:
            ov          = overrides[combo_name]
            learn_bonus = ov['bonus']
            extra_tags  = extra_tags + [
                f"📊승률{ov['win_rate']}%",
                f"📈기대수익{ov['expected']:+.1f}%",
            ]

        matched.append({
            'score':       base_score + learn_bonus,
            'base_score':  base_score,
            'learn_bonus': learn_bonus,
            'grade':       combo['grade'],
            'combination': combo_name,
            'tags':        combo['tags'] + extra_tags,
            'type':        combo['type'],
        })

    if matched:
        best          = max(matched, key=lambda x: x['score'])
        best['score'] = _apply_style_bonus(best, style)
        best['style'] = style
        return best

    # ── D급 기본 ─────────────────────────────────
    tags, bonus = [], 0
    if effective.get('obv_rising'):   bonus += 30; tags.append('📊OBV')
    if effective.get('mfi_strong'):   bonus += 20; tags.append('💰MFI')
    if effective.get('volume_surge'): bonus += 10; tags.append('⚡거래량')

    return {
        'score': 100 + bonus, 'base_score': 100, 'learn_bonus': 0,
        'grade': 'D', 'combination': '🔍기본',
        'tags': tags, 'type': None, 'style': style,
    }


# ──────────────────────────────────────────────────
# 6. 수익률 현황 리포트
#    스캔 후 터미널에서 확인용
# ──────────────────────────────────────────────────
def print_combo_report(top_n: int = 15):
    """패턴별 수익률 현황 출력"""
    overrides = _load_score_overrides()
    if not overrides:
        print("⚠️  아직 수익률 데이터 없음. rebuild_score_overrides() 먼저 실행 필요.")
        return

    rows = sorted(overrides.values(), key=lambda x: x['expected'], reverse=True)

    print(f"\n{'='*70}")
    print(f"  📊 패턴 수익률 현황 (상위 {top_n}개)")
    print(f"{'='*70}")
    print(f"  {'조합명':<30} {'횟수':>5} {'승률':>7} {'평균최고':>8} {'기대수익':>8} {'보정점수':>8}")
    print(f"  {'-'*65}")

    for r in rows[:top_n]:
        name = r['combination'][:28]
        print(f"  {name:<30} {r['count']:>5} "
              f"{r['win_rate']:>6.1f}% "
              f"{r['avg_max_r']:>+7.1f}% "
              f"{r['expected']:>+7.1f}% "
              f"{r['bonus']:>+7}")

    print(f"{'='*70}\n")
