# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# combo_storage.py
# GitHub Actions 환경용 수익률 저장 모듈
# JSON 파일을 레포에 자동 커밋해서 실행 간 데이터 유지
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
import os
import json
import subprocess
from datetime import datetime

# ──────────────────────────────────────────────────
# 파일 경로 (레포 루트 기준)
# .gitignore에 추가하면 안 됨 - 레포에 올라가야 유지됨
# ──────────────────────────────────────────────────
PERF_FILE          = "data/combo_performance.json"
SCORE_OVERRIDE_FILE= "data/combo_score_override.json"


def _load_json(path: str, default):
    """파일 없으면 default 반환"""
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return default


def _save_json(path: str, data):
    """data/ 폴더 없으면 자동 생성 후 저장"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _git_commit_and_push(message: str):
    """
    GitHub Actions 안에서 변경된 JSON을 레포에 자동 커밋/푸시
    workflow yml에서 아래 권한 설정 필요:
      permissions:
        contents: write
    """
    try:
        # Actions 봇 계정으로 커밋
        subprocess.run(['git', 'config', 'user.name',  'github-actions[bot]'], check=True)
        subprocess.run(['git', 'config', 'user.email', 'github-actions[bot]@users.noreply.github.com'], check=True)
        subprocess.run(['git', 'add', PERF_FILE, SCORE_OVERRIDE_FILE], check=True)

        # 변경사항 없으면 커밋 스킵 (오류 방지)
        result = subprocess.run(['git', 'diff', '--cached', '--quiet'])
        if result.returncode == 0:
            print("ℹ️  변경사항 없음 - 커밋 스킵")
            return

        subprocess.run(['git', 'commit', '-m', message], check=True)
        subprocess.run(['git', 'push'],                  check=True)
        print(f"✅ 수익률 데이터 레포 저장 완료: {message}")

    except subprocess.CalledProcessError as e:
        print(f"⚠️  git 커밋 실패 (데이터는 로컬 보존): {e}")


# ──────────────────────────────────────────────────
# 수익률 기록
# ──────────────────────────────────────────────────
def record_combo_performance(combination: str, max_return: float,
                              min_return: float, days_to_max: int,
                              style: str = 'NONE',
                              auto_push: bool = False):
    """
    패턴 조합의 실제 수익률 누적 기록
    auto_push=True 이면 매 기록마다 즉시 커밋 (느림)
    False 이면 스캔 끝난 후 flush_and_push() 로 한 번에 커밋 (권장)
    """
    perf = _load_json(PERF_FILE, {})
    key  = combination

    if key not in perf:
        perf[key] = {
            'combination': combination,
            'style':       style,
            'count':       0,
            'win':         0,
            'total_max_r': 0.0,
            'total_min_r': 0.0,
            'total_days':  0,
            'history':     [],
        }

    rec = perf[key]
    rec['count']       += 1
    rec['total_max_r'] += max_return
    rec['total_min_r'] += min_return
    rec['total_days']  += days_to_max
    if max_return > 3.0:
        rec['win'] += 1

    rec['history'].append({
        'date':  datetime.now().strftime('%Y-%m-%d'),
        'max_r': round(max_return, 2),
        'min_r': round(min_return, 2),
        'days':  days_to_max,
        'style': style,
    })
    rec['history'] = rec['history'][-20:]

    _save_json(PERF_FILE, perf)

    if auto_push:
        _git_commit_and_push(f"📊 combo perf update: {combination[:20]}")


# ──────────────────────────────────────────────────
# 스캔 종료 후 한 번에 커밋 (권장)
# ──────────────────────────────────────────────────
def flush_and_push():
    """
    스캔이 모두 끝난 뒤 1회만 커밋/푸시
    analyze_final 루프가 끝난 직후 main에서 호출
    """
    today = datetime.now().strftime('%Y-%m-%d')
    _git_commit_and_push(f"📊 combo performance update {today}")


# ──────────────────────────────────────────────────
# 점수 보정 재계산
# ──────────────────────────────────────────────────
MIN_SAMPLE  = 5
MAX_BONUS   = 150
MAX_PENALTY = 100

def rebuild_score_overrides():
    perf      = _load_json(PERF_FILE, {})
    overrides = {}

    for key, rec in perf.items():
        n = rec['count']
        if n < MIN_SAMPLE:
            continue

        win_rate  = rec['win'] / n
        avg_max_r = rec['total_max_r'] / n
        avg_min_r = rec['total_min_r'] / n
        avg_days  = rec['total_days']  / n
        expected  = avg_max_r * win_rate + avg_min_r * (1 - win_rate)

        if expected >= 20:
            bonus = min(MAX_BONUS,   int(expected * 5))
        elif expected >= 10:
            bonus = min(MAX_BONUS,   int(expected * 3))
        elif expected >= 0:
            bonus = int(expected * 1)
        else:
            bonus = max(-MAX_PENALTY, int(expected * 3))

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
    print(f"✅ 점수 보정 재계산 완료: {len(overrides)}개 조합")
    return overrides


def load_score_overrides():
    return _load_json(SCORE_OVERRIDE_FILE, {})


# ──────────────────────────────────────────────────
# 수익률 현황 출력
# ──────────────────────────────────────────────────
def print_combo_report(top_n: int = 15):
    overrides = load_score_overrides()
    if not overrides:
        print("⚠️  아직 수익률 데이터 없음")
        return

    rows = sorted(overrides.values(), key=lambda x: x['expected'], reverse=True)
    print(f"\n{'='*70}")
    print(f"  📊 패턴 수익률 현황 (상위 {top_n}개)")
    print(f"{'='*70}")
    print(f"  {'조합명':<30} {'횟수':>5} {'승률':>7} {'평균최고':>8} {'기대수익':>8} {'보정점수':>8}")
    print(f"  {'-'*65}")
    for r in rows[:top_n]:
        print(f"  {r['combination'][:28]:<30} {r['count']:>5} "
              f"{r['win_rate']:>6.1f}% "
              f"{r['avg_max_r']:>+7.1f}% "
              f"{r['expected']:>+7.1f}% "
              f"{r['bonus']:>+7}")
    print(f"{'='*70}\n")
