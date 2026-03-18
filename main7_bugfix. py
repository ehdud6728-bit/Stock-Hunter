# =============================================================
# 🐛 main7.py Ver 27.1 → 27.2 버그 수정 + 신호 튜닝 패치
# 적용 방법: main7.py에서 해당 줄을 찾아 아래 수정본으로 교체
# =============================================================

# ─────────────────────────────────────────────────────────────
# [BUG-1] bb_squeeze 임계값 오류 → 폭발직전 신호 복구
# 위치: analyze_final() 내부 ~line 2876
# ─────────────────────────────────────────────────────────────

# ❌ 기존 (BB40_Width <= 0.2 는 거의 불가능 → explosion_ready 항상 False)
# bb_squeeze = row['BB40_Width'] <= 0.2

# ✅ 수정
def _calc_bb_squeeze(row) -> bool:
    """
    BB40_Width 기준 응축 판단.
    - 5.0 이하: 강한 응축 (수박지표 폭발 직전)
    - get_indicators() 에서 BB40_Width = (BB40_Upper - BB40_Lower) / Close * 100
      → 보통 5~25 사이, 5 이하면 극강 응축
    """
    return float(row.get('BB40_Width', 99)) <= 5.0


# ─────────────────────────────────────────────────────────────
# [BUG-2] analyze_final() 내 explosion_ready 교체 블록
# ─────────────────────────────────────────────────────────────

# ❌ 기존
# bb_squeeze = row['BB40_Width'] <= 0.2
# supply_strong = row['OBV_Rising'] and row['MFI_Strong']
# explosion_ready = bb_squeeze and supply_strong

# ✅ 수정 (3단계 강도 분류)
def calc_explosion_ready(row) -> tuple:
    """
    폭발 준비 강도를 3단계로 반환.
    Returns: (explosion_ready: bool, explosion_level: str)
    """
    bb40_w = float(row.get('BB40_Width', 99))
    obv_ok = bool(row.get('OBV_Rising', False))
    mfi_ok = bool(row.get('MFI_Strong', False))
    buy_ok = bool(row.get('Buying_Pressure', False))
    supply_score = int(obv_ok) + int(mfi_ok) + int(buy_ok)

    if bb40_w <= 3.0 and supply_score >= 2:
        return True, "💎💎극강응축"
    elif bb40_w <= 5.0 and supply_score >= 2:
        return True, "💎강응축"
    elif bb40_w <= 8.0 and supply_score >= 2:
        return True, "💎약응축"
    return False, ""


# ─────────────────────────────────────────────────────────────
# [BUG-3] Stage TOP5 열거 인덱스 버그
# 위치: main 블록 stage_block 생성 루프
# ─────────────────────────────────────────────────────────────

# ❌ 기존 (i가 DataFrame의 실제 인덱스값 → 0,1,2 아닐 수 있음)
# for i, item in stage_candidates_top5.iterrows():
#     stage_block += f"{i+1}) [{item['종목명']}]\n"

# ✅ 수정
def build_stage_block(stage_candidates_top5) -> str:
    stage_block = ""
    if stage_candidates_top5.empty:
        return stage_block
    stage_block = "\n🚀 [단계 기반 급등 후보 TOP 5]\n\n"
    for rank, (_, item) in enumerate(stage_candidates_top5.iterrows(), 1):
        stage_block += (
            f"{rank}) [{item['종목명']}]\n"
            f"- 단계: {item.get('단계상태', 'N/A')} | {item.get('단계태그', '')}\n"
            f"- S1:{item.get('S1날짜', '-')}, "
            f"S2:{item.get('S2날짜', '-')}, "
            f"S3:{item.get('S3날짜', '-')}\n"
            f"- N조합: {item.get('N조합', '')}\n"
            f"- 재무: {item.get('재무', '미계산')} | 수급: {item.get('수급', '미계산')}\n"
            f"- 안전:{item.get('안전점수', 0)} | N점수:{item.get('N점수', 0)}\n"
            f"----------------------------\n"
        )
    return stage_block


# ─────────────────────────────────────────────────────────────
# [BUG-4] macro_status set literal 버그
# 위치: main 블록 ~line 3416
# ─────────────────────────────────────────────────────────────

# ❌ 기존 (중괄호 하나 더 감싸서 set이 됨)
# macro_status = {'nasdaq': m_ndx, ..., 'kospi': {get_index_investor_data('KOSPI')}}

# ✅ 수정
# macro_status = {
#     'nasdaq': m_ndx, 'sp500': m_sp5, 'vix': m_vix, 'fx': m_fx,
#     'kospi': get_index_investor_data('KOSPI')   # ← 중괄호 제거
# }


# ─────────────────────────────────────────────────────────────
# [BUG-5] target_dict 이중 할당 제거
# 위치: main 블록 ~line 3471, 3475
# ─────────────────────────────────────────────────────────────

# ❌ 기존 (같은 내용 두 번 할당)
# target_dict = dict(zip(sorted_df['Code'], sorted_df['Name']))   # line 3471
# weather_data = prepare_historical_weather()
# target_dict = dict(zip(sorted_df['Code'], sorted_df['Name']))   # line 3475 ← 삭제

# ✅ 수정: line 3475 두 번째 target_dict 할당 줄 삭제


# ─────────────────────────────────────────────────────────────
# [BUG-6] fillna(method=) FutureWarning → ffill() 으로 교체
# 위치: analyze_final() 내 ~line 2777
# ─────────────────────────────────────────────────────────────

# ❌ 기존
# df = df.join(historical_indices, how='left').fillna(method='ffill')

# ✅ 수정
# df = df.join(historical_indices, how='left').ffill()


# ─────────────────────────────────────────────────────────────
# [BUG-7] enrich 이중 호출 → 안전점수 중복 가산 방지
# 위치: build_and_sort_candidates() 내부
# ─────────────────────────────────────────────────────────────

# main 블록에서 이미 enrich 호출 후 → build_and_sort_candidates 에 넘기므로
# build_and_sort_candidates 내부의 enrich 호출은 제거

def build_and_sort_candidates_v2(all_hits_sorted, top_k=50):
    """
    Ver 27.2: enrich 이중 호출 제거.
    main 블록에서 enrich 완료된 리스트를 받아 정렬만 수행.
    """
    # Step 1: N점수 상위 cut
    n_top = sorted(all_hits_sorted, key=lambda x: x['N점수'], reverse=True)[:top_k]

    # Step 2: 단계랭크 → 안전점수 → N점수 순 정렬
    enriched = sorted(
        n_top,
        key=lambda x: (
            x.get('단계랭크', 0),
            x.get('안전점수', 0),
            x.get('N점수', 0)
        ),
        reverse=True
    )

    import pandas as pd
    return pd.DataFrame(enriched)


# ─────────────────────────────────────────────────────────────
# [TUNE-1] Stage 임계값 완화 → PASS_A/B 적절한 수 확보
# 위치: compute_stage_filters()
# ─────────────────────────────────────────────────────────────

# S3_READY 상단 허용 폭 확장: 1.03 → 1.05 (초동 돌파봉 포함)
# S1_ACC lookback: 12일 → 15일 (매집 감지 기간 확장)
# 아래 함수로 compute_stage_filters 전체 교체

import pandas as pd
import numpy as np

def compute_stage_filters_v2(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ver 27.2 Stage 필터 (튜닝된 임계값)
    변경사항:
    - S1 lookback 15일로 확장
    - S3 상단허용 1.05 (초동 돌파봉 포함)
    - S2 BB40_Width 기준 완화 18.0 → 보조 조건으로
    """
    df = df.copy()

    def _safe_rolling_mean(s, w):
        return s.rolling(w, min_periods=max(2, w // 2)).mean()

    def _safe_rolling_max(s, w):
        return s.rolling(w, min_periods=max(2, w // 2)).max()

    vol_ma5   = _safe_rolling_mean(df['Volume'], 5)
    vol_ma20  = _safe_rolling_mean(df['Volume'], 20)
    high20_prev = _safe_rolling_max(df['High'], 20).shift(1)
    bb40w_ma10  = _safe_rolling_mean(df['BB40_Width'], 10)

    # ── S1: 매집 수렴 (기간 완화)
    df['S1_ACC'] = (
        (df['Close'] > df['MA20']) &
        (df['Close'] > df['MA60']) &
        (vol_ma5 < vol_ma20) &
        (df['Close'] >= high20_prev * 0.82) &     # 0.85 → 0.82 완화
        (df['MA_Convergence'] <= 7.0)              # 6.0 → 7.0 완화
    )

    # ── S2: BB40 응축
    df['S2_SQUEEZE'] = (
        (df['BB40_Width'] <= bb40w_ma10) &
        (df['BB40_Width'] <= 18.0) &               # 15.0 → 18.0 완화
        (df['Close'] > df['MA20']) &
        (df['Close'] >= high20_prev * 0.88) &      # 0.90 → 0.88 완화
        (df['MA_Convergence'] <= 5.5)              # 4.5 → 5.5 완화
    )

    # ── S3: 돌파 직전 / 초동 돌파 허용
    df['S3_READY'] = (
        (df['Close'] > df['MA20']) &
        (df['Close'] > df['MA60']) &
        (df['Close'] >= high20_prev * 0.90) &      # 0.92 → 0.90 완화
        (df['Close'] <= high20_prev * 1.05) &      # 1.03 → 1.05 (초동 돌파봉 포함)
        (df['BB40_Width'] <= 20.0)                 # 18.0 → 20.0 완화
    )

    return df


def evaluate_stage_sequence_v2(df: pd.DataFrame) -> dict:
    """
    Ver 27.2: compute_stage_filters_v2 + lookback 확장 (S1 15일)
    """
    result = {
        "stage_status": "DROP",
        "s1_hit": False, "s2_hit": False, "s3_hit": False,
        "sequence_a": False, "sequence_b": False,
        "s1_date": None, "s2_date": None, "s3_date": None,
        "stage_tags": [],
    }

    if df is None or len(df) < 40:
        return result

    df = compute_stage_filters_v2(df)

    s3_today = bool(df['S3_READY'].iloc[-1])
    s3_date  = df.index[-1] if s3_today else None

    # S1 lookback 15일로 확장 (기존 12일)
    def _get_last_true_date(mask, lookback, include_today=True):
        if len(mask) == 0:
            return None
        sub = mask.iloc[-lookback:] if include_today else mask.iloc[-(lookback+1):-1]
        true_idx = sub[sub].index
        return true_idx[-1] if len(true_idx) > 0 else None

    s2_date = _get_last_true_date(df['S2_SQUEEZE'], lookback=8)   # 6 → 8
    s1_date = _get_last_true_date(df['S1_ACC'],     lookback=15)  # 12 → 15

    s2_hit = s2_date is not None
    s1_hit = s1_date is not None

    result.update({
        "s1_hit": s1_hit, "s2_hit": s2_hit, "s3_hit": s3_today,
        "s1_date": s1_date.strftime('%Y-%m-%d') if s1_date else None,
        "s2_date": s2_date.strftime('%Y-%m-%d') if s2_date else None,
        "s3_date": s3_date.strftime('%Y-%m-%d') if s3_date else None,
    })

    if not s3_today:
        return result

    if s1_hit and s2_hit and (s1_date <= s2_date <= s3_date):
        result.update({
            "stage_status": "PASS_A", "sequence_a": True,
            "stage_tags": ["🧬1→2→3", "📦매집", "🟣응축", "🚀돌파직전"]
        })
        return result

    if s2_hit and (s2_date <= s3_date):
        # pass_b_quality_filter: body_ratio 완화 0.12 → 0.15
        row = df.iloc[-1]
        vol_avg = row.get('Vol_Avg', np.nan)
        vol_now = row.get('Volume', 0)
        close_p = row.get('Close', 0)
        high_p  = row.get('High', 0)
        obv_ok  = bool(row.get('OBV_Rising', False))

        body_ratio = abs(close_p - row.get('Open', close_p)) / max(1e-9, row.get('Open', close_p))
        upper_wick_ratio = max(0.0, high_p - max(row.get('Open', 0), close_p)) / max(1e-9, high_p - row.get('Low', 0))

        cond_volume    = pd.notna(vol_avg) and (vol_now >= vol_avg * 1.05)
        cond_close_hi  = (high_p > 0) and (close_p >= high_p * 0.96)  # 0.97 → 0.96 완화
        cond_wick      = upper_wick_ratio <= 0.38                       # 0.35 → 0.38 완화
        cond_not_chase = body_ratio <= 0.15                            # 0.12 → 0.15 완화

        if cond_volume and cond_close_hi and cond_wick and cond_not_chase and obv_ok:
            result.update({
                "stage_status": "PASS_B", "sequence_b": True,
                "stage_tags": ["🟣2→3", "♻️재응축", "🚀재파동"]
            })

    return result


# ─────────────────────────────────────────────────────────────
# [TUNE-2] 로그 스팸 제거용 플래그
# get_indicators() 끝 print, analyze_final() 내 print 제거
# ─────────────────────────────────────────────────────────────

# get_indicators() 마지막 줄에서 아래 제거:
#   print("✅ 최종판독 완료")

# analyze_final() 내부에서 아래 제거:
#   print(f"✅ [본진] 조합 점수 계산!")

# 대신 hit 종목 발견 시에만 아래 1줄 유지:
#   print(f"✅ {name} 포착! 점수: {s_score} 태그: {tags}")


print("✅ main7_bugfix.py 패치 내용 확인 완료")
print("위 함수들로 main7.py 해당 부분을 교체하세요.")
