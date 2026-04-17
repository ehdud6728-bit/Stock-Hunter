from dataclasses import dataclass

# -----------------------------
# 출력/해설 토글
# -----------------------------
STRICT_FAKE_FILTER = True
SHOW_KKI_ONLY_WHEN_CONFIDENT = True
KKI_SHOW_MIN = 45
KKI_MEDIUM = 45
KKI_HIGH = 70

# -----------------------------
# 끼 패턴 탐지 기본값
# -----------------------------
KKI_LOOKBACK_DAYS = 240
KKI_IMPULSE_BODY_PCT = 0.055     # 장대양봉 최소 몸통 비율
KKI_IMPULSE_VOL_MULT = 1.8       # 장대양봉 최소 거래량 배수
KKI_PULLBACK_MAX_DAYS = 18       # 눌림 허용 길이
KKI_RELAUNCH_MAX_DAYS = 15       # 재발사 탐색 길이
KKI_SIDEWAYS_MAX_RANGE = 0.12    # 횡보 박스 최대 폭

# -----------------------------
# 밴드 적합도 비교군
# -----------------------------
BAND_FAMILIES = (
    ('BB20', 20, 2.0),
    ('BB40', 40, 2.0),
    ('ENV20', 20, 0.02),
    ('ENV40', 40, 0.02),
)
