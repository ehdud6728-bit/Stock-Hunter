# Closing_bet_scanner_v2 출력부 교체 패치
# 적용 방법:
# 1) 기존 Closing_bet_scanner_v2.py에서 _format_hit 함수를 통째로 아래 코드로 교체
# 2) [탈락진단] 로그 줄이 있으면 아래 한 줄로 교체:
#    log_info(f"[탈락진단] {STRATEGY_FAIL}")

def _format_hit(hit: dict, rank: int = 0, mins_left: int = 0) -> str:
    def _g(*keys, default=""):
        for k in keys:
            if k in hit and hit.get(k) is not None:
                return hit.get(k)
        return default

    code = str(_g("code", "Code", default="")).strip()
    name = str(_g("name", "Name", "종목명", default=code)).strip() or code

    close = _safe_float(_g("close", "Close", "현재가", "price", default=0), 0.0)
    score = _safe_float(_g("score", "점수", default=0), 0.0)
    vol_ratio = _safe_float(_g("vol_ratio", "volume_ratio", default=0), 0.0)

    grade = str(_g("grade", "등급", default="B")).strip()
    strategy = str(_g("strategy", "mode", "전략", default="")).strip()

    recommended_band = str(_g("recommended_band", default="")).strip()
    support_band = str(_g("support_band", default="")).strip()
    band_comment = str(_g("band_comment", default="")).strip()

    kki_pattern = str(_g("kki_pattern", default="")).strip()
    kki_habit = str(_g("kki_habit", default="")).strip()
    kki_comment = str(_g("kki_comment", default="")).strip()
    kki_score = _safe_int(_g("kki_score", default=0), 0)
    absorb_score = _safe_int(_g("absorb_score", default=0), 0)

    idx_label = str(_g("index_label", default="")).strip()
    universe_tag = str(_g("universe_tag", default="")).strip()
    idx_str = f" | {idx_label}" if idx_label else (f" | {universe_tag}" if universe_tag else "")

    mode_label = strategy if strategy else "종가배팅"

    lines = []
    title = f"{rank}) " if rank > 0 else ""
    lines.append(f"{title}{mode_label} {grade} [{name}({code})] {int(close):,}원{idx_str}")
    lines.append(f"- 점수: {score:.1f} | 거래량비: {vol_ratio:.2f}")

    if recommended_band:
        band_line = f"- 적합밴드: {recommended_band}"
        if support_band and support_band != recommended_band:
            band_line += f" / 보조밴드: {support_band}"
        if band_comment:
            band_line += f" | {band_comment}"
        lines.append(band_line)

    if kki_pattern or kki_score > 0 or absorb_score > 0:
        lines.append(f"- 끼 패턴: {kki_pattern or '혼합형'} | 끼점수 {kki_score} / 흡수점수 {absorb_score}")

    if kki_habit or kki_comment:
        lines.append(f"- 끼 해설: {(kki_habit + ' ' + kki_comment).strip()}")

    final_label = str(_g("final_label", default="")).strip()
    action = str(_g("action_summary", "action", default="")).strip()
    if final_label:
        lines.append(f"- 최종 판정: {final_label}")
    if action:
        lines.append(f"- 대응: {action}")

    return "\n".join(lines) + "\n"
