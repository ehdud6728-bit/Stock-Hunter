# Closing_bet_scanner_v2 가독성 개선 패치
# 적용:
# 1) 기존 Closing_bet_scanner_v2.py에서 _format_hit 함수를 아래 코드로 통째로 교체
# 2) 기존 Closing_bet_scanner_v2.py에서 _send_results 함수를 아래 코드로 통째로 교체
# 3) [탈락진단] 로그 줄은 아래 한 줄만 남기세요.
#    log_info(f"[탈락진단] {STRATEGY_FAIL}")

def _format_hit(hit: dict, rank: int = 0, mins_left: int = 0) -> str:
    def _g(*keys, default=""):
        for k in keys:
            if k in hit and hit.get(k) is not None:
                return hit.get(k)
        return default

    code = str(_g("code", "Code", default="")).strip()
    name = str(_g("name", "Name", "종목명", default=code)).strip() or code

    close = _safe_float(_g("close", "Close", "현재가", "price", "종가", "_close", default=0), 0.0)
    score = _safe_float(_g("score", "점수", default=0), 0.0)
    vol_ratio = _safe_float(_g("vol_ratio", "volume_ratio", default=0), 0.0)
    amount_b = _safe_float(_g("amount_b", "거래대금억", default=0), 0.0)

    grade = str(_g("grade_label", "grade", "등급", default="B급")).strip()
    strategy = str(_g("strategy", "mode", "전략", default="")).strip()
    mode_label = str(_g("mode_label", default=(strategy if strategy else "종가배팅"))).strip() or (strategy if strategy else "종가배팅")

    recommended_band = str(_g("recommended_band", default="")).strip()
    support_band = str(_g("support_band", default="")).strip()
    band_comment = str(_g("band_comment", "band_reason", default="")).strip()

    kki_pattern = str(_g("kki_pattern", default="")).strip()
    kki_habit = str(_g("kki_habit", default="")).strip()
    kki_comment = str(_g("kki_comment", default="")).strip()
    kki_score = _safe_int(_g("kki_score", default=0), 0)
    absorb_score = _safe_int(_g("absorb_score", default=0), 0)

    idx_label = str(_g("index_label", default="")).strip()
    universe_tag = str(_g("universe_tag", default="")).strip()
    location = idx_label if idx_label else universe_tag

    passed = _g("passed", default=[])
    if isinstance(passed, (list, tuple)):
        passed_str = " · ".join(str(x) for x in passed if str(x).strip())
    else:
        passed_str = str(passed).strip()

    interpretation_parts = []
    if mode_label == "돌파형":
        interpretation_parts.append("전고점 부근에서 종가가 강하게 버틴 돌파 후보")
    elif mode_label == "ENV엄격형":
        interpretation_parts.append("엔벨로프 하단 부근에서 받치며 반등을 준비하는 후보")
    elif mode_label == "BB확장형":
        interpretation_parts.append("볼린저 하단권에서 변동성 확장을 노리는 후보")

    if kki_pattern:
        interpretation_parts.append(f"끼 패턴은 '{kki_pattern}' 쪽")
    if kki_score >= 60:
        interpretation_parts.append("재상승 탄력 기대 가능")
    elif kki_score >= 35:
        interpretation_parts.append("약한 반등보다 눌림 확인이 유리")
    else:
        interpretation_parts.append("무리한 추격보다는 보수적 접근이 적절")

    interpretation = " / ".join(interpretation_parts)

    lines = []
    head = f"{rank}) {mode_label} {grade} | {name}({code})"
    if location:
        head += f" | {location}"
    lines.append(head)
    lines.append(f"   현재가 {int(close):,}원 | 점수 {score:.1f} | 거래량비 {vol_ratio:.2f} | 거래대금 {amount_b:.1f}억")

    if recommended_band:
        band_line = f"   밴드: {recommended_band}"
        if support_band and support_band != recommended_band:
            band_line += f" / 보조 {support_band}"
        if band_comment:
            band_line += f" | {band_comment}"
        lines.append(band_line)

    if passed_str:
        lines.append(f"   통과근거: {passed_str}")

    if kki_pattern or kki_score > 0 or absorb_score > 0:
        lines.append(f"   끼 분석: {kki_pattern or '혼합형'} | 끼 {kki_score} / 흡수 {absorb_score}")

    natural_kki = " ".join(x for x in [kki_habit, kki_comment] if x).strip()
    if natural_kki:
        lines.append(f"   해석: {natural_kki}")

    if interpretation:
        lines.append(f"   한줄해석: {interpretation}")

    if hit.get("mode") in ("B1", "B2") and hit.get("maejip_chart"):
        lines.append(f"   매집흔적: {hit.get('maejip_chart')}")

    return "\n".join(lines)


def _send_results(hits: list, mins_left: int):
    log_info(f"_send_results 호출: {len(hits)}개 | TOKEN={'✅' if TELEGRAM_TOKEN else '❌'}")

    if not hits:
        msg = (
            f"[{TODAY_STR}] 종가배팅 후보 없음\n"
            f"(대상: {SCAN_UNIVERSE} | 조건 미충족)"
        )
        log_info("→ 후보 없음 메시지 전송")
        send_telegram_photo(msg, [])
        return

    def _pick_strategy(hit):
        return str(hit.get("strategy") or hit.get("mode") or hit.get("전략") or "").strip()

    def _grade_core(hit):
        g = str(hit.get("grade", "")).strip().upper()
        if g == "COMPLETE":
            return "COMPLETE"
        if g == "A":
            return "A"
        return "B"

    def _safe_score(hit):
        return _safe_float(hit.get("score", hit.get("점수", 0)), 0.0)

    def _priority(h):
        gc = _grade_core(h)
        g_rank = 0 if gc == "COMPLETE" else (1 if gc == "A" else 2)
        return (
            g_rank,
            -_safe_score(h),
            -_safe_float(h.get("vol_ratio", h.get("volume_ratio", 0)), 0.0),
            -_safe_float(h.get("amount_b", 0), 0.0),
        )

    hits_a = [x for x in hits if _pick_strategy(x) == "A"]
    hits_b1 = [x for x in hits if _pick_strategy(x) == "B1"]
    hits_b2 = [x for x in hits if _pick_strategy(x) == "B2"]

    hits_a.sort(key=_priority)
    hits_b1.sort(key=_priority)
    hits_b2.sort(key=_priority)

    complete_hits = [x for x in hits if _grade_core(x) == "COMPLETE"]
    a_grade_hits = [x for x in hits if _grade_core(x) == "A"]
    b_grade_hits = [x for x in hits if _grade_core(x) == "B"]

    total = min(len(hits_a), 5) + min(len(hits_b1), 5) + min(len(hits_b2), 5)

    header = (
        f"📌 종가배팅 선별 TOP {total} ({TODAY_STR})\n"
        f"⏰ 마감까지 {mins_left}분\n"
        f"돌파형(A) {min(len(hits_a), 5)}개 | ENV엄격형(B1) {min(len(hits_b1), 5)}개 | BB확장형(B2) {min(len(hits_b2), 5)}개\n"
        f"완전체 {len(complete_hits)}개 | A급 {len(a_grade_hits)}개 | B급 {len(b_grade_hits)}개"
    )

    sections = [header]

    def _build_block(title: str, items: list, tag: str):
        block = [f"[{title}]"]
        if not items:
            block.append("해당 종목 없음")
            return "\n".join(block)

        for idx, hit in enumerate(items[:5], 1):
            try:
                entry = _format_hit(hit, idx, mins_left)
            except Exception as e:
                log_error(f"_format_hit 오류 [{tag}/{hit.get('code','')}]: {e}")
                entry = ""
            log_info(f"[FORMAT-{tag}] code={hit.get('code')} | len={len(entry)}")
            if entry:
                block.append(entry)
                block.append("")  # 종목 간 한 줄 띄우기
        return "\n".join(block).rstrip()

    sections.append(_build_block("돌파형(A) TOP5", hits_a, "A"))
    sections.append(_build_block("ENV엄격형(B1) TOP5", hits_b1, "B1"))
    sections.append(_build_block("BB확장형(B2) TOP5", hits_b2, "B2"))

    chunks = []
    current = ""
    for sec in sections:
        add = sec if not current else "\n\n" + sec
        if len(current) + len(add) > 3500:
            if current.strip():
                chunks.append(current.strip())
            current = sec
        else:
            current += add
    if current.strip():
        chunks.append(current.strip())

    for i, chunk in enumerate(chunks, 1):
        log_info(f"텔레그램 전송 {i}/{len(chunks)} | 길이={len(chunk)}")
        send_telegram_photo(chunk, [])

    log_info("✅ 텔레그램 전송 완료")
