"""Write the Korean research report from the validated outputs."""
import pandas as pd


def write_report(out, ledger, audit):
    groups = pd.read_csv(out / "GROUPS.csv", dtype={"year": str})
    policy = pd.read_csv(out / "D1_D2_POLICIES.csv", dtype={"year": str})
    cases = pd.read_csv(out / "CASES.csv", dtype={"code": str})
    full = groups[groups["sample"].eq("ALL")]
    lines = [
        "# BLUE20 매물대·구름대·AVWAP 통합 연구",
        "",
        "2022년 말 선정한 500종목의 2023·2024년 연구 결과입니다. "
        "LIVE·SEARCH·WATCH 또는 R1-D118 운영 규칙의 변경 근거로 사용하기에는 표본과 검증기간이 제한적입니다.",
        "",
        f'기준 청산·순손익 재현 {audit["baseline_trade_replays_matched"]:,}건 · '
        f'신호 종목 {audit["event_stocks"]}개 · 차트 {audit["chart_cases"]}개. '
        f'원장 간 일치 {audit["prior_cross_run_ledger_rows_matched"] or "이번 실행에서 미대조"}건, '
        f'선행 실행과 시세 해시 일치 {audit["prior_cross_run_event_stock_price_hashes_matched"] or "이번 실행에서 미대조"}개.',
        "",
        "## 전체와 연도별 기준 성과",
        "",
        "D+20 고가는 정확히 20번째 거래일의 고가, 기간 최고가는 D+1~D+20 중 최고가입니다. "
        "둘 다 D0 종가 대비이며 실제 청산 수익과 별개입니다. 아래 수익은 사건 평균, 손절·익절은 청산 사유 기준입니다.",
        "",
        "| 표본 | 연도 | 밴드 | 사건 / 종목 | 손절률 | 익절률 | 평균 순손익 | 기간 최고가 | D+20 당일 고가 |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for r in groups[groups.feature.eq("baseline")].itertuples():
        lines.append(f"| {r.sample} | {r.year} | {r.band} | {r.events}/{r.stocks} | "
                     f"{r.sl_pct:.2f}% | {r.tp_pct:.2f}% | {r.net_pct:+.3f}% | "
                     f"{r.mean_d1_to_d20_peak_pct:+.2f}% | {r.mean_d20_day_high_pct:+.2f}% |")
    upper = full[full.band.eq("UPPER")]
    crossings = upper[upper.feature.eq("vp_breakout") & upper.value.eq("BODY_CROSS")]
    aligned = upper[upper.feature.eq("confluence") & upper.value.eq("ALIGNED")]
    crossover = crossings[crossings.year.eq("ALL")].iloc[0]
    both = aligned[aligned.year.eq("ALL")].iloc[0]
    cross_years = " · ".join(f"{r.year}년 {r.net_pct:+.3f}%"
                             for r in crossings[~crossings.year.eq("ALL")].itertuples())
    align_years = " · ".join(f"{r.year}년 {r.net_pct:+.3f}%"
                             for r in aligned[~aligned.year.eq("ALL")].itertuples())
    lines += [
        "", f"상단 VAH 몸통 돌파 {crossover.events}건은 손절률 {crossover.sl_pct:.2f}%, "
        f"평균 순손익 {crossover.net_pct:+.3f}%였습니다. 연도별로 {cross_years}였습니다. "
        f"구름대·AVWAP·세 이평선까지 충족한 {both.events}건의 평균은 {both.net_pct:+.3f}%였으나 "
        f"연도별로 {align_years}였습니다.",
        "", "## 신호일 구조와 다음 날 반응", "",
        "아래는 전체기간의 관측치입니다. 연도별·비중복 표본과 전체 분류는 [집계 CSV](GROUPS.csv)에 있습니다. "
        "D1 반응별 기준 성과는 D1 종가까지 본 뒤 나눈 사후 집계입니다.",
        "", "| 밴드 | 특징 | 조건 | 사건 / 종목 | 손절률 | 익절률 | 평균 순손익 | 기간 최고가 |",
        "|---|---|---|---:|---:|---:|---:|---:|",
    ]
    labels = {
        "vp_breakout":"VAH 돌파", "vp_resistance_bucket":"매물대 저항 거리",
        "structure_resistance_bucket":"통합 저항 거리", "cloud_position":"구름대 위치",
        "avwap_position":"AVWAP 위치", "ma_stack":"60·112·224일선 배열",
        "body_bucket":"ATR 몸통", "turnover_bucket":"거래대금",
        "d0_candle_shape":"D0 캔들", "d1_support":"D1 종가 지지", "confluence":"통합 조건",
    }
    for feature, label in labels.items():
        for r in full[full.year.eq("ALL") & full.feature.eq(feature)].itertuples():
            lines.append(f"| {r.band} | {label} | {r.value} | {r.events}/{r.stocks} | "
                         f"{r.sl_pct:.2f}% | {r.tp_pct:.2f}% | {r.net_pct:+.3f}% | "
                         f"{r.mean_d1_to_d20_peak_pct:+.2f}% |")
    lines += [
        "", "## D1 시가와 D2 시가의 짝지은 비교", "",
        "D0 RSI 동시 확인 표본에서 공통 D0+20 만기를 적용했습니다. 미진입 수익은 0%입니다. "
        "D2 무조건 진입은 하루 지연의 영향을, D2 조건부 진입은 D1 종가가 몸통 중간값과 D0 고정 VAH를 모두 지킨 경우를 평가합니다.",
        "",
        "| 연도 | 밴드 | 정책 | 진입 / 기회 | 손절 / 익절 | 진입당 순손익 | 기회당 순손익 | 즉시 대비 | 놓친 기존 익절 |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for r in policy.itertuples():
        lines.append(f"| {r.year} | {r.band} | {r.policy} | {r.trades}/{r.opportunities} | "
                     f"{r.sl}/{r.tp} | {r.net_trade_pct:+.3f}% | {r.net_opportunity_pct:+.3f}% | "
                     f"{r.paired_delta_pp:+.3f}%p | {r.missed_baseline_tp} |")
    for band, label in [("UPPER", "상단"), ("LOWER", "하단")]:
        b = policy[policy.year.eq("ALL") & policy.band.eq(band)].set_index("policy")
        immediate = b.loc["D1_IMMEDIATE"]
        unconditional = b.loc["D2_UNCONDITIONAL"]
        gated = b.loc["D2_MID_AND_PRIOR_VAH"]
        lines += ["", f"{label} 기회당 순손익은 D1 즉시 {immediate.net_opportunity_pct:+.3f}%, "
                  f"무조건 D2 {unconditional.net_opportunity_pct:+.3f}%, "
                  f"지지 확인 D2 {gated.net_opportunity_pct:+.3f}%였습니다. "
                  f"지지 확인 정책은 {gated.trades}건 진입, {gated.missed_baseline_tp}건의 기존 익절을 놓쳤습니다."]
    lines += [
        "", "## 실제 일봉 8건", "",
        "차트 우측은 D0 이전 120거래일의 고정 추정 매물대입니다. D1 이후 봉은 사후 경로로 표시했습니다. "
        "사례의 성공과 실패는 기존 진입·청산 결과로 선정했습니다. [차트 갤러리](GALLERY_KO.html)",
        "", "| 사례 | 청산 / 수익 | VAH / POC / VAL | D0 VAH | 통합 저항 거리 | 구름 / AVWAP | MA60·112·224 이격 | 몸통 / 거래대금 | D1 지지 |",
        "|---|---|---:|---|---:|---|---:|---:|---|",
    ]
    for r in cases.itertuples():
        dist = f"{r.structure_resistance_pct:.2f}%" if pd.notna(r.structure_resistance_pct) else "위 레벨 없음"
        lines.append(f"| {r.name} {r.code} {r.date} | {r.baseline_exit_reason} {r.baseline_net_pct:+.2f}% | "
                     f"{r.vp_vah:,.0f} / {r.vp_poc:,.0f} / {r.vp_val:,.0f} | {r.vp_breakout} | "
                     f"{dist} | {r.cloud_position} / {r.avwap_position} | "
                     f"{r.ma60_gap_pct:+.1f}% / {r.ma112_gap_pct:+.1f}% / {r.ma224_gap_pct:+.1f}% | "
                     f"{r.d0_body_abs_prior_atr20:.2f}배 / {r.d0_amount_ratio20:.2f}배 | {r.d1_support} |")
    lines += [
        "", "HLB와 제주은행 모두 D0에 고정 VAH를 넘지 못하고 D1에 몸통 중간값만 지켰지만, "
        "기존 전략의 청산 결과는 서로 달랐습니다. 삼양사·한진칼은 통합 후보에 D0 종가보다 높은 레벨이 없었으나 손절됐고, "
        "씨아이에스·삼표시멘트·SBS는 같은 거리 분류에서 익절됐습니다. "
        "F&F는 VAH를 넘은 뒤에도 MA224 아래에 남았습니다. "
        "삼표시멘트는 D1에 중간값과 VAH를 모두 잃었지만, 이 사례의 원래 RSI는 D0 이후에 확인됐으므로 D0 동시 확인 정책의 평가 대상이 아닙니다.",
    ]
    lines += [
        "", "## 감사와 해석 범위", "",
        f'- 같은 종목·날짜 중복 {audit["same_stock_day_duplicates_beyond_first"]}건, '
        f'종목 반복 {audit["repeated_stock_events_beyond_first"]:,}건, '
        f'날짜 반복 {audit["repeated_day_events_beyond_first"]:,}건이며 하루 최대 {audit["max_events_one_day"]}건입니다. '
        f'동일 종목 D0~D+20 창이 겹치는 후속 신호를 제거하면 {audit["nonoverlap_events"]:,}건입니다. '
        "시장 공통 충격은 남으므로 이 표를 독립 관측의 검정으로 해석할 수 없습니다.",
        f'- 매물대 {audit["d0_vp_coverage"]:,}건, 구름 {audit["d0_cloud_coverage"]:,}건, '
        f'AVWAP {audit["d0_avwap_coverage"]:,}건, MA224 {audit["d0_ma224_coverage"]:,}건을 계산했습니다. '
        f'대표가격 한 구간에 거래량을 배정한 대안에서는 VAH 돌파 분류가 {audit["alt_profile_class_changed"]:,}건 바뀌었습니다. '
        "일봉만으로 체결가별 거래량·매수매도 방향을 알 수 없고 구간 설정에 민감합니다.",
        "- 구름대는 표시일 기준 26거래일 전까지 계산한 선행스팬입니다. AVWAP 기준일은 D0 전 60거래일 최저가의 가장 최근 날짜입니다. "
        "D0 특징은 D0 장 종료 뒤, D1 지지는 D1 장 종료 뒤에 이용할 수 있습니다. "
        "거래대금은 직전 20일 평균 대비 활동량으로 투자 주체를 나타내지 않습니다.",
        "- 원장 1,804건은 이후 D+10 안에 RSI 교차가 관측된 표본입니다. D0에서 관찰 가능한 전체 돌파 모집단의 기대성과는 별도 검증이 필요합니다. "
        "원래 증권사 RSI 공식과 아카이브의 기업행사 처리, 실거래 체결을 이번 연구에서 인증하지 않았습니다.",
        "- 2023/2024 분할은 동일 연구 표본의 기간 일관성 검사입니다. 임계값, 가격 구간, AVWAP 앵커, 사례 8건의 결과에 맞춘 재조정은 "
        "과최적화 위험이 있습니다. 별도 기간에 조건을 고정하고 종목·신호일 군집을 반영한 차이 구간으로 재검증해야 합니다.",
        "",
        "[장대양봉 선행 실행](https://github.com/ehdud6728-bit/Stock-Hunter/actions/runs/42217814430) · "
        "[이평선 선행 실행](https://github.com/ehdud6728-bit/Stock-Hunter/actions/runs/42200217422)",
        "",
    ]
    (out / "REPORT_KO.md").write_text("\n".join(lines), encoding="utf-8")

