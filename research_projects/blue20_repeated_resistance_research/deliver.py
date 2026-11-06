"""Write the Korean report and self-contained OHLCV gallery."""
from html import escape

import numpy as np
import pandas as pd

from research import CASES, KEY, PRIMARY

LABELS = dict(A="A 즉시", B="B 보류", C="C 안착", D="D 눌림·재신호", RSI="기존 RSI")


def fmt(value, digits=2):
    return "—" if not np.isfinite(value) else f"{value:.{digits}f}"


def select_cases(ledger, primary):
    requested = [(code, date, "UPPER", name, "지정 사례", *PRIMARY)
                 for (code, date), name in CASES.items()]
    order = ["date", "code", "band"]
    baseline = primary[primary.policy.eq("A") & primary.band.eq("UPPER")].sort_values(order)
    baseline = baseline[~baseline.set_index(["code", "date"]).index.isin(CASES)]
    contrasts = [
        (baseline.block & baseline.exit_reason.str.startswith("TP"), "보류 조건에 걸린 익절 반례"),
        (baseline.block & baseline.exit_reason.str.startswith("SL"), "보류 조건에 걸린 손절 대조"),
        (baseline.large_bull & ~baseline.block & baseline.exit_reason.str.startswith("SL"),
         "보류 조건에 걸리지 않은 장대양봉 손절"),
        (baseline.large_bull & ~baseline.block & baseline.exit_reason.str.startswith("TP"),
         "보류 조건에 걸리지 않은 장대양봉 익절"),
    ]
    for mask, label in contrasts:
        if mask.any():
            e = baseline[mask].iloc[0]
            requested.append((e.code, e.date, e.band, e.code, label, *PRIMARY))
    alternative = ledger[ledger.window.eq(240) & ledger.method.eq("range") & ledger.block
                         & ledger.band.eq("UPPER") & ledger.policy.eq("A")
                         & ledger.exit_reason.str.startswith("TP")].sort_values(order)
    if len(alternative):
        e = alternative.iloc[0]
        requested.append((e.code, e.date, e.band, e.code, "240일 보류로 놓친 익절 반례", 240, "range"))
    return requested


def svg(d, j, profile, rows):
    r = rows.loc["A"]
    start = max(0, j-int(r.window))
    span = d.iloc[start:j+21]
    dates = {date: i for i, date in enumerate(span.date)}
    lo, hi = span.low.min(), span.high.max()
    pad = (hi-lo) * .05 or 1
    lo, hi = lo-pad, hi+pad
    step = 840 / max(1, len(span)-1)
    x = lambda i: 63 + step*i
    y = lambda price: 380 - 330*(price-lo)/(hi-lo)
    out = ['<svg viewBox="0 0 1130 514" role="img" aria-label="일봉과 과거 추정 매물대">',
           '<rect width="1130" height="514" fill="#fafafa"/>']
    for price in np.linspace(lo, hi, 6):
        py = y(price)
        out.append(f'<path d="M62 {py} H1100" stroke="#ddd"/><text x="4" y="{py}" font-size="10">{price:,.0f}</text>')
    if np.isfinite(r.zone_high):
        top, bottom = y(r.zone_high), y(r.zone_low)
        color = "#d48608" if r.strong else "#64748b"
        out.append(f'<rect x="63" y="{top}" width="1037" height="{bottom-top}" fill="{color}" opacity=".18"/>')
    if profile is not None:
        weights = profile["weights"]
        for center, weight in zip(profile["centers"], weights):
            height = 330*profile["width"]/(hi-lo)
            out.append(f'<rect x="930" y="{y(center)-height/2}" width="{167*weight/weights.max()}" height="{height*.92}" fill="#7395b3" opacity=".65"/>')
    vmax = span.volume.max() or 1
    for i, bar in enumerate(span.itertuples()):
        color = "#bb342e" if bar.close >= bar.open else "#2470a2"
        cx = x(i)
        out.append(f'<path d="M{cx} {y(bar.low)} V{y(bar.high)}" stroke="{color}"/>')
        out.append(f'<rect x="{cx-step*.32}" y="{min(y(bar.open),y(bar.close))}" width="{step*.64}" height="{max(1,abs(y(bar.open)-y(bar.close)))}" fill="{color}"/>')
        out.append(f'<path d="M{cx} 440 v{-47*bar.volume/vmax}" stroke="{color}" stroke-width="3" opacity=".6"/>')
    for date in str(r.test_dates).split("|"):
        if date in dates:
            i = dates[date]
            out.append(f'<circle cx="{x(i)}" cy="{y(span.high.iloc[i])-5}" r="4" fill="#9a5500"><title>{date} 저항 재시험</title></circle>')
    for i in range(0, len(span), max(24,len(span)//6)):
        out.append(f'<text x="{x(i)}" y="462" font-size="10">{span.date.iloc[i]}</text>')
    sx = x(j-start)
    out.append(f'<path d="M{sx} 28 V445" stroke="#111" stroke-dasharray="3 3"/><text x="{sx}" y="20" font-size="12">D0</text>')
    for row_number, (policy, t) in enumerate(rows.iterrows()):
        if not t.entered:
            continue
        color = ("#1e293b", "#64748b", "#7e22ce", "#047857", "#a16207")[row_number]
        for date, price, glyph in ((t.entry_date, t.entry_price, "▲"), (t.exit_date, t.exit_price, "◆")):
            cx = x(dates[date]) + (row_number-2)*1.4
            py = y(price) + (row_number-2)*6
            out.append(f'<text x="{cx}" y="{py}" fill="{color}" font-size="10" text-anchor="middle">{glyph}<title>{policy} {date} {price:,.0f}</title></text>')
    out.append('<text x="64" y="494" font-size="12">▲ 진입  ◆ 청산  ● 과거 저항 재시험 · 오른쪽 추정 가격대별 거래량 · 아래 일별 거래량</text></svg>')
    return "".join(out)


def policy_table(group):
    lines = ["| 정책 | 기회 | 진입률 | 기회당 순수익 | 진입당 순수익 | 진입당 손절률 | 놓친 +10/+20 | 진입 후 D+20 최고 상승 | 평균/최악 MDD |",
             "|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for r in group.itertuples():
        lines.append(f"| {LABELS[r.policy]} | {r.n} | {fmt(r.entry_pct)}% | "
                     f"{fmt(r.net_opportunity_pct)}% | {fmt(r.net_trade_pct)}% | {fmt(r.sl_trade_pct)}% | "
                     f"{r.missed10}/{r.missed20} | {fmt(r.mean_entry_to_d20_peak_pct)}% | "
                     f"{fmt(r.mean_trade_mdd_pct)}/{fmt(r.worst_trade_mdd_pct)}% |")
    return lines


def write_outputs(out, ledger, daily, summary, frames, chart_data):
    ledger.to_csv(out/"POLICY_LEDGER.csv.gz", index=False)
    daily.to_csv(out/"DAILY_CONFIRMATION.csv.gz", index=False)
    summary.to_csv(out/"SUMMARY.csv", index=False, encoding="utf-8-sig")
    primary = ledger[ledger.window.eq(PRIMARY[0]) & ledger.method.eq(PRIMARY[1])]
    controls = primary[primary.policy.eq("A")].copy()
    controls["group"] = np.select(
        [~controls.coverage, controls.block, controls.large_bull & controls.strong, controls.large_bull],
        ["UNKNOWN", "BLOCK", "LARGE_STRONG_OTHER", "LARGE_NO_STRONG"], default="OTHER")
    controls["stop"] = controls.exit_reason.str.startswith("SL")
    controls["tp"] = controls.exit_reason.str.startswith("TP")
    control_stats = controls.groupby(["band", "year", "group"]).agg(
        n=("policy", "size"), stops=("stop", "sum"), tp=("tp", "sum"), mean_net=("net_pct", "mean"),
        mean_turnover=("d0_amount_ratio20", "mean"), mean_efficiency=("efficiency_atr_per_turnover", "mean"),
        mean_upper_wick=("upper_wick", "mean"), mean_ma60_slope=("ma60_slope10", "mean"),
        mean_ma112_slope=("ma112_slope10", "mean")).reset_index()
    control_stats.to_csv(out/"CONTROLS.csv", index=False, encoding="utf-8-sig")

    sections = ['<!doctype html><meta charset="utf-8"><title>BLUE20 반복 저항</title>',
                '<style>body{font:16px system-ui;margin:2em auto;max-width:1150px;padding:0 1em}'
                'section{margin:3em 0}svg{width:100%}td,th{padding:.25em .6em;text-align:left}</style>',
                '<h1>BLUE20 반복 저항 실제 일봉 비교</h1>',
                '<p>주황색은 강한 저항 존, 회색은 강도 기준 미달인 후보 존입니다. '
                '가격대별 거래량은 과거 일봉 고가·저가 균등 배분 추정치입니다.</p>']
    case_lines = ["| 종목 · D0 | 반복 시험 | 거절 ATR | 몸통 겹침 | 종가 전진 ATR | 보류 | 기존 RSI | A 순수익 |",
                  "|---|---:|---:|---:|---:|---|---:|---:|"]
    case_records = []
    for code, date, band, name, category, n, method in select_cases(ledger, primary):
        group = ledger[ledger.code.eq(code) & ledger.date.eq(date) & ledger.band.eq(band)
                       & ledger.window.eq(n) & ledger.method.eq(method)].set_index("policy")
        r = group.loc["A"]
        j, profile = chart_data[(code, date, band, n, method)]
        case_records.append(group.reset_index().assign(name=name, category=category))
        case_lines.append(f"| {name} · {date} · {n}일 | {r.tests} | {fmt(r.rejection_atr)} | {fmt(100*r.overlap)}% | "
                          f"{fmt(r.progress_atr)} | {'예' if r.block else '아니오'} | "
                          f"{fmt(group.loc['RSI','net_pct'])}% | {fmt(r.net_pct)}% |")
        sections.append(f'<section><h2>{escape(name)} {code} · {date}</h2><p>{category} · {n}일 · '
                        f'저항 {fmt(r.zone_low,0)}–{fmt(r.zone_high,0)} · 시험 {r.tests}회 · '
                        f'겹침 {fmt(100*r.overlap)}% · 종가 전진 {fmt(r.progress_atr)} ATR · '
                        f'보류 {"예" if r.block else "아니오"}</p>')
        sections.append(svg(frames[code], j, profile, group))
        sections.append('<table><tr><th>정책</th><th>경로</th><th>진입일</th><th>청산일</th><th>청산 사유</th><th>순수익</th></tr>')
        for policy, t in group.iterrows():
            entry_date = t.entry_date if t.entered else "—"
            exit_date = t.exit_date if t.entered else "—"
            sections.append(f'<tr><td>{LABELS[policy]}</td><td>{t.route}</td><td>{entry_date}</td>'
                            f'<td>{exit_date}</td><td>{t.exit_reason}</td><td>{fmt(t.net_pct)}%</td></tr>')
        sections.append('</table></section>')
    pd.concat(case_records, ignore_index=True).to_csv(out/"CASES.csv", index=False, encoding="utf-8-sig")
    (out/"GALLERY_KO.html").write_text("\n".join(sections), encoding="utf-8")

    selected = summary[summary.window.eq(PRIMARY[0]) & summary.method.eq(PRIMARY[1])]
    upper = controls[controls.band.eq("UPPER")]
    blocked = upper[upper.block]
    main = selected[selected.cohort.eq("all") & selected.band.eq("UPPER") & selected.year.eq("ALL")].set_index("policy")
    alternative = ledger[ledger.window.eq(240) & ledger.method.eq("range") & ledger.block
                         & ledger.band.eq("UPPER") & ledger.policy.eq("A")]
    reentered = {p: int(main.loc[p,"entries"]-main.loc["B","entries"]) for p in "CD"}
    lines = ["# BLUE20 반복 저항과 장대양봉 진입 연구", "",
             f"500종목의 기존 원장 1,804건을 재현했다. 주 분석인 상단 834건 중 {len(blocked)}건이 "
             f"보류 조건에 걸렸다. 이 중 즉시 진입 기준 손절 {blocked.stop.sum()}건, 익절 "
             f"{blocked.tp.sum()}건이었다. 8개 지정 사례는 규칙 선정과 수치 조정에 사용하지 않았다.", "",
             f"주 분석에서 기회당 평균 순수익은 A {fmt(main.loc['A','net_opportunity_pct'])}%, "
             f"B {fmt(main.loc['B','net_opportunity_pct'])}%였다. 보류 대상 가운데 C가 "
             f"{reentered['C']}건, D가 {reentered['D']}건에 재진입했다. "
             f"240일 균등 배분에서는 보류 대상 {len(alternative)}건 중 "
             f"{alternative.exit_reason.str.startswith('TP').sum()}건이 A에서 익절했다. "
             "표본 수가 적고 근사 방식에 따라 결과가 바뀌므로 일반적인 매수 보류 규칙의 근거는 부족하다.", "",
             "원장 자체가 D0~D+10 안에 RSI 골든크로스가 발생한 사건으로 구성된다. "
             "전체 A~D 비교에는 이 표본 선택 편향이 있다. 당시 RSI까지 확인된 800건만 분리한 "
             "GC0 분석도 함께 제공한다. 하단 원장은 동일 규칙을 적용한 대조군이다. "
             "D+1~D+20 확인 원장에서 D+11 이후는 사후 관찰용이다.", "",
             "## A~D 및 기존 RSI 비교", ""]
    for cohort, cohort_label in [("all","전체"), ("same_day","종목·날짜 중복 제거"),
                                 ("nonoverlap","종목별 D0~D+20 비중첩"), ("gc0","D0 RSI 확인")]:
        for band in ("UPPER", "LOWER"):
            for year in (2023, 2024, "ALL"):
                g = selected[selected.cohort.eq(cohort) & selected.band.eq(band) & selected.year.eq(year)]
                lines += [f"### {cohort_label} · {band} · {year}", ""] + policy_table(g) + [""]
                a = g[g.policy.eq("A")].iloc[0]
                lines += [f"D+20 당일 고가 평균 {fmt(a.mean_d20_high_vs_d0_pct)}%, "
                          f"D+1~D+20 기간 최고가 평균 {fmt(a.mean_path_high_vs_d0_pct)}% "
                          f"(D0 종가 대비, 공통 기회).", ""]
    lines += ["## 프로파일 근사 민감도", "",
              "| 기간 | 배분 | 상단 커버리지 | 보류 | A | B | C | D |",
              "|---:|---|---:|---:|---:|---:|---:|---:|"]
    sens = summary[summary.cohort.eq("all") & summary.band.eq("UPPER") & summary.year.eq("ALL")]
    for (n, m), g in sens.groupby(["window", "method"]):
        g = g.set_index("policy")
        lines += [f"| {n} | {m} | {g.loc['A','coverage']} | {g.loc['A','blocked']} | "
                  + " | ".join(fmt(g.loc[p,"net_opportunity_pct"])+"%" for p in "ABCD") + " |"]
    lines += ["", "## 실제 차트와 반례", ""] + case_lines
    lines += ["", "[실제 일봉 갤러리](GALLERY_KO.html) · [정책 전체 집계](SUMMARY.csv) · "
              "[대조군 집계](CONTROLS.csv) · [규칙 및 지표 정의](PROTOCOL_KO.md)", "",
              "기회당 순수익은 미진입을 0으로 처리한다. 놓친 상승은 미진입 사건에서 D+1 시가 대비 "
              "D+20까지 고가가 +10% 또는 +20%에 도달한 횟수다. 손절 후 가격도 포함되는 경로 지표다. "
              "MDD는 개별 거래 진입가, 청산 전 종가, 실제 청산가 경로로 계산한다. "
              "포트폴리오 자금 배분과 거래 중 고가·저가 발생 순서는 모델링 범위 밖이다.", ""]
    (out/"REPORT_KO.md").write_text("\n".join(lines), encoding="utf-8")
