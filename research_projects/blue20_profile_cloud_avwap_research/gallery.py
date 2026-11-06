"""Render standalone SVG OHLCV charts with a frozen pre-event profile."""
from html import escape

import numpy as np
import pandas as pd


def svg_chart(d, j, profile, facts, anchor_line):
    lo, hi = max(0, j-120), j+21
    bars = d.iloc[lo:hi]
    n = len(bars)
    x = lambda i: 72 + 7.2*(i-lo)
    series = {k: bars[k] for k in ("ma60", "ma112", "ma224", "span_a", "span_b")}
    series["AVWAP"] = anchor_line.reindex(bars.index)
    levels = {k: profile[k.lower()] for k in ("POC", "VAH", "VAL")}
    values = np.concatenate([bars.low, bars.high, *series.values(), list(levels.values())])
    low, high = np.nanmin(values), np.nanmax(values)
    margin = (high-low)*.045
    low, high = low-margin, high+margin
    y = lambda p: 490 - 396*(p-low)/(high-low)
    right = 72 + 7.2*n
    profile_x = right+26
    width = profile_x+250
    ratio = bars.amount_ratio20
    ratio_max = max(1, np.log10(1+ratio.max()))
    z = lambda r: 646 - 74*np.log10(1+r)/ratio_max
    name = escape(facts["name"])
    title = f'{name} {facts["code"]} · {facts["date"]} · {facts["baseline_exit_reason"]} {facts["baseline_net_pct"]:+.2f}%'
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} 710" role="img">',
        f"<title>{title}</title>",
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        '<g font-family="system-ui, sans-serif" fill="#18273a">',
        f'<text x="22" y="31" font-size="22" font-weight="700">{title}</text>',
        f'<text x="22" y="58" font-size="13">Archive OHLCV · D0 body/prior ATR {facts["d0_body_abs_prior_atr20"]:.2f}× · D0 turnover {facts["d0_amount_ratio20"]:.2f}× · AVWAP anchor {facts["avwap_anchor_date"]}</text>',
    ]
    for level in np.linspace(low, high, 7):
        yy = y(level)
        parts.append(f'<path d="M72 {yy:.2f}H{width-20}" stroke="#e7edf3"/>')
        parts.append(f'<text x="7" y="{yy+4:.2f}" font-size="11">{level:,.0f}</text>')
    # The visible cloud is computed using only data at least 26 bars earlier.
    for i in bars.index[:-1]:
        a0, b0 = d.loc[i, ["span_a", "span_b"]]
        a1, b1 = d.loc[i+1, ["span_a", "span_b"]]
        if np.isfinite([a0, b0, a1, b1]).all():
            fill = "#bde5d8" if a0 >= b0 else "#f4d0dc"
            points = " ".join(f"{x(k):.2f},{y(v):.2f}"
                              for k, v in [(i,a0), (i+1,a1), (i+1,b1), (i,b0)])
            parts.append(f'<polygon points="{points}" fill="{fill}"/>')
    for i, b in bars.iterrows():
        xx = x(i)
        color = "#c73954" if b.close >= b.open else "#276ab0"
        top = min(y(b.open), y(b.close))
        h = max(1, abs(y(b.open)-y(b.close)))
        parts.append(f'<path d="M{xx:.2f} {y(b.high):.2f}V{y(b.low):.2f}" stroke="{color}"/>')
        parts.append(f'<rect x="{xx-2.3:.2f}" y="{top:.2f}" width="4.6" height="{h:.2f}" fill="{color}"/>')
        if np.isfinite(ratio.loc[i]):
            zz = z(ratio.loc[i])
            parts.append(f'<rect x="{xx-2.3:.2f}" y="{zz:.2f}" width="4.6" height="{646-zz:.2f}" fill="{color}" opacity=".65"/>')
        if (i-lo) % 20 == 0:
            parts.append(f'<text x="{xx:.2f}" y="670" font-size="11">{b.date[5:]}</text>')
    palette = {"ma60":"#006f79", "ma112":"#6746ab", "ma224":"#38455a", "AVWAP":"#b96300"}
    for name, color in palette.items():
        coordinates = " ".join(f"{x(i):.2f},{y(v):.2f}" for i, v in series[name].dropna().items())
        parts.append(f'<polyline points="{coordinates}" fill="none" stroke="{color}" stroke-width="1.7"/>')
    colors = {"POC":"#a22b6d", "VAH":"#a45805", "VAL":"#a45805"}
    for name, level in levels.items():
        yy = y(level)
        parts.append(f'<path d="M{x(j):.2f} {yy:.2f}H{profile_x}" stroke="{colors[name]}" stroke-dasharray="6 4"/>')
    hvns = profile["hvn"]
    for center, vol in zip(profile["centers"], profile["weights"]):
        yy = y(center+profile["width"]/2)
        hh = y(center-profile["width"]/2)-yy
        w = 170*vol/profile["weights"].max()
        fill = "#a22b6d" if center == profile["poc"] else "#5665a6" if center in hvns else "#b8c6de"
        parts.append(f'<rect x="{profile_x}" y="{yy:.2f}" width="{w:.2f}" height="{max(0,hh-1):.2f}" fill="{fill}"/>')
    labels = []
    for name, level in sorted(levels.items(), key=lambda pair: -pair[1]):
        yy = y(level)
        label_y = max(yy, labels[-1]+15) if labels else yy
        labels.append(label_y)
        parts.append(f'<path d="M{profile_x+172} {yy:.2f}L{profile_x+179} {label_y:.2f}" stroke="{colors[name]}"/>')
        parts.append(f'<text x="{profile_x+180}" y="{label_y+4:.2f}" font-size="11" fill="{colors[name]}">{name} {level:,.0f}</text>')
    for offset, label, color in [(0,"D0","#14529f"), (1,"D1","#ae7506"), (2,"D2","#008363")]:
        xx = x(j+offset)
        parts.append(f'<path d="M{xx:.2f} 87V648" stroke="{color}" stroke-dasharray="4 4"/>')
        parts.append(f'<text x="{xx+24*offset-16:.2f}" y="{82-15*(offset%2)}" font-size="12" fill="{color}">{label}</text>')
    legend = ["MA60", "MA112", "MA224", "AVWAP"]
    for i, label in enumerate(legend):
        color = list(palette.values())[i]
        parts.append(f'<text x="{72+78*i}" y="522" font-size="13" fill="{color}">{label}</text>')
    parts += [
        '<text x="388" y="522" font-size="13">Ichimoku cloud 9 / 26 / 52 · +26</text>',
        f'<text x="{profile_x}" y="522" font-size="13">Volume profile · D−120 to D−1</text>',
        f'<text x="{profile_x}" y="545" font-size="12">Purple POC · Dark blue HVN</text>',
        '<text x="72" y="563" font-size="13">Turnover / prior 20 sessions · log scale</text>',
        '<text x="72" y="695" font-size="12">D1–D+20 bars show subsequent outcomes. The profile and AVWAP anchor were frozen before D0.</text>',
        "</g></svg>",
    ]
    return "\n".join(parts)


def write_gallery(out, charts, cases, anchor_avwap):
    folder = out / "charts"
    folder.mkdir(exist_ok=True)
    sections, facts = [], []
    for key, (name, expected) in cases.items():
        d, j, profile, event = charts[key]
        event = dict(event, name=name, case_label=expected)
        _, avwap = anchor_avwap(d.iloc[:j+21], j)
        stem = f"{key[0]}_{key[1]}"
        (folder / f"{stem}.svg").write_text(svg_chart(d, j, profile, event, avwap), encoding="utf-8")
        columns = ["date", "open", "high", "low", "close", "volume", "amount",
                   "ma60", "ma112", "ma224", "span_a", "span_b", "amount_ratio20"]
        d.iloc[max(0,j-120):j+21][columns].assign(avwap=avwap).to_csv(
            folder / f"{stem}_ohlcv.csv", index=False)
        facts.append(event)
        sections.append(
            f'<section><h2>{escape(name)} · {key[0]} · {key[1]}</h2>'
            f'<p>{escape(event["baseline_exit_reason"])} {event["baseline_net_pct"]:+.2f}% · '
            f'VAH {event["vp_vah"]:,.0f}원 · {event["vp_breakout"]} · '
            f'구름대 {event["cloud_position"]} · D1 지지 {event["d1_support"]} · '
            f'<a href="charts/{stem}_ohlcv.csv">원천 일봉과 표시 지표</a></p>'
            f'<img src="charts/{stem}.svg" alt="{escape(name)} 실제 일봉과 구조 지표" loading="lazy"></section>'
        )
    (out / "GALLERY_KO.html").write_text(
        '<!doctype html><html lang="ko"><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        '<title>BLUE20 매물대·구름대·AVWAP 사례</title>'
        '<style>body{margin:24px auto;padding:0 16px;max-width:1300px;background:#edf2f8;'
        'font:16px system-ui;color:#18273a}section{background:white;padding:18px;margin:18px 0;'
        'border-radius:12px}img{display:block;width:100%;height:auto}</style>'
        '<h1>BLUE20 매물대·구름대·AVWAP 실제 차트</h1>'
        '<p>KRX 파생 보관 일봉. 매물대는 D0 이전 120일 추정치이며, D1 이후 봉은 결과 확인용입니다. '
        '사례 8건은 기존 청산 결과로 고른 예시입니다.</p>' + "".join(sections) + "</html>",
        encoding="utf-8")
    pd.DataFrame(facts).to_csv(out / "CASES.csv", index=False)

