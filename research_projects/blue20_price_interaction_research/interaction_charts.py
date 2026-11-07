from html import escape
import numpy as np


def gallery(cases,path):
    page=['<!doctype html><meta charset="utf-8"><title>BLUE20 가격 저항 상호작용</title>',
          '<style>body{font:16px system-ui;max-width:1200px;margin:2em auto;padding:0 1em}section{margin:3em 0}svg{width:100%;background:#fafafa}</style>',
          '<h1>반복 고점 저항과 장대양봉</h1>',
          '<p>주황 존은 일봉 고점·반락으로만 계산한 반복 저항, 파랑 존은 최대 일봉 추정 거래량 구간이다. '
          '주황 점은 과거 시험 고점이다. 검은 세로선은 D0이며 그 오른쪽은 사후 관측이다.</p>']
    for name,d,r,p in cases:
        lo,hi=d.low.min(),d.high.max()
        y=lambda price:360-310*(price-lo)/(hi-lo)
        dx=840/max(1,len(d)-1)
        x=lambda i:60+i*dx
        page.append(f'<section><h2>{escape(name)} {r["code"]} · {r["date"]}</h2>'
                    f'<p>상태 {r["state"]} · 반복 {r.get("count",0)}회 · 장대양봉 {r["long_bull"]}'
                    f' · 기준가 단서 {r["basis_flags"]} · 거래대금 범위 단서 {r["amount_flags"]}</p>'
                    '<svg viewBox="0 0 1170 450" role="img" aria-label="과거 실제 고점 저항과 일봉 추정 거래량 분포">')
        for lower,upper,color in [(r["density_low"],r["density_high"],"#175cd3"),
                                  (r.get("low",np.nan),r.get("high",np.nan),"#c87608")]:
            if np.isfinite(lower):
                page.append(f'<rect x="60" y="{y(upper)}" width="840" height="{y(lower)-y(upper)}" fill="{color}" opacity=".18"/>')
        dates=r.get("test_dates","").split("|")
        for i,b in enumerate(d.itertuples()):
            color="#b42318" if b.close>=b.open else "#175d7a"
            top=y(max(b.close,b.open))
            ht=max(1,abs(y(b.close)-y(b.open)))
            page.append(f'<g><title>{b.date} O{b.open} H{b.high} L{b.low} C{b.close} V{b.volume}</title>'
                        f'<path d="M{x(i)} {y(b.high)}V{y(b.low)}" stroke="{color}"/>'
                        f'<rect x="{x(i)-2}" y="{top}" width="4" height="{ht}" fill="{color}"/>'
                        f'<path d="M{x(i)} 410 v{-40*b.volume/max(d.volume)}" stroke="{color}" opacity=".6"/></g>')
            if b.date in dates:
                page.append(f'<circle cx="{x(i)}" cy="{y(b.high)-5}" r="3" fill="#bd6900"/>')
            if b.date==r["date"]:
                page.append(f'<path d="M{x(i)} 30V415" stroke="#111" stroke-dasharray="4 4"/><text x="{x(i)+3}" y="24">D0</text>')
            if i%24==0:
                page.append(f'<text x="{x(i)}" y="436" font-size="10">{b.date}</text>')
        for center,weight in zip(p["centers"],p["weights"]):
            ht=310*p["width"]/(hi-lo)
            page.append(f'<rect x="924" y="{y(center)-ht/2}" width="{112*weight/max(p["weights"])}" height="{ht*.9}" fill="#698ba8"/>')
        for label in ("poc","vah","val"):
            py=y(p[label])
            page.append(f'<path d="M60 {py}H1040" stroke="#64748b" stroke-dasharray="2 6"/>'
                        f'<text x="1043" y="{py}" font-size="11">{label.upper()} {p[label]:,.0f}</text>')
        page.append('</svg></section>')
    path.write_text("\n".join(page),encoding="utf-8")
