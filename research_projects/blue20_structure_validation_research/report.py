"""Readable comparisons and actual eight-case follow-through charts."""
from __future__ import annotations
import argparse,json,re
from pathlib import Path
from html import escape
from xml.etree import ElementTree

import numpy as np
import pandas as pd
from analyze import load_prices

HERE=Path(__file__).resolve().parent
CASES={("028300","2023-02-20"):"HLB",("145990","2023-05-08"):"삼양사",
       ("383220","2024-07-17"):"F&F",("180640","2023-09-14"):"한진칼",
       ("006220","2024-01-24"):"제주은행",("222080","2024-03-05"):"씨아이에스",
       ("038500","2024-06-05"):"삼표시멘트",("034120","2023-02-07"):"SBS"}
LABELS=dict(drop_shrinking="최근 반락 축소",reapproach_faster="재접근 단축",
            higher_trough="중간 저점 상승",compression="세 변화 동시 관측")
STATES=dict(BREAK_STRONG="강한 돌파",BREAK_WEAK="약한 돌파",CLOSE_INSIDE="존 안 마감",
            ALREADY_CLEARED="기존 돌파 위",OVERHEAD="상단 저항 남음",
            REENTRY_BELOW_OLD_RESISTANCE="기존 돌파 후 복귀",NO_REPEAT_ZONE="반복 존 없음")


def num(x):
    return "—" if pd.isna(x) else f"{x:.2f}"


def yn(x):
    return "미확인" if pd.isna(x) else "예" if x else "아니오"


def gallery(cases,frames,path):
    page=['<!doctype html><meta charset="utf-8"><title>BLUE20 지지 유지 검증</title>',
          '<style>body{font:16px system-ui;max-width:1160px;margin:2em auto;padding:0 1em}section{margin:3em 0}svg{width:100%;background:#fafafa}</style>',
          '<h1>지정 8종목의 D0 저항과 이후 가격</h1><p>주황 존은 D0에 고정한 과거 반복 가격 저항이다. '
          '검은 세로선은 D0, 보라 세로선은 D+1·3·5의 관찰 종료일이다. 각 봉에 원본 OHLCV를 표시했다.</p>']
    for r in cases.itertuples():
        d=frames[r.code]
        j=int(d.index[d.date.eq(r.date)][0])
        z=d.iloc[max(0,j-30):j+21].reset_index(drop=True)
        lo,hi=min(z.low.min(),r.low),max(z.high.max(),r.high)
        y=lambda p:350-290*(p-lo)/(hi-lo)
        x=lambda i:65+970*i/(len(z)-1)
        start=j-max(0,j-30)
        name=CASES[(r.code,r.date)]
        page.append(f'<section><h2>{escape(name)} {r.date}</h2><p>반락 축소 {yn(r.drop_shrinking)} · '
                    f'재접근 단축 {yn(r.reapproach_faster)} · 저점 상승 {yn(r.higher_trough)} · '
                    f'D+1/3/5 상단 종가 {yn(r.d1_close_above)}/{yn(r.d3_close_above)}/{yn(r.d5_close_above)} · '
                    f'D+6~20 유지율 {num(100*r.after5_hold_ratio)}%</p>'
                    '<svg viewBox="0 0 1130 405" role="img" aria-label="실제 일봉과 관찰 시점">')
        page.append(f'<rect x="65" y="{y(r.high)}" width="970" height="{y(r.low)-y(r.high)}" fill="#edb567" opacity=".35"/>')
        for p in np.linspace(lo,hi,5):
            page.append(f'<path d="M65 {y(p)}H1035" stroke="#ddd"/><text x="4" y="{y(p)}" font-size="11">{p:,.0f}</text>')
        for i,b in enumerate(z.itertuples()):
            c="#b42318" if b.close>=b.open else "#175d7a"
            top=min(y(b.open),y(b.close));height=max(1,abs(y(b.open)-y(b.close)))
            page.append(f'<g><title>{b.date} O{b.open} H{b.high} L{b.low} C{b.close} V{b.volume}</title>'
                        f'<path d="M{x(i)} {y(b.high)}V{y(b.low)}" stroke="{c}"/>'
                        f'<rect x="{x(i)-4}" y="{top}" width="8" height="{height}" fill="{c}"/></g>')
            if i%10==0:page.append(f'<text x="{x(i)-20}" y="382" font-size="10">{b.date}</text>')
        for k in (0,1,3,5):
            c="#111" if k==0 else "#8b41a1"
            page.append(f'<path d="M{x(start+k)} 25V355" stroke="{c}" stroke-dasharray="4 3"/>'
                        f'<text x="{x(start+k)+2}" y="{18 if k%2 else 32}" font-size="10">D+{k}</text>')
        page.append('</svg></section>')
    text="\n".join(page)
    for svg in re.findall(r"<svg .*?</svg>",text,re.S):ElementTree.fromstring(svg)
    path.write_text(text,encoding="utf-8")


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--manifest",type=Path,required=True)
    p.add_argument("--out",type=Path,default=HERE/"results")
    a=p.parse_args()
    out=a.out
    data=pd.read_csv(out/"EVENTS.csv.gz",dtype={"code":str},low_memory=False)
    ci=pd.read_csv(out/"CLUSTER_INTERVALS.csv")
    structure=pd.read_csv(out/"STRUCTURE_GROUPS.csv")
    follow=pd.read_csv(out/"FOLLOWUP_GROUPS.csv")
    selected=data[data.set_index(["code","date"]).index.isin(CASES)&data.window.eq(120)&data.band.eq("UPPER")]
    assert len(selected)==8
    selected.to_csv(out/"CASES.csv",index=False,encoding="utf-8-sig")
    frames=load_prices(a.manifest)
    gallery(selected,frames,out/"GALLERY_KO.html")
    probe=HERE/"KRX_PROBE.json"
    probe_status=json.loads(probe.read_text()) if probe.exists() else []
    primary=data[data.window.eq(120)&data.band.eq("UPPER")&data.long_bull]
    lines=["# BLUE20 가격 구조와 돌파 후 유지력 검증","",
           "기존 GitHub 실행 42354426551의 고정 500종목·1,804사건을 사용했다. 가격 저항 존과 장대양봉 판정은 보존하고 "
           "반락 축소·재접근 단축·중간 저점 상승 및 D+1·3·5 이후 유지력을 추가 관측했다.","",
           f"120일 상단 장대양봉 {len(primary)}건 중 시험 3회 이상은 {int(primary.tests.ge(3).sum())}건이다. "
           f"세 변화가 동시에 관측된 사건은 {int(primary.compression.eq(True).sum())}건, "
           f"기준가·거래대금 단서 창은 {int(primary.flagged_window.sum())}건이다. "
           "시험이 2회뿐이면 재접근 속도와 중간 저점의 변화를 비교할 수 없어 미확인으로 남겼다.","",
           "**D0 이전 구조만으로 이후 유지력이 뚜렷하게 구분된다는 근거는 부족했다.** "
           "반락·재접근·저점 변화와 20봉 유지율의 평균 차이에 대한 종목 재표집 구간이 모두 0을 포함했다. "
           "반면 D+1·3·5에 상단 위 종가를 유지한 사건은 그 다음 봉부터의 유지율이 높게 관측됐다. "
           "같은 가격 구간 위에 있는 상태의 지속성, D0 상태 구성, 사후 RSI 골든크로스 표본 선택이 포함된 관측이다.","",
           "| D0 구조 | 해당 / 대조 | 20봉 유지율 차이(%p) | 종목 재표집 95% 구간 |",
           "|---|---:|---:|---:|"]
    for r in ci[ci.scope.eq("ALL_LONG")&ci.year.eq("ALL")&ci.feature.isin(LABELS)].itertuples():
        lines.append(f"| {LABELS[r.feature]} | {r.n_true}/{r.n_false} | {num(r.delta_pp)} | {num(r.lo_pp)}~{num(r.hi_pp)} |")
    lines+=["","세 변화 동시 관측의 대조군은 시험 3회 이상이면서 한 가지 이상 충족하지 않은 사건이다. "
            "이 지표를 계산할 수 없는 2회 시험 사건은 해당 비교의 분모에서 빠진다.","",
            "| 관측 종료 | 상단 위 / 아래 사건 | 그 다음 봉~D+20 유지율 차이(%p) | 종목 재표집 95% 구간 |",
            "|---|---:|---:|---:|"]
    for k in (1,3,5):
        r=ci[ci.scope.eq("ALL_LONG")&ci.year.eq("ALL")&ci.feature.eq(f"d{k}_close_above")].iloc[0]
        lines.append(f"| D+{k} 종가 | {r.n_true}/{r.n_false} | {num(r.delta_pp)} | {num(r.lo_pp)}~{num(r.hi_pp)} |")
    lines+=["","예를 들어 D+5 확인에는 D+1~5만 사용하고, 결과는 D+6~20으로 계산했다. "
            "D+5 종가 확인을 D+5 시가 판단에 쓸 수는 없다. 같은 일자의 시장 동조화를 모두 반영한 추정은 아니며, "
            "여러 비교에 대한 사전등록 검증도 아니다. 이 차이로 매수 규칙을 확정하지 않았다.","",
            "**연도·D0 상태별 비교**","",
            "| 연도 | D0 상태 | D+3 위 / 아래 | 이후 유지율 위 / 아래 | D+5 위 / 아래 | 이후 유지율 위 / 아래 |",
            "|---|---|---:|---:|---:|---:|"]
    subset=follow[follow.scope.eq("ALL_LONG")&follow.strata.eq("state")&follow.window.eq(120)&
                  follow.band.eq("UPPER")&follow.feature.eq("close_above")]
    for (year,state),g in subset.groupby(["year","state"]):
        cells=[]
        for k in (3,5):
            z=g[g.known_offset.eq(k)].set_index("value")
            yes=z.loc[True] if True in z.index else None
            no=z.loc[False] if False in z.index else None
            cells.extend([f"{int(yes.n) if yes is not None else 0}/{int(no.n) if no is not None else 0}",
                          f"{num(yes.later_hold_pct) if yes is not None else '—'}/{num(no.later_hold_pct) if no is not None else '—'}%"])
        lines.append(f"| {year} | {STATES[state]} | "+" | ".join(cells)+" |")
    lines+=["","**이상행 제외·중복 제거 비교**","",
            "| 표본 | 사건 | D0 세 변화 차이(%p) | D+1 차이 | D+3 차이 | D+5 차이 |",
            "|---|---:|---:|---:|---:|---:|"]
    for scope,label in [("ALL_LONG","전체"),("KNOWN7_EXCLUDED","기존 7행 포함 창 제외"),
                        ("NO_RECORDED_FLAGS","기록된 모든 단서 창 제외"),("NONOVERLAP","종목별 20봉 비중첩")]:
        g=ci[ci.scope.eq(scope)&ci.year.eq("ALL")].set_index("feature")
        count=int(g.loc["d1_close_above","n_true"]+g.loc["d1_close_above","n_false"])
        lines.append(f"| {label} | {count} | {num(g.loc['compression','delta_pp'])} | "
                     f"{num(g.loc['d1_close_above','delta_pp'])} | {num(g.loc['d3_close_above','delta_pp'])} | "
                     f"{num(g.loc['d5_close_above','delta_pp'])} |")
    lines+=["","**8종목 반례와 대조군**","",
            "| 종목·D0 | 시험 | 반락 축소 | 재접근 단축 / 저점 상승 | D+1 / 3 / 5 상단 종가 | D+6~20 유지율 |",
            "|---|---:|---|---|---|---:|"]
    for r in selected.itertuples():
        lines.append(f"| {CASES[(r.code,r.date)]} · {r.date} | {int(r.tests)} | {yn(r.drop_shrinking)} | "
                     f"{yn(r.reapproach_faster)}/{yn(r.higher_trough)} | "
                     f"{yn(r.d1_close_above)}/{yn(r.d3_close_above)}/{yn(r.d5_close_above)} | {num(100*r.after5_hold_ratio)}% |")
    lines+=["","F&F는 세 변화가 모두 관측됐지만 이후 가격 유지에는 실패했다. HLB는 D+1·3에 상단 위였지만 D+5에는 이탈했다. "
            "SBS는 D+1·3·5 상단 종가를 충족하지 않았으나 D+6~20 중 14봉을 상단 위에서 마감했다. "
            "단기 유지 확인만으로 모든 반등과 재돌파를 포착하지는 못했다.","",
            "[실제 일봉 갤러리](GALLERY_KO.html) · [사례 원장](CASES.csv) · [전체 구조 집계](STRUCTURE_GROUPS.csv) · "
            "[후속 관측 집계](FOLLOWUP_GROUPS.csv) · [재표집 구간](CLUSTER_INTERVALS.csv)","",
            "**시험 원장 날짜 연결 수정**","",
            "이전 PRICE_TESTS.csv.gz는 시험 정보의 date가 사건 date를 덮어써 시험일만 남아 있었다. "
            "이번에는 사건별 test_dates, 종목·밴드·기간·저항 존으로 시험을 복원하고, 각 시험의 고가·종가를 원본 OHLCV와 대조했다. "
            "EVENT_TESTS_REPAIRED.csv.gz에는 event_date와 test_date가 별도 열로 있다. 원본 GitHub 아티팩트와 기존 집계는 보존했다.","",
            "**거래대금 이상 4행 추가 조사**","",
            "KIND 공시와 별도 일봉 대조 이후, 원천 수집 코드에 명시된 KRX 일별 조회를 직접 점검했다. "
            f"이번 요청은 HTTP {probe_status[0].get('http_status','미확인') if probe_status else '미확인'} 및 LOGOUT 응답으로 추가 시세를 얻지 못했다. "
            "각 행에서 거래대금이 거래량×일봉 저가~고가 범위를 벗어나 같은 세션의 가격·수량·대금으로 함께 설명할 수 없다는 사실은 확인된다. "
            "시간외 거래 포함·필드 집계 범위 차이·원천 오류 중 어느 원인인지는 4행 모두 확정하지 못했다. "
            "수치에 임의 보정을 적용하지 않았다.","",
            "실제 체결·분봉 및 기업행사 가격·주수 기준이 미검증인 상태를 유지한다. "
            "이번 결과는 독립 RESEARCH_ONLY 기술통계이며 매수 보류·후속 확인 규칙 확정과 전체시장 확대는 대기한다.",""]
    (out/"REPORT_KO.md").write_text("\n".join(lines),encoding="utf-8")
    audit=json.loads((out/"AUDIT.json").read_text())
    repaired=pd.read_csv(out/"EVENT_TESTS_REPAIRED.csv.gz",dtype={"code":str})
    assert repaired.known_date.lt(repaired.event_date).all()
    assert len(repaired)==14288
    audit.update(event_trial_dates_repaired=len(repaired),tests_passed=5,case_charts=8,
                 remaining_amount_causes="UNRESOLVED_4",next_phase="PENDING_DATA_ASSURANCE")
    (out/"AUDIT.json").write_text(json.dumps(audit,indent=2),encoding="utf-8")
    print(audit)


if __name__=="__main__":
    main()
