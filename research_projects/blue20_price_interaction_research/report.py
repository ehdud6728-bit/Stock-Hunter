"""Create descriptive Korean tables from the frozen interaction study."""
import json,re
from pathlib import Path
from xml.etree import ElementTree
import pandas as pd

HERE=Path(__file__).resolve().parent
OUT=HERE/"results"
LABELS=dict(OVERHEAD="현재 위 저항",CLOSE_INSIDE="존 안 마감",BREAK_WEAK="이번 약한 돌파",
            BREAK_STRONG="이번 강한 돌파",ALREADY_CLEARED="기존 돌파 위 유지",
            REENTRY_BELOW_OLD_RESISTANCE="기존 돌파 후 존 복귀",NO_REPEAT_ZONE="반복 존 없음")
NAMES={"028300":"HLB","145990":"삼양사","383220":"F&F","180640":"한진칼",
       "006220":"제주은행","222080":"씨아이에스","038500":"삼표시멘트","034120":"SBS"}


def fmt(v):
    return "—" if pd.isna(v) else f"{v:,.2f}"


def run():
    r=pd.read_csv(OUT/"EVENTS.csv.gz",dtype={"code":str},low_memory=False)
    groups=pd.read_csv(OUT/"GROUPS.csv")
    cases=pd.read_csv(OUT/"CASES.csv",dtype={"code":str})
    a=json.loads((OUT/"AUDIT.json").read_text())
    assert len(r)==1804*3
    assert not r.duplicated(["code","date","band","window"]).any()
    with_z=r[r.state.ne("NO_REPEAT_ZONE")]
    for c in with_z.itertuples():
        assert all(t<c.date for t in c.test_dates.split("|"))
        if c.state in ("BREAK_STRONG","BREAK_WEAK","ALREADY_CLEARED"):
            assert c.d0_close>c.high
        if c.state in ("OVERHEAD","CLOSE_INSIDE"):
            assert c.d0_close<=c.high
    text=(OUT/"GALLERY_KO.html").read_text(encoding="utf-8")
    svgs=re.findall(r"<svg .*?</svg>",text,re.S)
    assert len(svgs)==8
    for svg in svgs:ElementTree.fromstring(svg)
    primary=r[r.window.eq(120)&r.band.eq("UPPER")]
    large=primary[primary.long_bull]
    concentration=large[large.state.ne("NO_REPEAT_ZONE")]
    lines=["# BLUE20 반복 고점 저항과 장대양봉 상호작용","",
           "가격 고점·반락으로 만든 반복 저항과 일봉 추정 거래량 집중 구간을 독립적으로 계산했다. "
           "2022년 말 고정 500종목의 기존 상단 834건·하단 970건을 보존한 기술통계 연구다. "
           "매수 보류·재진입 규칙이나 성과 문턱을 탐색하지 않았다.","",
           f"120일 주 분석에서 상단 834건 중 장대양봉은 {len(large)}건, 그중 관련 반복 가격 저항이 선택된 사건은 "
           f"{len(concentration)}건이었다. 이 중 최대 추정 거래량 구간과 실제 가격 저항 존이 겹치지 않은 사건은 "
           f"{int(concentration.density_overlap.eq(0).sum())}건이다. 거래량 집중과 가격 반락은 서로 다른 특성을 나타낸다.","",
           "## 원천 이상행 조사 진전","",
           "HLB 2022-10-13은 유상증자 권리락 기준가 40,200원, 2022-12-28은 주식배당 배당락 기준가 30,250원으로 공식 공시와 일치했다. "
           "한진칼 2022-12-06은 시간외대량매매 공시를 전체 거래량·대금에서 빼면 잔여 평균이 당일 OHLC 범위 안에 들어왔다. "
           "다른 한진칼 3행과 삼표시멘트 1행은 별도 공개 일봉에서 OHLCV가 일치했으나 정확한 세션별 구성은 확인되지 않았다. "
           "[행별 원인과 원문](ANOMALIES_KO.md) · [증거 원장](ANOMALY_CAUSES.csv)","",
           "## 사전에 고정한 가격 저항 규칙","",
           "뒤 3봉 종가까지 확인된 고점만 사용한다. 고점 전후 2봉 조건으로 가격 꼭지를 찾고, 한 고점의 가격 ±0.5 당시 사전 ATR을 존으로 고정한다. "
           "동일 구간에서 5봉 이상 간격의 실패 시험 2회 이상을 요구한다. 다음 3봉에 상단 위 종가가 없어야 하고, 하단 아래 종가와 고가 대비 0.5 ATR 이상의 반락이 있어야 한다. "
           "과거 시험일·확인일·반락 종가와 시험 간 저점은 모두 원장에 남긴다. D0의 몸통에 가까운 존을 선택하므로 이번 캔들에 관련된 사후 설명 변수다. "
           "위에 남은 가장 가까운 후보 존은 별도 열로 저장한다.","",
           "상단 종가 초과 폭이 0.5 사전 ATR 미만이면 약한 돌파, 이상이면 강한 돌파로 표시한다. "
           "직전 종가가 이미 상단 위이면 기존 돌파 상태다. 장대양봉은 몸통 2 사전 ATR 이상이다. "
           "[전체 규칙](../PROTOCOL_KO.md)","",
           "## 상단 장대양봉의 상태별 관측","",
           "가격 유지율은 D+1~D+20 중 저항 상단 위에 마감한 종가의 비율이다. 원래 아래에 있던 사건과 돌파 사건의 상태 차이를 포함하므로 예측 성과로 해석하지 않는다.","",
           "| 연도 | 상태 | 사건 | 반락 ATR 중앙값 | 재접근 봉수 중앙값 | 저점 상승 | 거래대금 배율 평균 | 전진 효율 평균 | 윗꼬리 평균 | 20봉 유지율 평균 |",
           "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    main=groups[groups.scope.eq("LONG_BULL")&groups.window.eq(120)&groups.band.eq("UPPER")]
    for g in main.itertuples():
        lines.append(f"| {g.year} | {LABELS[g.state]} | {g.n} | {fmt(g.median_drop_atr)} | {fmt(g.median_reapproach)} | "
                     f"{g.rising_troughs} | {fmt(g.mean_amount_ratio)} | {fmt(g.mean_efficiency)} | "
                     f"{fmt(100*g.mean_wick)}% | {fmt(100*g.mean_hold_ratio20)}% |")
    lines+=["","전진 효율은 저항 위 부호 있는 ATR 전진폭을 거래대금 배율로 나눈 값이다. "
            "원장에는 양수 전진폭만 사용한 효율도 있다. 반락 변화량이 음수이면 마지막 반락이 첫 반락보다 얕았고, "
            "재접근 간격 변화량이 음수이면 마지막 재접근이 빨라졌다. 저점 상승은 시험 사이 최저 저가가 연속해서 엄격히 높아졌을 때만 인정한다.","",
            "## 동일 규칙으로 본 지정 8종목","",
            "| 종목·D0 | 장대양봉 | 가격 저항 존 | 시험 | 반락 ATR | 재접근 | 저점 상승 | D0 상태 | 몸통 겹침 | 전진 ATR | D+5 / D+20 유지 |",
            "|---|---|---|---:|---:|---:|---|---|---:|---:|---|"]
    c120=cases[cases.window.eq(120)]
    for c in c120.itertuples():
        zone="—" if pd.isna(c.low) else fmt(c.low)+"~"+fmt(c.high)
        hold="—" if pd.isna(c.d5_hold) else f"{c.d5_hold} / {c.d20_hold}"
        lines.append(f"| {NAMES[c.code]} · {c.date} | {'예' if c.long_bull else '아니오'} | {zone} | "
                     f"{fmt(c.count)} | {fmt(c.median_drop_atr)} | {fmt(c.median_reapproach)} | "
                     f"{'예' if c.rising_troughs is True or c.rising_troughs==True else '아니오'} | {LABELS[c.state]} | "
                     f"{fmt(100*c.overlap)}% | {fmt(c.progress_atr)} | {hold} |")
    lines+=["","[실제 일봉 차트 8건](GALLERY_KO.html) · [8건 전체 수치](CASES.csv) · [과거 시험과 확인일](PRICE_TESTS.csv.gz)","",
            "## 기간·원천 단서 민감도","",
            "GROUPS에는 전체 일봉·장대양봉·알려진 기준가 및 거래대금 단서가 포함된 창을 제외한 장대양봉 집계가 있다. "
            "단서가 없는 창도 기업행사 가격·주수 기준이 완전 인증됐다는 뜻은 아니다. 기간은 60·120·240일, "
            "지정 사례의 군집 폭은 0.25·0.5·0.75 ATR로 비교했다. 가격 존이 바뀌면 상태도 달라질 수 있으므로 고정 저항선 하나로 단정하지 않는다. "
            "[범위·연도·상태별 집계](GROUPS.csv) · [사례 군집 폭 민감도](CASE_TOLERANCE_SENSITIVITY.csv)","",
            "## 진행 범위","",
            "실제 2022~2024년 분봉·틱 원장은 확보되지 않았다. 거래량 분포는 일봉 고저 구간 배분 추정치다. "
            "과거 가격 기준 및 거래 세션 인증이 남아 있어 전체 시장 확대와 매물대 기반 매매 판단은 대기한다. "
            "이번 연구는 독립 RESEARCH_ONLY이며 기존 500종목 기준선·LIVE·SEARCH·WATCH·R1-D118은 보존했다.",""]
    (OUT/"REPORT_KO.md").write_text("\n".join(lines),encoding="utf-8")
    a["structural_checks"]="PASS"
    a["case_svg_count"]=8
    a["original_reference_causes_confirmed"]=2
    a["amount_block_deal_corrobated"]=1
    a["remaining_amount_causes_unresolved"]=4
    (OUT/"AUDIT.json").write_text(json.dumps(a,indent=2),encoding="utf-8")
    print("Validated",len(r),"feature rows and",len(svgs),"charts")


if __name__=="__main__":
    run()
