"""Evidence ledger for every previously identified source anomaly."""
from pathlib import Path
import json,re,hashlib,gzip
from html import unescape
import pandas as pd

HERE=Path(__file__).resolve().parent
OUT=HERE/"results"
OUT.mkdir(exist_ok=True)


def run():
    docs={d["receipt"]:d for d in json.loads((HERE/"DOCUMENTS.json").read_text())}
    def text(rid):
        raw=(HERE/"evidence"/(rid+"_body.htm")).read_bytes()
        assert hashlib.sha256(raw).hexdigest()==docs[rid]["sha256"]
        try:body=raw.decode("utf-8")
        except UnicodeDecodeError:body=raw.decode("euc-kr")
        body=re.sub(r"<(script|style)\b.*?</\1>"," ",body,flags=re.S|re.I)
        return re.sub(r"\s+"," ",unescape(re.sub("<[^>]+>"," ",body)))
    assert "40,200" in text("20221012000523") and "2022-10-13" in text("20221012000523") and "유상증자" in text("20221012000523")
    assert "30,250" in text("20221227000903") and "2022-12-28" in text("20221227000903") and "주식배당" in text("20221227000903")
    assert "3,338,090" in text("20221206000422") and "125,896,064,350" in text("20221206000422")
    assert "시간외대량매매" in text("20221206000422")
    source=HERE/"reference"
    flags=pd.read_csv(source/"AMOUNT_SCOPE_FLAGS.csv",dtype={"code":str})
    basis=pd.read_csv(source/"REFERENCE_PRICE_FLAGS.csv",dtype={"code":str})
    rows=[]
    for b in basis.itertuples():
        rid="20221012000523" if b.day=="2022-10-13" else "20221227000903"
        cause="RIGHTS_EX_REFERENCE" if b.day=="2022-10-13" else "STOCK_DIVIDEND_EX_REFERENCE"
        assert b.reference==(40200 if b.day=="2022-10-13" else 30250)
        rows.append(dict(code=b.code,date=b.day,flag="REFERENCE",status="CAUSE_CONFIRMED",
                         cause=cause,source_url=docs[rid]["url"],source_sha256=docs[rid]["sha256"],
                         previous_close=b.previous,reference=b.reference,
                         close=b.close,reported_change=b.reported_change))
    for b in flags.itertuples():
        raw_xml=gzip.decompress((HERE/"evidence"/f"naver_{b.code}.xml.gz").read_bytes())
        xml=raw_xml.decode("euc-kr")
        values=re.search(r'data="'+b.date.replace("-","")+r'\|([^"]+)"',xml)
        assert values
        prices=list(map(float,values[1].split("|")))
        assert prices==[b.open,b.high,b.low,b.close,b.volume]
        result=dict(code=b.code,date=b.date,flag="AMOUNT_SCOPE",volume=b.volume,amount=b.amount,
                    low=b.low,high=b.high,amount_per_share=b.amount/b.volume,
                    second_source_ohlcv_match=True,
                    source_url=f"https://fchart.stock.naver.com/sise.nhn?symbol={b.code}&timeframe=day&count=2000&requestType=0",
                    source_sha256=hashlib.sha256(raw_xml).hexdigest(),
                    status="UNRESOLVED_SESSION_SCOPE",cause="MIXED_SESSION_OR_FIELD_SCOPE_NOT_ESTABLISHED")
        if b.code=="180640" and b.date=="2022-12-06":
            remaining_vol=b.volume-3338090
            remaining_amount=b.amount-125896064350
            remaining_price=remaining_amount/remaining_vol
            assert b.low<=remaining_price<=b.high
            result.update(status="BLOCK_DEAL_CORROBORATED_AGGREGATION_UNVERIFIED",
                          cause="DISCLOSED_AFTER_HOURS_BLOCK_DEAL",
                          official_url=docs["20221206000422"]["url"],
                          official_sha256=docs["20221206000422"]["sha256"],
                          disclosed_shares=3338090,disclosed_amount=125896064350,
                          residual_volume=remaining_vol,residual_amount=remaining_amount,
                          residual_average=remaining_price)
        rows.append(result)
    pd.DataFrame(rows).to_csv(OUT/"ANOMALY_CAUSES.csv",index=False,encoding="utf-8-sig")
    lines=["# 기준가·거래대금 원인 조사","",
           "기준가 2행은 공시의 실시일·금액으로 원인을 확인했다. 거래대금 5행은 별도 공개 일봉의 OHLCV와 일치했다. "
           "시간외 거래가 섞였다는 공시 근거는 한진칼 12월 6일에서 확보했고, 나머지 4행의 정확한 세션별 구성은 미확인이다.","",
           "| 종목·일자 | 조사 결과 | 근거 |","|---|---|---|",
           f"| HLB 2022-10-13 | 유상증자 권리락, 기준가 40,200원 확인 | [KIND 원문]({docs['20221012000523']['url']}) |",
           f"| HLB 2022-12-28 | 주식배당 배당락, 기준가 30,250원 확인 | [KIND 원문]({docs['20221227000903']['url']}) |",
           f"| 한진칼 2022-12-06 | 시간외대량매매 3,338,090주·125,896,064,350원 공시 확인 | [팬오션 취득 공시]({docs['20221206000422']['url']}) |",
           "| 한진칼 2022-11-08·11-11·11-16 | 공개 일봉과 OHLCV 일치. 거래대금의 세션 구성은 미확인 | evidence/naver_180640.xml |",
           "| 삼표시멘트 2024-05-17 | 공개 일봉과 OHLCV 일치. 거래대금의 세션 구성은 미확인 | evidence/naver_038500.xml |","",
           "한진칼 12월 6일 전체 수량·대금에서 공시 거래를 빼면 잔여 49,509주·1,979,690,950원, 평균 "
           f"{remaining_price:,.4f}원이다. 이는 당일 저가 39,450~고가 40,500원 범위 안이다. "
           "전체 수량·대금에 시간외대량매매가 포함되고 OHLC는 다른 세션일 가능성과 수치가 일치한다. "
           "공급처의 필드별 세션 명세와 체결 자료를 확보한 확정 판정은 남아 있다.","",
           "HLB 공시는 해당 두 날짜의 기준가 변경 이유를 설명한다. 과거 모든 가격과 거래량을 어떤 기준으로 변환해야 하는지까지 인증하지 않는다. "
           "권리락과 주식배당을 같은 계수로 가격·거래량에 일괄 적용하지 않았다.","",
           "실제 2022~2024년 분봉·틱 파일은 확보하지 못했다. 한국투자증권 공식 분봉 예제는 최대 1년 보관을 명시하며, "
           "코스콤 DATAMALL은 과거 Tick·1분 데이터 제공을 안내한다. 이번 결과는 일봉 추정치이다. "
           "[한국투자증권 예제](https://raw.githubusercontent.com/koreainvestment/open-trading-api/main/examples_llm/domestic_stock/inquire_time_dailychartprice/inquire_time_dailychartprice.py) · "
           "[코스콤 안내](https://www.koscom.co.kr/portal/main/contents.do?menuNo=200580)"]
    (OUT/"ANOMALIES_KO.md").write_text("\n".join(lines),encoding="utf-8")


if __name__=="__main__":
    run()
