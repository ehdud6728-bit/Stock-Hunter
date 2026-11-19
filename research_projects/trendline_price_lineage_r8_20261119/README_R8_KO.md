# R8 · 삼성SDI 2023 추세선 수정주가 계보 독립 감사 (RESEARCH_ONLY)

## 결론: 원인 후보 발견, 역사적 수정주가 **아직 미확인**

- 기존 R5의 **원본** 데이터/코드 재실행으로는 동일선 `TS_BODY_WICK_PROXY|BEAR_BODY_TOP|2023-09-19|2023-10-18|UPPER`가 2023-11-02·03에 활성화되고 11-06에 그 직전 선을 돌파한 것이 확인됨. 이 사실은 R8에서 바꾸지 않음.
- 보존 원본 2023-11-06 OHLC: **478,000 / 522,000 / 465,500 / 506,000원**. 사용자 HTS 화면: **467,962 / 511,038 / 455,725 / 495,374원**. 각 가격은 원본×0.979로 **±0.5원** 차이 내 설명됨.
- **새로운 독립 근거:** 삼성SDI 보통주는 **2025-04-10 유상증자 권리락**, 기준가격 **168,100원**. 전 거래일(2025-04-09) 종가 **171,700원**. `168100 / 171700 = 0.979033197437...`, 세 자리 반올림 시 **0.979**. 관측된 화면 계수에 대한 **강력한 후보**이지만, 2023년 전체 기간에 적용된 실제 HTS 보정 산식/반올림과는 다른 문제임.
- **정밀 차이:** 위 실제 기준가격 비율을 2023-11-06 원본 4가격에 적용해 원 단위 반올림하면 HTS 화면과 **15~17원 차이** 발생. 따라서 0.979033을 그대로 썼다고 단정할 수 없음. HTS가 0.979로 내부 계수를 절사/반올림했는지도 확인되지 않음.
- 2023-09-19/10-18 두 앵커와 11-02/03/06에 대한 **외부 공급자의 원주가·수정주가 5일치가 아직 없음**. 현재 출력 상태는 `UNKNOWN_MISSING_BOTH_KIS_SERIES`.

### 1차 문서·대조 자료

1. [유상증자 권리락 기준가격 안내 — 증권사 보관 KRX 공시](https://invest.kiwoom.com/inv/disclosure_detail?code=01&date=2025.04.09&keyword=%EC%82%BC%EC%84%B1SDI&seq=00036095&stockCode=&tp=&type=02) — 보통주 168,100원, 2025-04-10.
2. [KRX/DART 삼성SDI 2025-04-09 증권신고서](https://kind.krx.co.kr/external/2025/04/09/000225/20250409000671/10001.htm) — 보통주 11,821,000주 유상증자 신고.
3. [2025-04-09 장마감 시세표](https://dreago.kr/board/view/?gn=0&no=37347&sgn=3) — 삼성SDI 종가 171,700원 (이 자료 자체는 거래소의 원시 API 응답이 아님; *재확인 대상*).
4. [한국투자증권 공식 기간별 시세 API 샘플](https://github.com/koreainvestment/open-trading-api/blob/main/examples_llm/domestic_stock/inquire_daily_itemchartprice/inquire_daily_itemchartprice.py) — 날짜 범위 지정 과거 일봉, `FID_ORG_ADJ_PRC` **0=수정주가, 1=원주가**, 최대 100개 행/호출.
5. **중요한 API 함정:** 한국투자증권의 [별도 최근 30일 API](https://github.com/koreainvestment/open-trading-api/blob/main/examples_llm/domestic_stock/inquire_daily_price/inquire_daily_price.py)는 `0=수정주가미반영, 1=반영`으로 **반대**라고 표기됨. 서로 다른 엔드포인트의 파라미터 해석을 혼합하지 말 것.

## 재현 (외부 가격 없이 사전 점검)

```bash
python -m unittest discover -s research_projects/trendline_price_lineage_r8_20261119 -p 'test_*.py' -v
python research_projects/trendline_price_lineage_r8_20261119/r8_price_lineage.py \
  --out research_projects/trendline_price_lineage_r8_20261119/results/R8_PRECHECK.json
```

외부 파일 없이 생성된 `R8_PRECHECK.json`은 **원인 후보 산식만 기록**하고 독립 검증 결과를 `UNKNOWN`으로 남긴다. 유닛 테스트의 가상 수정주가는 테스트 픽스처이며 관측 자료가 **아니다**.

## 독립 원주가·수정주가 데이터를 얻으면

같은 KIS 과거 기간별 시세 API (`/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice`)에서 삼성SDI `006400`, 일봉 `D`, 날짜 2023-09-19~11-06을 **두 번** 조회한다. 원주가 요청은 `fid_org_adj_prc="1"`, 수정주가 요청은 `"0"`. 응답과 요청 파라미터·조회 시각을 보관한다. 본 R8은 KIS 인증키를 사용하거나 다른 프로젝트의 자격증명을 복사하지 않는다.

KIS 응답의 날짜·시가·고가·저가·종가를 독립 CSV에 변환한다. 각 CSV는 다음 스키마를 요구한다.

```csv
code,date,open,high,low,close
006400,2023-09-19,......
```

`......`은 **예시 자리표시자**다. 임의 데이터로 채우지 않는다. 가격은 원 단위 정수. 5개 날짜가 모두 포함되어야 한다. KIS 공급자 매니페스트 JSON 형식:

```json
{
  "provider": "KIS",
  "endpoint": "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
  "symbol": "006400",
  "period": "D",
  "fid_org_adj_prc": "1",
  "csv_sha256": "CSV 파일 자체의 실제 SHA-256",
  "captured_at_utc": "2026-11-19T00:00:00Z"
}
```

수정주가용 매니페스트는 선택값만 `"0"`으로 변경하고, 해당 수정주가 CSV의 SHA-256을 기록한다. 매니페스트는 누가 제출했는지 서술하는 자료이지 **KIS가 서명한 원본 인증서가 아니다**. 실제 API 원본 응답·수집 로그도 함께 보관해야 완전한 계보 감사가 가능하다.

```bash
python research_projects/trendline_price_lineage_r8_20261119/r8_price_lineage.py \
  --raw-csv raw.csv --adjusted-csv adjusted.csv \
  --raw-manifest raw.json --adjusted-manifest adjusted.json \
  --out r8_independent_audit.json
```

검사 항목:

1. 이전 R5 KRX 원본 SHA-256 불변 및 두 공급시세의 종목/날짜/양수 OHLC 유효성 확인.
2. 5개 날짜의 **KIS 원주가와 보존 KRX 원본 일치성** (불일치 시 `RAW_SOURCE_DISAGREEMENT`로 중단).
3. 각 5일×4가격에 대한 `수정/원본` 비율·최대편차. 사전 정의 허용편차 `0.0001`.
4. 수정주가의 두 **고정 앵커** 몸통 상단으로 선을 다시 계산해 11-03 종가가 선 이하이고 11-06 종가가 선 위인지 확인.
5. 11-06 수정 OHLC와 사용자 HTS 숫자의 **원 단위 직접 일치(±1원)** 확인. 일치하지 않으면 다른 공급자/조정 날짜/반올림 규칙 가능성을 `FALSE`로 남김.
6. 5일만 비교하므로 수정주가 전체 과거 기간 R116 후보선 순위·동일 활성 선 ID **재실행 인증 불가**. `r116_adjusted_full_replay_certified=false` 유지.

## 경계 및 다음 연구

- **실전 승인 금지**: `strategy_winner=null`에 해당하며 R5/R6/R7 손익/모형을 소급 변경하지 않음.
- 사용자 HTS 실제 조회 당시 시점, 수정주가 사용 여부, 유상증자 외 기업행위, 이전 분기 수정 이력, KIS 최신 사후 수정 시점에 대한 별도 검증 필요.
- 동일계수 가정만으로 전체 과거 차트/지표를 고치지 않음. 2025년 사건을 2023년 당시 알 수 있었던 정보로 쓰지 않음(PIT 금지).
- 가격 계보 확정 후, **당시 알 수 있는 원본 시세**로 전체시장 PIT 코호트를 재현하고 OOS를 사전 고정할 것. 별도 분봉/체결 자료 확보 전에는 가격대별 매물대를 계산했다는 주장 금지.
