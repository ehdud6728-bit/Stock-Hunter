# R9 · KIS 삼성SDI 과거 원주가·수정주가 독립 수집 (RESEARCH_ONLY)

## 역할과 한계

R8은 삼성SDI(006400) 2023-09-19/10-18 고정 추세선 앵커와 11-02/03/06의 독립 원주가·수정주가를 아직 확보하지 못했다. **R9는 KIS 공식 과거 기간별 시세 API를 2회 호출하여 실제 다섯 날짜를 R8에 전달하는 데이터 획득 단계**다.

1. KIS 공식 [기간별 일봉 조회](https://github.com/koreainvestment/open-trading-api/blob/main/examples_llm/domestic_stock/inquire_daily_itemchartprice/inquire_daily_itemchartprice.py) `GET /uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice`로 2023-09-01~11-06, `006400`, `D`, `J(KRX)` 조회.
2. **같은 종목·범위에서 `FID_ORG_ADJ_PRC=1` 원주가, `0` 수정주가**를 각각 수집. 최근 30일 시세 API와 선택값 정의가 반대이므로 다른 엔드포인트 대체 금지.
3. 실제 API에서 받은 응답의 SHA-256, 요청 플래그, 조회 UTC 시각, 정규화 CSV의 SHA-256을 기록. 토큰·인증키·계좌정보는 **아티팩트·로그·코드에 저장하지 않음**.
4. 수정주가를 임의로 `0.979` 곱해 생성하지 않음. 독립 소스 오류·날짜 누락·원본 불일치면 `UNKNOWN/ERROR` 유지.
5. 등록된 R8 원본 SHA 검증, 5일 OHLC 가격 대조, 앵커 보존과 수정주가 고정선 돌파 검사를 다시 수행. **이것만으로 R116 수정주가 전체 과거 활성선 순위까지 재실행 인증되지는 않음.**

공식 인증 API 예: [한국투자증권 오픈API 인증 샘플](https://github.com/koreainvestment/open-trading-api/blob/main/legacy/rest/current_price_samle.py). R9는 `/oauth2/tokenP`에서 발급받은 토큰으로 **시세 GET** 두 번만 실행하며, 주문·자산·체결/잔고 API를 호출하지 않음. 인증 자체는 POST가 필요하다.

## 키를 안전하게 준비하는 방법

**Stock-Hunter 저장소의 독립 R9 GitHub Secrets**를 사용한다. 이전 BLUE-Signal-Lab의 운영 API 키나 토큰을 코드에서 복사·조회하거나 자동으로 이동하지 않는다. 가급적 **Stock-Hunter 연구용 별도 KIS 애플리케이션 키**를 발급하여 기존 서비스에 영향을 주지 않도록 한다. KIS 앱 키는 조회용 코드에만 넣어도 자체 권한에 따라 거래 권한이 있을 수 있으므로 적절히 보호한다.

1. GitHub [Stock-Hunter 저장소 Secrets 설정](https://github.com/ehdud6728-bit/Stock-Hunter/settings/secrets/actions)에서 **New repository secret** 선택.
2. `R9_KIS_APP_KEY`와 `R9_KIS_APP_SECRET` 두 개를 각각 등록. **값 자체는 이 채팅에 게시하지 말 것.**
3. R9 PR 병합 후 새 `R9 KIS SamsungSDI Raw/Adjusted RESEARCH_ONLY` 수동 Actions에서 `main`을 선택하고 실행.

키가 둘 다 없으면 `PENDING_DEDICATED_STOCK_HUNTER_R9_SECRETS`: HTTP 요청을 하지 않고 대기 보고서만 남김. 하나만 있으면 `BLOCKED_PARTIAL_KIS_SECRETS`: 실패 종료.

## 성공 시 생성물

- `R9_SUMMARY.json` — 키/토큰 본문 없이 상태와 요청 범위, 행 수.
- `KIS_006400_202309_202311_raw.csv` + `_raw_manifest.json` — KIS 원주가.
- `KIS_006400_202309_202311_adjusted.csv` + `_adjusted_manifest.json` — KIS 수정주가.
- `R9_R8_INDEPENDENT_COMPARISON.json` — **R8 결과 계산을 실제 독립 시세에 적용**, 원본 일치·관찰 비율·화면과 차이·고정 앵커 돌파 여부.

R9는 키를 장기간 저장하거나 사용자 계좌를 조회하지 않는다. 외부 가격만 보관하며, 추후 연구 범위 밖 자동 실행이 없도록 `workflow_dispatch`만 허용한다. 모든 산출물은 기존 R5/R6/R7 결과와 분리된다.

## 실행/테스트

```bash
python -m unittest discover -s research_projects/trendline_kis_source_r9_20261119 -p 'test_*.py' -v
python research_projects/trendline_kis_source_r9_20261119/r9_kis_readonly.py --out-dir r9-audit
```

테스트의 KIS 응답은 순수 **가상 픽스처**다. 테스트 성적이나 Secrets 미등록 상태의 Actions 성공을 실데이터 확보/HTS 계보 인증으로 표현하지 않는다.

## 후속 단계 (R9 이후)

다섯 날짜의 원주가·수정주가 실제 결과를 확보한 뒤:

1. 2025년 권리락 비율 0.979033과 2023년 관측값 0.979의 **정확한 원인/적용 기준**을 공급자별로 구분.
2. 조정 비율이 바뀌는 일자·다른 기업행위 이력 확인. 사용자의 HTS 화면이 어느 날짜 기준의 수정주가인지 확인.
3. 수정주가 전체 기간 R116 활성선 재실행은 별도 R10 PIT 증명으로 진행. 미래인 2025년 유상증자 정보가 2023년 당시 신호 계산에 끼어들지 않도록 **2023 시점 원본 시세**와 사후 수정주가를 반드시 분리.
4. 전체시장 PIT/OOS/거래비용 확대는 이 기초 가격 계보 분리 이후. LIVE·SEARCH·WATCH 승인이나 전략 우위 선언 없음.
