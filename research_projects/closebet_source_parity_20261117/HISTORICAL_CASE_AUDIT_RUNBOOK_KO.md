# 종가베팅 × 엔벨하단: 과거 시세 17건 원본 대조 실행서

버전 `CB_ENV_HISTORICAL_FIXED_17_CASE_SOURCE_AUDIT_R1` · `RESEARCH_ONLY`

## 무엇을 비교하는가

이미 저장된 `R1D27R_PILOT_DMINUS60_DPLUS40_PRICE_CONTEXT.csv.gz`에서 2021~2025년 매년 `LOW/MID/HIGH` 거래대금군 각각 **종목코드·신호일 순으로 첫 번째** 사건을 1개씩 고정해 15건을 선택하고, 숫자 외 영문을 포함한 우선주 코드 `35320K`, `45014K`의 첫 사건을 각각 추가해 **총 17건**을 선택한다. **나중에 성과가 좋았던 종목을 골라내는 선택 과정이 없다.** 실제 상장폐지·조회 불가능 종목도 삭제하거나 다른 종목으로 대체하지 않는다.

`pykrx==1.2.9`의 KRX 유래 비수정(`adjusted=False`) 과거 OHLCVA를 각 종목·신호일에 대해 별도로 조회한다. 그 날짜 이전의 61개 일봉(`D-60~D0`) 각각을 시고저종·거래량·**실제 제공 거래대금**과 대조한다. 종가×거래량을 거래대금 대신 사용하지 않는다. 가격 224개 이상이 있는 경우 20/112/224 이동평균 수치도 비교한다.

다만 KRX 원천 웹사이트에서 값을 읽는 `pykrx` 호출이 **거래소 OPEN API의 공식 인증자료와 동일하다는 독립 입증은 아니다.** 기존 봇과 원천 계통이 겹칠 수도 있다. 원본 파일의 수정주가 기준도 아직 공식 인증되지 않았다. 모든 숫자가 같더라도 최종 결과는 `NUMERIC_PILOT_PARITY_ONLY`이며 전체시장·과거 모집단·전략 수익성을 입증한 것이 아니다.

## 실행

1. ZIP 안의 연구용 `.py`, `test_*.py`, 이 설명서를 기존 프로젝트의 같은 경로에 복사한다. 기존 LIVE·SEARCH·WATCH 파일은 건드리지 않는다.
2. ZIP 맨 위의 `.yml`을 GitHub 저장소의 `.github/workflows/stockhunter_closebet_env_historical_case_audit_research_only.yml`로 등록한다. ZIP에는 동일한 파일이 해당 경로에도 들어 있다.
3. Actions → `Stock-Hunter Closebet ENV Historical 17-Case Price Parity (RESEARCH ONLY)` → `Run workflow` 실행.
4. 실행 번호 링크와 아티팩트 `closebet-env-historical-case-audit-<실행번호>`를 확인한다.

## 결과를 어떻게 읽을까

| 상태 | 의미 | 다음 행동 |
|---|---|---|
| `NUMERIC_PILOT_PARITY_ONLY` | 고정 17사건의 D-60~D0 6개 필드/MA 수치가 요청된 pykrx 값과 같음 | 공식 KRX OPEN API 및 다른 공급자와의 독립 대조, 과거 전체시장·수정주가 기준 검증 계속 |
| `BLOCKED_INCOMPLETE_OR_MISMATCH` | 날짜 누락·가격 불일치·MA 불일치·공급자 조회 실패가 하나 이상 있음 | JSON의 `events`와 `difference_examples`를 보고 실제 차트/권리변동/원본 값 확인 |
| 실행 녹색/빨간색 | 녹색은 감사 실행 완료일 뿐 데이터 일치라는 뜻이 아님. 필드 불일치·상장 전 이력 부족은 명확히 `BLOCKED`로 표시. 조회 자체 실패·출력 누락은 빨간색 | `status`와 필드별 불일치 우선 분석 |

비교 기준은 오차 0원·0주로 엄격하다. 수정주가·비수정주가의 원본 정의가 다르면 대량 불일치가 나올 수 있다. 이는 조정기준 차이라는 가설이지 자동으로 어느 쪽이 잘못됐다는 증거는 아니다. 향후 동일 가격 기준의 공식 데이터와 기업행위 이력으로 따로 확인한다.

`HISTORICAL_CASE_AUDIT_RESEARCH_ONLY.json`에는 17건의 조회 여부, 가격 필드별 불일치 건수, 대표 사례, MA 값과 기록 해시가 남는다. `FROZEN_CASE_SELECTION.json`에는 선택된 17건과 사용한 입력 파일 해시가 남는다. 원시 KRX 시세 전체 파일은 아티팩트에 재배포하지 않는다.

원본에서 `45014K`의 2023-02-08 신호 사건은 D-60~D0 중 54개가 `valid_price=0`/NaN이고 2023-01-31부터 7개 거래일만 유효하다. 이 **상장 전/이력 미확보 행을 0원 거래로 간주하지 않고**, 유효한 7개만 가격을 비교하되 224일선 및 사건 전체 판정은 `UNKNOWN`/`BLOCKED`로 유지한다. 이 표시는 상장일 공식 확인의 대체가 아니다.

**금지:** 검색식 튜닝, 전략 수익률 계산, 실전 사용 승격, 가격/거래대금 결측을 0 또는 추정치로 채우기, 실패 종목 재선정.
