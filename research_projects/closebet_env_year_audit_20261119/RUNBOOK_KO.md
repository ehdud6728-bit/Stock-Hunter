# ENV10/ENV20 300건 연도별 안정성·극단값 민감도 연구

모드: RESEARCH_ONLY. 기존 검색식·매매 규칙·LIVE·SEARCH·WATCH 변경 없음.

## 실행 방법

1. 관련 PR 병합 후 GitHub Actions에서 Stock-Hunter ENV10 ENV20 Year Fragility Audit (RESEARCH ONLY)를 수동 실행한다.
2. 산출물 env10-env20-frozen-300-year-fragility Artifact를 내려받는다.
3. ENV10_ENV20_FROZEN_300_YEAR_FRAGILITY.json에는 출처 상태와 전체·연도별 집계, ENV10_ENV20_FROZEN_300_YEAR_GROUPS.csv에는 연도/그룹별 20행의 집계가 기록된다.

## 비교 원칙

- 이전 R2와 같은 D0 마감 확정 MA20 기준 ENV10/ENV20 터치 정의를 그대로 사용한다.
- 원천 가격 SHA256이 바뀌면 계산을 중단한다. 원래 결과를 보고 고정한 300건이나 분류 기준을 변경하지 않는다.
- D+1 시가에서 D+20 종가까지 왕복 총 20bps를 뺀 잠정 가격 경로만 본다. 이 수치는 실거래 수익이 아니다.
- ENV20_BOTH는 ENV10의 부분집합이다. ENV10_ONLY 37건, ENV20_BOTH 3건의 구분을 유지한다.
- 평균에서 가장 높은 결과 1건을 뺐을 때의 수치를 출력한다. 이는 극단값 민감도 점검일 뿐 종목을 실제로 제거하거나 선별하는 규칙이 아니다.
- 결측 경로는 UNKNOWN이다. 수익률 0%로 대체하지 않는다.
- 각 연도의 종목 수, 신호 날짜 수와 전체 중복 종목 수를 기록한다. 같은 종목과 겹치는 보유기간 때문에 사례 수가 그대로 독립 시행 횟수를 뜻하지 않는다.
- 원래의 1,500건 사전선정 파일에서 300건 전부를 추적한다. 전체 시장 대표성을 의미하지 않는다.

## 아직 검증되지 않은 조건

- 전체 시장 원천 요청 85,576건은 데이터 수집 완료가 아니다. 기존 매니페스트는 data_retrieved=false, not_pit_certified=true다.
- 현재 원본 지수 파일은 거래일 date 목록일 뿐 KOSPI/KOSDAQ 과거 지수 종가가 없다. 연도별 구분을 상승장/하락장으로 읽으면 안 된다.
- 독립 일봉 출처, PIT 상장폐지 포함 시장 분모, 별도 OOS, 거래비용·체결 가능성 검증이 선행되어야 한다.
- 이번 연구로 우월한 전략을 선언하거나 LIVE/SEARCH/WATCH를 수정할 수 없다.
