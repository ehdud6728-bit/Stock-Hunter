# Stock-Hunter R1-D118 — BB/ENV 상·하단 × 거래대금 × 단·중·장 추세선 통합 연구

**상태: 새 코드 제작 완료 · 3종목 부분 스모크 검증 통과 · 200~500종목 본연구 미실행 · GitHub 미적용**

이 프로젝트는 투자 성과의 입증이나 실제 검색식 교체가 아니라, 연구용 원본 시세와 시점별 추세선/시장 자료를 연결해 독립적으로 검증하는 **RESEARCH_ONLY** 코드이다. 새 GitHub Actions YML을 만들지 않는다. 기존 LIVE/SEARCH/WATCH나 R112~R117 원본을 수정하지 않는다.

## 기존 코드 확인 사항

- 현재 GitHub 저장소 `ehdud6728-bit/Stock-Hunter`의 `research_projects/r1d117` 존재 확인, R118은 이번 패치 전에는 존재하지 않음.
- 재사용 대상: `.github/workflows/stockhunter_r1d98_state_structure_research_only.yml` **기존 파일 1개**. 조회 당시 Git blob SHA `9d739df0284c88eded35f9f4825ade288c57902d`.
- 워크플로 수정은 동봉된 `patch_tools/update_existing_workflow.py`로만 수행하며, 해당 SHA가 아니면 **패치 거부**. 새 `.yml` 파일 추가 0개.
- 기존 R117 기본 선택, R98~R117 경로, 업로드 단계는 보존. R118만 새로운 `workflow_dispatch` 옵션 및 조건부 테스트/실행/아티팩트 업로드 추가.

## 실행되는 전략은 서로 독립

- `UPPER_TOUCH`: BB20/30/40 상단, ENV20/40 ±10/20%의 **첫 접촉**. 직전 10거래일 동일 밴드 미접촉. 이 사건은 나중에 눌림이 나타나지 않아도 분모에 포함한다.
- `UPPER_PULLBACK`: 상단 접촉 후 2~15일 이내 관측 고점에서 -3% 이상 조정, **7/15/20일선 부근에서 종가 방어한 첫날**. 신호일은 실제 눌림일이고, 상단 접촉일로 소급하지 않는다.
- `UPPER_REBOUND`: 눌림 후 최대 3거래일 이내 종가가 전일 고가와 MA7 위로 확인된 첫날. 별도 신호일·분모·성과.
- `LOWER_TOUCH`: BB/ENV 하단 저가 터치 또는 이탈의 첫날.
- `LOWER_RECLAIM`: 최초 하단 터치일을 포함하여 5거래일 안에 **종가가 해당 일 하단 밴드 위를 회복한 첫날**.

매 단계 거래대금: 신호 당일 절대 거래대금 `억 원`, 동일 시점의 **직전 20거래일 평균 거래대금(당일 제외)** 대비 배율. 기준 0/100/300/500억 × 0/1.0/1.2/1.5배 교차 비교. **필터 적용 전 사건별 고정 우선순위 BB20→BB30→BB40→ENV20/40, 동일 종목·시점 중복 제거 후 동일 패턴 10거래일 재진입 억제**. 거래대금 기준별로 중복 제거 규칙을 바꾸지 않는다.

성과 기준: 신호일 종가 확정 → 다음 거래일 시가 진입 → 신호일로부터 5거래일째 종가 청산(왕복 비용 0.25%p 차감). `next_day_tp3_high_reached`는 다음 날 일봉 최고가가 +3% 목표를 통과했는지의 **낙관적인 가격 도달 여부**이지 실제 주문 체결률이 아니다. 목표 미도달 당일 종가 청산의 순손익도 별도 계산.

## 단기·중기·장기 추세선 — 실제 원본 기준

R112/R116/R117의 **실제 일별 상태 파일** 중 하나를 `--line-states`로 전달. 기본 알고리즘 레인 `TS_BODY_WICK_PROXY`의 `SHORT, MEDIUM, LONG × UPPER, LOWER` 6개 상태를 기록한다. 이동평균선을 추세선으로 둔갑시키지 않으며, 원본 선 ID와 원본 날짜별 정확한 선 가격을 그대로 사용한다. `known_date > signal_date` 또는 `known_idx > signal_idx`이면 오류. **원본 전체 인덱스와 시세 데이터 인덱스가 다르면 미확보 상태**로 유지한다.

`--role-events`는 R112의 확정된 은퇴 저항선 ID의 역할 반전 재지지 기록을 별도로 연결한다. **정확히 현재 이벤트와 동일한 재지지 날짜이고, 돌파 날짜가 그보다 과거인 사건만** 접합한다. 향후 재접촉 결과를 이전 날짜의 입력 신호로 사용하지 않는다.

추세선 입력을 전달하지 않은 경우 모든 규모를 `MISSING_LINE_SOURCE`로 표시. 입력 파일은 있지만 날짜/인덱스가 맞지 않은 경우 `MISSING_SOURCE_DATE` 표시. `NO_ACTIVE_LINE`과 입력 누락을 혼동하지 않는다. **224일선은 필수 조건 없음**.

## 시장·업종 및 PIT/OOS 주의

- 대규모 연구용 시세는 `code,path,sha256` manifest로 지정하고 파일별 SHA-256 무결성 검사 후 처리. 종목을 미리 좋은 성과로 고르지 않는다.
- `--market-daily`: `market,date,close,amount` 실제 당시 KOSPI/KOSDAQ 지수 일봉. 20일 수익률/거래대금 배율은 해당 날짜 종가까지만 계산.
- `--sector-pit`: `code,date,sector,market` **해당 시점 기준** 시장·업종 분류. 과거 업종을 현재 업종으로 덮어쓰지 않는다.
- 업종 상대강도나 상승 종목 비율은 현재 버전에 *실제 원본 피처가 아직 없다*. 이를 사용한 수익 개선이라고 주장하지 않는다.
- 2024년 이후 결과는 **사전 등록 이전**의 역사적 표본이다. OOS라고 부르지 않는다.
- `R118_READINESS_AUDIT.json`에는 200종목 목표 달성, 단·중·장 6개 원본 추세선 커버리지, 시황/업종 PIT 연결, 스모크 표본 등을 표시한다. 완료되지 않은 항목이 있으면 **`RESEARCH_ONLY_NOT_PRODUCTION_READY` 고정**.

## 실행 및 재현

### 1. 동봉 3종목·부분 기간 스모크 테스트 (전체 연구가 아님)

```bash
python -m unittest discover -s research_projects/r1d118/tests -v
python research_projects/r1d118/research_r1d118.py --smoke \
  --out research_projects/r1d118/results
```

### 2. 진짜 200~500종목 데이터가 확보된 후

`research_projects/r1d118/inputs/manifest.csv` 예시:

```csv
code,path,sha256
006400,data/006400_KRX.csv,실제_원본_파일_SHA256_64자리
```

위 예시는 형식만 보여주며 실제 SHA가 아니라서 그대로 실행하면 거부된다. 전체 200~500종목의 각 행에 **실제 파일 SHA-256**을 사용해야 한다.

```bash
python research_projects/r1d118/research_r1d118.py \
  --manifest research_projects/r1d118/inputs/manifest.csv \
  --line-states research_projects/r1d117/results/R1D117_STABLE_ACTIVE_LINES_DAILY_PIT.csv \
  --line-mode R117 \
  --role-events research_projects/r1d112/results/R1D112_BROKEN_LINE_ROLE_RETEST_PIT.csv \
  --market-daily research_projects/r1d118/inputs/market_daily.csv \
  --sector-pit research_projects/r1d118/inputs/sector_pit.csv \
  --out research_projects/r1d118/results
```

원본 추세선 레인이 신규 종목에서 계산되지 않았거나 시세와 날짜/idx가 일치하지 않으면 **커버리지가 빈 값으로 남고 readiness에서 차단**된다. 새로운 종목에 R117 알고리즘을 자동으로 적용했다고 가장하지 않는다. 먼저 기존 R111→R117 계보의 실제 신규 종목 이력 생성·검증이 필요하다.

### 3. 대규모 샤드

동일한 SHA 고정 manifest로 각 샤드 실행: `--shard-index 0..N-1 --shard-count N`, 출력 경로 `shard_0` 등.

```bash
python research_projects/r1d118/merge_r118.py \
  --shards-dir reports/r118_shards --shards 6 \
  --out reports/r118_merged
```

병합은 원래 사건을 **다시 계산하지 않고**, 원장·입력 provenance·종목 배타성·샤드 누락을 검증하고 집계표만 다시 만든다. 누락/중복이면 fail closed. 별도 YML을 만들지 않으며 Actions 기존 워크플로에서 `study=r1d118`을 선택한다. 6개 샤드는 해당 워크플로에 각 shard-index로 6회 수동 실행하여 결과를 받는 구조로, 자동 일괄 분산/자동 머지는 **아직 미구현**이다.

## 로컬 검증에서 통과한 범위

- R109 기존 3종목 **부분 734개 종목-일** 입력으로 실행 완료; 샘플은 삼성SDI/디오/롯데하이마트 전 기간이 아니고 200~500종목 연구가 아니다.
- `R118_EVENT_LEDGER_PIT.csv`, `R118_ALL_ANCHOR_DENOMINATORS.csv`, `R118_THRESHOLD_MATRIX.csv`, 종목별 결과, `R118_READINESS_AUDIT.json` 생성 확인.
- 실제 R112/R116 일별 상태 CSV를 읽는 스모크 연결 실행 완료. 원본 인덱스가 달라 PIT 커버리지 미확보로 정확히 표시됨.
- PIT prefix 불변성, 신호일 종가/다음날 시가 분리, 같은 날 중복 제거, 동일 선 ID 역할 재지지의 사건일 준수, 미래 known_date 차단, 200종목 미달 차단, SHA 핀 불일치 차단, 2샤드 병합/누락 거부 등 검사.
- 검증 통과는 **현재 연구 코드의 실행/무결성 테스트**이며, 투자 수익성 또는 200~500종목 OOS 성과를 입증하지 않는다.
