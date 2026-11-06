# Stock-Hunter BLUE20 × RSI × Slow Stochastic × 시장·섹터·거래대금·1차파동

**2026-11-06 | RESEARCH_ONLY | 연구용 GitHub Actions 기존 YML에 통합하는 패키지**

## 기존 코드 보존

- 기존 R1-D118과 BLUE20 RSI 원본은 변경하지 않습니다.
- 새 `study=blue20_multifactor` 선택 시 기존 2022년 말 시총 상위 500종목의 2023~2024년 R118 pinned 시세를 새 결과 디렉터리에 만듭니다.
- 원래 BLUE20 RSI 연구를 **재실행**하고 기존 몸통 돌파 4,232/2,489건 재현 검증이 실패하면 분석을 중단합니다.
- 별도 통합 Actions 파일을 만들지 않고 **기존 단일 YML을 교체**합니다.
- `r118_mode=auto`(기본) 및 나머지 기존 설정은 새 연구에서는 사용하지 않습니다.
- LIVE / SEARCH / WATCH / 랭킹 변경 없음. 기존 `.github/workflows` 안의 다른 파일 변경 없음.

## 실행

1. 압축을 풀어 저장소 루트에 `research_projects/blue20_multifactor_research/`를 추가합니다.
2. 기존 `.github/workflows/stockhunter_r1d98_state_structure_research_only.yml`을 이번 ZIP에 포함한 동일 경로 YML로 교체합니다. ZIP 루트에도 같은 YML을 제공합니다.
3. GitHub에 새 커밋으로 업로드합니다.
4. Actions > 기존 StockHunter R1-D98 통합 연구 > Run workflow > `study=blue20_multifactor`를 선택합니다.
5. `r118_mode` 포함 기타 항목은 **전부 기본값** 유지.
6. 결과 아티팩트 `BLUE20_MULTIFACTOR_500_RSI_SLOW_RESEARCH_ONLY`에서 `stoch_14_3_3`, `stoch_12_5_5`, `stoch_20_12_12` 세 폴더를 비교합니다.
7. 반드시 각 폴더 `AUDIT.json`의 `sector_pit_coverage_pct`, `theme_pit_coverage_pct`, `full_sixway_conclusion_allowed`를 먼저 확인합니다.

## 분석 시점 — 미래정보 금지

- D0: 원래 BB20 2σ 상단/하단 26일 이동선 돌파 확정 종가.
- RSI: D0~D+10 첫 골든크로스. 원본 계산식 미확정이라 네 가지 RSI 변형을 계산하지만, 기준 보고서는 WILDER_SMA9 한 레인으로 고정합니다.
- Slow Stochastic: 기본 14,3,3, 민감도 12,5,5와 20,12,12. 오늘 Slow K/D 크로스, K의 위치 및 기울기, 첫 Slow GC가 RSI GC보다 먼저/같은날/나중에 발생했는지 별도 기록합니다.
- 실제 관찰조건: GC 당일 **종가 확정 후**에만 신호를 사용하며 다음 날 시가에 진입하는 가상 체결입니다.
- `JOINT_CONFIRMED`는 RSI/Slow가 둘 다 골든크로스하고 **늦은 확인 당일에도 두 지표가 강세 유지**된 경우만 인정합니다. 다음날 시가 매수, D0+20 거래일 종가 만기.
- +10% 익절 / -5% 손절, 같은 날 둘 다 가격 터치 시 손절 우선, 왕복 비용 0.25%p. 분봉 실제 체결 인증 아님.
- D+20까지 최고 고가는 손절 후 반등을 포함할 수 있으며 실현손익과 반드시 별도 표시됩니다.

## 독립 피처 정의

- 시황: 해당 종목 **그날** 코스피/코스닥 시장 소속의 지수 마감가 > MA20 > MA60 = `UPTREND`, 반대 방향은 `DOWNTREND`; 나머지는 `MIXED`. 5/20거래일 시장 수익률, 지수 거래대금/전일 이전 20일 평균, 고정 500코호트 내 종가가 MA20 위인 비율.
- 거래대금: 해당 종목 당시 거래대금/전일 이전 20거래일 평균, 고정 TOP500 내 신호일 거래대금 순위. **전체 시장의 거래대금 대장주라고 부를 수 없습니다.**
- 1차파동·눌림: D0 이전 35거래일 구간의 고점, 이전 35일 저점 → +15% 상승, 고점에서 -5~-30% 조정, D0에서 저점대비 +3% 재상승, 눌림 중 평균 거래량이 과거 5일의 80% 이하인지. 224일선 위·아래 별도 병렬 비교. **이것은 사전 정의한 프록시 조건이며 차트별 최초 파동이 실제로 맞는지는 별도 시각 검증 필요.**
- 업종: 역사적 업종 매핑이 있다면 **현재 고정 500종목 중 그 업종 소속으로 확인된 종목들의 20일 수익률 중앙값**과 해당 시장 20일 수익률 차. 공식 업종지수 수익률이라고 부르지 않습니다. 업종별 거래대금 1위도 고정 500종목 내 순위만 표시.
- 테마: 역사적 `known_at`이 있고 동시점 유효한 테마 소속 자료만 사용. 동시 확인된 같은 테마 종목 수가 5개 이상이면 20일 테마 내 중앙값-시장 수익률 대리값. 자료가 없으면 `PIT_MISSING`.

## 업종·테마 역사 자료를 추가하려면

- 현재 R118의 `sector_pit.csv`는 시장 소속은 실제 역사 정보이나 업종은 `UNVERIFIED_SECTOR`이므로 **섹터 상대강도는 계산할 수 없습니다.**
- 외부 검증 자료가 있을 때만 다음 경로에 CSV를 저장할 수 있습니다(동봉된 `*.EXAMPLE.csv`는 헤더 예시이며 데이터가 아님).
- `research_projects/blue20_multifactor_research/inputs/sector_pit.csv`
- `research_projects/blue20_multifactor_research/inputs/theme_pit.csv`
- 필수 헤더: `code,sector,known_at,valid_from,valid_to` 또는 `code,theme,known_at,valid_from,valid_to`.
- `known_at`은 해당 분류가 **당시 실제로 알려진 날짜**여야 합니다. 현재 공개된 업종·테마를 과거 시점으로 단순 소급한 값 금지.
- CSV가 제공되더라도 출처·진위 검증 전에는 `USER_SUPPLIED_HISTORICAL_PIT_UNCERTIFIED`로 표시하며, 실전 승인하지 않습니다.

## 출력 및 통계적 주의

- `01_market_regime.csv`: 시장 상승/혼조/하락, `03_sector_strength.csv`: 업종 PIT 있으면 비교.
- `04_leader_turnover.csv`: 코호트 상대 거래대금, `05_first_wave_pullback.csv`: 파동/눌림 및 MA224.
- `06_rsi_delay_stochastic.csv`: RSI 시차 × Slow 현재 상태, `13_slow_vs_rsi_order.csv`: Slow 첫 크로스 선/동시/후행.
- `14_joint_confirmation_paired.csv`: 둘 다 충족된 동일 사건의 RSI 단독/동시 확인 손익 비교(사후 매칭 편향 유의).
- `15_all_rsi_modes_sensitivity.csv`: 미확정 RSI 계산식 4종 민감도.
- `09_six_factor_full.csv`: 전체 매트릭스. 섹터 PIT이 없으면 `PIT_MISSING`으로 분리되고 6요인 정식 결론은 **금지**.
- 최소 표본: 조합당 30사건·15개 종목·2023/2024 각 10사건. 표본이 충분하더라도 독립 OOS가 없는 탐색 결과입니다.
- 전체 원장 `12_all_events_with_PIT_features.csv.gz`, `AUDIT.json`, `REPORT_KO.md`.
- 500종목 2022년 말 고정 코호트는 2023년 상장 신생주와 그때그때 등장한 대장주를 제외합니다. 2025~2026 독립 검증, 조정주가 검증, 실차트 확인 전 검색식 승격 금지.

## 현재 제공하는 예비 실측 값

`PRELIMINARY_DENSITY_BASELINE.csv`는 직전 성공한 GitHub 실행 #42170026439의 `RSI_DELAY_EVENTS.csv`를 재집계한 것입니다. 돌파 신호가 같은 날 동시 발생한 종목 수만 대리변수로 비교했으므로 시황/섹터/Slow Stochastic의 완성 분석 결과는 아닙니다.

**코드 단위 테스트 15개 및 YAML 문법·기존 분리 실행 확인. 실제 GitHub 전체 500종목 신규 6요인 연구 실행은 아직 미진행.**
