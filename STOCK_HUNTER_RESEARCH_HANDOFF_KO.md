# Stock-Hunter 연구 인계서 (새 채팅창에서 가장 먼저 읽을 파일)

최종 갱신: 2026-11-19 KST
저장소: https://github.com/ehdud6728-bit/Stock-Hunter

## 1. 변경할 수 없는 원칙

- 연구 모드 RESEARCH_ONLY. 기존 LIVE, SEARCH, WATCH, 실전 검색식, 매매 신호·순위, 주문 및 청산 로직 변경 금지.
- 과거 연구 원본 파일과 기존 워크플로를 보존한다. 새로운 연구는 별도 파일·실행 단계로 추가한다.
- 표본이 작거나 가격 출처/시점(PIT)이 불확실할 때 승자 전략을 선언하거나 실전 배포하지 않는다.
- 연구가 좋아 보여도 독립 시계열 검증, 시장 국면, 거래비용, 순차 OOS를 통과하고 사용자가 명시적으로 승인하기 전에는 전략을 바꾸지 않는다.
- 2026-11-19 현재 두 연구 트랙은 분리한다: ENV10/ENV20 하단 터치, 추세선·전고점 돌파의 구조 분석.

## 2. 최근 확인된 GitHub 실행 기록

1. 신호 전용 고정 표본 77건 실행: https://github.com/ehdud6728-bit/Stock-Hunter/actions/runs/44330869874
   - 신호 판정 76건, 미상 1건. ENV10 터치 13건, ENV20 터치 2건. 미래 수익률 연구가 아니다.
2. PR #6: https://github.com/ehdud6728-bit/Stock-Hunter/pull/6
   - 병합됨. R2 연구 코드는 research_projects/closebet_env_ab_20261119/frozen_300_forward_path.py
   - 실행: https://github.com/ehdud6728-bit/Stock-Hunter/actions/runs/44360984134
   - 결과 성공. Artifact ID 14455270296; 300건 사건, 1,500행의 5개 기간별 사후 경로 CSV.
   - 입력 원천 research_inputs/R1D27R_PILOT_DMINUS60_DPLUS40_PRICE_CONTEXT.csv.gz
   - 입력 SHA256 b6d1fd1f4e4fdab6680bc78705825ff8b9775b20adb14b4aa8e05b55a004742d.
3. R3 연도별·극단값 민감도 추가 연구:
   - 추가 경로 research_projects/closebet_env_year_audit_20261119/
   - 별도 수동 Actions: Stock-Hunter ENV10 ENV20 Year Fragility Audit (RESEARCH ONLY).
   - PR #7 병합됨: https://github.com/ehdud6728-bit/Stock-Hunter/pull/7
   - 실행 성공: https://github.com/ehdud6728-bit/Stock-Hunter/actions/runs/44439817447
   - R2 원본 단위 테스트 11개 + R3 단위 테스트 6개 통과; 연구 전용 가드 통과.
   - 산출물: https://github.com/ehdud6728-bit/Stock-Hunter/actions/runs/44439817447/artifacts/14491280579
   - ENV10_ONLY 37건 잠정 D+20 평균 +1.851956%, 최고 1건 제외 -0.437111%로 부호 반전.
4. R4 사전 차트·거래량 맥락 연구 제안:
   - 파일: research_projects/closebet_env_context_20261119/context_audit.py
   - Actions: Stock-Hunter ENV10 ENV20 Pre-Signal Context Audit (RESEARCH ONLY).
   - 20일 평균 거래량·거래대금과 D0 상대 비율, D-1 이전 20/60일 고저, MA20/60 대조·MA112/224 출처 상태, 장중 캔들, 60일 파동/눌림 대리값 기록.
   - 로컬 고정 300건 테스트 결과: ENV10_ONLY 중앙 D0/직전20일 거래량 비율 0.605025, 2배 이상 4/37건, MA224 아래(제공값) 26/37건, 파동·눌림 대리 후보 9/37건. NO_TOUCH 중앙 거래량 비율 0.787689. 미검증 원본 기반 기술통계이며 전략 성과가 아님.
   - 로컬 R4 단위 테스트 8개 통과; GitHub Actions 실실행은 PR 병합 이후 별도 검증 필요.

## 3. R2에서 확인된 사실과 한계

- 전체 300건, 신호 판정 가능 297건, 미상 3건.
- ENV10_ONLY 37건, ENV20_BOTH 3건, NO_TOUCH 257건, UNKNOWN 3건.
- ENV10 터치 총 40건, ENV20 총 3건(ENV20은 ENV10의 부분집합). ENV10 최초 터치 11건, ENV20 최초 터치 0건.
- D+1 시가 진입, D+20 종가 청산, 왕복 총비용 20bps 가정: 전체 ENV10_ONLY 평균 +1.851956%, 중앙값 +1.630664%.
- ENV10_ONLY 최고 1건(389500, 2024-12-09, +84.258399%) 제외 민감도 평균 -0.437111%. 이 제거는 실제 거래 필터가 아니다.
- 2024년 ENV10_ONLY 10건 평균 +8.246898%, 최고 1건 제외 시 -0.198824%. 평균이 한 종목에 민감하다.
- ENV10_ONLY 연도별 건수와 D+20 잠정 평균:
  - 2021: 7건 +6.902%; 2022: 5건 -8.807%; 2023: 10건 -0.940%; 2024: 10건 +8.247%; 2025: 5건 -1.766%.
- 300 사건에서 서로 다른 종목코드는 277개, 중복 종목 사건 초과분은 23건, 서로 다른 D0 날짜는 100개다.
- 원래 사전 선정된 1,500건 중에 300건이 모두 들어 있음을 확인했으나 전체 시장 대표 표본이라는 의미는 아니다.
- 2024년 ENV20 2건이 큰 양수 결과, 2021년 ENV20 1건은 음수 결과. 전체 3건으로 전략 우열 판정 불가.
- R2는 D0에 확정된 종가 포함 MA20을 기준으로 D0 저가가 0.90배/0.80배 이하면 터치로 정의한다. 장중 지정가 매매·체결 재현이 아니다.
- 원래 지수 파일 research_inputs/R1D27D_ORIGINAL_KRX_INDEX_1907_DAYS.csv에는 date 열만 있다. 실제 지수 종가가 없어 상승/하락/횡보/스트레스 국면은 미상이다.
- 전체 시장 원천 큐 85,576건은 아직 미수집: research_inputs/R1D27D_FULLMARKET_SOURCE_QUEUE_MANIFEST.json 안에 data_retrieved=false, not_pit_certified=true.
- 가격 데이터 독립 인증, PIT 당시 종목 분모, 상폐/정지 반영, 실전 체결, 포트폴리오 백테스트, OOS 모두 미완료다.
- 운영 허용 플래그는 strategy_winner=null, can_publish_strategy_performance=false, can_modify_live_search_watch=false.

## 4. 사용자가 요청한 돌파 구조 연구 (별도 트랙)

- 사용자는 단순히 돌파했다는 이진 판정이 아니라 어떻게 돌파했는지, 돌파 이전 차트 위치·거래량·직전 20일 평균 거래량·전고점·전저점·이전 파동 및 눌림목을 같이 확인하길 원한다.
- 가장 먼저 할 검증: 삼성SDI의 2026-11-02 추세선과 2026-11-06 돌파 판정에 사용된 추세선이 정말 같은 고정 앵커의 선인지 실제 차트에서 확인. 이 과정 없이 추세선 연구를 확장하거나 R1-D119 같은 다음 번호를 만드는 일을 우선하지 않는다.
- 추세선 앵커/생성시점/기울기/각 날짜의 선 값과 돌파선 ID를 기록하여 동일선 재사용 여부를 확인한다.
- 이후 과거 정보만으로 다음 속성을 사전 등록하여 사건마다 기록한다: MA20/60/112/224 대비 주가 위치, 1차 파동과 눌림, D-1까지의 전고점/전저점, 20거래일 가격 범위, 돌파 갭/윗꼬리/종가 안착, D0 거래량 대 D-20..D-1 평균 거래량, D0 거래대금 대 D-20..D-1 평균, 최근 거래량 증가 추세.
- 거래량 증가만을 매수세라고 판단하지 않는다. 캔들·종가 위치와 별도 매수/매도 수급 자료를 구분한다.
- 실제 성과/재이탈/리테스트는 사후 D+ 경로의 결과 라벨이다. D0 이전 패턴이나 실시간 검색 특징에 미래 결과를 섞지 않는다.
- 상세 설계: research_projects/trendline_breakout_quality_20261119/RESEARCH_PROTOCOL_KO.md

## 5. 다음 연구 순서

1. 이 인계서와 최근 Actions 결과를 먼저 검토하고 원본 SHA 및 300건 고정성 확인.
2. R3 성공 결과 확인 완료. R4는 PR 병합 후 별도 Actions를 실행하여 고정 300건의 D0 맥락 CSV/JSON과 테스트 결과를 확인한다.
3. R4의 거래량·MA 위치와 파동·눌림은 잠정적 설명변수일 뿐 승자 조건/필터로 채택하지 않는다. 20/60일 원시 봉으로 112/224일선 독립 검증이 불가능함을 유지한다.
4. 공식 지수 수준의 과거 D-1 정보와 전체시장 PIT 일봉, 상장·상폐·거래정지까지 포함하는 독립 가격 출처를 확보. 수집 실패를 정상 값으로 간주하지 않는다.
5. 2021~2025 기간의 사전 등록된 시계열 구간, 종목 중복을 제한한 독립 OOS 표본을 구성하고 비용·진입/청산·날짜를 동일하게 비교. 그 전까지 전략 비교는 기술통계 수준.
6. 추세선 연구는 삼성SDI 동일선 차트 검증을 선행하고, 돌파 방식 조건을 잠금 정의한 다음 독립 표본에서 사후 경로를 본다.
7. 두 트랙의 실전 신호 결합이나 조건 최적화는 사전 검증과 사용자 승인이 없으면 진행하지 않는다.

## 6. 새 채팅창에 붙여넣을 시작 문장

Stock-Hunter 연구를 이어서 진행합니다. 저장소 루트의 STOCK_HUNTER_RESEARCH_HANDOFF_KO.md를 먼저 읽고, 관련 PR/Actions 및 실제 파일을 확인한 뒤 이전 검증 결과부터 이어가세요. RESEARCH_ONLY, LIVE·SEARCH·WATCH와 기존 실전 검색식 수정 금지입니다. ENV10/20의 독립 시세·PIT·시장 국면 연구와 삼성SDI 동일 추세선 검증 후 돌파 구조 연구를 서로 분리하여 진행해주세요. 숫자가 좋아 보여도 OOS와 데이터 독립성 검증 전 전략 승격 금지입니다.
