# Stock-Hunter R1-D111 — 4계열 자동 변곡점·추세선 독립 비교 (RESEARCH_ONLY)

## 연구 목표
R1-D110의 에피소드와 R1-D106/109의 추세선을 **절대 변경하지 않고**, 공개 문서의 개념을 참고한 **서로 다른 4개 비교 구현**을 고정 원본 KRX OHLCV에 적용한다.

1. 사람이 발견한 **디오 2024-09-03→09-09 양봉 몸통 상단**, **롯데하이마트 2024-02-20→03-13 양봉 몸통 상단**을 자동 후보로 생성하는가?
2. 후보를 종가 후 **실제로 언제 사용할 수 있었는가?** (잠정 몸통 = 당일, 오른쪽 r봉 피벗 = r봉 후)
3. 새 변곡점 확정이나 연속 종가 이탈 후 **기존 활성선 교체 / 종료 / 재접촉** 시점은 언제였는가?

하단 대조: 디오 2024-08-26→09-06 저가 피벗2, 롯데하이마트 2024-02-01→03-11 저가 피벗2. 삼성SDI는 별도의 비교군으로 같은 4계열을 실행.

### 중요한 용어 구분
- **DISCOVERED**: 알고리즘이 사용자가 발견한 두 기준점의 선을 후보 목록에 자동 생성 (후보 기록에 사람이 발견한 날짜를 주입하지 않음).
- **FIRST_AVAILABLE**: 두 기준점이 실제 알려진 뒤 그 선이 등장한 최초 종가 날짜.
- **ACTIVE_SELECTED**: 다른 후보와 경쟁한 결과 실제 날짜별 유효 지지/저항선 1개에 채택. 발견 = 채택 아님.
- **ARCHIVED**: 종가 2회 연속 반대편 0.4% 초과로 확인되면 기존 선 종료·아카이브, 이후 재선택 금지.
- **REANCHORED**: 새로 확인된 기준점에서 더 높은 근거가 있어 기울기/기준점을 변경. 기존 선 ID·완료 시점 불변.
- **ROLE_RETEST**: 돌파 당시가 아니라 **이후** 기존 선 근처로 재접근하여 새 지지/저항 역할을 할 수 있는지 별도 검증.
- **VENDOR_PROXY**: 공개된 **원리만 참고한 연구 코드**. 해당 상용 업체의 실제 서비스 구현/프로그램 결과가 아니다.

## 4개 독립 비교군

| 코드 | 공개 원리에서 차용한 요소 | 우리 구현 | 비포함/주의 |
|---|---|---|---|
| TV_PIVOT_ATR_PROXY | TradingView Auto Trendlines 5/5·25/25, ATR 기반 ZigZag 규모 | 피벗 확정 지연 + ATR(후행 14봉 TR 단순평균) 2배/5배 규모, 번갈아 확인된 피벗, 고점/저점 독립선 | TradingView 실제 ZigZag 수학·터치구역·교차 필터를 100% 복제하지 않음 |
| TS_BODY_WICK_PROXY | TrendSpider의 Wick(H/L) 및 Body(O/C), 갱신·관련성 | 캔들 몸통선/꼬리 피벗선을 별도 생성, 접촉·내부 관통·근접도·시간에 따른 활성 관리 | 비공개 TrendSpider 점수, 자동 갭 아일랜드 로직, top 1% 엔진과 동일하지 않음 |
| AC_PATTERN_FLOW_PROXY | Autochartist의 Clarity, Uniformity, Breakout Strength, Volume | 상·하단 수렴 후보, 과거 구간 내 머문 비율, 접촉 간격 대리변수, 돌파 캔들 크기, 몸통/종가 위치, 거래대금·거래량 배율 개별 기록 | 업체 패턴 인식 및 Quality 1~10점 알고리즘 복제 아님 |
| OSS_TOUCH_PEN_PROXY | pytrendline/trendln의 접촉 허용오차·몸통 관통·유사성 | 캔들/피벗 기준점 후보, ATR 0.3배 접촉 대역, 관통 페널티, 중간 평균 몸통 거리 | 실제 오픈소스 코드 임포트/복사 없음; Hough 변환 미포함 |

공통: 사용자가 지정한 4개 참조선은 **후처리 평가 목록에만** 존재한다. `algorithms.py`/`lifecycle.py`에는 해당 종목·날짜가 하드코딩되어 있지 않다.

## 선별·생명주기 (연구용 정의, 업체 규칙 아님)
- 같은 종류 2개 기준점에서 후보를 생성, 원본 모든 후보는 별도 파일에 보존.
- 활성선은 알고리즘×종목×UPPER/LOWER당 최대 1개; 새 후보의 인식 시점이 이전보다 늦고 두 번째 기준점도 더 새로워야 교체 가능.
- 새 후보의 진단 점수가 기존보다 4점 이상 높은 때만 교체(2봉 이내 잦은 교체 방지); 결과 수익률로 조정하지 않은 설명용 상수.
- 활성선 반대편 0.4% 초과 종가가 **연속 2회** 나타나면 종료. 바로 새 매수 신호로 인정하지 않음.
- 종료 후 15봉 이내 기존 선 재접촉을 기록하고 재지지/재저항 가능성을 별도 진단.
- 너무 오래 관찰되지 않는 선의 *화면 활성*은 종료할 수 있지만, 과거 추세선 기록·ID·후보 CSV는 삭제하지 않음.
- 시계열 첫 14봉 ATR, 첫 20봉 거래대금 평균 등 미생성 구간은 임의 0이 아니라 `NaN`/분석 제외 처리.
- 생성점과 피벗은 최종 차트에 과거 날짜로 표시 가능하지만 **첫 인식 가능일을 과거로 이동하지 않음**.

## 출력
`research_projects/r1d111/results/`:
- `R1D111_INDEPENDENT_METHOD_LINES_PIT.csv`: 방법별 전체 후보 원본 (첫 확정일, 기준점, 관통·접촉)
- `R1D111_HUMAN_REFERENCE_RECALL_AND_TIMING_AUDIT.csv`: 4개 참조선 × 4방법 = 16행; 발견/사용일/활성선 선정/생성일 순위; 기존 R106 발견 내역과 분리
- `R1D111_FOUR_METHODS_ACTIVE_LINES_DAILY_PIT.csv`: 종목·날짜·방법·상/하단별 활성선/WAIT 상태
- `R1D111_DYNAMIC_REANCHOR_BREAK_ARCHIVE_PIT.csv`: 새 기준점, 재설정, 2종가 돌파 종료, 사용 중인 선 정보
- `R1D111_OLD_LINE_ROLE_FLIP_RETESTS_PIT.csv`: 과거 저항/지지선 이탈 후 재접촉과 당일 반응
- `R1D111_AC_PATTERN_CANDLE_VOLUME_GEOMETRY_PIT.csv`: 독립 2선 패턴, 클리어리티·유니포미티, 캔들·거래대금
- `R1D111_TRANSITIONS_CANDLE_MA_BB_ENV_CLOUD_VOLUME_PIT.csv`: 앞선 R109의 동시점 캔들, 이평선, BB/ENV, 구름대, 거래량·거래대금 맥락
- `R1D111_TWO_STOCK_REFERENCE_ASOF_COMPARISON_PIT.csv`: 사용자 참조일의 방법별 실제 활성선
- `R1D111_FOUR_ALGORITHM_COMPARISON_COUNTS.csv`: 원본 후보와 변화 이벤트의 집계(예측 승률 아님)
- `R1D111_AUDIT.json`, `charts/*.png`: 원본 SHA-256 검증 및 예제 차트

## GitHub 사용
기존 **단일 공통 워크플로** `.github/workflows/stockhunter_r1d98_state_structure_research_only.yml` 선택, `study=r1d111` 실행. R1-D105→106→108→109→110→111 순서. 별도 새 워크플로 없음. LIVE/SEARCH/WATCH, R1-D96~110 소스는 수정하지 않음.

### 시점·검증 한계
- 일봉으로는 장중 '캔들보다 거래량이 먼저' 등의 순서를 알 수 없다. 같은 날 사건은 동시 발생으로만 해석.
- ATR·ZigZag와 품질 가중치 등은 공개 원리와 일치시키려는 **연구용 대리 구현**이지 상용 제품의 정답 신호가 아니다.
- 사용자 사례의 발견률은 테스트 4개 선에 대한 재현율이며, 시장 전체 유효성/매매 성과를 나타내지 않는다.
- 전체 GitHub 실행으로 원본 3종목 결과가 재현되기 전에는 최종 백테스트 성과를 주장하지 않는다.

## 공개 출처 (방법론 연구용)
- TradingView official Auto Trendlines: https://www.tradingview.com/support/solutions/43000741165-auto-trendlines/
- TrendSpider official Trendline Customization: https://help.trendspider.com/kb/automated-technical-analysis/trendline-customization-and-filtering
- TrendSpider official Trendline Analysis Settings: https://help.trendspider.com/kb/automated-technical-analysis/trend-analysis-settings
- Autochartist Chart Pattern Quality Indicators: https://support.autochartist.com/en/knowledgebase/article/chart-pattern-quality-indicators
- ednunezg/pytrendline (MIT): https://github.com/ednunezg/pytrendline
- GregoryMorse/trendln (MIT): https://github.com/GregoryMorse/trendln
