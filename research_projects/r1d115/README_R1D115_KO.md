# Stock-Hunter R1-D115 — 제공 코드 유용 요소 보존 및 독립 비교 연구 (RESEARCH ONLY)

## 목적

이 버전은 기존 R1-D112 **활성 추세선 A축**과 R1-D113~D114 **박스/채널/수렴 구조 B축**을 **그대로 유지**합니다. 별도 C축에서 사용자 제공 Python 원본 알고리즘 중 아래의 재사용할 만한 요소를 독립적으로 비교합니다.

- `scipy.signal.find_peaks` + ATR prominence 기반 국소 고저점 탐지
- 반복 고점·저점의 ±1.5% 가격대 군집 및 독립 터치 횟수
- 과거 20거래일 대비 당일 거래량 증가 비율
- 신호가 실제 사용한 고정 가격대를 실제 캔들/거래대금 차트에 표시
- 코드 원형 결과(`ORIGINAL_LIKE`)와 **전일 종가까지 확정된 후보**로 판단한 `CAUSAL_PIT` 결과 동시 비교

**제외:** 이름 문자열 파싱으로 만드는 임의 매수 점수, `falling`의 방향만으로 보너스 부여, 중복 돌파를 합산해 높은 매수 점수 만들기, 미확정 피벗을 과거로 소급, 실제 신호 선과 다른 선을 차트에 표시, 과거 LIVE/SEARCH/WATCH 변경.

## 시간적 인과성

1. **t-1 종가**: 80거래일 OHLCV와 당일 확인 가능한 ATR14로 고가/저가 피크를 추출합니다. 이때 `find_peaks`가 그 시점의 80개 과거 봉만 보며, 미래 봉을 읽지 않습니다.
2. **t 종가**: 전날 고정된 가격대/하락 회귀선에 대해 상향 돌파 또는 하단 이탈을 평가하고, 당일 몸통/꼬리와 **직전 20거래일** 거래량을 별도로 기록합니다.
3. **t 이후**: D+1,3,5,10,20,40 수익률은 `EXPOST.csv`에만 분리 기록합니다. 이를 신호 점수나 후보 발견에 사용하는 코드는 없습니다.
4. **스냅샷**: 과거 피크를 현재 ATR로 다시 검사하는 것은 허용하되, 이미 추출한 과거 피벗이 처음부터 알려져 있었다고 주장하지 않습니다. 후보의 이용 가능 시점은 `formed_asof=t-1`로 기록합니다. 지속형 활성 피벗 등록은 기존 R1-D112 축에서 계속 담당합니다.

## 적용되는 원본과 제한

- 전일-고정 비교 알고리즘은 상용 TrendSpider/Autochartist/TradingView 원본 엔진이 아니며 공개 Python 아이디어를 재구성한 것
- 고점가격 군집 = 저항 후보. **실제 거래량별 매물대(Volume Profile) 또는 기관 매집의 증거가 아님**
- `falling_channel_breakout` 원본 이름은 두 개의 경계선이 있는 채널이 아니라 **단일 하락 고점 회귀선 돌파**에 해당
- 가격대 군집의 터치 횟수는 독립 시점(3봉 이상 간격)을 기준으로 기록. 원본과의 비교에서는 사용자 원형의 독립터치 제한 없는 군집을 별도로 계산
- 50/40/30점 누적은 매매 신호로 검증된 규칙이 아니므로 적용하지 않음
- 데이터셋은 **동결된 KRX 3종목**으로 현재까지 제한되어 일반화/수익성은 미검증
- 종목별 경계선 및 신호별 D+20은 검증 후 참조할 결과로, 미래 수익을 현재 신호 선정에 반영하지 않음

## CSV 결과

- `R1D115_USER_ORIGINAL_LIKE_DIAGNOSTIC_ASOF.csv`: 제공 원본 알고리즘의 날짜별 재현 진단. 원본과 비교하기 위한 별도 출력
- `R1D115_CAUSAL_ATR_PIVOT_CLUSTER_EVENTS_PIT.csv`: 전날 알고 있던 가격대에 대한 당일 실측 사건, 13개 MA/BB/ENV/구름대 계열 연구 중 대표 지표, R1-D112 활성선 비교, R1-D114 엄격 구조와 동시 발생 여부
- `R1D115_DESCRIPTIVE_DEDUP_EPISODES.csv`: 10봉·유사 가격대 사건 묶음; 독립 매매 표본 아님
- `R1D115_FIXED_CASE_AUDIT_NOT_TRAINING.csv`: 디오 2024-09-13, 롯데하이마트 2024-03-13, 삼성SDI 2024-02-16 사후 지정 검증. 사람의 기준선은 선별에 관여하지 않음
- `R1D115_CAUSAL_EVENT_FOLLOWTHROUGH_EXPOST.csv`: 사후 종가 결과를 미래 정보 파일로 독립 분리
- `R1D115_AUDIT.json`: 기존 R1-D112/R1-D114 입력 SHA-256 변경 없음 및 결과 수치
- `results/charts/*_ASOF_PIT.png`: 신호 발생일 당시까지의 캔들만 있는 차트
- `results/charts/*_EXPOST_20.png`: D+20 미래 캔들을 노란 영역으로 구분해 표시한 사후 검증 차트

## GitHub Actions 실행

기존 `.github/workflows/stockhunter_r1d98_state_structure_research_only.yml`을 **같은 경로에서 갱신**하고 `study: r1d115`로 실행합니다. 기존 연구 단계 R1-D105 → D106 → D108 → D109 → D110 → D111 → D112 → D113 → D114를 마친 다음 이 연구만 추가합니다. `study: all`에서도 실행됩니다. 새로운 workflow 파일이나 LIVE/SEARCH/WATCH 변경은 없습니다.

CI는 Python 3.11, `numpy`, `pandas`, `matplotlib`, `scipy>=1.13,<2`를 사용합니다. `mplfinance`를 별도로 설치하지 않아도 이전 연구 환경의 matplotlib로 같은 선을 표시할 수 있습니다.

**검증 주의:** 적용 ZIP의 로컬 테스트 성공은 GitHub 전체 동결 KRX 3종목 실행 결과가 아닙니다. GitHub 실행 후 `R1D115_USER_ATR_PIVOT_CLUSTER_PIT_VISUAL_RESEARCH_ONLY` 아티팩트 수치로만 실전 탐지 결과를 판단하세요.
