# BLUE20 선행 관심·첫 파동·눌림·재상승 독립 연구 R0

기존 main 5766325f042924b0c254e00354e7661d5d8d16dc, common workflow blob c05f9b8bd9c26037b8d2e8c3dea17bf4a5b32ba5를 확인하고 준비했다. 기존 알고리즘·이전 연구는 보존한다. 신규 코드는 research_projects/blue20_early_wave_pullback_research뿐이다.

별도 research/blue20-early-wave-r0-20261108 브랜치에 적용해 검토하는 것을 권장한다. 현재 연결의 브랜치 생성은403으로 거절되어 직접 업로드·커밋·실행은 하지 못했다. 루트 research_projects 폴더를 업로드하고, 루트 YML 사본을 .github/workflows/stockhunter_r1d98_state_structure_research_only.yml에 반영한다. 동봉 .github YML과 루트 사본은 동일하다. 기존 main이 바뀌었으면 최신 YML에 WORKFLOW.patch를 검토하여 합친다. 기존 전체 파일 blob 검증이 실패하면 원인을 조사하고 보호 해시를 임의로 갱신하지 않는다.

Actions의 공통 연구에서 study=blue20_early_wave 선택. r118_mode는 이 연구에서 사용하지 않는다. artifact=BLUE20_EARLY_WAVE_PULLBACK_RESEARCH_ONLY.

500종목 전체2023~2024거래일을 검사한다. 2021년 고정 원천은448일선 비교에만 추가하며 기존 RSI seed와 원장1804건은 전수 재현한다. P0 실패·P1·P2 초기 후보·P3 확인·단계 실패·미완료·거짓 경보를 모두 기록한다. 주 비교는 동일P1기회와P1+20동일만기, 보조 비교는 각 단계+20만기다. 두 표의 평균을 섞어 해석하지 않는다.

핵심 산출물: REPORT_KO.md, ALL_STOCK_DAYS.csv.gz, P0_CANDIDATES.csv.gz, SIGNALS.csv.gz, EPISODES.csv.gz, TRANSITIONS.csv.gz, P0_CHAIN_OUTCOMES.csv.gz, TRADES.csv.gz, P1_SAME_OPPORTUNITIES.csv.gz, BASELINE_IDENTICAL_OPPORTUNITIES.csv.gz, VOLUME_RESISTANCE_EVIDENCE.csv.gz, LEAD_COMPARISONS.csv.gz, GALLERY.html, VERIFICATION.json.

가격 저항 증거와 일봉 거래량 집중 후보를 별도로 유지한다. ‘미검출’은 실제 매도물량 부재가 아니다. 실체결량·가격 보정·HTS/TradingView 완전 재현은 UNVERIFIED다. 수익률은 원천 품질 한계가 남은 탐색값이며 실전 인증이 아니다. 매물대 보류 조건을 최적화하지 않는다.

영상은 로컬에서 확인했지만 종목·점선 수식은 알 수 없다. 2025년 성공 장면을 보았으므로 추후 해당 사례를 독립 검증으로 중복 사용하지 않아야 한다. 2025~2026 OHLCV는 읽지 않았다. 원본 영상·개인 기기 프레임은 이 패치에 넣지 않는다.
