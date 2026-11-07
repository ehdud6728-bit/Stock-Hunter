# BLUE20 신호 시점 매물대 원인 감사 업로드

research_projects 폴더를 저장소 루트에 올리고 루트 YML 사본을 .github/workflows/stockhunter_r1d98_state_structure_research_only.yml 에 덮어쓴다. 동봉 .github 폴더의 같은 YML을 써도 된다. 두 사본은 동일하다.

기준 원격 workflow blob은 e3cdba7bd6dfe692b4dc47a87446586cc234c124다. 기존 내용은 보존하고 선택지·일반 작업 중복 실행 방지·독립 작업만 추가했다. 이후 원격 YML 변경이 생기면 WORKFLOW.patch를 검토해 최신 내용에 합쳐야 한다.

기존 공통 Actions의 study=blue20_vp_signal_audit 선택. r118_mode는 사용하지 않는다. 결과 artifact는 BLUE20_SIGNAL_TIME_VOLUME_PROFILE_AUDIT_RESEARCH_ONLY.

PAST_ONLY와 SIGNAL_CLOSE를 구분하고 신호 당일 종가를 기본 판단 시점으로 삼는다. 신호 이후 거래량은 계산하지 않는다. 기존 500종목·1,804사건, 1,803개 고유 차트의 129,816개 설정 스냅샷을 재계산한다. 기존 사전 프로파일 32,454개를 재현한다. 총량·시점·손 계산 테스트, 원인 분해, HTS 고정 구간 대조, 기업행사 부분 시나리오, 신호일에서 멈춘 9개 사례 차트를 만든다.

일봉 분포는 실제 체결가별 거래량이 아니다. HTS 설정은 MISSING_SETTINGS, 전체 가격 보정과 실체결량은 UNVERIFIED다. 20.8%·45.2%에 수치를 맞추는 최적화, 매수 보류 최적화, 실전 적용·시장 확대는 하지 않는다.

HTS_SETTINGS.json은 미확인 설정을 명시한 증거다. 새 설정을 확보하면 이전 파일을 보존한 별도 버전으로 바꾸고 원천 해시를 갱신해야 한다. 현재 참조파일은 해시가 다르면 중단한다. 임의로 해시를 맞춰 원천 차이를 숨기지 않는다.

로컬 연구를 실행했으며 이번 신규 GitHub Actions는 아직 미실행이다. 이전 #42375577392는 다른 선행 연구다. 현재 연결의 GitHub 쓰기는 이전 403으로 제한되어 직접 업로드 완료를 주장하지 않는다. 기존 LIVE/SEARCH/WATCH·R1-D118·이전 연구는 수정하지 않았다.
