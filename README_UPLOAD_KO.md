# BLUE20 가격 저항 상호작용 GitHub 실행 패치

기준 커밋은 `106c32057125b984ac969fd5851d3e2ef487116f`입니다. 기존 연구 선택지와 작업은 유지하면서 공통 워크플로에 `blue20_price_interaction` 선택지와 독립 RESEARCH_ONLY 작업을 추가했습니다.

## 적용할 파일

- `research_projects/blue20_price_interaction_research/` 폴더의 파일을 저장소의 동일 경로에 올립니다.
- `.github/workflows/stockhunter_r1d98_state_structure_research_only.yml`을 동일 경로에 올립니다.
- ZIP 루트의 `stockhunter_r1d98_state_structure_research_only.yml`은 숨김 폴더 처리 없이 올릴 수 있도록 넣은 동일 사본입니다. 저장소 루트에 올리지 말고 기존 워크플로 경로에 넣습니다.

## 실행

GitHub Actions → 공통 Trendline Research → Run workflow에서 `study=blue20_price_interaction`을 선택합니다. 다른 입력은 기존 기본값을 유지합니다.

이 작업은 원본 500종목 데이터와 RSI 사건을 재생성하고 SHA-256·사건 동일성·기존 거래 재현을 확인한 뒤, 가격 저항 기술통계와 차트를 만듭니다. 결과 아티팩트 이름은 `BLUE20_PRICE_RESISTANCE_INTERACTION_RESEARCH_ONLY`입니다.

완료 후 아티팩트의 `AUDIT.json`, `REPORT_KO.md`, `GALLERY_KO.html`, `ANOMALIES_KO.md`, `TEST.log`를 확인합니다. 데이터 인증 미해결 항목은 결과에 그대로 남습니다.

현재 연결된 GitHub 앱은 업로드 시 `403 Resource not accessible by integration`을 반환했습니다. 이번 작업에서 원격 업로드와 Actions 실행은 완료되지 않았습니다.
