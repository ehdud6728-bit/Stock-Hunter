# R11: 목표가 매도 이후 놓친 상승·손실꼬리·평균 수익 차이 추적 (RESEARCH_ONLY)

## 검증하려는 의문

R10의 `DAILY_AMOUNT_HLC3_PROXY_120 + ATR_INITIAL_3`는 ATR 손절 단독 대비 **승률·중앙값은 개선**됐으나 255건의 평균 수익 차이는 +0.014%p에 그쳤다. 왜 그랬는지, 실제 미래 고점을 목표가로 쓰지 않는 **사후 설명**을 수행한다.

## 고정 분석 규칙

1. R10 원본 소스 해시를 고정하고 R10 3종목·506 선 이벤트·368 대표 종목/일·94건 비중복 집합을 그대로 재생한다. R10→R7→R6→R5 계보를 자동 검증한다.
2. 2개 목표가 가설(`PRIOR_PIVOT_120`, `DAILY_AMOUNT_HLC3_PROXY_120`)을 **분리**해 분석한다. 각 R10 `TARGET_ATR_INITIAL_3`과 같은 신호의 R7 `ATR_INITIAL_3`을 비교한다. 목표가 미산출·완전 D+20 경로 부족은 `UNKNOWN`으로 남긴다.
3. 가격이 목표가에 먼저 도달하여 매도한 경우, 고정 ATR 손절에 먼저 도달한 경우, D+20까지 둘 다 미도달한 경우를 분리한다. 목표가 아닌 청산 결과는 ATR 단독 결과와 정확히 일치해야 하며 위반 시 즉시 실패한다.
4. **목표가 매도일 다음 거래일부터 D+20까지**의 실현 불가능한 사후 최고가, 목표가 대비 추가 상승률 및 D+20 종가를 기술 통계로만 측정한다. 매도 당일 고가는 일봉 내부 순서를 알 수 없어 제외한다. D+21 이후 정보도 사용하지 않는다.
5. 목표가+ATR과 ATR 단독의 같은 신호 수익률 차이에서 개선/악화/동률 건수, 평균·중앙·하위10%·상위90%, 최악 5건을 기록한다. 손실 구간은 순수익률 -5%·-10%·-15% 이하의 사건 수로 고정한다.
6. 전체 대표 사건과 동일 종목 20거래일 겹침을 제거한 94건 표본을 **별도** 보고한다. 종목별·연도별 그룹을 제공하지만 최적화·전략 선택은 금지한다.

**해석 제한:** 목표가 후 추가 상승 최고가는 사후 기술 값이다. 실제 매매할 수 있었던 가격, 보유했을 때의 수익, 미래를 내다볼 수 있었다는 뜻이 아니다. ATR 단독과 같은 사건의 실제 규칙 기반 모의 성과 차이만 청산 대안의 비교 수익으로 사용한다.

## 한계와 안전

- 일봉 HLC3 거래대금 구간은 실제 가격대별 매물대가 아니다. 체결 틱·매수/매도 주체·분봉 내 순서를 알 수 없다.
- 수정주가/원주가 계보의 R9 독립 검증, 전체 시장의 시점별 모집단, 기업행동 검증, OOS는 여전히 필요하다.
- 추가 승리 조건·필터·손절·목표가 튜닝 없음. R11은 진입이나 매도 규칙을 만들거나 수정하지 않는다.
- `research_only=true`, `strategy_winner=null`, `can_modify_live_search_watch=false`를 강제한다. LIVE·SEARCH·WATCH 및 기존 프로그램 파일은 변경하지 않는다.

## 실행과 산출물

```bash
python -m unittest discover -s research_projects/trendline_resistance_exit_diagnostics_r11_20261120 -p 'test_*.py' -v
python research_projects/trendline_resistance_exit_diagnostics_r11_20261120/r11_exit_diagnostics.py --repo . --out reports/trendline_r11
```

산출물: `R11_SUMMARY.json`, `R11_FULL_AUDIT.json`, `R11_PAIRED_EXIT_DIAGNOSTICS.csv`. Actions는 수동 실행만 가능하며 결과를 artifact로 보관한다.
