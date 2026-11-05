# Stock-Hunter data provenance R1: confirmed vs not yet proven

2026-11-05 · Static code inspection + synthetic offline self-test. Main/LIVE not modified.

## Confirmed from repo main (2026-11-04 head `05fdafd9c58bd0d810033b27a37fb59615e54ea0`)

1. `real_full_r1c1_runtime_adapter.py` advertises `EXACT_RUNTIME_FDR_CACHE_220_CAPTURED_DURING_REAL_FULL` as `RUNTIME_PRICE_PROVENANCE`. `real_full_r1c1_capture_wrapper.py` invokes live `fdr_cached(code, days=220)` for the separate shadow sidecar. This demonstrates an FDR-cache path exists; it does **not** prove a bad fallback in the main LIVE prices.
2. `closebet_research/native_price_regime_forensics.py` already distinguishes `pykrx adjusted=True` (upstream NAVER) from `adjusted=False` (KRX), and describes a 2026-09-14 source/regime hypothesis. It explicitly disallows treating provider match as proof of a valid trade signal.
3. `closebet_research/krx_only_provider.py` is isolated behind a research-only environment flag and reads pre-materialized original-price KRX day records. It specifically disclaims a proven official regular-session 15:30-only price basis.
4. `research_core224/krx_truth_r1.py` has strict original OHLCV+actual traded Amount checks. Independent historical KRX reference integrity work already exists and should be reused.
5. `real_full_trust_audit.py` and `real_full_source_postrun_guard.py` normalize ticker codes by dropping all non-digits (e.g. `re.sub(r'\D', '', ...)`), whereas `historical_asof_universe.py` preserves KRX 6-character alphanumeric codes. This is an explicit cross-module identity inconsistency; whether it changed this sample's selected stocks still needs a dated case audit.
6. `today_candidates.json`, generated 2026-11-04 12:23:32, starts with placeholder `000000` in the `quick_candidates` array. Whether that placeholder reached actual production recommendations or only a preparatory file is **not established**.
7. `run_scanner.yml` sets `V1107_4_5_68_PRICE_SCALE_WATCH=0` and `V1107_4_5_70_DISABLE_PRICE_SCALE_WATCH=1`. Price-scale monitoring is disabled in this workflow, but this is *not* proof of the root cause of the anomalous Samsung SDI wave invalidation price.
8. `v25_core224_live_runtime.py` declares a research/PAPER EOD authority with actual Amount only, no `Close*Volume` substitute and a strict completion barrier. That guarantee is specific to CORE224, not all LIVE outputs.

## Hypotheses to test, NOT conclusions

- The same code/date price may have come from differing adjusted/unadjusted history, NXT/intraday/regular-session clocks, stale cache, or an unrelated price calculation error. A split/adjustment explanation for `삼성SDI 18,752` is not yet proven.
- `KRX native original` must become the *comparison baseline for completed daily Korean-stock bars*, but it cannot be used as a clock-blind replacement for intraday quotes. Corporate action normalization must be explicit; do not combine original prices with historically adjusted moving averages without transformation and clear provenance.
- `SAME-DAY KRX` missing in research is not necessarily LIVE bad data. ODOLI SOURCE_STALE remains lane-specific unless linked runtime artifacts prove otherwise.

## Gate before any production patch

- Recover exact Actions run IDs, emitted `today_candidates.json`, final selection outputs, price cache manifests, source trading dates and timestamps.
- Reproduce five scalar anomaly cases (Samsung SDI wave invalidation, TYM/Korea Electric target below entry, logged RR, source-date mismatch, placeholder code).
- For each comparison declare `code`, `signal_date`, `snapshot_kind` (INTRADAY vs EOD), `price_basis` (ORIGINAL vs ADJUSTED), provider, capture timestamp, corporate-action state, and upstream SHA.
- Only call `INVALID` on same-date, same-price-basis, same-market-session comparable records. Missing proof means `UNKNOWN`, never substitute another vendor's close.
- Use separate research branch. Do not change LIVE/SEARCH/WATCH ranking, point/score, order logic or existing research history without case-level validation and explicit approval.

## Code & outputs

`stockhunter_source_integrity_audit_r1.py --self-test` passed (synthetic fixtures only). See `README.md` for CLI and limitations. The script is fully offline; it does not fetch source data and makes no authoritative KRX data claims.
