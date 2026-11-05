# Stock-Hunter Source Integrity Audit R1 — 2026-11-05

**Mode:** offline `RESEARCH_ONLY` / read-only. This module **does not** import, call, patch, or mutate LIVE/SEARCH/WATCH, ranking, Telegram, or order logic. It only reads supplied local files and writes diagnostics to a new report directory.

## Purpose

First-pass source provenance and candidate snapshot QA before changing any price provider. It checks malformed/placeholder ticker identities (including `000000`), duplicate candidates, invalid prices, mismatched signal-date labels, and optionally compares EOD unadjusted candidate closing prices against an independently captured per-session KRX reference file.

The existing repository already includes research tools `closebet_research/native_price_regime_forensics.py`, `closebet_research/krx_native_gate.py`, and `research_core224/krx_truth_r1.py`. This script **does not replace** their historical OHLCV/amount parity checks. It is a lightweight safety front-end for daily candidates and intentionally has no network fetches.

## Usage

```bash
python stockhunter_source_integrity_audit_r1.py --self-test

# Candidate-only QA: KRX parity remains unverified.
python stockhunter_source_integrity_audit_r1.py \
  --candidate-json today_candidates.json \
  --snapshot-kind UNKNOWN --price-basis UNKNOWN \
  --out-dir reports/source_integrity_r1

# Only when the candidate is a confirmed EOD snapshot in the same unadjusted basis.
python stockhunter_source_integrity_audit_r1.py \
  --candidate-json today_candidates.json \
  --signal-date 2026-11-04 \
  --snapshot-kind EOD --price-basis ORIGINAL \
  --krx-csv /path/to/independently_verified_krx_original.csv \
  --out-dir reports/source_integrity_r1_eod
```

The reference CSV needs columns `date,code,open,high,low,close,volume,amount`, one record per `(date, code)`.

### Important semantics

- A provided CSV does **not** automatically become certified KRX data; upstream manifest, SHA-256, source session and capture-time evidence must separately establish that claim.
- If `snapshot-kind=INTRADAY` or `price-basis` is not `ORIGINAL`, exact KRX EOD parity is **not** attempted. There is no false contamination finding from different time/adjustment bases.
- Distinguish **regular-session close** vs intraday/NXT/after-hours quote. Missing KRX sessions, unverified EOD close, split/adjusted historical prices, and different publisher clocks must be investigated, never silently filled.
- `UNKNOWN`, `TIME_BASIS_INCOMPARABLE`, or `PRICE_BASIS_INCOMPARABLE` is **not** a pass. It requires evidence, rather than a guessed repair.
- The JSON output should be treated as research evidence only, never fed back as LIVE candidate authority.

## Files

- `candidate_integrity.csv`: individual row diagnostics.
- `audit_summary.json`: source hashes, counts, flags, and comparison status.

## Next independent tests

1. Retrieve the **exact GitHub Actions run ID**, source artifacts and date/clock metadata for the Telegram sample; map each LIVE price field to its source loader and authoritative cache.
2. Use verified KRX full-market daily `(date, code)` OHLCV + **actual Amount** as the independent *EOD baseline*; preserve unadjusted/adjusted histories as separate named columns.
3. Only after matching source dates and time bases, test high/low/close/volume/amount strict equality and identify whether a mismatch comes from KRX, NAVER, FDR, pykrx adjusted path, another cache, or a stale quote.
4. Audit pricing-derived fields (BB40, MA, Fibonacci, blue line, trendline identity, targets, stops, RR) and trace origin dates/anchors. Do not patch strategy heuristics in the same commit.

**Status:** self-test passed on synthetic fixtures; no claim of completed live-data parity or root-cause identification.
