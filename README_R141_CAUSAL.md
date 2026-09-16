# REAL_FULL R1C1 R1.4.1 Causal Daily Artifact Adapter

## Purpose

This package completes the prospective R1C1 shadow without changing REAL_FULL search, score, rank, order, or order-routing logic.

Frozen research rule:

- scope: `v72_pullback_restart_score == 100`
- R1C1 positive: `median(PB daily Volume) / median(Wave1 daily Volume) < 1.0`
- MA224: `CONTEXT_ONLY_NOT_GATE`
- production eligibility: false
- same-sample retuning: prohibited

## Important change from the rejected first R1.4.1 draft

The earlier draft called `fdr_cached(code, days=220)` again at graceful shutdown. That could fetch a later signal-day OHLCV/Volume snapshot than the one used by REAL_FULL.

This package does **not** refetch.

At graceful shutdown, after the final REAL_FULL Top15 order is already frozen, the adapter reads the existing legacy scanner `_fdr_cache` entry for the exact key:

`<code>_220_<signal YYYYMMDD>`

It then:

1. verifies no price row is later than `signal_date`;
2. requires the captured signal-day row to be present and the maximum price date to equal `signal_date`;
3. reuses the existing V72 detector against that exact captured frame;
4. runs `CAUSAL_LOW_HIGH_PULLBACK_V1` against the same captured frame;
5. freezes Wave1/PB volume medians and `pb_volume_vs_wave1` from that frame;
6. emits research-only sidecar files;
7. never performs a network refetch for the predictor.

If any Top15 stock lacks the exact 220-day runtime cache, the whole R1.4.1 sidecar is `FAIL_CLOSED`. This is intentional: an unknown score cannot be treated as non-score100.

An anchor that is validly serialized but **R1C1-ineligible** (for example, `pullback_low_date == signal_date`) does not kill the whole day. It remains an append-only score100 observation and the tracker marks it `INELIGIBLE`. This preserves the historical R1.3 behavior: 96 score100 events, 95 causal-eligible.

## Files emitted by a successful daily REAL_FULL run

- `reports/real_full_r1c1_v72_runtime_sidecar.csv`
- `reports/v72_formula_selector_anchor_serialization.csv`
- `reports/real_full_r1c1_v72_runtime_sidecar_meta.json`
- `reports/real_full_r1c1_v72_runtime_sidecar_report.txt`

The existing R1.4 workflow can continue receiving `real_full_trust_source.csv`. The patched tracker automatically resolves the same-artifact R1.4.1 runtime sidecar as the V72-score authority.

The patched tracker also prefers the already-frozen Wave1/PB volume metrics from the R1.4.1 anchor file, so predictor construction itself does not refetch later data.

## Historical validation authority

Use:

- REAL_FULL Run #1390 source artifact (`34799606739`)
- R1.3 artifact run (`34919078900`)

Command example after extracting both artifacts:

```bash
python real_full_r1c1_historical_replay_validation.py \
  --r13-ledger r13/real_full_structure_research_r13/r13_event_ledger.csv \
  --run1390-anchor run1390/reports/v72_formula_selector_anchor_serialization.csv \
  --r1c1-ledger r13/real_full_structure_research_r13/r1c1_shadow_historical_event_ledger.csv \
  --run1390-materialized-dir run1390/reports/v23_materialized \
  --out-json reports/r141_historical_replay_validation.json
```

Required identity:

- 115 events
- Run #1390 materialized V72 capped score exact = 115/115
- Run #1390 materialized V72 raw score exact = 115/115
- V72 score100 = 96
- causal eligible = 95
- PB_CONTRACTION = 51
- NO_PB_CONTRACTION = 44
- contraction +5% hits = 36/51 = 70.5882%
- non-contraction +5% hits = 21/44 = 47.7273%
- score100 anchor event join = 96/96
- all frozen anchor fields event-by-event exact against Run #1390 serialization

## Installation

Upload the package files to the repository preserving paths. In particular upload:

- root Python/JSON files
- `.github/workflows/real_full_r1c1_r141_install.yml`

A root-level copy of the workflow is included only for easy inspection.

Then manually dispatch **Install REAL_FULL R1C1 R1.4.1 Causal Adapter** once.

Unlike the prior installer, this installer does not modify any workflow file. It only patches:

- `real_full_capture_runner.py`
- `real_full_r1c1_oos_tracker.py`

Therefore its `git push` requires normal `contents: write`, not GitHub's extra workflow-file permission that caused Run `35059829919` to fail.

## Prospective operation

After install:

1. normal daily REAL_FULL runs unchanged;
2. the existing bridge captures the final Top15;
3. only after final rank capture, R1.4.1 builds its causal research sidecar;
4. dispatch existing R1.4 `LOCK` with that day's REAL_FULL run ID;
5. R1.4 appends immutable score100 R1C1 observations;
6. after D+5, run existing `MATURE` against the latest tracker state.

No auto-order path is added.

## Do not use the superseded installer

The older workflow `Install REAL_FULL R1C1 R1.4.1 Runtime Sidecar` was based on shutdown-time refetch and its patch push also failed on workflow-file permissions. Do not rerun that installer. Use **Install REAL_FULL R1C1 R1.4.1 Causal Adapter** from this package.
