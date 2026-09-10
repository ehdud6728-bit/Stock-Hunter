# LOW224_ACCUM_WAVE1_PB_R1

Independent research lane. It does **not** alter TRIANGLE1PB, R2C1, CORE224,
LIVE, scores, ranks, policy, or orders.

## Research question

Does the user's intended lifecycle exist as a reproducible causal structure?

`224일선 아래 소외/바닥권`
→ `실제 거래대금이 급등 1회가 아니라 서서히 증가`
→ `1차 상승파동`
→ `첫 눌림`
→ `저점 안정`
→ `재가속`

The first R1 is **structure-first**, not performance-first.

## Frozen R1 chronology

1. `LOW224_BASE`
2. `GRADUAL_AMOUNT_ACCUM`
3. `WAVE1`
4. `FIRST_PULLBACK`
5. `STABILIZATION`
6. `REACCELERATION`

The thresholds are broad, transparent audit definitions and are not optimized
from returns.

### Gradual Amount proxy

- recent 5-bar actual-Amount median >= prior 20-bar median × 1.10
- at least 3 of the recent 5 bars above the prior median
- no single recent bar > prior median × 3.0
- log actual-Amount slope > 0
- 20-bar price absolute return <= 8%

This is intended to distinguish "스물스물" inflow from a single explosive bar.
It is a hypothesis definition, not a claim of literal institutional
accumulation.

### Wave 1

The first bar within 20 bars that:
- closes above the prior 20-bar high,
- is at least +8% above the base low,
- has actual Amount >= prior Amount20 mean × 1.50,
- closes green.

### First pullback

First 3%–15% close drawdown from the running first-wave high while remaining
above the base low.

### Stabilization

A causal marker only:
- current low >= previous low,
- current close >= previous close,
- actual Amount <= wave1 Amount × 0.80.

### Reacceleration

Broad current-strength marker:
- close > previous high,
- green candle,
- actual Amount >= pullback-median Amount × 1.20.

`close >= prior wave high` is saved only as an audit tag. It is **not** part of
the R1 reacceleration gate, so this lane does not simply copy the R2C1
hypothesis.

## Data authority

Uses only the audited Stock-Hunter cache adapters from `triangle1pb_research.py`:
- historical OHLC,
- causal as-of universe,
- explicit actual Amount.

`Close × Volume` is forbidden.

No TRIANGLE pattern detector or stage/gate logic is imported.

## R1 outputs

- `low224_stage_ledger.csv`
- `low224_stage_counts.csv`
- `low224_gate_funnel.csv`
- `low224_forward_outcomes.csv`
- `low224_stage_outcome_summary.csv`
- `low224_manual_review_sample.csv`
- `low224_manual_review_bars.csv`
- `low224_authority_audit.csv`
- `low224_manifest.json`
- `low224_report.txt`

`manual_review_bars` includes future bars around sampled anchors for visual
audit only. `future_relative_to_sample_anchor=1` makes those rows explicit and
they are never used in event gates.

## First decision gate

Do **not** tune thresholds after the first result.

First inspect the manual sample and answer:
1. Are the BASE cases genuinely below/around a long-term neglected region?
2. Does `GRADUAL_AMOUNT_ACCUM` visually resemble slow inflow rather than one-day noise?
3. Is WAVE1 really a first impulse rather than a mature/late trend?
4. Is FIRST_PULLBACK the first meaningful retracement?
5. Does STABILIZATION occur near a plausible bottom or still too early?
6. Is REACCELERATION a genuine second impulse or a late chase?

Only after this structural fidelity review should any single definition be
revised, and any revision creates a new frozen revision before outcome study.


## R1.0.1 sample fidelity audit

No R1 detector threshold or chronology gate is changed.

The first full run showed that a continuing gradual-Amount condition can create
a new episode on consecutive bars. R1.0.1 therefore measures, but does not yet
collapse, those consecutive detections.

New audit outputs:
- `low224_episode_overlap_audit.csv`
- `low224_episode_overlap_summary.csv`
- `low224_stratified_manual_review_sample.csv`
- `low224_episode_review_bars.csv`

The stratified sample draws deterministic cases from:
- ACCUM_NO_WAVE
- WAVE_NO_PB
- PB_NO_STABLE
- STABLE_NO_REACCEL
- REACCEL_NO_RECLAIM
- REACCEL_RECLAIM

`low224_episode_review_bars.csv` spans from 30 bars before BASE through 15 bars
after the terminal stage, with exact stage labels on each event bar. This is
designed to inspect the entire chronology rather than only the first 15 bars
after BASE.

Consecutive-run overlap remains an audit tag only. It is not used as a gate,
dedup rule, ranking rule, score, or performance filter in R1.0.1.


## R1.0.2 run/context/wave-character audit

No detector threshold or chronology gate is changed.

R1.0.1 showed that raw accumulation dates are not independent pattern units.
R1.0.2 therefore adds a counterfactual **run-level** view while preserving the
raw detector exactly.

New outputs:
- `low224_run_level_funnel_detail.csv`
- `low224_run_level_funnel_summary.csv`
- `low224_structure_context_audit.csv`
- `low224_structure_context_summary.csv`

The context audit records, causally at BASE/WAVE1:
- 60-bar range position and drawdown from the 60-bar high,
- fraction of the prior 20 bars below MA224,
- recent 5-bar actual-Amount smoothness descriptors,
- prior 20-bar Amount spike count,
- WAVE1 latency from BASE,
- WAVE1 gap %, close-day return %, candle body %,
- WAVE1 Amount20 ratio and base-low gain.

These are descriptive audit variables only. No range-position, gap, Amount-ratio
upper bound, MA224-duration, or run-dedup rule is promoted to the detector in
R1.0.2.

The purpose is to decide whether the first conceptual error is:
1. the pattern unit (raw daily accumulation vs accumulation run),
2. BASE being insufficiently "neglected/long-bottom",
3. the Amount proxy not being smooth enough,
4. WAVE1 accepting news-like explosive jumps.

Only one structural definition should be revised in the next frozen revision,
and only after this audit.


## R1.0.3 raw accumulation persistence audit

No detector threshold or chronology gate is changed.

R1.0.2 revealed that the previously reported `consecutive accumulation runs`
were based on **emitted detector episodes**. That view is useful for duplicate
counting, but it is not an unbiased measure of accumulation persistence:
when the detector finds WAVE1/PB/etc., `next_allowed_i` advances and subsequent
BASE emissions are intentionally suppressed.

R1.0.3 therefore recomputes the existing `GRADUAL_AMOUNT_ACCUM` mask on every
eligible historical bar, independently of detector state.

New outputs:
- `low224_raw_accum_pass_detail.csv`
- `low224_raw_accum_run_detail.csv`
- `low224_raw_accum_episode_context.csv`
- `low224_raw_accum_persistence_summary.csv`

For every existing WAVE1 episode it records:
- accumulation-pass count in the 10 and 20 bars before WAVE1,
- longest raw accumulation-pass streak in the 20 bars before WAVE1,
- pass count from BASE toward WAVE1,
- raw run length containing the BASE anchor.

This audit is causal and uses the existing R1 accumulation definition exactly.
It does not use outcomes and does not promote persistence into a gate.

Decision rule after R1.0.3:
- if raw pre-WAVE persistence is genuinely repeated, keep C and move to the
  next structural stage;
- if raw persistence is mostly isolated/noisy, revise only
  `GRADUAL_AMOUNT_ACCUM` in R1.1 using a semantic persistence definition,
  without tuning from returns.


## R1.0.4 first-pullback anatomy audit

No detector threshold or chronology gate is changed.

R1.0.3 showed that the existing accumulation proxy is not merely a single-day
spike. A raw pass already describes a 5-bar Amount state, and the independent
raw mask has median run length 2, with repeated pre-WAVE passes. The next
structural concern is therefore the very high `WAVE1 -> FIRST_PULLBACK`
transition rate.

R1.0.4 audits the existing FIRST_PULLBACK without changing it.

New outputs:
- `low224_first_pullback_anatomy_detail.csv`
- `low224_first_pullback_anatomy_summary.csv`
- `low224_first_pullback_review_sample.csv`

Causal pullback descriptors:
- WAVE1 -> FIRST_PULLBACK trading-bar delay,
- drawdown from running wave high,
- FIRST_PULLBACK Amount / WAVE1 Amount,
- pullback-path median/max Amount / WAVE1 Amount,
- FIRST_PULLBACK Amount / pre-WAVE Amount20 mean,
- close/low relative to the WAVE1 prior-20-bar breakout high,
- candle direction and path red-close fraction.

A separately named future taxonomy records whether a lower low occurs within
the next 5 bars. It is explicitly future-only and never used as a gate.

The purpose is to decide whether `FIRST_PULLBACK` currently means a meaningful
first retracement or merely almost any routine 3% post-breakout fluctuation.
No support, Amount-contraction, or future-lower-low field is promoted to a
filter in R1.0.4.


## R1.0.5 stabilization anatomy audit

No detector threshold or chronology gate is changed.

R1.0.4 showed that FIRST_PULLBACK behaves as an early chronology marker:
- most WAVE1 episodes eventually reach a 3-15% first retracement,
- FIRST_PULLBACK occurs quickly,
- and a later lower low is common.

Adding breakout-support or Amount-contraction requirements to FIRST_PULLBACK
would mix the role of "pullback has begun" with the later role of
STABILIZATION. Those causal descriptors also did not materially separate the
future lower-low taxonomy in R1.0.4.

R1.0.5 therefore keeps FIRST_PULLBACK unchanged and audits STABILIZATION.

New outputs:
- `low224_stabilization_anatomy_detail.csv`
- `low224_stabilization_anatomy_summary.csv`
- `low224_stabilization_review_sample.csv`

Causal stabilization descriptors:
- FIRST_PULLBACK -> STABILIZATION delay,
- STABILIZATION Amount / WAVE1 Amount,
- STABILIZATION Amount / FIRST_PULLBACK Amount,
- path Amount contraction from PB to STABLE,
- STABLE close/low relative to PB close/low,
- STABLE close/low relative to the original breakout high,
- recovery from the PB-to-STABLE path minimum.

Explicit future taxonomy, never gates:
- lower low vs STABLE within 5 bars,
- break of original PB low within 5 bars,
- close above STABLE high within 5 bars,
- wave-high reclaim within 8 bars,
- whether the existing REACCELERATION stage later appears.

The decision after R1.0.5 is whether STABILIZATION is a credible causal
"bottom is becoming stable" marker. If not, R1.1 may revise only the
STABILIZATION definition. FIRST_PULLBACK remains the early pullback-start
marker.


## R1.0.6 reacceleration anatomy audit

No detector threshold or chronology gate is changed.

R1.0.5 showed that current STABILIZATION is still an early structural marker:
many cases later make another low. A causal `close > FIRST_PULLBACK high`
descriptor reduced later PB-low breaks and increased wave-high reclaim, but
same-sample D15 return quality did not improve. It is therefore not promoted to
STABILIZATION.

R1.0.6 moves to the stage that is supposed to represent the actual second-wave
restart: REACCELERATION.

New outputs:
- `low224_reacceleration_anatomy_detail.csv`
- `low224_reacceleration_anatomy_summary.csv`
- `low224_reacceleration_time_split_audit.csv`

Causal descriptors:
- STABILIZATION -> REACCELERATION delay,
- REACCEL Amount vs pullback median / WAVE1 / STABILIZATION / FIRST_PULLBACK,
- close relative to prior wave high, PB high, STABLE high, breakout high,
- low relative to PB and STABLE lows,
- gap %, candle body %, one-day return %.

Explicit future taxonomy only:
- lower low vs REACCEL within 5 bars,
- break STABLE or PB low within 5 bars,
- +5% MFE within 5/10 bars,
- future wave-high reclaim for non-reclaim cases.

The existing `reaccel_reclaims_prior_wave_high` tag is also compared in
FULL/H1/H2. This remains post-hoc on the same discovery sample and is not a
filter. If a structurally coherent tag is stable across H1/H2, the next step is
to freeze it as a new candidate revision before prospective/OOS validation,
not to optimize thresholds further on this sample.


## R1.1 R1C1 prospective shadow freeze

R1.0.6 completes the structure-first discovery audit. No R1 detector threshold
or chronology gate is changed in R1.1.

Frozen candidate:
`LOW224_R1C1_REACCELERATION_WAVE_HIGH_RECLAIM`

Definition:
- existing LOW224 R1 chronology through `REACCELERATION`,
- and REACCELERATION close is at or above the prior wave high calculated
  strictly before the REACCELERATION bar.

Frozen contemporaneous control:
`LOW224_R1C1_REACCELERATION_NO_WAVE_HIGH_RECLAIM`

Definition:
- the same existing REACCELERATION chronology,
- REACCELERATION close remains below the prior wave high.

Freeze date: `2026-09-01`
Prospective/OOS start: `2026-09-02`

All historical candidate/control rows are permanently labeled
`DISCOVERY_OR_PRE_FREEZE_NOT_VALIDATION`.

Rationale for freezing, not tuning:
- FULL candidate/reclaim group materially reduced subsequent PB-low breaks,
- that structural direction persisted in H1 and H2,
- +5% within 10 bars was directionally better in FULL and especially H2,
- but D15 close-return quality was not better.

Therefore reclaim is treated as a **structural second-wave confirmation
hypothesis**, not as an optimized return filter.

New outputs:
- `low224_r1c1_candidate_definition.csv`
- `low224_r1c1_candidate_shadow_detail.csv`
- `low224_r1c1_candidate_shadow_summary.csv`
- `low224_r1c1_prospective_control_detail.csv`
- `low224_r1c1_prospective_control_summary.csv`
- `low224_r1c1_oos_readiness.csv`

Readiness is fail-closed:
- `WAIT_DATA_CATCHUP`
- `FAIL_NO_ELIGIBLE_ASOF_UNIVERSE`
- `FAIL_STALE_ASOF_UNIVERSE`
- `READY_PROSPECTIVE_OOS`

Only when readiness is `READY_PROSPECTIVE_OOS` may zero candidate/control rows
be interpreted as genuine "no new event".

No additional tuning of R1C1 is allowed on the discovery sample. Any future
candidate revision must be separate and cannot overwrite R1C1.


## R1.1.1 current-overlay handoff freshness guard

# CORE224 → TRIANGLE1PB / LOW224 current-overlay handoff freshness guard

Infrastructure only. No CORE224 PRIMARY, TRIANGLE R2C1, LOW224 R1C1,
detector threshold, chronology, score/rank, LIVE decision, or order behavior is changed.

Root cause:
GitHub Actions cache versions include the cached path set. The CORE224 weekly
seed bridge saved only as-of + live-state paths, while TRIANGLE1PB/LOW224
restored price + as-of + actual-Amount + live-state. The fresh weekly cache was
therefore invisible to research consumers, which could fall back to an older
V21 price cache.

Fix:
1. CORE224 weekly bridge now saves the exact four-path current-overlay set.
2. CORE224 writes current_overlay_handoff_authority.json from seed_meta.
3. Rolling TRIANGLE1PB/LOW224 runs require that authority.
4. Producer snapshot date is compared with the research script's actual data_end.
5. producer > data_end => FAIL_CURRENT_OVERLAY_HANDOFF_LAG.
6. no rolling authority => FAIL_CURRENT_OVERLAY_AUTHORITY_MISSING.
7. candidate/control zero is meaningful only at READY_PROSPECTIVE_OOS.
8. Explicit historical end-date runs remain available without the rolling guard.


## R1.1.2 append-only OOS ledger

# Append-only OOS ledger authority

This revision fixes prospective-validation bookkeeping only. It does not change
any detector, threshold, chronology, candidate definition, score/rank, LIVE
logic, CORE224 PRIMARY, or order behavior.

## Problem found

A full-history rerun can revise older episode membership when the right edge
advances or when a rolling price-cache warm-up boundary shifts. Therefore a row
with `event_date >= prospective_start` is not automatically valid OOS evidence:
an event dated yesterday can be created only today.

## OOS membership authority

Only a candidate/control event visible on its own event date can be admitted.

- append-only membership;
- first-observed candidate/control group is immutable;
- older rows created by later recomputation are
  `retroactive_recomputed_not_admitted`;
- group flips cannot rewrite an admitted ledger event;
- D5/D10/D15 outcomes may mature later without changing membership.

Confirmed zero-event bootstrap through: `2026-09-02`.

## Frozen discovery baselines

TRIANGLE R2: 7 reclaim / 9 no-reclaim
SHA256 `65ac0f184045ae57a58e48f5b16042e79790398ffc22e5ef4686e89fed5068d5`

LOW224 R1C1: 251 reclaim / 336 no-reclaim
SHA256 `1999b2bdf6bd172642bf5348b778d03b6184afe28c6357c5a103a053cb731c8d`

Rolling historical recomputation is compared with these baselines only as an
audit. It is never OOS membership authority.
