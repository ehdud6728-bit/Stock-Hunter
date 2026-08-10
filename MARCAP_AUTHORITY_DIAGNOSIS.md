# MARCAP AUTHORITY DIAGNOSIS — v49.76

## Resolved production issue

The v49.72.2 same-period reruns showed identical OHLCV/Amount history with different MARCAP maps and raw populations (~56.9k vs ~85.2k). v49.74 fixed this by resolving the real KRX trading day before the MARCAP request and freezing one Prepare-owned MARCAP snapshot through every shard and merge.

The real v49.74 run resolved:

- requested end: `2026-08-09` (Sunday)
- verified trading-asof: `2026-08-07`
- MARCAP SHA: `9b913f940ff41ec8`
- raw population: `85,181`
- Prepare -> 8 shards -> Merge: consensus valid

This is the production/search authority retained by v49.76.

## Remaining research question

A fixed END-DATE MARCAP can still leak future size information into older signals. Example: a stock may be >=2,000억 on 2026-08-07 but only 1,200억 on a 2025 signal date. If it is not in the fixed index override, that old signal is admitted using information unavailable at the time.

v49.76 measures this direction with signal-date MARCAP.

## What v49.76 does not change

- It does not replace the production universe gate.
- It does not change authoritative predicates.
- It does not reconstruct historical index membership.
- It does not add reverse-direction signals that never entered the existing raw population.
- It does not auto-apply any PIT result.

The output is a diagnostic evidence layer for deciding whether a future full point-in-time universe replay is worth the runtime and complexity.
