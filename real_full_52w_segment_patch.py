#!/usr/bin/env python3
from pathlib import Path

P=Path(".github/workflows/run_scanner.yml")
if not P.exists():
    raise SystemExit("MISSING run_scanner.yml")
s=P.read_text(encoding="utf-8")

old="${{ inputs.run_profile == 'REAL_FULL_VALIDATION_BACKTEST' && 'ROLLING' || inputs.v25_cohort_mode || 'ROLLING' }}"
new="${{ inputs.run_profile == 'REAL_FULL_VALIDATION_BACKTEST' && inputs.v25_cohort_mode == 'CUSTOM' && 'CUSTOM' || (inputs.run_profile == 'REAL_FULL_VALIDATION_BACKTEST' && 'ROLLING' || inputs.v25_cohort_mode || 'ROLLING') }}"
n=s.count(old)
if n < 2:
    raise SystemExit(f"validation mode anchors missing: {n}")
s=s.replace(old,new)

old_job='''    runs-on: ubuntu-latest
    timeout-minutes: 180
    permissions:
      contents: read
    env:
      TZ: Asia/Seoul
      PYTHONUNBUFFERED: "1"
      PYTHONPATH: ${{ github.workspace }}:${{ github.workspace }}/scanner
      STOCKHUNTER_WEEKLY_BACKTEST_ONLY: "1"
      STOCKHUNTER_HAM_1503_ONLY: "0"
      V1081_BACKTEST_SOURCE: DIRECT_REPLAY
'''
new_job='''    runs-on: ubuntu-latest
    timeout-minutes: ${{ inputs.run_profile == 'REAL_FULL_VALIDATION_BACKTEST' && 300 || 180 }}
    permissions:
      contents: read
    env:
      TZ: Asia/Seoul
      PYTHONUNBUFFERED: "1"
      PYTHONPATH: ${{ github.workspace }}:${{ github.workspace }}/scanner
      STOCKHUNTER_WEEKLY_BACKTEST_ONLY: "1"
      STOCKHUNTER_HAM_1503_ONLY: "0"
      V1081_BACKTEST_SOURCE: DIRECT_REPLAY
'''
if old_job not in s:
    raise SystemExit("primary shard job anchor missing")
s=s.replace(old_job,new_job,1)

old_worker='''      - name: Run V23 materialized shard worker
        id: v23-shard-worker
        timeout-minutes: 155
        shell: bash
'''
new_worker='''      - name: Run V23 materialized shard worker
        id: v23-shard-worker
        timeout-minutes: ${{ inputs.run_profile == 'REAL_FULL_VALIDATION_BACKTEST' && 270 || 155 }}
        shell: bash
'''
if old_worker not in s:
    raise SystemExit("primary worker anchor missing")
s=s.replace(old_worker,new_worker,1)

old_parent='''  run-scanner:
    needs: [v23-backtest-shard, v23-backtest-shard-a, v23-backtest-shard-b, v23-backtest-shard-c, v23-backtest-shard-d]
    if: ${{ always() && github.event.schedule != '20 06 * * 1-5' && (github.event_name != 'workflow_dispatch' || inputs.run_profile != 'TRIANGLE1PB_RESEARCH') }}
    runs-on: ubuntu-latest
    timeout-minutes: 240
'''
new_parent='''  run-scanner:
    needs: [v23-backtest-shard, v23-backtest-shard-a, v23-backtest-shard-b, v23-backtest-shard-c, v23-backtest-shard-d]
    if: ${{ always() && github.event.schedule != '20 06 * * 1-5' && (github.event_name != 'workflow_dispatch' || inputs.run_profile != 'TRIANGLE1PB_RESEARCH') }}
    runs-on: ubuntu-latest
    timeout-minutes: ${{ inputs.run_profile == 'REAL_FULL_VALIDATION_BACKTEST' && 300 || 240 }}
'''
if old_parent not in s:
    raise SystemExit("parent timeout anchor missing")
s=s.replace(old_parent,new_parent,1)

for cron in ["cron: '30 00 * * 1-5'","cron: '20 06 * * 1-5'","cron: '45 06 * * 1-5'"]:
    assert cron in s
assert new in s
P.write_text(s,encoding="utf-8")
print("REAL_FULL_52W_SEGMENT_PATCH PASS", n)
