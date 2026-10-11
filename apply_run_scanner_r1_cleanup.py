from pathlib import Path
import re, shutil, sys

SRC = Path('.github/workflows/run_scanner.yml')
REUSABLE = Path('.github/workflows/V23_COHORT_SHARD_REUSABLE_R1.yml')
BACKUP = Path('.github/workflows/run_scanner.pre_r1_cleanup.yml.bak')

if not SRC.exists():
    raise SystemExit('ERROR: .github/workflows/run_scanner.yml not found')

text = SRC.read_text(encoding='utf-8')
original = text

# Safety fingerprints from the audited current file.
required_markers = [
    'name: Run Main Scanner V1107.4.5.73.3.6.6.25.4.12-HF12 + TRIANGLE1PB R1 Independent Research',
    '  v23-backtest-shard-a:\n',
    '  v23-backtest-shard-b:\n',
    '  v23-backtest-shard-c:\n',
    '  v23-backtest-shard-d:\n',
    '  triangle1pb-research:\n',
    '  real-full-trust-fallback-watchdog:\n',
    '  run-scanner:\n',
]
missing = [m for m in required_markers if m not in text]
if missing:
    raise SystemExit('ERROR: audited structure mismatch; missing: ' + repr(missing))

# Preserve a local backup before transformation.
BACKUP.parent.mkdir(parents=True, exist_ok=True)
shutil.copy2(SRC, BACKUP)

# Helpers to isolate top-level jobs exactly.
def job_block(src: str, name: str):
    pat = re.compile(rf'^  {re.escape(name)}:\n', re.M)
    m = pat.search(src)
    if not m:
        raise SystemExit(f'ERROR: job not found: {name}')
    nextm = re.compile(r'^  [A-Za-z0-9_-]+:\n', re.M).search(src, m.end())
    end = nextm.start() if nextm else len(src)
    return m.start(), end, src[m.start():end]

# Extract shard-A implementation as the single reusable authority.
_, _, a_block = job_block(text, 'v23-backtest-shard-a')
# Remove top-level job key, then deindent its job body by two spaces.
a_body = a_block.split('\n', 1)[1]
a_body = ''.join((ln[2:] if ln.startswith('  ') else ln) + '\n' for ln in a_body.splitlines())

# Reusable workflow must not keep caller-only gate/needs; callers retain those.
a_body = re.sub(r'^  if:.*\n', '', a_body, count=1, flags=re.M)
a_body = re.sub(r'^  needs:.*\n', '', a_body, count=1, flags=re.M)

# Parameterize cohort-specific values while preserving all existing logic.
a_body = a_body.replace('V23_HANDOFF_COHORT_ID: "A"', 'V23_HANDOFF_COHORT_ID: ${{ inputs.cohort_id }}')
a_body = a_body.replace('V25_COHORT_MODE: "A"', 'V25_COHORT_MODE: ${{ inputs.cohort_id }}')

# Caller workflow inputs used inside shard-A -> reusable workflow inputs.
repl = {
    "${{ inputs.direct_top_n || '500' }}": "${{ inputs.direct_top_n }}",
    "${{ inputs.direct_limit_per_date || '15' }}": "${{ inputs.direct_limit_per_date }}",
    "${{ inputs.backtest_weeks || '24' }}": "${{ inputs.backtest_weeks }}",
    "${{ inputs.backtest_hold_days || '10' }}": "${{ inputs.backtest_hold_days }}",
    "${{ inputs.backtest_stop_pct || '-5' }}": "${{ inputs.backtest_stop_pct }}",
    "${{ inputs.backtest_stop_mode || 'SIGNAL_STOP_WITH_FALLBACK' }}": "${{ inputs.backtest_stop_mode }}",
    "${{ inputs.v25_two_year_end_date || '' }}": "${{ inputs.v25_two_year_end_date }}",
    "${{ inputs.v25_cohort_start_date || '' }}": "${{ inputs.v25_cohort_start_date }}",
    "${{ inputs.v25_cohort_end_date || '' }}": "${{ inputs.v25_cohort_end_date }}",
}
for k, v in repl.items():
    a_body = a_body.replace(k, v)

reusable_header = '''name: V23 Cohort Shard Reusable R1\n\non:\n  workflow_call:\n    inputs:\n      cohort_id:\n        required: true\n        type: string\n      direct_top_n:\n        required: false\n        type: string\n        default: "500"\n      direct_limit_per_date:\n        required: false\n        type: string\n        default: "15"\n      backtest_weeks:\n        required: false\n        type: string\n        default: "24"\n      backtest_hold_days:\n        required: false\n        type: string\n        default: "10"\n      backtest_stop_pct:\n        required: false\n        type: string\n        default: "-5"\n      backtest_stop_mode:\n        required: false\n        type: string\n        default: "SIGNAL_STOP_WITH_FALLBACK"\n      v25_two_year_end_date:\n        required: false\n        type: string\n        default: ""\n      v25_cohort_start_date:\n        required: false\n        type: string\n        default: ""\n      v25_cohort_end_date:\n        required: false\n        type: string\n        default: ""\n\npermissions:\n  contents: read\n\njobs:\n  shard:\n'''
# indent existing body under jobs.shard
indented = ''.join(('    ' + ln if ln.strip() else ln) + '\n' for ln in a_body.splitlines())
reusable = reusable_header + indented
REUSABLE.write_text(reusable, encoding='utf-8')

# Build thin callers, preserving exact A->B->C->D sequencing.
def caller(name, cohort, needs=None):
    lines = [f'  {name}:']
    if needs:
        lines.append(f'    needs: {needs}')
    lines += [
        "    if: ${{ always() && github.event_name == 'workflow_dispatch' && inputs.run_profile == 'WEEKLY_BACKTEST' && (inputs.backtest_source == 'DIRECT_REPLAY' || inputs.backtest_source == '') && inputs.v25_cohort_mode == 'ALL' }}",
        '    uses: ./.github/workflows/V23_COHORT_SHARD_REUSABLE_R1.yml',
        '    with:',
        f'      cohort_id: "{cohort}"',
        "      direct_top_n: ${{ inputs.direct_top_n || '500' }}",
        "      direct_limit_per_date: ${{ inputs.direct_limit_per_date || '15' }}",
        "      backtest_weeks: ${{ inputs.backtest_weeks || '24' }}",
        "      backtest_hold_days: ${{ inputs.backtest_hold_days || '10' }}",
        "      backtest_stop_pct: ${{ inputs.backtest_stop_pct || '-5' }}",
        "      backtest_stop_mode: ${{ inputs.backtest_stop_mode || 'SIGNAL_STOP_WITH_FALLBACK' }}",
        "      v25_two_year_end_date: ${{ inputs.v25_two_year_end_date || '' }}",
        "      v25_cohort_start_date: ${{ inputs.v25_cohort_start_date || '' }}",
        "      v25_cohort_end_date: ${{ inputs.v25_cohort_end_date || '' }}",
        '    secrets: inherit',
        '',
    ]
    return '\n'.join(lines)

new_callers = (
    caller('v23-backtest-shard-a', 'A') +
    caller('v23-backtest-shard-b', 'B', 'v23-backtest-shard-a') +
    caller('v23-backtest-shard-c', 'C', 'v23-backtest-shard-b') +
    caller('v23-backtest-shard-d', 'D', 'v23-backtest-shard-c')
)

# Replace A-D blocks in one contiguous operation.
a_start, _, _ = job_block(text, 'v23-backtest-shard-a')
_, d_end, _ = job_block(text, 'v23-backtest-shard-d')
text = text[:a_start] + new_callers + text[d_end:]

# Remove legacy direct TRIANGLE job; dedicated workflow is now authoritative.
t_start, t_end, _ = job_block(text, 'triangle1pb-research')
text = text[:t_start] + text[t_end:]

# Remove TRIANGLE1PB_RESEARCH option from run_profile choices only.
text, n = re.subn(r'^\s{10}- TRIANGLE1PB_RESEARCH\n', '', text, count=1, flags=re.M)
if n != 1:
    raise SystemExit('ERROR: failed to remove TRIANGLE1PB_RESEARCH run_profile option')

# Remove legacy TRIANGLE-only dispatch inputs.
for key, nxt in [
    ('tri_start_date', 'tri_end_date'),
    ('tri_end_date', 'tri_max_codes'),
    ('tri_max_codes', 'tri_manual_sample_limit'),
    ('tri_manual_sample_limit', 'v73_universe_limit'),
]:
    pattern = rf'^      {key}:\n[\s\S]*?(?=^      {nxt}:\n)'
    text, n = re.subn(pattern, '', text, count=1, flags=re.M)
    if n != 1:
        raise SystemExit(f'ERROR: failed to remove legacy TRIANGLE input {key}')

# Simplify run-scanner gate now that direct TRIANGLE profile no longer exists.
old_gate = "if: ${{ always() && github.event.schedule != '20 06 * * 1-5' && (github.event_name != 'workflow_dispatch' || inputs.run_profile != 'TRIANGLE1PB_RESEARCH') }}"
new_gate = "if: ${{ always() && github.event.schedule != '20 06 * * 1-5' }}"
if old_gate not in text:
    raise SystemExit('ERROR: legacy run-scanner TRIANGLE exclusion gate not found')
text = text.replace(old_gate, new_gate, 1)

# R1 also carries the already-diagnosed CORE224 timeout hardening.
needle = "      - name: Refresh current CORE224 LIVE seed (lightweight no-hold, one date)\n        if: env.STOCKHUNTER_CORE224_LIVE_ONLY == '1'\n        timeout-minutes: 45\n"
replacement = "      - name: Refresh current CORE224 LIVE seed (lightweight no-hold, one date)\n        if: env.STOCKHUNTER_CORE224_LIVE_ONLY == '1'\n        timeout-minutes: 90\n"
if needle in text:
    text = text.replace(needle, replacement, 1)
elif 'Refresh current CORE224 LIVE seed (lightweight no-hold, one date)' in text and 'timeout-minutes: 90' in text:
    pass
else:
    raise SystemExit('ERROR: CORE224 timeout block not recognized')

# Update top name minimally to make cleanup revision visible.
text = text.replace(
    'name: Run Main Scanner V1107.4.5.73.3.6.6.25.4.12-HF12 + TRIANGLE1PB R1 Independent Research',
    'name: Run Main Scanner V1107.4.5.73.3.6.6.25.4.12-HF12 R1 CLEANUP',
    1,
)

# Structural invariants.
assert 'triangle1pb-research:' not in text
assert 'TRIANGLE1PB_RESEARCH' not in text
assert text.count('uses: ./.github/workflows/V23_COHORT_SHARD_REUSABLE_R1.yml') == 4
assert 'v23-backtest-shard-a:' in text and 'v23-backtest-shard-d:' in text
assert 'needs: v23-backtest-shard-a' in text
assert 'needs: v23-backtest-shard-b' in text
assert 'needs: v23-backtest-shard-c' in text
assert 'timeout-minutes: 90' in text
assert 'real-full-trust-fallback-watchdog:' in text
assert 'run-scanner:' in text

SRC.write_text(text, encoding='utf-8')

print('R1_CLEANUP_PASS')
print('original_bytes=', len(original.encode('utf-8')))
print('cleaned_bytes=', len(text.encode('utf-8')))
print('saved_bytes=', len(original.encode('utf-8')) - len(text.encode('utf-8')))
print('backup=', BACKUP)
print('reusable=', REUSABLE)
print('run_scanner=', SRC)
