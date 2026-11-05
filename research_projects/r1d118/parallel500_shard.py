#!/usr/bin/env python3
"""R118 independent 500-cohort 5-way research shard; no LIVE/SEARCH/WATCH changes.

One shard handles exactly 100 predetermined 2022-asof codes, computes canonical
R111->R117 sources and R118 events, then materializes an immutable artifact.
The original 500-stock manifest is supplied to canonical R118 so that global
identity/provenance and shard assignments are tested by the original engine.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import shutil
import sys
from pathlib import Path

import pandas as pd

from auto_data import load_pinned_sources, build_inputs
from research_r1d118 import main as run_research

HERE = Path(__file__).resolve().parent
FROZEN_200 = HERE / 'FROZEN_2022_200_COHORT_20261104.json'
FILE_NAMES = ('R118_READINESS_AUDIT.json', 'R118_EVENT_LEDGER_PIT.csv',
              'R118_ALL_ANCHOR_DENOMINATORS.csv')


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def partition_manifest(full_manifest, index, count=5):
    """The canonical R118 shard formula: sorted 500 codes, index % 5."""
    if count != 5 or not 0 <= index < count:
        raise ValueError('R118_500_REQUIRES_EXACT_FIVE_SHARDS')
    df = pd.read_csv(full_manifest, dtype=str).fillna('')
    if len(df) != 500 or set(('code', 'path', 'sha256')) - set(df):
        raise ValueError('R118_500_INVALID_GLOBAL_MANIFEST')
    if df.code.duplicated().any() or df.code.tolist() != sorted(df.code.tolist()):
        raise ValueError('R118_500_NON_CANONICAL_STOCK_ORDER')
    selected = df.iloc[index::count].reset_index(drop=True)
    if len(selected) != 100:
        raise AssertionError('R118_500_SHARD_NOT_100')
    return df, selected


def verify_frozen_200(codes, path=FROZEN_200):
    x = json.loads(Path(path).read_text(encoding='utf-8'))
    older = x['codes']
    if len(older) != 200 or len(set(older)) != 200 or not set(older).issubset(codes):
        raise ValueError('FROZEN_200_NOT_SUBSET_OF_500')
    return older


def validate_receipt(receipt, artifacts_dir):
    p = Path(artifacts_dir)
    idx = receipt['shard_index']; n = receipt['shard_count']
    if n != 5 or not 0 <= idx < 5:
        raise ValueError('BAD_SHARD_RECEIPT_ID')
    if receipt['selected_count'] != 100 or len(set(receipt['selected_codes'])) != 100:
        raise ValueError('BAD_SHARD_RECEIPT_STOCK_COUNT')
    if set(receipt['price_sha256']) != set(receipt['selected_codes']):
        raise ValueError('PRICE_CODE_MISMATCH')
    for code, sha in receipt['price_sha256'].items():
        f = p / 'ohlcv' / (code + '.csv')
        if not f.is_file() or digest(f) != sha:
            raise ValueError('PRICE_SHA256_MISMATCH ' + code)
    for name in FILE_NAMES:
        f = p / name
        if not f.is_file() or digest(f) != receipt['materialized_sha256'][name]:
            raise ValueError('SHARD_MATERIALIZATION_SHA256_MISMATCH ' + name)
    if receipt.get('truebreak_sha256'):
        f=p / 'R118_TRUEBREAK_3CANDLE_SHARD.csv'
        if not f.is_file() or digest(f)!=receipt['truebreak_sha256']:
            raise ValueError('TRUEBREAK_SHARD_MATERIALIZATION_SHA256_MISMATCH')
    return True


def run(index, *, out_root=None, cache=None):
    if not 0 <= index < 5: raise ValueError('shard 0..4 required')
    work = HERE / '.auto_parallel500_work'
    inputs_dir = work / 'inputs'
    lines_dir = work / f'line_shard_{index}'
    results_dir = work / f'results_shard_{index}'
    root = Path(out_root) if out_root else HERE / '.auto_parallel500_artifacts'
    artifact = root / f'shard_{index}'
    artifact.mkdir(parents=True, exist_ok=True)
    status_file = artifact / 'R118_SHARD_STAGE.json'
    status = {'shard_index': index, 'shard_count': 5, 'success': False,
              'stage': 'PREPARING_SOURCES'}

    def save():
        status_file.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding='utf-8')

    save()
    years, indices, pinned = load_pinned_sources(cache or HERE / '.local_source_cache')
    inp = build_inputs(years, indices, inputs_dir, count=500, sha_sources=pinned)
    del years, indices
    manifest, selected = partition_manifest(inp['manifest'], index)
    codes = manifest.code.tolist()
    verify_frozen_200(codes)
    subset_manifest = inputs_dir / f'manifest_shard_{index}.csv'
    selected.to_csv(subset_manifest, index=False)
    status.update(stage='R111_R117_CANONICAL_REPLAY', selected_count=len(selected),
                  full_manifest_sha256=digest(inp['manifest']))
    save()
    from auto_trendlines import generate_all  # original R111-R117 imports only during real execution
    line_states, roles, audit = generate_all(subset_manifest, lines_dir)
    if audit['stock_count'] != 100:
        raise AssertionError('R118_SHARD_LINE_COVERAGE_NOT_100')
    # RESEARCH_ONLY: consume the original full R117 daily states WHILE they
    # still exist. The previous design discarded them after the band study.
    # No R111/R112/R116/R117 algorithm or production behavior is modified.
    from r118_truebreak500 import run_shard as truebreak_run
    truebreak_path = artifact / 'R118_TRUEBREAK_3CANDLE_SHARD.csv'
    status['stage'] = 'R117_TRUEBREAK_3CANDLE_MA_ICHI_RESEARCH'
    save()
    truebreak_run(subset_manifest, line_states, inp['sector'], inp['market'], truebreak_path)
    status['stage'] = 'R118_SHARD_RESEARCH'
    save()
    run_research([
        '--manifest', str(inp['manifest']), '--out', str(results_dir),
        '--line-states', str(line_states), '--line-mode', 'R117',
        '--role-events', str(roles), '--market-daily', str(inp['market']),
        '--sector-pit', str(inp['sector']), '--study-start', '2023-01-01',
        '--target-stocks', '500', '--shard-index', str(index), '--shard-count', '5'
    ])
    # A single job is a 100-stock atomic unit. Only completed shards get a
    # receipt; all data are uploaded before the job can count as SUCCESS.
    status['stage'] = 'MATERIALIZING_ATOMIC_SHARD'
    save()
    readiness = json.loads((results_dir / 'R118_READINESS_AUDIT.json').read_text('utf-8'))
    expected = set(selected.code)
    if (readiness['shard_index'] != index or readiness['shard_count'] != 5
            or readiness['total_input_stocks'] != 500
            or set(readiness['raw_bars_by_code']) != expected
            or set(readiness['upstream_source']) != set(codes)):
        raise AssertionError('SHARD_READINESS_IDENTITY_MISMATCH')
    for name in FILE_NAMES:
        shutil.copy2(results_dir / name, artifact / name)
    ohlcv = artifact / 'ohlcv'; ohlcv.mkdir(exist_ok=True)
    price_hashes = {}
    for row in selected.itertuples(index=False):
        src = inputs_dir / row.path
        dst = ohlcv / (row.code + '.csv')
        if digest(src).lower() != row.sha256.lower():
            raise ValueError('PRICE_SOURCE_SHA_MISMATCH ' + row.code)
        shutil.copy2(src, dst)
        price_hashes[row.code] = digest(dst)
    if index == 0:
        shutil.copy2(inp['market'], artifact / 'market_daily.csv')
        shutil.copy2(inputs_dir / 'R118_AUTO_SOURCE_AUDIT.json', artifact / 'R118_AUTO_SOURCE_AUDIT.json')
    receipt = {
        'version': 'R118_PARALLEL500_SHARD_V1', 'shard_index': index,
        'shard_count': 5, 'cohort_selection_asof': inp['audit']['selection_asof'],
        'full_manifest_sha256': digest(inp['manifest']),
        'selected_count': 100, 'selected_codes': sorted(expected),
        'price_sha256': price_hashes,
        'materialized_sha256': {name:digest(artifact/name) for name in FILE_NAMES},
        'truebreak_sha256': digest(truebreak_path),
        'original_r111_r117_unchanged': True,
        'research_only': True, 'success': True,
    }
    validate_receipt(receipt, artifact)
    (artifact / 'R118_SHARD_RECEIPT.json').write_text(
        json.dumps(receipt, ensure_ascii=False, indent=2), encoding='utf-8')
    status.update(stage='COMPLETED_100_STOCK_SHARD', success=True)
    save()
    print('R118_500_ATOMIC_SHARD_COMPLETE', index, '100 stocks', flush=True)
    return receipt


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--shard-index', type=int, required=True)
    p.add_argument('--out-root', default=None)
    p.add_argument('--cache', default=None)
    args = p.parse_args()
    try:
        run(args.shard_index, out_root=args.out_root, cache=args.cache)
    except Exception as e:
        print('::error::R118_PARALLEL500_SHARD_FAIL_CLOSED', type(e).__name__, str(e),
              file=sys.stderr, flush=True)
        raise


if __name__ == '__main__':main()
