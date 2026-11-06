#!/usr/bin/env python3
"""R118 pinned historical 500-stock dataset -> independent BLUE20 RSI delay study.

Research-only: never imports search/live/watch, never changes R118 outputs.
Published outputs are exploratory until the historical event baseline matches.
"""
from __future__ import annotations
import argparse
import csv
import gzip
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import traceback

import pandas as pd

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
R118_DATA = REPO / 'research_projects' / 'r1d118' / 'auto_data.py'
BASELINE = {'UPPER': {'original': 1992, 'clear': 1187},
            'LOWER': {'original': 2240, 'clear': 1302}}


def sha256(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def load_r118():
    if not R118_DATA.is_file():
        raise FileNotFoundError('Missing existing R118 immutable source module: ' + str(R118_DATA))
    spec = importlib.util.spec_from_file_location('blue20_r118_auto_data', R118_DATA)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def materialize_verified_500(manifest_path, index_csv, outdir):
    """Convert signed per-code R118 inputs into 1 merged CSV.gz & common-day calendar.

    This function does not download or alter R118 sources and is unit-testable.
    """
    manifest_path = Path(manifest_path)
    index_csv = Path(index_csv)
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    m = pd.read_csv(manifest_path, dtype=str).fillna('')
    if not {'code', 'path', 'sha256'}.issubset(m) or len(m) != 500:
        raise ValueError('EXACT_500_MANIFEST_REQUIRED')
    codes = m.code.tolist()
    if sorted(set(codes)) != codes or not all(len(c) == 6 and c.isdigit() for c in codes):
        raise ValueError('MANIFEST_CODE_ORDER_OR_UNIQUENESS_INVALID')
    md = pd.read_csv(index_csv, dtype={'market': str, 'date': str})
    if not {'market', 'date'}.issubset(md):
        raise ValueError('MARKET_DAILY_INDEX_SCHEMA_INVALID')
    if md.duplicated(['market','date']).any():
        raise ValueError('DUPLICATE_MARKET_DAY')
    markets = set(md.market)
    if not {'KOSPI','KOSDAQ'}.issubset(markets):
        raise ValueError('BOTH_MARKET_INDEX_SOURCES_REQUIRED')
    days = (set(md.loc[md.market == 'KOSPI','date']) &
            set(md.loc[md.market == 'KOSDAQ','date']))
    dates = sorted(days)
    if len(dates) < 400:
        raise ValueError('INSUFFICIENT_2022_2024_MARKET_CALENDAR')
    calendar = outdir/'blue20_common_calendar.csv'
    pd.DataFrame({'date': dates}).to_csv(calendar, index=False)
    merged = outdir/'blue20_pinned_500_prices.csv.gz'
    wanted = ['code','date','open','high','low','close']
    hashes, rowcount = {}, 0
    with gzip.open(merged, mode='wt', encoding='utf-8', newline='') as z:
        writer = csv.writer(z)
        writer.writerow(wanted)
        for r in m.itertuples(index=False):
            # Prevent absolute-path and parent traversal in the manifest.
            rel = Path(r.path)
            if rel.is_absolute() or '..' in rel.parts:
                raise ValueError('MANIFEST_PATH_OUTSIDE_INPUT_ROOT ' + r.code)
            source = (manifest_path.parent / rel).resolve()
            if not source.is_file() or not source.is_relative_to(manifest_path.parent.resolve()):
                raise ValueError('MISSING_OR_OUTSIDE_INPUT_SOURCE ' + r.code)
            got = sha256(source)
            if got != r.sha256:
                raise ValueError('PINNED_500_SOURCE_SHA_MISMATCH ' + r.code)
            d = pd.read_csv(source, dtype={'code':str,'date':str})
            if not set(wanted).issubset(d):
                raise ValueError('PRICE_SCHEMA_MISMATCH ' + r.code)
            if not d.code.eq(r.code).all() or d.date.duplicated().any():
                raise ValueError('PRICE_IDENTITY_MISMATCH ' + r.code)
            if not d.date.is_monotonic_increasing:
                raise ValueError('PRICE_DATE_ORDER_MISMATCH ' + r.code)
            for item in d[wanted].itertuples(index=False, name=None):
                writer.writerow(item)
            hashes[r.code] = got
            rowcount += len(d)
    if rowcount < 500 * 120:
        raise ValueError('INSUFFICIENT_500_PRICE_WARMUP')
    return dict(ohlcv=str(merged), calendar=str(calendar),
                data_sha256=sha256(merged), calendar_sha256=sha256(calendar),
                source_manifest_sha256=sha256(manifest_path),
                per_stock_sha256=hashes, rows=rowcount, codes=codes,
                market_calendar_intersection=True)


def run(out, cache, strict=False):
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    status_file = out/'BLUE20_RUN_STAGE.json'
    state = {'research_only':True, 'success':False, 'stage':'INIT',
             'reference_counts':BASELINE}
    def record(stage, **updates):
        state.update(stage=stage,**updates)
        status_file.write_text(json.dumps(state,ensure_ascii=False,indent=2),encoding='utf-8')
    record('LOAD_R118_PINNED_SOURCES')
    try:
        r118 = load_r118()
        years,indices,sources = r118.load_pinned_sources(cache)
        record('R118_ASOF_500_COHORT')
        inputs_dir = HERE/'.auto500_work'/'r118_inputs'
        sourceset = r118.build_inputs(years,indices,inputs_dir,count=500,sha_sources=sources)
        del years,indices
        if sourceset['audit']['selected_count'] != 500:
            raise ValueError('R118_COHORT_SELECTION_NOT_500')
        record('VERIFY_500_INPUT_SHA_AND_COMMON_CALENDAR')
        local = materialize_verified_500(sourceset['manifest'],sourceset['market'],HERE/'.auto500_work'/'merged')
        manifest = dict(local)
        manifest['selection_asof'] = sourceset['audit']['selection_asof']
        manifest['pinned_repos'] = sourceset['audit']['source_git_commits']
        manifest['pinned_source_files'] = sources
        manifest['source_provenance'] = 'R118_PINNED_ARCHIVED_KRX_DERIVED_NOT_OFFICIAL_KRX_DIRECT'
        manifest['baseline_reproduction_required'] = BASELINE
        manifest['strict_count_mode'] = strict
        (out/'BLUE20_AUTO500_SOURCE_PROVENANCE.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')
        record('RUN_RSI_GC_EXACT_DELAY')
        import blue20_rsi_delay
        cli = ['--ohlcv',local['ohlcv'],'--calendar',local['calendar'],
               '--out',str(out),'--start-year','2023','--end-year','2024']
        if strict: cli.append('--strict-legacy-counts')
        blue20_rsi_delay.main(cli)
        audited = json.loads((out/'RSI_DELAY_AUDIT_MANIFEST.json').read_text('utf-8'))
        status = audited['status']
        record('COMPLETED_RESEARCH_ONLY',success=True,
               baseline_status=status,baseline_verified=status.startswith('REFERENCE_MATCHED'),
               oos=False,production_ready=False)
        print('BLUE20_RSI_GC_DELAY_RESEARCH_COMPLETE',status,flush=True)
    except BaseException as e:
        record('FAILED_CLOSED',success=False,error_type=type(e).__name__,error=str(e))
        print('::error::BLUE20_RSI_FAIL_CLOSED '+str(e),file=sys.stderr,flush=True)
        raise


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--out',default=str(HERE/'results_auto500'))
    p.add_argument('--cache',default=str(REPO/'research_projects'/'r1d118'/'.local_source_cache'))
    p.add_argument('--strict-legacy-counts',action='store_true',
                   help='Use only after verifying this cohort is the exact old 500')
    a=p.parse_args()
    run(a.out,a.cache,strict=a.strict_legacy_counts)

if __name__=='__main__': main()
