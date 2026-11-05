#!/usr/bin/env python3
"""Merge 5 immutable R118 100-stock shard artifacts; zero signal/line recompute.

The merge checks cohort membership and file hashes and calls the ORIGINAL
R118 merge. Charts independently replay 10 events against SHA-pinned OHLCV.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path

import pandas as pd

from merge_r118 import merge as canonical_merge
from parallel500_shard import FILE_NAMES, digest, validate_receipt, verify_frozen_200

HERE = Path(__file__).resolve().parent


def validate_five_artifacts(download_dir, destination, *, frozen200=None):
    root = Path(download_dir)
    out = Path(destination)
    if not root.is_dir():raise FileNotFoundError(root)
    roots=[]; all_codes=set(); receipts=[]; global_digest=None; selection_date=None
    for idx in range(5):
        shard = root / f'R118500_SHARD_{idx}'
        receipt_path = shard / 'R118_SHARD_RECEIPT.json'
        if not receipt_path.is_file():
            raise FileNotFoundError(f'SHARD_INCOMPLETE_{idx}_NO_RECEIPT')
        r = json.loads(receipt_path.read_text(encoding='utf-8'))
        if r.get('success') is not True or r['shard_index'] != idx or r['shard_count'] != 5:
            raise ValueError(f'SHARD_RECEIPT_ID_MISMATCH_{idx}')
        validate_receipt(r, shard)
        a = json.loads((shard/'R118_READINESS_AUDIT.json').read_text(encoding='utf-8'))
        if (a['shard_index'] != idx or a['shard_count'] != 5 or
                a['study_stocks'] != 100 or a['total_input_stocks'] != 500 or
                set(a['raw_bars_by_code']) != set(r['selected_codes'])):
            raise ValueError(f'SHARD_R118_AUDIT_MISMATCH_{idx}')
        if global_digest is None:
            global_digest = r['full_manifest_sha256']
            selection_date = r['cohort_selection_asof']
        if r['full_manifest_sha256'] != global_digest or r['cohort_selection_asof'] != selection_date:
            raise ValueError('COHORT_SELECTION_OR_MANIFEST_DRIFT')
        if all_codes.intersection(r['selected_codes']):
            raise ValueError('DUPLICATE_STOCK_IN_SHARDS')
        all_codes.update(r['selected_codes']); receipts.append(r); roots.append(shard)
    if len(all_codes) != 500: raise ValueError('MISSING_GLOBAL_500_STOCKS')
    verify_frozen_200(all_codes, frozen200 or HERE/'FROZEN_2022_200_COHORT_20261104.json')
    # The shard indices MUST follow canonical global lexicographic ordering;
    # this proves no silently dropped, swapped, or duplicated names.
    sorted_codes=sorted(all_codes)
    for idx,r in enumerate(receipts):
        if r['selected_codes'] != sorted(sorted_codes[idx::5]):
            raise ValueError('GLOBAL_COHORT_SHARD_IDENTITY_MISMATCH')
    market = roots[0]/'market_daily.csv'
    audit = roots[0]/'R118_AUTO_SOURCE_AUDIT.json'
    if not (market.is_file() and audit.is_file()):
        raise FileNotFoundError('MISSING_MARKET_OR_SOURCE_AUDIT_FROM_SHARD_0')
    source = json.loads(audit.read_text(encoding='utf-8'))
    if (source['selected_count'] != 500 or
        set(source['selected_codes']) != all_codes or
        source['selection_asof'] != selection_date):
        raise ValueError('SHARD_SOURCE_COHORT_IDENTITY_MISMATCH')
    out.mkdir(parents=True, exist_ok=True)
    manifest_rows=[]
    for idx,r in enumerate(receipts):
        for code in r['selected_codes']:
            price=roots[idx]/'ohlcv'/(code+'.csv')
            manifest_rows.append({'code':code,'path':os.path.relpath(price,out),
                                  'sha256':r['price_sha256'][code]})
    manifest=out/'R118_500_VERIFIED_INPUT_MANIFEST.csv'
    pd.DataFrame(sorted(manifest_rows,key=lambda q:q['code'])).to_csv(manifest,index=False)
    return roots, market, audit, manifest, receipts


def merge_and_chart(download_dir, output, *, frozen200=None, charts=True):
    out=Path(output)
    roots,market,audit,manifest,receipts=validate_five_artifacts(
        download_dir,out,frozen200=frozen200)
    # Existing frozen R118 merge expects shard_0..shard_4 subdirectories.
    # Symlinks only: no modification of materialized source artifacts.
    staging=out/'_shard_aliases'
    staging.mkdir(exist_ok=True)
    for idx,original in enumerate(roots):
        alias=staging/f'shard_{idx}'
        if alias.exists() or alias.is_symlink():
            if not alias.is_symlink() or alias.resolve()!=original.resolve():
                raise ValueError(f'SHARD_ALIAS_COLLISION_{idx}')
        else:
            alias.symlink_to(original.resolve(),target_is_directory=True)
    summary=canonical_merge(staging,5,out)
    if summary['total_stocks']!=500 or not summary['all_shards_complete']:
        raise AssertionError('R118_CANONICAL_MERGE_INCOMPLETE')
    (out/'R118_PARALLEL500_COMPLETE.json').write_text(json.dumps({
        'status':'MERGE_ONLY_COMPLETE','stocks':500,'shards':5,
        'global_cohort_manifest_sha256':receipts[0]['full_manifest_sha256'],
        'original_200_subset_verified':True,
        'merged_ledger_sha256':digest(out/'R118_MERGED_EVENT_LEDGER_PIT.csv'),
        'no_signal_or_trendline_recomputation':True,'research_only':True
    },ensure_ascii=False,indent=2),encoding='utf-8')
    if charts:
        from visual_audit_500 import main as visual_audit_main
        visual_audit_main(['--ledger',str(out/'R118_MERGED_EVENT_LEDGER_PIT.csv'),
            '--manifest',str(manifest),'--market',str(market),
            '--source-audit',str(audit), '--baseline-200',str(frozen200 or HERE/'FROZEN_2022_200_COHORT_20261104.json'),
            '--out',str(out/'visual_cases_500'),'--per-outcome','5'])
    return summary


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--download-dir',required=True)
    p.add_argument('--out',required=True)
    p.add_argument('--baseline-200',default=None)
    p.add_argument('--skip-charts',action='store_true',help='For offline regression only')
    a=p.parse_args()
    result=merge_and_chart(a.download_dir,a.out,frozen200=a.baseline_200,
                           charts=not a.skip_charts)
    print(json.dumps(result,ensure_ascii=False,indent=2))


if __name__=='__main__':main()
