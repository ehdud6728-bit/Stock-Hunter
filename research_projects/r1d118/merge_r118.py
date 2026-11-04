#!/usr/bin/env python3
"""Fail-closed R118 shard merge: zero signal recomputation, only existing event materialization."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import pandas as pd
from research_r1d118 import compare_rules, sha256


def merge(shards_dir, shard_count, output):
    root=Path(shards_dir); dest=Path(output)
    if shard_count<2:raise ValueError('shard_count >= 2 required for merge')
    all_codes=set(); ledgers=[]; anchors=[]; receipts=[]; original_prov=None
    for idx in range(shard_count):
        sub=root/f'shard_{idx}'
        receipt=sub/'R118_READINESS_AUDIT.json'
        ledger=sub/'R118_EVENT_LEDGER_PIT.csv'
        denominator=sub/'R118_ALL_ANCHOR_DENOMINATORS.csv'
        if not (receipt.is_file() and ledger.is_file() and denominator.is_file()):
            raise FileNotFoundError(f'SHARD_INCOMPLETE {idx}: missing receipt / event ledger / denominator')
        audit=json.loads(receipt.read_text(encoding='utf-8'))
        if audit['shard_index']!=idx or audit['shard_count']!=shard_count:raise ValueError(f'SHARD_ID_MISMATCH {idx}')
        if audit.get('sample'):raise ValueError(f'SMOKE_SHARD_NOT_MERGEABLE {idx}')
        if audit['decision']!='RESEARCH_ONLY_NOT_PRODUCTION_READY':raise ValueError('unknown research lane decision')
        if original_prov is None:original_prov=audit['upstream_source']
        if audit['upstream_source']!=original_prov:raise ValueError(f'SOURCE_MANIFEST_DRIFT shard {idx}')
        codes=set(audit['raw_bars_by_code'])
        if codes & all_codes:raise ValueError(f'DUPLICATE_SHARD_STOCK {idx}')
        all_codes|=codes
        d=pd.read_csv(ledger,dtype={'code':str},encoding='utf-8-sig',low_memory=False)
        a=pd.read_csv(denominator,dtype={'code':str},encoding='utf-8-sig')
        if not set(d.code).issubset(codes) or not set(a.code).issubset(codes):
            raise ValueError(f'INVALID_SHARD_CODE {idx}')
        if d.duplicated(['family','code','signal_date']).any():raise ValueError(f'SHARD_DUPLICATE_EVENT {idx}')
        ledgers.append(d);anchors.append(a)
        receipts.append({'shard_index':idx,'codes':sorted(codes),
          'event_ledger_sha256':sha256(ledger),'anchor_ledger_sha256':sha256(denominator),
          'readiness_sha256':sha256(receipt),'row_count':len(d)})
    n=len(original_prov)
    if len(all_codes)!=n:raise ValueError(f'PARTIAL_SHARD_SET got {len(all_codes)} codes expected {n}')
    if set(original_prov)!=all_codes:raise ValueError('SOURCE_CODE_IDENTITY_MISMATCH')
    d=pd.concat(ledgers,ignore_index=True)
    if d.duplicated(['family','code','signal_date']).any():raise ValueError('CROSS_SHARD_DUPLICATES')
    den=pd.concat(anchors,ignore_index=True)
    dest.mkdir(parents=True,exist_ok=True)
    d.to_csv(dest/'R118_MERGED_EVENT_LEDGER_PIT.csv',index=False,encoding='utf-8-sig')
    den.to_csv(dest/'R118_MERGED_ALL_ANCHORS.csv',index=False,encoding='utf-8-sig')
    compare_rules(d).to_csv(dest/'R118_MERGED_THRESHOLD_MATRIX.csv',index=False,encoding='utf-8-sig')
    report={'research_only':True,'merge_only_no_signal_recomputation':True,
       'total_stocks':len(all_codes),'shard_count':shard_count,'merged_event_rows':len(d),
       'all_shards_complete':True,'shards':receipts,'decision':'RESEARCH_ONLY_NOT_PRODUCTION_READY'}
    (dest/'R118_MERGE_AUDIT.json').write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding='utf-8')
    return report


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--shards-dir',required=True);p.add_argument('--shards',type=int,required=True)
    p.add_argument('--out',required=True)
    x=p.parse_args();print(json.dumps(merge(x.shards_dir,x.shards,x.out),indent=2,ensure_ascii=False))

if __name__=='__main__':main()
