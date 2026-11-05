"""No-network tests for deterministic 500-cohort partition, immutable shard receipts and merge."""
import json
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

HERE=Path(__file__).resolve().parents[1]
if str(HERE) not in sys.path:sys.path.insert(0,str(HERE))

from parallel500_shard import partition_manifest,digest,validate_receipt,FILE_NAMES
from parallel500_merge import validate_five_artifacts,merge_and_chart


class Parallel500Tests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        root=Path(self.tmp.name)
        self.download=root/'downloads';self.download.mkdir()
        self.out=root/'results';self.out.mkdir()
        self.codes=[f'{i:06d}' for i in range(1,501)]
        self.baseline=root/'frozen_200.json'
        self.baseline.write_text(json.dumps({'codes':self.codes[:200],'selection_asof':'2022-12-29'}))
        self.manifest=root/'manifest.csv'
        pd.DataFrame([{'code':c,'path':'ohlcv/'+c+'.csv','sha256':'x'*64} for c in self.codes]).to_csv(self.manifest,index=False)
        self._make_artifacts()

    def tearDown(self):self.tmp.cleanup()

    def _make_artifacts(self):
        global_provenance={c:{'file':'same_runner_workspace/ohlcv/'+c+'.csv','sha256':'placeholder'} for c in self.codes}
        for idx in range(5):
            shard=self.download/f'R118500_SHARD_{idx}'
            shard.mkdir();(shard/'ohlcv').mkdir()
            codes=self.codes[idx::5]
            hashes={}
            for code in codes:
                f=shard/'ohlcv'/(code+'.csv')
                f.write_text('code,date,open,high,low,close,amount\n'+code+',2023-04-01,10,11,9,10,100\n')
                hashes[code]=digest(f)
            # One synthetic event per stock; merge is intentionally NOT a signal recomputation.
            events=pd.DataFrame([{
                'code':c,'family':'LOWER_RECLAIM','band':'BB20','signal_date':'2023-04-01',
                'amount_eok':120,'amount_ratio20':1.3,'d5_net_pct':1.0,
                'tp3_high_reached':0} for c in codes])
            events.to_csv(shard/'R118_EVENT_LEDGER_PIT.csv',index=False)
            pd.DataFrame([{'code':c,'anchor_date':'2023-03-31'} for c in codes]).to_csv(
                shard/'R118_ALL_ANCHOR_DENOMINATORS.csv',index=False)
            (shard/'R118_READINESS_AUDIT.json').write_text(json.dumps({
                'shard_index':idx,'shard_count':5,'study_stocks':100,
                'total_input_stocks':500,'sample':False,
                'decision':'RESEARCH_ONLY_NOT_PRODUCTION_READY',
                'raw_bars_by_code':{c:200 for c in codes},
                'upstream_source':global_provenance
            }))
            receipt={'success':True,'shard_index':idx,'shard_count':5,
                'full_manifest_sha256':'a'*64,'cohort_selection_asof':'2022-12-29',
                'selected_count':100,'selected_codes':codes,'price_sha256':hashes,
                'materialized_sha256':{x:digest(shard/x) for x in FILE_NAMES}}
            (shard/'R118_SHARD_RECEIPT.json').write_text(json.dumps(receipt))
        (self.download/'R118500_SHARD_0'/'market_daily.csv').write_text('market,date,close,amount\n')
        (self.download/'R118500_SHARD_0'/'R118_AUTO_SOURCE_AUDIT.json').write_text(json.dumps({
            'selected_count':500,'selected_codes':self.codes,'selection_asof':'2022-12-29'}))

    def test_canonical_five_way_partition(self):
        selected=[]
        for idx in range(5):
            whole,part=partition_manifest(self.manifest,idx)
            self.assertEqual(500,len(whole));self.assertEqual(100,len(part))
            self.assertEqual(self.codes[idx::5],part.code.tolist())
            selected.extend(part.code.tolist())
        self.assertEqual(sorted(self.codes),sorted(selected))

    def test_reject_invalid_shard_count(self):
        with self.assertRaisesRegex(ValueError,'EXACT_FIVE'):partition_manifest(self.manifest,1,4)

    def test_reject_partial_499_global_universe(self):
        d=pd.read_csv(self.manifest,dtype=str).iloc[:499]
        d.to_csv(self.manifest,index=False)
        with self.assertRaisesRegex(ValueError,'INVALID_GLOBAL_MANIFEST'):partition_manifest(self.manifest,0)

    def test_all_five_recoverable(self):
        roots,market,audit,manifest,receipts=validate_five_artifacts(
            self.download,self.out,frozen200=self.baseline)
        self.assertEqual(5,len(roots))
        self.assertEqual(500,len(pd.read_csv(manifest,dtype=str)))
        self.assertEqual(5,len(receipts))
        self.assertTrue(market.exists())

    def test_partial_shards_must_not_merge(self):
        p=self.download/'R118500_SHARD_4'/'R118_SHARD_RECEIPT.json'
        data=p.read_bytes();p.unlink()
        try:
            with self.assertRaisesRegex(FileNotFoundError,'SHARD_INCOMPLETE'):
                validate_five_artifacts(self.download,self.out,frozen200=self.baseline)
        finally:p.write_bytes(data)

    def test_detect_price_tamper(self):
        p=self.download/'R118500_SHARD_2'/'ohlcv'/(self.codes[2]+'.csv')
        data=p.read_bytes();p.write_bytes(data+b'CORRUPTED')
        try:
            with self.assertRaisesRegex(ValueError,'PRICE_SHA256_MISMATCH'):
                validate_five_artifacts(self.download,self.out,frozen200=self.baseline)
        finally:p.write_bytes(data)

    def test_detect_missing_stock_from_receipt(self):
        p=self.download/'R118500_SHARD_2'/'R118_SHARD_RECEIPT.json'
        data=p.read_text();r=json.loads(data);r['selected_codes'].pop();p.write_text(json.dumps(r))
        try:
            with self.assertRaisesRegex(ValueError,'BAD_SHARD_RECEIPT_STOCK_COUNT'):
                validate_five_artifacts(self.download,self.out,frozen200=self.baseline)
        finally:p.write_text(data)

    def test_detect_global_manifest_drift(self):
        p=self.download/'R118500_SHARD_3'/'R118_SHARD_RECEIPT.json'
        data=p.read_text();r=json.loads(data);r['full_manifest_sha256']='b'*64;p.write_text(json.dumps(r))
        try:
            with self.assertRaisesRegex(ValueError,'COHORT_SELECTION_OR_MANIFEST_DRIFT'):
                validate_five_artifacts(self.download,self.out,frozen200=self.baseline)
        finally:p.write_text(data)

    def test_merge_all_500_without_recalculating_signals(self):
        result=merge_and_chart(self.download,self.out,frozen200=self.baseline,charts=False)
        self.assertEqual(500,result['total_stocks'])
        self.assertEqual(500,result['merged_event_rows'])
        ledger=pd.read_csv(self.out/'R118_MERGED_EVENT_LEDGER_PIT.csv',dtype={'code':str})
        self.assertEqual(500,ledger.code.nunique())
        self.assertEqual(500,len(ledger))
        self.assertTrue((self.out/'R118_MERGE_AUDIT.json').exists())


if __name__=='__main__':unittest.main()
