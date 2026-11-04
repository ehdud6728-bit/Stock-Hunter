import hashlib
from pathlib import Path
import sys
import tempfile
import unittest

import numpy as np
import pandas as pd

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from research_r1d118 import (features,trade,upper_events,lower_events,dedup_events,
                             load_data,verified_files,_line_source,join_line_context,run,
                             join_exact_retest_event)
from merge_r118 import merge


class R118ResearchOnlyTests(unittest.TestCase):
    def data(self,n=125):
        dates=pd.bdate_range('2023-01-02',periods=n).strftime('%Y-%m-%d')
        # deterministic nonlinear prices, no intraday invalid OHLC
        prices=np.array([100+0.12*i+9*np.sin(i/7)+2*np.cos(i/3) for i in range(n)])
        return pd.DataFrame({'code':['123456']*n,'date':dates,'open':prices-1,
         'high':prices+2,'low':prices-3,'close':prices,'amount':1e10*(1+np.arange(n)%5)})

    def test_close_asof_prefix_invariance(self):
        s=self.data(125)
        full=features(s)
        for t in [41,50,75,95,104]:
            cut=features(s.iloc[:t+1])
            for col in ['MA7','MA15','MA20','BB20_UPPER','BB20_LOWER','BB30_UPPER','ENV20_10_UPPER','ENV40_20_LOWER','AMOUNT_RATIO20']:
                self.assertAlmostEqual(float(full.at[t,col]),float(cut.at[t,col]),places=8)

    def test_no_same_close_entry(self):
        s=self.data(85)
        pnl=trade(s,50)
        self.assertEqual(pnl['entry_date'],s.at[51,'date'])
        self.assertEqual(pnl['exit_date'],s.at[55,'date'])

    def test_dedup_fixed_order_and_cooldown(self):
        f=lambda b,i:{'code':'123456','family':'UPPER_PULLBACK','band':b,
                     'signal_date':str(i),'signal_idx':i,'anchor_idx':1}
        x=dedup_events([f('ENV20_10',54),f('BB20',54),f('BB20',60),f('BB30',67)])
        self.assertEqual(list(x.band),['BB20','BB30'])
        self.assertEqual(list(x.signal_idx),[54,67])

    def test_no_future_line_known_date(self):
        with tempfile.TemporaryDirectory() as folder:
            p=Path(folder)/'line.csv'
            pd.DataFrame([{'code':'123456','date':'2023-01-12','idx':8,
              'algorithm':'TS_BODY_WICK_PROXY','scale':'SHORT','kind':'LOWER',
              'active_line_id':'FAKE1','active_level':100,'active_known_date':'2023-01-13'}]).to_csv(p,index=False)
            with self.assertRaisesRegex(ValueError,'PIT_FUTURE_LINE_LEAKAGE'):
                _line_source(p,'R112')

    def test_join_original_line_id_only_when_idx_matches(self):
        s=self.data(90)
        k=50
        event=pd.DataFrame([{'code':'123456','signal_date':s.at[k,'date'],'signal_idx':k}])
        with tempfile.TemporaryDirectory() as folder:
            p=Path(folder)/'line.csv'
            line=pd.DataFrame([{'code':'123456','date':s.at[k,'date'],'idx':k,
              'algorithm':'TS_BODY_WICK_PROXY','scale':'SHORT','kind':'LOWER',
              'active_line_id':'S-EXACT','active_level':float(s.at[k,'close']),
              'active_known_date':s.at[k-3,'date']}])
            line.to_csv(p,index=False)
            joined=join_line_context(event,{'123456':s},_line_source(p,'R112'))
            self.assertEqual(joined.iloc[0]['SHORT_LOWER_id'],'S-EXACT')
            self.assertEqual(joined.iloc[0]['SHORT_LOWER_coverage'],'VALID_SOURCE_DATE')
            self.assertEqual(joined.iloc[0]['MEDIUM_UPPER_coverage'],'MISSING_SOURCE_DATE')
            line.at[0,'idx']=k+4;line.to_csv(p,index=False)
            bad=join_line_context(event,{'123456':s},_line_source(p,'R112'))
            self.assertEqual(bad.iloc[0]['SHORT_LOWER_coverage'],'MISSING_SOURCE_DATE')

    def test_sha_manifest_refuses_unpinned(self):
        with tempfile.TemporaryDirectory() as folder:
            d=Path(folder); file=d/'stock.csv'
            self.data().to_csv(file,index=False)
            manifest=d/'manifest.csv'; pd.DataFrame([{'code':'123456','path':file.name,'sha256':'f'*64}]).to_csv(manifest,index=False)
            with self.assertRaisesRegex(ValueError,'PINNED_SHA_MISMATCH'):
                verified_files(manifest)
            pd.DataFrame([{'code':'123456','path':file.name,'sha256':hashlib.sha256(file.read_bytes()).hexdigest()}]).to_csv(manifest,index=False)
            self.assertEqual(len(verified_files(manifest)),1)

    def test_research_not_ready_without_trendlines(self):
        with tempfile.TemporaryDirectory() as folder:
            s=self.data(160)
            summary=run({'123456':s},folder,sample=True,required_min_stocks=200)
            self.assertEqual(summary['decision'],'RESEARCH_ONLY_NOT_PRODUCTION_READY')
            self.assertIn('TRENDLINE_PIT_COVERAGE_MISSING',summary['blocked_reasons'])
            self.assertFalse(summary['large_universe_met'])
            self.assertTrue((Path(folder)/'R118_THRESHOLD_MATRIX.csv').exists())

    def test_exact_retest_does_not_join_future_or_wrong_index(self):
        s=self.data(120);idx=57
        with tempfile.TemporaryDirectory() as folder:
            p=Path(folder)/'roles.csv'
            pd.DataFrame([{'code':'123456','algorithm':'TS_BODY_WICK_PROXY',
                'scale':'SHORT','broken_line_id':'R112-EXACT-OLD-UPPER',
                'old_kind':'UPPER','new_role_candidate':'SUPPORT',
                'break_idx':42,'break_date':s.at[42,'date'],
                'retest_idx':idx,'retest_date':s.at[idx,'date'],
                'status':'ROLE_RETEST_FIRST_RESPECTED'}]).to_csv(p,index=False)
            ev=pd.DataFrame([{'code':'123456','signal_idx':idx-1,'signal_date':s.at[idx-1,'date']},
                             {'code':'123456','signal_idx':idx,'signal_date':s.at[idx,'date']}])
            got=join_exact_retest_event(ev,{'123456':s},p)
            self.assertEqual(got.iloc[0]['SHORT_retired_upper_support_retests'],0)
            self.assertEqual(got.iloc[1]['SHORT_retired_upper_support_retests'],1)
            self.assertEqual(got.iloc[1]['SHORT_retired_upper_line_ids'],'R112-EXACT-OLD-UPPER')

    def test_two_shards_merge_only_and_fail_on_missing_shard(self):
        with tempfile.TemporaryDirectory() as folder:
            f=Path(folder);a=self.data(160);b=self.data(160);b['code']='234567'
            stock={'123456':a,'234567':b}
            source={'123456':{'sha256':'a'*64},'234567':{'sha256':'b'*64}}
            for i in range(2):
                run(stock.copy(),f/f'shard_{i}',sample=False,required_min_stocks=200,
                    shard_index=i,shard_count=2,input_provenance=source)
            audit=merge(f,2,f/'merged')
            self.assertEqual(audit['total_stocks'],2)
            self.assertTrue(audit['merge_only_no_signal_recomputation'])
            self.assertTrue((f/'merged'/'R118_MERGED_THRESHOLD_MATRIX.csv').is_file())
            with self.assertRaisesRegex(ValueError,'shard_count'):merge(f,1,f/'error')
            (f/'shard_1'/'R118_EVENT_LEDGER_PIT.csv').unlink()
            with self.assertRaisesRegex(FileNotFoundError,'SHARD_INCOMPLETE'):merge(f,2,f/'error')

    def test_not_forced_ma224(self):
        s=features(self.data())
        self.assertFalse(any(x.startswith('MA224') for x in s.columns))
        events,_=upper_events('123456',s)
        self.assertTrue(any(x['family']=='UPPER_TOUCH' for x in events))
        lower_events('123456',s)  # optional no events is fine


if __name__=='__main__':unittest.main()
