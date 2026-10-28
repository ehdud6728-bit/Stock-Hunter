"""Offline, no KRX/network. For local package run: python -m unittest discover tests."""
from __future__ import annotations
import hashlib,io,json,pathlib,sys,tempfile,types,unittest,zipfile,pandas as pd
ROOT=pathlib.Path(__file__).resolve().parents[1]
# In a regular repository these are naturally present on PYTHONPATH.
if str(ROOT) not in sys.path:sys.path.insert(0,str(ROOT))
from research_formulas import stockhunter_r1d27d_fullmarket_freeze as freeze

class FrozenSelectionTest(unittest.TestCase):
    def setUp(self):self.dir=ROOT/'research_inputs'
    def test_seal_and_balance(self):
        path=self.dir/'R1D27D_FROZEN_1500_TICKER_DISJOINT_OUTCOME_BLIND.csv'
        m=json.loads((self.dir/'R1D27D_FROZEN_MANIFEST.json').read_text())
        self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(),freeze.sha(path.read_bytes()))
        self.assertEqual(m['selected_rows_sha256'],freeze.sha(path.read_bytes()))
        d=pd.read_csv(path,dtype={'code':str,'date':str,'bb_period':str})
        self.assertEqual(len(d),1500);self.assertEqual(d.code.nunique(),1500)
        self.assertEqual(len(d.groupby(freeze.STRATA)),30)
        self.assertTrue(d.groupby(freeze.STRATA).size().eq(50).all())
        self.assertTrue((d.pre60_to<d.date).all())
    def test_no_outcome_columns(self):
        d=pd.read_csv(self.dir/'R1D27D_FROZEN_1500_TICKER_DISJOINT_OUTCOME_BLIND.csv')
        self.assertFalse(any(x in d.columns for x in ['net_20bps_pct','exit_close','MFE_intraday_pct','hit15','MAE_intraday_pct']))
    def test_price_census_manifest(self):
        p=ROOT/'reports/R1D27D_FULLMARKET_PRICE_ONLY_133180_STRATA_HORIZON.csv'
        d=pd.read_csv(p);m=json.loads((ROOT/'reports/R1D27D_PRICE_ONLY_MANIFEST.json').read_text())
        self.assertEqual(len(d),150);self.assertEqual(m['observations'],665900)
        self.assertEqual(m['completed_price_paths']+m['censored'],m['observations'])
        self.assertEqual(freeze.sha(p.read_bytes()),m['files_sha256'][p.name])
    def test_offline_two_batches_preserve_source(self):
        from research_formulas import stockhunter_r1d27d_flow60 as collect
        from research_formulas import stockhunter_r1d27c_flow60 as c27
        from unittest.mock import patch
        import types,sys
        calendar=pd.read_csv(self.dir/'R1D27D_ORIGINAL_KRX_INDEX_1907_DAYS.csv',dtype=str).date.tolist()
        d6=types.ModuleType('research_formulas.formula66_r1d6_oos_holdout')
        d6.index_asof=lambda _root:(calendar,None)
        def artificial_data(start,end,code,fetch=None):
            dates=[d for d in calendar if start<=d.replace('-','')<=end]
            self.assertEqual(len(dates),60)
            ix=pd.to_datetime(dates);frames={}
            for kind in ('VALUE','VOLUME'):
                for side in ('매수','매도','순매수'):
                    v=0 if side=='순매수' else 100
                    tab=pd.DataFrame({actor:[v]*60 for actor in c27.ACTORS},index=ix)
                    tab['전체']=4*v;frames[(kind,side)]=tab
            return frames
        with tempfile.TemporaryDirectory() as t,patch.dict(sys.modules,{'research_formulas.formula66_r1d6_oos_holdout':d6}),patch.object(c27,'get_flow',side_effect=artificial_data):
            folder=pathlib.Path(t)
            m1=collect.collect_batch(self.dir,'OFFLINE',folder/'part1',batch_size=2,cooldown=0)
            m2=collect.collect_batch(self.dir,'OFFLINE',folder/'part2',batch_size=2,seed_root=folder/'part1',cooldown=0)
            self.assertEqual((m1['n_complete'],m2['n_complete']),(2,4))
            old=pd.read_csv(folder/'part1/reports/r1d27d/result/flow/R1D27D_FLOW60_DAILY.csv.gz',dtype={'code':str})
            new=pd.read_csv(folder/'part2/reports/r1d27d/result/flow/R1D27D_FLOW60_DAILY.csv.gz',dtype={'code':str})
            self.assertEqual((len(old),len(new)),(480,960))
            oldcodes=set(old.code)
            pd.testing.assert_frame_equal(old.sort_values(list(old.columns)).reset_index(drop=True),
                new[new.code.isin(oldcodes)].sort_values(list(old.columns)).reset_index(drop=True))
    def test_sha_tamper_fail_closed(self):
        from research_formulas import stockhunter_r1d27d_flow60 as collect
        with tempfile.TemporaryDirectory() as t:
            q=pathlib.Path(t)
            for name in ['R1D27D_FROZEN_1500_TICKER_DISJOINT_OUTCOME_BLIND.csv','R1D27D_FROZEN_MANIFEST.json']:
                (q/name).write_bytes((self.dir/name).read_bytes())
            with (q/'R1D27D_FROZEN_1500_TICKER_DISJOINT_OUTCOME_BLIND.csv').open('ab') as fh:fh.write(b'\nTAMPER')
            with self.assertRaises(ValueError):collect.read_event_index(q)

if __name__=='__main__':unittest.main()
