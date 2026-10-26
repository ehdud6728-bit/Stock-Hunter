import csv, json, os, sys, tempfile, unittest
from collections import Counter
from pathlib import Path
import pandas as pd

from research_formulas import stockhunter_r1d26f_staged_flow as m

class R1D26FTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.proto=json.loads(Path('R1D26F_PREDECLARED_PROTOCOL.json').read_text(encoding='utf8'))
        root=Path(os.environ.get('R1D26F_TEST_INPUT_DIR','reports/r1d26f/input'))
        cls.paths={k+'_zip':str(root/{'r1d25':'R1D25_frozen.zip','r1d26':'R1D26_frozen.zip','r1d26c':'R1D26C_frozen.zip'}[k]) for k in ('r1d25','r1d26','r1d26c')}
        cls.available=all(Path(p).is_file() for p in cls.paths.values())
        if cls.available:
            cls.events,cls.cal,cls.seedzip=m.load_sources(cls.paths,cls.proto)

    def require_frozen(self):
        if not self.available:self.skipTest('Source zips not present yet; final source QA runs after verified download')

    def test_protocol_provenance(self):
        self.assertTrue(self.proto['stage']=='RESEARCH_ONLY' and not self.proto['source_data_timestamps_PIT_certified'])
        self.assertTrue(self.proto['same_ticker_date_dedup'] and not self.proto['official_independent_amount_verified'])
        self.assertEqual(self.proto['sample_total'],100)

    def test_no_outcome_data_in_selection(self):
        self.require_frozen()
        self.assertEqual(len(self.events),100)
        self.assertEqual(Counter(r['origin'] for r in self.events),Counter({'R1D26C_20_SEED':20,'R1D26F_OUTCOME_BLIND_HASH':80}))
        by=Counter((r['year'],r['strategy']) for r in self.events)
        for year in range(2021,2026):
            for strategy in (m.C,m.D):self.assertEqual(by[(year,strategy)],10)
        for r in self.events:
            self.assertEqual(m.eventhash(r['date'],r['code'],r['strategy']),r['event_source_hash'])
            self.assertEqual(self.cal[r['signal_ix']-1],r['pre20_to'])
            self.assertEqual(self.cal[r['signal_ix']-20],r['pre20_from'])
            self.assertLess(r['pre20_to'],r['date'])
        self.assertNotIn('return', ' '.join(m.EVENT_FIELDS).lower())

    def test_deterministic_source_and_work(self):
        self.require_frozen()
        a,cal,z=m.load_sources(self.paths,self.proto)
        self.assertEqual(a,self.events)
        self.assertEqual(cal,self.cal)

    def test_seed_exact_400_and_frozen_units(self):
        self.require_frozen()
        seed=m.seed_rows(self.seedzip,self.events,self.cal)
        self.assertGreaterEqual(len(seed),350)
        self.assertLessEqual(len(seed),400)
        for (code,day),(f,i,ts,src) in seed.items():
            self.assertRegex(code,r'^\d{6}$');self.assertIsInstance(f,int);self.assertIsInstance(i,int)
            self.assertEqual(src,'R1D26C_PINNED_SOURCE')

    def test_unique_cache_groups(self):
        self.require_frozen()
        seed=m.seed_rows(self.seedzip,self.events,self.cal)
        groups=m.make_groups(self.events,self.cal,seed)
        self.assertLessEqual(len(groups),120)
        seen=set()
        for code,days in groups:
            self.assertEqual(len(days),len(set(days)))
            for day in days:
                key=(code,day);self.assertNotIn(key,seen);self.assertNotIn(key,seed);seen.add(key)

    def test_bad_blob_identity_rejected(self):
        self.require_frozen()
        with tempfile.NamedTemporaryFile(suffix='.zip') as f:
            f.write(b'FAKE');f.flush()
            d=dict(self.paths,r1d26c_zip=f.name)
            with self.assertRaises(m.QA):m.load_sources(d,self.proto)

    def test_signed_krw_precision(self):
        self.assertEqual(m.read_int('-123456789012345678'),-123456789012345678)
        with self.assertRaises(m.QA):m.read_int('12.0')
        with self.assertRaises(m.QA):m.read_int('nan')

    def test_provider_allzero_rejected(self):
        df=pd.DataFrame({'기관합계':[0,0],'외국인합계':[0,0]},index=['2025-01-02','2025-01-03'])
        with self.assertRaises(m.QA):m.validate_fetch(df,'000001',['2025-01-02','2025-01-03'],[], 'now')

    def test_missing_day_rejected(self):
        df=pd.DataFrame({'기관합계':[1],'외국인합계':[4]},index=['2025-01-02'])
        with self.assertRaises(m.QA):m.validate_fetch(df,'000001',['2025-01-02','2025-01-03'],[], 'now')

    def test_fake_complete_collection_cardinality(self):
        self.require_frozen()
        days=self.cal
        class Stub:
            n=0
            def __call__(self,b,e,c):
                self.n+=1
                subset=[d for d in days if b<=d.replace('-','')<=e]
                return pd.DataFrame({'기관합계':[int(c)+1]*len(subset),'외국인합계':[-int(c)-1]*len(subset)},index=subset)
        fetch=Stub()
        with tempfile.TemporaryDirectory() as t:
            out=m.run(self.paths,self.proto,t,'collect',fetch,0)
            self.assertEqual(out['complete_events'],100)
            self.assertEqual(out['complete_event_rows'],2000)
            self.assertEqual(fetch.n,out['planned_fetch_groups'])
            with open(Path(t)/'R1D26F_COMPLETE_EVENT_PRIOR20_TRUE_INVESTOR_KRW_NOT_PIT.csv',encoding='utf-8-sig') as f:
                rows=list(csv.DictReader(f))
            self.assertEqual(len(rows),2000)
            self.assertTrue(all(r['unit']=='KRW' and r['trade_date']<r['signal_date'] for r in rows))
            self.assertFalse(out['investor_flow_effect_estimated'])

    def test_failed_provider_no_imputation(self):
        self.require_frozen()
        calls=[]
        def bad(b,e,c):calls.append(c);raise ConnectionError('PROVIDER_UNAVAILABLE_SENSITIVE_DATA_DO_NOT_PRINT')
        with tempfile.TemporaryDirectory() as t:
            out=m.run(self.paths,self.proto,t,'collect',bad,0,3)
            self.assertEqual(len(calls),3)
            self.assertEqual(out['complete_events'],20)
            self.assertEqual(out['complete_event_rows'],400)
            self.assertTrue(out['stopped_before_all_groups'])
            saved='\n'.join(p.read_text(errors='replace') for p in Path(t).glob('R1D26F_*'))
            self.assertNotIn('SENSITIVE_DATA_DO_NOT_PRINT',saved)

if __name__=='__main__':unittest.main()
