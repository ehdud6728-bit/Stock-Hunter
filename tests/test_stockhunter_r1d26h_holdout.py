import csv, json, os, tempfile, unittest, copy
from pathlib import Path
from collections import Counter
import pandas as pd
from research_formulas import stockhunter_r1d26h_holdout as h

class HoldoutHTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.p=json.loads(Path('R1D26H_PREDECLARED_PROTOCOL.json').read_text(encoding='utf-8'))
        inp=Path(os.environ.get('R1D26H_INPUT_DIR','reports/r1d26h/input'))
        cls.paths={k:str(inp/(k+'.zip')) for k in ('r1d25','r1d26','r1d26f')}
        cls.e=str(inp/'r1d26e.zip')
        cls.available=all(Path(x).exists() for x in cls.paths.values())
        if cls.available:cls.rows,cls.seed,cls.calendar,cls.fresh=h.source(cls.p,cls.paths)

    def need_real(self):
        if not self.available:self.skipTest('Pinned archives absent; integration tests must run after source download')

    def test_protocol_locked_from_previous_g_and_not_live(self):
        h.protocol_ok(self.p)
        self.assertEqual(self.p['primary_flag'],'institution_recent5_accel_pos')
        self.assertEqual(self.p['primary_target'],'hit15_by_d30')
        self.assertTrue(self.p['historical_not_temporal_OOS'])
        self.assertFalse(self.p['PIT_certified'])

    def test_original_100_and_new_200_disjoint_tickers(self):
        self.need_real()
        self.assertEqual(len(self.rows),300)
        self.assertEqual(len(self.fresh),200)
        self.assertEqual(len({r['code'] for r in self.fresh}),200)
        self.assertFalse({r['code'] for r in self.fresh}&{r['code'] for r in self.rows[:100]})
        count=Counter((r['year'],r['strategy']) for r in self.fresh)
        self.assertEqual(len(count),10)
        self.assertTrue(all(x==20 for x in count.values()))

    def test_seed_2000_real_rows_and_determinism(self):
        self.need_real()
        same=h.source(self.p,self.paths)
        self.assertEqual(self.fresh,same[3])
        cache=h.seed_cache(self.seed,self.rows[:100],self.calendar)
        self.assertGreaterEqual(len(cache),1920)
        self.assertLessEqual(len(cache),2000)

    def test_all_20_before_signal_no_price_features(self):
        self.need_real()
        self.assertNotIn('hit15',','.join(h.FIELDS).lower())
        self.assertNotIn('return',','.join(h.FIELDS).lower())
        for r in self.fresh:
            ix=r['signal_ix'];self.assertEqual(self.calendar[ix-20],r['pre20_from'])
            self.assertEqual(self.calendar[ix-1],r['pre20_to'])
            self.assertLess(r['pre20_to'],r['date'])
            self.assertEqual(h.eh(r['date'],r['code'],r['strategy']),r['event_source_hash'])

    def test_bad_fetch_missing_or_all_zero_or_duplicate(self):
        d=pd.date_range('2025-01-02',periods=3,freq='B').strftime('%Y-%m-%d').tolist()
        bad=pd.DataFrame({'기관합계':[1,1],'외국인합계':[2,2]},index=d[:2])
        with self.assertRaises(h.QA):h.validate_provider(bad,'000001',d,'date')
        zero=pd.DataFrame({'기관합계':[0]*3,'외국인합계':[0]*3},index=d)
        with self.assertRaises(h.QA):h.validate_provider(zero,'000001',d,'date')
        dup=pd.DataFrame({'기관합계':[1]*3,'외국인합계':[1]*3},index=[d[0],d[0],d[2]])
        with self.assertRaises(h.QA):h.validate_provider(dup,'000001',d,'date')
        with self.assertRaises(h.QA):h.i64('123.00')

    def test_prepare_plan_contains_no_lookahead(self):
        self.need_real()
        with tempfile.TemporaryDirectory() as t:
            m=h.collect(self.paths,self.p,t,'prepare')
            self.assertEqual(m['combined_rows'],2000)
            self.assertEqual(m['holdout_new_completed'],0)
            with open(Path(t)/'R1D26H_NEW200_DEDUP_FETCH_PLAN.csv',encoding='utf-8-sig') as f:
                plan=list(csv.DictReader(f))
            self.assertEqual(len(plan),200)
            self.assertTrue(all(int(r['expected_sessions'])==20 for r in plan))

    def test_synthetic_full_source_6000_and_primary_evaluation(self):
        self.need_real()
        def mock(start,end,code):
            days=[d for d in self.calendar if start<=d.replace('-','')<=end]
            v=int(code)
            return pd.DataFrame({'기관합계':[v+1]*(len(days)-5)+([v*2+3]*5 if int(h.sh(code.encode())[-1],16)%2 else [-v-2]*5),'외국인합계':[-v-1]*len(days)},index=days)
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);s=root/'src'
            m=h.collect(self.paths,self.p,s,'collect',mock,0)
            self.assertEqual(m['holdout_new_completed'],200)
            self.assertEqual(m['combined_rows'],6000)
            self.assertEqual(m['fetch_attempted'],200)
            self.assertEqual(m['official_audit_template_rows'],10)
            with open(s/'R1D26H_KRX_INDEPENDENT_AMOUNT_CROSSCHECK_10POINT_TEMPLATE.csv',encoding='utf-8-sig') as f:
                checks=list(csv.DictReader(f))
            self.assertEqual(len(checks),10)
            self.assertTrue(all(x['independently_verified']=='NO_OFFICIAL_INPUT' and not x['official_KRX_foreign_net_krw'] for x in checks))
            if Path(self.e).exists():
                prot=dict(self.p,bootstrap_draws=80,permutation_draws=100)
                outcome=h.eval_locked(prot,s,self.e,root/'test',80,100)
                self.assertEqual(outcome['holdout_events'],200)
                self.assertEqual(outcome['D30_complete']+outcome['D30_censored'],200)
                self.assertFalse(outcome['strategy_ready'])
                self.assertGreater(outcome['primary']['n_accel'],10)
                self.assertGreater(outcome['primary']['n_not_accel'],10)
                self.assertIsNotNone(outcome['primary']['difference_percentage_points'])
                # Integrity check against tampered amount file
                with (s/'R1D26H_COMPLETE_300_PRIOR20_SIGNED_KRW_NOT_PIT.csv').open('a') as o:o.write('tamper')
                with self.assertRaises(h.QA):h.eval_locked(prot,s,self.e,root/'test2',80,100)

    def test_fail_closed_no_new_values_and_no_secret_leaks(self):
        self.need_real()
        class Unavailable:
            n=0
            def __call__(self,*args):self.n+=1;raise RuntimeError('TOP_SECRET_MUST_NOT_LEAK')
        fetch=Unavailable()
        with tempfile.TemporaryDirectory() as t:
            m=h.collect(self.paths,self.p,t,'collect',fetch,0,4)
            self.assertEqual(fetch.n,4)
            self.assertEqual(m['holdout_new_completed'],0)
            self.assertEqual(m['combined_rows'],2000)
            self.assertTrue(m['consecutive_failure_stop'])
            for x in Path(t).glob('*'):
                self.assertNotIn('TOP_SECRET_MUST_NOT_LEAK',x.read_text(encoding='utf-8-sig'))

    def test_modified_pinned_bytes_rejected(self):
        self.need_real()
        with tempfile.TemporaryDirectory() as t:
            p=Path(t)/'bad.zip';p.write_bytes(b'NOT_PINNED')
            bad=dict(self.paths,r1d26f=str(p))
            with self.assertRaises(h.QA):h.source(self.p,bad)

if __name__=='__main__':unittest.main()
