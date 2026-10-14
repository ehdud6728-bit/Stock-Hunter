"""R1.6.2 causal guards + verification on the previously archived evidence."""
import importlib.util, json, pathlib, tempfile, unittest, sys
import pandas as pd

MODULE=pathlib.Path(__file__).resolve().parents[1]/'closebet_research/native_price_regime_forensics.py'
spec=importlib.util.spec_from_file_location('price_regime',MODULE)
mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)

class TestR162(unittest.TestCase):
    def fixture(self,root):
        s=pd.DataFrame([{'signal_date':'2026-09-14','code':'000001','mode':'A','entry_price':105,
            '_open':100,'_high':110,'_low':95,'_close':105,'_vol':1000},
            {'signal_date':'2026-09-14','code':'000002','mode':'B1','entry_price':100,
            '_open':100,'_high':110,'_low':95,'_close':100,'_vol':1000}])
        p=pd.DataFrame([{'date':'2026-09-14','code':'000001','open':100,'high':110,'low':95,'close':100,'volume':1000},
            {'date':'2026-09-14','code':'000002','open':100,'high':110,'low':95,'close':100,'volume':1000}])
        a=root/'signals.csv';b=root/'krx.csv';s.to_csv(a,index=False);p.to_csv(b,index=False);return a,b

    def test_fail_closed_parity_without_price_rewrite(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=pathlib.Path(tmp);a,b=self.fixture(root)
            f,doc=mod.build_analysis(a,b,'2026-10-12',root/'out')
            self.assertEqual(doc['overall_mismatched_bars'],1)
            self.assertEqual(doc['overall_matching_bars'],1)
            self.assertEqual(doc['only_close_diff_bars'],1)
            self.assertEqual(doc['strategy_performance_status'],'WITHHELD')
            self.assertEqual(float(f.loc[f.code=='000001','_close'].iloc[0]),105)
            self.assertEqual(float(f.loc[f.code=='000001','close'].iloc[0]),100)

    def test_refuse_future_bar_or_future_signal(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=pathlib.Path(tmp);a,b=self.fixture(root)
            x=pd.read_csv(a,dtype={'code':str});x.loc[0,'signal_date']='2026-10-14';x.to_csv(a,index=False)
            with self.assertRaisesRegex(ValueError,'FUTURE_NATIVE_SIGNAL'):
                mod.build_analysis(a,b,'2026-10-12',root/'out')
            x.loc[0,'signal_date']='2026-09-14';x.to_csv(a,index=False)
            y=pd.read_csv(b,dtype={'code':str});y.loc[0,'date']='2026-10-14';y.to_csv(b,index=False)
            with self.assertRaisesRegex(ValueError,'FUTURE_INDEPENDENT_KRX_BAR'):
                mod.build_analysis(a,b,'2026-10-12',root/'out')

    def test_regime_date_unchanged(self):
        self.assertEqual(mod.JUMP_DATE,'2026-09-14')

if __name__=='__main__':unittest.main()
