"""No live KRX network is contacted by these structural/regression tests."""
import importlib,io,json,os,sys,tempfile,types,unittest
from pathlib import Path
from unittest import mock
import pandas as pd

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from closebet_research.krx_only_provider import _convert,_dates
from closebet_research.krx_native_gate import compare_selected, check_shard, strict_price_equality

class KRXOnlyTests(unittest.TestCase):
    def test_official_columns_and_fdr_fraction_change(self):
        f=pd.DataFrame({'시가':[100,108],'고가':[110,125],'저가':[95,104],
            '종가':[100,120],'거래량':[300,400],'거래대금':[31000,45000], '등락률':[5.2,20]},
            index=pd.to_datetime(['2026-10-08','2026-10-12']))
        out=_convert(f)
        self.assertEqual(out.loc['2026-10-12','Close'],120)
        self.assertAlmostEqual(out.loc['2026-10-12','Change'],.2)
        self.assertEqual(out.loc['2026-10-12','Amount'],45000)
    def test_strict_asof_ceiling(self):
        with mock.patch.dict(os.environ,{'CLOSING_BET_KRX_REQUEST_ASOF':'2026-10-12'}):
            self.assertEqual(_dates('2026-08-03','2026-10-14'),('20260803','20261012'))
            self.assertEqual(_dates('20260803','20261014'),('20260803','20261012'))
    def test_signal_churn(self):
        with tempfile.TemporaryDirectory() as t:
            p=Path(t)
            pd.DataFrame([{'signal_date':'2026-09-22','code':'044450','mode':'C','entry_price':10060},
                 {'signal_date':'2026-09-23','code':'005930','mode':'A','entry_price':100}]).to_csv(p/'a.csv',index=False)
            pd.DataFrame([{'signal_date':'2026-09-22','code':'044450','mode':'C','entry_price':11540},
                 {'signal_date':'2026-09-23','code':'000660','mode':'I','entry_price':100}]).to_csv(p/'b.csv',index=False)
            r=compare_selected(p/'a.csv',p/'b.csv',p)
            self.assertEqual((r['krx_only'],r['legacy_only'],r['same_key']),(1,1,1))
            self.assertEqual(r['different_entry_over_0_5pct'],1)
    def test_bridge_forces_krx_raw_and_clips_asof(self):
        from closebet_research import krx_only_provider as mod
        dates=[]
        raw=pd.DataFrame({'시가':[100,108],'고가':[110,125],'저가':[95,104],
               '종가':[100,120],'거래량':[300,400],'거래대금':[31000,45000]},
               index=pd.to_datetime(['2026-10-08','2026-10-12']))
        def stock_raw(start,end,ticker,adjusted=True,**kwargs):
            dates.append((start,end,ticker,adjusted))
            return raw
        fake_stock=types.SimpleNamespace(get_market_ohlcv_by_date=stock_raw,
               get_index_ohlcv_by_date=lambda *args:raw)
        fake_pykrx=types.ModuleType('pykrx');fake_pykrx.stock=fake_stock
        fake_fdr=types.ModuleType('FinanceDataReader')
        fake_fdr.DataReader=lambda *args,**kwargs:None
        mod._INSTALLED=False;mod._PERSIST=None
        with mock.patch.dict(sys.modules,{'pykrx':fake_pykrx,'FinanceDataReader':fake_fdr}):
            with mock.patch.dict(os.environ,{'CLOSING_BET_KRX_ONLY_RESEARCH':'1',
                                             'CLOSING_BET_KRX_REQUEST_ASOF':'2026-10-12'},clear=False):
                mod.install()
                d=fake_fdr.DataReader('005930',start='2026-10-08',end='2026-10-14')
                self.assertEqual(d.Close.iloc[-1],120)
                self.assertEqual(dates[-1],('20261008','20261012','005930',False))
                fake_stock.get_market_ohlcv_by_date('20261008','20261015','005930',adjusted=True)
                self.assertEqual(dates[-1][-1],False)
                with self.assertRaisesRegex(ValueError,'KRX_NON_KRX_SYMBOL_FORBIDDEN'):
                    fake_fdr.DataReader('YAHOO',start='2026-10-08')
        mod._INSTALLED=False
    def test_strict_bar_equality_rejects_changed_close(self):
        with tempfile.TemporaryDirectory() as t:
            p=Path(t)
            s={'signal_date':'2026-09-22','code':'044450','entry_price':11540,
              '_open':10100,'_high':12000,'_low':10000,'_close':11540,'_vol':1234}
            b={'date':'2026-09-22','code':'044450','open':10100,
              'high':12000,'low':10000,'close':10060,'volume':1234}
            pd.DataFrame([s]).to_csv(p/'s.csv',index=False)
            pd.DataFrame([b]).to_csv(p/'b.csv',index=False)
            with self.assertRaisesRegex(RuntimeError,'STRICT_KRX_SOURCE_MISMATCH'):
                strict_price_equality(p/'s.csv',p/'b.csv',p)
            z=json.loads((p/'r17_strict_krx_price_audit.json').read_text())
            self.assertEqual(z['exact_mismatched_or_missing'],1)
            s['entry_price']=10060;s['_close']=10060
            pd.DataFrame([s]).to_csv(p/'s.csv',index=False)
            v=strict_price_equality(p/'s.csv',p/'b.csv',p)
            self.assertEqual(v['exact_matched'],1)
    def test_gate_rejects_no_trace(self):
        with tempfile.TemporaryDirectory() as t:
            p=Path(t)
            (p/'shard_00_raw.csv').write_text('hello')
            import hashlib
            raw=(p/'shard_00_raw.csv').read_bytes()
            (p/'shard_00_manifest.json').write_text(json.dumps({'engine_status':'VALID','control_flow':'V49_75_SHARD_EXPORT_RETURN_MAIN_EXIT','shard_index':0,'raw_sha256':hashlib.sha256(raw).hexdigest()}))
            (p/'shard_00_complete.json').write_text(json.dumps({'status':'SHARD_ONLY_COMPLETE'}))
            with self.assertRaisesRegex(RuntimeError,'NO_KRX_PROVIDER_TRACE'):check_shard(p,0)

if __name__=='__main__':unittest.main()
