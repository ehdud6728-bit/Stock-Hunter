"""Network-free R1.7.1 materialized daily KRX end-to-end regression cases."""
import os,sys,tempfile,unittest,json,hashlib,types
from pathlib import Path
from unittest import mock
import pandas as pd
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from closebet_research.krx_daily_materialize import (normalize_day,fetch_shard,build_db,official_sessions)
from closebet_research.krx_only_provider import _convert,_dates,_fetch_sqlite
from closebet_research.krx_native_gate import check_shard,strict_price_equality,compare_selected

def fixture():
    ix=[str(i).zfill(6) for i in range(250)]
    all_ix=[str(i).zfill(6) for i in range(500)]
    raw=pd.DataFrame({'시가':[100+i for i in range(500)],'고가':[110+i for i in range(500)],
        '저가':[90+i for i in range(500)],'종가':[105+i for i in range(500)],
        '거래량':[300+i for i in range(500)],'거래대금':[31000+i for i in range(500)]},index=all_ix)
    days=['2026-10-08','2026-10-12']
    class KRX:
        def get_market_ohlcv_by_ticker(self,day,market='ALL'):
            assert market=='ALL' and day in ('20261008','20261012')
            return raw
    return KRX(),days,ix

class MaterializeTests(unittest.TestCase):
    def test_materialize_to_sqlite_and_offline_per_symbol_loader(self):
        src,days,codes=fixture()
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);cache=root/'snap';database=root/'stocks.db'
            (root/'universe.json').write_text(json.dumps({'codes':codes}))
            for i in range(4):
                m=fetch_shard(src,days,set(codes),i,4,cache)
                self.assertEqual(m['status'],'COMPLETE')
            report=build_db(cache,database,4,root/'universe.json')
            self.assertEqual(report['price_rows'],500)
            with mock.patch.dict(os.environ,{'CLOSING_BET_KRX_DATABASE':str(database),'CLOSING_BET_KRX_REQUEST_ASOF':'2026-10-12'}):
                d=_fetch_sqlite('20261008','20261014','000010')
                self.assertEqual(len(d),2)
                self.assertEqual(d.loc['2026-10-12','종가'],115)
                v=_convert(d)
                self.assertEqual(v.loc['2026-10-12','Amount'],31010)
                self.assertEqual(v.loc['2026-10-12','Change'],0)
                with self.assertRaisesRegex(RuntimeError,'KRX_HISTORY_START_NOT_COVERED'):
                    _fetch_sqlite('20261007','20261012','000010')
            # Changing one compressed day MUST fail sealed-source identity.
            p=cache/'20261008.csv.gz';p.write_bytes(p.read_bytes()+b'TAMPER')
            with self.assertRaisesRegex(RuntimeError,'KRX_MATERIALIZE_FILE_SHA_MISMATCH'):
                build_db(cache,database,4,root/'universe.json')
    def test_no_naver_adjusted_fallback_if_db_missing(self):
        with mock.patch.dict(os.environ,{'CLOSING_BET_KRX_DATABASE':'/missing/invalid_file.db','CLOSING_BET_KRX_REQUEST_ASOF':'2026-10-12'}):
            with self.assertRaisesRegex(RuntimeError,'KRX_PREMATERIALIZED_DB_MISSING'):
                _fetch_sqlite('20261008','20261012','000010')
    def test_bad_daily_session_is_fatal(self):
        with self.assertRaisesRegex(RuntimeError,'TOO_FEW_TICKERS'):
            normalize_day(pd.DataFrame({'시가':[1],'종가':[2]}),'2026-10-08',{'000010'})
    def test_asof_cutoff_and_no_future_data(self):
        with mock.patch.dict(os.environ,{'CLOSING_BET_KRX_REQUEST_ASOF':'2026-10-12'}):
            self.assertEqual(_dates('20261008','20261014'),('20261008','20261012'))
    def test_real_gate_rejects_absent_trace(self):
        with tempfile.TemporaryDirectory() as t:
            p=Path(t);(p/'shard_00_raw.csv').write_text('z')
            (p/'shard_00_manifest.json').write_text(json.dumps({'engine_status':'VALID','control_flow':'V49_75_SHARD_EXPORT_RETURN_MAIN_EXIT','shard_index':0,'raw_sha256':hashlib.sha256(b'z').hexdigest()}))
            (p/'shard_00_complete.json').write_text(json.dumps({'status':'SHARD_ONLY_COMPLETE'}))
            with self.assertRaisesRegex(RuntimeError,'NO_KRX_PROVIDER_TRACE'):check_shard(p,0)
    def test_cross_session_parity_rejects_any_changed_signal_close(self):
        with tempfile.TemporaryDirectory() as t:
            p=Path(t)
            pd.DataFrame([{'signal_date':'2026-09-22','code':'044450','entry_price':11540,'_open':10100,'_high':12000,'_low':10000,'_close':11540,'_vol':1234}]).to_csv(p/'s.csv',index=False)
            pd.DataFrame([{'date':'2026-09-22','code':'044450','open':10100,'high':12000,'low':10000,'close':10060,'volume':1234}]).to_csv(p/'d.csv',index=False)
            with self.assertRaisesRegex(RuntimeError,'STRICT_KRX_SOURCE_MISMATCH'):strict_price_equality(p/'s.csv',p/'d.csv',p)

if __name__=='__main__':unittest.main()
