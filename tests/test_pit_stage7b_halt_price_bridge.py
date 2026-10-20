"""Offline Stage7-B fail-closed contracts. No KRX API / auth credentials."""
import csv
import gzip
import hashlib
import io
import json
import pathlib
import sys
import tempfile
import unittest
ROOT=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'research_pit'))
import pit_stage7b_halt_price_bridge as m


def row(code,date,volume):return {'code':code,'date':date,'volume':volume,'amount':0 if not volume else 1000,'close':6000}
def event(code,date):return {'code':code,'name':'test','official_halt_event_date':date,'official_resume_date':None,
                             'provider_last_trade_date_snapshot_only':'2026/09/18'}

class Stage7BTests(unittest.TestCase):
    def test_positive_volume_on_halt_start_does_not_prove_resume(self):
        obs={'418250':[row('418250','2024-04-04',500),row('418250','2024-04-05',51071),
                       row('418250','2024-04-08',0),row('418250','2024-04-09',0)]}
        s,d,a=m.make_bridge([event('418250','2024-04-05')],obs)
        self.assertTrue(s[0]['event_day_volume_positive'])
        self.assertEqual(s[0]['strictly_post_event_zero_volume_sessions'],2)
        self.assertEqual(s[0]['strictly_post_event_positive_volume_sessions'],0)
        self.assertFalse(s[0]['actual_official_resume_date_confirmed'])
        self.assertFalse(s[0]['provider_last_trade_date_is_official_resume_evidence'])
        self.assertEqual({v['official_daily_halt_status'] for v in d},{'UNKNOWN'})

    def test_later_positive_print_is_not_official_resume(self):
        obs={'377460':[row('377460','2023-11-27',10),row('377460','2023-11-28',0),
                       row('377460','2023-11-29',0),row('377460','2023-11-30',7)]}
        s,detail,neighborhood=m.make_bridge([event('377460','2023-11-28')],obs)
        self.assertEqual(s[0]['first_positive_volume_after_event'],'2023-11-30')
        self.assertEqual(s[0]['zero_volume_run_immediately_after_event_sessions'],1)
        self.assertFalse(s[0]['after_event_positive_trade_is_official_resume_evidence'])
        self.assertIsNone(s[0]['official_resume_date'])
        self.assertTrue(all(v['tradability_for_backtest']=='UNKNOWN' for v in detail))

    def test_provider_last_trade_date_with_zero_original_volume_is_not_resume(self):
        e=event('377460','2023-11-28');e['provider_last_trade_date_snapshot_only']='2023/11/29'
        x={'377460':[row('377460','2023-11-27',10),row('377460','2023-11-28',0),row('377460','2023-11-29',0)]}
        summary,_,_=m.make_bridge([e],x)
        self.assertEqual(summary[0]['provider_last_trade_date_price_volume'],0)
        self.assertFalse(summary[0]['provider_last_trade_date_positive_volume_corrob'])
        self.assertFalse(summary[0]['provider_last_trade_date_is_official_resume_evidence'])

    def test_missing_on_event_day_is_fail_closed(self):
        obs={'377460':[row('377460','2023-11-27',10),row('377460','2023-11-29',0)]}
        with self.assertRaisesRegex(RuntimeError,'FOCUS_MISSING_PRE_POST_HALT_DAY'):
            m.make_bridge([event('377460','2023-11-28')],obs)

    def test_manifest_raw_sha_tamper_fails_closed(self):
        with tempfile.TemporaryDirectory() as d:
            p=pathlib.Path(d);p.joinpath('raw_krx').mkdir()
            first=[]
            for code,date in [('377460','2023/11/28'),('418250','2024/04/05')]:
                path='raw_krx/issue_'+code+'_20230101_20241230.json'
                raw=json.dumps({'output':[{'ISU_CD':code,'ISU_NM':'test','TRD_HALT_DD':date,'RESUMP_DD':'',
                                           'LST_TRD_DD':'2026/09/18'}]}).encode()
                (p/path).write_bytes(raw)
                first.append({'code':code,'start':'20230101','end':'20241230','raw_file':path,
                              'sha256':m.digest(raw),'source_listing_isin':'KR7000000000'})
            m.jout(p/'official_halt_raw_manifest.json',{'status':'COMPLETE','files':first})
            m.jout(p/'stage7_official_halt_canary_summary.json',{'provider_rows_returned':2})
            evt, info=m.load_events(p,strict=False)
            self.assertEqual(len(evt),2)
            self.assertEqual(info['raw_query_files_verified'],2)
            (p/first[0]['raw_file']).write_bytes(b'{}')
            with self.assertRaisesRegex(RuntimeError,'STAGE7A_RAW_SHA_MISMATCH'):m.load_events(p,strict=False)

    def test_wrong_event_identity_fails_closed(self):
        with tempfile.TemporaryDirectory() as d:
            p=pathlib.Path(d);(p/'raw_krx').mkdir()
            name='raw_krx/issue_377460_20230101_20241230.json'
            raw=b'{"output":[{"ISU_CD":"377460","TRD_HALT_DD":"2024/04/05","RESUMP_DD":""}]}'
            (p/name).write_bytes(raw)
            m.jout(p/'official_halt_raw_manifest.json',{'status':'COMPLETE','files':[{'code':'377460','start':'20230101','end':'20241230',
                 'raw_file':name,'sha256':m.digest(raw),'source_listing_isin':'KR7377460001'}]})
            m.jout(p/'stage7_official_halt_canary_summary.json',{})
            with self.assertRaisesRegex(RuntimeError,'MISMATCHED_POSITIVE_HALT_IDENTITY'):m.load_events(p,strict=False)

    def test_stage1_price_gzip_sha_and_count(self):
        with tempfile.TemporaryDirectory() as d:
            base=pathlib.Path(d);s=base/'shard_00';s.mkdir()
            day='2024-04-05'
            payload=('code,date,open,high,low,close,volume,amount\n'
                     '418250,2024-04-05,6000,7000,5000,6550,51071,500000\n').encode()
            raw=gzip.compress(payload,mtime=0)
            (s/'20240405.csv.gz').write_bytes(raw)
            m.jout(s/'manifest_shard_00.json',{'status':'COMPLETE_PRICE_COLLECTION_ONLY','universe_filter':'NONE',
               'shard':0,'shards':1,'files':[{'date':day,'file':'20240405.csv.gz','sha256':m.digest(raw),'stored_rows':1}]})
            obs,inf=m.price_observations(base,{day:1},{},targets=['418250'],shards=1,strict=False)
            self.assertEqual(obs['418250'][0]['volume'],51071)
            self.assertEqual(inf['all_market_price_rows_counted'],1)
            (s/'20240405.csv.gz').write_bytes(raw+b'altered')
            with self.assertRaisesRegex(RuntimeError,'SOURCE_PRICE_DAY_SHA_MISMATCH'):
                m.price_observations(base,{day:1},{},targets=['418250'],shards=1,strict=False)

    def test_negative_volume_fails_closed(self):
        with tempfile.TemporaryDirectory() as d:
            base=pathlib.Path(d);s=base/'shard_00';s.mkdir()
            day='2024-04-05';raw=gzip.compress(b'code,date,open,high,low,close,volume,amount\n418250,2024-04-05,1,1,1,1,-1,0\n',mtime=0)
            (s/'20240405.csv.gz').write_bytes(raw)
            m.jout(s/'manifest_shard_00.json',{'status':'COMPLETE_PRICE_COLLECTION_ONLY','universe_filter':'NONE',
               'shard':0,'shards':1,'files':[{'date':day,'file':'20240405.csv.gz','sha256':m.digest(raw),'stored_rows':1}]})
            with self.assertRaisesRegex(RuntimeError,'FOCUS_NUMERIC_VALUE_INVALID'):
                m.price_observations(base,{day:1},{},targets=['418250'],shards=1,strict=False)

    def test_read_only_and_strategy_never_ready(self):
        code=(ROOT/'research_pit/pit_stage7b_halt_price_bridge.py').read_text()
        self.assertIn("'strategy_ready':False",code)
        self.assertIn("'official_daily_halt_status':'UNKNOWN'",code)
        self.assertIn("'official_halt_duration_verified':False",code)
        for prohibited in ['place_order(', 'LIVE_ENABLE=1', 'strategy_ready=True', "'tradability_for_backtest':'YES'"]:
            self.assertNotIn(prohibited,code)

if __name__=='__main__':unittest.main()
