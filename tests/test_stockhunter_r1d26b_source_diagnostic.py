import unittest
import io
import zipfile
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock
import pandas as pd
from research_formulas import stockhunter_r1d26b_source_diagnostic as m

ARCHIVE=Path('testdata/r1d26_verified_pilot.zip')

class SourceDiagnosticContract(unittest.TestCase):
    def test_frozen_real_source_verified_and_5day_prior(self):
        if not ARCHIVE.is_file():self.skipTest('optional runtime original historical ZIP')
        event,days=m.asof_case(ARCHIVE.read_bytes())
        self.assertEqual(len(days),5)
        self.assertTrue(all(d<event['date'] for d in days))
        self.assertEqual(event['date'],'2025-08-29')
        self.assertEqual(event['code'],'101490')

    def test_reject_bad_source_zip(self):
        with self.assertRaises(m.Reject):m.asof_case(b'not original archive')

    def test_verified_pykrx_five_full_days_provisional_not_pit(self):
        dates=['2025-08-22','2025-08-25','2025-08-26','2025-08-27','2025-08-28']
        df=pd.DataFrame({'기관합계':[4,2,-1,0,3],'외국인합계':[5,1,-2,-1,6]},index=pd.to_datetime(dates))
        z=m.pykrx_probe({'code':'101490'},dates,lambda a,b,c:df)
        self.assertEqual(z['verified_sessions'],5)
        self.assertFalse(z['same_date_publication_PIT_verified'])

    def test_pykrx_missing_day_not_zero(self):
        dates=['2025-08-22','2025-08-25','2025-08-26','2025-08-27','2025-08-28']
        df=pd.DataFrame({'기관합계':[1],'외국인합계':[4]},index=pd.to_datetime([dates[0]]))
        z=m.pykrx_probe({'code':'101490'},dates,lambda a,b,c:df)
        self.assertEqual(z['verified_sessions'],0)

    def test_pykrx_empty_and_allzero_rejected(self):
        dates=['2025-08-22','2025-08-25','2025-08-26','2025-08-27','2025-08-28']
        for df in [pd.DataFrame(),pd.DataFrame({'기관합계':[0]*5,'외국인합계':[0]*5},index=pd.to_datetime(dates))]:
            self.assertFalse(m.pykrx_probe({'code':'101490'},dates,lambda *args:df)['provider_values_present'])

    def test_official_landing_not_json_certification(self):
        r=Mock(status_code=200,headers={'content-type':'text/html'},text='<html><body></body></html>',content=b'<html>')
        z=m.official_landing_probe(lambda *args,**kw:r)
        self.assertTrue(z['html_verified'])
        self.assertFalse(z['data_api_verified'])

    def test_naver_html_extract_shares_not_krw(self):
        html='''<html><table class="type2"><tr><td>2025.08.28</td><td>123</td><td>1</td><td>1%</td><td>1,000</td><td>+1,234</td><td>-99</td><td>100</td></tr></table></html>'''
        result=m.extract_naver_share_rows(html)
        self.assertEqual(result,[{'trade_date':'2025-08-28','institution_net_shares':1234,'foreign_net_shares':-99}])
        r=Mock(status_code=200,headers={'content-type':'text/html; charset=utf-8'},text=html,content=html.encode())
        z=m.naver_probe({'code':'101490'},['2025-08-28'],lambda *args,**kw:r,1)
        self.assertEqual(z['exact_requested_days_found'],1)
        self.assertFalse(z['eligible_for_KRW_model'])

    def test_naver_empty_no_fabricated_values(self):
        r=Mock(status_code=200,headers={'content-type':'text/html'},text='<html/>',content=b'<html/>')
        z=m.naver_probe({'code':'101490'},['2025-08-28'],lambda *args,**kw:r,1)
        self.assertEqual(z['rows'],[])
        self.assertEqual(z['status'],'NO_COMPLETE_HISTORICAL_WINDOW')

    def test_naver_page_limit(self):
        with self.assertRaises(m.Reject):m.naver_probe({'code':'101490'},[],None,4)

    def test_frozen_source_integration_no_network_mocked(self):
        if not ARCHIVE.is_file():self.skipTest('optional runtime original historical ZIP')
        with TemporaryDirectory() as t:
            r=Mock(status_code=403,headers={'content-type':'text/plain'},text='blocked',content=b'blocked')
            v=m.produce(ARCHIVE,Path(t)/'out',krx_get=lambda *a,**k:r,
                pykrx_fetch=lambda *a:pd.DataFrame(),naver_get=lambda *a,**kw:r,naver_pages=1)
            self.assertFalse(v['historical_investor_model_ready'])
            self.assertEqual(v['verified_full_historical_KRW_20_sessions'],0)
            self.assertFalse(v['live_changed'])
            self.assertTrue((Path(t)/'out'/'R1D26B_SINGLE_CASE_SOURCE_DIAGNOSTIC.json').is_file())
            self.assertEqual((Path(t)/'out'/'R1D26B_NAVER_SHARES_DIAGNOSTIC_NOT_KRW.csv').read_text(encoding='utf-8-sig').count('\n'),1)

    def test_no_future_or_strategy_labels(self):
        forbidden=['net_20bps_pct','D30_return','outcome_win_label','order_enable']
        s=Path(m.__file__).read_text()
        for t in forbidden:self.assertNotIn(t,s)

if __name__=='__main__':unittest.main()
