import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock
from research_formulas.stockhunter_r1d27b_krx_auth_diagnostic import (secrets_status, configured_secret_pair, provider_codes, classify, diagnostic)

class TestDiagnostics(unittest.TestCase):
    def test_presence_does_not_leak(self):
        d=secrets_status({'KRX_ID':'my-user','KRX_PW':'super_secret_123'})
        self.assertEqual(d,{'KRX_ID':True,'KRX_PW':True})
        self.assertNotIn('super_secret_123',str(d))
    def test_secret_alias_mixed_detection(self):
        env={'KRX_DATA_ID_DEFINED':'true','KRX_DATA_PW_DEFINED':'false',
             'KRX_LEGACY_ID_DEFINED':'false','KRX_LEGACY_PW_DEFINED':'true'}
        self.assertTrue(configured_secret_pair(env)['mixed_pair_risk'])
        env['KRX_DATA_PW_DEFINED']='true'
        self.assertFalse(configured_secret_pair(env)['mixed_pair_risk'])

    def test_error_codes_whitelist(self):
        self.assertEqual(provider_codes('foo CD010, ERR123, token=abc123'),['CD010','ERR123'])
    def test_missing(self):
        self.assertEqual(classify({'secret_presence':{'KRX_ID':True,'KRX_PW':False}},[]),'SECRETS_NOT_INJECTED')
    def test_auth(self):
        self.assertEqual(classify({'secret_presence':{'KRX_ID':True,'KRX_PW':True}},[{'http_status':403}]),'HTTP_AUTH_OR_PERMISSION_REJECTED')
    def test_html(self):
        self.assertEqual(classify({'secret_presence':{'KRX_ID':True,'KRX_PW':True}},[{'http_status':200,'content_kind':'HTML'}]),'HTML_INSTEAD_OF_EXPECTED_KRX_DATA')
    def test_throttle(self):
        self.assertEqual(classify({'secret_presence':{'KRX_ID':True,'KRX_PW':True}},[{'http_status':429}]),'HTTP_RATE_LIMITED')
    def test_success(self):
        self.assertEqual(classify({'secret_presence':{'KRX_ID':True,'KRX_PW':True},'complete_data_endpoints':4,'attempted_data_endpoints':4},[]),'KRX_PROVIDER_DIAGNOSTIC_DATA_COMPLETE')
    def test_http_200_html_with_all_valid_data_must_not_fail(self):
        # Reproduces real GitHub run #40607154677: 4/4 valid data, six 200 HTML.
        d={'secret_presence':{'KRX_ID':True,'KRX_PW':True},
           'complete_data_endpoints':4,'attempted_data_endpoints':4,
           'configured_secret_pair':{'mixed_pair_risk':False}}
        http=[{'http_status':200,'content_kind':'HTML'} for _ in range(6)]
        self.assertEqual(classify(d,http),'KRX_PROVIDER_DIAGNOSTIC_DATA_COMPLETE')
    def test_http_200_html_without_valid_data_still_fails(self):
        d={'secret_presence':{'KRX_ID':True,'KRX_PW':True},
           'complete_data_endpoints':0,'attempted_data_endpoints':4}
        self.assertEqual(classify(d,[{'http_status':200,'content_kind':'HTML'}]),
                         'HTML_INSTEAD_OF_EXPECTED_KRX_DATA')
    def test_http_403_overrides_valid_data(self):
        d={'secret_presence':{'KRX_ID':True,'KRX_PW':True},
           'complete_data_endpoints':4,'attempted_data_endpoints':4}
        self.assertEqual(classify(d,[{'http_status':403,'content_kind':'HTML'}]),
                         'HTTP_AUTH_OR_PERMISSION_REJECTED')
    def test_date_mismatch_is_not_validated(self):
        import pandas as pd
        from unittest.mock import patch
        v=pd.DataFrame({'기관합계':[12]*5,'외국인합계':[-1]*5},
                       index=pd.date_range('2025-02-03',periods=5,freq='D'))
        with tempfile.TemporaryDirectory() as d:
            req=Mock();req.sessions.Session.request=Mock()
            with patch('importlib.metadata.version',return_value='1.2.9'):
                z=diagnostic(d,env={'KRX_ID':'abc','KRX_PW':'xyz'},
                    fetch={'NET_VALUE_KRW':lambda *a,**k:v,'NET_VOLUME_SHARES':lambda *a,**k:v},requests_module=req)
            self.assertEqual(z['complete_data_endpoints'],0)
            self.assertTrue(all(x['status']=='DATA_FRAME_DATE_NUMERIC_OR_ZERO_INVALID' for x in z['calls']))
    def test_write_no_secret_when_missing(self):
        with tempfile.TemporaryDirectory() as d:
            o=diagnostic(d,env={'KRX_ID':'my-user','KRX_PW':''})
            self.assertEqual(o['diag_status'],'SECRETS_NOT_INJECTED')
            raw=(Path(d)/'R1D27B_KRX_AUTH_DIAGNOSTIC.json').read_text()
            self.assertNotIn('my-user',raw)
    def test_readonly_data_success(self):
        import pandas as pd
        v=pd.DataFrame({'기관합계':[123,45,67,89,12],'외국인합계':[-45,21,-3,55,2]},
                       index=pd.date_range('2025-01-06',periods=5,freq='D'))
        with tempfile.TemporaryDirectory() as d:
            from unittest.mock import patch
            req=Mock()
            req.sessions.Session.request=Mock()
            with patch('importlib.metadata.version',return_value='1.2.8'):
                o=diagnostic(d,env={'KRX_ID':'person','KRX_PW':'pass'},
                  fetch={'NET_VALUE_KRW':lambda *a,**kw:v,'NET_VOLUME_SHARES':lambda *a,**kw:v},requests_module=req)
            self.assertEqual(o['complete_data_endpoints'],4)
            self.assertEqual(o['diag_status'],'KRX_PROVIDER_DIAGNOSTIC_DATA_COMPLETE')
            self.assertNotIn('person',(Path(d)/'R1D27B_KRX_AUTH_DIAGNOSTIC.json').read_text())

if __name__=='__main__':unittest.main()
