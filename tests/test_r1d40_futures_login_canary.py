import importlib.util
import json
import os
import pathlib
import tempfile
import unittest
from unittest.mock import patch
import pandas as pd

MODULE=pathlib.Path(__file__).resolve().parents[1]/'research_formulas'/'r1d40_futures_login_canary.py'
spec=importlib.util.spec_from_file_location('r1d40_canary',MODULE)
canary=importlib.util.module_from_spec(spec)
spec.loader.exec_module(canary)

class TestProbe(unittest.TestCase):
    def fake(self,date,product,alternative=False):
        n='코스피200 F 202512 (주간)' if product=='KRDRVFUK2I' else '코스닥150 F 202512 (주간)'
        df=pd.DataFrame([{'종목명':n,'시가':350,'고가':360,'저가':345,'종가':355,'거래량':12345},
                         {'종목명':'코스피200 SP 2512-2603 (주간)','시가':1,'고가':2,'저가':1,'종가':1.5,'거래량':5}])
        df.index=['101XX000','401XX000']
        return df
    def test_nominal_no_secret_leak(self):
        with tempfile.TemporaryDirectory() as td:
            out=pathlib.Path(td)
            env={'KRX_ID':'do-not-log-private-id','KRX_PW':'do-not-log-private-password'}
            x=canary.run(out,provider=self.fake,env=env)
            self.assertEqual(x['status'],'PASS_2DAY_2PRODUCT_SOURCE_ONLY')
            b=(out/'CANARY_SAFE_AUDIT.json').read_text()
            self.assertNotIn(env['KRX_ID'],b)
            self.assertNotIn(env['KRX_PW'],b)
            self.assertEqual(len(x['results']),4)
            self.assertEqual(x['results'][0]['contracts_checked'],1)
    def test_missing_credentials_fail_closed(self):
        with tempfile.TemporaryDirectory() as td:
            x=canary.run(pathlib.Path(td),provider=self.fake,env={})
            self.assertEqual(x['status'],'CREDENTIALS_NOT_INJECTED')
            self.assertEqual(x['results'],[])
    def test_provider_exception_no_body(self):
        with tempfile.TemporaryDirectory() as td:
            def bad(*args,**kwargs):
                raise RuntimeError('Authorization: Bearer SECRET-RAW-UPSTREAM')
            x=canary.run(pathlib.Path(td),provider=bad,env={'KRX_ID':'v','KRX_PW':'v'})
            self.assertEqual(x['status'],'FAIL_CLOSED_PROBE_INCOMPLETE')
            self.assertNotIn('SECRET-RAW-UPSTREAM',(pathlib.Path(td)/'CANARY_SAFE_AUDIT.json').read_text())
    def test_empty_response_never_success(self):
        with tempfile.TemporaryDirectory() as td:
            x=canary.run(pathlib.Path(td),provider=lambda *a,**k:pd.DataFrame(),env={'KRX_ID':'v','KRX_PW':'v'})
            self.assertEqual(x['status'],'FAIL_CLOSED_PROBE_INCOMPLETE')
            self.assertTrue(all(r['status']=='NO_DATA' for r in x['results']))

if __name__=='__main__':unittest.main()
