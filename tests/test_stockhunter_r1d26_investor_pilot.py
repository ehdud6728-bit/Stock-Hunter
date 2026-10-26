import json
import tempfile
import unittest
from pathlib import Path
import pandas as pd
from research_formulas.stockhunter_r1d26_investor_pilot import (
    Reject,calendar_from_indices,datecheck,filehash,to_integer,
    normalize_pykrx,select_pilot,EVENT_COLS,FLOW_COLS,C,D)

class PilotContractTests(unittest.TestCase):
    def setUp(self):
        self.expected=['2021-01-01','2021-01-04']
        self.e={'date':'2021-01-05','code':'005930','strategy':C,'source_event_sha256':'abcd'}
    def frame(self):
        return pd.DataFrame({'기관합계':[10,-20],'외국인합계':[-3,7]},index=pd.to_datetime(self.expected))
    def test_provider_complete_signed_krw(self):
        x=normalize_pykrx(self.frame(),self.e,self.expected,'timestamp')
        self.assertEqual(len(x),2);self.assertEqual(x[0]['institution_net_krw'],10)
        self.assertEqual(x[0]['foreign_net_krw'],-3)
    def test_provider_missing_day_not_zero(self):
        with self.assertRaises(Reject):normalize_pykrx(self.frame().iloc[:1],self.e,self.expected,'timestamp')
    def test_provider_future_day_rejected(self):
        x=self.frame();x.index=pd.to_datetime(['2021-01-01','2021-01-05'])
        with self.assertRaises(Reject):normalize_pykrx(x,self.e,self.expected,'timestamp')
    def test_provider_duplicate_day_rejected(self):
        x=self.frame();x.index=pd.to_datetime(['2021-01-01','2021-01-01'])
        with self.assertRaises(Reject):normalize_pykrx(x,self.e,self.expected,'timestamp')
    def test_provider_schema_required(self):
        with self.assertRaises(Reject):normalize_pykrx(self.frame().drop(columns=['외국인합계']),self.e,self.expected,'timestamp')
    def test_all_zero_response_quarantined(self):
        x=self.frame()*0
        with self.assertRaises(Reject):normalize_pykrx(x,self.e,self.expected,'timestamp')
    def test_integer_contract(self):
        self.assertEqual(to_integer('-1,000'),-1000)
        with self.assertRaises(Reject):to_integer('미제공')
    def test_date_iso(self):
        self.assertEqual(str(datecheck('2021-01-05')),'2021-01-05')
        with self.assertRaises(Reject):datecheck('20210105')
    def test_no_secret_outcomes_columns(self):
        self.assertNotIn('net_20bps_pct',EVENT_COLS)
        self.assertNotIn('mfe_intraday_pct',EVENT_COLS)
        self.assertNotIn('sector_name',FLOW_COLS)
    def test_sample_uses_asof_only_deterministic(self):
        sample=[];cal=[f'{y}-01-{d:02d}' for y in range(2021,2026) for d in range(1,25)]
        for y in range(2021,2026):
            for strat in [C,D]:
                for d in range(21,25):
                    date=f'{y}-01-{d:02d}'
                    sample.append({'date':date,'code':f'{d:06d}','strategy':strat,'signal_ix':cal.index(date),
                                   'signal_close':100.0,'amount_vs_prior20':1.2,'ma224_slope20_recomputed':0.5,
                                   'rs20_vs_kospi_pctp':2,'rs20_vs_kosdaq_pctp':1,'preflow_rising_and_price_soft':0})
        f=pd.DataFrame(sample)
        a=select_pilot(f,cal,2);b=select_pilot(f,cal,2)
        self.assertEqual(len(a),20);self.assertEqual(a.to_csv(index=False),b.to_csv(index=False))
        self.assertTrue(all(a.date>a.pre20_to))
        self.assertTrue(all(a.date.str[:4]==a.year.astype(str)))
        self.assertTrue(a.code.str.len().eq(6).all())

if __name__=='__main__':unittest.main()
