"""Fail-closed tests for frozen R1D26G retrospective-only source/outcome join."""
import json
import os
import tempfile
import unittest
from pathlib import Path
import pandas as pd
from research_formulas import stockhunter_r1d26g_flow_ladder as mod

class R1D26G_FrozenSourceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path=Path(os.environ.get('R1D26G_TEST_INPUT_DIR','reports/r1d26g/input'))
        cls.files=[path/'R1D26F_frozen.zip',path/'R1D26E_frozen.zip',path/'R1D25_frozen.zip']
        cls.protocol_path=Path('R1D26G_PREDECLARED_PROTOCOL.json')
        cls.protocol=json.loads(cls.protocol_path.read_text(encoding='utf-8'))
        for f in cls.files:
            if not f.exists():raise RuntimeError('R1D26G frozen source missing: '+str(f))
        cls.ev,cls.fl,cls.path,cls.asof,*_=mod.load(cls.protocol,cls.files)
        cls.join=mod.validate_and_aggregate(cls.ev,cls.fl,cls.path,cls.asof)

    def test_frozen_sha_and_locked_identifiers(self):
        self.assertEqual(len(self.ev),100)
        self.assertEqual(len(self.fl),2000)
        self.assertEqual(len(self.path),10198)
        self.assertTrue(self.protocol['research_only'])
        self.assertFalse(self.protocol['strategy_ready'])

    def test_no_future_rows_join_and_C_D_censor(self):
        self.assertEqual(self.join.D30_completed.sum(),99)
        self.assertEqual(self.join.groupby('strategy').D30_completed.sum().tolist(),[50,49])
        self.assertEqual(len(self.join),100)
        self.assertFalse((self.fl.trade_date>=self.fl.signal_date).any())

    def test_per_year_stratification(self):
        self.assertEqual(len(self.ev.groupby(['year','strategy'])),10)
        self.assertTrue((self.ev.groupby(['year','strategy']).size()==10).all())

    def test_source_tamper_values_rejected(self):
        f=self.fl.copy();f.loc[0,'foreign_net_krw']=pd.NA
        with self.assertRaisesRegex(ValueError,'MISSING_AMOUNTS'):mod.validate_and_aggregate(self.ev,f,self.path,self.asof)

    def test_wrong_provider_unit_rejected(self):
        f=self.fl.copy();f.loc[0,'unit']='SHARES'
        with self.assertRaisesRegex(ValueError,'UNIT_OR_PROVIDER'):mod.validate_and_aggregate(self.ev,f,self.path,self.asof)

    def test_duplicate_ticker_day_rejected(self):
        f=self.fl.copy();f.loc[1,['signal_date','code','strategy','trade_date']]=f.loc[0,['signal_date','code','strategy','trade_date']]
        with self.assertRaisesRegex(ValueError,'FLOW_DUPLICATE'):mod.validate_and_aggregate(self.ev,f,self.path,self.asof)

    def test_forged_post_signal_day_rejected(self):
        f=self.fl.copy();f.loc[0,'trade_date']=f.loc[0,'signal_date']
        with self.assertRaisesRegex(ValueError,'EVENT_PRIOR_ONLY'):mod.validate_and_aggregate(self.ev,f,self.path,self.asof)

    def test_missing_daily_row_rejected(self):
        with self.assertRaisesRegex(ValueError,'SOURCE_CARDINALITY'):mod.validate_and_aggregate(self.ev,self.fl.iloc[1:],self.path,self.asof)

    def test_wrong_zip_sha_rejected(self):
        p=self.protocol.copy();p['r1d26f_zip_sha256']='0'*64
        with self.assertRaisesRegex(ValueError,'R1D26F_ZIP_SHA'):mod.load(p,self.files)

    def test_binary_thresholds_and_monotone_hits(self):
        joined=self.join[self.join.D30_completed]
        self.assertTrue(((joined.hit5_by_d30>=joined.hit10_by_d30)&(joined.hit10_by_d30>=joined.hit15_by_d30)).all())
        self.assertTrue(joined.foreign_signed_abs_ratio20.dropna().between(-1,1).all())
        self.assertEqual(int(joined.hit15_by_d30.sum()),34)

    def test_group_denominators_and_year_coverage(self):
        a,b,c,d=mod.make_summaries(self.join)
        self.assertEqual(a.n.sum(),99)
        self.assertEqual(len(c),2*len(mod.FLAGS)*2)
        self.assertEqual(len(d),2*len(mod.PRIMARY))
        self.assertEqual(int(b.n.sum()),99)
        self.assertTrue(all(c.groupby(['strategy','feature']).n.sum()==[50 if st==mod.STRATEGIES[0] else 49 for st,feat in c.groupby(['strategy','feature']).groups]))

    def test_bootstrap_predeclared_determinism(self):
        *_,contrasts=mod.make_summaries(self.join)
        a=mod.bootstrap_cluster_year_ticker(self.join,contrasts,iterations=200,seed=26026)
        b=mod.bootstrap_cluster_year_ticker(self.join,contrasts,iterations=200,seed=26026)
        pd.testing.assert_frame_equal(a,b)
        self.assertEqual(len(a),2*len(mod.PRIMARY)*3)

    def test_protocol_no_live_no_fill_no_pit_claim(self):
        self.assertFalse(self.protocol['LIVE_changed'])
        self.assertEqual(self.protocol['orders_placed'],0)
        self.assertEqual(self.protocol['PIT_publication'],'NOT_VERIFIED')
        self.assertEqual(self.protocol['turnover_ratio'],'NOT_COMPUTED_NO_TOTAL_TURNOVER_INPUT')

if __name__=='__main__':unittest.main()
