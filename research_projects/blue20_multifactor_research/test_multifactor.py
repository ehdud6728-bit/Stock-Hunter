from __future__ import annotations
import sys,tempfile,unittest
from pathlib import Path
import numpy as np
import pandas as pd
HERE=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(HERE))
import multifactor as M

class TestSixFactorPIT(unittest.TestCase):
    def prices(self,n=270):
        dates=pd.bdate_range('2022-01-03',periods=n)
        c=np.linspace(100,150,n)
        return pd.DataFrame({'code':['000001']*n,'date':dates.strftime('%Y-%m-%d'),
           'open':c,'high':c+2,'low':c-2,'close':c,'volume':[1_000_000]*n,
           'amount':[100_000_000]*n,'market':['KOSPI']*n})
    def market(self,n=270):
        dates=pd.bdate_range('2022-01-03',periods=n)
        c=np.linspace(2000,2500,n)
        return pd.DataFrame({'market':['KOSPI']*n,'date':dates.strftime('%Y-%m-%d'),
                             'close':c,'amount':[3e12]*n})
    def test_stoch_standard_constant_price(self):
        d=self.prices()
        d[['high','low','close']]=100
        k,slow,cross=M.slow_stochastic(d)
        self.assertAlmostEqual(k.iloc[-1],50)
        self.assertAlmostEqual(slow.iloc[-1],50)
        self.assertFalse(cross.any())
    def test_stoch_gc_uses_previous_day(self):
        d=self.prices()
        k,slow,cross=M.slow_stochastic(d)
        self.assertFalse(bool(cross.iloc[0]))
        self.assertTrue(pd.isna(k.iloc[12]))
        self.assertTrue(pd.isna(slow.iloc[15]))
    def test_wave_no_future_leakage(self):
        d=self.prices()
        x=M.wave_pre_d0(d,240)
        d2=d.copy();d2.loc[241:,'high']=9_999_999
        y=M.wave_pre_d0(d2,240)
        self.assertEqual(x,y)
    def test_wave_prior_peak_and_pullback(self):
        d=self.prices();d.loc[165:180,'low']=98
        d.loc[195,'high']=130
        d.loc[196:220,'low']=114
        d.loc[200:239,'volume']=100
        d.loc[240,'close']=130
        x=M.wave_pre_d0(d,240)
        self.assertTrue(np.isfinite(x['wave_advance_pct']))
        self.assertTrue(np.isfinite(x['wave_pullback_pct']))
    def test_pit_known_at_blocks_backfill(self):
        d=self.prices(100)
        r=pd.DataFrame([{'code':'000001','sector':'Semiconductors','known_at':'2022-04-01',
                         'valid_from':'2022-01-03','valid_to':''}])
        a=M.assign_pit(d,r,'sector')
        self.assertEqual(a.loc[a.date=='2022-03-01','sector'].iloc[0],'PIT_MISSING')
        self.assertEqual(a.loc[a.date=='2022-04-01','sector'].iloc[0],'Semiconductors')
    def test_pit_overlap_fails_closed(self):
        d=self.prices(100)
        r=pd.DataFrame([
            {'code':'000001','sector':'A','known_at':'2022-01-03','valid_from':'2022-01-03','valid_to':''},
            {'code':'000001','sector':'B','known_at':'2022-01-03','valid_from':'2022-01-03','valid_to':''}])
        with self.assertRaisesRegex(ValueError,'OVERLAPPING_SECTOR'):
            M.assign_pit(d,r,'sector')
    def test_absent_pit_not_neutral(self):
        d=M.assign_pit(self.prices(60),None,'sector')
        self.assertTrue(d.sector.eq('PIT_MISSING').all())
    def test_market_regime_uses_only_past(self):
        d=self.prices();m=self.market()
        a,aa=M.prepare_panel(d,m)
        self.assertEqual(aa.market_regime.iloc[-1],'UPTREND')
        m2=m.copy();m2.loc[260:,'close']=9999
        _,bb=M.prepare_panel(d,m2)
        self.assertEqual(float(aa.loc[250,'market_ret20_pct']),float(bb.loc[250,'market_ret20_pct']))
    def test_leader_rank_subset_not_market_universe(self):
        d=self.prices();m=self.market()
        p,md=M.prepare_panel(d,m)
        x=M.current_day_factors(p,md)
        self.assertTrue(x.sector.eq('PIT_MISSING').all())
        self.assertTrue(x.cohort_amount_rank_500.eq(1).all())
    def test_summary_sparse_does_not_pass_gate(self):
        x=pd.DataFrame({'code':['000001'],'date':['2023-01-02'],
             'GC_NEXT_OPEN_mfe_pct':[9.75],'GC_NEXT_OPEN_net_pct':[9.75],
             'high10_gross':[True],'realized_tp':[True],'realized_sl':[False],'band':['UPPER']})
        result=M.summarize(x,['band'])
        self.assertEqual(result.iloc[0]['n'],1)
        self.assertFalse(bool(result.iloc[0]['screen_n30_stocks15_year10']))
    def test_500_manifest_fails_missing(self):
        with tempfile.TemporaryDirectory() as t:
            p=Path(t)/'manifest.csv'
            p.write_text('code,path,sha256\n000001,no.csv,aaa\n')
            with self.assertRaisesRegex(ValueError,'EXACT_500_REQUIRED'):
                M.load_verified_manifest(p)

if __name__=='__main__':unittest.main()

class TestJointExecution(unittest.TestCase):
    def test_joint_entry_next_open_tp_sl_same_day_stop_first(self):
        dates=pd.bdate_range('2022-01-03',periods=27)
        x=pd.DataFrame({'date':dates.strftime('%Y-%m-%d'),'open':[100.]*27,
                        'high':[101.]*27,'low':[99.]*27,'close':[100.]*27})
        x.loc[7,'high']=112.;x.loc[7,'low']=94.
        result=M.simulate_joint_next_open(x,d0_idx=2,confirm_offset=3)
        self.assertEqual(result['JOINT_exit_reason'],'SL_BOTH_HIT')
        self.assertAlmostEqual(result['JOINT_net_pct'],-5.25)
        self.assertAlmostEqual(result['JOINT_peak_gross_pct'],12)
    def test_slow_later_rsi_state_must_still_be_bullish(self):
        n=27;dates=pd.bdate_range('2022-01-03',periods=n)
        x=pd.DataFrame({'code':['000001']*n,'date':dates.strftime('%Y-%m-%d'),
                        'open':[100.]*n,'high':[102.]*n,'low':[99.]*n,'close':[100.]*n,
                        'stoch_gc':[False]*n,'stoch_k':[70.]*n,'stoch_d':[60.]*n,
                        'rsi_WILDER_SMA9':[60.]*n,'rsi_signal_WILDER_SMA9':[50.]*n})
        x.loc[5,'stoch_gc']=True
        e=pd.DataFrame([{'code':'000001','date':str(dates[3].date()),
           'rsi_mode':'WILDER_SMA9','gc_offset':0,'gc_status':'GC_OBSERVED'}])
        rows=M.enrich_stoch_timing(e,{'000001':x})
        self.assertEqual(rows.slow_vs_rsi.iloc[0],'SLOW_AFTER_RSI')
        self.assertEqual(rows.joint_status.iloc[0],'JOINT_CONFIRMED')
        x.loc[5,'rsi_WILDER_SMA9']=40.
        rows=M.enrich_stoch_timing(e,{'000001':x})
        self.assertEqual(rows.joint_status.iloc[0],'RSI_NOT_BULLISH_AT_JOIN')

class TestExternalPITAvailability(unittest.TestCase):
    def test_sector_and_theme_prices_are_asof_only(self):
        n=85; dates=pd.bdate_range('2022-01-03',periods=n); parts=[]
        for i in range(6):
            c=np.arange(n)+100+i
            parts.append(pd.DataFrame({'code':[str(i+1).zfill(6)]*n,
                'date':dates.strftime('%Y-%m-%d'),'open':c,'high':c+2,'low':c-2,
                'close':c,'volume':[10000+i*10]*n,'amount':[1e8+i*1e6]*n,
                'market':['KOSPI']*n}))
        prices=pd.concat(parts,ignore_index=True)
        market=pd.DataFrame({'market':['KOSPI']*n,'date':dates.strftime('%Y-%m-%d'),
                             'close':np.arange(n)+2000,'amount':[3e12]*n})
        membership=pd.DataFrame([{'code':str(i+1).zfill(6),'sector':'GROUP',
          'known_at':str(dates[50].date()),'valid_from':str(dates[0].date()),'valid_to':''}
          for i in range(6)])
        theme=membership.rename(columns={'sector':'theme'})
        panel,md=M.prepare_panel(prices,market)
        d=M.current_day_factors(panel,md,membership,theme)
        self.assertTrue(d.loc[d.date==str(dates[49].date()),'sector'].eq('PIT_MISSING').all())
        after=d.loc[d.date==str(dates[80].date())]
        self.assertEqual(after.sector_amount_rank.nunique(),6)
        self.assertEqual(after.sector_leader.eq('COHORT_SECTOR_TURNOVER_1').sum(),1)
        self.assertTrue(after.theme_relative20_pp.notna().all())

class TestMappingLoadPIT(unittest.TestCase):
    def test_sector_file_schema_requires_known_at(self):
        with tempfile.TemporaryDirectory() as t:
            p=Path(t)/'sector.csv'
            p.write_text('code,sector,known_at,valid_from,valid_to\n000001,TEST,2022-01-03,2022-01-03,\n')
            a=M.load_pit_membership(p,'sector')
            self.assertEqual(a.loc[0,'sector'],'TEST')
            p.write_text('code,sector\n000001,TEST\n')
            with self.assertRaisesRegex(ValueError,'SECTOR_PIT_REQUIRED'):
                M.load_pit_membership(p,'sector')
