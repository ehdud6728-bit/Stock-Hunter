import sys,unittest
from pathlib import Path
import numpy as np
import pandas as pd
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from band_structure import enrich_bands,band_report,future_price_tamper_copy


def sample(n=500):
    dates=pd.bdate_range('2022-01-03',periods=n)
    z=np.arange(n)
    close=100+z*.03+4*np.sin(z/6)
    return pd.DataFrame({'date':dates,'open':close,'close':close,'high':close+2,'low':close-2})

class TestFullBands(unittest.TestCase):
    def setUp(self):
        self.raw=sample();self.d=enrich_bands(self.raw)
        self.day=str(self.d.iloc[470].date.date())
        self.rows=band_report(self.d,self.day)
    def test_full_five_band_families(self):
        self.assertEqual([(r['family'],r['period']) for r in self.rows],
            [('BB',20),('BB',30),('BB',40),('ENV',20),('ENV',40)])
    def test_upper_and_lower_present(self):
        self.assertTrue(all(r['upper']>r['lower'] for r in self.rows))
    def test_envelope_relative_width_formula_fixed(self):
        for r in self.rows:
            if r['family']=='ENV':
                self.assertAlmostEqual(r['band_width_pct'],20.,places=6)
                self.assertAlmostEqual(r['upper']/r['center'],1.1,places=3)
                self.assertAlmostEqual(r['lower']/r['center'],.9,places=3)
    def test_bb_width_matches_population_deviation(self):
        k=470;x=self.raw.close.iloc[k-19:k+1]
        expected=4*x.std(ddof=0)
        self.assertAlmostEqual(self.d.iloc[k]['BB20_WIDTH_ABS'],expected,places=6)
    def test_bb_width_changes_while_env_relative_does_not(self):
        a=band_report(self.d,str(self.d.iloc[400].date.date()))
        b=band_report(self.d,str(self.d.iloc[450].date.date()))
        self.assertNotEqual(a[0]['band_width_pct'],b[0]['band_width_pct'])
        self.assertEqual(a[3]['band_width_pct'],b[3]['band_width_pct'])
    def test_20day_changes_are_asof_only(self):
        self.assertTrue(any(r['width_change20_pp'] is not None for r in self.rows))
        self.assertTrue(all(r['upper_slope20_pct'] is not None for r in self.rows))
    def test_future_tamper_no_lookahead(self):
        modified=self.raw.copy()
        modified.loc[modified.index>470,'close']=2000
        modified.loc[modified.index>470,'high']=2100
        modified.loc[modified.index>470,'low']=1800
        self.assertEqual(self.rows,band_report(enrich_bands(modified),self.day))
    def test_outside_upper_and_lower_distinguished(self):
        changed=self.raw.copy();changed.loc[470,'high']=1000;changed.loc[470,'low']=1
        r=band_report(enrich_bands(changed),self.day)
        self.assertTrue(all(x['high_touched_upper'] and x['low_touched_lower'] for x in r))
    def test_older_window_unavailable(self):
        with self.assertRaises(ValueError):band_report(self.d,str(self.d.iloc[20].date.date()))
    def test_missing_band_columns(self):
        with self.assertRaises(KeyError):band_report(self.raw,self.day)
    def test_preserve_original(self):
        self.assertNotIn('BB30_HI',self.raw.columns)
        self.assertIn('BB30_HI',self.d.columns)
    def test_ratio_normalized_to_env(self):
        self.assertTrue(all(r['bb_to_envelope_width_ratio']>0 for r in self.rows if r['family']=='BB' and r['period'] in (20,40)))
        self.assertIsNone([r for r in self.rows if r['family']=='BB' and r['period']==30][0]['bb_to_envelope_width_ratio'])


class TestPITIntOHLCRegression(unittest.TestCase):
    """Regression for GitHub Actions run 41373486891: int64 -> float crash."""

    def make_integer_ohlc(self, count=100):
        dates=pd.bdate_range('2024-01-01',periods=count)
        close=np.arange(count,dtype=np.int64)+5000
        return pd.DataFrame({'date':dates,'open':close+3,'high':close+17,
                             'low':close-19,'close':close,'amount':close*100000})

    def test_future_fractional_write_is_dtype_safe_and_preserves_past(self):
        raw=self.make_integer_ohlc()
        original=raw.copy(deep=True)
        t=55
        changed=future_price_tamper_copy(raw,t)
        pd.testing.assert_frame_equal(raw,original)
        for col in ('open','high','low','close'):
            self.assertTrue(pd.api.types.is_float_dtype(changed[col]))
            np.testing.assert_array_equal(changed[col].iloc[:t+1], raw[col].iloc[:t+1])
            np.testing.assert_allclose(changed[col].iloc[t+1:],raw[col].iloc[t+1:]*1.217)
        pd.testing.assert_series_equal(changed['amount'],raw['amount'])
        pd.testing.assert_series_equal(changed['date'],raw['date'])

    def test_asof_bands_identical_after_future_tamper(self):
        raw=self.make_integer_ohlc()
        asof_idx=65
        day=str(raw.iloc[asof_idx].date.date())
        original=enrich_bands(raw)
        changed=enrich_bands(future_price_tamper_copy(original,asof_idx))
        self.assertEqual(band_report(original,day),band_report(changed,day))
        for n in (20,30,40):
            self.assertAlmostEqual(float(original.iloc[asof_idx][f'BB{n}_WIDTH_PCT']),
                                   float(changed.iloc[asof_idx][f'BB{n}_WIDTH_PCT']))

    def test_irregular_integer_index_is_safe(self):
        raw=self.make_integer_ohlc(80)
        raw.index=np.arange(900,980)
        changed=future_price_tamper_copy(raw,40)
        self.assertEqual(changed.loc[940,'close'],float(raw.loc[940,'close']))
        self.assertAlmostEqual(changed.loc[941,'close'],raw.loc[941,'close']*1.217)

    def test_out_of_range_and_invalid_multiplier_rejected(self):
        d=self.make_integer_ohlc(50)
        with self.assertRaises(ValueError):future_price_tamper_copy(d,50)
        with self.assertRaises(ValueError):future_price_tamper_copy(d,35,0)

if __name__=='__main__':unittest.main()
