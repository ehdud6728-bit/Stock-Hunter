import unittest
import numpy as np
import pandas as pd
from closebet_research.r114_threshold_free_wave_gallery import asof_metrics, METRICS, summarize_metrics, pinned

class R114ResearchChecks(unittest.TestCase):
    def make_history(self):
        days=pd.date_range('2025-01-01',periods=330,freq='B').strftime('%Y-%m-%d')
        v=np.linspace(1000,4500,330)
        c=np.linspace(10000,11500,330)
        return pd.DataFrame({'open':c,'high':c*1.02,'low':c*.98,'close':c,'volume':v},index=days)
    def test_no_future_price_dependency(self):
        h=self.make_history();dt=h.index[-12]
        a=asof_metrics(h,dt)
        h.loc[h.index[-10]:,'close']=999999
        h.loc[h.index[-10]:,'volume']=1
        b=asof_metrics(h,dt)
        self.assertEqual(a,b)
    def test_all_descriptive_metrics_exposed(self):
        h=self.make_history();x=asof_metrics(h,h.index[-1]);self.assertTrue(all(np.isfinite(x[k]) for k in METRICS))
    def test_older20_does_not_leak_signal_day(self):
        h=self.make_history();dt=h.index[-1];a=asof_metrics(h,dt)
        h.loc[dt,'volume']*=1000
        b=asof_metrics(h,dt)
        self.assertAlmostEqual(a['prior5_over_older20'],b['prior5_over_older20'])
        self.assertNotEqual(a['inclusive5_over_older20'],b['inclusive5_over_older20'])
    def test_nontradeable_history_is_not_filled(self):
        h=self.make_history();dt=h.index[-1];d=h.index[-20]
        h.loc[d,['open','high','low','volume']]=0
        x=asof_metrics(h,dt)
        self.assertEqual(x['descriptive_window_valid'],0)
        self.assertEqual(x['window_224_nontradeable_bar_count'],1)
        self.assertTrue(np.isnan(x['prior5_over_older20']))
    def test_insufficient_history_is_unevaluable(self):
        h=self.make_history().iloc[:250]
        rec=asof_metrics(h,h.index[-1]);self.assertEqual(rec['descriptive_window_valid'],0)
        self.assertTrue(np.isnan(rec['prior5_over_older20']))
    def test_quantile_summary_not_future_outcome(self):
        x=asof_metrics(self.make_history(),self.make_history().index[-1])
        d=pd.DataFrame([{**x,'descriptive_group':'NO_WAVE_PROXY','code':'000001'}])
        res=summarize_metrics(d)
        self.assertEqual(len(res),len(METRICS))
        self.assertEqual(res.iloc[0]['n'],1)
    def test_missing_source_fails_closed(self):
        with self.assertRaises((RuntimeError,FileNotFoundError)):pinned('/tmp/non-r114-r112','/tmp/non-r114-r113','/tmp/non-r114-r171')
if __name__=='__main__':unittest.main()
