import unittest, math
import numpy as np,pandas as pd
from research_formulas import stockhunter_r1d25_448_d30 as m

class R1D25Tests(unittest.TestCase):
    @staticmethod
    def synthetic(n=580):
        a=[]
        for t in range(n):
            cl=100+0.03*t
            a.append((t,cl,cl+2,cl-2,cl,10000+t,5e8+t*10000))
        idx={r[0]:i for i,r in enumerate(a)}
        market=pd.DataFrame({'KOSPI':np.linspace(1800,2600,n),'KOSDAQ':np.linspace(600,900,n)})
        return a,idx,market
    def test_448_same_day_only(self):
        a,idx,k=self.synthetic();f=m.get_feature(a,540,idx,k)
        self.assertAlmostEqual(f['ma448'],np.mean([r[4] for r in a[540-447:541]]))
        self.assertEqual(f['asof_lookback_last_index'],540)
    def test_future_candles_do_not_change_signal(self):
        a,idx,k=self.synthetic();f=m.get_feature(a,510,idx,k)
        for t in range(580,620): a.append((t,100000,100002,99999,100000,20000,9000000))
        g=m.get_feature(a,510,idx,k)
        self.assertEqual(f,g)
    def test_448_early_history_unavailable(self):
        a,idx,k=self.synthetic()
        f=m.get_feature(a,420,idx,k)
        self.assertEqual(f['ma448_available'],0)
        self.assertTrue(math.isnan(f['ma448']))
    def test_448_slope_requires_past_20(self):
        a,idx,k=self.synthetic()
        f=m.get_feature(a,467,idx,k)
        self.assertEqual(f['ma448_available'],1)
        self.assertGreater(f['ma448_slope20_pct'],0)
    def test_d30_calculates_entry_plus_cost(self):
        a,idx,k=self.synthetic(560)
        out=m.forward_path30(a,499,idx,[str(t) for t in range(560)],a[499][4]);r=out[-1]
        self.assertEqual(r['status_out'],m.DONE)
        expected=100*(a[529][4]/a[500][1]-1)-.20
        self.assertAlmostEqual(r['net_20bps_pct'],expected)
    def test_d30_gap_censors_but_d5_stays_complete(self):
        a,idx,k=self.synthetic(560)
        x=list(a[511]);x[1]=x[4]*1.7;a[511]=tuple(x)
        out=m.forward_path30(a,499,idx,[str(t) for t in range(560)],a[499][4]);self.assertEqual(out[0]['status_out'],m.DONE)
        self.assertEqual(out[-1]['status_out'],m.CENS)
    def test_d30_missing_middle_date_censored(self):
        a,idx,k=self.synthetic(560)
        a.pop(514);idx={r[0]:i for i,r in enumerate(a)}
        out=m.forward_path30(a,499,idx,[str(t) for t in range(560)],a[499][4]);self.assertEqual(out[-1]['status_out'],m.CENS)
    def test_preflow_uses_prior_only(self):
        a,idx,k=self.synthetic()
        f=m.get_feature(a,520,idx,k)
        y=list(a[520]);y[6]=1e13;a[520]=tuple(y)
        q=m.get_feature(a,520,idx,k)
        self.assertAlmostEqual(f['amount_pre5_vs_pre20'],q['amount_pre5_vs_pre20'])
        self.assertAlmostEqual(f['signed_turnover_pre20'],q['signed_turnover_pre20'])
    def test_signal_day_amount_spike_does_not_change_priorflow(self):
        a,idx,k=self.synthetic()
        x=m.get_feature(a,500,idx,k)
        v=list(a[500]);v[6]=v[6]*200;a[500]=tuple(v)
        y=m.get_feature(a,500,idx,k)
        self.assertEqual(x['amount_pre20_vs_pre40'],y['amount_pre20_vs_pre40'])
        self.assertEqual(x['clv_turnover_pre20'],y['clv_turnover_pre20'])
    def test_448_rejects_historical_action_gap_only_for_448(self):
        a,idx,k=self.synthetic()
        v=list(a[500]);v[1]=v[4]*1.6;a[500]=tuple(v)
        f=m.get_feature(a,530,idx,k)
        self.assertEqual(f['ma448_available'],0)
        self.assertTrue(math.isnan(f['ma448']))
    def test_d30_mfe_intraday_not_close(self):
        a,idx,k=self.synthetic(560)
        v=list(a[510]);v[2]=v[1]*1.25;a[510]=tuple(v)
        r=m.forward_path30(a,499,idx,[str(t) for t in range(560)],a[499][4])[-1]
        self.assertGreater(r['mfe_intraday_pct'],20)
        self.assertLess(r['net_20bps_pct'],20)
    def test_d30_censor_stays_censored(self):
        a,idx,k=self.synthetic(560)
        a.pop(507);idx={r[0]:i for i,r in enumerate(a)}
        out=m.forward_path30(a,499,idx,[str(t) for t in range(560)],a[499][4])
        self.assertEqual([r['status_out'] for r in out],[m.DONE]+[m.CENS]*4)
    def test_market_relative_strength_is_asof(self):
        a,idx,k=self.synthetic()
        x=m.get_feature(a,520,idx,k)
        market2=k.copy();market2.loc[550:,'KOSPI']=99999;market2.loc[550:,'KOSDAQ']=99999
        y=m.get_feature(a,520,idx,market2)
        self.assertEqual(x['rs20_vs_kospi_pctp'],y['rs20_vs_kospi_pctp'])
        self.assertEqual(x['rs60_vs_kosdaq_pctp'],y['rs60_vs_kosdaq_pctp'])
    def test_industry_classification_not_imputed(self):
        a,idx,k=self.synthetic()
        self.assertNotIn('sector',m.get_feature(a,500,idx,k))
        self.assertEqual(len(m.experiment_groups()),15)

if __name__=='__main__':unittest.main()
