"""Regression for GitHub Actions run 40215421099 ('date' index/column ambiguity)."""
import tempfile
import unittest
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np
import pandas as pd
from research_formulas import stockhunter_r1d23_strategy_challenge as m


class MarketRegimeRegression(unittest.TestCase):
    def test_index_date_merge_and_asof_lag20(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            krx = root / 'krx' / 'shard_00'
            krx.mkdir(parents=True)
            result = root / 'result'
            result.mkdir()
            dates = pd.bdate_range('2019-01-02', periods=1907).strftime('%Y-%m-%d')
            signals = [dates[450], dates[460]]
            rows = []
            for i, day in enumerate(dates):
                rows.extend([
                    {'date':day,'index':'KOSPI','close':1000.0+i},
                    {'date':day,'index':'KOSDAQ','close':800.0+2*i},
                ])
            pd.DataFrame(rows).to_csv(krx / 'official_indices.csv',index=False)
            signals_df = pd.DataFrame([
                {'strategy':m.STRATEGIES[0], 'date':signals[0], 'code':'000001', 'period':'EARLY_2021_2023'},
                {'strategy':m.STRATEGIES[1], 'date':signals[1], 'code':'000002', 'period':'EARLY_2021_2023'},
            ])
            signals_df.to_csv(result/'R1D23_FULLMARKET_A_B_C_D_ASOF_ONLY.csv.gz', index=False,compression='gzip')
            paths = []
            for row in signals_df.to_dict('records'):
                paths.append({**row,'horizon':5,'status_out':m.DONE,'net_20bps_pct':1.0})
            pd.DataFrame(paths).to_csv(result/'R1D23_FROZEN_FUTURE_PATHS.csv.gz',index=False,compression='gzip')
            n = m.audit_market_regime(result, root/'krx')
            self.assertEqual(n,2)
            annotated = pd.read_csv(result/'R1D23_ASOF_MARKET_REGIME_DESCRIPTIVE.csv.gz')
            self.assertEqual(len(annotated),2)
            self.assertTrue((annotated['regime']=='MIXED_OR_MODERATE').all())
            for day in signals:
                t=list(dates).index(day)
                actual=annotated.loc[annotated.date==day].iloc[0]
                self.assertAlmostEqual(actual.KOSPI_RET20,100*(1000+t)/(1000+t-20)-100, places=8)
                self.assertAlmostEqual(actual.KOSDAQ_RET20,100*(800+2*t)/(800+2*(t-20))-100,places=8)
            self.assertTrue((pd.read_csv(result/'R1D23_ASOF_MARKET_REGIME_SENSITIVITY.csv')['n']==1).all())

if __name__=='__main__':unittest.main()
