import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

from research_formulas import stockhunter_r1d19_bb_reclaim as m


class R1D19ResearchOnly(unittest.TestCase):
    def setUp(self):
        self.p = m.source_protocol('R1D19_PREDECLARED_PROTOCOL.json')

    def history(self, values):
        return list(enumerate(values))

    def test_protocol_exploration_not_future_oos(self):
        self.assertEqual(self.p['historical_population_expected'], 5247)
        self.assertEqual(self.p['bands'], [20, 30, 40])
        self.assertTrue(self.p['constraints']['no_live_score_rank_order_change'])

    def test_requires_224_contiguous_sessions(self):
        x = self.history([100.0] * 223)
        self.assertEqual(m.compute_flags(x, 222)['asof_status'], 'INSUFFICIENT_224_HISTORY')

    def test_gap_rejected_without_fill(self):
        x = self.history([100.0] * 224)
        x[-2] = (888, 100.0)
        self.assertEqual(m.compute_flags(x, 223)['asof_status'], 'NONCONTIGUOUS_SESSION_HISTORY')

    def test_extreme_corporate_action_guard(self):
        x = self.history([100.0] * 223 + [10.0])
        self.assertEqual(m.compute_flags(x, 223)['asof_status'], 'UNADJUSTED_EXTREME_GAP_GUARD')

    def test_nan_bar_rejected(self):
        x = self.history([100.0] * 222 + [np.nan, 100.0])
        self.assertEqual(m.compute_flags(x, 223)['asof_status'], 'INVALID_RAW_CLOSE')

    def test_ddof_population_matches_source_indicators(self):
        arr = np.arange(1.0, 61.0)
        expected = arr[-20:].mean() - 2 * arr[-20:].std(ddof=0)
        self.assertAlmostEqual(m.lower(arr, 20)[-1], expected, places=12)
        self.assertNotAlmostEqual(m.lower(arr, 20)[-1], m.lower(arr, 20, 1)[-1], places=6)

    def test_both_day_bands_not_single_static_threshold(self):
        # The previous close must be tested against previous dynamic lower.
        # Last close against current dynamic lower.
        close = np.array([100.0] * 222 + [75.0, 93.0])
        flags = m.compute_flags(self.history(close), 223)
        lo20 = m.lower(close, 20)
        self.assertEqual(flags['bb20_reclaim_today'], int(close[-2] < lo20[-2] and close[-1] >= lo20[-1]))
        self.assertAlmostEqual(flags['bb20_lower_prev'], lo20[-2])
        self.assertAlmostEqual(flags['bb20_lower_signal'], lo20[-1])

    def test_single_below_then_reclaim_not_assumed_all_three(self):
        close = np.array([100.] * 222 + [75., 99.])
        v = m.compute_flags(self.history(close), 223)
        self.assertEqual(v['asof_status'], 'ASOF_OK')
        self.assertEqual(v['bb20_reclaim_today'], 1)
        self.assertEqual(v['bb_any_today'], max(v[f'bb{n}_reclaim_today'] for n in m.BANDS))
        self.assertEqual(v['bb_all3_today'], min(v[f'bb{n}_reclaim_today'] for n in m.BANDS))
        self.assertLessEqual(v['bb_all3_recent5'], min(v[f'bb{n}_reclaim_recent5'] for n in m.BANDS))

    def test_recent5_can_find_before_signal_not_today(self):
        close = [100.] * 220 + [75., 99., 100., 101.]
        v = m.compute_flags(self.history(close), 223)
        self.assertEqual(v['bb20_reclaim_today'], 0)
        self.assertEqual(v['bb20_reclaim_recent5'], 1)

    def test_224_classification_excludes_future(self):
        close = [100.] * 223 + [90.]
        v = m.compute_flags(self.history(close), 223)
        self.assertEqual(v['ma224_below'], 1)

    def test_outcome_in_asof_features_fails_closed(self):
        features = pd.DataFrame([{'date':'2024-01-02','code':'000001','gross_pct':100.0}])
        asof = pd.DataFrame([{'date':'2024-01-02','code':'000001'}])
        with self.assertRaisesRegex(RuntimeError, 'OUTCOME_IN_FEATURES'):
            m.compare(features, pd.DataFrame(), asof, '.')

    def test_outcome_low_sample_warning(self):
        g = pd.DataFrame([{'date':'2024-01-02','code':'000001','horizon':5,
                           'flag':1,'status_out':m.DONE,'net_20bps_pct':2.,
                           'mae_intraday_pct':-3.,'mfe_intraday_pct':4.}])
        stat=m.one_score(g,'flag',1,'UNIT')
        self.assertEqual(stat['complete'],1)
        self.assertEqual(stat['low_n_warning'],1)
        self.assertEqual(stat['win_net_positive_pct'],100.)

    def test_protocol_only_does_not_read_market_data(self):
        with tempfile.TemporaryDirectory() as tmp:
            args=type('Args',(),{'mode':'protocol','protocol':'R1D19_PREDECLARED_PROTOCOL.json',
                                'out':tmp,'d6_asof':'','d7':'','krx_root':''})()
            m.run(args)
            data=json.loads((Path(tmp)/'R1D19_PROTOCOL_LOCK.json').read_text())
            self.assertFalse(data['historical_backtest_executed'])
            self.assertFalse(data['new_oos'])


if __name__ == '__main__':
    unittest.main()
