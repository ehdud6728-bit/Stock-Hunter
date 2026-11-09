import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd

from features import GROUPS, candle_features, investor_features, load_flows
from study import forward, line_states


def bars(n=100):
    dates = pd.date_range("2023-01-02", periods=n, freq="B").strftime("%Y-%m-%d")
    return pd.DataFrame(dict(code="000001", open=100., high=102., low=98., close=100.,
                             volume=1000., amount=100000.), index=dates)


class ResearchTest(unittest.TestCase):
    def test_price_features_do_not_change_when_future_changes(self):
        price = bars()
        price.iloc[75:, price.columns.get_loc("high")] = 140
        pd.testing.assert_frame_equal(candle_features(price).iloc[:75],
                                      candle_features(price.iloc[:75]))

    def test_candles_and_line_boundaries(self):
        price = bars()
        f = candle_features(price)
        self.assertEqual(f.range_atr.dropna().iloc[-1], 1)
        self.assertEqual(f.body_atr.dropna().iloc[-1], 0)
        self.assertFalse(f.nr7.iloc[-1])
        states, hits = line_states(price, f)
        self.assertTrue(states["ENV20:CENTER_near"].iloc[-1])
        self.assertFalse(states["ENV20:LOWER_near"].iloc[-1])
        self.assertFalse(states["ENV20:UPPER_near"].iloc[-1])
        self.assertFalse(hits)

    def test_publication_lag_and_missing_not_zero(self):
        calendar = bars(35).index
        available = pd.to_datetime(calendar).tz_localize("Asia/Seoul") + pd.Timedelta(hours=20)
        source = pd.DataFrame({g: 100. for g in GROUPS}, index=calendar)
        source["turnover_krw"] = 1000.
        source["published_at"] = available
        source.loc[calendar[5], "published_at"] += pd.Timedelta(days=5)
        f = investor_features(calendar, source)
        self.assertTrue(np.isnan(f.institution_net5.iloc[4]))
        self.assertEqual(f.institution_net5.iloc[5], 500)
        self.assertTrue(np.isnan(f.institution_net5.iloc[6]))
        self.assertEqual(f.institution_net5.iloc[11], 500)
        self.assertEqual(f.institution_turnover_pct5.iloc[11], 10)
        self.assertEqual(f.institution_streak.iloc[11], 5)
        self.assertEqual(investor_features(calendar, None).flow_status.unique().tolist(), ["UNKNOWN"])

    def test_candidate_outcomes_distinguish_wave_trade_and_censoring(self):
        price = bars()
        price.iloc[2, price.columns.get_loc("high")] = 115.
        price.iloc[2, price.columns.get_loc("low")] = 90.
        wave = pd.Series(False, index=price.index)
        wave.iloc[[0, 3]] = True
        episodes = pd.DataFrame({"final_status": ["P2_BASE_LOW_FAILED"], "known_date": [price.index[10]]},
                                index=[price.index[3]])
        f = forward(price, wave, episodes)
        self.assertEqual(f.days_to_wave5.iloc[0], 3)
        self.assertEqual(f.exit5.iloc[0], "SL")
        self.assertEqual(f.net5.iloc[0], -5.25)
        self.assertEqual(f.wait_p1_net20_within5.iloc[0], -.25)
        self.assertEqual(f.first_p1_final_status.iloc[0], "P2_BASE_LOW_FAILED")
        self.assertTrue(pd.isna(f.wave60.iloc[-1]))
        price.iloc[4] = np.nan
        incomplete = forward(price, wave, episodes)
        self.assertTrue(pd.isna(incomplete.wave5.iloc[0]))

    def test_csv_preserves_real_zero_and_rejects_unbalanced_totals(self):
        row = dict(code="000001", date="2023-01-02", published_at="2023-01-02T20:00:00+09:00",
                   turnover_krw=1000, institution=120, foreign=0, individual=-100, other_corp=-20)
        with TemporaryDirectory() as folder:
            path = Path(folder) / "flows.csv"
            pd.DataFrame([row]).to_csv(path, index=False)
            parsed = load_flows(path)
            self.assertEqual(parsed["000001"].foreign.iloc[0], 0)
            self.assertTrue(pd.isna(parsed["000001"].get("pension")))
            pd.DataFrame([row | {"other_corp": 200}]).to_csv(path, index=False)
            with self.assertRaisesRegex(ValueError, "do not reconcile"):
                load_flows(path)


if __name__ == "__main__":
    unittest.main()
