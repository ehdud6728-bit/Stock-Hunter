"""Deterministic offline timing and source safety tests for R20 moving-average exits."""
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

import r20_ma_exit as r20


def bars(n=100):
    dates = pd.date_range("2021-01-01", periods=n, freq="B").strftime("%Y-%m-%d")
    return pd.DataFrame({"code": ["TEST"] * n, "date": dates,
                         "open": [10.] * n, "high": [11.] * n,
                         "low": [9.] * n, "close": [10.] * n})


class MovingAverageExitTests(unittest.TestCase):
    def test_three_sma_periods_are_exact(self):
        self.assertEqual(r20.MA_METHODS, {5: "MA5_CLOSE_BELOW_NEXT_OPEN",
                                          10: "MA10_CLOSE_BELOW_NEXT_OPEN",
                                          20: "MA20_CLOSE_BELOW_NEXT_OPEN"})

    def test_breach_on_dplus1_close_exits_dplus2_open(self):
        for length in (5, 10, 20):
            with self.subTest(length=length):
                f = bars()
                f.loc[25, "close"] = 15.
                f.loc[26, "open"] = 12.
                f.loc[26, "close"] = 9.
                f.loc[27, "open"] = 8.5
                x = r20.ma_close_break_next_open(f, 25, length)
                self.assertEqual(x["exit_day"], 2)
                self.assertEqual(x["exit_reason"], "CLOSE_BELOW_ASOF_MA")
                self.assertEqual(x["net_pct"], round(100 * (8.5 / 12 - 1) - .2, 6))

    def test_intraday_low_without_close_breach_is_not_exit(self):
        f = bars()
        f.loc[26, "low"] = 1.0
        f.loc[26, "close"] = 10.0
        f.loc[27, "open"] = 7.0
        for length in (5, 10, 20):
            x = r20.ma_close_break_next_open(f, 25, length)
            self.assertEqual(x["exit_day"], r20.HORIZON)
            self.assertEqual(x["net_pct"], -.2)

    def test_future_highs_lows_and_closes_cannot_move_first_exit(self):
        f = bars()
        f.loc[25, "close"] = 15.
        f.loc[26, "close"] = 9.
        f.loc[27, "open"] = 8.5
        first = r20.ma_close_break_next_open(f, 25, 20)
        f.loc[28:85, "close"] = 10000.
        f.loc[26:85, "high"] = 99999.
        f.loc[26:85, "low"] = .01
        self.assertEqual(r20.ma_close_break_next_open(f, 25, 20), first)

    def test_price_equal_to_sma_does_not_exit(self):
        f = bars()
        for n in (5, 10, 20):
            self.assertEqual(r20.ma_close_break_next_open(f, 25, n)["exit_day"], 60)

    def test_reject_incomplete_horizon_or_invalid_period(self):
        f = bars(80)
        with self.assertRaises(ValueError):
            r20.ma_close_break_next_open(f, 25, 5)
        with self.assertRaises(ValueError):
            r20.ma_close_break_next_open(bars(), 25, 21)

    def test_fail_closed_on_unpinned_price_source(self):
        with TemporaryDirectory() as temp:
            base = Path(temp)
            (base / "R16_SOURCE_MANIFEST.json").write_text('{"kis_run_id": "wrong"}')
            with self.assertRaises(ValueError):
                r20.read_sources(base, base)

    def test_no_fixed_percentage_stops_or_secret_dependency(self):
        source = Path(r20.__file__).read_text(encoding="utf-8")
        self.assertNotIn("-5%", source)
        self.assertNotIn("+10%", source)
        self.assertNotIn("KIS_APP_SECRET", source)


if __name__ == "__main__":
    unittest.main()
