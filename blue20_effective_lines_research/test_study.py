import unittest
import numpy as np
import pandas as pd
from study import select_known, trade, lines_and_hits, next_in, FIXED


class ResearchTests(unittest.TestCase):
    def test_future_or_same_day_label_cannot_select_line(self):
        history = {"BB": [("2024-12-30", True)] * 7 + [("2024-12-31", True)],
                   "ENV": [("2025-01-01", True)] * 8}
        self.assertEqual(select_known(history, "2024-12-31", FIXED), (FIXED, 0))
        self.assertEqual(select_known(history, "2025-01-01", FIXED), ("BB", 8))

    def test_dynamic_exit_uses_next_open(self):
        x = ([100,100,91,95], [101,104,95,97], [98,96,90,94], [100,97,94,96])
        self.assertEqual(trade(x,0,3,.10,dynamic_stop=[99]*4)[1:], ("LINE_EXIT",1,2))
        self.assertAlmostEqual(trade(x,0,3,.10,dynamic_stop=[99]*4)[0], -9.25)

    def test_same_bar_stop_before_target_and_no_trade_at_deadline(self):
        x = ([100,100], [101,112], [98,92], [100,110])
        self.assertEqual(trade(x,0,1,.10)[1], "SL")
        self.assertEqual(trade(x,1,1,.10)[1], "NO_ENTRY")

    def test_lines_keep_config_and_do_not_repaint(self):
        c = pd.Series(np.arange(100, dtype=float) + 100)
        d = pd.DataFrame({"close":c,"open":c-1})
        lines, _, starts = lines_and_hits(d)
        prefix, _, old_starts = lines_and_hits(d.iloc[:90])
        pd.testing.assert_frame_equal(lines.iloc[:90], prefix)
        np.testing.assert_array_equal(starts[:90], old_starts)
        self.assertEqual(len(lines.columns), 31)
        self.assertEqual(lines["ENV20:CENTER"].first_valid_index(), 19)
        self.assertEqual(lines["BB20:CENTER"].first_valid_index(), 45)

    def test_future_signal_outside_window_is_excluded(self):
        self.assertIsNone(next_in([4,10],5,9))
        self.assertEqual(next_in([4,10],5,10), 10)


if __name__ == "__main__":
    unittest.main()
