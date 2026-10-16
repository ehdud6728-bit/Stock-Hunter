import json, tempfile, unittest
from pathlib import Path
import numpy as np
import pandas as pd
from closebet_research import r119_d20_first_touch_research as r

class D20Tests(unittest.TestCase):
    def mk(self,rows):
        # `rows` as 20 daily (open,high,low,close) tuples; fake official dates for bounded unit tests only.
        official=['2026-01-%02d'%d for d in range(1,23)]
        arr={k:np.array([100]+[v[i] for v in rows],float) for i,k in enumerate(['open','high','low','close'])}
        # Above arr makes every preceding D0 field 100; intentionally omit other days.
        return r.path_outcome(official[0], official, 0, 100., arr, {i:i for i in range(len(rows)+1)})

    def test_high_touch_d20_not_d5(self):
        bars=[(100,104,98,100)]*19+[(100,105.01,99,103)]
        x=self.mk(bars)
        self.assertTrue(x['d20_complete']);self.assertEqual(x['t5_by_d5'],'NOT_HIT_WITH_COMPLETE_HORIZON')
        self.assertEqual(x['t5_by_d20'],'HIT_HIGH_NOT_FILL');self.assertEqual(x['t5_first_day'],20)

    def test_d5_close_loss_can_still_touch_plus5(self):
        bars=[(100,106,97,98)]+[(100,104,98,100)]*19
        x=self.mk(bars)
        self.assertEqual(x['t5_first_day'],1)
        self.assertEqual(x['t5_vs_s5_d20'],'TP_FIRST_HIGH_TOUCH')

    def test_same_day_both_conservative_and_optimistic(self):
        bars=[(100,105.1,94.9,100)]+[(100,101,99,100)]*19
        x=self.mk(bars)
        self.assertEqual(x['t5_vs_s5_d20'],'AMBIGUOUS_BOTH_SAME_DAY_CONSERVATIVE_STOP')
        self.assertEqual(x['t5_vs_s5_d20_best'],'AMBIGUOUS_BOTH_SAME_DAY_OPTIMISTIC_TP')
        self.assertTrue(x['t5_intraday_same_day_s5_ambiguous'])

    def test_open_gap_order_has_precedence(self):
        bars=[(100,102,98,100),(106,108,94,102)]+[(100,101,99,100)]*18
        self.assertEqual(self.mk(bars)['t5_vs_s5_d20'],'TP_OPEN_GAP_FIRST')
        bars[1]=(94,107,90,103)
        self.assertEqual(self.mk(bars)['t5_vs_s5_d20'],'STOP_OPEN_GAP_FIRST')

    def test_stop_earlier_than_later_target(self):
        bars=[(100,103,94,95),(98,106,97,105)]+[(100,101,99,100)]*18
        x=self.mk(bars);self.assertEqual(x['t5_first_day'],2)
        self.assertEqual(x['s5_first_day'],1)
        self.assertEqual(x['t5_vs_s5_d20'],'STOP_FIRST_LOW_TOUCH')

    def test_incomplete_early_hit_known_later_absent_censored(self):
        bars=[(100,106,99,102)]*3
        x=self.mk(bars)
        self.assertEqual(x['t5_by_d20'],'HIT_HIGH_NOT_FILL')
        self.assertEqual(x['t10_by_d20'],'UNKNOWN_CENSORED')
        self.assertFalse(x['d20_complete'])

    def test_incomplete_no_hit_unknown_not_failure(self):
        bars=[(100,104,99,100)]*19
        x=self.mk(bars)
        self.assertEqual(x['t5_by_d20'],'UNKNOWN_CENSORED')
        self.assertEqual(x['t5_by_d15'],'NOT_HIT_WITH_COMPLETE_HORIZON')

    def test_d1_entry_missing(self):
        official=['2026-01-01','2026-01-02']
        arr={k:np.array([100.],float) for k in ('open','high','low','close')}
        x=r.path_outcome(official[0],official,0,100.,arr,{0:0})
        self.assertEqual(x['input_status'],'D1_ENTRY_MISSING');self.assertEqual(x['t5_by_d20'],'UNKNOWN_CENSORED')

    def test_postd0_suspect_jump_is_flagged(self):
        bars=[(100,145,99,145)]+[(100,101,99,100)]*19
        x=self.mk(bars);self.assertTrue(x['corp_action_future_suspect'])

    def test_full_no_touch_stop_is_neither(self):
        bars=[(100,104,96,100)]*20
        x=self.mk(bars)
        self.assertEqual(x['t5_vs_s5_d20'],'NO_FIRST_TOUCH_WITH_COMPLETE_D20')

    def test_sha_manifest_fail_closed(self):
        from closebet_research import r118_partial_market_wave_geometry as b
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);(root/r.SHAFILE).write_bytes(b'bad')
            (root/'r118_integrity_manifest.json').write_text(json.dumps({
             'version':'R118_PARTIAL_MARKET_GAP_224_ORIGIN_BROAD_GEOMETRY_V1',
             'status':'PASS_FULL_MARKET_GAP_AND_BROAD_WAVE_COVERED_COHORT_ONLY_NOT_FULL_PIT_RECALL',
             'file_sha256':{r.SHAFILE:'not_actual'}}))
            with self.assertRaisesRegex(RuntimeError,'R118_COHORT_SHA_MISMATCH'):
                r.read_r118(root)

    def test_d20_oversubscription_is_unknown(self):
        bars=[(100,104,99,100)]*20
        x=self.mk(bars)
        self.assertEqual(x['t15_by_d20'],'NOT_HIT_WITH_COMPLETE_HORIZON')
        self.assertEqual(x['d20_close_net05pct'],-.5)

if __name__=='__main__':unittest.main()
