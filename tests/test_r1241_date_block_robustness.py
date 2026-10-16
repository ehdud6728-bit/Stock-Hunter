import json
import tempfile
import unittest
from pathlib import Path
import numpy as np
import pandas as pd
from closebet_research import r1241_date_block_robustness as m

ROOT=Path(__file__).resolve().parents[1]

class R1241Test(unittest.TestCase):
    def test_predeclared_gates_unchanged(self):
        self.assertEqual(m.GATES[1:4],('H1_PREV_LOW_HELD','H2_HELD_MA20_UP_ABOVE_20_60','H3_HELD_ACCUM20_DRY5'))
    def test_h2_only_compared_to_h1(self):
        d=pd.DataFrame({'H1_PREV_LOW_HELD':[True,True,False],
                        'H2_HELD_MA20_UP_ABOVE_20_60':[True,False,False]})
        g,c,_=m.selectors('H2_HELD_MA20_UP_ABOVE_20_60',d)
        self.assertEqual(g.tolist(),[True,False,False]);self.assertEqual(c.tolist(),[False,True,False])
    def test_h3_only_compared_to_h1(self):
        d=pd.DataFrame({'H1_PREV_LOW_HELD':[True,True,False],
                        'H3_HELD_ACCUM20_DRY5':[True,False,False]})
        g,c,_=m.selectors('H3_HELD_ACCUM20_DRY5',d)
        self.assertEqual(c.tolist(),[False,True,False])
    def test_h1_compares_all_other(self):
        d=pd.DataFrame({'H1_PREV_LOW_HELD':[True,False]});g,c,_=m.selectors('H1_PREV_LOW_HELD',d)
        self.assertEqual(c.tolist(),[False,True])
    def test_ci_single_day_not_statistic(self):
        x=pd.DataFrame({'delta_win_pp':[55.]})
        self.assertTrue(np.isnan(m.date_bootstrap_ci(x)[0]))
    def test_ci_resamples_dates_deterministically(self):
        x=pd.DataFrame({'delta_win_pp':[10.,-10.,20.,-20.,30.]})
        self.assertEqual(m.date_bootstrap_ci(x),m.date_bootstrap_ci(x))
    def test_contrast_counts_and_no_borrowing(self):
        rows=[]
        for day in range(6):
            for i in range(2):
                rows.append({'scope':'S','code':f'{100000+day*2+i:06}',
                             'signal_date':f'2026-08-{day+1:02}',
                             'H1_PREV_LOW_HELD':i==0,'H2_HELD_MA20_UP_ABOVE_20_60':False,
                             'H3_HELD_ACCUM20_DRY5':False,'NEG_OVERHEAD_HEAVY':False,
                             'bucket':'TP_FIRST' if i==0 else 'STOP_NO_TP',
                             'r122_baseline_budget_net_pct':3. if i==0 else -5.})
        c,dates,comp=m.robust_audit(pd.DataFrame(rows))
        h=c[c.gate.eq('H1_PREV_LOW_HELD')].iloc[0]
        self.assertEqual((h.n_gate,h.n_control,h.matched_dates),(6,6,6))
        self.assertAlmostEqual(h.matched_date_win_delta_mean_pp,100.)
    def test_research_authority(self):
        self.assertIn('NOT_OOS',m.REV)
    def test_source_pin_hash_length(self):
        self.assertEqual(len(m.R123_SHA),64);self.assertEqual(len(m.R124_SHA),64)
    def test_missing_manifest_fails(self):
        with tempfile.TemporaryDirectory() as p:
            with self.assertRaisesRegex(ValueError,'PINNED_MANIFEST_CHANGED'):m.source_verify(p,'R123')
    def test_optional_root_yml_copy_matches_if_present(self):
        # ZIP root YML is a human-readable convenience copy, not a GitHub requirement.
        # A checkout normally contains only .github/workflows/, so absence is valid.
        actual=ROOT/'.github/workflows/closing_bet_r1241_date_block_robustness.yml'
        visible=ROOT/'closing_bet_r1241_date_block_robustness.yml'
        self.assertTrue(actual.is_file(), 'REQUIRED_WORKFLOW_MISSING')
        if visible.is_file():
            self.assertEqual(actual.read_bytes(),visible.read_bytes(),
                             'OPTIONAL_ROOT_YML_COPY_DIFFERS_FROM_WORKFLOW')
    def test_no_workflow_automation_or_live(self):
        f=(ROOT/'.github/workflows/closing_bet_r1241_date_block_robustness.yml').read_text()
        self.assertIn('workflow_dispatch:',f)
        self.assertNotIn('  schedule:',f)
        self.assertNotIn('TELEGRAM',f)
        self.assertNotIn('secrets.',f)

if __name__=='__main__':unittest.main()
