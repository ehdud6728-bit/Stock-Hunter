"""Offline synthetic tests for R1.10 fail-closed semantics. No GitHub/artifacts required."""
import unittest
import tempfile
from pathlib import Path
import numpy as np
import pandas as pd
from closebet_research import r110_strength_pullback_entry_audit as m


def small_features():
    return pd.DataFrame({
        'prewave_impulse_pct':[30.,50.,np.nan],
        'volume_dryup_10_30':[.69,1.1,.5],
        'prewave_pullback_vol_ratio':[.4,.8,np.nan],
        'prewave_retrace_fraction':[.2,.81,np.nan],
        'prewave_trough_below224':[True,False,False],
        'ma224_dist_signal_pct':[-1,3,np.nan],
        'wave_setup_stage':[4,0,0],
        'ma20_signal_dist_pct':[5.0,4.,np.nan],
        'D5_close_pct':[-100.,100.,333.],
        'outcome_class':['GIVEBACK','NO_5PCT_RISE','STRONG_PERSISTENT'],
    })


def simple_pairs():
    rows=[]
    for groupidx,kind in enumerate(('STRONG_PERSISTENT__VS__GIVEBACK','STRONG_PERSISTENT__VS__OTHER_PROFIT','OTHER_PROFIT__VS__NO_5PCT_RISE')):
        for i in range(12):
            a={'pair_id':kind+str(i),'pair_type':kind,'signal_session_gap':1,
               'left_signal_date':'2026-08-03','right_signal_date':'2026-08-04',
               'left_code':('%06d'%(1000+groupidx*100+i)),'right_code':('%06d'%(2000+groupidx*100+i))}
            for col in ('prewave_impulse_pct','prewave_pullback_vol_ratio','volume_dryup_10_30',
                        'prewave_prevolume_accum_ratio','prewave_pullback_pct','ma20_signal_dist_pct',
                        'ma224_dist_signal_pct','candle_close_location'):
                a['left_'+col]=2.;a['right_'+col]=1.
            rows.append(a)
    # Make exactly four reused events across different contrast groups.
    for i in range(4):
        rows[12+i]['left_code']=rows[i]['left_code']
        rows[12+i]['left_signal_date']=rows[i]['left_signal_date']
    return pd.DataFrame(rows)


class R110IntegrityTests(unittest.TestCase):
    def test_ex_ante_flags_never_read_forward_results(self):
        a=small_features();before=m.make_flags(a)
        a.loc[:,'D5_close_pct']=[1000.,-1000.,0.]
        a.loc[:,'outcome_class']=['A','B','C']
        pd.testing.assert_frame_equal(before,m.make_flags(a))

    def test_numeric_boundary_is_explicit(self):
        f=m.make_flags(small_features())
        self.assertEqual(f.impulse_ge30.iloc[0],1)
        self.assertEqual(f.impulse_ge50.iloc[0],0)
        self.assertEqual(f.impulse_ge50.iloc[1],1)
        self.assertEqual(f.dryup10_lt070.iloc[0],1)
        self.assertEqual(f.ma20_extended_ge5.iloc[0],1)
        self.assertEqual(f.retrace_20_80.iloc[0],1)

    def test_missing_presignal_is_unknown_not_false(self):
        f=m.make_flags(small_features())
        self.assertTrue(pd.isna(f.pullbackvol_lt070.iloc[2]))
        self.assertTrue(pd.isna(f.impulse_ge30.iloc[2]))
        self.assertTrue(pd.isna(f.signal_below224.iloc[2]))
        self.assertTrue(pd.isna(f.retrace_20_80_and_pullbackvol_lt070.iloc[2]))

    def test_known_false_not_unknown(self):
        f=m.make_flags(small_features())
        self.assertEqual(f.dryup10_lt070.iloc[1],0)
        self.assertEqual(f.origin_below224.iloc[1],0)

    def test_pinned_source_missing_fails_closed(self):
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaisesRegex(RuntimeError,'MISSING_PINNED_SOURCE'):
                m.validate_source(Path(folder),Path(folder))

    def test_hash_mismatch_detected(self):
        with tempfile.TemporaryDirectory() as folder:
            p=Path(folder)/'r18_all_unique_signal_features.csv'
            p.write_text('corrupted',encoding='utf-8')
            with self.assertRaisesRegex(RuntimeError,'SOURCE_HASH_MISMATCH'):
                m.validate_source(Path(folder),Path(folder))

    def test_cross_contrast_duplicate_is_disclosed_not_falsely_independent(self):
        x=simple_pairs()
        self.assertEqual(len(m.paired_report(x)),24)
        a=list(zip(x.left_signal_date,x.left_code))+list(zip(x.right_signal_date,x.right_code))
        self.assertEqual(len(set(a)),68)

    def test_duplicate_within_group_fails(self):
        x=simple_pairs();x.loc[1,'left_code']=x.loc[0,'left_code']
        with self.assertRaisesRegex(RuntimeError,'MATCH_REUSES_EVENT_WITHIN_GROUP'):
            m.paired_report(x)

    def test_different_stock_required_for_pair(self):
        x=simple_pairs();x.loc[0,'right_code']=x.loc[0,'left_code']
        with self.assertRaisesRegex(RuntimeError,'SAME_STOCK_PAIR'):
            m.paired_report(x)

    def test_global_threshold_set_is_frozen(self):
        f=m.make_flags(small_features())
        self.assertEqual(tuple(f.columns),m.FLAG_NAMES)
        self.assertNotIn('D20_close_pct',m.PRESIGNAL_NUMERIC)
        self.assertNotIn('D5_close_pct',m.PRESIGNAL_NUMERIC)

if __name__ == '__main__':
    unittest.main()
