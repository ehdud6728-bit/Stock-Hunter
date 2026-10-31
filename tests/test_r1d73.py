import importlib.util, pathlib, unittest
import pandas as pd

HERE=pathlib.Path(__file__).resolve().parents[1]
SPEC=importlib.util.spec_from_file_location('r1d73_signal_quality',HERE/'research_formulas/r1d73_signal_quality.py')
MOD=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(MOD)


def sample(**change):
    v=dict(date='2024-02-26',code='123456',band='BB20_L',level=100.0,
           source_close=102.0,signal_close=102.0,signal_open=101.0,
           signal_low=99.0,signal_high=103.0,volume=1_200_000,
           amount_krw=121_000_000,near=1,low_touch=1,touch_reject=1,
           fresh_reclaim=0,signal_amount_ratio_prev20=1.0,
           wave_trough_low=93,pullback_min_low=99.0,wave_gain_pct=16,
           wave_trough_date='2024-01-05',wave_peak_date='2024-01-22',pullback_min_date='2024-02-20',
           entry1_open=102,entry1_day='2024-02-27',entry1_net_pct=-70.0,price_jump_or_mae_watch=1,
           CA_LEDGER_STATUS='UNCERTIFIED')
    # Real turnover must be between volume*low and volume*high, unless the test
    # intentionally simulates an incorrect-source value.
    v.update(change)
    return pd.DataFrame([v])

class SignalTruth(unittest.TestCase):
 def test_dayzero_clean_candidate(self):
  d=MOD.classify_signal_only(sample());self.assertEqual(d.research_quality_tier.iloc[0],'PROVISIONAL_ASOF_PATTERN_REVIEW')
 def test_future_outcome_never_changes_signal(self):
  a=MOD.classify_signal_only(sample(entry1_open=1,entry1_net_pct=-99,next_day_confirm=0))
  b=MOD.classify_signal_only(sample(entry1_open=100000,entry1_net_pct=900,next_day_confirm=1))
  self.assertEqual(a.research_quality_tier.iloc[0],b.research_quality_tier.iloc[0])
 def test_volume_zero_hard_fail(self):
  d=MOD.classify_signal_only(sample(volume=0));self.assertEqual(d.structural_quality_hard_fail.iloc[0],1)
 def test_one_share_hard_fail(self):
  d=MOD.classify_signal_only(sample(volume=1,amount_krw=101));self.assertEqual(d.structural_quality_hard_fail.iloc[0],1)
 def test_market_not_assumed_kosdaq(self):
  d=MOD.classify_signal_only(sample());self.assertEqual(d.market_asof_evidence.iloc[0],'UNKNOWN_NOT_PIT_CERTIFIED')
 def test_konex_case_excluded(self):
  d=MOD.classify_signal_only(sample(code='279060',volume=1,amount_krw=102));self.assertEqual(d.audit_f_known_non_target_market.iloc[0],1)
 def test_band_chase_watch(self):
  d=MOD.classify_signal_only(sample(level=96,near=1));self.assertEqual(d.audit_f_band_chase_gt5.iloc[0],1)
 def test_late_pullback_watch(self):
  d=MOD.classify_signal_only(sample(pullback_min_low=90));self.assertEqual(d.audit_f_pullback_chase_gt8.iloc[0],1)
 def test_no_contact_reclaim(self):
  d=MOD.classify_signal_only(sample(near=0,low_touch=0,fresh_reclaim=1));self.assertEqual(d.audit_f_reclaim_no_contact.iloc[0],1)
 def test_future_pivot_date_invalid(self):
  d=MOD.classify_signal_only(sample(pullback_min_date='2024-03-01'));self.assertEqual(d.audit_f_invalid_ohlc_or_asof.iloc[0],1)
 def test_price_amount_ohlc_mismatch(self):
  d=MOD.classify_signal_only(sample(amount_krw=30_000_000));self.assertEqual(d.audit_f_amount_ohlc_inconsistent.iloc[0],1)
 def test_below_lower_is_not_recovered(self):
  d=MOD.classify_signal_only(sample(level=106));self.assertEqual(d.audit_f_below_band_not_reclaimed.iloc[0],1)
 def test_no_trade_ready_even_when_provisional(self):
  d=MOD.classify_signal_only(sample());self.assertNotEqual(d.LIVE_TRADE_READY.iloc[0],'YES')
 def test_reality_freeze_2049(self):
  x=MOD.read_inputs(HERE/'research_inputs/r1d73/FROZEN_R1D72_1_STAGE_LIQ_2049.csv.gz',
     HERE/'research_inputs/r1d73/FROZEN_R1D6_ASOF_5247.csv.gz')
  self.assertEqual(len(x),2049)
  d=MOD.classify_signal_only(x)
  self.assertEqual(int((d.research_quality_tier=='PROVISIONAL_ASOF_PATTERN_REVIEW').sum()),446)
  self.assertEqual(int(d.audit_f_under100m.sum()),818)
  self.assertEqual(int(d.audit_f_pullback_chase_gt8.sum()),127)

if __name__=='__main__':unittest.main()
