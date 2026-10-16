"""R121 research fail-closed tests, no outcome driven selection."""
import tempfile,unittest,json,shutil,os
from pathlib import Path
import numpy as np
import pandas as pd
from closebet_research.r121_frozen_type_policy_replay import (
    gate_match,time_window,ticker_cluster_ci,source_load,run_research,SCOPES,REVISION,
    sha256,FROZEN_POLICY_SIGNATURE
)
ROOT=Path(__file__).resolve().parents[1]
CONTRACT=ROOT/'closebet_research/r121_frozen_hypothesis_contract.json'

class ResearchTests(unittest.TestCase):
    def get_source(self):
        src=os.environ.get('R121_SOURCE_ROOT','')
        if not src or not Path(src).is_dir(): self.skipTest('R121_SOURCE_ROOT not yet downloaded')
        return Path(src)
    @classmethod
    def setUpClass(cls):
        cls.contract=json.loads(CONTRACT.read_text(encoding='utf-8'))
    def test_contract_version_and_seven_frozen_policies(self):
        self.assertEqual(self.contract['revision'],REVISION)
        self.assertEqual(tuple(tuple(p[k] for k in ('id','gate','entry_policy','stop_policy')) for p in self.contract['policies']),FROZEN_POLICY_SIGNATURE)
    def test_train_first_day_and_20_day_information_embargo(self):
        dates=pd.read_csv(self.get_source()/'r120_episodes_asof_and_evidence.csv',usecols=['signal_date']).signal_date
        m=time_window(dates)
        self.assertEqual(m['2026-08-03'],'TRAIN_MATURE')
        self.assertEqual(m['2026-09-01'],'EMBARGO')
        self.assertEqual(m['2026-09-02'],'VALIDATION_MATURE')
        self.assertEqual(m['2026-09-08'],'VALIDATION_MATURE')
        self.assertEqual(m['2026-09-09'],'RIGHT_CENSORED')
        self.assertEqual(sum(v=='VALIDATION_MATURE' for v in m.values()),5)
    def test_historical_universe_drift_fail_closed(self):
        with self.assertRaisesRegex(ValueError,'HISTORICAL_45_DATE_LOCK_FAILED'):
            time_window(['2026-08-03','2026-09-02'])
    def test_outcomes_do_not_change_candidate_gate(self):
        a=pd.DataFrame({'scope':['BROAD_NOT_RAW']*4,'type':['SHALLOW_PULLBACK','MID_PULLBACK','DEEP_PULLBACK','SHALLOW_PULLBACK'],
         'volume_context':['ACCUM20_RECENT_DRY5','ACCUM20_RECENT_DRY5','ACCUM20_RECENT_DRY5','BOTH_RISING'],
         'r112_stage_asof':['PULLBACK_QUALITY']*4,'future_outcome_fake':[-99,10,999,0]})
        first=gate_match(a,'PB_ACCUM_DRY')
        a['future_outcome_fake']=[999,999,-99,-99]
        self.assertEqual(first.tolist(),gate_match(a,'PB_ACCUM_DRY').tolist())
        self.assertEqual(first.tolist(),[True,True,False,False])
    def test_selected_only_r112_stages(self):
        a=pd.DataFrame({'scope':['V4976_SELECTED','V4976_SELECTED','BROAD_NOT_RAW'],
                        'r112_stage_asof':['PULLBACK_QUALITY','NO_CONFIRMED_WAVE','REBOUND_CONFIRMED'],
                        'type':['MID_PULLBACK']*3})
        self.assertEqual(gate_match(a,'R112_CONFIRMED_SELECTED_ONLY').tolist(),[True,False,False])
    def test_risk_avoid_guard(self):
        a=pd.DataFrame({'type':['AGED_PULLBACK_AFTER_PEAK','DEEP_PULLBACK','SHALLOW_PULLBACK']})
        self.assertEqual(gate_match(a,'NOT_DEEP_AGED').tolist(),[False,False,True])
    def test_bad_gate_must_raise(self):
        with self.assertRaisesRegex(ValueError,'UNKNOWN_GATE'):
            gate_match(pd.DataFrame({'type':['x']}),'UNKNOWN')
    def test_ticker_cluster_no_small_n_claim(self):
        low,high=ticker_cluster_ci(pd.Series([1,2,3]),pd.Series(['000001','000002','000003']))
        self.assertTrue(np.isnan(low) and np.isnan(high))
    def test_ticker_cluster_zero_delta_exact(self):
        low,high=ticker_cluster_ci(pd.Series([0.]*50),pd.Series(['%06d'%i for i in range(50)]),draws=200)
        self.assertEqual((low,high),(0.,0.))
    def test_repo_source_pin_and_leading_zero(self):
        # This test only uses original downloaded immutable parent, never live data.
        a,s,m=source_load(self.get_source())
        self.assertEqual((len(a),len(s)),(4567,63200))
        self.assertTrue(a.code.str.len().eq(6).all())
        self.assertIn('000050',set(a.code))
        self.assertFalse(m['trade_authority'])
    def test_source_tampering_is_detected(self):
        source=self.get_source()
        with tempfile.TemporaryDirectory() as td:
            d=Path(td)
            shutil.copy2(source/'r120_integrity_manifest.json',d/'r120_integrity_manifest.json')
            # pre-existing hashed output copied but deliberately one byte corrupted
            for name in json.loads((d/'r120_integrity_manifest.json').read_text())['file_sha256']:
                (d/name).write_bytes(b'CORRUPT')
            with self.assertRaisesRegex(ValueError,'R120_MEMBER_SHA_FAIL'):
                source_load(d)
    def test_outputs_are_hard_readonly_research(self):
        with tempfile.TemporaryDirectory() as td:
            manifest=run_research(self.get_source(),CONTRACT,td)
            self.assertEqual(manifest['source_scenarios'],63200)
            self.assertEqual(manifest['shared_tickers'],799)
            self.assertFalse(manifest['trade_authority'])
            self.assertTrue(manifest['past_studies_reused_same_sources'])
            for name,digest in manifest['output_files_sha256'].items():
                self.assertEqual(sha256(Path(td)/name),digest)
    def test_changed_contract_is_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            c=json.loads(CONTRACT.read_text())
            c['policies'][1]['gate']='ALL' # would create an attractive but disallowed implicit redefinition
            cp=Path(td)/'bad.json';cp.write_text(json.dumps(c))
            with self.assertRaisesRegex(ValueError,'FROZEN_POLICIES_MODIFIED'):
                run_research(self.get_source(),cp,Path(td)/'out')

if __name__=='__main__': unittest.main()
