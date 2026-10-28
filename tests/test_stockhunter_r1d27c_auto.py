import json
import pathlib
import tempfile
import unittest
from unittest import mock
from research_formulas import stockhunter_r1d27c_auto as a
from research_formulas import stockhunter_r1d27c_flow60 as r

class AutoCheckpointTests(unittest.TestCase):
    def test_rejects_too_large_batch_or_extra_stage(self):
        with self.assertRaises(r.AuditError):
            a.run_batch(1, '/missing', '/missing', '/missing', 31)
        with self.assertRaises(r.AuditError):
            a.run_batch(9, '/missing', '/missing', '/missing', 30)

    def test_stage_chain_and_file_upload_state(self):
        with tempfile.TemporaryDirectory() as td:
            root=pathlib.Path(td)
            seed=root/'seed';out=root/'reports/r1d27c/result'
            (seed/'reports/r1d27c/result/freeze').mkdir(parents=True)
            (seed/'reports/r1d27c/result/flow').mkdir(parents=True)
            (seed/'reports/r1d27c/result/freeze'/r.FROZEN).write_text('pinned')
            (seed/'reports/r1d27c/result/flow'/r.FLOW).write_text('raw')
            calls=[];n=[87]
            def fake_audit(path,calendar):
                return None,None,None,None,dict(n_source_complete=n[0])
            def fake_collect(checkpoint,krx_root,out_dir,p,**kwargs):
                calls.append(kwargs)
                assert kwargs['skip_known_calendar_mismatch']
                out_dir=pathlib.Path(out_dir);out_dir.mkdir(parents=True,exist_ok=True)
                (out_dir/r.FLOW).write_text('batch-'+str(len(calls)))
                freeze=out_dir.parent/'freeze';freeze.mkdir(exist_ok=True)
                (freeze/r.FROZEN).write_text('pinned')
                before=n[0];n[0]+=30
                return dict(n_source_complete=n[0],n_unattempted=300-1-n[0],n_source_incomplete=1,
                    n_cached_reused=before,batch_attempted=30,batch_new_success=30,
                    consecutive_failure_abort=False,live_changed=False,orders_placed=0,research_only=True,
                    checkpoint_source_digest='digest',files_sha256={r.FLOW:'sha'})
            with mock.patch.object(a.d6,'index_asof',return_value=(["2019-01-02"]*1907,None)),\
                 mock.patch.object(a.r,'audit_checkpoint',side_effect=fake_audit),\
                 mock.patch.object(a.r,'collect_resume',side_effect=fake_collect):
                one=a.run_batch(1,seed,None,out,30)
                self.assertEqual(one['n_source_complete'],117)
                two=a.run_batch(2,seed,None,out,30)
                self.assertEqual(two['n_source_complete'],147)
                self.assertEqual(two['last_batch'],2)
                self.assertEqual(len(calls),2)
                self.assertTrue((out.parent/'auto_input_checkpoint/reports/r1d27c/result/flow'/r.FLOW).is_file())
                with self.assertRaises(r.AuditError):
                    a.run_batch(4,seed,None,out,30)

if __name__=='__main__':unittest.main()
