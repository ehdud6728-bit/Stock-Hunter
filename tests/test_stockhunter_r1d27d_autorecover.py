"""Offline tests for R1D27D sealed source auto-recovery, no GitHub/network calls."""
import importlib.util,json,pathlib,tempfile,unittest
ROOT=pathlib.Path(__file__).resolve().parents[1]
SRC=ROOT/'research_formulas/stockhunter_r1d27d_autochain.py'
spec=importlib.util.spec_from_file_location('stockhunter_r1d27d_autochain',SRC)
ac=importlib.util.module_from_spec(spec);spec.loader.exec_module(ac)

class FakeAPI:
    repo='ehdud6728-bit/Stock-Hunter'
    def __init__(self): self.sent=[]
    def get(self,url):
        if '/actions/workflows/' in url:
            return {'workflow_runs':[
              {'id':111,'repository':{'full_name':self.repo},'path':ac.WF_PATH,'head_branch':'main','conclusion':'success'},
              {'id':222,'repository':{'full_name':self.repo},'path':ac.WF_PATH,'head_branch':'main','conclusion':'failure'},
              {'id':223,'repository':{'full_name':self.repo},'path':ac.WF_PATH,'head_branch':'main','conclusion':'in_progress'}
            ]}
        if '/actions/runs/' in url and url.endswith('/artifacts?per_page=100'):
            run=int(url.split('/actions/runs/')[1].split('/')[0]);
            return {'artifacts':[{'name':ac.ART_PREFIX+('03' if run==222 else '08'),
                 'expired':False,'digest':'sha256:'+'0'*64,
                 'size_in_bytes':3800000 if run==222 else 3100000}]}
        return {'repository':{'full_name':self.repo},'path':ac.WF_PATH,'status':'completed','head_branch':'main'}
    def dispatch(self, branch, inputs):
        self.sent.append((branch,inputs))

def stage(path,abort=False,complete=561,quarantined=8,unattempted=931):
    path.write_text(json.dumps({'n_frozen':1500,'freeze_sha256':ac.FREEZE,
       'n_complete':complete,'n_quarantined':quarantined,'n_unattempted':unattempted,
       'research_only':True,'strategy_ready':False,'orders_placed':0,
       'prospective_OOS':False,'provider_abort':abort,'batch_attempted':29}))

class TestAutoRecover(unittest.TestCase):
    def test_auto_picks_failed_completed_sealed_checkpoint(self):
        self.assertEqual(ac.select_seed(FakeAPI(),'main','auto','8'),('222',3))
    def test_explicit_fail_run_can_be_used(self):
        self.assertEqual(ac.select_seed(FakeAPI(),'main','222','3'),('222',3))
    def test_regular_chain_resets_abort_streak(self):
        with tempfile.TemporaryDirectory() as t:
            p=pathlib.Path(t)/'m.json';stage(p)
            api=FakeAPI();r=ac.dispatch_if_needed(api,'222','main','0','30',p,abort_streak_text='1',pause_seconds=0,sleeper=lambda n:None)
            self.assertEqual(r['next_abort_streak'],0)
            self.assertEqual(api.sent[0][1]['r1d27d_abort_streak'],'0')
            self.assertEqual(api.sent[0][1]['r1d27d_seed_part'],'3')
            self.assertEqual(len(api.sent[0][1]),10)
    def test_abort_pause_then_dispatch_once(self):
        with tempfile.TemporaryDirectory() as t:
            p=pathlib.Path(t)/'m.json';stage(p,abort=True)
            api=FakeAPI();slept=[]
            r=ac.dispatch_if_needed(api,'222','main','0','30',p,abort_streak_text='0',pause_seconds=180,sleeper=slept.append)
            self.assertEqual(slept,[180]);self.assertTrue(r['provider_abort']);self.assertEqual(r['next_abort_streak'],1)
            self.assertEqual(api.sent[0][1]['r1d27d_abort_streak'],'1')
    def test_abort_streak_three_halts(self):
        with tempfile.TemporaryDirectory() as t:
            p=pathlib.Path(t)/'m.json';stage(p,abort=True)
            api=FakeAPI()
            with self.assertRaisesRegex(ValueError,'THREE_CONSECUTIVE_PROVIDER_ABORTS'):
                ac.dispatch_if_needed(api,'222','main','2','30',p,abort_streak_text='2',pause_seconds=0,sleeper=lambda n:None)
            self.assertEqual(api.sent,[])
    def test_complete_does_not_dispatch(self):
        with tempfile.TemporaryDirectory() as t:
            p=pathlib.Path(t)/'m.json';stage(p,abort=False,complete=1492,quarantined=8,unattempted=0)
            api=FakeAPI()
            r=ac.dispatch_if_needed(api,'222','main','0','30',p,abort_streak_text='0',pause_seconds=0,sleeper=lambda n:None)
            self.assertEqual(r['status'],'COMPLETE');self.assertFalse(api.sent)
    def test_chain_depth_blocks(self):
        with tempfile.TemporaryDirectory() as t:
            p=pathlib.Path(t)/'m.json';stage(p)
            with self.assertRaisesRegex(ValueError,'CHAIN_DEPTH_EXHAUSTED'):
                ac.dispatch_if_needed(FakeAPI(),'222','main','9','30',p,pause_seconds=0,sleeper=lambda n:None)
    def test_source_state_tamper_blocks(self):
        with tempfile.TemporaryDirectory() as t:
            p=pathlib.Path(t)/'m.json';stage(p)
            j=json.loads(p.read_text());j['orders_placed']=1;p.write_text(json.dumps(j))
            with self.assertRaisesRegex(ValueError,'RESEARCH_LOCK'):
                ac.dispatch_if_needed(FakeAPI(),'222','main','0','30',p,pause_seconds=0,sleeper=lambda n:None)
    def test_bad_abort_streak_fails(self):
        with tempfile.TemporaryDirectory() as t:
            p=pathlib.Path(t)/'m.json';stage(p)
            with self.assertRaisesRegex(ValueError,'ABORT_STREAK_INPUT'):
                ac.dispatch_if_needed(FakeAPI(),'222','main','0','30',p,abort_streak_text='99',pause_seconds=0,sleeper=lambda n:None)
    def test_rerun_guard(self):
        with tempfile.TemporaryDirectory() as t:
            p=pathlib.Path(t)/'m.json';stage(p)
            with self.assertRaisesRegex(ValueError,'RERUN_NOT_AUTO_DISPATCHED'):
                ac.dispatch_if_needed(FakeAPI(),'222','main','0','30',p,attempt_text='2',pause_seconds=0,sleeper=lambda n:None)

if __name__=='__main__':unittest.main()
