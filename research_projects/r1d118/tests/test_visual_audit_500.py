"""Offline research-only tests: no network, no GitHub, no production outputs."""
import json,sys,tempfile,unittest
from pathlib import Path
import numpy as np
import pandas as pd
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from visual_audit_500 import (check_500_cohort,summarize,corp_risk,verify_one_event,load_manifest)
from research_r1d118 import features,lower_events,dedup_events

class VisualAudit500Tests(unittest.TestCase):
    def test_require_complete_500_and_frozen_200(self):
        files={f'{i:06d}':'x' for i in range(500)}
        audit={'selected_count':500,'selected_codes':list(files)}
        old=list(files)[:200]
        self.assertEqual(len(check_500_cohort(files,audit,old)),200)
        with self.assertRaisesRegex(ValueError,'NOT_500'):
            check_500_cohort(dict(list(files.items())[:499]),audit,old)
        with self.assertRaisesRegex(ValueError,'FROZEN_200'):
            check_500_cohort(files,audit,['999999']+old[:-1])

    def test_summarize_frozen200_vs_added300_exact(self):
        d=pd.DataFrame([
          {'code':'000001','signal_date':'2023-07-10','market_ret20_pct':-3.,'d5_net_pct':1.},
          {'code':'000001','signal_date':'2023-08-10','market_ret20_pct':3.,'d5_net_pct':-1.},
          {'code':'000345','signal_date':'2023-09-10','market_ret20_pct':-4.,'d5_net_pct':2.},
          {'code':'000345','signal_date':'2023-10-10','market_ret20_pct':3.,'d5_net_pct':-2.}])
        s=summarize(d,{'000001'})
        g=s[(s.cohort=='ADDED_300')&(s.market_condition=='MARKET_DOWN_LT_MINUS2')].iloc[0]
        self.assertEqual(int(g.events),1);self.assertEqual(float(g.mean_d5_net_pct),2.)

    def test_corporate_action_large_gap_flagged(self):
        dates=pd.bdate_range('2023-01-02',periods=90).strftime('%Y-%m-%d')
        s=pd.DataFrame({'date':dates,'open':[100.]*90,'close':[100.]*90})
        s.loc[50:,'open']=20.;s.loc[50:,'close']=20.
        f=corp_risk(s,53,list(dates))
        self.assertIn('OVERNIGHT_GAP_GT32PCT',f)
        self.assertIn('CLOSE_MOVE_GT35PCT',f)

    def test_market_suspension_mismatch_flagged(self):
        full=pd.bdate_range('2023-01-02',periods=91).strftime('%Y-%m-%d').tolist()
        # Stock did not trade on market session at index 44.
        actual=[x for j,x in enumerate(full) if j!=44]
        s=pd.DataFrame({'date':actual,'open':[100.]*90,'close':[100.]*90})
        self.assertIn('MARKET_TRADING_DAYS_MISSING_POSSIBLE_SUSPENSION',corp_risk(s,43,full))
        self.assertEqual(corp_risk(s,10,full),[])

    def test_replay_rejects_ledger_pnl_mismatch(self):
        dates=pd.bdate_range('2023-01-02',periods=230).strftime('%Y-%m-%d')
        rng=np.random.default_rng(13)
        close=100*np.exp(np.cumsum(rng.normal(0,.028,len(dates))))
        d=pd.DataFrame({'code':['000001']*len(dates),'date':dates,'open':close,'high':close*1.024,
                        'low':close*.976,'close':close,'amount':[1e10]*len(dates)})
        s=features(d)
        events,_=lower_events('000001',s)
        picked=dedup_events(events)
        picked=picked[(picked.family=='LOWER_RECLAIM')&(picked.band=='BB20')]
        self.assertFalse(picked.empty)
        e=picked.iloc[0].to_dict();e['market']='KOSPI';e['market_ret20_pct']=-3.
        key={('LOWER_RECLAIM','BB20',str(e['signal_date']),int(e['signal_idx']),int(e['anchor_idx']))}
        lookup={('KOSPI',e['signal_date']):-3.}
        r,det=verify_one_event(e,d,lookup,key)
        self.assertAlmostEqual(det['net_pct'],float(e['d5_net_pct']))
        bad=dict(e,d5_net_pct=float(e['d5_net_pct'])+1.)
        with self.assertRaisesRegex(ValueError,'LEDGER_PNL'):
            verify_one_event(bad,d,lookup,key)
        with self.assertRaisesRegex(ValueError,'MARKET_RET20'):
            verify_one_event(e,d,{('KOSPI',e['signal_date']):-4.},key)
        with self.assertRaisesRegex(ValueError,'CANONICAL'):
            verify_one_event(e,d,lookup,set())

    def test_real_ohlcv_chart_renders_png(self):
        from visual_audit_500 import make_chart
        import importlib.util
        if importlib.util.find_spec('matplotlib') is None:self.skipTest('matplotlib not installed in local test environment')
        dates=pd.bdate_range('2023-01-02',periods=80).strftime('%Y-%m-%d')
        close=100+10*np.sin(np.arange(80)/9.)
        d=pd.DataFrame({'code':['000001']*80,'date':dates,'open':close*.997,'high':close*1.03,
                        'low':close*.97,'close':close,'amount':[1e10]*80})
        s=features(d);i=57
        e={'code':'000001','signal_idx':i,'signal_date':str(d.at[i,'date'])}
        with tempfile.TemporaryDirectory() as td:
            target=Path(td)/'case.png'
            make_chart(s,e,{'net_pct':1.5,'market_ret20_pct':-3},target)
            self.assertTrue(target.exists());self.assertGreater(target.stat().st_size,20000)

    def test_sha_manifest_detects_tampering(self):
        import hashlib
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);x=root/'000001.csv';x.write_text('a,b\n1,2\n')
            digest=hashlib.sha256(x.read_bytes()).hexdigest()
            (root/'manifest.csv').write_text('code,path,sha256\n000001,000001.csv,'+digest+'\n')
            self.assertEqual(len(load_manifest(root/'manifest.csv')),1)
            x.write_text('a,b\n3,4\n')
            with self.assertRaisesRegex(ValueError,'OHLCV_SHA256'):
                load_manifest(root/'manifest.csv')

if __name__=='__main__':unittest.main()
