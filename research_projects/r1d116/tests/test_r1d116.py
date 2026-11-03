"""R116 invariant tests. No fixed human reference used in algorithm training."""
from __future__ import annotations
import sys,tempfile,unittest,hashlib
from pathlib import Path
import pandas as pd
HERE=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(HERE))
from recent_turn import line_level,scale_of,validate_line,score_line,acceptable,replay_one,RULES
from research_r1d116 import read_sources,_comparison


def line(**override):
    q={'algorithm':'TS_BODY_WICK_PROXY','line_id':'synthetic|upper|0|3','kind':'UPPER',
       'family':'BULL_BODY_TOP','origin':'BODY','a1':'2024-01-01','a2':'2024-01-04',
       'i1':0,'i2':3,'v1':110.,'v2':107.,'m':-1.,'known_idx':3,'known_date':'2024-01-04',
       'span':3,'anchor_strength':.8,'interior_body_touches':2,'interior_wick_touches':2,
       'mean_body_error_atr':.3,'body_pen':0,'wick_pen':0}
    q.update(override);return q


def stamps(prices):
    return pd.DataFrame({'idx':range(len(prices)),'date':[f'2024-01-{i+1:02d}' for i in range(len(prices))],
                         'close':[float(x) for x in prices]})

class BasicGeometry(unittest.TestCase):
    def test_original_anchors_exact(self):
        q=line();self.assertAlmostEqual(line_level(q,q['i1']),q['v1']);self.assertAlmostEqual(line_level(q,q['i2']),q['v2'])
    def test_valid(self):validate_line(line())
    def test_reversed(self):
        with self.assertRaises(AssertionError):validate_line(line(i1=7,i2=3))
    def test_future_anchor(self):
        with self.assertRaises(AssertionError):validate_line(line(known_idx=2))
    def test_wrong_endpoint_price(self):
        with self.assertRaises(AssertionError):validate_line(line(v2=108.))
    def test_short_scale(self):self.assertEqual(scale_of(line()),'SHORT')
    def test_medium_scale(self):self.assertEqual(scale_of(line(i2=15,span=15)),'MEDIUM')
    def test_long_scale(self):self.assertEqual(scale_of(line(i2=45,span=45)),'LONG')
    def test_pivot25_forces_long(self):self.assertEqual(scale_of(line(family='WICK_HIGH_PIVOT25')),'LONG')
    def test_score_decomposition(self):
        x=score_line(line(),4,105.);self.assertAlmostEqual(x[0],sum(x[1:4]),places=4)
    def test_future_known_rejected(self):self.assertFalse(acceptable(line(known_idx=6),4,107.,'SHORT','UPPER'))
    def test_same_day_new_rejected_as_active(self):self.assertFalse(acceptable(line(),3,107.,'SHORT','UPPER'))
    def test_next_day_known_accepted(self):self.assertTrue(acceptable(line(),4,106.,'SHORT','UPPER'))
    def test_mismatched_scale(self):self.assertFalse(acceptable(line(),4,106.,'MEDIUM','UPPER'))
    def test_wrong_side_upper(self):self.assertFalse(acceptable(line(),4,130.,'SHORT','UPPER'))
    def test_wrong_side_lower(self):
        q=line(kind='LOWER');self.assertFalse(acceptable(q,4,90.,'SHORT','LOWER'))
    def test_old_endpoint_rejected(self):self.assertFalse(acceptable(line(),22,107.,'SHORT','UPPER'))
    def test_price_distance(self):self.assertFalse(acceptable(line(),4,83.,'SHORT','UPPER'))

class Replay(unittest.TestCase):
    def test_result_shape_and_pit(self):
        q=line();d,changes,ranks=replay_one('000001',q['algorithm'],[q],stamps([112,110,109,107,105,104,104]))
        self.assertEqual(len(d),42);self.assertTrue(d.known_idx.fillna(0).le(d.idx).all())
        upper=d[(d.scale=='SHORT')&(d.kind=='UPPER')]
        self.assertEqual(upper.loc[upper.idx.eq(3),'line_id'].iloc[0],'')
        self.assertEqual(upper.loc[upper.idx.eq(4),'line_id'].iloc[0],q['line_id'])
    def test_ignore_future_appended_candles(self):
        q=line()
        a,_,_=replay_one('000001',q['algorithm'],[q],stamps([112,110,109,107,105,104,103]))
        b,_,_=replay_one('000001',q['algorithm'],[q],stamps([112,110,109,107,105,888,500]))
        aa=a[a.idx<=4].reset_index(drop=True);bb=b[b.idx<=4].reset_index(drop=True)
        pd.testing.assert_frame_equal(aa,bb)
    def test_ignore_future_known_candidates(self):
        q=line();future=line(line_id='future',i1=5,i2=8,v1=110.,v2=107.,known_idx=8)
        a,_,_=replay_one('000001',q['algorithm'],[q],stamps([112,110,109,107,105,104,103]))
        b,_,_=replay_one('000001',q['algorithm'],[q,future],stamps([112,110,109,107,105,104,103]))
        pd.testing.assert_frame_equal(a,b)
    def test_reject_bad_algorithm(self):
        with self.assertRaises(AssertionError):replay_one('000001','TS_BODY_WICK_PROXY',[line(algorithm='X')],stamps([1,2,3,4,5]))
    def test_reject_missing_day(self):
        d=stamps([1,2,3,4,5]).drop(index=2)
        with self.assertRaises(AssertionError):replay_one('000001','TS_BODY_WICK_PROXY',[line()],d)
    def test_no_historical_line_reactivate_after_exit(self):
        q=line()
        d,_,_=replay_one('000001',q['algorithm'],[q],stamps([112,110,109,107,105,126,108,105,104,103]))
        active=d[(d.scale=='SHORT')&d.kind.eq('UPPER')&d.line_id.eq(q['line_id'])]
        self.assertEqual(active.idx.tolist(),[4])
    def test_unrelated_reference_never_training(self):
        # replay_one accepts NO manual references or fixed-stock labels.
        import inspect
        self.assertNotIn('references',inspect.signature(replay_one).parameters)
    def test_empty_candidates_return_wait(self):
        d,changes,rank=replay_one('000001','TS_BODY_WICK_PROXY',[],stamps([1,2,3,4]))
        self.assertTrue(d.line_id.eq('').all());self.assertTrue(rank.empty);self.assertTrue(changes.empty)
    def test_source_close_nonpositive(self):
        with self.assertRaises(AssertionError):replay_one('000001','TS_BODY_WICK_PROXY',[],stamps([1,2,0,4]))

class FrozenInputs(unittest.TestCase):
    def test_original_r112_geometry_comparison(self):
        q=line();z=pd.DataFrame([{**q,'code':'000001'}]);rec={
         'code':'000001','algorithm':q['algorithm'],'date':'2024-01-05','idx':4,'scale':'SHORT',
         'kind':'UPPER','line_id':q['line_id'],'r112_level':106.,'close':105.}
        p=pd.DataFrame([rec]).drop(columns='r112_level')
        orig=pd.DataFrame([{'code':'000001','algorithm':q['algorithm'],'date':'2024-01-05',
           'scale':'SHORT','kind':'UPPER','idx':4,'active_line_id':q['line_id'],
           'active_level':106.,'state':'ACTIVE','event':'NONE','anchor1':q['a1'],'anchor2':q['a2']}])
        x=_comparison(p,orig,z);self.assertFalse(bool(x.different_line.iloc[0]));self.assertEqual(x.r112_level.iloc[0],106.)
    def test_detect_corrupt_baseline_line_projection(self):
        q=line();z=pd.DataFrame([{**q,'code':'000001'}]);p=pd.DataFrame([{'code':'000001',
         'algorithm':q['algorithm'],'date':'2024-01-05','scale':'SHORT','kind':'UPPER','idx':4,
         'line_id':q['line_id']}]);orig=pd.DataFrame([{'code':'000001','algorithm':q['algorithm'],
         'date':'2024-01-05','scale':'SHORT','kind':'UPPER','idx':4,'active_line_id':q['line_id'],
         'active_level':999.,'state':'ACTIVE','event':'NONE','anchor1':q['a1'],'anchor2':q['a2']}])
        with self.assertRaises(AssertionError):_comparison(p,orig,z)
    def test_source_hash_always_recomputed(self):
        with tempfile.TemporaryDirectory() as temp:
            p=Path(temp);q=p/'r111';q.mkdir();r=p/'r112';r.mkdir()
            a=pd.DataFrame([{**line(),'code':'000001'}]);a.to_csv(q/'R1D111_INDEPENDENT_METHOD_LINES_PIT.csv',index=False)
            b=[]
            for scale in ('SHORT','MEDIUM','LONG'):
                for side in ('UPPER','LOWER'):
                    for alg in ('TS_BODY_WICK_PROXY','TV_PIVOT_ATR_PROXY','AC_PATTERN_FLOW_PROXY','OSS_TOUCH_PEN_PROXY'):
                        b.append({'code':'000001','date':'2024-01-05','algorithm':alg,'scale':scale,'kind':side})
            pd.DataFrame(b).to_csv(r/'R1D112_MULTISCALE_ACTIVE_LINES_DAILY_PIT.csv',index=False)
            files,paths,sha=read_sources(q,r)
            self.assertEqual(len(files['r112_daily']),24)
            with open(q/'R1D111_INDEPENDENT_METHOD_LINES_PIT.csv','a') as f:f.write('\n')
            self.assertNotEqual(hashlib.sha256((q/'R1D111_INDEPENDENT_METHOD_LINES_PIT.csv').read_bytes()).hexdigest(),sha['candidates'])

if __name__=='__main__':unittest.main()
