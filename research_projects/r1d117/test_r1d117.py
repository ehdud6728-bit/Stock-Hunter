import unittest,sys,copy
from pathlib import Path
import pandas as pd
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from stable_roles import level,verify,admissible,beyond,score,replay_one,POLICY,_near_candle

class TestStableRoles(unittest.TestCase):
    def q(self,lid='l1',kind='UPPER',m=-1,known=4,span=4,start=0,price=120):
        return dict(line_id=lid,kind=kind,known_idx=known,i1=start,i2=start+span,
          v1=float(price),v2=float(price+m*span),m=float(m),
          anchor_strength=1.0,interior_body_touches=2,interior_wick_touches=1,
          mean_body_error_atr=0.1,body_pen=0,wick_pen=0,
          a1='2024-01-01',a2='2024-01-05',known_date='2024-01-05',family='BULL_BODY_TOP')
    def data(self,closes,known=4):
        rec=[];top=[];raw=[]
        for t,px in enumerate(closes):
            for sc in POLICY:
                for kind in ['UPPER','LOWER']:
                    lid='l1' if sc=='SHORT' and kind=='UPPER' and t>known else ''
                    rec.append({'code':'000001','algorithm':'TEST','idx':t,'date':f'2024-01-{t+1:02d}',
                      'close':px,'scale':sc,'kind':kind,'line_id':lid})
                    if lid:top.append({'idx':t,'scale':sc,'kind':kind,'line_id':lid})
            raw.append({'idx':t,'date':f'2024-01-{t+1:02d}','open':px,'high':px+2,'low':px-2,'close':px,'volume':1,'amount':100})
        return pd.DataFrame(rec),pd.DataFrame(top),pd.DataFrame(raw)
    def test_geometry_first(self):self.assertEqual(level(self.q(),4),116.)
    def test_geometry_valid(self):verify(self.q())
    def test_geometry_invalid(self):
        q=self.q();q['v2']+=3
        with self.assertRaises(AssertionError):verify(q)
    def test_future_anchor_rejected(self):
        q=self.q();q['known_idx']=2
        with self.assertRaises(AssertionError):verify(q)
    def test_known_not_usable_same_close(self):self.assertFalse(admissible(self.q(),4,110,'SHORT','UPPER'))
    def test_available_next_day(self):self.assertTrue(admissible(self.q(),5,110,'SHORT','UPPER'))
    def test_wrong_side_rejected(self):self.assertFalse(admissible(self.q(),5,125,'SHORT','UPPER'))
    def test_wrong_kind_rejected(self):self.assertFalse(admissible(self.q(),5,110,'SHORT','LOWER'))
    def test_retired_rejected(self):self.assertFalse(admissible(self.q(),5,110,'SHORT','UPPER',{'l1'}))
    def test_far_rejected(self):self.assertFalse(admissible(self.q(),5,50,'SHORT','UPPER'))
    def test_beyond_upper(self):self.assertTrue(beyond(116,115,'UPPER'))
    def test_beyond_lower(self):self.assertTrue(beyond(100,102,'LOWER'))
    def test_close_touch(self):self.assertTrue(_near_candle(self.q(),8,dict(low=109,high=115,close=111)))
    def test_score_finite(self):self.assertTrue(-100<score(self.q(),6,110)<100)
    def test_wait_for_known(self):
        b,top,r=self.data([110]*8)
        daily,events,roles=replay_one('000001','TEST',b,top,[self.q()],r)
        short=daily[(daily.scale=='SHORT')&(daily.kind=='UPPER')]
        self.assertTrue(short[short.idx.le(4)].line_id.eq('').all())
        self.assertEqual(short[short.idx.eq(5)].line_id.iloc[0],'l1')
    def test_two_consecutive_close_break(self):
        b,top,r=self.data([110]*8+[120,122,110,110])
        d,e,roles=replay_one('000001','TEST',b,top,[self.q()],r)
        active=d[(d.scale=='SHORT')&(d.kind=='UPPER')].set_index('idx')
        self.assertEqual(active.loc[8,'event'],'FIRST_CLOSE_OUTSIDE_PENDING')
        self.assertEqual(active.loc[9,'event'],'TWO_CLOSE_BREAK_ARCHIVE')
    def test_one_close_only_not_archived(self):
        b,top,r=self.data([110]*8+[120,110,110])
        d,e,_=replay_one('000001','TEST',b,top,[self.q()],r)
        active=d[(d.scale=='SHORT')&(d.kind=='UPPER')].set_index('idx')
        self.assertEqual(active.loc[8,'status'],'PENDING_BREAK')
        self.assertEqual(active.loc[9,'status'],'ACTIVE')
    def test_no_recycle_after_break(self):
        b,top,r=self.data([110]*8+[120,122]+[110]*8)
        d,e,_=replay_one('000001','TEST',b,top,[self.q()],r)
        active=d[(d.scale=='SHORT')&(d.kind=='UPPER')].set_index('idx')
        self.assertTrue(active.loc[10:,'line_id'].eq('').all())
    def test_future_tail_cannot_change_past(self):
        data=[110]*8+[120,122]+[110]*4
        b,t,r=self.data(data)
        d1,_,_=replay_one('000001','TEST',b,t,[self.q()],r)
        b2=b.copy();b2.loc[b2.idx.ge(12),'close']=400
        r2=r.copy();r2.loc[r2.idx.ge(12),'close']=400
        d2,_,_=replay_one('000001','TEST',b2,t,[self.q()],r2)
        x=d1[d1.idx.lt(12)][['idx','scale','kind','line_id','event']].reset_index(drop=True)
        y=d2[d2.idx.lt(12)][['idx','scale','kind','line_id','event']].reset_index(drop=True)
        pd.testing.assert_frame_equal(x,y)
    def test_no_cross_retest_before_confirm(self):
        b,t,r=self.data([110]*8+[120,122,110,110,110])
        d,e,roles=replay_one('000001','TEST',b,t,[self.q()],r)
        self.assertTrue(roles.empty or (roles.idx>9).all())
    def test_six_lanes_every_day(self):
        b,t,r=self.data([110]*15)
        d,_,_=replay_one('000001','TEST',b,t,[self.q()],r)
        self.assertTrue(d.groupby('idx').size().eq(6).all())
    def test_original_input_unmodified(self):
        b,t,r=self.data([110]*15);bb=b.copy(deep=True);tt=t.copy(deep=True)
        replay_one('000001','TEST',b,t,[self.q()],r)
        pd.testing.assert_frame_equal(b,bb);pd.testing.assert_frame_equal(t,tt)
    def test_stable_active_held_when_no_new_proposal(self):
        b,t,r=self.data([110]*15);b.loc[b.idx.ge(8),'line_id']='';t=t[t.idx.lt(8)]
        d,_,_=replay_one('000001','TEST',b,t,[self.q()],r)
        sel=d[(d.scale=='SHORT')&(d.kind=='UPPER')].set_index('idx')
        self.assertEqual(sel.loc[10,'line_id'],'l1')
    def test_no_forced_selection_other_lanes(self):
        b,t,r=self.data([110]*10);d,_,_=replay_one('000001','TEST',b,t,[self.q()],r)
        self.assertTrue(d[d.scale.eq('MEDIUM')].line_id.eq('').all())

if __name__=='__main__':unittest.main()
