from pathlib import Path
import sys,unittest,math
import numpy as np
import pandas as pd
BASE=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(BASE))
from algorithms import prepare,make_anchors,generate_candidates,value,quality,ALGORITHMS
from lifecycle import monitor,assess_manual,archived_line_retests
from patterns import pattern_asof


def fixture(n=65):
    xs=np.arange(n)
    close=110-0.17*xs+1.7*np.sin(xs*.48)
    op=close-.6*np.sin(xs*1.25)
    hi=np.maximum(close,op)+.65
    lo=np.minimum(close,op)-.7
    return prepare(pd.DataFrame({'date':pd.date_range('2024-01-02',periods=n,freq='B'),
        'open':op,'high':hi,'low':lo,'close':close,'volume':1000+5*xs,'amount':close*(1000+5*xs)}))


def mock_line(d,kind='UPPER',known=30):
    v=105. if kind=='UPPER' else 95.
    return {'algorithm':'TS_BODY_WICK_PROXY','line_id':'test-a','kind':kind,'family':'BULL_BODY_TOP',
      'origin':'BODY','a1':str(d.iloc[15].date.date()),'a2':str(d.iloc[28].date.date()),'i1':15,'i2':28,
      'v1':v,'v2':v,'m':0.,'known_idx':known,'known_date':str(d.iloc[known].date.date()),
      'span':13,'atr_at_known':1,'body_pen':0,'wick_pen':0,'interior_wick_touches':1,
      'interior_body_touches':0,'mean_body_error_atr':0,'anchor_strength':.8}

class PivotTests(unittest.TestCase):
    def test_candle_anchor_known_at_close(self):
        d=fixture();a=make_anchors(d)
        self.assertTrue(all(x['known']==x['idx'] for x in a if x['radius']==0))
    def test_confirmed_pivot_known_after_right_bars(self):
        d=fixture();a=make_anchors(d)
        self.assertTrue(any(x['radius']==5 for x in a))
        self.assertTrue(all(x['known']==x['idx']+x['radius'] for x in a if x['radius']))
    def test_pivot_radius25_mature_only(self):
        d=fixture();a=make_anchors(d)
        self.assertTrue(all(x['idx']>=25 and x['known']<len(d) for x in a if x['radius']==25))
    def test_future_mutation_does_not_change_past_known_candidates(self):
        d=fixture();t=42
        future=d.copy();future.loc[future.index>t,'high']=future.loc[future.index>t,'high']*1.4
        future.loc[future.index>t,'close']=future.loc[future.index>t,'close']*1.2
        future.loc[future.index>t,'open']=future.loc[future.index>t,'open']*1.2
        future.loc[future.index>t,'low']=future.loc[future.index>t,'low']*.8
        future=prepare(future)
        for alg in ALGORITHMS:
            o=generate_candidates(d,alg);z=generate_candidates(future,alg)
            old={x['line_id'] for x in o if x['known_idx']<=t}
            new={x['line_id'] for x in z if x['known_idx']<=t}
            self.assertEqual(old,new,alg)
    def test_candidate_cannot_be_used_before_known(self):
        d=fixture();ll=generate_candidates(d,'TS_BODY_WICK_PROXY')
        self.assertTrue(ll)
        l=next((x for x in ll if x['known_idx']>0),None)
        if l:self.assertRaises(ValueError,quality,d,l,l['known_idx']-1)
    def test_line_ends_not_in_future(self):
        d=fixture();ll=generate_candidates(d,'TS_BODY_WICK_PROXY')
        for l in ll:self.assertLessEqual(l['i2'],l['known_idx'])
    def test_prefix_equivalence_active_state_at_asof(self):
        full=fixture(85);t=51
        prefix=prepare(full.iloc[:t+1].copy())
        for alg in ALGORITHMS:
            x=generate_candidates(full,alg)
            y=generate_candidates(prefix,alg)
            a,_=monitor(full,alg,x);b,_=monitor(prefix,alg,y)
            a=a[a.idx<=t].reset_index(drop=True)
            b=b.reset_index(drop=True)
            self.assertEqual(a[['date','kind','active_line_id','event']].to_dict('records'),
                             b[['date','kind','active_line_id','event']].to_dict('records'),alg)
    def test_pattern_readiness_never_preconfirmed(self):
        d=fixture(75);lines=generate_candidates(d,'AC_PATTERN_FLOW_PROXY')
        for t in (40,50,60):
            rows=pattern_asof(d,lines,t)
            self.assertTrue(all(str(x['first_both_known_date'])<=str(d.date.iloc[t].date()) for x in rows))
    def test_pattern_prefix_future_invariance(self):
        d=fixture(75);t=52
        prefix=prepare(d.iloc[:t+1].copy())
        before=pattern_asof(d,generate_candidates(d,'AC_PATTERN_FLOW_PROXY'),t)
        after=pattern_asof(prefix,generate_candidates(prefix,'AC_PATTERN_FLOW_PROXY'),t)
        self.assertEqual(before,after)
    def test_bodies_are_not_wick_pivots(self):
        d=fixture();a=make_anchors(d)
        self.assertTrue(any(x['radius']==0 for x in a))
        self.assertTrue(any(x['radius'] in (2,3,5) for x in a))
    def test_all_four_method_names_distinct(self):
        self.assertEqual(len(set(ALGORITHMS)),4)
    def test_all_four_produce_distinct_outputs(self):
        d=fixture();names=[]
        for alg in ALGORITHMS:
            lines=generate_candidates(d,alg)
            names.append(tuple(x['line_id'] for x in lines))
        self.assertEqual(len(set(names)),4)
    def test_no_negative_or_nan_price(self):
        d=fixture();d.loc[2,'low']=-1
        self.assertRaises(ValueError,prepare,d)
    def test_bad_ohlc_rejected(self):
        d=fixture();d.loc[2,'high']=d.loc[2,'low']-1
        self.assertRaises(ValueError,prepare,d)
    def test_duplicate_date_rejected(self):
        d=fixture();d.loc[2,'date']=d.loc[1,'date']
        self.assertRaises(ValueError,prepare,d)

class ActiveLifecycleTests(unittest.TestCase):
    def _flat(self):
        n=35;close=np.ones(n)*10.
        close[5:7]=10.5;close[7:]=10.4
        op=close.copy();high=close+.3;low=close-.3
        low[7]=10.1;high[7]=10.6
        d=pd.DataFrame({'date':pd.date_range('2024-01-01',periods=n,freq='B'),
            'open':op,'high':high,'low':low,'close':close,'volume':np.ones(n)*1000,
            'amount':close*1000})
        d=prepare(d)
        l={'algorithm':'TS_BODY_WICK_PROXY','line_id':'mock','kind':'UPPER',
          'family':'BULL_BODY_TOP','origin':'BODY','a1':str(d.iloc[0].date.date()),
          'a2':str(d.iloc[3].date.date()),'i1':0,'i2':3,'v1':10.2,'v2':10.2,'m':0.,
          'known_idx':4,'known_date':str(d.iloc[4].date.date()),'span':3,'atr_at_known':1.,
          'body_pen':0,'wick_pen':0,'interior_wick_touches':0,'interior_body_touches':0,
          'mean_body_error_atr':0.,'anchor_strength':.8}
        return d,l
    def test_active_only_after_confirmation(self):
        d,l=self._flat();states,events=monitor(d,'TS_BODY_WICK_PROXY',[l])
        r=states[states.kind.eq('UPPER')]
        self.assertTrue((r[r.idx<4].active_line_id=='').all())
        self.assertEqual(r[r.idx==4].iloc[0].active_line_id,'mock')
    def test_two_close_break_not_single_close(self):
        d,l=self._flat();states,events=monitor(d,'TS_BODY_WICK_PROXY',[l])
        r=states[states.kind.eq('UPPER')]
        self.assertEqual(r[r.idx==5].iloc[0].active_line_id,'mock')
        self.assertEqual(r[r.idx==6].iloc[0].event,'BREAK_CONFIRMED_TWO_CLOSES')
        self.assertEqual(r[r.idx==6].iloc[0].active_line_id,'')
    def test_retired_not_reactivated(self):
        d,l=self._flat();s,e=monitor(d,'TS_BODY_WICK_PROXY',[l]);r=s[s.kind.eq('UPPER')]
        self.assertTrue((r[r.idx>=6].active_line_id=='').all())
    def test_support_retest_only_after_break(self):
        d,l=self._flat();s,e=monitor(d,'TS_BODY_WICK_PROXY',[l]);r=archived_line_retests(d,'TS_BODY_WICK_PROXY',[l],e)
        self.assertTrue((r.days_after_break>=1).all())
        self.assertTrue((r.status=='ROLE_RETEST_CONFIRMED_PIT').any())
    def test_reference_is_not_injected_into_selection(self):
        d,l=self._flat();s,e=monitor(d,'TS_BODY_WICK_PROXY',[l]);refs=[('2024-01-02','2024-01-03','BULL_BODY_TOP','UPPER','MANUAL')]
        audit=assess_manual(d,'TS_BODY_WICK_PROXY',[l],s,refs)
        self.assertFalse(bool(audit.iloc[0].candidate_detected))
        self.assertEqual(len(s),len(d)*2)
    def test_quality_immutable_past(self):
        d,l=self._flat();a=quality(d,l,4)[0];d2=d.copy();d2.loc[d2.index>4,'close']+=7
        self.assertEqual(a,quality(d2,l,4)[0])
    def test_daily_has_two_independent_lanes(self):
        d,l=self._flat();s,e=monitor(d,'TS_BODY_WICK_PROXY',[l]);self.assertEqual(len(s),len(d)*2)
        self.assertEqual(set(s.kind),{'UPPER','LOWER'})
    def test_reanchor_has_newer_anchor_and_known_date(self):
        d=fixture(85)
        for alg in ALGORITHMS:
            lines=generate_candidates(d,alg)
            _,events=monitor(d,alg,lines)
            byid={l['line_id']:l for l in lines}
            if events.empty:continue
            for r in events[events.event.eq('REANCHORED_NEW_CONFIRMED_PIVOT_OR_CANDLE')].to_dict('records'):
                new=byid[r['active_line_id']];old=byid[r['old_line_id']]
                self.assertGreater(new['i2'],old['i2'])
                self.assertLessEqual(new['known_idx'],int(r['idx']))
                self.assertNotEqual(new['line_id'],old['line_id'])
    def test_break_retains_original_line_id(self):
        d,l=self._flat();s,e=monitor(d,'TS_BODY_WICK_PROXY',[l]);br=e[e.event.eq('BREAK_CONFIRMED_TWO_CLOSES')]
        self.assertEqual(br.iloc[0].old_line_id,'mock')

if __name__=='__main__':unittest.main()
