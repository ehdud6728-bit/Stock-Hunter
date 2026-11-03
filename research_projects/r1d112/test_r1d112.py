from pathlib import Path
import sys,unittest
import numpy as np,pandas as pd
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT),str(ROOT.parent/'r1d111')]
from algorithms import prepare,generate_candidates,ALGORITHMS
from multiscale import scale_of,SCALES,monitor_multiscale,manual_audit,level,_score


def fixture(n=78):
    x=np.arange(n)
    c=100+.13*x+1.4*np.sin(x*.5)
    o=c-.4*np.sin(x*.9)
    hi=np.maximum(o,c)+.85;lo=np.minimum(o,c)-.9
    return prepare(pd.DataFrame({'date':pd.date_range('2024-01-02',periods=n,freq='B'),
       'open':o,'high':hi,'low':lo,'close':c,'volume':1000+10*x,'amount':c*(1000+10*x)}))


def mock(d,scale='SHORT',known=10,kind='UPPER',v=100.,id='test'):
    span={'SHORT':5,'MEDIUM':18,'LONG':40}[scale]
    end=max(span+1,known)
    i1=end-span
    return {'algorithm':'TS_BODY_WICK_PROXY','line_id':id,'kind':kind,'family':'BULL_BODY_TOP',
      'origin':'BODY','a1':str(d.date.iloc[i1].date()),'a2':str(d.date.iloc[end].date()),
      'i1':i1,'i2':end,'v1':v,'v2':v,'m':0.,'known_idx':end,
      'known_date':str(d.date.iloc[end].date()),'span':span,'atr_at_known':1.,
      'body_pen':0,'wick_pen':0,'interior_wick_touches':0,'interior_body_touches':0,
      'mean_body_error_atr':0.,'anchor_strength':.7}


class MultiScaleTests(unittest.TestCase):
    def test_three_scales(self):
        d=fixture();self.assertEqual([scale_of(mock(d,s)) for s in SCALES],list(SCALES))
    def test_pivot25_is_long_even_if_span_short(self):
        d=fixture();l=mock(d,'SHORT');l['family']='WICK_LOW_PIVOT25'
        self.assertEqual(scale_of(l),'LONG')
    def test_six_lanes_per_day(self):
        d=fixture();s,e,r=monitor_multiscale(d,'TS_BODY_WICK_PROXY',[])
        self.assertEqual(len(s),len(d)*6)
        self.assertEqual(len(s[['scale','kind']].drop_duplicates()),6)
    def test_never_use_unknown_line(self):
        d=fixture();l=mock(d,known=0,v=104.,id='a')
        s,e,r=monitor_multiscale(d,'TS_BODY_WICK_PROXY',[l])
        self.assertTrue((s[s.idx<l['known_idx']].active_line_id=='').all())
        self.assertRaises(ValueError,_score,d,l,l['known_idx']-1)
    def test_archive_never_revive(self):
        d=fixture(78);d.loc[d.index>=50,'close']=125.;d.loc[d.index>=50,'high']=127.;d.loc[d.index>=50,'low']=120.;d.loc[d.index>=50,'open']=124.
        d=prepare(d)
        l=mock(d,scale='MEDIUM',known=35,v=106.,id='upper-z')
        s,e,r=monitor_multiscale(d,'TS_BODY_WICK_PROXY',[l])
        seen=s[s.active_line_id.eq('upper-z')]
        # If selected, a line cannot return after its first confirmed break.
        if len(seen):
            breaks=e[e.event.eq('TWO_CLOSE_BREAK_ARCHIVE')]
            if len(breaks):
                t=int(breaks.iloc[0].idx)
                self.assertFalse(s[s.idx>t].active_line_id.eq('upper-z').any())
    def test_future_data_not_used_for_history(self):
        d=fixture();t=48
        p=prepare(d.iloc[:t+1].copy())
        for alg in ALGORITHMS:
            complete=generate_candidates(d,alg)
            prefix=generate_candidates(p,alg)
            a,_,_=monitor_multiscale(d,alg,complete)
            b,_,_=monitor_multiscale(p,alg,prefix)
            left=a[a.idx<=t][['idx','scale','kind','active_line_id','event']].reset_index(drop=True)
            right=b[['idx','scale','kind','active_line_id','event']].reset_index(drop=True)
            pd.testing.assert_frame_equal(left,right)
    def test_evaluate_manual_not_used_in_monitor(self):
        d=fixture();l=mock(d,scale='MEDIUM',known=30,v=102.,id='manual')
        s,e,r=monitor_multiscale(d,'TS_BODY_WICK_PROXY',[l])
        old=pd.DataFrame([{'algorithm':'TS_BODY_WICK_PROXY','reference':'TEST','first_active_date':''}])
        ref=[(l['a1'],l['a2'],l['family'],l['kind'],'TEST')]
        out=manual_audit(d,'TS_BODY_WICK_PROXY',[l],s,ref,old)
        self.assertTrue(out.iloc[0].discovered)
        self.assertEqual(len(s),len(d)*6)
    def test_missing_reference_not_injected(self):
        d=fixture();s,_,_=monitor_multiscale(d,'TS_BODY_WICK_PROXY',[])
        out=manual_audit(d,'TS_BODY_WICK_PROXY',[],s,[('x','y','z','UPPER','TEST')],pd.DataFrame(columns=['algorithm','reference','first_active_date']))
        self.assertFalse(out.iloc[0].discovered)
    def test_all_four_independent(self):
        self.assertEqual(len(set(ALGORITHMS)),4)
    def test_role_retest_never_same_day_as_break(self):
        d=fixture();l=mock(d,'SHORT',known=11,v=100,id='r')
        s,e,r=monitor_multiscale(d,'TS_BODY_WICK_PROXY',[l])
        if len(r):self.assertTrue((r.retest_idx>r.break_idx).all())
    def test_only_six_per_algo_stock_date(self):
        d=fixture();s,_,_=monitor_multiscale(d,'TS_BODY_WICK_PROXY',generate_candidates(d,'TS_BODY_WICK_PROXY'))
        self.assertTrue((s.groupby('idx').size()==6).all())
    def test_no_forward_label_in_outputs(self):
        d=fixture();s,e,r=monitor_multiscale(d,'TS_BODY_WICK_PROXY',[])
        self.assertFalse(any('EXPOST' in col for col in s.columns))
        self.assertFalse(any('EXPOST' in col for col in e.columns))
    def test_level_extrapolation(self):
        d=fixture();l=mock(d,'MEDIUM');l['m']=2.
        self.assertEqual(level(l,l['i2']+3),l['v2']+6)
    def test_replacement_requires_newer_endpoint(self):
        d=fixture();ls=generate_candidates(d,'AC_PATTERN_FLOW_PROXY')
        s,e,_=monitor_multiscale(d,'AC_PATTERN_FLOW_PROXY',ls)
        lookup={l['line_id']:l for l in ls}
        for ev in e[e.event.eq('REANCHOR_NEW_CONFIRMED')].to_dict('records'):
            self.assertGreater(lookup[ev['new_line_id']]['i2'],lookup[ev['old_line_id']]['i2'])
    def test_slope_flip_is_not_prediction(self):
        d=fixture();s,e,_=monitor_multiscale(d,'TS_BODY_WICK_PROXY',[])
        self.assertNotIn('win_rate',s.columns)

if __name__=='__main__':unittest.main()

class MarketStructureTests(unittest.TestCase):
    def test_automatic_shape_is_descriptive_not_predictive(self):
        from multiscale import structure_asof
        d=fixture();s,_,_=monitor_multiscale(d,'TS_BODY_WICK_PROXY',[])
        shape=structure_asof(s)
        self.assertEqual(len(shape),len(d)*3)
        self.assertTrue(shape.line_geometry_state_PIT.eq('INSUFFICIENT_ACTIVE_LINES').all())
        self.assertTrue(shape.warning.str.contains('NOT_CONFIRMED').all())
    def test_no_same_bar_swing_confirmation_claim(self):
        d=fixture();ls=generate_candidates(d,'TS_BODY_WICK_PROXY')
        s,ev,_=monitor_multiscale(d,'TS_BODY_WICK_PROXY',ls)
        by={x['line_id']:x for x in ls}
        for r in s[s.active_line_id.ne('')].to_dict('records'):
            self.assertLessEqual(int(by[r['active_line_id']]['known_idx']),int(r['idx']))
    def test_retired_lines_record_old_id(self):
        d=fixture(78);s,e,r=monitor_multiscale(d,'OSS_TOUCH_PEN_PROXY',generate_candidates(d,'OSS_TOUCH_PEN_PROXY'))
        if len(e):
            self.assertTrue(e[e.event.isin(['REANCHOR_NEW_CONFIRMED','TWO_CLOSE_BREAK_ARCHIVE','ARCHIVE_STALE_DISPLAY'])].old_line_id.ne('').all())
    def test_candle_context_not_future_outcomes(self):
        from research_r1d112 import TECH
        self.assertFalse(any('EXPOST' in c.upper() for c in TECH))
    def test_old_files_are_loaded_readonly(self):
        from research_r1d112 import FILES
        self.assertEqual(len(FILES),4)
        self.assertTrue(all(str(x).endswith('.csv') for x in FILES.values()))

class ExplicitRoleTransitionTests(unittest.TestCase):
    def test_two_close_break_then_two_later_retests(self):
        # A deterministic original resistance near 10.20 first becomes active,
        # is broken on TWO observed closes, then revisited on later bars.
        n=32;close=np.ones(n)*10.
        close[8]=10.6;close[9]=10.65;close[10]=10.40;close[11]=10.35
        high=close+.22;low=close-.23;op=close.copy()
        d=prepare(pd.DataFrame({'date':pd.date_range('2024-01-02',periods=n,freq='B'),
          'open':op,'high':high,'low':low,'close':close,
          'volume':np.ones(n)*1000,'amount':close*1000}))
        l=mock(d,scale='SHORT',known=6,kind='UPPER',v=10.2,id='upper-break-role')
        s,e,r=monitor_multiscale(d,'TS_BODY_WICK_PROXY',[l])
        found=e[e.event.eq('TWO_CLOSE_BREAK_ARCHIVE')]
        self.assertEqual(len(found),1)
        self.assertEqual(int(found.iloc[0].idx),9)
        rr=r[r.broken_line_id.eq('upper-break-role')]
        self.assertGreaterEqual(len(rr),2)
        self.assertTrue((rr.retest_idx>9).all())
        self.assertTrue(rr.status.eq('ROLE_RETEST_CONFIRMED_REPEATED').any())
        self.assertFalse(s[s.idx>9].active_line_id.eq('upper-break-role').any())
