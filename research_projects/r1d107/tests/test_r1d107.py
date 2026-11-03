import sys,unittest,tempfile
from pathlib import Path
import pandas as pd
import numpy as np
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/'r1d106'),str(ROOT/'r1d107')]
from quality import line_quality,display_groups,triangle_quality,apex_limited_outcome
from multiline import make_lines,triangles
from research_r1d107 import enrich_asof,_snapshot,run


def price_frame(count=76):
    dates=pd.bdate_range('2024-01-02',periods=count)
    q=np.arange(count)
    close=100+0.08*q+np.sin(q/2.2)*1.8
    o=close-0.7
    hi=np.maximum(o,close)+1
    lo=np.minimum(o,close)-1
    return pd.DataFrame({'date':dates,'open':o,'high':hi,'low':lo,'close':close,
        'volume':1000+q*3,'amount':(1000+q*3)*close})


def ldef(known=10,kind='UPPER',family='BULL_BODY_TOP',i1=3,i2=10,v1=101,v2=104):
    df=price_frame(76)
    return {'line_id':'L1','kind':kind,'family':family,'i1':i1,'i2':i2,'v1':v1,'v2':v2,
       'known_idx':known,'known_date':str(df.iloc[known].date.date()),
       'a1':str(df.iloc[i1].date.date()),'a2':str(df.iloc[i2].date.date()),
       'slope_krw_per_bar':(v2-v1)/(i2-i1),'slope_pct_per_bar':.42,
       'anchor_strength':.67,'penetrations_1p2pct':0}


class QualityPITTests(unittest.TestCase):
    def setUp(self):
        self.d=price_frame();self.l=ldef()

    def test_not_before_line_known(self):
        with self.assertRaises(ValueError):line_quality(self.d,self.l,9)

    def test_known_is_provisional(self):
        v=line_quality(self.d,self.l,10)
        self.assertEqual(v['post_confirmation_contacts'],0)
        self.assertEqual(v['readiness'],'NEW_LINE_PROVISIONAL')

    def test_early_maturity(self):
        self.assertEqual(line_quality(self.d,self.l,12)['readiness'],'OBSERVED_1_2_BARS')

    def test_mature(self):
        self.assertEqual(line_quality(self.d,self.l,13)['readiness'],'OBSERVED_3PLUS_BARS')

    def test_future_tamper_unchanged(self):
        old=line_quality(self.d,self.l,20)
        d=self.d.copy();d.loc[21:,['open','high','low','close']]*=1.8
        d.loc[21:,['volume','amount']]*=2
        self.assertEqual(old,line_quality(d,self.l,20))

    def test_asof_tamper_changes_relevant(self):
        old=line_quality(self.d,self.l,20)
        d=self.d.copy();d.loc[20,['open','high','low','close']]*=1.1
        self.assertNotEqual(old['close_to_line_pct'],line_quality(d,self.l,20)['close_to_line_pct'])

    def test_preconfirmed_future_pivot_rejected(self):
        later=ldef(known=14)
        with self.assertRaises(ValueError):line_quality(self.d,later,13)

    def test_line_input_unmodified(self):
        old=self.l.copy();line_quality(self.d,self.l,20)
        self.assertEqual(old,self.l)

    def test_tiny_anchor_penalty(self):
        x=self.l.copy();x['anchor_strength']=.02
        self.assertLess(line_quality(self.d,x,10)['quality_diagnostic_0_100'],
                        line_quality(self.d,self.l,10)['quality_diagnostic_0_100'])

    def test_penetration_penalty(self):
        x=self.l.copy();x['penetrations_1p2pct']=3
        self.assertLess(line_quality(self.d,x,10)['quality_diagnostic_0_100'],
                        line_quality(self.d,self.l,10)['quality_diagnostic_0_100'])

    def test_bad_span_rejected(self):
        x=self.l.copy();x['i1']=x['i2']
        with self.assertRaises(ValueError):line_quality(self.d,x,14)

    def test_upper_lower_kind(self):
        x=self.l.copy();x['kind']='LOWER'
        self.assertEqual(line_quality(self.d,x,20)['kind'],'LOWER')

    def test_line_stays_archived(self):
        lines=make_lines(self.d)
        out=display_groups(pd.DataFrame([line_quality(self.d,l,60) for l in lines.to_dict('records') if l['known_idx']<=60]))
        self.assertEqual(len(out),int((lines.known_idx<=60).sum()))

    def test_display_near_duplicated(self):
        x=pd.DataFrame([
          {**line_quality(self.d,self.l,14),'line_id':'A'},
          {**line_quality(self.d,self.l,14),'line_id':'B','line_price':105.9},
          {**line_quality(self.d,self.l,14),'line_id':'C','line_price':120.0}])
        a=display_groups(x)
        self.assertEqual(len(a),3)
        self.assertEqual(int(a.is_display_representative.sum()),2)
        self.assertEqual(int(a.display_group.nunique()),2)

    def test_separate_opposite_line(self):
        x=line_quality(self.d,self.l,15);y={**x,'line_id':'Lower','kind':'LOWER'}
        r=display_groups(pd.DataFrame([x,y]))
        self.assertEqual(r.display_group.nunique(),2)

    def test_empty_group(self):
        self.assertTrue(display_groups(pd.DataFrame()).empty)


class TrianglePITTests(unittest.TestCase):
    def setUp(self):
        self.d=price_frame(45)
        self.upper=ldef(10,'UPPER',i1=2,i2=8,v1=107,v2=104)
        self.lower=ldef(12,'LOWER','WICK_LOW_PIVOT2',i1=3,i2=10,v1=93,v2=95)
        self.lower['line_id']='L2';self.lower['known_idx']=12
        self.tri={'idx':12,'upper_id':'L1','lower_id':'L2','date':str(self.d.iloc[12].date.date()),
                  'upper_a1':self.upper['a1'],'upper_a2':self.upper['a2'],
                  'lower_a1':self.lower['a1'],'lower_a2':self.lower['a2'],
                  'shape':'CONVERGING','price_position_at_first_known':'INSIDE',
                  'apex_index_estimate':23.5}
        self.ids={'L1':self.upper,'L2':self.lower}

    def test_no_premature_triangle(self):
        with self.assertRaises(ValueError):triangle_quality(self.d,self.tri,self.ids,11)

    def test_first_known_no_prior_inside(self):
        self.assertEqual(triangle_quality(self.d,self.tri,self.ids,12)['state_PIT'],'INSUFFICIENT_PRIOR_INSIDE_BARS')

    def test_future_tamper_unchanged(self):
        old=triangle_quality(self.d,self.tri,self.ids,15)
        altered=self.d.copy();altered.loc[16:,['open','high','low','close']]*=1.2
        self.assertEqual(old,triangle_quality(altered,self.tri,self.ids,15))

    def test_already_outside_not_breakout(self):
        t=self.tri.copy();t['price_position_at_first_known']='ABOVE_TOP'
        self.assertEqual(triangle_quality(self.d,t,self.ids,15)['state_PIT'],
                         'ALREADY_OUTSIDE_AT_FIRST_CONFIRMATION')

    def test_apex_near(self):
        t=self.tri.copy();t['apex_index_estimate']=15.5
        self.assertEqual(triangle_quality(self.d,t,self.ids,14)['state_PIT'],
                         'APEX_TOO_CLOSE_FOR_NEW_BREAKOUT')

    def test_post_apex(self):
        self.assertEqual(triangle_quality(self.d,self.tri,self.ids,24)['state_PIT'],
                         'APEX_PASSED_OR_INVALID')

    def test_r106_geometry_may_be_many_but_no_future_use(self):
        l=make_lines(self.d)
        t=triangles(self.d,l)
        if len(t):
            row=t.iloc[0].to_dict();m={x['line_id']:x for x in l.to_dict('records')}
            out=triangle_quality(self.d,row,m,int(row['idx']))
            self.assertEqual(out['asof'],row['date'])

    def test_expost_short(self):
        v=apex_limited_outcome(self.d,self.tri,self.ids,18,horizon=20)
        self.assertTrue(v['expost_full'])
        self.assertLess(v['pre_apex_days_EXPOST'],20)

    def test_expost_censor(self):
        v=apex_limited_outcome(self.d,self.tri,self.ids,30,horizon=20)
        self.assertFalse(v['expost_full'])

    def test_rolling_context_future_invariant(self):
        d=price_frame(600);a=enrich_asof(d);q=_snapshot(a,530)
        d2=d.copy();d2[['volume','amount']]=d2[['volume','amount']].astype(float);d2.loc[531:,['open','high','low','close','volume','amount']]*=1.2
        a2=enrich_asof(d2)
        self.assertEqual(q,_snapshot(a2,530))

    def test_width_and_MA(self):
        d=enrich_asof(price_frame(600))
        self.assertTrue(pd.notna(d.iloc[-1].ma448))
        self.assertGreater(d.iloc[-1].bb20_width_pct,0)


if __name__=='__main__':unittest.main()

class RunnerSmokeTests(unittest.TestCase):
    def test_full_pipeline_on_synthetic_source_not_real_stock_results(self):
        from multiline import anchors
        d=price_frame(610)
        code='000001';name='합성'
        first=ldef(10,i1=3,i2=8,v1=143,v2=141)
        second=ldef(12,'LOWER','WICK_LOW_PIVOT2',i1=3,i2=10,v1=136,v2=138)
        first.update(known_idx=540,i1=530,i2=538,slope_krw_per_bar=-.25)
        second.update(known_idx=542,i1=529,i2=540,slope_krw_per_bar=2/11)
        first['line_id']=code+'|'+first['line_id']
        second['line_id']=code+'|L2'
        for l in (first,second):
            l['known_date']=str(d.iloc[l['known_idx']].date.date())
            l['a1']=str(d.iloc[l['i1']].date.date());l['a2']=str(d.iloc[l['i2']].date.date())
        tri={'date':str(d.iloc[542].date.date()),'idx':542,'upper_id':first['line_id'],
             'lower_id':second['line_id'],'upper_a1':first['a1'],'upper_a2':first['a2'],
             'lower_a1':second['a1'],'lower_a2':second['a2'],
             'shape':'CONVERGING','price_position_at_first_known':'INSIDE',
             'apex_index_estimate':574.3}
        row=dict(first);row.update(code=code,name=name);row2=dict(second);row2.update(code=code,name=name)
        dt=str(d.iloc[554].date.date())
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp);src=p/'source';src.mkdir();dest=p/'out'
            pd.DataFrame([row,row2]).to_csv(src/'R1D106_ALL_IMMUTABLE_LINES.csv',index=False)
            pd.DataFrame([{**tri,'code':code,'name':name}]).to_csv(src/'R1D106_ALL_TRIANGLE_CANDIDATES_PIT.csv',index=False)
            report=run(output=dest,base=src,raw_provider=lambda c:d.copy(),plot=False,
                 case_refs=[(code,name,dt,'SYNTHETIC')],user_refs=[])
            self.assertEqual(report['original_line_count'],2)
            self.assertEqual(report['original_triangle_count'],1)
            self.assertTrue(report['input_hashes_verified_unchanged'])
            self.assertEqual(report['context_case_count'],1)
            self.assertTrue((dest/'R1D107_AUDIT.json').is_file())
            self.assertEqual(len(pd.read_csv(dest/'R1D107_ALL_LINE_CREATION_QUALITY_PIT.csv')),2)


class GitHubProductionImportRegressionTests(unittest.TestCase):
    def test_import_search_paths_include_real_r104_dependency(self):
        import research_r1d107 as r107
        from pathlib import Path
        path=Path(r107.ROOT/'r1d104').resolve()
        self.assertIn(str(path), sys.path,
            'R1-D107 production imports research_r1d104._verify; path required')

    def test_production_dependency_is_importable_from_repo(self):
        # This is the actual dependency used when raw_provider=None, unlike
        # synthetic run(raw_provider=...) tests which bypass frozen _verify.
        from research_r1d104 import _verify
        self.assertTrue(callable(_verify))
