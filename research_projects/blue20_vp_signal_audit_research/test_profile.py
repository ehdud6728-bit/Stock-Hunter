import unittest
import numpy as np
import pandas as pd
from profile_core import snapshot,edges_for,extend_edges,profile,features,query_band,partial_rebase

class ProfileTests(unittest.TestCase):
    def bars(self):
        return pd.DataFrame(dict(date=['2023-01-01','2023-01-02','2023-01-03','2023-01-04'],open=[10.,12,14,16],low=[10.,11,13,15],high=[12.,13,15,18],close=[12.,13,15,18],volume=[20.,5,10,30]))
    def test_exact_cutoff_and_window(self):
        d=self.bars();self.assertEqual(list(snapshot(d,2,2,'PAST_ONLY').date),['2023-01-01','2023-01-02'])
        self.assertEqual(list(snapshot(d,2,2,'SIGNAL_CLOSE').date),['2023-01-02','2023-01-03'])
        with self.assertRaises(ValueError):snapshot(d,0,2,'PAST_ONLY')
    def test_future_mutation_no_effect(self):
        d=self.bars();w=snapshot(d,2,2,'SIGNAL_CLOSE');p=profile(w,edges_for(w,4),'range')
        d.loc[3,['open','low','high','close','volume']]=[1000,999,9999,9999,1e10]
        w2=snapshot(d,2,2,'SIGNAL_CLOSE');np.testing.assert_array_equal(p,profile(w2,edges_for(w2,4),'range'))
    def test_hand_example_and_value_area(self):
        w=pd.DataFrame(dict(open=[10.,12],low=[10.,12],high=[12.,12],close=[12.,12],volume=[20.,5]))
        p=profile(w,np.array([10.,11,12]),'range');np.testing.assert_allclose(p,[10,15])
        f=features(p,np.array([10.,11,12]),11.5);self.assertEqual((f['poc'],f['val'],f['vah']),(11.5,10,12))
        self.assertEqual(query_band(w,11,12,'range'),15)
    def test_all_methods_conserve_volume_and_last_boundary(self):
        d=self.bars();edges=edges_for(d,3)
        for method in ['range','typical','close','ohlc4']:self.assertAlmostEqual(profile(d,edges,method).sum(),65)
    def test_point_method_is_different_proxy(self):
        w=self.bars().iloc[:1];e=np.array([10.,11,12])
        np.testing.assert_allclose(profile(w,e,'range'),[10,10]);np.testing.assert_allclose(profile(w,e,'close'),[0,20])
    def test_no_silent_out_of_range_clipping(self):
        d=self.bars()
        with self.assertRaises(ValueError):profile(d,np.array([11.,14]),'range')
        ed=extend_edges(np.array([11.,12,13]),d);self.assertLessEqual(ed[0],10);self.assertGreaterEqual(ed[-1],18)
        self.assertAlmostEqual(profile(d,ed,'range').sum(),65)
    def test_decomposition_mass(self):
        d=self.bars();old=snapshot(d,2,2,'PAST_ONLY');new=snapshot(d,2,2,'SIGNAL_CLOSE');ed=extend_edges(edges_for(old,3),new)
        a=profile(old,ed,'range');removed=profile(old.iloc[:1],ed,'range');added=profile(d.iloc[2:3],ed,'range')
        np.testing.assert_allclose(a-removed+added,profile(new,ed,'range'))
    def test_ties_choose_first_poc_and_contiguous_value_area(self):
        f=features(np.array([10.,10]),np.array([10.,11,12]),12.5)
        self.assertEqual(f['poc'],10.5);self.assertEqual(f['peak_position'],'BELOW_CLOSE')
    def test_no_future_action_or_late_discovery(self):
        d=self.bars();actions=[dict(effective_date='2023-01-03',known_date='2023-01-02',previous_close=20,reference=10)]
        a,used=partial_rebase(d,'2023-01-02',actions);pd.testing.assert_frame_equal(a,d);self.assertEqual(used,[])
        actions[0]['known_date']='2023-01-05';a,used=partial_rebase(d,'2023-01-04',actions);pd.testing.assert_frame_equal(a,d)
    def test_partial_scenario_keeps_raw_volume(self):
        d=self.bars();a,_=partial_rebase(d,'2023-01-04',[dict(effective_date='2023-01-03',known_date='2023-01-02',previous_close=20,reference=10)])
        np.testing.assert_array_equal(a.volume,d.volume);self.assertEqual(a.open.iloc[0],5)
    def test_bad_inputs_rejected(self):
        d=self.bars();d.loc[0,'volume']=-1
        with self.assertRaises(ValueError):edges_for(d,3)

if __name__=='__main__':unittest.main()
