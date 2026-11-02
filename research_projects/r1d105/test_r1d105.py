import sys,unittest
from pathlib import Path
import pandas as pd
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from event_sequences import detect_events,unique_event_dates,episode_anchors,episode_sequence,attach_later_outcomes,collect_snapshots


def _row(idx,*,code='039840',date=None,price=100,ma=110,bb='INSIDE',env='INSIDE',
         high='NO_CROSS',low='NO_CROSS',reanchor=False,amount=0.9,structure='MIXED_OR_FLAT'):
    row={'code':code,'name':'TEST','idx':idx,'date':date or f'2024-01-{idx+1:02d}',
         'close':price,'structure':structure,'amount_vs_prev20':amount}
    for n in (5,20,60,112,224):row.update({f'ma{n}_value':ma,f'ma{n}_slope20_pct':-1})
    for n in (20,30,40):row.update({f'bb{n}_position':bb,f'bb{n}_width_delta5_pp':-1})
    for n in (20,40):row.update({f'env{n}_10_position':env,f'env{n}_10_width_change5_pct':-1})
    for n in ('ma_cloud_fast','ma_cloud_mid','ma_cloud_long','ma_cloud_all','ichimoku_current'):
        row.update({f'{n}_position':'BELOW',f'{n}_thickness_change5_pp':-1,f'{n}_height_change5_pp':-1})
    for key in ('short_high_trend','short_low_trend','medium_high_trend','medium_low_trend'):
        row[key+'_validated_cross']='NO_CROSS';row[key+'_reanchored_today']=False;row[key+'_line_id']='LINE'
    row['short_high_trend_validated_cross']=high
    row['short_low_trend_validated_cross']=low
    row['short_high_trend_reanchored_today']=reanchor
    return row


class EventTests(unittest.TestCase):
    def test_01_own_two_days_ma_reclaim(self):
        z=detect_events(pd.DataFrame([_row(0,price=100),_row(1,price=120)]))
        self.assertEqual(len(z[(z.family=='MA')&(z.signal=='RECLAIM_UP')]),5)
    def test_02_ma_lose(self):
        z=detect_events(pd.DataFrame([_row(0,price=120),_row(1,price=100)]))
        self.assertEqual(len(z[(z.family=='MA')&(z.signal=='LOSE_DOWN')]),5)
    def test_03_trend_up_independent_ma(self):
        z=detect_events(pd.DataFrame([_row(0),_row(1,high='CROSS_UP')]))
        self.assertEqual(len(z[(z.family=='TRENDLINE')&(z.signal=='CROSS_UP')]),1)
        self.assertEqual(len(z[z.family=='MA']),0)
    def test_04_no_fake_reanchor_cross(self):
        z=detect_events(pd.DataFrame([_row(0),_row(1,reanchor=True)]))
        self.assertEqual(len(z[(z.family=='TRENDLINE')&(z.signal=='CROSS_UP')]),0)
        self.assertEqual(len(z[(z.family=='TRENDLINE')&(z.signal=='REANCHOR')]),1)
    def test_05_bb_break_and_env_break_even_without_ma(self):
        z=detect_events(pd.DataFrame([_row(0),_row(1,bb='BELOW_LOWER',env='BELOW_LOWER')]))
        self.assertEqual(len(z[(z.family=='BB')&(z.signal=='BREAK_LOWER')]),3)
        self.assertEqual(len(z[(z.family=='ENV')&(z.signal=='BREAK_LOWER')]),2)
    def test_06_return_from_env_lower(self):
        z=detect_events(pd.DataFrame([_row(0,env='BELOW_LOWER'),_row(1,env='INSIDE')]))
        self.assertEqual(len(z[(z.family=='ENV')&(z.signal=='REENTER_FROM_LOWER')]),2)
    def test_07_bb_width_flip(self):
        a=_row(0);b=_row(1);b['bb20_width_delta5_pp']=3
        z=detect_events(pd.DataFrame([a,b]))
        self.assertEqual(len(z[(z.family=='BB_WIDTH')&(z.signal=='CONTRACT_TO_EXPAND')]),1)
    def test_08_env_absolute_width_flip(self):
        a=_row(0);b=_row(1);b['env20_10_width_change5_pct']=2
        z=detect_events(pd.DataFrame([a,b]))
        self.assertEqual(len(z[(z.family=='ENV_WIDTH')&(z.signal=='SHRINK_TO_GROW_ABSOLUTE')]),1)
    def test_09_all_cloud_height_flips(self):
        a=_row(0);b=_row(1);b['ma_cloud_all_height_change5_pp']=.2
        z=detect_events(pd.DataFrame([a,b]))
        self.assertEqual(len(z[(z.family=='CLOUD_HEIGHT')&(z.component=='ma_cloud_all')]),1)
    def test_10_volume_flow_cross(self):
        z=detect_events(pd.DataFrame([_row(0,amount=.2),_row(1,amount=1.3)]))
        self.assertEqual(len(z[(z.family=='AMOUNT')&(z.signal=='ABOVE_PRIOR_AVG')]),1)
    def test_11_preserve_same_day_multiple_events(self):
        z=detect_events(pd.DataFrame([_row(0),_row(1,high='CROSS_UP',bb='ABOVE_UPPER')]))
        self.assertGreater(len(z),2);self.assertFalse(z.event_id.duplicated().any())
    def test_12_duplicate_stock_date_rejected(self):
        with self.assertRaises(ValueError):detect_events(pd.DataFrame([_row(0),_row(0)]))
    def test_13_no_noncontiguous_transition(self):
        z=detect_events(pd.DataFrame([_row(0,price=90),_row(3,price=120)]));self.assertEqual(len(z),0)
    def test_14_future_tamper_no_change(self):
        base=pd.DataFrame([_row(0),_row(1,bb='BELOW_LOWER'),_row(2,high='CROSS_UP'),_row(3,price=130)])
        e=detect_events(base);copy=base.copy();copy.loc[copy.idx>2,'close']*=1.5
        ee=detect_events(copy)
        self.assertEqual(e[e.idx<=2].event_id.tolist(),ee[ee.idx<=2].event_id.tolist())
    def test_15_all_source_union(self):
        x=pd.DataFrame([_row(0),_row(1,high='CROSS_UP'),_row(2,env='BELOW_LOWER')]);events=detect_events(x)
        z=unique_event_dates(events,x,refs=[('039840','2024-01-01')]);self.assertEqual(set(z.date),{'2024-01-01','2024-01-02','2024-01-03'})
    def test_16_reference_kept_no_event(self):
        x=pd.DataFrame([_row(0),_row(1)]); z=unique_event_dates(pd.DataFrame(columns=['code','date','idx']),x,refs=[('039840','2024-01-01')]);self.assertEqual(len(z),1)
    def test_17_episodes_20bar_sep(self):
        x=pd.DataFrame([_row(i,high='CROSS_UP') for i in range(28)]);z=episode_anchors(detect_events(x),bar_separation=20)
        self.assertEqual(z.idx.tolist(),[1,21])
    def test_18_episode_sequence_time_sides(self):
        x=pd.DataFrame([_row(i,high='CROSS_UP') for i in range(4)]);e=detect_events(x); anchor=episode_anchors(e,bar_separation=2)
        seq=episode_sequence(e,anchor,before=1,after=2)
        self.assertTrue(set(seq.time_side)=={'PIT_HISTORY','POST_EXPOST_ONLY'})
    def test_19_future_result_appended_after(self):
        x=pd.DataFrame([_row(0),_row(1,high='CROSS_UP')]); z=detect_events(x)
        a=episode_anchors(z)
        c=x[['code','date']].copy();c['forward20_close_pct_EXPOST']=[10,-5];c['forward20_peak_pct_EXPOST']=[15,0];c['outcome_EXPOST']=['SURGE','CONTROL']
        y=attach_later_outcomes(a,c);self.assertEqual(y.iloc[0].outcome_EXPOST,'CONTROL')
        self.assertNotIn('outcome_EXPOST',z.columns)
    def test_20_snapshot_contains_no_future_outcomes(self):
        x=pd.DataFrame([_row(i) for i in range(25)]);x['forward20_close_pct_EXPOST']=100
        a=pd.DataFrame([{'code':'039840','date':'2024-01-25','idx':24,'anchor_type':'FIXED_REFERENCE'}])
        s=collect_snapshots(x,a);self.assertEqual(len(s),5)
        self.assertNotIn('forward20_close_pct_EXPOST',s.columns)
    def test_21_reference_first_priority(self):
        x=pd.DataFrame([_row(0),_row(1,high='CROSS_UP')]);e=detect_events(x)
        a=unique_event_dates(e,x,refs=[('039840','2024-01-02')]);self.assertEqual(a.iloc[0].anchor_type,'FIXED_REFERENCE')
    def test_22_all_5_ma_periods_distinct(self):
        x=pd.DataFrame([_row(0,price=90),_row(1,price=120)])
        z=detect_events(x);self.assertEqual(set(z[z.family=='MA'].component),{'MA5','MA20','MA60','MA112','MA224'})
    def test_23_both_upper_and_lower_events(self):
        x=pd.DataFrame([_row(0,bb='BELOW_LOWER'),_row(1,bb='ABOVE_UPPER')]);z=detect_events(x)
        self.assertEqual(len(z[(z.family=='BB') & (z.signal=='GAP_ACROSS_UP')]),3)
    def test_24_no_event_on_first_date(self):
        self.assertEqual(len(detect_events(pd.DataFrame([_row(0,bb='ABOVE_UPPER')]))),0)

if __name__=='__main__':unittest.main()
