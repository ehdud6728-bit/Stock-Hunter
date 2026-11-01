import sys,unittest
from pathlib import Path
import pandas as pd
HERE=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(HERE))
from recency_dedup import describe_line,enrich,presentation_groups,assert_unmodified,is_similar


def prices(n=80):
    days=pd.bdate_range('2024-01-01',periods=n)
    return pd.DataFrame({'date':days,'idx':range(n),'open':[99.]*n,
                         'high':[102.]*n,'low':[99.]*n,'close':[101.]*n})


def line(d,kind='RESISTANCE',status='BREAKOUT_RETEST_SUPPORT_FLIP_WATCH',bucket='WATCH',
         first=9,last=9,line_id='L1',score=30.,tier='SMALL',code='ABC',h1=100.,h2=100.):
    def day(i):return str(d.iloc[i].date.date()) if i is not None else ''
    return dict(code=code,asof=day(len(d)-1),kind=kind,tier=tier,
                h1_idx=1,h2_idx=5,h2_confirm_idx=7,
                h1_value=h1,h2_value=h2,close_asof=101.,line_asof=h2,
                slope_pct_per_bar=0.,rank_score=score,line_id=line_id,
                status=status,bucket=bucket,fresh_break_today=False,
                eligible_primary=bucket=='PRIMARY',eligible_watch=bucket=='WATCH',
                first_event_date=day(first),last_event_date=day(last),
                structure_flags='')

class R1D99NonExpiringWatchTest(unittest.TestCase):
    def test_old_watch_preserved_80_bars(self):
        d=prices();r=line(d);a=dict(r,**describe_line(r,d,r['asof']))
        self.assertEqual(a['bucket'],'WATCH');self.assertEqual(a['d99_bucket'],'WATCH')
        self.assertEqual(a['status'],r['status']);self.assertTrue(a['d99_kept_without_expiry'])
    def test_old_watch_preserved_200_bars(self):
        d=prices(200);r=line(d);a=dict(r,**describe_line(r,d,r['asof']))
        self.assertEqual(a['d99_bucket'],'WATCH')
    def test_all_later_touches_are_counted_even_after_20_bars(self):
        d=prices(80);r=line(d)
        a=describe_line(r,d,r['asof'])
        self.assertEqual(a['d99_last_contact_age_bars'],0)
        self.assertEqual(a['d99_contact_count_all'],70)
    def test_no_confirmed_support_from_touch(self):
        d=prices();a=describe_line(line(d),d,str(d.iloc[-1].date.date()))
        self.assertFalse(a['d99_support_flip_proven'])
    def test_recent_recross_is_not_first_fresh(self):
        d=prices(80);r=line(d,last=79);a=describe_line(r,d,r['asof'])
        self.assertTrue(a['d99_repeat_cross_recorded']);self.assertFalse(a['d99_repeat_cross_is_fresh'])
        self.assertEqual(a['d99_last_cross_age_bars'],0)
    def test_primary_status_never_changes(self):
        d=prices();r=line(d,bucket='PRIMARY',status='ACTIVE_SUPPORT',kind='SUPPORT',first=None,last=None)
        a=enrich([r],d,r['asof']);assert_unmodified([r],a)
        self.assertEqual(a[0]['d99_bucket'],'PRIMARY')
    def test_broken_support_archive_kept(self):
        d=prices();r=line(d,bucket='ARCHIVE',status='BROKEN_SUPPORT_BELOW',kind='SUPPORT')
        a=enrich([r],d,r['asof']);assert_unmodified([r],a)
        self.assertEqual(a[0]['d99_bucket'],'ARCHIVE')
    def test_support_reclaim_recorded(self):
        d=prices(80);d.loc[44,'close']=97.;d.loc[45,'close']=101.
        r=line(d,kind='SUPPORT',bucket='WATCH',status='BROKEN_SUPPORT_RECLAIM_WATCH',first=10,last=44)
        a=describe_line(r,d,r['asof']);self.assertGreaterEqual(a['d99_reclaim_count_all'],1)
        self.assertEqual(a['d99_last_reclaim_age_bars'],34)
    def test_future_mutation_cannot_change_asof(self):
        d=prices();asof=str(d.iloc[45].date.date());r=line(d,last=40)
        a=enrich([r],d,asof)
        changed=d.copy();changed.loc[46:,'close']=1450.;changed.loc[46:,'high']=1500.
        b=enrich([r],changed,asof)
        self.assertEqual(a,b)
    def test_future_event_rejected(self):
        d=prices(80);r=line(d,last=70)
        asof=str(d.iloc[40].date.date())
        with self.assertRaises(ValueError):describe_line(r,d,asof)
    def test_unconfirmed_line_rejected(self):
        d=prices();r=line(d);r['h2_confirm_idx']=len(d)
        with self.assertRaises(ValueError):describe_line(r,d,r['asof'])
    def test_duplicate_dates_rejected(self):
        d=prices();r=line(d);d.loc[10,'date']=d.loc[9,'date']
        with self.assertRaises(ValueError):enrich([r],d,r['asof'])
    def test_unsorted_dates_rejected(self):
        d=prices();r=line(d);d.loc[10,'date']=d.loc[11,'date']+pd.Timedelta(days=2)
        with self.assertRaises(ValueError):enrich([r],d,r['asof'])
    def test_similar_lines_grouped_not_removed(self):
        d=prices();r=line(d);b=dict(r,line_id='L2',line_asof=100.2,slope_pct_per_bar=.02,rank_score=29.)
        groups=presentation_groups([r,b]);self.assertEqual(len(groups),2)
        self.assertEqual(groups[1]['representative_line_id'],'L1')
        self.assertEqual(groups[1]['decision'],'DISPLAY_GROUP_MEMBER_NO_REMOVAL')
        self.assertEqual(groups[1]['group_size'],2)
        self.assertTrue(groups[1]['preserved_in_watch'])
    def test_distinct_status_not_grouped(self):
        d=prices();r=line(d);b=dict(r,line_id='L2',status='FIRST_BREAK_TODAY')
        self.assertFalse(is_similar(r,b))
    def test_distinct_tier_not_grouped(self):
        d=prices();r=line(d);b=dict(r,line_id='L2',tier='MIDDLE')
        self.assertFalse(is_similar(r,b))
    def test_distinct_bucket_not_grouped(self):
        d=prices();r=line(d);b=dict(r,line_id='L2',bucket='PRIMARY')
        self.assertFalse(is_similar(r,b))
    def test_distinct_stock_not_grouped(self):
        d=prices();r=line(d);b=dict(r,line_id='L2',code='XYZ')
        self.assertFalse(is_similar(r,b))
    def test_distinct_asof_not_grouped(self):
        d=prices();r=line(d);b=dict(r,line_id='L2',asof='2023-12-31')
        self.assertFalse(is_similar(r,b))
    def test_different_price_not_grouped(self):
        d=prices();r=line(d);b=dict(r,line_id='L2',line_asof=103.)
        self.assertFalse(is_similar(r,b))
    def test_different_slope_not_grouped(self):
        d=prices();r=line(d);b=dict(r,line_id='L2',slope_pct_per_bar=.3)
        self.assertFalse(is_similar(r,b))
    def test_original_fields_immutable(self):
        d=prices();r=line(d);a=enrich([r],d,r['asof'])
        assert_unmodified([r],a)
        a[0]['bucket']='ARCHIVE'
        with self.assertRaises(AssertionError):assert_unmodified([r],a)
    def test_no_changing_freshness_state(self):
        d=prices(50);r=line(d,status='FIRST_BREAK_TODAY',first=49,last=49)
        r['fresh_break_today']=True;a=enrich([r],d,r['asof']);assert_unmodified([r],a)
        self.assertTrue(a[0]['fresh_break_today'])
    def test_no_freshness_forgery_for_old_watch(self):
        d=prices();r=line(d);a=enrich([r],d,r['asof'])[0]
        self.assertFalse(a['d99_repeat_cross_is_fresh'])
    def test_empty_events_allowed(self):
        d=prices();r=line(d,first=None,last=None)
        a=describe_line(r,d,r['asof'])
        self.assertEqual(a['d99_bucket'],'WATCH');self.assertIsNone(a['d99_last_contact_age_bars'])

if __name__=='__main__':unittest.main()
