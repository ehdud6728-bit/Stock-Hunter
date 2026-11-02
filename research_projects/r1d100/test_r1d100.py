import sys,unittest
from pathlib import Path
import pandas as pd
HERE=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(HERE))
from dynamic_structure import index_at,pivots,_pair,structure,ma_report,timeline,FLAT_PCT


def sample(n=590):
    dates=pd.bdate_range('2022-01-03',periods=n)
    close=[100+i*.025 for i in range(n)]
    return pd.DataFrame({'date':dates,'idx':range(n),'open':close.copy(),
      'high':[v+1 for v in close],'low':[v-1 for v in close],
      'close':close,'amount':[1e8]*n,'volume':[10000]*n})


def shape():
    d=sample()
    # isolated, robust swing peaks/troughs, including a later upward re-anchor
    for idx,price in [(510,135),(523,128),(540,139),(555,145),(570,140)]:
        d.loc[idx,'close']=price
    for idx,price in [(511,98),(528,101),(541,106),(559,109),(571,111)]:
        d.loc[idx,'low']=price
    return d


def day(d,i):return str(d.iloc[i].date.date())

class TestR1D100(unittest.TestCase):
    def test_01_index_sorted(self):
        d=sample();self.assertEqual(index_at(d,day(d,500)),500)
    def test_02_future_pivot_confirmation_delay(self):
        d=shape();p=pivots(d,556,'HIGH');self.assertTrue(all(x['confirm_idx']<=556 for x in p))
        self.assertNotIn(555,[x['idx'] for x in p])  # 555 only confirms on 557
        self.assertIn(555,[x['idx'] for x in pivots(d,557,'HIGH')])
    def test_03_high_uses_close_low_uses_low(self):
        d=shape();self.assertTrue(any(x['idx']==540 and x['value']==139 for x in pivots(d,543,'HIGH')))
        self.assertTrue(any(x['idx']==541 and x['value']==106 for x in pivots(d,545,'LOW')))
    def test_04_ma_values(self):
        d=sample();r=ma_report(d,day(d,555))
        self.assertAlmostEqual(r['MA112'],d.close.iloc[444:556].mean(),places=2)
        self.assertTrue(r['MA112_slope20_pct']>0)
        self.assertTrue(r['MA224_slope20_pct']>0)
        self.assertTrue(r['MA448_slope20_pct']>0)
    def test_05_ma_slope_changes_without_future(self):
        d=sample();t=555;original=ma_report(d,day(d,t));d.loc[t+1:,'close']*=7
        self.assertEqual(original,ma_report(d,day(d,t)))
    def test_06_ma_spread_direction(self):
        d=sample();x=ma_report(d,day(d,555))
        self.assertGreater(x['MA112_to_MA224_spread_pct'],0)
    def test_07_ma_price_gap(self):
        d=sample();r=ma_report(d,day(d,555));self.assertGreater(r['close_to_MA112_pct'],0)
    def test_08_short_ascending_line_supported(self):
        d=shape();p=pivots(d,557,'HIGH');pair=_pair(p,557,'SHORT');self.assertIsNotNone(pair)
        self.assertEqual(pair['direction'],'UP')
    def test_09_short_descending_line_supported(self):
        d=shape();d.loc[555,'close']=125
        p=pivots(d,557,'HIGH');q=_pair(p,557,'SHORT');self.assertEqual(q['direction'],'DOWN')
    def test_10_low_pair_selected(self):
        d=shape();p=pivots(d,564,'LOW');q=_pair(p,564,'SHORT');self.assertEqual(q['kind'],'LOW')
        self.assertEqual(q['direction'],'UP')
    def test_11_no_future_change_to_structure(self):
        d=shape();asof=day(d,563);old=structure(d,asof)
        d.loc[564:,'close']*=3;d.loc[564:,'low']*=.1
        self.assertEqual(old,structure(d,asof))
    def test_12_anchored_at_confirmed_date(self):
        d=shape();r=structure(d,day(d,557))
        self.assertTrue(all(x['h2_confirm_idx']<=557 for x in r['lines']))
    def test_13_reanchor_emitted(self):
        d=shape();records=timeline(d,day(d,578),lookback=50)
        self.assertTrue(any(x['reason']=='NEW_CONFIRMED_SECOND_PIVOT' for x in records))
    def test_14_no_data_no_fake_ma(self):
        d=sample(110);r=ma_report(d,day(d,109))
        self.assertEqual(r['ma_order'],'MISSING_MA');self.assertIsNone(r['MA112'])
    def test_15_missing_asof_rejected(self):
        d=sample();self.assertRaises(ValueError,ma_report,d,'2035-01-01')
    def test_16_duplicate_asof_rejected(self):
        d=sample();d.loc[200,'date']=d.loc[199,'date'];self.assertRaises(ValueError,index_at,d,day(d,500))
    def test_17_unsorted_rejected(self):
        d=sample();d.loc[210,'date']=d.loc[211,'date']+pd.Timedelta(days=1)
        self.assertRaises(ValueError,index_at,d,day(d,500))
    def test_18_invalid_pivot_kind_rejected(self):
        d=sample();self.assertRaises(ValueError,pivots,d,550,'INVALID')
    def test_19_not_trading_signal(self):
        d=shape();r=ma_report(d,day(d,565));self.assertNotIn('buy_signal',r)
    def test_20_new_pivot_updates_without_rewriting_history(self):
        d=shape();before=structure(d,day(d,550));after=structure(d,day(d,557))
        first={(x['kind'],x['tier']):x['line_id'] for x in before['lines']}
        last={(x['kind'],x['tier']):x['line_id'] for x in after['lines']}
        self.assertNotEqual(first.get(('HIGH','SHORT')),last.get(('HIGH','SHORT')))

if __name__=='__main__':unittest.main()
