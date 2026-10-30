import sys,unittest,copy
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'research_formulas'))
from r1d65_strict_triangle import geometry,pivots,RULE

T=300

def fixture():
    n=350
    o=[119.0]*n;h=[120.0]*n;l=[118.0]*n;c=[119.0]*n;a=[10.0]*n
    for j in range(235,301):o[j]=100;h[j]=101;l[j]=99;c[j]=100
    for j in range(260,300):h[j]=102;l[j]=98
    for j,(op,hi,lo,cl) in {272:(110,114,109,112),286:(105,109,104,107),
                             276:(95,96,88,95),291:(98,99,94,98),
                             258:(82,84,80,82),299:(102,103,98,102),
                             300:(106,107,105,106.5)}.items():
        o[j],h[j],l[j],c[j]=op,hi,lo,cl
    a[300]=16
    return dict(open=o,high=h,low=l,close=c,amount=a)

class StrictGeometry(unittest.TestCase):
    def test_valid_alternating_triangle(self):
        z=geometry(fixture(),T)
        self.assertEqual(z['reason'],'PASS_STRICT_TRIANGLE',z)
        self.assertEqual((z['H1_ix'],z['L1_ix'],z['H2_ix'],z['L2_ix']),(272,276,286,291))
        self.assertTrue(z['qualified_with_amount'])
    def test_future_values_never_change_signal(self):
        v=fixture();first=geometry(v,T)
        for k in ('open','high','low','close','amount'):v[k][T+1:]=[9999.0]*len(v[k][T+1:])
        self.assertEqual(geometry(v,T),first)
    def test_signal_bar_must_not_move_pivots(self):
        v=fixture();a=geometry(v,T);v['high'][T]=500
        b=geometry(v,T)
        self.assertEqual(a['H2_ix'],b['H2_ix']);self.assertEqual(a['upper_line_t'],b['upper_line_t'])
    def test_no_recent_price_breakout(self):
        v=fixture();v['open'][T]=102;v['high'][T]=103;v['low'][T]=100;v['close'][T]=102
        self.assertEqual(geometry(v,T)['reason'],'NO_FRESH_CLOSE_BREAKOUT')
    def test_not_below224(self):
        v=fixture();r=geometry(v,T,ma224=90)
        self.assertEqual(r['reason'],'NOT_BELOW224')
    def test_no_amount_reentry(self):
        v=fixture();v['amount'][T]=9
        self.assertEqual(geometry(v,T)['reason'],'NO_AMOUNT_REENTRY')
    def test_non_alt_pivots_rejected(self):
        # Geometry alone cannot pass if last four pivot chronology is H H L L.
        v=fixture()
        # Force the late 286 high pivot to be absent and add a higher late
        # local high at 297, outside existing chronological ordering.
        v['open'][297]=109;v['high'][297]=116;v['low'][297]=101;v['close'][297]=108
        r=geometry(v,T)
        self.assertNotEqual(r['reason'],'PASS_STRICT_TRIANGLE')
    def test_future_pivot_tminus2_unconfirmed(self):
        v=fixture();v['high'][T-2]=160;v['close'][T-2]=101
        r=geometry(v,T)
        self.assertNotEqual(r.get('H2_ix'),T-2)
    def test_no_madeup_price_on_invalid_ohlc(self):
        v=fixture();v['high'][T]=100
        self.assertEqual(geometry(v,T)['reason'],'INVALID_PRICE_WINDOW')
    def test_missing_ma224_fails_closed(self):
        v=fixture();v={k:val[240:301] for k,val in v.items()};
        self.assertEqual(geometry(v,60,ma224=None)['reason'],'MISSING_ASOF_MA224')
    def test_fixed_asof_line_anchoring(self):
        v=fixture();s=geometry(v,T)
        self.assertAlmostEqual(s['upper_line_t'],104.0,places=4)
    def test_non_triangle_flat(self):
        v=fixture()
        for j in range(T-40,T):
            v['open'][j]=100;v['high'][j]=102;v['low'][j]=98;v['close'][j]=100
        self.assertEqual(geometry(v,T)['reason'],'MISSING_TWO_CONFIRMED_HIGHS_OR_LOWS')

if __name__=='__main__':unittest.main()
