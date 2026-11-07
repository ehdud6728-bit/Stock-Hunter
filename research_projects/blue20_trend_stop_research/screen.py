"""Post-hoc source discontinuity sensitivity, never an entry filter."""
import argparse
from pathlib import Path
import pandas as pd
from analyze import HERE,loader

def main(manifest):
    out=HERE/'results';d=loader.load_prices(Path(manifest));gaps=pd.read_csv(out/'SOURCE_DISCONTINUITIES.csv',dtype={'code':str})
    f=pd.read_csv(out/'TREND_FEATURES.csv.gz',dtype={'code':str});t=pd.read_csv(out/'TRADES.csv.gz',dtype={'code':str})
    keys=set();evidence=[]
    for e in f.itertuples():
        p=d[e.code];j=int(p.index[p.date.eq(e.date)][0]);start=p.date.iloc[max(0,j-120)];end=p.date.iloc[j+20]
        g=gaps[(gaps.code==e.code)&gaps.date.between(start,end)]
        for row in g.to_dict('records'):evidence.append(dict(event_date=e.date,band=e.band,**row))
        if len(g):keys.add((e.code,e.date,e.band))
    pd.DataFrame(evidence).to_csv(out/'EVENT_DISCONTINUITY_LINKS.csv',index=False)
    a=t[[tuple(x) not in keys for x in t[['code','date','band']].to_numpy()]]
    records=[]
    for key,g in a.groupby(['timing','stop','buffer_atr','mode','year','band']):
        b=g[g.eligible];records.append(dict(zip(['timing','stop','buffer_atr','mode','year','band'],key),opportunities=len(g),entries=len(b),mean_net=b.net.mean(),mean_distance=b.distance_pct.mean(),mean_R=b.net_R.mean(),worst_net=b.net.min(),stop_rate=100*b.reason.str.contains('SL|CLOSE_BREAK').mean()))
    pd.DataFrame(records).to_csv(out/'NO_DISCONTINUITY_SUMMARY.csv',index=False)
    paired=[]
    for subset,df in [('ALL',t),('NO_LARGE_DISCONTINUITY',a),('GC0',t[t.gc_offset.eq(0)]),('LONG_BULL',t[t.long_bull])]:
        df=df[df.buffer_atr.eq(0)&df['mode'].eq('INTRADAY')]
        keys0=['code','date','band','timing'];base=df[df.stop.eq('FIXED5')][keys0+['net','example_account_net']]
        for stop in ['PULLBACK','ORIGIN','RESISTANCE']:
            b=df[df.stop.eq(stop)&df.eligible].merge(base,on=keys0,suffixes=('_structure','_fixed'),validate='one_to_one')
            for (timing,year,band),g in b.groupby(['timing','year','band']):
                paired.append(dict(subset=subset,stop=stop,timing=timing,year=year,band=band,n=len(g),
                    fixed_net=g.net_fixed.mean(),structure_net=g.net_structure.mean(),delta_net=(g.net_structure-g.net_fixed).mean(),
                    fixed_example_account_net=g.example_account_net_fixed.mean(),structure_example_account_net=g.example_account_net_structure.mean()))
    pd.DataFrame(paired).to_csv(out/'PAIRED_COMPARISONS.csv',index=False)
    pre=[]
    for e in t[t.buffer_atr.eq(0)&t['mode'].eq('INTRADAY')&~t.stop.eq('FIXED5')].itertuples():
        p0=d[e.code];history=p0[p0.date.ge(e.date)&p0.date.lt(e.entry_date)]
        pre.append(dict(code=e.code,date=e.date,band=e.band,timing=e.timing,stop=e.stop,eligible=e.eligible,
                        d0_to_preentry_low_hold=bool(history.low.ge(e.line).all()) if pd.notna(e.line) else None,
                        d0_to_preentry_close_hold=bool(history.close.ge(e.line).all()) if pd.notna(e.line) else None))
    pd.DataFrame(pre).to_csv(out/'PREENTRY_HOLD.csv.gz',index=False)
    p=out/'REPORT_KO.md';text=p.read_text(encoding='utf-8');text+=f'\n\n## 추가 원천 단절 제외 민감도\n\n42개 큰 시가 단절 단서 중 D0−120~D0+20에 걸리는 사건 {len(keys)}건을 사후 제외한 NO_DISCONTINUITY_SUMMARY.csv를 제공한다. 이는 미래 데이터 품질 사건을 아는 진입 필터가 아니라 원천 위험 민감도다. 작은 기업행사·배당·유상증자 조정까지 인증하지 않으므로 제외 후에도 경제적 성과는 미확정이다.\n';p.write_text(text,encoding='utf-8')
    print({'flagged_events':len(keys),'remaining_events':len(f)-len(keys)})
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--manifest',required=True);main(p.parse_args().manifest)
