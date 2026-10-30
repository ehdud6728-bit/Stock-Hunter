#!/usr/bin/env python3
"""Reconstruct historical raw OHLC charts directly from 8 SHA-locked KRX shards.
Figure is historical data visualization, never adjusted-current-app price.
"""
import csv,gzip,hashlib,io,json,os, pathlib,zipfile
import numpy as np,pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

SHARDS=['/mnt/data/stockhunter-allkrx-price-shard-0.zip']+[f'/mnt/data/stockhunter-allkrx-recovered-{i}.zip' for i in range(1,8)]
CASES=[('BB20 reclaim loss','279060','2024-02-26'),('BB20 reclaim gain','214420','2025-02-04'),
('Envelope20 10pct loss','099220','2024-06-03'),('Envelope20 20pct loss','032980','2025-01-14'),
('Envelope40 10pct loss','279060','2025-03-25'),('Envelope40 10pct gain','348080','2025-04-09'),
('Envelope40 40pct loss','288330','2024-05-27')]

def source(shards):
    f={}; opened=[]
    for p in shards:
        z=zipfile.ZipFile(p);opened.append(z)
        mfname=next(n for n in z.namelist() if 'manifest_shard' in n and n.endswith('.json'))
        mf=json.loads(z.read(mfname));ns=f"shard_{int(mf['shard']):02d}/"
        for day in mf['files']:
            d=day['date']; assert d not in f
            f[d]=(z,ns+day['file'],day['sha256'])
    assert len(f)==1907
    return f,opened

def get_prices(shards,cases):
    index,opened=source(shards);days=sorted(index);needed={}
    for _,code,date in cases:
        t=days.index(date)
        for dt in days[max(0,t-95):t+21]:needed.setdefault(dt,set()).add(code)
    values={code:[] for _,code,_ in cases}
    try:
        for d,codes in sorted(needed.items()):
            z,p,digest=index[d];b=z.read(p)
            assert hashlib.sha256(b).hexdigest()==digest,'DAY_SHA_MISMATCH '+d
            rows=csv.DictReader(io.StringIO(gzip.decompress(b).decode()))
            for r in rows:
                if r['code'] in codes:
                    dct={'date':d,'code':r['code']}
                    for k in ['open','high','low','close','amount']:dct[k]=float(r[k])
                    values[r['code']].append(dct)
    finally:
        for z in opened:z.close()
    return {code:pd.DataFrame(v).sort_values('date').drop_duplicates('date').reset_index(drop=True) for code,v in values.items()}

def make_charts(cases,prices,out):
    out=pathlib.Path(out);out.mkdir(parents=True,exist_ok=True)
    audit=[];paths=[]
    for tag,code,date in cases:
        df=prices[code].copy()
        if date not in df.date.values:continue
        t=df.index[df.date.eq(date)][0]
        if t<40 or t+20>=len(df):continue
        # actual chronological ticker rows, no forward close to compute today's indicator
        c=df.close.astype(float)
        df['MA20']=c.rolling(20,min_periods=20).mean()
        df['MA40']=c.rolling(40,min_periods=40).mean()
        df['BB20_L']=df.MA20-2*c.rolling(20,min_periods=20).std(ddof=0)
        df['BB40_L']=df.MA40-2*c.rolling(40,min_periods=40).std(ddof=0)
        df['ENV20_10_L']=df.MA20*.9
        df['ENV20_20_L']=df.MA20*.8
        df['ENV40_10_L']=df.MA40*.9
        df['ENV40_40_L']=df.MA40*.6
        i0=max(0,t-52);i1=t+21;view=df.iloc[i0:i1]
        fig,ax=plt.subplots(figsize=(12,5),dpi=125)
        x=np.arange(len(view));cur=t-i0
        ax.vlines(x,view.low,view.high,alpha=.34,linewidth=1.2,label='Daily high-low')
        ax.plot(x,view.close,linewidth=1.4,label='Close (KRX raw)')
        if tag.startswith('BB'):
            ax.plot(x,view['BB20_L'],linewidth=1.4,label='BB20 lower (2 sigma, ddof0)')
            ax.plot(x,view['BB40_L'],linewidth=1.2,label='BB40 lower (2 sigma, ddof0)')
        elif 'Envelope20' in tag:
            ax.plot(x,view.ENV20_10_L,linewidth=1.3,label='Env20 lower -10%')
            ax.plot(x,view.ENV20_20_L,linewidth=1.3,label='Env20 lower -20%')
        else:
            ax.plot(x,view.ENV40_10_L,linewidth=1.3,label='Env40 lower -10%')
            ax.plot(x,view.ENV40_40_L,linewidth=1.3,label='Env40 lower -40%')
        ax.axvline(cur,linestyle='--',linewidth=1.5,label='Signal: '+date)
        ax.scatter([cur,cur+1],[df.loc[t,'close'],df.loc[t+1,'open']],s=[40,55],marker='o',zorder=4)
        n=min(len(view)-1,cur+20)
        ax.scatter([n],[df.loc[t+20,'close']],s=55,marker='x',zorder=5,label='D20 close')
        idx=list(range(0,len(view),max(1,len(view)//8)))
        if cur not in idx:idx.append(cur)
        ax.set_xticks(sorted(idx));ax.set_xticklabels([view.iloc[j].date[5:] for j in sorted(idx)],rotation=45,fontsize=8)
        ax.set_title(f'{code}  |  {tag}  |  Signal {date}  |  KRX raw price (CA unverified)',fontsize=10)
        ax.grid(True,alpha=.2);ax.legend(fontsize=7,loc='best');ax.set_ylabel('KRW')
        fig.tight_layout()
        fname=f'{code}_{date}_{tag.replace(" ","_")}.png';fig.savefig(out/fname);plt.close(fig);paths.append(str(out/fname))
        record={'code':code,'date':date,'strategy':tag,'raw_close_t':float(df.loc[t,'close']),
           'raw_open_d1':float(df.loc[t+1,'open']),'raw_close_d20':float(df.loc[t+20,'close']),
           'BB20_lower_t':float(df.loc[t,'BB20_L']),'BB40_lower_t':float(df.loc[t,'BB40_L']),
           'ENV20_10_lower_t':float(df.loc[t,'ENV20_10_L']),'ENV20_20_lower_t':float(df.loc[t,'ENV20_20_L']),
           'ENV40_10_lower_t':float(df.loc[t,'ENV40_10_L']),'ENV40_40_lower_t':float(df.loc[t,'ENV40_40_L']),
           'MA20_t':float(df.loc[t,'MA20']),'MA40_t':float(df.loc[t,'MA40']),
           'D20_raw_close_from_entry_pct':100*(df.loc[t+20,'close']/df.loc[t+1,'open']-1),
           'prior20_high':float(df.loc[t-20:t-1,'high'].max()),'prior20_low':float(df.loc[t-20:t-1,'low'].min()),
           'next5_low':float(df.loc[t+1:t+5,'low'].min()),'next5_high':float(df.loc[t+1:t+5,'high'].max()),
           'prior20_change_pct':100*(df.loc[t,'close']/df.loc[t-20,'close']-1),
           'next5_change_pct':100*(df.loc[t+5,'close']/df.loc[t+1,'open']-1),
           'price_basis':'KRX_RAW_UNADJUSTED_OR_UNKNOWN_CA_NOT_CERTIFIED',
           'date_daily_sha_checked':True,'chart_file':fname}
        audit.append(record)
    pd.DataFrame(audit).to_csv(out.parent/'06_REAL_OHLC_CHART_REVIEW_DATA.csv',index=False,encoding='utf-8-sig')
    print('R1D71_REAL_OHLC_CHARTS',len(audit),'\n',pd.DataFrame(audit)[['code','date','raw_close_t','raw_open_d1','raw_close_d20','prior20_change_pct','next5_change_pct','D20_raw_close_from_entry_pct']].to_string(index=False))
    return paths

if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser();p.add_argument('--shards',nargs='+',required=True);p.add_argument('--out',required=True);a=p.parse_args()
    charts=make_charts(CASES,get_prices(a.shards,CASES),a.out)
    print('CHARTS',charts)
