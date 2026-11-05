#!/usr/bin/env python3
"""Actual SHA-verified OHLCV + TRUE original R117 event charts after 5-shard merge."""
from __future__ import annotations
from pathlib import Path
import hashlib
import pandas as pd
import numpy as np
from r118_causal_indicators import feat


def make_charts(event_csv,price_manifest,out,per_outcome=5):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.patches import Rectangle
    plt.rcParams['font.family']='NanumGothic'
    plt.rcParams['axes.unicode_minus']=False
    out=Path(out);out.mkdir(parents=True,exist_ok=True)
    d=pd.read_csv(event_csv,dtype={'code':str,'signal_date':str})
    d=d[d.signal_kind.isin(['EARLY_FIRST_CLOSE','CONFIRMED_TWO_CLOSE'])]
    d=d[~d.suspect_big_move & ~d.suspect_zero_volume].copy()
    f=Path(price_manifest).resolve()
    mf=pd.read_csv(f,dtype={'code':str})
    files={}
    for z in mf.itertuples(index=False):
        p=Path(z.path)
        if not p.is_absolute():p=(f.parent/p).resolve()
        files[z.code]=(p,z.sha256)
    if set(d.code)-set(files):raise ValueError('EVENT_CODE_NOT_IN_MANIFEST')
    # Deterministic medoid-ish sampling, NOT cherry-picked extremes.
    records=[]
    for side in ['WIN','LOSS']:
        q=d[d.TP3SL5_net_pct.gt(0) if side=='WIN' else d.TP3SL5_net_pct.lt(0)].copy()
        q=q.sort_values(['code','signal_date','scale','signal_kind'],kind='stable')
        if q.empty:raise ValueError('CANNOT_SHOW_BOTH_OUTCOMES')
        picks=[];used=set()
        for kind in ['EARLY_FIRST_CLOSE','CONFIRMED_TWO_CLOSE']:
            g=q[q.signal_kind==kind]
            for scale in ['SHORT','MEDIUM','LONG']:
                z=g[g.scale==scale]
                if not z.empty:
                    row=z.iloc[len(z)//2]
                    if (row.code,row.signal_date) not in used:
                        picks.append(row);used.add((row.code,row.signal_date))
                    if len(picks)>=per_outcome:break
            if len(picks)>=per_outcome:break
        if len(picks)<per_outcome:
            for _,row in q.iterrows():
                if (row.code,row.signal_date) not in used:
                    picks.append(row);used.add((row.code,row.signal_date))
                if len(picks)>=per_outcome:break
        for k,row in enumerate(picks[:per_outcome],1):
            fp,sha=files[row.code]
            if hashlib.sha256(fp.read_bytes()).hexdigest()!=sha:raise ValueError('CHART_PRICE_SHA_MISMATCH')
            s=feat(pd.read_csv(fp,encoding='utf-8-sig',dtype={'date':str}).sort_values('date').reset_index(drop=True))
            t=int(row.signal_idx)
            if s.at[t,'date']!=row.signal_date or abs(s.at[t,'close']-row.r117_source_close)>1e-3:raise ValueError('CHART_SIGNAL_SOURCE_MISMATCH')
            start=max(0,t-65);end=min(len(s)-1,t+15)
            a=s.iloc[start:end+1].reset_index(drop=True);x=np.arange(len(a));loc=t-start
            fig,(ax,av)=plt.subplots(2,1,figsize=(13,7.1),sharex=True,gridspec_kw={'height_ratios':[4,1]},constrained_layout=True)
            for j,v in enumerate(a.itertuples(index=False)):
                color='#D75050' if v.close>=v.open else '#3086BA'
                ax.plot([j,j],[v.low,v.high],color=color,lw=.7,zorder=3)
                ax.add_patch(Rectangle((j-.28,min(v.open,v.close)),.56,max(abs(v.open-v.close),v.close*.0001),color=color,alpha=.84,zorder=4))
            av.bar(x,a.amount/1e8,color=['#D75050' if v.close>=v.open else '#3086BA' for v in a.itertuples(index=False)],width=.72)
            ax.plot(x,a.ma20,label='MA20',color='#7E5EBA',lw=1.0)
            ax.plot(x,a.ma60,label='MA60',color='#A87C3D',lw=1.0)
            ax.plot(x,a.ma224,label='MA224',color='#666666',lw=.8)
            ca=a.cloudA_today.to_numpy(float);cb=a.cloudB_today.to_numpy(float)
            ax.fill_between(x,ca,cb,where=ca>=cb,color='#93C7A0',alpha=.30,label='구름 양운')
            ax.fill_between(x,ca,cb,where=ca<cb,color='#E0A3A7',alpha=.29,label='구름 음운')
            ax.axvspan(loc-2.5,loc+.4,color='#E2C977',alpha=.2,label='3캔들 D-2~D')
            ax.axvline(loc,color='#505050',lw=.9,ls='--')
            ax.scatter(loc,s.at[t,'close'],s=75,color='black',label='R117 실제 이벤트',zorder=10)
            ax.scatter(loc+1,s.at[t+1,'open'],s=75,color='#2B6EB0',marker='^',label='D+1 매수',zorder=10)
            lvl=row.line_level_at_signal
            slope=row.line_projection_slope
            if pd.notna(lvl):
                if pd.notna(slope):
                    xx=np.arange(max(0,loc-6),min(len(a),loc+11))
                    yy=lvl+(xx-loc)*slope
                    ax.plot(xx,yy,color='black',ls=':',lw=1.9,label='원본 R117 추세선 기울기 복원/투영')
                else:
                    ax.scatter(loc,lvl,s=90,marker='_',color='black',label='원본 R117 가격 한 점')
            exitday=int(row.TP3SL5_day)
            exloc=loc+exitday
            if exloc<len(a):
                reason=row.TP3SL5_reason
                ent=row.entry_open
                level=(ent*1.03 if reason in ['TP','GAP_TP'] else ent*.95 if reason in ['STOP','GAP_STOP','BOTH_STOP_FIRST'] else s.at[t+exitday,'close'])
                ax.scatter(exloc,level,s=120,marker='X',color='#82398B',label='TP3/SL5 청산',zorder=10)
            labels=list(range(0,len(a),10))+[loc];labels=sorted(set(labels))
            av.set_xticks(labels);av.set_xticklabels([str(a.at[j,'date'])[5:] for j in labels],rotation=30)
            ax.set_ylabel('원');av.set_ylabel('거래대금 억')
            ax.grid(axis='y',alpha=.2);ax.legend(loc='upper left',fontsize=7,ncol=3)
            title=f'R117 {row.signal_kind} {row.scale} · {row.code} · {row.signal_date}'
            subtitle=f'3거래일전 상향={bool(row.c_gt_c3)} / 구름={row.cloud_position} / MA20상승={bool(row.ma20_rising)} / TP3·SL5 순수익={row.TP3SL5_net_pct:+.2f}%'
            fig.suptitle(title+'\n'+subtitle,fontsize=11)
            name=f'TRUE_{side}_{k:02d}_{row.code}_{row.signal_date}_{row.signal_kind}.png'
            fig.savefig(out/name,dpi=135);plt.close(fig)
            records.append({'file':name,'outcome':side,'code':row.code,'date':row.signal_date,
                            'stage':row.signal_kind,'scale':row.scale,'profit_net_pct':row.TP3SL5_net_pct})
    pd.DataFrame(records).to_csv(out/'R118_TRUEBREAK_REAL_CHART_LIST.csv',index=False,encoding='utf-8-sig')
    html=['<!doctype html><html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>R118 진짜 추세선 돌파 사례</title><style>body{font-family:sans-serif;background:#f7f9fa;max-width:1180px;margin:auto;padding:15px}article{background:white;padding:12px;margin:12px 0;border-radius:12px}img{width:100%;height:auto}</style></head><body><h1>R117 진짜 상단 추세선 돌파 · 성공 5 / 실패 5</h1><p>R117 원본 상태 이벤트로 신호 확정. 현재 구름대는 과거 D-26에서 투영된 것. 같은 날 TP·SL 충돌은 손절 먼저.</p>']
    for rec in records:html.append(f'<article><h3>{rec["outcome"]} {rec["code"]} {rec["date"]} {rec["stage"]} {rec["profit_net_pct"]:+.2f}%</h3><img loading="lazy" src="{rec["file"]}"></article>')
    html.append('</body></html>')
    (out/'R118_TRUEBREAK_10_CHARTS_GALLERY.html').write_text('\n'.join(html),encoding='utf-8')
    return len(records)
