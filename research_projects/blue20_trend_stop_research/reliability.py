"""Independent daily volume proxies and pre-D0 frozen resistance validation."""
import argparse
from pathlib import Path
import numpy as np
import pandas as pd
from analyze import HERE,ROOT,loader,module
vp=module('vp_formula',HERE/'vendor/profile.py' if (HERE/'vendor/profile.py').exists() else ROOT/'BLUE20_VP_AUDIT/study.py')

def touches(validation,previous,low,high):
    records=[];last=-100
    for i,b in enumerate(validation.itertuples(index=False)):
        prev=previous if i==0 else validation.close.iloc[i-1]
        if prev>=low or b.high<low or i-last<5:continue
        last=i;future=validation.iloc[i:i+4]
        if len(future)<4:result='PENDING'
        elif future.close.gt(high).any():result='BREAK'
        elif future.close.min()<low and (b.high-future.close.min())/b.prior_atr20>=.5:result='REJECT'
        else:result='OTHER'
        records.append(dict(touch_date=b.date,outcome=result))
    return records

def main(manifest):
    frames=loader.load_prices(Path(manifest));f=pd.read_csv(HERE/'results/TREND_FEATURES.csv.gz',dtype={'code':str})
    rows=[];alltouch=[];gaps=[]
    for code,d in frames.items():
        prev=d.close.shift();tr=pd.concat([d.high-d.low,(d.high-prev).abs(),(d.low-prev).abs()],axis=1).max(axis=1)
        d['prior_atr20']=tr.rolling(20).mean().shift()
        for i in np.flatnonzero(((d.open/prev-1).abs()>.35).to_numpy()):
            gaps.append(dict(code=code,date=d.date.iloc[i],previous_close=prev.iloc[i],open=d.open.iloc[i],gap_pct=100*(d.open.iloc[i]/prev.iloc[i]-1),status='UNVERIFIED_DISCONTINUITY_NOT_AUTOMATIC_ERROR'))
    for e in f.drop_duplicates(['code','date']).itertuples():
        d=frames[e.code];j=int(d.index[d.date.eq(e.date)][0])
        for n in [60,120,240]:
            h=d.iloc[max(0,j-n):j]
            if len(h)!=n:continue
            discovery=h.iloc[:-40];validation=h.iloc[-40:];cutoff=discovery.close.iloc[-1]
            for bins in [16,32,64]:
                for method in ['range','typical']:
                    p=vp.volume_profile(discovery,bins,method)
                    if p is None:continue
                    width=p['width'];candidates=[c for c in p['hvn'] if c-width/2>cutoff]
                    center=min(candidates) if candidates else np.nan;low=center-width/2;high=center+width/2
                    record=dict(code=e.code,date=e.date,window=n,discovery_bars=n-40,validation_bars=40,bins=bins,method=method,
                                poc=p['poc'],vah=p['vah'],val=p['val'],zone_low=low,zone_high=high,cutoff=discovery.date.iloc[-1],has_overhead=bool(candidates),
                                mass_error=float(p['weights'].sum()-discovery.volume.sum()))
                    rows.append(record)
                    if candidates:
                        for t in touches(validation,cutoff,low,high):alltouch.append({**record,**t})
    out=HERE/'results';pd.DataFrame(rows).to_csv(out/'VOLUME_PROXY_SENSITIVITY.csv.gz',index=False)
    a=pd.DataFrame(alltouch);a.to_csv(out/'VOLUME_PROXY_TOUCHES.csv.gz',index=False)
    summary=a.groupby(['window','bins','method','outcome']).size().unstack(fill_value=0).reset_index();summary.to_csv(out/'VOLUME_PROXY_VALIDATION.csv',index=False)
    pd.DataFrame(gaps).to_csv(out/'SOURCE_DISCONTINUITIES.csv',index=False)
    r=out/'REPORT_KO.md';text=r.read_text(encoding='utf-8');text+='\n\n## 일봉 거래량 집중 구간 대조\n\n동일한 과거 발견/40봉 검증 구간에서 가격 고점과 독립적으로 추정 HVN을 선택했다. 발견 종료 종가 위 가장 가까운 HVN 한 구간이며, 60/120/240봉과 16/32/64구간, 고저 균등분배/대표가격 집중의 18조합을 제공한다. 60봉 설정은 발견 표본이 20봉뿐이어서 120봉 가격 저항과 동등한 증거량으로 해석하면 안 된다. 가격 저항은 폭이 ATR 기반, HVN은 가격 구간 기반이라 접촉·반락 횟수를 직접 우열로 해석할 수 없다. VOLUME_PROXY_VALIDATION.csv에서 방법별 차이를 확인한다. 실제 거래량 인증이 아닌 대리량의 민감도다.\n\n추가로 전일 종가 대비 시가의 절대 변화가 35%를 넘는 원천 연속성 단서를 SOURCE_DISCONTINUITIES.csv에 기록했다. 기업행사·거래재개·가격 기준을 확인하지 않은 상태이며 자동 오류로 제거하지 않았다. 기존 7행 제외 CLEAN도 이 신규 단서를 제거한 결과가 아니다. 원천 미확인 상태에서는 큰 손실·수익을 실현 가능 결과로 확정할 수 없다.\n'
    r.write_text(text,encoding='utf-8');print({'proxy_configurations':len(rows),'touches':len(a),'discontinuities':len(gaps),'max_volume_error':max(abs(x['mass_error']) for x in rows)})

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--manifest',required=True);main(p.parse_args().manifest)
