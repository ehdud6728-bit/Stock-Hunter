from pathlib import Path
import sys,os,json,zipfile,hashlib,html,re
import pandas as pd
import numpy as np
H=Path(__file__).resolve().parent;R=H.parent;O=H/'results'
sys.path.insert(0,str(R/'BLUE20_PULLBACK_RESET'));sys.path.insert(0,str(R/'BLUE20_PULLBACK_RESET/vendor'))
from loader import load_prices
from report import CASES
def main():
 a=pd.read_csv(O/'OPPORTUNITIES.csv.gz',dtype={'code':str});t=pd.read_csv(O/'TRADES.csv.gz',dtype={'code':str});s=pd.read_csv(O/'SNAPSHOTS.csv.gz',dtype={'code':str});z=pd.read_csv(O/'ZONES.csv.gz',dtype={'code':str})
 frames=load_prices(Path(os.environ.get('BLUE20_MANIFEST',str(Path(os.environ.get('TEMP','/tmp'))/'stock-hunter-blue20-full-replay/research_projects/blue20_multifactor_research/.auto500_work/r118_inputs/manifest.csv'))));cal=pd.Index(sorted({v for d in frames.values() for v in d.date}));cp={v:i for i,v in enumerate(cal)}
 known=pd.concat([pd.read_csv(R/'BLUE20_PULLBACK_RESET/reference/KNOWN_ANOMALIES.csv',dtype={'code':str})[['code','date']],pd.read_csv(R/'BLUE20_PULLBACK_RESET/reference/KNOWN_DISCONTINUITIES.csv',dtype={'code':str})[['code','date']]])
 kd={c:list(g.date) for c,g in known.groupby('code')}
 a['known_source_issue']=[any(cal[max(0,cp[d]-240)]<=v<=cal[min(len(cal)-1,cp[d]+100)] for v in kd.get(c,[])) for c,d in zip(a.code,a.date)]
 union=a[['code','date','band']].drop_duplicates().sort_values(['code','band','date']);keep=set()
 for _,g in union.groupby(['code','band']):
  last=-10000
  for q in g.itertuples():
   p=cp[q.date]
   if p-last>60:keep.add((q.code,q.date,q.band));last=p
 a['union_nonoverlap60']=[(c,d,b) in keep for c,d,b in zip(a.code,a.date,a.band)]
 t=t.merge(a[['opportunity_id','known_source_issue','union_nonoverlap60']],on='opportunity_id',validate='many_to_one')
 t['outcome']=np.where(t.status.eq('TP10'),'SUCCESS_TP10',np.where(t.status.eq('NO_TP60'),'FAIL_NO_TP60',np.where(t.status.eq('NO_ENTRY_SIGNAL'),'NO_ENTRY','UNVERIFIED_OR_CENSORED')))
 t['net_outcome']=np.where(t.net_pct.lt(0),'LOSS',np.where(t.net_pct.ge(0),'NONLOSS','UNKNOWN'))
 t.to_csv(O/'CLASSIFIED_TRADES.csv.gz',index=False)
 sums=[]
 for scope,mask in [('ALL',pd.Series(True,index=t.index)),('FAIR100',t.fair100),('FAIR100_NONOVERLAP60',t.fair100&t.nonoverlap60),('FAIR100_UNION_NONOVERLAP60',t.fair100&t.union_nonoverlap60),('FAIR100_CLEAN',t.fair100&~t.known_source_issue)]:
  for keys,g in t[mask].groupby(['model','band','year','signal_group','policy']):
   full=g[g.complete60.eq(True)&g.status.isin(['TP10','NO_TP60'])];hit=full.status.eq('TP10');noentry=g.status.eq('NO_ENTRY_SIGNAL');opnet=g.net_pct.copy();opnet.loc[noentry]=0
   sums.append(dict(zip(['model','band','year','signal_group','policy'],keys))|dict(scope=scope,opportunities=len(g),entries=int(g.entry_date.notna().sum()),complete60=len(full),tp10=int(hit.sum()),no_tp60=int((~hit).sum()),tp_pct=100*hit.mean(),no_entry=int(noentry.sum()),unknown=int((~g.status.isin(['TP10','NO_TP60','NO_ENTRY_SIGNAL'])).sum()),entry_pct=100*g.entry_date.notna().mean(),mean_net=full.net_pct.mean(),opportunity_net=opnet.mean() if scope!='ALL' else np.nan,mean_hold=full.holding_sessions.mean(),median_adverse=full.adverse_low_pct.median()))
 summary=pd.DataFrame(sums);summary.to_csv(O/'SUMMARY.csv',index=False)
 pairs=[]
 for oid,g in t.groupby('opportunity_id'):
  first=g[g.policy.eq('A_FIRST')].iloc[0]
  for q in g[~g.policy.eq('A_FIRST')].itertuples():
   fstatus=first.status;pstatus=q.status
   category=('NO_ENTRY_FIRST_SUCCESS' if fstatus=='TP10' else 'NO_ENTRY_FIRST_FAIL' if fstatus=='NO_TP60' else 'NO_ENTRY_FIRST_UNKNOWN') if pstatus=='NO_ENTRY_SIGNAL' else 'UNVERIFIED'
   if fstatus in ['TP10','NO_TP60'] and pstatus in ['TP10','NO_TP60']:category=('FIRST_SUCCESS' if fstatus=='TP10' else 'FIRST_FAIL')+('_PULL_SUCCESS' if pstatus=='TP10' else '_PULL_FAIL')
   pairs.append(dict(opportunity_id=oid,code=q.code,date=q.date,year=q.year,model=q.model,band=q.band,signal_group=q.signal_group,policy=q.policy,fair100=q.fair100,nonoverlap60=q.nonoverlap60,known_source_issue=q.known_source_issue,category=category,first_net=first.net_pct,pull_net=q.net_pct,net_delta=q.net_pct-first.net_pct))
 pairs=pd.DataFrame(pairs);pairs.to_csv(O/'PAIRS.csv.gz',index=False)
 matrix=pairs[pairs.fair100&pairs.nonoverlap60].groupby(['model','band','year','signal_group','policy','category']).size().reset_index(name='n');matrix.to_csv(O/'PAIR_MATRIX.csv',index=False)
 res=[]
 for stage,key in [('INITIAL','initial_snapshot'),('CONFIRM','confirm_snapshot')]:
  v=t.merge(s,on=None,left_on=key,right_on='snapshot_id',suffixes=('','_snap'),how='left',validate='many_to_one')
  v=v[v.fair100&v.nonoverlap60&v.complete60.eq(True)&v.status.isin(['TP10','NO_TP60'])]
  for feature in ['strong_active','strong_body','resistance_bucket']:
   for keys,g in v.groupby(['model','band','year','signal_group','policy',feature],dropna=False):
    res.append(dict(zip(['model','band','year','signal_group','policy','feature_value'],keys))|dict(stage=stage,feature=feature,n=len(g),tp_pct=100*g.status.eq('TP10').mean(),mean_net=g.net_pct.mean()))
  for method in ['range','typical']:
   col='volume_above10_'+method;v['vol_bucket']=pd.cut(v[col],[-.001,10,30,100.001],labels=['0-10%','10-30%','30-100%'])
   for keys,g in v.groupby(['model','band','year','signal_group','policy','vol_bucket'],observed=True):res.append(dict(zip(['model','band','year','signal_group','policy','feature_value'],keys))|dict(stage=stage,feature=col,n=len(g),tp_pct=100*g.status.eq('TP10').mean(),mean_net=g.net_pct.mean()))
 pd.DataFrame(res).to_csv(O/'RESISTANCE_EFFECT.csv',index=False)
 # 기간별 같은 초기 기회에 다른 스냅샷을 결합한다.
 sen=[]
 for n in [60,120,240]:
  v=t[t.policy.eq('A_FIRST')&t.fair100&t.nonoverlap60&t.complete60.eq(True)].merge(s[s.period.eq(n)],on=['code','date'],suffixes=('','_snap'),validate='many_to_one')
  for keys,g in v.groupby(['model','band','year','strong_active'],dropna=False):sen.append(dict(zip(['model','band','year','strong_active'],keys))|dict(period=n,n=len(g),tp_pct=100*g.status.eq('TP10').mean(),mean_net=g.net_pct.mean()))
 pd.DataFrame(sen).to_csv(O/'LOOKBACK_SENSITIVITY.csv',index=False)
 # 설명용 사례: 고정8 + 결과유형별 날짜순 첫건
 selected=[]
 for (c,d),name in CASES.items():
  q=t[t.code.eq(c)&t.date.eq(d)&t.model.eq('BB20')&t.band.eq('UPPER')]
  if len(q):selected.append((q.opportunity_id.iloc[0],name+' / 지정'))
  else:selected.append((None,f'{name} {c} {d}: BB20상단 신호 없음'))
 for year in [2023,2024]:
  for category in ['FIRST_FAIL_PULL_SUCCESS','FIRST_SUCCESS_PULL_FAIL','FIRST_FAIL_PULL_FAIL']:
   q=pairs[pairs.fair100&pairs.nonoverlap60&pairs.band.eq('UPPER')&pairs.policy.eq('B_RECOVERY')&pairs.category.eq(category)&pairs.year.eq(year)&~pairs.known_source_issue].sort_values(['date','code','model'])
   if len(q):selected.append((q.opportunity_id.iloc[0],f'{year} {category} / 날짜순첫건'))
 pages=['<!doctype html><meta charset="utf-8"><style>body{font:16px Arial;max-width:1300px;margin:auto;padding:24px}svg{width:100%}td,th{border:1px solid #ccc;padding:4px}</style><h1>첫진입과 눌림진입: 성공·미도달 대조</h1><p>성공=각매수후60일내+10%,실패=완전관찰후미도달. 미진입·기간부족별도. 주황존=첫신호전일에고정한과거가격저항,녹색존=이미종가아래인과거존. 실제매도잔량아님. 검은선D0, E=첫진입, B=회복후진입. 빨강상승/파랑하락. 두진입각자의목표선을따로표시. 이후캔들은당시미래정보.</p>'];caseout=[]
 for idx,(oid,label) in enumerate(selected,1):
  if oid is None:pages.append('<p>'+html.escape(label)+'</p>');continue
  g=t[t.opportunity_id.eq(oid)];r=g.iloc[0];d=frames[r.code];j=int(d.index[d.date.eq(r.date)][0]);end=min(len(d),j+115);w=d.iloc[max(0,j-40):end].reset_index(drop=True);firststart=max(0,j-40);zz=z[z.snapshot_id.eq(r.initial_snapshot)];lo=w.low.min();hi=w.high.max();span=max(1,hi-lo);x=lambda k:65+1100*k/max(1,len(w)-1);y=lambda p:365-310*(p-lo)/span
  svg=['<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1280 510">']
  for v in zz.itertuples():
   if v.high<lo or v.low>hi:continue
   col='#3e9861' if v.position=='BELOW' else '#dd9233';svg.append(f'<rect x="65" y="{y(min(hi,v.high))}" width="1100" height="{max(1,y(max(lo,v.low))-y(min(hi,v.high)))}" fill="{col}" opacity="{.15 if v.strong else .05}"><title>{v.low:.0f}~{v.high:.0f} {v.count}회 {v.position}</title></rect>')
  for k,b in w.iterrows():
   xx=x(k);col='#c44' if b.close>=b.open else '#2871b2';svg.append(f'<g><title>{b.date} O {b.open} H {b.high} L {b.low} C {b.close} V {b.volume}</title><path d="M{xx} {y(b.high)}V{y(b.low)}" stroke="{col}"/><rect x="{xx-2}" y="{y(max(b.open,b.close))}" width="4" height="{max(1,abs(y(b.open)-y(b.close)))}" fill="{col}"/></g><rect x="{xx-2}" y="{455-55*b.volume/w.volume.max()}" width="4" height="{55*b.volume/w.volume.max()}" fill="{col}"/>')
  for policy,col,tag in [('A_FIRST','#222','E'),('B_RECOVERY','#9244aa','B')]:
   q=g[g.policy.eq(policy)].iloc[0]
   if pd.isna(q.entry_date) or q.entry_date not in set(w.date):continue
   k=int(w.index[w.date.eq(q.entry_date)][0]);svg.append(f'<circle cx="{x(k)}" cy="{y(q.entry_price)}" r="5" fill="{col}"/><text x="{x(k)}" y="{y(q.entry_price)-10}" fill="{col}">{tag}</text><path d="M{x(k)} {y(q.target_price)}H1165" stroke="{col}" stroke-dasharray="4 4"/>')
   if pd.notna(q.exit_date) and q.exit_date in set(w.date):
    k=int(w.index[w.date.eq(q.exit_date)][0]);svg.append(f'<text x="{x(k)}" y="{y(q.exit_price)}" fill="{col}">X<title>{policy} {q.exit_date} {q.status}</title></text>')
  svg.append(f'<path d="M{x(j-firststart)} 30V460" stroke="black"/><text x="65" y="485">{w.date.iloc[0]} → {w.date.iloc[-1]} / {r.code} D0={r.date}</text></svg>')
  pages.extend([f'<h2>{idx}. {html.escape(label)} / {r.model} {r.code} {r.date}</h2>',''.join(svg),g[['policy','confirm_date','entry_date','entry_price','target_price','status','exit_date','holding_sessions','net_pct','adverse_low_pct']].to_html(index=False,float_format=lambda v:f'{v:.2f}')]);w.to_csv(O/f'CASE_{idx:02}_{r.code}_OHLCV.csv',index=False)
  for q in g.to_dict('records'):caseout.append(dict(case=idx,label=label,**q))
 pages.append('</html>');(O/'GALLERY.html').write_text('\n'.join(pages),encoding='utf-8');pd.DataFrame(caseout).to_csv(O/'CASES.csv',index=False)
 import xml.etree.ElementTree as ET
 for v in re.findall(r'<svg.*?</svg>',''.join(pages),re.S):ET.fromstring(v)
 # 공정 기회에서 확인 없는 정책을 실패로 채우지 않았는지 검사
 assert not t.loc[t.status.eq('NO_ENTRY_SIGNAL'),'outcome'].eq('FAIL_NO_TP60').any()
 for q in t[t.status.isin(['TP10','NO_TP60'])].itertuples():
  d=frames[q.code];assert d.loc[d.date.eq(q.entry_date),'open'].iloc[0]==q.entry_price
  if q.status=='TP10':assert d.loc[d.date.eq(q.exit_date),'high'].iloc[0]>=q.target_price
 (O/'REPORT_VERIFICATION.json').write_text(json.dumps(dict(status='PASS',entry_ohlcv_match=True,target_touch_match=True,no_entry_not_failure=True,chart_structures_checked=len(re.findall('<svg',''.join(pages))),hts_crosscheck='NOT_PERFORMED'),indent=2),encoding='utf-8')
 print('reports complete',len(t),'cases',len(selected),flush=True)
 print(summary[(summary.scope=='FAIR100_NONOVERLAP60')&summary.band.eq('UPPER')&summary.policy.isin(['A_FIRST','B_RECOVERY'])][['model','year','signal_group','policy','opportunities','complete60','tp_pct','mean_net','opportunity_net']].to_string(index=False))
if __name__=='__main__':main()
