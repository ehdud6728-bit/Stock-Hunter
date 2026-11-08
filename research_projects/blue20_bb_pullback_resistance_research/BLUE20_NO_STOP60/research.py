from pathlib import Path
import sys,os,json,zipfile
import pandas as pd
import numpy as np
H=Path(__file__).resolve().parent;R=H.parent;O=H/'results';O.mkdir(exist_ok=True)
sys.path.insert(0,str(R/'BLUE20_PULLBACK_RESET'))
from study import load_prices
from report import CASES

def sim(d,calendar,entry_day):
 pos={v:i for i,v in enumerate(d.date)}
 if entry_day not in pos:return dict(status='MISSING_ENTRY')
 cp=calendar.get_loc(entry_day);price=float(d.open.iloc[pos[entry_day]]);target=price*1.1;end=cp+59;complete=end<len(calendar)
 days=calendar[cp:min(end+1,len(calendar))];w=d[d.date.isin(days)]
 if len(w)!=len(days):return dict(status='MISSING_BAR',entry_date=entry_day,entry_price=price)
 base=dict(entry_date=entry_day,entry_price=price,target_price=target,complete60=complete)
 hits=w[w.high>=target]
 if len(hits):
  b=hits.iloc[0];hold=calendar.get_loc(b.date)-cp+1;px=float(b.open) if hold>1 and b.open>=target else target;used=w[w.date<=b.date]
  return base|dict(status='TP10',exit_date=b.date,exit_price=px,holding_sessions=hold,gross_pct=100*(px/price-1),net_pct=100*(px/price-1)-.25,adverse_low_pct=100*(used.low.min()/price-1))
 if not complete:return base|dict(status='RIGHT_CENSORED',observed_sessions=len(w))
 b=w.iloc[-1];return base|dict(status='NO_TP60',exit_date=b.date,exit_price=float(b.close),holding_sessions=60,gross_pct=100*(b.close/price-1),net_pct=100*(b.close/price-1)-.25,adverse_low_pct=100*(w.low.min()/price-1))

def main():
 frames=load_prices(Path(os.environ['TEMP'])/'stock-hunter-blue20-full-replay/research_projects/blue20_multifactor_research/.auto500_work/r118_inputs/manifest.csv')
 cal=pd.Index(sorted({v for d in frames.values() for v in d.date}))
 src=R/'BLUE20_ACTIONS_42466372968/results';t=pd.read_csv(src/'TRADES.csv.gz',dtype={'code':str});o=pd.read_csv(src/'OPPORTUNITIES.csv.gz',dtype={'code':str})
 t=t[t.horizon.eq('FRESH20')&t.stop_mode.eq('FIXED5')].merge(o[['opportunity_id','strong_relevant']],on='opportunity_id',validate='many_to_one')
 rows=[]
 for r in t.itertuples():
  base={k:getattr(r,k) for k in ['opportunity_id','universe','code','date','year','band','policy','nonoverlap60','strong_relevant','known_issue_initial','known_gap_lookback_through60']}
  base['name']=CASES.get((r.code,r.date),r.code)
  if pd.isna(r.confirm_date):rows.append(base|dict(status='NO_ENTRY_SIGNAL'));continue
  cp=cal.get_loc(r.confirm_date)
  if cp+1>=len(cal):rows.append(base|dict(status='NO_NEXT_SESSION'));continue
  rows.append(base|sim(frames[r.code],cal,cal[cp+1]))
 a=pd.DataFrame(rows);a.to_csv(O/'ALL_POLICIES.csv.gz',index=False)
 sums=[]
 for sub,m in [('ALL',pd.Series(True,index=a.index)),('NONOVERLAP60',a.nonoverlap60),('STRONG_NONOVERLAP60',a.nonoverlap60&a.strong_relevant)]:
  for keys,g in a[m].groupby(['universe','year','policy']):
   full=g[g.complete60.eq(True)&g.status.isin(['TP10','NO_TP60'])];hit=full.status.eq('TP10');no=full[~hit]
   sums.append(dict(zip(['universe','year','policy'],keys))|dict(subset=sub,opportunities=len(g),complete60=len(full),tp10=int(hit.sum()),no_tp60=int((~hit).sum()),tp_pct=100*hit.mean(),not_full=int((g.complete60.eq(False)).sum()),no_entry=int(g.status.eq('NO_ENTRY_SIGNAL').sum()),missing=int(g.status.isin(['MISSING_BAR','MISSING_ENTRY']).sum()),mean_net=full.net_pct.mean(),no_tp_mean_net=no.net_pct.mean(),median_holding=full.holding_sessions.median(),worst_adverse=full.adverse_low_pct.min()))
 s=pd.DataFrame(sums);s.to_csv(O/'SUMMARY.csv',index=False)
 primary=a[a.universe.eq('ALL_DAYS_P1')&a.nonoverlap60&a.complete60.eq(True)&a.policy.eq('A_FIRST')]
 for key,mask in [('TP10',primary.status.eq('TP10')),('NO_TP60',primary.status.eq('NO_TP60'))]:primary[mask].to_csv(O/(key+'_FIRST_SIGNAL.csv'),index=False)
 specified=a[a.universe.eq('BASELINE_1804')&a.band.eq('UPPER')&pd.Series([(c,d) in CASES for c,d in zip(a.code,a.date)],index=a.index)]
 specified.to_csv(O/'SPECIFIED8.csv',index=False)
 pages=['<!doctype html><meta charset="utf-8"><style>body{font:16px Arial;margin:24px}td,th{padding:5px;border:1px solid #ccc}</style><h1>손절 없이 +10% 익절 · 최대60거래일</h1><p>첫진입 별도P1,60일 중복제거,완전60일 관찰가능 사건. 한 종목도 신호일에 따라 성공/미도달이 달라집니다. 미도달은 반드시 손실이라는 뜻이 아닙니다. 매수일을1일째로 세며 60일째 종가 청산. 거래정지·누락은 실패로 처리하지 않습니다.</p>']
 cols=['name','code','date','entry_date','entry_price','target_price','exit_date','holding_sessions','gross_pct','net_pct','adverse_low_pct']
 for label,mask in [('+10% 매도 도달',primary.status.eq('TP10')),('60일 안에 +10% 미도달',primary.status.eq('NO_TP60'))]:pages.extend(['<h2>'+label+'</h2>',primary[mask][cols].to_html(index=False,float_format=lambda v:f'{v:.2f}')])
 pages.extend(['<h2>지정8종목: 첫진입 및 후속진입</h2>',specified[['name','date','policy','status','entry_date','exit_date','net_pct']].to_html(index=False),'</html>']);(O/'LISTS.html').write_text('\n'.join(pages),encoding='utf-8')
 print(s[(s.subset=='NONOVERLAP60')&(s.universe=='ALL_DAYS_P1')&s.policy.isin(['A_FIRST','B_RECOVERY','C_RETEST'])][['year','policy','complete60','tp10','no_tp60','tp_pct','not_full','mean_net']].to_string(index=False))
 print(specified[specified.policy.eq('A_FIRST')][['name','code','date','status','exit_date','holding_sessions','net_pct']].to_string(index=False))
 # 경계검증: 매수당일 목표,60일째 목표,61일째 목표 제외,부분관찰 보류
 caltest=pd.Index([str(i) for i in range(61)]);d=pd.DataFrame(dict(date=caltest,open=100.,high=105.,low=50.,close=80.))
 assert sim(d,caltest,'0')['status']=='NO_TP60'
 d.loc[60,'high']=120;assert sim(d,caltest,'0')['status']=='NO_TP60'
 d.loc[59,'high']=120;assert sim(d,caltest,'0')['holding_sessions']==60
 d.loc[0,'high']=120;assert sim(d,caltest,'0')['holding_sessions']==1
 d.high=105;assert sim(d,caltest[:30],'0')['status']=='RIGHT_CENSORED'
 (O/'VERIFICATION.json').write_text(json.dumps(dict(boundary_tests='PASS',stop_loss=False,hold_sessions=60,source_run=42466372968,real_execution='NOT_SIMULATED',corporate_actions='UNVERIFIED'),indent=2),encoding='utf-8')
 with zipfile.ZipFile(H/'BLUE20_No_Stop_60.zip','w',zipfile.ZIP_DEFLATED) as z:
  for p in H.rglob('*'):
   if p.is_file() and p.suffix!='.zip':z.write(p,p.relative_to(H))
if __name__=='__main__':main()
