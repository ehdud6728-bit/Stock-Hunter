"""R1-D97 fresh three-stock independent application of frozen R1-D96 auto-S/R.
Outputs: PIT snapshots, all candidates, human review evidence, forward audit and integrated MA/BB/ENV charts.
This is not a trading strategy or full-market backtest. No future outcomes in line selection.
"""
from __future__ import annotations
from pathlib import Path
import hashlib,json,math
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import research_r1d96 as baseline

ROOT=Path(__file__).resolve().parent
OUT=ROOT/'results'
(OUT/'charts').mkdir(exist_ok=True,parents=True)
CASES=[('039840','디오','2024-09-13'),('071840','롯데하이마트','2024-03-04'),('006400','삼성SDI','2024-02-07')]
plt.rcParams['axes.unicode_minus']=False
try:
 from matplotlib import font_manager
 f=Path('/usr/share/fonts/truetype/nanum/NanumGothic.ttf')
 if f.exists():font_manager.fontManager.addfont(str(f));plt.rcParams['font.family']='NanumGothic'
except Exception:pass

def snapshot(d,asof):
 t=int(np.flatnonzero(d.date.eq(pd.Timestamp(asof)))[0]); q=d.iloc[t]
 def near(event):
  n=0
  for k in range(max(1,t-24),t+1):
   v=d.iloc[k];p=d.iloc[k-1]
   if event=='bb':lo=v.BB20_LO;prev=p.BB20_LO
   elif event=='e20':lo=v.ENV20_10_LO;prev=p.ENV20_10_LO
   else:lo=v.ENV40_10_LO;prev=p.ENV40_10_LO
   if np.isfinite(lo) and v.low <= lo*1.01 and v.close>=lo:n+=1
  return n
 close20=d.close.iloc[max(0,t-19):t+1]
 return dict(close=int(q.close),MA112=round(q.MA112,1),MA224=round(q.MA224,1),MA448=round(q.MA448,1),
             BB20_LO=round(q.BB20_LO,1),ENV20_LO=round(q.ENV20_10_LO,1),ENV40_LO=round(q.ENV40_10_LO,1),
             amount_yi=float(q.amount)/1e8,amount_vs_prev20=round(float(q.amount/q.AMOUNT20),3) if q.AMOUNT20>0 else None,
             full_inverse_order=bool(q.MA112<q.MA224<q.MA448),below224=bool(q.close<q.MA224),
             close20_range_pct=round(100*(close20.max()/close20.min()-1),3),
             bb_touch_recover_25d=near('bb'),env20_touch_recover_25d=near('e20'),env40_touch_recover_25d=near('e40'))

def asof_status(line,asof):
 if line['kind']=='RESISTANCE':
  if line['new_close_break_today']:return 'NEW_BREAK_TODAY'
  if line['prior_close_break']:return 'PRIOR_BREAK_NOT_NEW'
  if line['close_asof']<line['line_asof']:return 'UNBROKEN_RESISTANCE'
  return 'ABOVE_LINE_NOT_FRESH'
 if line['close_asof']<line['line_asof']:return 'SUPPORT_ALREADY_BELOW'
 return 'POTENTIAL_SUPPORT_NOT_PROVEN'

def forward(d,asof,lines,endbars=20):
 t=int(np.flatnonzero(d.date.eq(pd.Timestamp(asof)))[0]);ret=[]
 for line in lines:
  last_state=None
  for k in range(t,min(len(d),t+endbars+1)):
   q=d.iloc[k]
   lv=baseline.calc_line(line['h1_value'],line['h2_value'],line['h1_idx'],line['h2_idx'],k)
   low=float(q.low);high=float(q.high);cl=float(q.close)
   above=cl>=lv;touch=bool(low<=lv*1.01 and high>=lv*.99)
   # One candle is a test, not proof of subsequent holding.
   state=('RESISTANCE_RETEST_CLOSE_ABOVE' if above and touch else 'ABOVE_LINE' if above else 'BELOW_LINE') if line['kind']=='RESISTANCE' else ('SUPPORT_TOUCH_CLOSE_ABOVE' if above and touch else 'SUPPORT_ABOVE' if above else 'SUPPORT_BELOW')
   ret.append(dict(code=str(q.code).zfill(6),asof=asof,date=str(q.date.date()),days_after=k-t,line_id=line['line_id'],
                tier=line['tier'],kind=line['kind'],line_value=round(lv,2),low=low,high=high,close=cl,
                touch=touch,close_above_line=above,close_above_frozen_asof_price=cl>=line['line_asof'],
                line_status=state,source_selection_frozen_asof=True))
 return ret

def draw(d,code,name,asof,lines,expost=False,tag="CENTER"):
 t=int(np.flatnonzero(d.date.eq(pd.Timestamp(asof)))[0]);stop=min(len(d)-1,t+(20 if expost else 0))
 start=max(0,t-65);v=d.iloc[start:stop+1]
 fig,(ax,axma,axamt)=plt.subplots(3,1,figsize=(15.8,11.2),sharex=True,
      gridspec_kw={'height_ratios':[4.1,1.7,1.05]})
 for r in v.itertuples(index=False):
  c='#d24b46' if r.close>=r.open else '#367fb4'
  k=int(r.idx)
  ax.vlines(k,r.low,r.high,color=c,lw=1.1,zorder=2)
  ax.add_patch(Rectangle((k-.29,min(r.close,r.open)),.58,max(.3,abs(r.close-r.open)),facecolor=c,edgecolor=c,lw=.45,zorder=2))
 for col,clr,label in [('BB20_LO','#0497bd','BB20 하단'),('ENV20_10_LO','#c29b28','ENV20 -10%'),('ENV40_10_LO','#9f7440','ENV40 -10%')]:
  ax.plot(v.idx,v[col],color=clr,label=label,lw=1.15,alpha=.85,ls='--' if 'ENV40' in col else '-')
 palette={('RESISTANCE','SMALL'):'#ed7428',('RESISTANCE','MIDDLE'):'#9c4415',('SUPPORT','SMALL'):'#14aa86',('SUPPORT','MIDDLE'):'#116c5d'}
 seen=set()
 for line in lines:
  grp=(line['kind'],line['tier'])
  if grp in seen:continue
  seen.add(grp)
  # plot anchor construction as faint dashed reference, then forward from actual confirmation.
  x=np.arange(line['h1_idx'],min(stop,line['h2_confirm_idx'])+1)
  if len(x)>1:
   y=[baseline.calc_line(line['h1_value'],line['h2_value'],line['h1_idx'],line['h2_idx'],k) for k in x]
   ax.plot(x,y,color=palette[grp],lw=1,ls=':',alpha=.35)
  x=np.arange(max(start,line['h2_confirm_idx']),stop+1)
  if len(x):
   y=[baseline.calc_line(line['h1_value'],line['h2_value'],line['h1_idx'],line['h2_idx'],k) for k in x]
   ax.plot(x,y,color=palette[grp],lw=2.1 if line['tier']=='SMALL' else 1.7,ls='--' if line['kind']=='RESISTANCE' else '-',label=f'{"저항" if line["kind"]=="RESISTANCE" else "지지"} {"소" if line["tier"]=="SMALL" else "중"}: {line["h1"][5:]}→{line["h2"][5:]}')
  for key,val in [('h1_idx','h1_value'),('h2_idx','h2_value')]:
   if start<=line[key]<=stop: ax.scatter([line[key]],[line[val]],s=28,color=palette[grp],zorder=5)
 ax.axvline(t,color='#888',ls='-.',lw=1.3,label='판정 기준일')
 if expost:ax.axvspan(t,stop,color='#ffd7a0',alpha=.15)
 ax.grid(alpha=.13);ax.set_ylabel('일봉/밴드/추세선 (원)')
 ax.legend(loc='upper left',fontsize=8,ncol=2)
 # Separate scale avoids huge MA448 compressing the candle action.
 axma.plot(v.idx,v.close,color='#444',lw=1.2,label='종가')
 for n,c in [(112,'#298b47'),(224,'#272727'),(448,'#8850af')]:axma.plot(v.idx,v[f'MA{n}'],label=f'MA{n}',lw=1.4,color=c)
 axma.axvline(t,color='#888',ls='-.',lw=1);axma.legend(fontsize=8,loc='upper left',ncol=4)
 axma.set_ylabel('112·224·448 (원)');axma.grid(alpha=.15)
 axamt.bar(v.idx,v.amount/1e8,color=np.where(v.close>=v.open,'#d24b46','#367fb4'),width=.68)
 axamt.axvline(t,color='#888',ls='-.',lw=1);axamt.set_ylabel('거래대금 (억원)');axamt.grid(alpha=.15)
 xt=v.iloc[::max(1,len(v)//14)];axamt.set_xticks(xt.idx);axamt.set_xticklabels(xt.date.dt.strftime('%Y-%m-%d'),rotation=32,fontsize=8)
 ax.set_xlim(start-.5,stop+.5)
 fig.suptitle(f'{name} ({code}) | R1-D96 원본 알고리즘 독립 검증 | 기준일 {asof} | {"이후 20거래일 공개" if expost else "기준일 당시"}\n아래 두 패널: 장기 이평선과 거래대금 (수정주가 전체 인증 전)',fontsize=12)
 fig.tight_layout(rect=[0,0,.99,.95])
 fn=f'{code}_{tag}_{"AFTER20" if expost else "ASOF"}_AUTO_SR_BB_ENV_MA.png'
 fig.savefig(OUT/'charts'/fn,dpi=150,bbox_inches='tight');plt.close(fig)
 return fn

def daily_break_scan(d,code,name,center,back=25,forward_bars=20):
 """Scan every day in fixed window with that day's data; NEVER select windows from future outcomes."""
 t=int(np.flatnonzero(d.date.eq(pd.Timestamp(center)))[0]); events=[];coverage=[]
 for k in range(max(448,t-back),min(len(d),t+forward_bars+1)):
  day=str(d.iloc[k].date.date());rows,_,selected=baseline.choose(d,day)
  hits=[r for r in selected if r['kind']=='RESISTANCE' and r['new_close_break_today']]
  coverage.append(dict(code=code,name=name,date=day,selected_count=len(selected),fresh_selected_breaks=len(hits)))
  for r in hits:
   y=float(d.iloc[k].close);prev=float(d.iloc[k-1].close)
   events.append(dict(code=code,name=name,date=day,line_id=r['line_id'],tier=r['tier'],
      h1=r['h1'],h2=r['h2'],h2_confirm_date=r['h2_confirm_date'],
      line_today=r['line_asof'],close_today=y,close_prev=prev,
      close_day_up=bool(y>prev),fresh_selected_break=True,
      warning_wick_or_shock=r['resistance_structure_warning'],
      source='PIT scan R1D96 FROZEN ALGORITHM'))
 return coverage,events

def run():
 score=[];sel=[];cand=[];pivs=[];frozen=[];timecheck=[];coverage=[];fresh=[];first_fresh=[]
 for code,name,asof in CASES:
  d=baseline.load(code);t=int(np.flatnonzero(d.date.eq(pd.Timestamp(asof)))[0]);
  b,r,s=baseline.choose(d,asof)
  stats=snapshot(d,asof)
  print('CASE',code,name,asof,'candidates',len(b),'selected',len(s),'stats',stats)
  score.append(dict(code=code,name=name,asof=asof,n_price_rows=len(d),candidate_lines=len(b),selected_lines=len(s),**stats))
  for z in b:cand.append(dict(code=code,name=name,**z))
  for z in r:pivs.append(dict(code=code,asof=asof,**z))
  for z in s:sel.append(dict(code=code,name=name,independent_label=asof_status(z,asof),**z))
  frozen.extend(forward(d,asof,s))
  cov,ev=daily_break_scan(d,code,name,asof);coverage.extend(cov);fresh.extend(ev)
  draw(d,code,name,asof,s,False,tag="CENTER");draw(d,code,name,asof,s,True,tag="CENTER")
  first=next((e for e in ev if e['fresh_selected_break']),None)
  if first is not None:
   _,_,fresh_lines=baseline.choose(d,first['date'])
   draw(d,code,name,first['date'],fresh_lines,False,tag='FIRSTFRESH')
   draw(d,code,name,first['date'],fresh_lines,True,tag='FIRSTFRESH')
   chosen=next(z for z in fresh_lines if z['line_id']==first['line_id'])
   first_fresh.append(dict(code=code,name=name,first_fresh_date=first['date'],h1=chosen['h1'],h2=chosen['h2'],
        tier=chosen['tier'],line_value=chosen['line_asof'],close=chosen['close_asof'],
        first_fresh_warning=chosen['resistance_structure_warning'],
        distance_pct=chosen['close_minus_line_pct']))
  # Backdating/Future-taint: altering prices after snapshot must not change the selected set at asof.
  altered=d.copy()
  future=altered.index>t
  for col in ['open','high','low','close']:
   altered[col]=altered[col].astype(float)
   altered.loc[future,col]=altered.loc[future,col]*1.217
  _,_,s2=baseline.choose(altered,asof)
  frozen_id=[(x['line_id'],x['tier'],x['rank_score']) for x in s]
  altered_id=[(x['line_id'],x['tier'],x['rank_score']) for x in s2]
  assert frozen_id==altered_id,(code,'lookahead detected')
  timecheck.append(dict(code=code,asof=asof,future_tamper_selection_identical=True,selected=len(s)))
 for filename,records in [('R1D97_ASOF_INDICATORS.csv',score),('R1D97_ALL_LINES.csv',cand),('R1D97_SELECTED_LINES_WITH_WARNINGS.csv',sel),('R1D97_CONFIRMED_PIVOTS.csv',pivs),('R1D97_FROZEN_FORWARD_20D.csv',frozen),('R1D97_PIT_TAMPER_TEST.csv',timecheck),('R1D97_DAILY_SCAN_COVERAGE.csv',coverage),('R1D97_DAILY_FRESH_BREAK_EVENTS.csv',fresh),('R1D97_FIRST_FRESH_PER_STOCK.csv',first_fresh)]:
  pd.DataFrame(records).to_csv(OUT/filename,index=False,encoding='utf-8-sig')
 audit=dict(version='R1D97_THREE_NEW_STOCKS_FROZEN_R1D96_REVIEW',research_only=True,
       original_algorithm='R1D96 exact copied from latest locally tested source; no code logic modified',cases=3,
       selected_lines=len(sel),candidate_lines=len(cand),price_basis='raw OHLCV, corporate-action completeness not verified',
       source_shas={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((ROOT/'data').glob('*.csv'))},
       no_lookahead_future_tamper=True,readiness='CASE_STUDY_ONLY_NOT_LIVE_NOT_FULLMARKET',
       output_chart_count=12,asof_dates=[a for _,_,a in CASES],daily_window_scanned=len(coverage),fresh_selected_break_rows=len(fresh));
 (OUT/'R1D97_AUDIT.json').write_text(json.dumps(audit,ensure_ascii=False,indent=2),encoding='utf-8')
 print('SUMMARY',json.dumps({k:audit[k] for k in ['selected_lines','candidate_lines','output_chart_count']}))
if __name__=='__main__':run()
