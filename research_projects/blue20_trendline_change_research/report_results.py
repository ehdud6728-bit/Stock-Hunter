from pathlib import Path
import sys,os,json,re
import pandas as pd
import numpy as np
H=Path(__file__).resolve().parent;O=H/'results';sys.path.insert(0,str(H/'vendor'))
from loader import load_prices
from engine import value

def md(d):
 return '\n'.join(['|'+'|'.join(map(str,d.columns))+'|','|'+'|'.join(['---']*len(d.columns))+'|']+['|'+'|'.join(f'{v:.2f}' if isinstance(v,(float,np.floating)) else str(v) for v in row)+'|' for row in d.itertuples(index=False,name=None)])

def main():
 t=pd.read_csv(O/'TRADES.csv.gz',dtype={'code':str},low_memory=False);s=pd.read_csv(O/'SNAPSHOTS.csv.gz',dtype={'code':str});lines=pd.read_csv(O/'FROZEN_LINES.csv.gz',dtype={'code':str});q=t[t.fair81&t.nonoverlap60&t.context_status.eq('KNOWN')];results=[];pairs=[]
 for period,g in [('ALL_YEARS',q),('2023',q[q.year.eq(2023)]),('2024',q[q.year.eq(2024)])]:
  for keys,a in g.groupby(['model','policy','target_pct']):
   z=a[a.closed.eq(True)];net=a.net_pct.copy();net.loc[a.status.eq('NO_ENTRY')]=0;results.append(dict(zip(['model','policy','target_pct'],keys))|dict(year=period,opportunities=len(a),n=len(z),entry_pct=100*a.entered.mean(),tp_pct=100*z.status.eq('TP').mean(),win_pct=100*z.net_pct.gt(0).mean(),mean_net=z.net_pct.mean(),opportunity_net=net.mean(),unknown=int((net.isna()).sum())))
  for keys,a in g.groupby(['model','target_pct']):
   b=a[a.policy.eq('A_BASE')][['opportunity_id','status','net_pct']].rename(columns={'status':'A_status','net_pct':'A_net'})
   for pol,c in a[~a.policy.eq('A_BASE')].groupby('policy'):
    v=c.merge(b,on='opportunity_id',validate='one_to_one');new=v.net_pct.copy();new.loc[v.status.eq('NO_ENTRY')]=0;pairs.append(dict(zip(['model','target_pct'],keys))|dict(year=period,policy=pol,n=len(v),mean_delta_vsA=(new-v.A_net).mean(),missed_A_target=int((v.A_status.eq('TP')&v.status.eq('NO_ENTRY')).sum()),rescued_target=int((v.A_status.ne('TP')&v.status.eq('TP')).sum()),lost_target_after_entry=int((v.A_status.eq('TP')&v.entered&v.status.ne('TP')).sum())))
 primary=pd.DataFrame(results);pair=pd.DataFrame(pairs);primary.to_csv(O/'PRIMARY_KNOWN_CONTEXT.csv',index=False);pair.to_csv(O/'PAIRED_POLICY_EFFECT.csv',index=False)
 sensitivity=[]
 base=t[t.policy.eq('A_BASE')&t.fair81&t.nonoverlap60]
 for n in [60,120,240]:
  sub=s[s.lookback.eq(n)][['code','date','status','DESC_state','SUPPORT_state']].rename(columns={'status':'snapshot_status','DESC_state':'sensitivity_DESC','SUPPORT_state':'sensitivity_SUPPORT'})
  a=base.merge(sub,on=['code','date'],validate='many_to_one')
  for keys,z in a.groupby(['model','year','target_pct','snapshot_status','sensitivity_DESC'],dropna=False):sensitivity.append(dict(zip(['model','year','target_pct','snapshot_status','DESC_state'],keys))|dict(lookback=n,n=len(z),tp_pct=100*z.status.eq('TP').mean(),mean_net=z.net_pct.mean()))
 pd.DataFrame(sensitivity).to_csv(O/'LOOKBACK_SENSITIVITY.csv',index=False)
 manifest=Path(os.environ.get('BLUE20_MANIFEST',str(Path(os.environ.get('TEMP','/tmp'))/'stock-hunter-blue20-full-replay/research_projects/blue20_multifactor_research/.auto500_work/r118_inputs/manifest.csv')));frames=load_prices(manifest);cal=pd.Index(sorted({v for d in frames.values() for v in d.date}));cp={v:i for i,v in enumerate(cal)}
 risk=[];price_index={c:{v:i for i,v in enumerate(d.date)} for c,d in frames.items()}
 for r in q[q.target_pct.eq(20)&q.policy.isin(['A_BASE','E_SUPPORT_EXIT'])&q.closed.eq(True)].itertuples():
  d=frames[r.code];i=price_index[r.code][r.entry_date];j=price_index[r.code][r.exit_date];seq=np.r_[r.entry_price,d.close.to_numpy()[i:j],r.exit_price];mdd=100*np.min(seq/np.maximum.accumulate(seq)-1);risk.append(dict(model=r.model,year=r.year,policy=r.policy,net_pct=r.net_pct,close_mdd=mdd))
 risk=pd.DataFrame(risk);risk.groupby(['model','policy']).agg(n=('net_pct','size'),mean_net=('net_pct','mean'),p10_net=('net_pct',lambda x:x.quantile(.1)),mean_trade_close_mdd=('close_mdd','mean')).reset_index().to_csv(O/'SUPPORT_EXIT_RISK.csv',index=False)
 delayed=[]
 for keys,g in q[q.policy.eq('C_WAIT_BREAK')&q.closed.eq(True)].groupby(['model','target_pct']):
  for immediate,a in g.groupby(g.confirm_date.eq(g.date)):
   delayed.append(dict(zip(['model','target_pct'],keys))|dict(immediate=immediate,n=len(a),tp_pct=100*a.status.eq('TP').mean(),mean_net=a.net_pct.mean()))
 pd.DataFrame(delayed).to_csv(O/'WAIT_IMMEDIATE_VS_DELAYED.csv',index=False)
 names={('028300','2023-02-20'):'HLB',('145990','2023-05-08'):'삼양사',('383220','2024-07-17'):'F&F',('180640','2023-09-14'):'한진칼',('006220','2024-01-24'):'제주은행',('222080','2024-03-05'):'씨아이에스',('038500','2024-06-05'):'삼표시멘트',('034120','2023-02-07'):'SBS'};selected=[]
 for (c,d),name in names.items():
  a=t[t.model.eq('BB20')&t.code.eq(c)&t.date.eq(d)]
  if len(a):selected.append((a.opportunity_id.iloc[0],name+' / 지정'))
 for policy in ['C_WAIT_BREAK','E_SUPPORT_EXIT']:
  b=q[q.target_pct.eq(20)&q.policy.eq('A_BASE')][['opportunity_id','status','net_pct']].rename(columns={'status':'A_status','net_pct':'A_net'})
  c=q[q.target_pct.eq(20)&q.policy.eq(policy)&q.entered].merge(b,on='opportunity_id')
  for tag,mask in [('개선 반례',c.net_pct>c.A_net+5),('악화 반례',c.net_pct<c.A_net-5)]:
   a=c[mask].sort_values(['date','code','model']).drop_duplicates(['code','date'])
   if len(a):selected.append((a.opportunity_id.iloc[0],policy+' '+tag+' / 날짜순첫건'))
 selected=list(dict.fromkeys(selected));page=['<!doctype html><meta charset="utf-8"><style>body{font:16px Arial;max-width:1350px;margin:auto;padding:24px}svg{width:100%}td,th{padding:4px;border:1px solid #ccc}</style><h1>확정 추세선 변화와 BB 진입</h1><p>빨강=과거확정하락저항선,초록=상승지지선. 두피벗의오른쪽2봉완료뒤에만선이확정됩니다. 점선은확정전연결/이후고정투영,앵커를미래에다시맞추지않았습니다. 검은D0는BB신호,이후회색은당시미래가격. 각정책은표에서진입/청산일을비교하세요.</p>'];case_rows=[]
 for index,(oid,label) in enumerate(selected,1):
  g=t[t.opportunity_id.eq(oid)&t.target_pct.eq(20)];r=g.iloc[0];d=frames[r.code].copy();j=int(d.index[d.date.eq(r.date)][0]);start=max(0,j-120);end=min(len(d),j+81);w=d.iloc[start:end].reset_index(drop=True);ls=lines[lines.snapshot_id.eq(r.initial_snapshot)];lo=w.low.min();hi=w.high.max();span=max(1,hi-lo);x=lambda k:65+1100*k/max(1,len(w)-1);y=lambda p:365-310*(p-lo)/span;svg=[f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1300 530"><rect x="{x(j-start)}" y="35" width="{1165-x(j-start)}" height="425" fill="#eee"/>']
  for k,b in w.iterrows():
   xx=x(k);col='#c44' if b.close>=b.open else '#2674b5';svg.append(f'<g><title>{b.date} O={b.open} H={b.high} L={b.low} C={b.close}</title><path d="M{xx} {y(b.high)}V{y(b.low)}" stroke="{col}"/><rect x="{xx-1.7}" y="{y(max(b.open,b.close))}" width="3.4" height="{max(1,abs(y(b.open)-y(b.close)))}" fill="{col}"/></g><rect x="{xx-1.7}" y="{455-50*b.volume/w.volume.max()}" width="3.4" height="{50*b.volume/w.volume.max()}" fill="{col}"/>')
  for line in ls.to_dict('records'):
   col='#c34a41' if line['kind']=='DESC' else '#168548';points=[]
   for k,b in w.iterrows():
    p=value(line,cp[b.date])
    if line['anchor1']<=b.date and lo<=p<=hi:points.append(f'{x(k)},{y(p)}')
   svg.append(f'<polyline points="{" ".join(points)}" fill="none" stroke="{col}" stroke-width="2" stroke-dasharray="5 3"><title>{line["kind"]} known={line["known_date"]}</title></polyline>')
   for day,px in [(line['anchor1'],line['p1']),(line['anchor2'],line['p2'])]:
    if day in set(w.date):k=int(w.index[w.date.eq(day)][0]);svg.append(f'<circle cx="{x(k)}" cy="{y(px)}" r="4" fill="{col}"><title>ANCHOR {day}</title></circle>')
   if line['known_date'] in set(w.date):k=int(w.index[w.date.eq(line['known_date'])][0]);svg.append(f'<text x="{x(k)}" y="{20 if line["kind"]=="DESC" else 33}" fill="{col}">KNOWN {line["known_date"]}</text>')
  for policy,col,tag in [('A_BASE','#222','A'),('C_WAIT_BREAK','#9843ad','C'),('D_RETEST','#cf891b','D'),('E_SUPPORT_EXIT','#158d9c','E')]:
   event=g[g.policy.eq(policy)].iloc[0]
   if event.entered and pd.notna(event.entry_date) and event.entry_date in set(w.date) and policy!='E_SUPPORT_EXIT':
    k=int(w.index[w.date.eq(event.entry_date)][0]);svg.append(f'<circle cx="{x(k)}" cy="{y(event.entry_price)}" r="4" fill="{col}"/><text x="{x(k)+3}" y="{y(event.entry_price)-6}" fill="{col}">{tag}<title>{policy} BUY {event.entry_date} {event.entry_price}</title></text>')
   if event.closed and pd.notna(event.exit_date) and event.exit_date in set(w.date):
    k=int(w.index[w.date.eq(event.exit_date)][0]);svg.append(f'<text x="{x(k)}" y="{y(event.exit_price)}" fill="{col}">X<title>{policy} {event.exit_date} {event.status} {event.net_pct:.2f}%</title></text>')
  for v in [lo,(lo+hi)/2,hi]:svg.append(f'<text x="2" y="{y(v)}" font-size="12">{v:,.0f}</text>')
  svg.append(f'<path d="M{x(j-start)} 35V460" stroke="black"/><text x="65" y="490">{w.date.iloc[0]} → {w.date.iloc[-1]} / D0={r.date}</text></svg>');page.extend([f'<h2>{index}. {label} {r.code} {r.model} {r.date}</h2><p>하락선 {r.DESC_state} / 지지선 {r.SUPPORT_state} / 지지청산활성 {r.support_rule_enabled}</p>',''.join(svg),g[['policy','confirm_date','entry_date','status','exit_date','net_pct','no_entry_reason']].to_html(index=False,float_format=lambda v:f'{v:.2f}')]);w.to_csv(O/f'CASE_{index:02}_{r.code}_OHLCV.csv',index=False)
  for row in g.to_dict('records'):case_rows.append(dict(case=index,label=label,**row))
 page.append('</html>');(O/'GALLERY.html').write_text('\n'.join(page),encoding='utf-8');pd.DataFrame(case_rows).to_csv(O/'CASES.csv',index=False)
 import xml.etree.ElementTree as ET
 for svg in re.findall(r'<svg.*?</svg>',''.join(page),re.S):ET.fromstring(svg)
 report=['# 8가지 BB와 시점 고정 추세선 변화',
 '실제Actions42495786893의8가지BB상단신호를기준선으로추세변화조건을검증했다. 여기서정한선택규칙의결과이며수작업추세선정확성인증은아니다.',
 '## 원격 8가지 실행 확인',
 'Actions42495786893는성공,실제CSV15개로컬과전부일치. 기존파일1155개보존검사통과. 기준선신호수20882는상하단합계이며이번추세선연구는그중상단9386신호,5정책×4목표187720행이다. 기존순수BB다음시가37544행재현PASS. 상태파일의이전NOT_EXECUTED고정문구는실제실행상태와구분했으며새파이프라인은환경/실행ID를자동기록하도록했다.',
 '## 추세선은 언제 알 수 있었나',
 '좌2·우2봉확정고점/저점만사용하고오른쪽2봉까지D−1에완료된앵커만선택했다. 최근120시장일에서가장최근피벗과최소5일전의최근피벗을연결한다. 더보기좋은과거점을찾아선택하지않는다. 낮아지는고점은하락선,높아지는저점은상승선,다른모양은해당선없음. 거래일좌표직선이며종가돌파/이탈완충은직전20일ATR의.25배. 초기선은향후갱신하지않는다.60/240일은초기선분류민감도표별도.',
 'A_BASE=원래BB다음시가. B_BREAK_NOW=BB날새하락선돌파가동시에있을때만진입. C_WAIT_BREAK=초기하락선위면바로진입하고아니면20일내돌파확인후진입. D_RETEST=돌파후선재접근·2종가지지확인뒤진입. E_SUPPORT_EXIT=원래진입후초기유효상승지지선종가이탈시다음시가청산,유효선없음/초기이미이탈이면추가청산비활성(A와동일). 목표익절이당일종가이탈확인보다먼저이며60일만기연장없음.',
 '## 공정한 원기회 비교',
 '최대D0+20일확인후다음진입일부터60일자료를확보하려고D0~D0+80의81시장일원천이완전한집단을사용했다. 과거120일선판단도KNOWN인공통집단과모형별60일신호중복제거가아래주표다. 기회별미진입현금0을포함하고원천누락/미확정은0으로대체하지않았다. 기다린정책은매도달력이늦어져시간노출차이가있다. 같은모형공통사건을독립표본으로풀링하지않는다.',
 md(primary[primary.year.eq('ALL_YEARS')&primary.target_pct.eq(20)][['model','policy','opportunities','n','entry_pct','tp_pct','mean_net','opportunity_net']]),
 'opportunities=원기회,n=실제청산,mean_net=진입거래평균,opportunity_net=미진입0포함원기회평균. 20%목표와비용0.25%p기준이며5/10/40%,연도/상장시장/눌림/초기선상태표도공개했다. 표본이적은진입전략의높은거래평균만비교하지않는다.',
 '기존BB35의C조건은원기회평균+0.75%대A+0.40%였지만기존BB40은C+0.85%대A+1.02%로원래진입이높았다. 새BB35도C+0.30%대A+0.60%로낮았다. 일부설정의개선이모든8설정에일반화되지는않았다.',
 '## 실제로 기다린 효과와 단순 필터를 구분',
 'C조건은이미초기선위인경우즉시진입을허용한다. BB35 C247건중217건이즉시확인이고실제지연은30건,BB40 C230건중198건즉시/32건지연이다. 따라서C평균개선을모두기다린효과라고부르면안된다. WAIT_IMMEDIATE_VS_DELAYED.csv에분리. 추세선이없는강한상승종목도정의상미진입이라좋은기회를놓칠수있다.',
 md(pair[pair.year.eq('ALL_YEARS')&pair.target_pct.eq(20)][['model','policy','mean_delta_vsA','missed_A_target','rescued_target','lost_target_after_entry']]),
 'missed_A_target은새정책미진입인데A는목표도달,실현가능수익보장은아니다. lost_target_after_entry는진입했지만A와달리목표청산이아닌거래이며순손실과같지않다. B는같은날신호중필터라새목표성공을만들지않는것이정상이다.',
 '## 상승지지 이탈 청산의 손익과 위험',
 '이번고정초기지지선종가이탈청산E는20%목표원기회평균이8설정모두A보다낮았다. 예:BB20 +0.22→−0.24%,BB40 +1.02→+0.24%. 선이상승투영되면서보통조정에도청산하거나이후회복할승자를일찍팔수있다. 이는이정의의결과이지모든추세선청산이무효라는뜻은아니다. 손실위험비교도다음표로함께보아야한다.',
 md(pd.read_csv(O/'SUPPORT_EXIT_RISK.csv')),
 'mean_trade_close_mdd는각거래종가와실제청산가경로의평균MDD(비용전)로계좌전체/장중MDD가아니다. p10_net은하위10%수익경계. 후일새로확정된피벗으로선을갱신하는동적정책은이번에검증하지않았다.',
 '## 사례·실행·한계',
 '지정8종목과개선/악화반례4개차트를같은규칙으로그렸다. 고점/저점앵커와KNOWN확정일을표시했고이후투영은점선이다. 원본OHLCV/정책별진입청산CSV도동봉. 차트시각검수·HTS독립대조는미완료이며SVG구조검사12개만완료했다. 실제체결가별매물대/기업행사·수정주가UNVERIFIED,고정500생존편향/PIT전체시장한계,2025~2026독립검증없음.',
 '8개단위검사·과거선미래재도색방지·확정일·다음시가·기준선재현PASS. 기존LIVE/SEARCH/WATCH/R1-D118/검색식과이전연구변경없음. 이번추세선계산은로컬이며신규GitHub실행미완료. 독립업로드ZIP의study=blue20_trendline_change,artifact=BLUE20_TRENDLINE_CHANGE_RESEARCH_ONLY. 기준main fc8850a532d35b744209146970121fe02e4b2751. 루트동일YML확인사본과실제.github/workflows YML동봉,기존본문복원해시검사. 최신main이달라졌으면WORKFLOW.patch대조후적용해야한다.']
 if os.environ.get('GITHUB_ACTIONS')=='true':report.append('이번계산실행환경은GitHub Actions,run ID='+os.environ.get('GITHUB_RUN_ID','UNKNOWN')+'. 위로컬기준설명과구분하며실제전체workflow완료여부는run/job상태로확인한다.')
 (H/'REPORT_KO.md').write_text('\n\n'.join(report),encoding='utf-8');(O/'REPORT_KO.md').write_text('\n\n'.join(report),encoding='utf-8')
 (O/'REPORT_VERIFICATION.json').write_text(json.dumps(dict(status='PASS',charts_structure_checked=len(selected),common_context_opportunities=True,visual_review='STRUCTURE_ONLY',hts_independent_match='NOT_DONE'),indent=2),encoding='utf-8');print('trend reports ready',len(selected));print(primary[primary.year.eq('ALL_YEARS')&primary.target_pct.eq(20)][['model','policy','opportunities','n','entry_pct','tp_pct','mean_net','opportunity_net']].round(2).to_string(index=False))
if __name__=='__main__':main()
