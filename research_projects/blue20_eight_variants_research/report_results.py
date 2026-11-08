from pathlib import Path
import sys,os,json,re
import pandas as pd
import numpy as np
H=Path(__file__).resolve().parent;O=H/'results';sys.path.insert(0,str(H/'vendor'))
from loader import load_prices
def md(d):
 return '\n'.join(['|'+'|'.join(map(str,d.columns))+'|','|'+'|'.join(['---']*len(d.columns))+'|']+['|'+'|'.join(f'{v:.2f}' if isinstance(v,(float,np.floating)) else str(v) for v in row)+'|' for row in d.itertuples(index=False,name=None)])
def main():
 s=pd.read_csv(O/'SUMMARY.csv');t=pd.read_csv(O/'TRADES.csv.gz',dtype={'code':str},low_memory=False);a=pd.read_csv(O/'SIGNALS.csv.gz',dtype={'code':str});q=s[s.scope.eq('NONOVERLAP60')&s.band.eq('UPPER')&s.year.ne('ALL_YEARS')&s.target_pct.isin([5,20])].copy();q['setting']=np.where(q.model.str.contains('S18'),'1.8 / 이동20','2 / 이동26');q['period']=q.model.str.extract(r'BB(\d+)').astype(int)
 market=pd.read_csv(O/'LISTING_MARKET.csv');market=market[market.scope.eq('NONOVERLAP60')&market.band.eq('UPPER')&market.year.eq('ALL_YEARS')&market.target_pct.eq(20)]
 report=['# BB20·25·35·40 × 두 설정, 총8가지',
'기존4가지(배수2·26시장일이동)는보존했고추가4가지(배수1.8·20시장일이동)를별도모형으로계산했다. 기간은각각20/25/35/40. 표준편차는모표준편차ddof0. 새묶음은배수와이동을동시에바꾸므로배수1.8하나의효과로해석하지않는다. 몸통돌파·하락선조건은기존과동일하다.',
'500종목2023~2024전체날짜에서8가지신호20882건을재탐지했다(상하단모두). 같은날·같은종목이여러모형에등장하므로독립20882종목/기회로풀링하지않는다. 첫신호다음시장시가매수,초기손절없음,5/10/20/40%익절,각매수일부터최대60시장일,미도달마지막종가,비용0.25%p. 주표는완전60일자료와모형별60일신호중복제거. 미래관찰부족/누락은실패로바꾸지않는다.',
'## 연도별 상단 결과',md(q[['period','setting','year','target_pct','n','tp_pct','mean_net','mean_hold']].sort_values(['period','setting','target_pct','year'])),
'tp_pct=목표접촉률,mean_net=목표미도달종가청산까지포함한평균순수익,mean_hold=평균보유시장일. 각설정의신호가다르므로단순최고평균을단독요소의인과우위로해석하지않는다. 10/40%와하단·전체원시신호·8모형합집합중복제거는SUMMARY.csv에공개했다.',
'새설정BB35는20%목표평균이2023 +0.43%,2024 +0.11%로두해양수였다. 그러나기존BB35는2023 +1.09%,2024 −0.76%여서매년같은방향의개선은아니다. 새BB20/25/40의2024 20%목표평균은각각−2.41/−1.67/−1.82%. 전체적으로새설정이더좋다는결론은아니다.',
'5%익절도새BB35가2023 +0.32%,2024 −0.50%로2024손실이남았다. 높은도달률이큰미도달손실을상쇄한다는보장은없다. 추가조건을최적화해이결과를실전검색식에반영하지않았다.',
'## 코스피·코스닥 분리',md(market[['listing_market','model','n','tp_pct','mean_net']]),
'새BB35의통합20%평균은코스피+0.99%,코스닥−1.66%로달랐다. 따라서두연도전체평균이양수라는사실만으로전체시장공통전략으로확정하면안된다. 시장구분은신호일의보관MarketId로결합,누락0이다. 공식이전상장공시의독립전수인증은아니다.',
'## 신호 전 눌림과 공통 신호',
'PULLBACK.csv는직전20시장일조정폭과상승후눌림정의를직전연구와동일하게유지한표다. 엔벨로프중심선선행40일은ENV_PRIOR.csv,같은날함께발생한모형수는COMMON_SIGNALS.csv. 새8모형합집합의중복제거는기존4모형합집합과선택기회가달라질수있으므로기준선불변검사는원시신호/수익/모형별중복제거를기준으로했다. 같은날같은가격에매수한공통신호는수익이동일한것이정상이다.',
'## 차트·검증·실행상태',
'지정8종목차트는기존2·26묶음과새1.8·20묶음을위아래패널로분리해각기간의상단선을동일가격축으로표시했다. 기준검은선은이전BB20신호일이며다른7모형도이날신호였다는뜻은아니다. 각모형의신호점은컬러원과툴팁,전수신호원장으로대조한다. 회색오른쪽은기준일당시미래영역이다. 차트는보관일봉이며HTS독립수치대조/화면시각검수는미완료다.',
'원본500종목정규화가격해시확인,기존20/40전체신호4915개·10%성과재현,직전4모형전체신호9852개와4목표39408행의체결일/수익전수일치PASS. 새설정경계·배수·시프트·과거접두불변4검사PASS. 기존4모형연구폴더와실전LIVE/SEARCH/WATCH/R1-D118변경없음. 고정500생존편향/PIT전체시장한계,수정주가·기업행사UNVERIFIED,2025~2026독립검증없음.',
'이번결과는로컬계산이며신규GitHub실행미완료다. 독립업로드패치는공통워크플로에blue20_eight_variants선택지와독립작업만추가하고기존본문복원해시검사를포함한다. 실제적용경로.github/workflows/stockhunter_r1d98_state_structure_research_only.yml,루트동일사본동봉. artifact=BLUE20_EIGHT_VARIANTS_RESEARCH_ONLY. 기준main b73e0ee102c5800998774edcfce5e3aa8cb92fdf가달라졌으면최신YML과WORKFLOW.patch대조필요. 업로드/원격실행완료여부는실제상태를따른다.']
 (H/'REPORT_KO.md').write_text('\n\n'.join(report),encoding='utf-8');(O/'REPORT_KO.md').write_text('\n\n'.join(report),encoding='utf-8')
 manifest=Path(os.environ.get('BLUE20_MANIFEST',str(Path(os.environ.get('TEMP','/tmp'))/'stock-hunter-blue20-full-replay/research_projects/blue20_multifactor_research/.auto500_work/r118_inputs/manifest.csv')));f=load_prices(manifest)
 cases={('028300','2023-02-20'):'HLB',('145990','2023-05-08'):'삼양사',('383220','2024-07-17'):'F&F',('180640','2023-09-14'):'한진칼',('006220','2024-01-24'):'제주은행',('222080','2024-03-05'):'씨아이에스',('038500','2024-06-05'):'삼표시멘트',('034120','2023-02-07'):'SBS'}
 page=['<!doctype html><meta charset="utf-8"><style>body{font:16px Arial;max-width:1350px;margin:auto;padding:24px}svg{width:100%}td,th{padding:4px;border:1px solid #ccc}</style><h1>BB 8가지 상단선 비교</h1><p>위=기존배수2·이동26,아래=추가배수1.8·이동20. 색은기간20파랑/25초록/35주황/40보라. 검은기준일은이전BB20신호일. 컬러원은각기간의실제신호일(마우스상세),회색은기준일이후. 기간별겹친신호를독립표본으로보지않습니다.</p>'];colors={20:'#2674b5',25:'#16824a',35:'#d08a16',40:'#9945a8'}
 for (code,date),name in cases.items():
  d=f[code].copy();j=int(d.index[d.date.eq(date)][0]);start=max(0,j-45);end=min(len(d),j+61)
  for group,mult,shift in [('BASE',2.,26),('NEW',1.8,20)]:
   for n in [20,25,35,40]:
    r=d.close.rolling(n);d[f'{group}{n}']=(r.mean()+mult*r.std(ddof=0)).shift(shift)
  w=d.iloc[start:end].reset_index(drop=True);cols=[f'{g}{n}' for g in ['BASE','NEW'] for n in [20,25,35,40]];lo=min(w.low.min(),w[cols].min().min());hi=max(w.high.max(),w[cols].max().max());span=max(1,hi-lo);x=lambda k:65+1100*k/max(1,len(w)-1);svg=['<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1280 790">']
  for panel,group in enumerate(['BASE','NEW']):
   y=lambda p:330+panel*380-270*(p-lo)/span;svg.append(f'<rect x="{x(j-start)}" y="{45+panel*380}" width="{1165-x(j-start)}" height="295" fill="#eee"/>')
   for k,b in w.iterrows():
    col='#c44' if b.close>=b.open else '#2674b5';xx=x(k);svg.append(f'<g><title>{b.date} O={b.open} H={b.high} L={b.low} C={b.close}</title><path d="M{xx} {y(b.high)}V{y(b.low)}" stroke="{col}"/><rect x="{xx-2}" y="{y(max(b.open,b.close))}" width="4" height="{max(1,abs(y(b.open)-y(b.close)))}" fill="{col}"/></g>')
   for n in [20,25,35,40]:
    field=f'{group}{n}';col=colors[n];pts=' '.join(f'{x(k)},{y(p)}' for k,p in enumerate(w[field]) if pd.notna(p));svg.append(f'<polyline points="{pts}" fill="none" stroke="{col}" stroke-width="1.5"/>');model='BB'+str(n) if group=='BASE' else 'BB'+str(n)+'_S18_SHIFT20'
    for r in a[a.code.eq(code)&a.band.eq('UPPER')&a.model.eq(model)&a.date.isin(set(w.date))].itertuples():
     k=int(w.index[w.date.eq(r.date)][0]);svg.append(f'<circle cx="{x(k)}" cy="{y(w.high.iloc[k])-5-(n%4)*3}" r="3" fill="{col}"><title>{model} SIGNAL {r.date}</title></circle>')
   for p in [lo,(lo+hi)/2,hi]:svg.append(f'<text x="2" y="{y(p)}" font-size="12">{p:,.0f}</text>')
   svg.append(f'<path d="M{x(j-start)} {45+panel*380}V{340+panel*380}" stroke="black"/><text x="65" y="{25+panel*380}">{group}: {"2 x SD / shift26" if group=="BASE" else "1.8 x SD / shift20"}</text><text x="65" y="{365+panel*380}">{w.date.iloc[0]} → {w.date.iloc[-1]}</text>')
  svg.append('</svg>');page.extend([f'<h2>{name} {code} / 기준 {date}</h2>',''.join(svg)]);w.to_csv(O/f'CASE_{code}_{date}_LINES.csv',index=False)
 page.append('</html>');(O/'GALLERY.html').write_text('\n'.join(page),encoding='utf-8')
 import xml.etree.ElementTree as ET
 for svg in re.findall(r'<svg.*?</svg>',''.join(page),re.S):ET.fromstring(svg)
 (O/'REPORT_VERIFICATION.json').write_text(json.dumps(dict(status='PASS',charts_structure_checked=8,hts_independent_match='NOT_DONE',settings=8),indent=2),encoding='utf-8');print('eight-setting Korean report and chart gallery ready')
if __name__=='__main__':main()
