from pathlib import Path
from html import escape
import argparse,json
import numpy as np
import pandas as pd
from analyze import loader,module
HERE=Path(__file__).resolve().parent
CASES={('028300','2023-02-20'):'HLB',('145990','2023-05-08'):'삼양사',('383220','2024-07-17'):'F&F',('180640','2023-09-14'):'한진칼',('006220','2024-01-24'):'제주은행',('222080','2024-03-05'):'씨아이에스',('038500','2024-06-05'):'삼표시멘트',('034120','2023-02-07'):'SBS'}
def table(d):
    labels={'line':'저점 규칙','year':'연도','defined':'정의 건수','d0_low_hold_pct':'D0 저가 유지%','close_hold20_pct':'20일 종가 유지율%','ever_close_breach_pct':'20일 내 종가 이탈%','stop':'손절선','n':'진입 건수','entry_pct':'진입률%','mean_net':'평균 순수익%','median_net':'중앙 순수익%','stop_pct':'손절률%','mean_distance':'손절 거리%','worst_net':'최악 순수익%','mean_R':'평균 R','close_MDD':'평균 종가 MDD%','miss10':'미진입 +10 경로','miss20':'미진입 +20 경로','grade':'과거 시험 횟수','BREAK':'돌파','REJECT':'반락','OTHER':'그 외','PENDING':'미확인'}
    d=d.rename(columns=labels)
    cols=list(d.columns);lines=['|'+'|'.join(cols)+'|','|'+'|'.join(['---']*len(cols))+'|']
    for row in d.itertuples(index=False,name=None):lines.append('|'+'|'.join('—' if pd.isna(v) else f'{v:.2f}' if isinstance(v,(float,np.floating)) else str(v) for v in row)+'|')
    return '\n'.join(lines)
def main(manifest):
    out=HERE/'results';t=pd.read_csv(out/'TRADES.csv.gz',dtype={'code':str});f=pd.read_csv(out/'TREND_FEATURES.csv.gz',dtype={'code':str});touch=pd.read_csv(out/'FROZEN_RESISTANCE_TOUCHES.csv.gz',dtype={'code':str})
    report=['# BLUE20 반복 저항 신뢰도·추세 저점 손절 탐색 결과','기존 500종목 1,804사건을 보존한 독립 RESEARCH_ONLY 계산이다. 2023~2024년 규칙 개발 자료이며 독립 검증 성과가 아니다. 기존 매물대 기반 성과 확정은 계속 동결한다.','## 무결성과 해석 범위',
    '- 원본 manifest SHA256 500개를 대조했다. 기존 RSI 진입 기준 −5% 손절의 1,804건 수익률·청산일을 전수 재현했다.\n- 확인 저점은 D−1까지 뒤 2봉이 완성된 것만 사용한다. D0 이후 저점은 선 선택에 들어가지 않는다.\n- 일봉 가격 분포는 체결가별 실제 거래량이 아니다. 수정주가·기업행사 기준, 실제 체결량은 UNVERIFIED다. 이번 결과로 실제 강한 매물대 인증을 선언할 수 없다.\n- D0 이후 RSI 신호가 발생한 사건도 포함하는 기존 원장의 표본 선택을 유지했다. D1 진입 결과는 모든 BLUE20 신호의 결과가 아니며 GC0를 따로 확인해야 한다.\n- 평균 수익은 거래당 평균이고 포트폴리오 수익률이 아니다. MDD는 거래 중 일별 종가와 실제 청산가격의 drawdown이며 장중 최대 손실이나 계좌 MDD가 아니다. 청산봉 고저의 순서는 알 수 없어 청산 전 MFE/MAE는 봉 전체 범위 참고치다.',
    '## 추세 저점 유지', '저점 미정의 사건은 유지율 분모에서 제외하고 개수를 명시한다. 저가·종가 유지율은 청산 여부와 무관하게 D+1~20 전체를 본다.']
    hold=[]
    for name in ['pullback','origin']:
        for year,g in f.groupby('year'):
            a=g[g[name].notna()];hold.append(dict(line=name,year=year,defined=len(a),d0_low_hold_pct=100*a[name+'_d0_low_hold'].mean(),close_hold20_pct=100*a[name+'_close_hold20'].mean(),ever_close_breach_pct=100*a[name+'_close_breach_date'].notna().mean()))
    report.append(table(pd.DataFrame(hold)))
    report+=['## 동일 기회 비교','익절 +10%, 만기 D0+20, 비용 0.25%. 장중 동시 도달 손절 우선, 갭은 실제 시가. 종가형은 이탈 다음 시가에 청산한다. ORIGIN은 상승 저점 사슬이 두 개 이상인 시작점이다. 미정의·진입가 이하 조건을 충족하지 못한 선은 미진입이며 놓친 상승에 포함한다. 표의 miss10/20은 보유 규칙과 무관한 진입일부터 D0+20까지 고가 경로이며 실현가능 익절 성과가 아니다.']
    for timing in ['D1','ORIGINAL_GC']:
        report.append('### '+timing+' / 원선 장중형 / 전체 사건')
        a=t[(t.timing==timing)&t.buffer_atr.eq(0)&t['mode'].eq('INTRADAY')];records=[]
        for stop,g in a.groupby('stop'):
            b=g[g.eligible];records.append(dict(stop=stop,n=len(b),entry_pct=100*len(b)/len(g),mean_net=b.net.mean(),median_net=b.net.median(),stop_pct=100*b.reason.str.contains('SL').mean(),mean_distance=b.distance_pct.mean(),worst_net=b.net.min(),mean_R=b.net_R.mean(),close_MDD=b.close_mdd.mean(),miss10=g.miss10.sum(),miss20=g.miss20.sum()))
        report.append(table(pd.DataFrame(records)))
    report+=['FIXED5=고정 −5%, PULLBACK=직전 눌림 저점, ORIGIN=상승 저점 사슬 시작, RESISTANCE=이미 돌파한 존 하단. 모든 선이 동시에 진입 가능한 공통표본, 2023/2024·상단/하단, GC0, 장대양봉, 알려진 원천 이상 포함 창 제외, 중복 사건 제거, 0.25 ATR 완충·종가형 민감도는 SUMMARY.csv에 제공한다. PAIRED_COMPARISONS.csv는 구조선별로 그 선이 유효한 동일 사건의 5% 기준선과 일대일 비교한다. 공통표본은 네 선 모두 유효한 사건이어서 일반화 범위가 좁다. 구조선은 손절 폭이 달라 동일 수량 평균수익만으로 우열을 정하면 안 된다. R은 실제 순수익을 최초 손절 거리로 나눈 값이며 종가형·갭에서 −1R보다 손실이 커질 수 있다. 계좌 1% 위험 예시의 무차입 비중 min(1,1/손절거리%)를 적용한 값도 제공하며 실제 동시 보유 계좌 성과가 아니다.']
    # Source-known seven rows were carried forward by the pinned artifact.
    report+=['## 반복 저항의 과거 내 독립 재시험','120봉의 앞 80봉에서 가격 고점과 이후 반락으로 저항을 고정하고, 뒤 40봉 접근을 평가했다. 발견 종료 종가 위에 남은 가장 가까운 존만 선택한다. D0와 뒤 40봉으로 존을 재선택하지 않는다. 같은 종목·선·접근이 여러 D0에 중복될 수 있어 고유 접근도 별도 집계한다. 2회 시험과 3회 이상을 성과를 보고 바꾸지 않고 구분한다.']
    unique=touch.drop_duplicates(['code','zone_low','zone_high','touch_date']);unique.to_csv(out/'UNIQUE_RESISTANCE_TOUCHES.csv',index=False)
    for label,df in [('사건창 반복 포함',touch),('동일선·접근 중복 제거',unique)]:
        df=df.copy();df['grade']=np.where(df.discovery_tests>=3,'3+','2');summary=pd.crosstab(df.grade,df.outcome).reset_index();report+=[label,table(summary)]
    report+=['여기서 높은 반락 빈도가 관측되더라도 실제 잔존 매도 주문을 입증하지 않는다. 재시험의 반락 판정은 관측 종료 후 라벨이며 D0 매수 보류 최적화에 사용하지 않았다. 접근하지 않은 존은 반락률 분모에 들어가지 않는다. 신규 접근이 적거나 돌파가 빈번하면 강한 저항이라는 명칭을 보류해야 한다.',
    '## 원천 한계와 후속 작업','HLB 원천 기준가 2행은 권리락·배당락 근거가 있지만 시계열 전체의 가격 보정 일관성을 인증한 것은 아니다. 한진칼 거래대금 이상 1행은 시간외 대량매매로 상당 부분 설명되며 나머지 4행은 집계 범위를 확인하지 못했다. 해당 창 제외 결과는 CLEAN을 확인한다. 실제 과거 분봉·틱은 확보되지 않았다. 기존 60/120/240·구간 수 민감도는 보존되며 이번 신규 저점 규칙은 120봉 탐색·60봉 최근 저점 정의에 한정한다. 이 파라미터의 독립 검증과 point-in-time 전체 시장 확장은 아직 수행하지 않았다.',
    '## 실행 구분','로컬 원천·수익률 재현과 테스트를 실행했다. 이번 신규 연구의 GitHub Actions 실행은 하지 않았다. 실전 LIVE/SEARCH/WATCH·R1-D118 및 이전 연구 파일은 수정하지 않았다.']
    frames=loader.load_prices(Path(manifest));page=['<!doctype html><meta charset="utf-8"><title>BLUE20 추세 저점 손절</title><style>body{font:16px system-ui;max-width:1200px;margin:auto;padding:24px}svg{width:100%;background:#fafafa}table{border-collapse:collapse}td,th{border:1px solid #ccc;padding:6px}</style><h1>8종목 실제 일봉과 D0 고정 손절선</h1><p>주황: 반복 저항, 초록: 눌림 저점, 파랑: 파동 시작점, 검정: D0. 봉에 마우스를 올리면 원본 OHLCV를 볼 수 있습니다. 원선 장중형·D1 진입이며 선별 청산일은 아래 표에 표시합니다.</p>'];cases=[]
    for (code,date),name in CASES.items():
        r=f[(f.code==code)&(f.date==date)].iloc[0];d=frames[code];j=int(d.index[d.date.eq(date)][0]);start=max(0,j-120);w=d.iloc[start:j+21].reset_index(drop=True)
        lo=w.low.min();hi=w.high.max();y=lambda p:400-350*(p-lo)/(hi-lo);x=lambda i:50+1080*i/(len(w)-1)
        parts=[f'<section><h2>{escape(name)} {code} {date}</h2><svg viewBox="0 0 1320 580">']
        if pd.notna(r.resistance_low):parts.append(f'<rect x="50" y="{y(r.resistance_high)}" width="1080" height="{y(r.resistance_low)-y(r.resistance_high)}" fill="orange" opacity=".2"/>')
        for field,color in [('pullback','green'),('origin','blue')]:
            if pd.notna(r[field]):parts.append(f'<path d="M50 {y(r[field])}H1130" stroke="{color}" stroke-dasharray="5 4"/><text x="55" y="{y(r[field])-5}" fill="{color}">{field} {r[field]:.0f}</text>')
        for k,b in w.iterrows():
            color='#c33' if b.close>=b.open else '#268';xx=x(k);parts.append(f'<g><title>{b.date} O={b.open} H={b.high} L={b.low} C={b.close} V={b.volume}</title><path d="M{xx} {y(b.high)}V{y(b.low)}" stroke="{color}"/><rect x="{xx-2}" y="{y(max(b.open,b.close))}" width="4" height="{max(1,abs(y(b.open)-y(b.close)))}" fill="{color}"/></g>')
            parts.append(f'<rect x="{xx-2}" y="{560-80*b.volume/w.volume.max()}" width="4" height="{80*b.volume/w.volume.max()}" fill="{color}" opacity=".5"/>')
        proxy=module('gallery_profile',HERE/'vendor/profile.py');profile=proxy.volume_profile(d.iloc[max(0,j-120):j],32,'range')
        if profile is not None:
            for center,mass in zip(profile['centers'],profile['weights']):
                height=350*profile['width']/(hi-lo)
                parts.append(f'<rect x="1145" y="{y(center)-height/2}" width="{120*mass/max(profile["weights"])}" height="{height}" fill="gray" opacity=".5"/>')
        parts+=['<text x="1145" y="25">Daily VP proxy</text>','<text x="50" y="475">Volume</text>']
        parts.append(f'<path d="M{x(j-start)} 35V410" stroke="black"/><text x="{x(j-start)+3}" y="30">D0</text>')
        a=t[(t.code==code)&t.date.eq(date)&t.timing.eq('D1')&t.buffer_atr.eq(0)&t['mode'].eq('INTRADAY')]
        entryx=x(j+1-start);parts.append(f'<path d="M{entryx} 40V410" stroke="purple"/><text x="{entryx+3}" y="445">entry</text>')
        for q in a[a.eligible].itertuples():
            k=int(w.index[w.date.eq(q.exit_date)][0]);parts.append(f'<circle cx="{x(k)}" cy="{y(q.exit_price)}" r="4" fill="black"><title>{q.stop} EXIT {q.exit_date} {q.exit_price}</title></circle>')
        parts.append('</svg>');cols=['band','stop','eligible','line','distance_pct','exit_date','reason','net'];parts.append(a[cols].to_html(index=False,float_format=lambda v:f'{v:.2f}'));parts.append(f'<p>눌림 확인일 {r.known_date} / 사슬 {r.chain} / 저항 분류 {r.resistance_state}</p></section>');page+=parts
        original=t[(t.code==code)&t.date.eq(date)&t.timing.eq('ORIGINAL_GC')&t.buffer_atr.eq(0)&t['mode'].eq('INTRADAY')]
        page+=['<p>기존 RSI 확인 후 진입 비교: 위 차트의 D1과 진입일이 다를 수 있습니다.</p>',original[['band','stop','entry_date','exit_date','reason','net']].to_html(index=False,float_format=lambda v:f'{v:.2f}')]
        for row in a.to_dict('records'):cases.append({'name':name,**row})
    page.append('</body>');(out/'GALLERY.html').write_text('\n'.join(page),encoding='utf-8');pd.DataFrame(cases).to_csv(out/'CASES.csv',index=False)
    appendix=out/'RELIABILITY_APPENDIX.md'
    if appendix.exists():report.append(appendix.read_text(encoding='utf-8'))
    report+=['## 판단', '추세 시작 저점이 유지되는 비율은 높지만 선이 멀수록 자연히 이탈이 적어지는 효과가 있다. 현재 원선 장중형에서 시작 저점의 평균 손절 거리는 약 13~14%이며 고정 5% 대비 우월성을 확정할 수 없다. SBS처럼 저점 손절이 반등을 기다릴 여지를 주는 사례와 F&F·한진칼처럼 손실을 키우는 반례를 동일 규칙으로 함께 제공한다. 과거 실패 4건에 맞춘 조건 조정은 하지 않았다.\n\n추가 발견한 큰 가격 단절은 기업행사 가격 보정과 보유 주식 수를 반영하지 않은 손익이 왜곡될 수 있음을 보여준다. 예를 들어 위지윅스튜디오 299900의 2023-05-02 단절은 1주당 3주 무상증자 권리락 보도와 일치한다. 단순 원주가 비율의 −68% 등은 정상적인 투자 손실로 해석할 수 없다. [당시 무상증자·권리락 보도](https://www.news1.kr/amp/finance/general-stock/5033753). 원장 재현 PASS와 경제적 수익률 검증 PASS는 별개다. 모든 단절의 기업행사·현금흐름·수량 변환을 검증하기 전까지 본 보고서의 수익률은 원천 재현용 참고값이다.']
    (out/'REPORT_KO.md').write_text('\n\n'.join(report),encoding='utf-8');print('report and gallery complete')
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--manifest',required=True);main(p.parse_args().manifest)
