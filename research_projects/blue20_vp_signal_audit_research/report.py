from pathlib import Path
from html import escape
import argparse,json,os
import numpy as np
import pandas as pd
from analyze import HERE,CASES,loader

LABEL={'code':'종목코드','date':'신호일','snapshot':'스냅샷','window':'기간','bins':'구간 수','method':'일봉 배분법','poc':'POC','val':'VAL','vah':'VAH','peak_low':'최대 구간 하단','peak_high':'최대 구간 상단','peak_pct':'최대 구간 추정%','signal_volume_pct':'D0 거래량 비중%','source_end':'원천 종료일','estimated_pct':'지정 구간 추정%','screen_pct':'화면 표시%','position':'종가 대비 위치','stage':'분해 단계','grid_width':'가격 구간 폭','total_volume':'거래량 분모','quantity_scenario':'수량 시나리오'}
def table(d):
    d=d.rename(columns=LABEL);lines=['|'+'|'.join(d.columns)+'|','|'+'|'.join(['---']*len(d.columns))+'|']
    for row in d.itertuples(index=False,name=None):lines.append('|'+'|'.join('—' if pd.isna(v) else f'{v:.3f}' if isinstance(v,(float,np.floating)) else str(v) for v in row)+'|')
    return '\n'.join(lines)

def gallery(frames,s,bins,zones,out):
    page=['<!doctype html><html lang="ko"><meta charset="utf-8"><title>BLUE20 신호 시점 매물대</title><style>body{font:16px system-ui;max-width:1500px;padding:24px;margin:auto;background:#fff;color:#182332}.pair{display:flex;gap:12px}.panel{width:50%}svg{width:100%;border:1px solid #ddd}table{border-collapse:collapse;font-size:13px}td,th{border:1px solid #ddd;padding:5px}h2{margin-top:48px}@media(max-width:1000px){.pair{display:block}.panel{width:100%}}</style>',
          '<h1>신호 당일에서 멈춘 매물대 비교</h1><p>왼쪽: 신호 전일까지 누적. 오른쪽: 신호 당일까지 누적. 두 그림의 원본 캔들과 표시 축은 같은 신호일까지만 사용합니다. 신호 이후 봉은 없습니다. 모두 120일·32구간·고저 균등분배의 일봉 추정 거래량입니다. 오른쪽 계산이 종가 확정 후 판단의 기본 스냅샷입니다.</p>',
          '<p>회색 막대: 추정 가격대별 거래량. 주황: 신호일 종가 위의 확인 고점 반락 존. 초록: 종가 아래의 과거 존. 파랑: POC. 빨간 세로선: D0. ATR 전진폭은 매수 안전성 판정으로 사용하지 않습니다.</p>']
    count=0
    for (code,date),name in CASES.items():
        d=frames[code];j=int(d.index[d.date.eq(date)][0]);w=d.iloc[j-120:j+1].reset_index(drop=True)
        cand=zones[(zones.code==code)&zones.date.eq(date)&zones.window.eq(120)]
        lo=w.low.min();hi=w.high.max();y=lambda p:355-310*(p-lo)/(hi-lo);x=lambda i:45+710*i/(len(w)-1)
        page.append(f'<section><h2>{escape(name)} {code} {date}</h2><div class="pair">')
        for kind,title in [('PAST_ONLY','D−1까지 누적'),('SIGNAL_CLOSE','D0 종가까지 누적')]:
            q=bins[(bins.code==code)&bins.date.eq(date)&bins.window.eq(120)&bins.bins_requested.eq(32)&bins.method.eq('range')&bins.snapshot.eq(kind)]
            r=s[(s.code==code)&s.date.eq(date)&s.window.eq(120)&s.bins.eq(32)&s.method.eq('range')&s.snapshot.eq(kind)].iloc[0]
            parts=[f'<div class="panel"><h3>{title}</h3><svg viewBox="0 0 1040 500">']
            for z in cand.itertuples():
                color='#e9a443' if z.position=='ABOVE_CLOSE' else '#3e9b71' if z.position=='BELOW_CLOSE' else '#bc6bd7'
                parts.append(f'<rect x="45" y="{y(z.high)}" width="710" height="{y(z.low)-y(z.high)}" fill="{color}" opacity=".13"><title>확인 가격 저항 {z.low:.1f}~{z.high:.1f}, {z.tests}회, {z.position}; {z.test_dates}</title></rect>')
            for k,b in w.iterrows():
                color='#cc4545' if b.close>=b.open else '#2d70b8';xx=x(k)
                parts.append(f'<g><title>{b.date} O={b.open} H={b.high} L={b.low} C={b.close} V={b.volume}</title><path d="M{xx} {y(b.high)}V{y(b.low)}" stroke="{color}"/><rect x="{xx-1.7}" y="{y(max(b.open,b.close))}" width="3.4" height="{max(1,abs(y(b.open)-y(b.close)))}" fill="{color}"/></g>')
                parts.append(f'<rect x="{xx-1.7}" y="{465-60*b.volume/w.volume.max()}" width="3.4" height="{60*b.volume/w.volume.max()}" fill="{color}" opacity=".6"/>')
            for b in q.itertuples():
                width=215*b.estimated_volume/q.estimated_volume.max()
                parts.append(f'<rect x="780" y="{y(b.high)}" width="{width}" height="{max(.5,y(b.low)-y(b.high))}" fill="#8493a5" opacity=".7"><title>{b.low:.2f}~{b.high:.2f}; 추정 {b.estimated_volume:.3f}주; {b.estimated_pct:.3f}%</title></rect>')
            for price in np.linspace(lo,hi,5):parts.append(f'<text x="4" y="{y(price)}" font-size="11">{price:,.0f}</text>')
            parts.append(f'<path d="M45 {y(r.poc)}H1000" stroke="#245fc3" stroke-dasharray="4 3"/><text x="785" y="{y(r.poc)-4}" fill="#245fc3" font-size="12">POC {r.poc:,.1f}</text>')
            parts.append(f'<path d="M{x(len(w)-1)} 35V470" stroke="#c22"/><text x="{x(len(w)-1)-17}" y="30">D0</text><text x="45" y="490">{w.date.iloc[0]} → {date}</text></svg>')
            parts.append(f'<p>집계 {r.source_start}~{r.source_end}, {int(r.window)}봉 / 총 {r.total_volume:,.0f}주<br>최대 구간 {r.peak_low:,.1f}~{r.peak_high:,.1f}원: 추정 {r.peak_pct:.2f}% / {r.peak_position}<br>D0 거래량 비중 {r.signal_volume_pct:.2f}%</p></div>');page+=parts;count+=1
        page.append('</div><p>회색 분포는 미체결 잔존 매도 주문이 아닙니다. 기업행사·실제 체결량 인증은 미완료입니다.</p></section>')
    page.append('</html>');(out/'GALLERY.html').write_text('\n'.join(page),encoding='utf-8');return count

def main(manifest):
    out=HERE/'results';s=pd.read_csv(out/'SNAPSHOTS.csv.gz',dtype={'code':str});q=pd.read_csv(out/'HTS_BAND_QUERIES.csv.gz',dtype={'code':str});de=pd.read_csv(out/'DECOMPOSITION.csv.gz',dtype={'code':str});z=pd.read_csv(out/'ALL_PRICE_RESISTANCE_ZONES.csv.gz',dtype={'code':str});b=pd.read_csv(out/'CASE_BIN_WEIGHTS.csv.gz',dtype={'code':str})
    status=json.loads((out/'STATUS.json').read_text());primary=s[s.window.eq(120)&s.bins.eq(32)&s.method.eq('range')]
    focus=primary[((primary.code=='028300')&(primary.date=='2023-02-20'))|((primary.code=='145990')&(primary.date=='2023-05-08'))]
    lines=['# BLUE20 신호 시점 매물대 차이 원인 분석',
    '주 분석은 **신호 당일 완료 일봉 기준(SIGNAL_CLOSE)**이다. 신호 전일까지(PAST_ONLY)는 과거 매물의 대조군이다. 신호 다음 날 판단을 위한 일봉 마감 후 스냅샷을 비교한다. 실제 원천의 공표 시각·정규장/시간외 집계 범위는 미검증이며 15:30 즉시 이용 가능했던 데이터로 인증하지 않는다. next_observed_bar_date는 후속 원천 봉의 날짜 메타데이터일 뿐 당시 확정된 진입 가능일이나 매매 규칙이 아니다. 후일 가격·거래량은 프로파일에 사용하지 않았다. 기존 500종목·1,804사건은 보존했고, 같은 종목·날짜의 밴드 중복을 제외한 1,803개 차트를 계산했다. 매수 보류·목표가·손절 성과의 최적화는 하지 않았다.',
    '## 확인된 차이와 미확인 원인',
    '1. **기준 시점 차이: 확인.** 이전 프로파일은 D−1까지였다. 신호 당일 거래량이 큰 장대양봉에서 D0 포함 여부는 중요하다. D0 종가 스냅샷은 종가가 확정된 후에만 알 수 있다.\n2. **비교 가격 구간 차이: 확인.** 이전 연구의 최대밀도 구간/POC와 화면에 표시된 특정 가격 구간은 서로 다른 구간이었다. 같은 고정 구간을 직접 적분한 표를 별도로 만들었다.\n3. **가격 구간 재설정 효과: 계산 확인.** 새 고저가로 등분 구간이 이동하면 같은 봉의 거래량도 다른 구간에 분배된다. 오래된 봉 제거·D0 추가·재구간화의 순서 고정 분해를 제공한다.\n4. **일봉 배분법 영향: 계산 확인.** 균등·대표가격·종가·OHLC4 방식은 다른 추정치다. 어느 방식이 실제 체결 분포인지 이번 일봉만으로 검증하지 못했다.\n5. **기업행사 기준 차이: 부분 설명, 미인증.** HLB 기준가 2행의 공식 근거로 부분 시나리오만 계산했다. 이를 완전 수정주가로 적용하지 않았다.\n6. **HTS 집계 설정 및 미래 거래 포함: 미확인.** 두 화면에 이후 봉이 보이지만 매물대 집계 범위 설정은 확인되지 않았다. 화면의 미래 포함이나 HTS 계산 오류를 단정하지 않는다.',
    '## HLB·삼양사: 같은 시점과 같은 구간으로 대조',
    '아래 최대 구간은 120일·32구간·고저 균등분배 기준이다. 최대 구간 위치는 신호 당일 종가와 비교한다. BELOW_CLOSE는 돌파된 과거 거래 집중 구간이며 미돌파 상단 저항이 아니다.',
    table(focus[['code','date','snapshot','source_end','poc','peak_low','peak_high','peak_pct','signal_volume_pct']]),
    '첨부 화면의 고정 가격 구간에 직접 배분한 결과는 다음과 같다. HLB 범위는 사용자가 적은 36,900~38,000원으로 계산했으며 정밀 OCR 판독값이 아니다. 삼양사는 표시된 44,960~46,350원이다. HTS 기간·종료일·구간 수·분배법·수정주가가 없어 수치 일치 여부는 MISSING_SETTINGS다.',
    table(q[q.window.eq(120)][['code','snapshot','method','estimated_pct','screen_pct']]),
    '화면의 20.8%/45.2%와 차이가 난다는 이유로 모델을 그 수치에 맞추지 않았다. 비교 대상 가격 범위와 분모·시간이 고정되기 전에는 어느 한쪽 계산 오류를 확정할 수 없다. 사후 D+20 프로파일을 D0 판단에 끼워 넣지 않았다.',
    '동일한 원본 일봉이라도 장대양봉 거래량을 넓은 고저 범위에 펴는지, 종가 한 곳에 두는지에 따라 최대 구간이 달라진다. 다음은 D0 종가·120일·32구간의 같은 원천에서 배분법만 바꾼 결과다. 모델 이름이 곧 실제 체결 방법이라는 뜻은 아니다.',
    table(s[s.snapshot.eq('SIGNAL_CLOSE')&s.window.eq(120)&s.bins.eq(32)&(((s.code=='028300')&(s.date=='2023-02-20'))|((s.code=='145990')&(s.date=='2023-05-08')))][['code','method','poc','peak_low','peak_high','peak_pct']]),
    '## 차이의 분해',
    'OLD_N→REMOVE_OLDEST→ADD_D0→REBIN_NEW_N 순서다. 중간 단계는 D−1의 구간 폭·원점을 유지하면서 필요시 바깥 구간을 연장해 거래량을 보존한다. 분해 순서에 따라 기여 크기가 달라질 수 있으므로 인과 기여도/유일한 분해로 해석하지 않는다.',
    table(de[de.window.eq(120)&de.code.eq('145990')&de.date.eq('2023-05-08')][['stage','total_volume','grid_width','poc','peak_low','peak_high','peak_pct']]),
    '전체 차이 집계는 SNAPSHOT_DIFFERENCES.csv, 원시 구간별 계산은 CASE_BIN_WEIGHTS.csv.gz에 있다. 60/120/240일·16/32/64구간·4방식·2시점을 모두 제공한다. 기간과 구간 수가 바뀌면 구간 폭·선택 표본도 달라지므로 비중만 단순 비교하지 않는다. RAW_CASE_SNAPSHOTS.csv.gz는 신호일/봉 날짜를 별도 필드로 저장한 원본 봉, CASE_BAR_BIN_ALLOCATIONS.csv.gz는 HLB·삼양사 각 봉에서 각 구간으로 배분된 거래량, HTS_BAND_BAR_CONTRIBUTIONS.csv.gz는 화면 지정 구간에 각 봉이 기여한 거래량이다. CASE_INPUT_HASHES.csv.gz로 사용한 원천 창을 고정했다.',
    '## 실제 저항과 거래 집중의 분리',
    '과거 확인 고점과 반락으로 만든 모든 가격 저항 후보는 ALL_PRICE_RESISTANCE_ZONES.csv.gz에 별도 저장했다. 후보를 한 개만 골라 나머지 상단 저항을 숨기지 않는다. ABOVE_CLOSE/CLOSE_INSIDE/BELOW_CLOSE와 ATR 전진폭을 별도로 표시한다. 이전 BREAK_STRONG은 전진폭 분류였고 매물 소화·안착·매수 안전성 인증이 아니었다. 이번 연구에서는 해당 명칭으로 매수 판단을 하지 않는다.',
    '## 기업행사·실제 체결량',
    'HLB의 2022-10-13 및 2022-12-28 기준가 조정은 동결한 공식 원문과 해시로 근거를 보존했다. 공개 당시 알 수 있었던 기준가 비율을 이전 OHLC에 적용한 부분 재기준화 시나리오와, 역가격 배율로 수량을 변환한 합성 민감도를 구분했다. 후자는 실제 주식 수 변환 인증이 아니다. 현금배당·유상증자 권리가치·전체 기업행사에 대한 완전한 시계열 보정이 아니며 정답으로 채택하지 않는다. PARTIAL_ACTION_SCENARIOS.csv.gz를 참조한다.',
    '실제 분봉·틱 체결 자료는 이번 실행에서 확보하지 못했다. 일봉 거래대금/거래량으로 평균 체결가나 실제 가격대별 분포를 복원했다고 주장하지 않는다. 기존 7행 원천 단서와 42개 큰 시가 단절의 미확인 상태도 유지한다. 원천 품질 문제가 해결되기 전 매물대 기반 성과 판단은 동결한다.',
    '## 원천·검증',json.dumps(status,ensure_ascii=False,indent=2),
    '손 계산: 10~12원 범위의 20주를 [10,11), [11,12]에 균등 분배하면 10주씩이다. 마지막 경계 12원에서만 거래된 5주를 더하면 [10,15]주, POC=11.5원, 연속70% 가치영역=10~12원이다. 종가 집중 방식이면 첫 20주도 마지막 구간에 배분되어 [0,25]주가 된다. 산술 총량 보존이 같아도 분포는 달라진다. 이 차이는 일봉 정보만으로 어느 쪽이 실체결인지 결정할 수 없음을 보여준다.',
    '원본 500개 manifest 파일 해시와 481개 사건종목 정규화 가격 해시를 대조했다. 기존 range/typical 사전 프로파일을 전수 재현했다. 미래 봉 변조, 시점 경계, 거래량 보존, 가격 구간 바깥 무단 절삭 금지, 부분 기업행사의 당시 인지 가능 여부를 테스트했다.',
    '## 실행 상태']
    if os.environ.get('GITHUB_ACTIONS')=='true':
        url=f'https://github.com/{os.environ.get("GITHUB_REPOSITORY","")}/actions/runs/{os.environ.get("GITHUB_RUN_ID","")}'
        lines.append(f'GitHub Actions 안에서 이 보고서를 생성했다: {url}. 현재 단계까지의 계산 완료를 의미하며 전체 실행 성공 여부는 최종 작업 상태를 확인해야 한다.')
    else:lines.append('이 보고서는 로컬 계산 결과다. 이번 신규 연구의 GitHub 실행은 아직 하지 않았다. 이전 #42375577392는 선행 연구의 실행이며 이번 계산의 원격 실행을 대신하지 않는다.')
    lines+=['## 다음에 필요한 확인','HTS의 매물대 시작·끝 날짜, 가격 구간 수, 수정주가/권리락 옵션, 일봉/분봉 기준, 거래량 배분 방식과 표시 비중의 분모를 확보해야 같은 조건 재현을 확정할 수 있다. 사용자에게 이미 요청한 설정은 미회신 상태로 기록한다. 삼양사 두 번째 기회 날짜도 임의로 정하지 않았다.\n\n정확한 설정과 원천 검증 후에만 신호별 저항 잔존·재돌파·안착·상승 여유 연구를 재개한다. 실전 LIVE/SEARCH/WATCH·R1-D118과 이전 연구는 보존했다.']
    (out/'REPORT_KO.md').write_text('\n\n'.join(lines),encoding='utf-8')
    frames=loader.load_prices(Path(manifest));panels=gallery(frames,s,b,z,out)
    (out/'HTS_SETTINGS_STATUS.json').write_text(json.dumps({'exact_match':'MISSING_SETTINGS','required':['profile_start','profile_end','bins','price_basis','allocation_method','volume_denominator'],'screens':2,'gallery_signal_dates':9,'gallery_panels':panels},ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({'report':'COMPLETE','gallery_panels':panels}))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--manifest',required=True);main(p.parse_args().manifest)
