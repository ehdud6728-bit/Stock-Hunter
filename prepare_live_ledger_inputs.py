#!/usr/bin/env python3
"""Read-only, explicitly scoped LIVE recommendation ledger -> research input bridge.
Never treat reconstructed/undelivered candidate rows as real delivered recommendations.
"""
import argparse, json, os
from pathlib import Path
import pandas as pd

HEADERS=['event_id','recommendation_id','event_type','event_time_kst','run_date','data_date','scan_time','version','code','name','ignition_date','episode_id','final_decision','decision_reason','next_action','stage','recommendation_rank','score','recommended_price','target_3','target_5','stop_3','structure_support','market_state','risk_status','risk_reason','telegram_sent','telegram_chat_id_masked','telegram_message_ids','delivery_error','note','decision_phase','decision_time_kst','data_bar_state','kpi_eligible','kpi_exclusion_reason']

def normalize(rows, start, end):
    if not rows: raise ValueError('EMPTY_LIVE_LEDGER')
    width=max(len(rows[0]), max(map(len,rows[1:]),default=0))
    headers=list(rows[0])+['extra_'+str(i) for i in range(width-len(rows[0]))]
    df=pd.DataFrame([list(row)+['']*(width-len(row)) for row in rows[1:]],columns=headers)
    if not {'recommendation_id','event_type','run_date','code','recommended_price'}.issubset(df.columns): raise ValueError('BAD_LEDGER_HEADER')
    df=df[df.event_type.astype(str).eq('DELIVERED')].copy()
    if 'telegram_sent' in df:df=df[df.telegram_sent.astype(str).str.upper().eq('TRUE')]
    df=df[df.run_date.astype(str).between(start,end)]
    df=df.drop_duplicates('recommendation_id',keep='first')
    sig=pd.DataFrame({'code':df.code.astype(str).str.replace(r'\.0$','',regex=True).str.zfill(6),'signal_date':df.run_date,'mode':df.get('stage','ENTRY'), 'entry_price':pd.to_numeric(df.recommended_price,errors='coerce'),'name':df.get('name',''),'recommendation_id':df.recommendation_id,'source':'GOOGLE_SHEET_DELIVERED'})
    return sig[sig.entry_price.gt(0)].reset_index(drop=True)

def sheet_rows(spreadsheet_id,credentials_json):
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    cred=service_account.Credentials.from_service_account_info(json.loads(credentials_json),scopes=['https://www.googleapis.com/auth/spreadsheets.readonly'])
    service=build('sheets','v4',credentials=cred,cache_discovery=False)
    result=service.spreadsheets().values().get(spreadsheetId=spreadsheet_id,range="'LIVE_추천원장'!A1:AK20000").execute()
    return result.get('values',[])

def fetch_daily(codes,start,end):
    import yfinance as yf
    # yfinance auto adjustment would change historic signal-price basis; never use it here.
    begin=(pd.Timestamp(start)-pd.Timedelta(days=540)).date().isoformat()
    finish=(pd.Timestamp(end)+pd.Timedelta(days=1)).date().isoformat()
    rows=[];issues=[]
    for code in sorted(codes):
        try:
            series=None
            for suffix in ('.KS','.KQ'):
                x=yf.download(code+suffix,start=begin,end=finish,auto_adjust=False,actions=False,progress=False,threads=False)
                if x is not None and not x.empty:
                    series=x.copy(); break
            if series is None:
                issues.append({'code':code,'reason':'NO_DAILY_OHLCV'});continue
            if isinstance(series.columns,pd.MultiIndex): series.columns=series.columns.get_level_values(0)
            for date,item in series.iterrows():
                vol=float(item['Volume']); close=float(item['Close'])
                rows.append({'code':code,'date':date.strftime('%Y-%m-%d'),'open':float(item['Open']),'high':float(item['High']),'low':float(item['Low']),'close':close,'volume':vol,'amount':close*vol,'sector':'UNKNOWN'})
        except Exception as exc:
            issues.append({'code':code,'reason':type(exc).__name__+': '+str(exc)[:200]})
    return pd.DataFrame(rows,columns=['code','date','open','high','low','close','volume','amount','sector']),issues

def main():
    p=argparse.ArgumentParser();p.add_argument('--out',required=True);p.add_argument('--start',default='2026-08-18');p.add_argument('--asof',default='2026-10-12');p.add_argument('--spreadsheet-id',default='16OXJmYNnwWMjEp2UAhNxLPbRiTEYC29-6S7StvJxYt0');a=p.parse_args()
    out=Path(a.out);out.mkdir(parents=True,exist_ok=True)
    cred=os.environ.get('GOOGLE_JSON_KEY','').strip()
    if not cred:raise SystemExit('MISSING_GOOGLE_JSON_KEY: read-only source access unavailable')
    sig=normalize(sheet_rows(a.spreadsheet_id,cred),a.start,a.asof)
    sig.to_csv(out/'signals.csv',index=False,encoding='utf-8-sig')
    if sig.empty:
        (out/'source_audit.json').write_text(json.dumps({'status':'ZERO_DELIVERED_EVENTS','source':'LIVE_추천원장','start':a.start,'asof':a.asof,'n':0},ensure_ascii=False,indent=2))
        raise SystemExit('ZERO_DELIVERED_EVENTS: no verified delivered events in requested window; no backfill fabricated')
    daily,issues=fetch_daily(sig.code.unique(),a.start,a.asof)
    daily.to_csv(out/'daily.csv',index=False,encoding='utf-8-sig')
    report={'source':'LIVE_추천원장 DELIVERED+telegram_sent','start':a.start,'asof':a.asof,'signals':len(sig),'daily_rows':len(daily),'missing_prices':issues,'sector':'UNKNOWN (unavailable; do not infer as-of sector)','not_historical_search_replay':True,'pricing':'yfinance raw OHLC, verify adjustments'}
    (out/'source_audit.json').write_text(json.dumps(report,ensure_ascii=False,indent=2))
    if issues or daily.empty:raise SystemExit('DAILY_FETCH_INCOMPLETE: inspect source_audit.json')
    print(json.dumps(report,ensure_ascii=False))
if __name__=='__main__':main()
