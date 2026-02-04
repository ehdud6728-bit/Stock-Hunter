# ------------------------------------------------------------------
# 🥣 [단테 봇] main_dante.py (시황 차트 브리핑 + 고속 스캔 Ver)
# ------------------------------------------------------------------
import FinanceDataReader as fdr
import pandas as pd
import numpy as np
import requests
import os
import mplfinance as mpf
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# 💎 OpenAI
try:
    from openai import OpenAI
except ImportError:
    OpenAI = None

# 시트 매니저
from google_sheet_manager import update_google_sheet

# =================================================
# ⚙️ [설정] 파라미터
# =================================================
TOP_N = 2500            
DROP_RATE = 0.15        
STOP_LOSS_BUFFER = 0.95 

# API 키
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID_LIST = os.environ.get('TELEGRAM_CHAT_ID', '').split(',')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY') 
GROQ_API_KEY = os.environ.get('GROQ_API_KEY')     

# =================================================

# 📸 [New] 지수 차트 그리기 (코스피/코스닥/나스닥)
def create_index_chart(ticker, name):
    try:
        # 최근 6개월 데이터
        df = fdr.DataReader(ticker, start=(datetime.now() - timedelta(days=180)))
        
        # 스타일 설정 (상승 빨강, 하락 파랑)
        mc = mpf.make_marketcolors(up='r', down='b', inherit=True)
        s  = mpf.make_mpf_style(marketcolors=mc)
        
        # 이평선 (20일, 60일)
        apds = [
            mpf.make_addplot(df['Close'].rolling(20).mean(), color='orange', width=1),
            mpf.make_addplot(df['Close'].rolling(60).mean(), color='purple', width=1)
        ]
        
        filename = f"{name}.png"
        
        # 차트 저장
        mpf.plot(
            df, 
            type='candle', 
            style=s, 
            addplot=apds,
            title=f"{name} ({ticker})",
            volume=False, # 지수는 거래량 생략하거나 false
            savefig=filename,
            figscale=1.0,
            figratio=(10, 5)
        )
        return filename
    except Exception as e:
        print(f"⚠️ {name} 차트 실패: {e}")
        return None

# 📸 사진 전송 함수
def send_telegram_photo(message, image_paths=[]):
    if not TELEGRAM_TOKEN or not CHAT_ID_LIST: return
    
    url_photo = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
    url_text = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    
    real_id_list = []
    for item in CHAT_ID_LIST:
        real_id_list.extend([x.strip() for x in item.split(',') if x.strip()])
    
    for chat_id in real_id_list:
        if not chat_id: continue
        
        # 1. 텍스트 먼저 전송
        if message:
            requests.post(url_text, data={'chat_id': chat_id, 'text': message})
            
        # 2. 이미지가 있으면 전송
        if image_paths:
            for img_path in image_paths:
                if img_path and os.path.exists(img_path):
                    try:
                        with open(img_path, 'rb') as f:
                            # 캡션 없이 사진만 깔끔하게
                            requests.post(url_photo, data={'chat_id': chat_id}, files={'photo': f})
                    except: pass

    # 전송 후 이미지 삭제 (청소)
    for img_path in image_paths:
        if img_path and os.path.exists(img_path):
            os.remove(img_path)

# ---------------------------------------------------------
# 🌍 시황 브리핑 (텍스트 생성)
# ---------------------------------------------------------
def get_market_briefing():
    if not OPENAI_API_KEY: return None
    print("🌍 시황 데이터 분석 중...")

    try:
        kospi = fdr.DataReader('KS11', start=datetime.now() - timedelta(days=5))
        kosdaq = fdr.DataReader('KQ11', start=datetime.now() - timedelta(days=5))
        nasdaq = fdr.DataReader('IXIC', start=datetime.now() - timedelta(days=5))
        
        def get_change(df):
            if len(df) < 2: return "0.00 (0.00%)"
            curr = df['Close'].iloc[-1]
            prev = df['Close'].iloc[-2]
            rate = (curr - prev) / prev * 100
            return f"{curr:,.2f} ({rate:+.2f}%)"

        market_data = (
            f"나스닥: {get_change(nasdaq)}\n"
            f"코스피: {get_change(kospi)}\n"
            f"코스닥: {get_change(kosdaq)}"
        )

        prompt = (f"데이터: {market_data}\n"
                  f"위 데이터를 보고 트레이더들에게 '오늘의 증시 요약'을 단테 스타일(반말)로 3줄 요약해줘.\n"
                  f"오늘 장의 분위기와 대응 전략 위주로.")

        client = OpenAI(api_key=OPENAI_API_KEY)
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=300
        )
        return f"📢 [오늘의 시황]\n{response.choices[0].message.content.strip()}"

    except Exception:
        return None

# ---------------------------------------------------------
# 🧠 AI 종목 분석
# ---------------------------------------------------------
def get_chatgpt_opinion(name, ticker, signal, stop_loss):
    if not OPENAI_API_KEY: return ""
    
    prompt = (f"종목: {name} ({ticker}), 신호: {signal}\n"
              f"1. 테마/업종 1단어 정의 (예: [반도체])\n"
              f"2. 밥그릇 기법 관점 매력/리스크 1줄 요약 (반말)\n"
              f"형식: '[테마] 분석내용'")

    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=200
        )
        return f"🧠 {response.choices[0].message.content.strip()}"
    except:
        return ""

# ---------------------------------------------------------
# 🔍 단테 검색식 (속도 최적화)
# ---------------------------------------------------------
def analyze_dante_stock(ticker, name):
    try:
        df = fdr.DataReader(ticker, start=(datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d'))
        if len(df) < 225: return None
        
        row = df.iloc[-1]
        current_price = row['Close']
        if current_price < 500 or row['Volume'] == 0: return None

        ma112 = df['Close'].rolling(112).mean().iloc[-1]
        ma224 = df['Close'].rolling(224).mean().iloc[-1]
        past_high = df['High'].iloc[:-120].max() 
        
        if current_price > past_high * (1 - DROP_RATE): return None 
        dist_112 = (current_price - ma112) / ma112
        if not (-0.10 <= dist_112 <= 0.30): return None
        
        score = 50
        signal_list = []
        
        if 0 <= dist_112 <= 0.05:
            score += 30
            signal_list.append("🎯맥점")
        if row['Close'] > ma224:
            score += 20
            signal_list.append("🔥224돌파")
        elif (ma224 - current_price) / current_price < 0.05:
            score += 15
            signal_list.append("🔨224도전")
        
        recent_volatility = df['Close'].iloc[-5:].std() / df['Close'].iloc[-5:].mean()
        if recent_volatility < 0.02:
            score += 15
            signal_list.append("🛡️공구리")
            
        vol_avg = df['Volume'].iloc[-20:].mean()
        has_volume_spike = any((df['Volume'].iloc[-20:] > vol_avg * 2) & (df['Close'].iloc[-20:] > df['Open'].iloc[-20:]))
        if has_volume_spike and dist_112 < 0.1:
            score += 15
            signal_list.append("🤫매집")

        stop_loss_price = int(ma112 * STOP_LOSS_BUFFER)
        signal = " / ".join(signal_list) if signal_list else "관심"
        
        if score < 70: return None

        # AI 분석
        ai_msg = get_chatgpt_opinion(name, ticker, signal, stop_loss_price)
        
        theme_tag = ""
        if "[" in ai_msg and "]" in ai_msg:
            try:
                start = ai_msg.find("[")
                end = ai_msg.find("]")
                if end - start < 15: theme_tag = ai_msg[start:end+1] + " "
            except: pass

        return {
            'code': ticker,
            '종목명': name,
            '현재가': int(current_price),
            '신호': signal,
            '총점': score,
            'msg': f"🥣 [단테 Pick] {name} {theme_tag}({score}점)\n"
                   f"👉 {signal}\n"
                   f"💰 현재가: {int(current_price):,}원\n"
                   f"🛡️ 손절가: {stop_loss_price:,}원\n"
                   f"{ai_msg}"
        }

    except Exception:
        return None

# ---------------------------------------------------------
# 🚀 실행
# ---------------------------------------------------------
if __name__ == "__main__":
    print(f"🥣 [단테 봇] 시황 차트 & 종목 스캔 시작...")
    
    # 1. 📸 시황 차트 생성 (나스닥, 코스피, 코스닥)
    print("📊 지수 차트 생성 중...")
    chart_files = []
    
    # 나스닥 (IXIC), 코스피 (KS11), 코스닥 (KQ11)
    chart_files.append(create_index_chart('IXIC', 'NASDAQ'))
    chart_files.append(create_index_chart('KS11', 'KOSPI'))
    chart_files.append(create_index_chart('KQ11', 'KOSDAQ'))
    
    # 2. 🌍 시황 브리핑 멘트 생성
    market_brief = get_market_briefing()
    
    # 3. 📨 시황 전송 (텍스트 + 차트 3장)
    if market_brief:
        print(market_brief)
        send_telegram_photo(market_brief, chart_files)
    
    # -----------------------------------------------------
    
    # 4. 🔍 종목 스캔 (빠르게!)
    print("🔍 종목 스캔 시작 (차트 생성 X)...")
    df_krx = fdr.StockListing('KRX')
    df_leaders = df_krx.sort_values(by='Amount', ascending=False).head(TOP_N)
    target_dict = dict(zip(df_leaders['Code'].astype(str), df_leaders['Name']))
    
    # 테스트용
    force_list = {'008350':'남선알미늄', '294630':'서남', '028300':'HLB'}
    for k, v in force_list.items():
        if k not in target_dict: target_dict[k] = v

    results = []
    with ThreadPoolExecutor(max_workers=30) as executor: # 다시 속도 높임 (30)
        futures = [executor.submit(analyze_dante_stock, t, n) for t, n in target_dict.items()]
        for future in futures:
            res = future.result()
            if res: results.append(res)
            
    if results:
        results.sort(key=lambda x: x['총점'], reverse=True)
        final_msgs = [r['msg'] for r in results[:15]]
        
        report = f"🥣 [오늘의 단테 픽] {len(results)}개 발견\n\n" + "\n\n".join(final_msgs)
        print(report)
        # 종목 리스트는 텍스트로만 빠르게 전송
        send_telegram_photo(report, []) 
        
        try:
            update_google_sheet(results, datetime.now().strftime('%Y-%m-%d'))
            print("💾 시트 저장 완료")
        except: pass
    else:
        print("❌ 검색된 종목이 없습니다.")
