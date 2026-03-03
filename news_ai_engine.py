import requests, json, os, datetime
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import FinanceDataReader as fdr
import ta

DB_FILE = "news_reaction_db.json"

# =========================
# 📦 DB 관리
# =========================
def load_db():
    if os.path.exists(DB_FILE):
        with open(DB_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

def save_db(db):
    with open(DB_FILE, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)

# =========================
# 📰 뉴스 크롤링
# =========================
def fetch_news(keyword="전쟁"):
    url = f"https://search.naver.com/search.naver?where=news&query={keyword}"
    r = requests.get(url, headers={"User-Agent":"Mozilla/5.0"})
    soup = BeautifulSoup(r.text,"html.parser")
    titles = [t.text for t in soup.select(".news_tit")][:5]
    return titles

# =========================
# 🤖 뉴스 AI 분석
# =========================
def analyze_news(text):
    impact = "LOW"
    category = "기타"

    if any(k in text for k in ["전쟁","공습","충돌"]):
        category = "지정학"
        impact = "HIGH"
    elif any(k in text for k in ["금리","인상","연준"]):
        category = "금융정책"
        impact = "MEDIUM"
    elif any(k in text for k in ["감염","확산","바이러스"]):
        category = "보건"
        impact = "HIGH"

    comment = f"이슈: {text} → {category} 리스크({impact})"
    return {"raw": text, "category": category, "impact": impact, "comment": comment}

# =========================
# 🔍 유사 뉴스 계산
# =========================
def similarity(new_text, db_texts):
    if not db_texts:
        return []
    vectorizer = TfidfVectorizer()
    tfidf = vectorizer.fit_transform([new_text] + db_texts)
    return cosine_similarity(tfidf[0:1], tfidf[1:])[0]

# =========================
# 📈 기술적 점수 (DMI + MA)
# =========================
def technical_score(df):
    df['MA20'] = df['Close'].rolling(20).mean()
    df['MA40'] = df['Close'].rolling(40).mean()

    df['plus_di'] = ta.trend.ADXIndicator(df['High'],df['Low'],df['Close']).adx_pos()
    df['minus_di']= ta.trend.ADXIndicator(df['High'],df['Low'],df['Close']).adx_neg()
    df['adx']     = ta.trend.ADXIndicator(df['High'],df['Low'],df['Close']).adx()

    c,p = df.iloc[-1], df.iloc[-2]

    ma_cross = c['MA20']>c['MA40'] and p['MA20']<=p['MA40']
    dmi_cross = c['plus_di']>c['minus_di'] and p['plus_di']<=p['minus_di']
    adx_ok = c['adx']>20

    score = 0
    if ma_cross: score+=30
    if dmi_cross: score+=20
    if adx_ok: score+=20

    return score

# =========================
# 🧠 종목 뉴스 반응 학습 점수
# =========================
def stock_news_score(stock, issue):
    db = load_db()
    texts = [e["issue"]["raw"] for e in db]
    sims = similarity(issue["raw"], texts)

    score = 0
    for i,e in enumerate(db):
        if stock in e["stocks"]:
            r = e["stocks"][stock]["return"]
            score += sims[i]*r*100
    return round(score,2)

# =========================
# 🏁 통합 판단
# =========================
def ai_decision(stock):
    df = fdr.DataReader(stock)
    tech = technical_score(df)

    news_titles = fetch_news()
    issue = analyze_news(news_titles[0])

    news_score = stock_news_score(stock, issue)
    total = tech + news_score

    return {
        "stock": stock,
        "tech_score": tech,
        "news_score": news_score,
        "total_score": round(total,2),
        "comment": issue["comment"]
    }

# =========================
# 🧾 DB 업데이트
# =========================
def update_db(issue, stock_returns):
    db = load_db()
    db.append({
        "date": str(datetime.date.today()),
        "issue": issue,
        "stocks": stock_returns
    })
    save_db(db)

# =========================
# 🚀 실행부
# =========================
if __name__=="__main__":
    targets = ["005930","000660","035420"]  # 삼성전자, SK하이닉스, NAVER

    results=[]
    for s in targets:
        r = ai_decision(s)
        results.append(r)
        print(f"\n📊 {s}")
        print(r)

    # 예시 DB 저장
    update_db(
        analyze_news(fetch_news()[0]),
        {"005930":{"reaction":"up","return":0.03}}
    )
