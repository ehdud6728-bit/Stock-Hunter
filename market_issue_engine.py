from auto_theme_news import analyze_market_issues
from your_strategy_file import jongbe_triangle_combo_v3   # 네 함수 있는 파일
import numpy as np

# ─────────────────────────────
# 1️⃣ 이슈 점수 → 시장 가중치
# ─────────────────────────────
def calc_market_bias(issues):
    """
    이슈들의 평균 점수로 시장 환경 점수 계산 (0~100)
    """
    if not issues:
        return 50, "시장 이슈 없음 (중립)"

    scores = [i["score"] for i in issues]
    avg_score = np.mean(scores)

    # 코멘트 합치기
    comments = " | ".join([i["comment"] for i in issues])

    return round(avg_score, 1), comments


# ─────────────────────────────
# 2️⃣ 종목 점수 + 시장 점수 결합
# ─────────────────────────────
def final_stock_judgement(df, ticker="UNKNOWN"):
    """
    df: 종목 OHLCV 데이터
    ticker: 종목명
    """

    # ① 종목 기술 분석
    stock_result = jongbe_triangle_combo_v3(df)
    if stock_result is None:
        return None

    stock_score = stock_result["score"]

    # ② 시장 이슈 분석
    issues = analyze_market_issues()
    market_score, market_comment = calc_market_bias(issues)

    # ③ 가중치 결합 (기술 70%, 이슈 30%)
    final_score = stock_score * 0.7 + market_score * 0.3

    # ④ 최종 등급
    if final_score >= 80:
        grade = "S"
    elif final_score >= 65:
        grade = "A"
    elif final_score >= 50:
        grade = "B"
    else:
        grade = "C"

    return {
        "ticker": ticker,
        "stock_score": stock_score,
        "market_score": market_score,
        "final_score": round(final_score, 1),
        "grade": grade,
        "stock_signal": stock_result,
        "market_issues": issues,
        "market_comment": market_comment
    }


# ─────────────────────────────
# 3️⃣ 테스트용
# ─────────────────────────────
if __name__ == "__main__":
    import yfinance as yf

    df = yf.download("AAPL", period="6mo")
    result = final_stock_judgement(df, "AAPL")

    print("📊 종목:", result["ticker"])
    print("기술 점수:", result["stock_score"])
    print("시장 점수:", result["market_score"])
    print("최종 점수:", result["final_score"])
    print("등급:", result["grade"])
    print("\n📰 시장 코멘트:")
    print(result["market_comment"])
