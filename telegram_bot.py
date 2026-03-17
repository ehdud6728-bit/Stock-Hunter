import os
import traceback
import requests
import pandas as pd
import FinanceDataReader as fdr

from flask import Flask, request, jsonify

# =========================================================
# 기존 메인 코드에서 가져올 함수들
# main7.py 파일명에 맞게 수정하세요.
# =========================================================
from main7 import (
    load_krx_listing_safe,
    get_indicators,
    evaluate_stage_sequence,
    evaluate_exit_signal,
    safe_float,   # 없다면 아래 로컬 함수로 대체 가능
)

# safe_float가 main7.py에 없으면 이걸 쓰세요.
# def safe_float(x, default=0.0):
#     try:
#         return float(x)
#     except:
#         return default

app = Flask(__name__)

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "my-secret-path")

# 선택: top 후보를 별도 JSON/CSV로 저장해두고 봇이 읽게 만들 수도 있음
TOP_CANDIDATES_FILE = os.environ.get("TOP_CANDIDATES_FILE", "top_stage_candidates.csv")

REAL_HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

# =========================================================
# 유틸
# =========================================================

def send_telegram_message(chat_id: int, text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text[:4000]
    }
    requests.post(url, json=payload, timeout=15)

def normalize_stock_name(text: str) -> str:
    return str(text).strip().replace(" ", "")

def find_stock_code_by_name(stock_name: str) -> tuple[str, str] | tuple[None, None]:
    """
    종목명으로 Code, Name 찾기
    """
    try:
        df_krx = load_krx_listing_safe()
        if df_krx is None or df_krx.empty:
            return None, None

        df_krx["Code"] = (
            df_krx["Code"]
            .fillna("")
            .astype(str)
            .str.replace(".0", "", regex=False)
            .str.zfill(6)
        )
        df_krx["Name"] = df_krx["Name"].astype(str)

        target = normalize_stock_name(stock_name)

        # 1차: 완전일치
        exact = df_krx[df_krx["Name"].apply(normalize_stock_name) == target]
        if not exact.empty:
            row = exact.iloc[0]
            return row["Code"], row["Name"]

        # 2차: 포함 검색
        partial = df_krx[df_krx["Name"].apply(normalize_stock_name).str.contains(target, na=False)]
        if not partial.empty:
            row = partial.iloc[0]
            return row["Code"], row["Name"]

        return None, None
    except Exception:
        return None, None

def fetch_stock_df(code: str, days: int = 250) -> pd.DataFrame | None:
    try:
        df = fdr.DataReader(code)
        if df is None or df.empty:
            return None

        df = df.tail(days).copy()
        if len(df) < 80:
            return None
        return df
    except Exception:
        return None

def format_profit(entry_price: float | None, current_price: float) -> str:
    if entry_price is None or entry_price <= 0:
        return "미입력"
    pct = ((current_price - entry_price) / entry_price) * 100
    return f"{pct:+.2f}%"

def build_analyze_reply(stock_name: str, code: str, df: pd.DataFrame, entry_price: float | None = None) -> str:
    """
    보유종목/현재종목 분석 답변
    """
    row = df.iloc[-1]

    current_price = float(row["Close"])
    stage_eval = evaluate_stage_sequence(df)
    exit_eval = evaluate_exit_signal(df, entry_price=entry_price)

    rsi = safe_float(row.get("RSI", 0))
    bb40 = safe_float(row.get("BB40_Width", 0))
    ma_conv = safe_float(row.get("MA_Convergence", 0))
    obv_slope = safe_float(row.get("OBV_Slope", 0))

    lines = [
        f"[{stock_name}({code})]",
        f"현재가: {int(current_price):,}",
        f"매수가: {int(entry_price):,}" if entry_price else "매수가: 미입력",
        f"수익률: {format_profit(entry_price, current_price)}",
        "",
        f"단계: {stage_eval.get('stage_status', 'DROP')} | {' '.join(stage_eval.get('stage_tags', []))}",
        f"S1: {stage_eval.get('s1_date', '-')}",
        f"S2: {stage_eval.get('s2_date', '-')}",
        f"S3: {stage_eval.get('s3_date', '-')}",
        "",
        f"청산: {exit_eval.get('exit_status', 'HOLD')} | {' '.join(exit_eval.get('exit_tags', []))}",
        f"자동손절가: {int(exit_eval['stop_price']):,}" if exit_eval.get("stop_price") else "자동손절가: 계산불가",
        f"재진입감시: {'예' if exit_eval.get('reentry_watch') else '아니오'}",
        "",
        f"RSI: {rsi:.1f} | BB40폭: {bb40:.1f} | MA수렴: {ma_conv:.1f} | OBV기울기: {obv_slope:.1f}",
    ]

    # 간단 코멘트
    if stage_eval.get("stage_status") == "PASS_A":
        lines.append("코멘트: 초동 파동형. 돌파 유지 여부가 핵심")
    elif stage_eval.get("stage_status") == "PASS_B":
        lines.append("코멘트: 재파동형. 눌림 후 재상승 또는 재돌파 체크")
    else:
        lines.append("코멘트: 지금은 급등 후보 단계는 아님")

    if exit_eval.get("exit_status") == "PARTIAL_SELL":
        lines.append("전략: 분할 익절 후 눌림 재진입 감시가 유리")
    elif exit_eval.get("exit_status") == "FULL_SELL":
        lines.append("전략: 구조 훼손 가능성. 전량 정리 우선")
    else:
        lines.append("전략: 구조 유지 시 보유 가능")

    return "\n".join(lines)

def load_top_candidates_text() -> str:
    """
    main 배치에서 저장한 top 후보 파일을 읽어서 보여주는 간단 버전
    """
    if not os.path.exists(TOP_CANDIDATES_FILE):
        return "TOP 후보 파일이 아직 없습니다."

    try:
        df = pd.read_csv(TOP_CANDIDATES_FILE)
        if df.empty:
            return "TOP 후보가 비어 있습니다."

        lines = ["🚀 [단계 기반 급등 후보 TOP]"]
        for i, (_, row) in enumerate(df.head(5).iterrows(), start=1):
            lines.append(
                f"{i}) {row.get('종목명','')}\n"
                f"- 단계: {row.get('단계상태','')}\n"
                f"- 태그: {row.get('단계태그','')}\n"
                f"- 안전:{row.get('안전점수',0)} | N점수:{row.get('N점수',0)}"
            )
        return "\n\n".join(lines)
    except Exception as e:
        return f"TOP 후보 로드 실패: {e}"

# =========================================================
# Flask routes
# =========================================================

@app.route("/")
def health():
    return "ok", 200

@app.route(f"/webhook/{WEBHOOK_SECRET}", methods=["POST"])
def telegram_webhook():
    try:
        data = request.get_json(silent=True) or {}
        message = data.get("message", {})
        chat = message.get("chat", {})
        text = (message.get("text") or "").strip()
        chat_id = chat.get("id")

        if not chat_id:
            return jsonify({"ok": True})

        # /top
        if text.startswith("/top"):
            reply = load_top_candidates_text()
            send_telegram_message(chat_id, reply)
            return jsonify({"ok": True})

        # /scan 종목명
        if text.startswith("/scan"):
            parts = text.split(maxsplit=1)
            if len(parts) < 2:
                send_telegram_message(chat_id, "사용법: /scan 종목명")
                return jsonify({"ok": True})

            stock_name_input = parts[1].strip()
            code, stock_name = find_stock_code_by_name(stock_name_input)

            if not code:
                send_telegram_message(chat_id, f"종목을 찾지 못했습니다: {stock_name_input}")
                return jsonify({"ok": True})

            df = fetch_stock_df(code)
            if df is None:
                send_telegram_message(chat_id, f"차트 데이터를 불러오지 못했습니다: {stock_name}")
                return jsonify({"ok": True})

            df = get_indicators(df)
            if df is None or df.empty:
                send_telegram_message(chat_id, f"지표 계산 실패: {stock_name}")
                return jsonify({"ok": True})

            reply = build_analyze_reply(stock_name, code, df, entry_price=None)
            send_telegram_message(chat_id, reply)
            return jsonify({"ok": True})

        # /analyze 종목명 매수가
        if text.startswith("/analyze"):
            parts = text.split(maxsplit=2)

            if len(parts) < 2:
                send_telegram_message(chat_id, "사용법: /analyze 종목명 [매수가]")
                return jsonify({"ok": True})

            stock_name_input = parts[1].strip()
            entry_price = None

            if len(parts) >= 3:
                try:
                    entry_price = float(parts[2].replace(",", ""))
                except Exception:
                    entry_price = None

            code, stock_name = find_stock_code_by_name(stock_name_input)
            if not code:
                send_telegram_message(chat_id, f"종목을 찾지 못했습니다: {stock_name_input}")
                return jsonify({"ok": True})

            df = fetch_stock_df(code)
            if df is None:
                send_telegram_message(chat_id, f"차트 데이터를 불러오지 못했습니다: {stock_name}")
                return jsonify({"ok": True})

            df = get_indicators(df)
            if df is None or df.empty:
                send_telegram_message(chat_id, f"지표 계산 실패: {stock_name}")
                return jsonify({"ok": True})

            reply = build_analyze_reply(stock_name, code, df, entry_price=entry_price)
            send_telegram_message(chat_id, reply)
            return jsonify({"ok": True})

        # 기본 도움말
        help_text = (
            "사용 가능한 명령어\n"
            "/analyze 종목명 매수가\n"
            "예) /analyze 비츠로셀 21400\n\n"
            "/scan 종목명\n"
            "예) /scan 우리기술\n\n"
            "/top"
        )
        send_telegram_message(chat_id, help_text)
        return jsonify({"ok": True})

    except Exception as e:
        traceback.print_exc()
        try:
            if 'chat_id' in locals() and chat_id:
                send_telegram_message(chat_id, f"오류 발생: {e}")
        except Exception:
            pass
        return jsonify({"ok": False, "error": str(e)}), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)