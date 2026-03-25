import os
import re
import json
import math
import argparse
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import FinanceDataReader as fdr
import pytz
from jinja2 import Template

KST = pytz.timezone("Asia/Seoul")

MIN_PRICE = 5_000
MIN_AMOUNT = 3_000_000_000
NEAR_HIGH20_MIN = 85.0
NEAR_HIGH20_MAX = 100.0
UPPER_WICK_MAX = 0.20
VOL_MULT = 2.0
DISPARITY_MIN = 98.0
DISPARITY_MAX = 112.0

ENV20_PCT = 20.0
ENV40_PCT = 40.0
ENV20_NEAR_MIN = -2.0
ENV20_NEAR_MAX = 2.0
ENV40_NEAR_MIN = -10.0
ENV40_NEAR_MAX = 10.0

HTML_TEMPLATE = """
<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{{ name }}({{ code }}) 분석 리포트</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; background: #0b1020; color: #e5e7eb; }
    .wrap { max-width: 980px; margin: 0 auto; padding: 20px; }
    .card { background: #121932; border: 1px solid #263155; border-radius: 16px; padding: 18px; margin-bottom: 16px; }
    .title { font-size: 28px; font-weight: 800; margin: 0 0 8px; }
    .muted { color: #94a3b8; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; }
    .metric { background: #0f1730; border: 1px solid #243257; border-radius: 14px; padding: 12px; }
    .metric h3 { margin: 0 0 8px; font-size: 14px; color: #cbd5e1; }
    .metric .value { font-size: 24px; font-weight: 800; }
    table { width: 100%; border-collapse: collapse; }
    th, td { border-bottom: 1px solid #23304f; padding: 10px; text-align: left; vertical-align: top; }
    th { color: #cbd5e1; font-size: 14px; }
    .pass { color: #22c55e; font-weight: 800; }
    .fail { color: #ef4444; font-weight: 800; }
    .small { font-size: 13px; }
    .pill { display: inline-block; padding: 4px 10px; border-radius: 999px; background: #1e293b; border: 1px solid #334155; margin-right: 6px; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
  </style>
</head>
<body>
<div class="wrap">
  <div class="card">
    <div class="title">{{ name }} <span class="muted">({{ code }})</span></div>
    <div class="muted">생성시각: {{ generated_at }}</div>
    <div style="margin-top:10px;">
      <span class="pill">분석모드: {{ mode }}</span>
      <span class="pill">현재가: {{ fmt(price.close) }}</span>
      <span class="pill">거래대금: {{ price.amount_b }}억</span>
    </div>
  </div>

  <div class="card">
    <h2>핵심 요약</h2>
    <p>{{ summary }}</p>
    <p><strong>AI 코멘트</strong><br>{{ ai_comment }}</p>
  </div>

  <div class="card">
    <h2>기본 수치</h2>
    <div class="grid">
      <div class="metric"><h3>현재가</h3><div class="value">{{ fmt(price.close) }}</div></div>
      <div class="metric"><h3>시가</h3><div class="value">{{ fmt(price.open) }}</div></div>
      <div class="metric"><h3>거래량 배수</h3><div class="value">{{ price.vol_ratio }}배</div></div>
      <div class="metric"><h3>이격도</h3><div class="value">{{ price.disparity }}</div></div>
      <div class="metric"><h3>20일 전고점 근접도</h3><div class="value">{{ price.near_high20_pct }}%</div></div>
      <div class="metric"><h3>윗꼬리(몸통 기준)</h3><div class="value">{{ price.upper_wick_body_pct }}%</div></div>
      <div class="metric"><h3>Envelope20 하단 괴리</h3><div class="value">{{ price.env20_pct }}%</div></div>
      <div class="metric"><h3>Envelope40 하단 괴리</h3><div class="value">{{ price.env40_pct }}%</div></div>
    </div>
  </div>

  <div class="card">
    <h2>조건 판정</h2>
    <table>
      <thead>
        <tr><th>전략</th><th>조건</th><th>현재값</th><th>기준</th><th>결과</th><th>사유</th></tr>
      </thead>
      <tbody>
      {% for row in rows %}
        <tr>
          <td>{{ row.strategy }}</td>
          <td>{{ row.label }}</td>
          <td class="mono">{{ row.current }}</td>
          <td class="mono">{{ row.target }}</td>
          <td class="{{ 'pass' if row.ok else 'fail' }}">{{ '통과' if row.ok else '미달' }}</td>
          <td>{{ row.reason }}</td>
        </tr>
      {% endfor %}
      </tbody>
    </table>
  </div>

  <div class="card small muted">
    이 리포트는 GitHub Actions에서 생성되었습니다.
  </div>
</div>
</body>
</html>
"""

def safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return default
        return float(v)
    except Exception:
        return default

def fmt_int(v: Any) -> str:
    try:
        return f"{int(round(float(v))):,}"
    except Exception:
        return str(v)

def normalize_code(code: str) -> str:
    digits = re.sub(r"\D", "", code or "")
    return digits.zfill(6)

def detect_name(code: str, given_name: str = "") -> str:
    if given_name.strip():
        return given_name.strip()
    try:
        from pykrx import stock as pk
        name = pk.get_market_ticker_name(code)
        if name:
            return str(name)
    except Exception:
        pass
    return code

def load_price_history(code: str) -> pd.DataFrame:
    end = datetime.now(KST).strftime("%Y-%m-%d")
    start = (datetime.now(KST) - pd.Timedelta(days=220)).strftime("%Y-%m-%d")
    df = fdr.DataReader(code, start, end)
    if df is None or df.empty:
        raise RuntimeError(f"가격 데이터를 불러오지 못했습니다: {code}")
    df = df.rename(columns={c: c.capitalize() for c in df.columns})
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        if col not in df.columns:
            raise RuntimeError(f"필수 컬럼 누락: {col}")
    return df

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["MA20"] = out["Close"].rolling(20).mean()
    out["MA40"] = out["Close"].rolling(40).mean()
    out["VMA20"] = out["Volume"].rolling(20).mean()
    out["High20"] = out["High"].rolling(20).max()
    out["Disparity"] = (out["Close"] / out["MA20"] * 100).round(1)
    out["NearHigh20_Pct"] = (out["Close"] / out["High20"] * 100).round(1)
    tr1 = out["High"] - out["Low"]
    tr2 = (out["High"] - out["Close"].shift(1)).abs()
    tr3 = (out["Low"] - out["Close"].shift(1)).abs()
    out["TR"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    out["ATR"] = out["TR"].rolling(14).mean()
    out["Amount"] = out["Close"] * out["Volume"]
    return out

def calc_envelope(df: pd.DataFrame, period: int, pct: float) -> Dict[str, Any]:
    ma = df["Close"].rolling(period).mean()
    upper = ma * (1 + pct / 100)
    lower = ma * (1 - pct / 100)
    return {"ma": ma, "upper": upper, "lower": lower}

def check_envelope_bottom(row: pd.Series, df: pd.DataFrame) -> Dict[str, Any]:
    close = safe_float(row.get("Close"))
    env20 = calc_envelope(df, 20, ENV20_PCT)
    env40 = calc_envelope(df, 40, ENV40_PCT)
    lower20 = safe_float(env20["lower"].iloc[-1])
    lower40 = safe_float(env40["lower"].iloc[-1])
    env20_pct = round((close - lower20) / lower20 * 100, 1) if lower20 > 0 else 0.0
    env40_pct = round((close - lower40) / lower40 * 100, 1) if lower40 > 0 else 0.0
    return {
        "env20_near": ENV20_NEAR_MIN <= env20_pct <= ENV20_NEAR_MAX,
        "env40_near": ENV40_NEAR_MIN <= env40_pct <= ENV40_NEAR_MAX,
        "env20_pct": env20_pct,
        "env40_pct": env40_pct,
        "lower20": round(lower20),
        "lower40": round(lower40),
    }

def calc_upper_wick_body_ratio(row: pd.Series) -> float:
    high_p = safe_float(row.get("High"))
    open_p = safe_float(row.get("Open"))
    close_p = safe_float(row.get("Close"))
    body_top = max(open_p, close_p)
    body_size = max(abs(close_p - open_p), 1e-9)
    upper_wick = max(0.0, high_p - body_top)
    return upper_wick / body_size

def build_snapshot(row: pd.Series, df: pd.DataFrame) -> Dict[str, Any]:
    close = safe_float(row["Close"])
    open_p = safe_float(row["Open"])
    high = safe_float(row["High"])
    low = safe_float(row["Low"])
    volume = safe_float(row["Volume"])
    vma20 = safe_float(row.get("VMA20"))
    atr = safe_float(row.get("ATR"))
    amount_b = round(close * volume / 1e8, 1)
    total = max(high - low, 1e-9)
    body_size = max(abs(close - open_p), 1e-9)
    body_top = max(open_p, close)
    upper_wick_len = max(0.0, high - body_top)
    env = check_envelope_bottom(row, df)
    return {
        "open": round(open_p),
        "close": round(close),
        "high": round(high),
        "low": round(low),
        "amount_b": amount_b,
        "vol_ratio": round(volume / vma20, 1) if vma20 > 0 else 0.0,
        "disparity": round(safe_float(row.get("Disparity")), 1),
        "near_high20_pct": round(safe_float(row.get("NearHigh20_Pct")), 1),
        "ma20": round(safe_float(row.get("MA20")), 1),
        "atr": round(atr),
        "upper_wick_body_pct": round(calc_upper_wick_body_ratio(row) * 100, 1),
        "upper_wick_total_pct": round(upper_wick_len / total * 100, 1),
        "body_size": round(body_size, 1),
        "env20_pct": env["env20_pct"],
        "env40_pct": env["env40_pct"],
        "env20_near": env["env20_near"],
        "env40_near": env["env40_near"],
        "lower20": env["lower20"],
        "lower40": env["lower40"],
    }

def judge_rows(price: Dict[str, Any]) -> list[Dict[str, Any]]:
    rows = []

    def add(strategy: str, label: str, current: str, target: str, ok: bool, reason: str):
        rows.append({
            "strategy": strategy,
            "label": label,
            "current": current,
            "target": target,
            "ok": ok,
            "reason": reason,
        })

    ok = MIN_PRICE <= price["close"]
    add("종가배팅", "최소 주가", f"{fmt_int(price['close'])}", f">= {fmt_int(MIN_PRICE)}", ok,
        "가격 조건 충족" if ok else "저가주 구간이라 제외")

    ok = NEAR_HIGH20_MIN <= price["near_high20_pct"] <= NEAR_HIGH20_MAX
    add("종가배팅", "20일 전고점 근접도", f"{price['near_high20_pct']}%", f"{NEAR_HIGH20_MIN}~{NEAR_HIGH20_MAX}%", ok,
        "전고점 부근" if ok else "전고점까지 아직 멀거나 이미 돌파")

    ok = price["upper_wick_body_pct"] <= UPPER_WICK_MAX * 100
    add("종가배팅", "윗꼬리(몸통 기준)", f"{price['upper_wick_body_pct']}%", f"<= {UPPER_WICK_MAX * 100:.1f}%", ok,
        "강봉 마감" if ok else "윗꼬리가 길어 종가 힘이 약함")

    ok = price["vol_ratio"] >= VOL_MULT
    add("종가배팅", "거래량 배수", f"{price['vol_ratio']}배", f">= {VOL_MULT}배", ok,
        "거래량 폭발" if ok else "거래량 확산이 부족")

    ok = price["close"] >= price["open"]
    add("종가배팅", "양봉 마감", f"시가 {fmt_int(price['open'])} / 종가 {fmt_int(price['close'])}", "종가 >= 시가", ok,
        "양봉" if ok else "음봉 마감")

    ok = DISPARITY_MIN <= price["disparity"] <= DISPARITY_MAX
    add("종가배팅", "이격도", f"{price['disparity']}", f"{DISPARITY_MIN}~{DISPARITY_MAX}", ok,
        "적정 이격" if ok else "과열 또는 힘 부족")

    ok = price["close"] >= price["ma20"]
    add("종가배팅", "MA20 위 마감", f"종가 {fmt_int(price['close'])} / MA20 {price['ma20']}", "종가 >= MA20", ok,
        "추세선 위" if ok else "MA20 아래라 추세 확인 부족")

    ok = price["env20_near"]
    add("엔벨로프", "Envelope(20,20) 하단 근접", f"{price['env20_pct']}%", f"{ENV20_NEAR_MIN}~{ENV20_NEAR_MAX}%", ok,
        "하단 근접" if ok else "하단선과 거리 있음")

    ok = price["env40_near"]
    add("엔벨로프", "Envelope(40,40) 하단 근접", f"{price['env40_pct']}%", f"{ENV40_NEAR_MIN}~{ENV40_NEAR_MAX}%", ok,
        "하단 근접" if ok else "장기 Envelope 하단과 거리 있음")

    return rows

def summarize(rows: list[Dict[str, Any]], name: str) -> str:
    closing = [r for r in rows if r["strategy"] == "종가배팅"]
    envelope = [r for r in rows if r["strategy"] == "엔벨로프"]
    closing_pass = sum(1 for r in closing if r["ok"])
    envelope_pass = sum(1 for r in envelope if r["ok"])
    parts = [f"{name}은 종가배팅 조건 {closing_pass}/{len(closing)}개, 엔벨로프 조건 {envelope_pass}/{len(envelope)}개를 충족했습니다."]
    failed = [r for r in rows if not r["ok"]]
    if failed:
        top = failed[:3]
        reasons = ", ".join(f"{r['label']}({r['current']})" for r in top)
        parts.append(f"핵심 미달 항목은 {reasons} 입니다.")
    else:
        parts.append("모든 핵심 체크를 통과했습니다.")
    return " ".join(parts)

def make_ai_comment(price: Dict[str, Any], rows: list[Dict[str, Any]], name: str) -> str:
    closing_fail = [r for r in rows if r["strategy"] == "종가배팅" and not r["ok"]]
    envelope_fail = [r for r in rows if r["strategy"] == "엔벨로프" and not r["ok"]]

    comments = []
    if not closing_fail:
        comments.append("종가배팅 관점에서는 기본 뼈대가 상당히 잘 맞습니다.")
    else:
        if any(r["label"] == "거래량 배수" for r in closing_fail):
            comments.append("거래량이 아직 부족해 세력이 종가까지 밀어붙였다고 보기엔 증거가 약합니다.")
        if any(r["label"] == "윗꼬리(몸통 기준)" for r in closing_fail):
            comments.append("윗꼬리가 길어 종가 힘이 약하므로 다음날 갭상승 연속성은 보수적으로 보는 편이 좋습니다.")
        if any(r["label"] == "20일 전고점 근접도" for r in closing_fail):
            comments.append("전고점 근처가 아니라면 종가배팅보다는 추세 확인 단계로 보는 해석이 더 자연스럽습니다.")
    if not envelope_fail:
        comments.append("반대로 엔벨로프 하단 관점에서는 되돌림 매매 후보로는 해석 가능합니다.")
    else:
        comments.append("엔벨로프 하단 근접도도 부족하면 저점 반등형 근거 역시 강하지 않습니다.")

    if price["disparity"] > DISPARITY_MAX:
        comments.append("이격도가 높아 추격 매수는 불리할 수 있습니다.")
    elif price["disparity"] < DISPARITY_MIN:
        comments.append("이격도가 낮아 추세 탄력 확인이 더 필요합니다.")

    return " ".join(dict.fromkeys(comments))

def render_html(result: Dict[str, Any]) -> str:
    tpl = Template(HTML_TEMPLATE)
    return tpl.render(**result, fmt=fmt_int)

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--code", required=True)
    ap.add_argument("--name", default="")
    ap.add_argument("--mode", default="closing_bet", choices=["closing_bet", "envelope_bet", "all"])
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--output-json", default="reports/latest_result.json")
    ap.add_argument("--output-html", default="site/index.html")
    args = ap.parse_args()

    code = normalize_code(args.code)
    name = detect_name(code, args.name)

    df = load_price_history(code)
    df = add_indicators(df)
    row = df.iloc[-1]
    price = build_snapshot(row, df)
    rows = judge_rows(price)

    if args.mode == "closing_bet":
        rows = [r for r in rows if r["strategy"] == "종가배팅"]
    elif args.mode == "envelope_bet":
        rows = [r for r in rows if r["strategy"] == "엔벨로프"]

    summary = summarize(rows, name)
    ai_comment = make_ai_comment(price, rows, name)

    result = {
        "code": code,
        "name": name,
        "mode": args.mode,
        "generated_at": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S KST"),
        "price": price,
        "rows": rows,
        "summary": summary,
        "ai_comment": ai_comment,
    }

    out_json = Path(args.output_json)
    out_html = Path(args.output_html)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_html.parent.mkdir(parents=True, exist_ok=True)

    out_json.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    out_html.write_text(render_html(result), encoding="utf-8")

    print(f"saved: {out_json}")
    print(f"saved: {out_html}")

if __name__ == "__main__":
    main()