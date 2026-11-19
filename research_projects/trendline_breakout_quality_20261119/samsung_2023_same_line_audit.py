#!/usr/bin/env python3
"""Samsung SDI 2023-11-02 -> 2023-11-06 original-line identity audit.

RESEARCH_ONLY. Reads pinned historical OHLCV and canonical R111/R116 code.
The screenshot adjustment factor is observational; its corporate-action
provenance and applicability to earlier dates are NOT independently certified.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "research_projects/r1d97/data/006400_KRX.csv"
SOURCE_SHA256 = "d0e1ec07da7972728b84865c54312e387db20c3e3dd9dadc813dada394635566"
ALGORITHM = "TS_BODY_WICK_PROXY"
EXPECTED_ID = (
    "TS_BODY_WICK_PROXY|BEAR_BODY_TOP|2023-09-19|2023-10-18|UPPER"
)
SCREENSHOT_2023_11_06 = {
    "open": 467962, "high": 511038, "low": 455725, "close": 495374
}
FACTOR = 0.979


def load_frozen(source: Path = SOURCE) -> pd.DataFrame:
    if hashlib.sha256(source.read_bytes()).hexdigest() != SOURCE_SHA256:
        raise ValueError("FROZEN_KRX_SOURCE_HASH_CHANGED")
    raw = pd.read_csv(source, dtype={"code": str})
    if set(raw.code) != {"006400"} or raw.date.duplicated().any():
        raise AssertionError("SAMSUNG_SOURCE_IDENTITY_INVALID")
    return raw


def replay(raw: pd.DataFrame):
    sys.path.insert(0, str(ROOT / "research_projects/r1d111"))
    sys.path.insert(0, str(ROOT / "research_projects/r1d116"))
    from algorithms import prepare, make_anchors, generate_candidates
    from recent_turn import line_level, replay_one

    d = prepare(raw)
    catalog = generate_candidates(d, ALGORITHM, make_anchors(d))
    found = [q for q in catalog if q["line_id"] == EXPECTED_ID]
    if len(found) != 1:
        raise AssertionError("EXPECTED_LINE_NOT_UNIQUELY_GENERATED")
    q = found[0]
    marks = pd.DataFrame({
        "idx": np.arange(len(d)),
        "date": d.date.dt.strftime("%Y-%m-%d"),
        "close": d.close,
    })
    daily, _, _ = replay_one("006400", ALGORITHM, catalog, marks)
    focus = daily.loc[
        daily.scale.eq("MEDIUM") & daily.kind.eq("UPPER")
        & daily.date.isin(["2023-11-02", "2023-11-03", "2023-11-06"])
    ].set_index("date")
    if len(focus) != 3:
        raise AssertionError("FOCAL_DATES_MISSING")
    for day in ["2023-11-02", "2023-11-03"]:
        if focus.loc[day, "line_id"] != EXPECTED_ID:
            raise AssertionError("PREBREAK_LINE_ID_CHANGED " + day)
    cross = focus.loc["2023-11-06"]
    if cross["prior_line_id"] != EXPECTED_ID or (
        cross["prior_line_cross"] != "CROSSED_ABOVE_PREVIOUS_ACTIVE_RESISTANCE"
    ):
        raise AssertionError("NOV06_DID_NOT_CROSS_IDENTICAL_PRIOR_LINE")
    if q["known_date"] > "2023-11-02" or not q["known_idx"] < int(focus.loc["2023-11-02", "idx"]):
        raise AssertionError("LINE_LOOKAHEAD")

    nov6 = d.loc[d.date.eq(pd.Timestamp("2023-11-06"))].iloc[0]
    original = {key: int(nov6[key]) for key in SCREENSHOT_2023_11_06}
    residuals = {
        key: round(SCREENSHOT_2023_11_06[key] - original[key] * FACTOR, 5)
        for key in original
    }
    if any(abs(x) > 0.51 for x in residuals.values()):
        raise AssertionError("SCREENSHOT_CORRECTION_NOT_UNIFORM")
    dates = ["2023-11-02", "2023-11-03", "2023-11-06"]
    levels = {
        day: round(line_level(q, int(d.index[d.date.eq(pd.Timestamp(day))][0])), 4)
        for day in dates
    }
    result = {
        "revision": "SAMSUNG_SDI_2023_SAME_LINE_REPRODUCED_R5_PRECHECK",
        "research_only": True, "operational_changes": False,
        "source_file": str(SOURCE.relative_to(ROOT)),
        "source_sha256": SOURCE_SHA256,
        "event_year": 2023,
        "line_id": EXPECTED_ID,
        "anchors": [
            {"date": q["a1"], "bar_idx": q["i1"], "body_top_raw": q["v1"]},
            {"date": q["a2"], "bar_idx": q["i2"], "body_top_raw": q["v2"]},
        ],
        "known_date": q["known_date"], "slope_krw_per_trading_bar": q["m"],
        "r116_medium_active_2023_11_02": focus.loc["2023-11-02", "line_id"],
        "r116_medium_active_2023_11_03": focus.loc["2023-11-03", "line_id"],
        "r116_medium_prior_line_on_2023_11_06": cross["prior_line_id"],
        "r116_cross_at_close_2023_11_06": cross["prior_line_cross"],
        "r116_active_line_after_break_2023_11_06": cross["line_id"],
        "raw_line_levels": levels,
        "assumed_adjusted_levels": {k: round(v * FACTOR, 4) for k, v in levels.items()},
        "raw_2023_11_06_ohlc": original,
        "user_screenshot_2023_11_06_ohlc": SCREENSHOT_2023_11_06,
        "observed_screenshot_to_raw_factor": FACTOR,
        "adjustment_residuals_krw": residuals,
        "line_identity_same": True,
        "correction_factor_uniform_on_screenshot_day": True,
        "historical_adjustment_provenance_certified": False,
        "uniform_historical_adjustment_before_nov06_proven": False,
        "not_a_2026_event": True,
        "may_modify_live_search_watch": False,
    }
    return d, q, result


def plot(d: pd.DataFrame, line: dict, out: Path):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.patches import Rectangle
    plt.rcParams["font.family"] = "Noto Sans CJK KR"
    plt.rcParams["axes.unicode_minus"] = False
    a = int(d.index[d.date.eq(pd.Timestamp("2023-09-11"))][0])
    b = int(d.index[d.date.eq(pd.Timestamp("2023-11-24"))][0])
    x = np.arange(a, b + 1)
    v = d.iloc[x]
    fig, axs = plt.subplots(2, 1, figsize=(15, 10), sharex=True, layout="constrained")
    for ax, factor, label in zip(
        axs, (1.0, FACTOR), ("원본 KRX 연구값", "제공 차트 0.979배 환산 (추정)")
    ):
        for idx in x:
            bar = d.iloc[idx]
            color = "#cc4a45" if bar.close >= bar.open else "#2465a9"
            ax.vlines(idx, bar.low * factor, bar.high * factor, color=color, lw=0.85)
            ax.add_patch(Rectangle(
                (idx - 0.28, min(bar.open, bar.close) * factor),
                0.56, max(1, abs(bar.open - bar.close) * factor),
                facecolor=color, edgecolor=color, alpha=0.88,
            ))
        pre = x[x < int(line["known_idx"])]
        live = x[x >= int(line["known_idx"])]
        lv = lambda indices: (float(line["v1"]) + float(line["m"]) * (indices - int(line["i1"]))) * factor
        pre = pre[pre >= int(line["i1"])]
        if len(pre): ax.plot(pre, lv(pre), color="#f49a26", ls=":", lw=1.7, alpha=0.7)
        ax.plot(live, lv(live), color="#f49a26", lw=2.4, label="동일한 R116 중기 저항선")
        ax.scatter([line["i1"], line["i2"]], np.array([line["v1"], line["v2"]]) * factor,
                   s=50, color="#d46f00", edgecolor="black", zorder=8)
        for date, color in [("2023-11-02", "#447acc"), ("2023-11-06", "#bd3e84")]:
            t = int(d.index[d.date.eq(pd.Timestamp(date))][0])
            ax.axvline(t, color=color, lw=1.35, ls="--")
            ax.text(t, ax.get_ylim()[1], date[5:], color=color, ha="center", va="top")
        ax.set_ylabel("원 / " + label)
        ax.legend(loc="upper right")
        ax.grid(alpha=0.18)
    ticks = np.linspace(a, b, 9, dtype=int)
    axs[-1].set_xticks(ticks)
    axs[-1].set_xticklabels(d.date.iloc[ticks].dt.strftime("%m-%d"), rotation=25)
    axs[0].set_title("삼성SDI 006400 | 2023-11-02 동일 추세선 → 2023-11-06 돌파 | 실제 OHLC")
    path = out / "SamsungSDI_2023-11-06_RAW_vs_0979_adjusted.png"
    fig.savefig(path, dpi=145)
    plt.close(fig)
    return path


def run(out: Path):
    original = load_frozen()
    d, line, result = replay(original)
    out.mkdir(parents=True, exist_ok=True)
    chart = plot(d, line, out)
    result["chart_file"] = chart.name
    (out / "SamsungSDI_2023_SAME_LINE_AUDIT.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return result


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--out", required=True)
    args = p.parse_args()
    r = run(Path(args.out))
    print(json.dumps(r, ensure_ascii=False, indent=2))
