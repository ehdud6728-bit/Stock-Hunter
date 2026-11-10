#!/usr/bin/env python3
"""BLUE Signal Lab R1.1 - verified same-day EOD scanner (RESEARCH_ONLY).

Additive wrapper around R0.8/R1.0 selection and chart logic. Does not write
orders, use intraday quotes, or mutate Stock-Hunter. For a request equal to
KST today the existing marcap snapshot is NOT accepted as proof of EOD.

Data provider selection: prefer the proven KRX-account/pykrx route if both
KRX_DATA_ID and KRX_DATA_PW are set in this repository; else use KRX Open API
with KRX_AUTH_KEY. These are independent credentials and must never be logged.
"""
from __future__ import annotations

import argparse
import contextlib
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import re
import sys

import pandas as pd
import requests

from market_scan import load_marcap, normalise, export
from market_scan_r08 import SourcePending, krx_day, COLS
from market_scan_r08_diagnostics import scan_r08
from chart_research_r10 import generate as generate_chart_research

KST = timezone(timedelta(hours=9))
EOD_MIN_TIME_KST = time(15, 40)
MIN_MARKET_ROWS = {"KOSPI": 400, "KOSDAQ": 400}
SAMPLES = {"KOSPI": "005930", "KOSDAQ": "086520"}
REQUIRED_KRX_COLS = {"시가": "Open", "고가": "High", "저가": "Low",
                     "종가": "Close", "거래량": "Volume", "거래대금": "Amount"}


def ensure_eod_request(target: date, now: datetime) -> bool:
    """Return whether the target is KST today; reject incomplete/future days.

    15:40 is only the *earliest attempt*, not a guarantee of vendor readiness.
    Fail-closed provider proof must still pass after this gate.
    """
    if now.tzinfo is None:
        raise ValueError("Timezone-aware clock required")
    local = now.astimezone(KST)
    if target > local.date():
        raise SourcePending("SOURCE_PENDING: Future market date")
    if target == local.date() and local.time() < EOD_MIN_TIME_KST:
        raise SourcePending("SOURCE_PENDING: Current-day EOD not eligible before 15:40 KST; use separate intraday source")
    if target.weekday() >= 5:
        raise SourcePending("SOURCE_PENDING: Weekend is not a KRX regular trading day")
    return target == local.date()


def _quiet(fn, *args, **kwargs):
    # KRX library may print login identities or HTML body text; keep them out of logs.
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        return fn(*args, **kwargs)


def _provider_exception(exc: Exception, market: str) -> SourcePending:
    return SourcePending(f"KRX_PROVIDER_UNAVAILABLE: {market} {type(exc).__name__}")


def pykrx_verified_day(day: date, stock_module=None) -> pd.DataFrame:
    """Read KRX daily cross-sections with exact-date ticker AND index parity.

    The cross-sectional API has no date index: never trust it by itself, as
    older-day fallback has occurred in market libraries. Independently request
    one-day sample ticker bars on each market plus one-day KOSPI index.
    """
    if stock_module is None:
        try:
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                from pykrx import stock as stock_module
        except Exception as exc:
            raise SourcePending(f"PYKRX_UNAVAILABLE: {type(exc).__name__}") from exc
    stamp = day.strftime("%Y%m%d")
    frames = []
    for market in ("KOSPI", "KOSDAQ"):
        try:
            # "alternative=False" ensures ordinary KRX shares, as in last passed audit.
            df = _quiet(stock_module.get_market_ohlcv_by_ticker, stamp, market=market, alternative=False)
            if not isinstance(df, pd.DataFrame) or len(df) < MIN_MARKET_ROWS[market]:
                raise SourcePending(f"KRX_DATE_NOT_AVAILABLE: {day} {market} incomplete")
            if not set(REQUIRED_KRX_COLS).issubset(df.columns):
                raise SourcePending(f"KRX_SCHEMA_INVALID: {market}")
            codes = pd.Index([str(v).strip().zfill(6) for v in df.index])
            if not codes.map(lambda x: bool(re.fullmatch(r"\d{6}", x))).all() or codes.has_duplicates:
                raise SourcePending(f"KRX_TICKERS_INVALID: {market}")
            if SAMPLES[market] not in codes:
                raise SourcePending(f"KRX_SAMPLE_MISSING: {market}")
            values = df[list(REQUIRED_KRX_COLS)].apply(pd.to_numeric, errors="coerce")
            ohlc = values[["시가", "고가", "저가", "종가"]]
            if values.isna().any().any() or (values < 0).any().any():
                raise SourcePending(f"KRX_NUMERIC_INVALID: {market}")
            if (ohlc <= 0).any().any() or (values["고가"] < values["저가"]).any():
                raise SourcePending(f"KRX_OHLC_INVALID: {market}")
            # Verify requested exact market date and independently consistent values.
            one = _quiet(stock_module.get_market_ohlcv_by_date, stamp, stamp,
                         SAMPLES[market], adjusted=False)
            if not isinstance(one, pd.DataFrame) or len(one) != 1:
                raise SourcePending(f"KRX_EXACT_DAY_UNVERIFIED: {market}")
            observed = pd.Timestamp(one.index[0]).strftime("%Y%m%d")
            if observed != stamp or not {"종가", "거래량"}.issubset(one.columns):
                raise SourcePending(f"KRX_EXACT_DAY_UNVERIFIED: {market}")
            frame_index = codes.get_loc(SAMPLES[market])
            source_row = values.iloc[frame_index]
            if (float(source_row["종가"]) != float(one.iloc[0]["종가"]) or
                    float(source_row["거래량"]) != float(one.iloc[0]["거래량"])):
                raise SourcePending(f"KRX_SAMPLE_PRICE_MISMATCH: {market}")
            cleaned = values.rename(columns=REQUIRED_KRX_COLS).copy()
            cleaned.insert(0, "Code", list(codes))
            cleaned.insert(0, "Date", pd.Timestamp(day))
            cleaned.insert(2, "Name", "")  # prior marcap names are attached later
            cleaned["Market"] = market
            frames.append(cleaned[COLS])
        except SourcePending:
            raise
        except Exception as exc:
            raise _provider_exception(exc, market) from exc

    try:
        index = _quiet(stock_module.get_index_ohlcv_by_date, stamp, stamp, "1001")
        if not isinstance(index, pd.DataFrame) or len(index) != 1 or pd.Timestamp(index.index[0]).strftime("%Y%m%d") != stamp:
            raise SourcePending("KRX_INDEX_DATE_UNVERIFIED")
    except SourcePending:
        raise
    except Exception as exc:
        raise _provider_exception(exc, "KOSPI_INDEX") from exc
    result = pd.concat(frames, ignore_index=True)
    if result.duplicated(["Code", "Date"]).any():
        raise SourcePending("KRX_DUPLICATE_TICKERS")
    return result


def openapi_verified_day(day: date, key: str, session=requests) -> pd.DataFrame:
    """Wrap existing KRX Open API adapter with strict returned BAS_DD proof."""
    if not key:
        raise SourcePending("KRX_AUTH_KEY_MISSING")

    class DateVerifiedSession:
        def get(self, url, params, headers, timeout):
            response = session.get(url, params=params, headers=headers, timeout=timeout)
            response.raise_for_status()
            payload = response.json()
            rows = payload.get("OutBlock_1") if isinstance(payload, dict) else None
            if isinstance(rows, list) and rows:
                stamp = day.strftime("%Y%m%d")
                if any(not isinstance(row, dict) or str(row.get("BAS_DD", "")).replace("-", "") != stamp for row in rows):
                    raise SourcePending("KRX_OPENAPI_DATE_MISMATCH_OR_MISSING")
            return response

    return krx_day(day, key, session=DateVerifiedSession())


def selected_provider(provider: str, key: str, has_account: bool) -> str:
    if provider not in {"auto", "openapi", "pykrx"}:
        raise ValueError("Bad KRX provider")
    if provider == "auto":
        provider = "pykrx" if has_account else "openapi"
    if provider == "openapi" and not key:
        raise SourcePending("KRX_AUTH_KEY_MISSING_IN_BLUE_REPO")
    if provider == "pykrx" and not has_account:
        raise SourcePending("KRX_ACCOUNT_SECRETS_MISSING_IN_BLUE_REPO")
    return provider


def upgraded_history(raw: pd.DataFrame, target: date, today: bool, provider: str,
                     key: str, stock_module=None, session=requests, max_days: int = 45):
    """Replace incomplete/required target with proved daily rows; preserve history.

    On today, force KRX proof even if marcap already has >=1700 rows. On older
    days, preserve exact existing well-covered baseline dates and results.
    """
    prices = normalise(raw)
    if prices.empty:
        raise SourcePending("MARCAP_NO_VALID_RECORDS")
    latest = prices.date.max().date()
    current_rows = prices.loc[prices.date.dt.date.eq(target)]
    if not today and target <= latest and len(current_rows) >= 1700:
        return raw, [], "FinanceData/marcap daily"
    if target > latest:
        gap = (target - latest).days
        if gap > max_days:
            raise SourcePending(f"BACKFILL_TOO_LARGE: {gap} days; refresh marcap first")
        days = [latest + timedelta(days=i) for i in range(1, gap+1)
                if (latest+timedelta(days=i)).weekday() < 5]
    else:
        days = [target]
    additions = []
    added_days = []
    for d in days:
        try:
            if provider == "pykrx":
                daily = pykrx_verified_day(d, stock_module=stock_module)
            else:
                daily = openapi_verified_day(d, key, session=session)
        except SourcePending as exc:
            # No normal weekday is silently accepted as missing for target.
            if d != target and str(exc).startswith("KRX_DATE_NOT_AVAILABLE:"):
                continue  # intermediate exchange holiday
            raise
        additions.append(daily)
        added_days.append(d.isoformat())
    if target.isoformat() not in added_days:
        raise SourcePending("KRX_TARGET_DATE_NOT_PROVEN")
    base = raw.reset_index() if "Date" not in raw.columns and raw.index.name == "Date" else raw.copy()
    if "Date" not in base.columns or "Code" not in base.columns:
        raise SourcePending("MARCAP_DATE_CODE_MISSING")
    dates_to_replace = {date.fromisoformat(v) for v in added_days}
    base = base.loc[~pd.to_datetime(base.Date).dt.date.isin(dates_to_replace)].copy()
    incoming = pd.concat(additions, ignore_index=True)
    # Preserve known stock names from historical marcap where pykrx only supplies code.
    if "Name" in raw.columns:
        named = raw.copy()
        if "Date" not in named.columns and named.index.name == "Date":
            named = named.reset_index()
        named = named.loc[pd.to_datetime(named.Date).dt.date.lt(target)]
        if not named.empty:
            named["Code"] = named.Code.astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(6)
            names = named.sort_values("Date").drop_duplicates("Code", keep="last").set_index("Code")["Name"]
            empty = incoming["Name"].astype(str).eq("")
            incoming.loc[empty, "Name"] = incoming.loc[empty, "Code"].map(names).fillna("")
    for col in base.columns:
        if col not in incoming.columns:
            incoming[col] = None
    combined = pd.concat([base, incoming[base.columns]], ignore_index=True)
    return combined, added_days, ("KRX pykrx verified account daily" if provider == "pykrx"
                                  else "KRX Open API verified daily")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--date", default="latest")
    p.add_argument("--years", default="2025,2026")
    p.add_argument("--out", default="output")
    p.add_argument("--local-data-dir")
    p.add_argument("--daily-provider", choices=["auto", "openapi", "pykrx"], default="auto")
    args = p.parse_args()
    raw, hashes = load_marcap([int(x) for x in args.years.split(",")], args.local_data_dir)
    latest = normalise(raw).date.max().date()
    target = latest if args.date == "latest" else date.fromisoformat(args.date)
    today = ensure_eod_request(target, datetime.now(timezone.utc))
    key = os.getenv("KRX_AUTH_KEY", "").strip()
    has_account = bool((os.getenv("KRX_DATA_ID") or os.getenv("KRX_ID")) and
                       (os.getenv("KRX_DATA_PW") or os.getenv("KRX_PW")))
    prices = normalise(raw)
    requires_fetch = today or target > prices.date.max().date() or len(prices.loc[prices.date.dt.date.eq(target)]) < 1700
    if requires_fetch:
        provider = selected_provider(args.daily_provider, key, has_account)
        upgraded, days, target_source = upgraded_history(raw, target, today, provider, key)
    else:
        upgraded, days, target_source = raw, [], "FinanceData/marcap daily"
    rows, audit = scan_r08(upgraded, target.isoformat())
    for row in rows:
        row["source"] = target_source
        row["data_frequency"] = "EOD_DAILY"
        row["intraday_verified"] = False
    audit.update({
        "target_day_source": target_source,
        "supplemental_dates": days,
        "same_day_eod": today,
        "data_phase": "SAME_DAY_EOD_VERIFIED" if today else "HISTORICAL_EOD",
        "intraday_verified": False,
        "source_policy": "MARCAP_BASELINE_PLUS_EXACT_DATE_KRX; NEVER_INTRADAY; NO_SILENT_FALLBACK",
        "source_verified_at_kst": datetime.now(KST).isoformat(timespec="seconds"),
    })
    if days:
        supplement = upgraded.loc[pd.to_datetime(upgraded.Date).dt.date.isin(
            {date.fromisoformat(d) for d in days})].sort_values(["Date", "Code"])
        audit["supplemental_rows"] = len(supplement)
        audit["supplemental_sha256"] = hashlib.sha256(
            supplement[COLS].to_csv(index=False).encode("utf-8")).hexdigest()
    export(rows, audit, args.out, hashes)
    count = generate_chart_research(upgraded, rows, target.isoformat(),
                                    str(Path(args.out) / "chart_snapshots.jsonl"))
    if count != len(rows):
        raise ValueError("Incomplete chart snapshots: refusing partial publish")
    print("BLUE_R11_EOD_STATUS", json.dumps({
        "asof": target.isoformat(), "data_phase": audit["data_phase"],
        "source": target_source, "observed": audit["observed_stocks"],
        "signals": len(rows), "intraday_verified": False,
    }, ensure_ascii=False))


if __name__ == "__main__":
    try:
        main()
    except SourcePending as exc:
        # No credentials, provider URLs or raw response bodies in exception type.
        print("SOURCE_PENDING:", str(exc), file=sys.stderr)
        sys.exit(20)
