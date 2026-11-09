"""Point-in-time daily observations for the independent research."""

import numpy as np
import pandas as pd

WINDOWS = (5, 10, 20, 40)
GROUPS = ("institution", "foreign", "individual", "other_corp")
DETAIL = ("financial_investment", "insurance", "investment_trust", "private_fund",
          "bank", "other_finance", "pension", "foreign_main", "other_foreign")


def ratio(numerator, denominator):
    return numerator / denominator.where(denominator > 0)


def candle_features(d):
    close, op, high, low, volume, amount = (
        d[c] for c in ("close", "open", "high", "low", "volume", "amount")
    )
    previous = close.shift()
    move = close.pct_change(fill_method=None)
    span = high - low
    tr = pd.concat([span, (high - previous).abs(), (low - previous).abs()], axis=1).max(axis=1)
    atr = tr.where(previous.notna()).rolling(20).mean().shift()
    f = pd.DataFrame(index=d.index)
    f["atr20"] = atr
    f["body_atr"] = ratio((close - op).abs(), atr)
    f["range_atr"] = ratio(span, atr)
    f["body_pct"] = 100 * ratio((close - op).abs(), op)
    f["range_pct"] = 100 * ratio(span, close)
    for n in (4, 7):
        floor = span.shift().rolling(n - 1).min()
        f[f"nr{n}"] = span.lt(floor).where(span.notna() & floor.notna())
    f["small_body_days5"] = f.body_atr.le(.35).where(f.body_atr.notna()).rolling(5).sum()
    f["tr_compression"] = ratio(tr.rolling(5).mean(), tr.shift(5).rolling(20).mean())
    f["width20_pct"] = 100 * ratio(high.rolling(20).max() - low.rolling(20).min(), close)
    f["return20_pct"] = 100 * (ratio(close, close.shift(20)) - 1)
    f["amount_5_20"] = ratio(amount.rolling(5).mean(), amount.shift(5).rolling(20).mean())
    f["volume_5_20"] = ratio(volume.rolling(5).mean(), volume.shift(5).rolling(20).mean())
    f["amount_today_20"] = ratio(amount, amount.shift().rolling(20).mean())
    f["return1_pct"] = 100 * move
    for field, values in (("volume", volume), ("amount", amount)):
        up = values.where(move.gt(0), 0).where(move.notna())
        down = values.where(move.lt(0), 0).where(move.notna())
        f[f"{field}_up_down20"] = ratio(up.rolling(20).sum(), down.rolling(20).sum())
    f["obv_delta20"] = (np.sign(move) * volume).rolling(20).sum()
    location = ratio(2 * close - low - high, span).mask(span.eq(0), 0)
    f["cmf20"] = ratio((location * volume).rolling(20).sum(), volume.rolling(20).sum())
    typical = (high + low + close) / 3
    change = typical.diff()
    money = typical * volume
    positive = money.where(change.gt(0), 0).where(change.notna()).rolling(14).sum()
    negative = money.where(change.lt(0), 0).where(change.notna()).rolling(14).sum()
    f["mfi14"] = 100 * ratio(positive, positive + negative)
    f["low_change_atr"] = ratio(low.rolling(5).min() - low.shift(5).rolling(15).min(), atr)
    f["ma224_distance_pct"] = 100 * (ratio(close, close.rolling(224).mean()) - 1)
    f["quiet"] = (f.tr_compression.le(.8) & f.width20_pct.le(15) & f.return20_pct.abs().le(5)).where(
        f[["tr_compression", "width20_pct", "return20_pct"]].notna().all(axis=1))
    f["high_amount_small_advance"] = (f.amount_today_20.ge(1.5) & move.between(0, .01)).where(
        f.amount_today_20.notna() & move.notna())
    return f


def load_flows(path):
    if path is None:
        return {}
    flow = pd.read_csv(path, dtype={"code": str})
    required = {"code", "date", "published_at", "turnover_krw", *GROUPS}
    missing = required - set(flow)
    if missing:
        raise ValueError(f"Missing investor columns: {sorted(missing)}")
    if flow.empty or not flow.code.str.fullmatch(r"\d{6}").all():
        raise ValueError("Investor data must contain six-digit stock codes")
    dates = pd.to_datetime(flow.date, format="%Y-%m-%d", errors="raise")
    if flow.published_at.isna().any() or not flow.published_at.str.contains(
            r"(?:Z|[+-]\d{2}:\d{2})$").all():
        raise ValueError("Each publication timestamp needs a timezone")
    flow["published_at"] = pd.to_datetime(flow.published_at, utc=True, format="ISO8601")
    if flow.published_at.isna().any() or flow.date.isna().any():
        raise ValueError("Missing investor date or publication timestamp")
    if (flow.published_at < dates.dt.tz_localize("Asia/Seoul") + pd.Timedelta(hours=15, minutes=30)).any():
        raise ValueError("Final daily investor data cannot precede the market close")
    if flow.duplicated(["code", "date"]).any():
        raise ValueError("Duplicate investor stock dates")
    for column in ("turnover_krw", *GROUPS, *DETAIL):
        if column in flow:
            flow[column] = pd.to_numeric(flow[column], errors="raise")
            if np.isinf(flow[column]).any():
                raise ValueError(f"Infinite investor value in {column}")
    if flow.turnover_krw.le(0).any():
        raise ValueError("Supplied turnover must be positive")
    known = flow[list(GROUPS)].notna().all(axis=1)
    # Normalized input uses KRW and must reconcile after summing the four groups.
    residual = flow[list(GROUPS)].sum(axis=1).abs()
    if (known & (residual > 10)).any():
        raise ValueError("Investor groups do not reconcile within 10 KRW")
    return {code: frame.set_index("date") for code, frame in flow.groupby("code")}


def investor_features(calendar, source):
    f = pd.DataFrame(index=calendar)
    if source is None:
        f["flow_status"] = "UNKNOWN"
        return f
    aligned = source.reindex(calendar).shift(1)
    cutoff = pd.Series(pd.to_datetime(calendar).tz_localize("Asia/Seoul") +
                       pd.Timedelta(hours=20, minutes=30), index=calendar)
    aligned = aligned.where(aligned.published_at.le(cutoff), axis=0)
    metrics = {}
    for group in (*GROUPS, *DETAIL):
        if group not in aligned:
            continue
        value = aligned[group]
        buy = value.gt(0)
        streak = buy.astype(int).groupby((~buy).cumsum()).cumsum().where(value.notna())
        metrics[f"{group}_streak"] = streak
        for n in WINDOWS:
            total = value.rolling(n).sum()
            metrics[f"{group}_net{n}"] = total
            metrics[f"{group}_buy_days{n}"] = buy.where(value.notna()).rolling(n).sum()
            metrics[f"{group}_turnover_pct{n}"] = 100 * ratio(
                total, aligned.turnover_krw.rolling(n).sum())
        net5 = metrics[f"{group}_net5"]
        prior5 = net5.shift(5)
        metrics[f"{group}_turn_positive"] = (net5.gt(0) & prior5.le(0)).where(
            net5.notna() & prior5.notna())
        current10 = metrics[f"{group}_net10"]
        previous10 = current10.shift(10)
        metrics[f"{group}_buy_strengthening"] = (current10.gt(0) & current10.gt(previous10)).where(
            current10.notna() & previous10.notna())
    f = pd.DataFrame(metrics, index=calendar)
    composition = f[[f"{group}_net20" for group in GROUPS[:3]]]
    sign = np.sign(composition).astype("Int8").astype("string")
    f["flow_combination20"] = (sign.iloc[:, 0] + "/" + sign.iloc[:, 1] + "/" + sign.iloc[:, 2]).where(
        composition.notna().all(axis=1))
    f["flow_status"] = np.where(f[[f"{g}_net20" for g in GROUPS]].notna().all(axis=1),
                                "KNOWN20", "UNKNOWN")
    return f
