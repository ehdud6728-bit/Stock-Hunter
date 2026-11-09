"""RESEARCH_ONLY. Run from the shared workspace against frozen research inputs."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys

import numpy as np
import pandas as pd

from features import GROUPS, DETAIL, candle_features, investor_features, load_flows, ratio

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
BASE = ROOT / "BLUE20_EIGHT_VARIANTS"
EARLY = ROOT / "BLUE20_EARLY_WAVE"
REFERENCE = HERE / "reference"
BUNDLED = REFERENCE.is_dir()
if BUNDLED:
    sys.path.insert(0, str(HERE / "vendor"))
    from blue20_bands import bands, signal
else:
    sys.path[:0] = [str(BASE), str(BASE / "vendor"), str(EARLY)]
    from research import bands, signal
from loader import load_prices
from core import indicators

HORIZONS = (5, 10, 20, 40, 60)
CONFIGS = [(n, k, lag) for k, lag in ((2., 26), (1.8, 20)) for n in (20, 25, 30, 35, 40)]


def forward(d, p1, episodes):
    """Outcomes only. Mark incomplete windows even when an early hit was seen."""
    result = pd.DataFrame(index=d.index)
    entry = d.open.shift(-1)
    target, stop = entry * 1.10, entry * .95
    realized = pd.Series(np.nan, index=d.index)
    trade_reason = pd.Series(None, index=d.index, dtype=object)
    max_high = pd.Series(-np.inf, index=d.index)
    min_low = pd.Series(np.inf, index=d.index)
    complete = pd.Series(True, index=d.index)
    next_wave = pd.Series(np.nan, index=d.index)
    for offset in range(1, max(HORIZONS) + 1):
        op, high, low, close = (d[c].shift(-offset) for c in ("open", "high", "low", "close"))
        complete &= op.gt(0) & high.notna() & low.notna() & close.notna()
        max_high = np.maximum(max_high, high)
        min_low = np.minimum(min_low, low)
        next_wave = next_wave.mask(next_wave.isna() & p1.shift(-offset, fill_value=False), offset)
        sl_gap, tp_gap = op.le(stop), op.ge(target)
        sl, tp = low.le(stop), high.ge(target)
        live = realized.isna() & complete
        realized = realized.mask(live, np.select(
            [sl_gap | tp_gap, sl, tp], [100 * (op / entry - 1) - .25, -5.25, 9.75], default=np.nan))
        trade_reason = trade_reason.mask(live, np.select(
            [sl_gap, tp_gap, sl, tp], ["SL", "TP", "SL", "TP"], default="TIME"))
        if offset not in HORIZONS:
            continue
        close_return = 100 * (close / entry - 1)
        reason = trade_reason.where(complete)
        result[f"observed{offset}"] = complete
        result[f"wave{offset}"] = next_wave.notna().where(complete)
        result[f"days_to_wave{offset}"] = next_wave.where(complete)
        result[f"no_wave_path{offset}"] = pd.Series(np.select(
            [close_return.lt(-5), close_return.gt(5)], ["DOWN", "UP"], default="SIDEWAYS"),
            index=d.index).where(complete & next_wave.isna())
        result[f"high{offset}"] = (100 * (max_high / entry - 1)).where(complete)
        result[f"low{offset}"] = (100 * (min_low / entry - 1)).where(complete)
        result[f"net{offset}"] = realized.fillna(close_return - .25).where(complete)
        result[f"exit{offset}"] = reason
    # Follow the first P1's existing P2/P3 chain, without using it in candidate selection.
    position = pd.Series(np.arange(len(d)), index=d.index)
    wave_position = position + result.days_to_wave60
    date_at = pd.Series(d.index, index=np.arange(len(d)))
    wave_date = wave_position.map(date_at)
    result["first_p1_date60"] = wave_date
    for c in ("final_status", "known_date"):
        result[f"first_p1_{c}"] = wave_date.map(episodes[c])
    # P1 next-open trade has its own 20-session clock.
    wave_net = pd.Series(result.net20.to_numpy(), index=np.arange(len(d)))
    for horizon in HORIZONS:
        result[f"wait_p1_net20_within{horizon}"] = (position + result[f"days_to_wave{horizon}"]).map(wave_net)
    return result


def line_states(d, candles):
    states = {}
    for n, multiplier, lag in CONFIGS:
        model = f"BB{n}" if lag == 26 else f"BB{n}_S18_SHIFT20"
        lines = bands(d.close, n, multiplier, lag)
        lines["CENTER"] = d.close.rolling(n).mean().shift(lag)
        width = ratio(lines["UPPER"] - lines["LOWER"], lines["CENTER"])
        ref = width.shift().rolling(20).median()
        squeeze = width.le(.8 * ref).where(width.notna() & ref.notna())
        states[model] = (lines, squeeze)
    center = d.close.rolling(20).mean()
    states["ENV20"] = ({"CENTER": center, "UPPER": 1.4 * center, "LOWER": .6 * center}, None)
    output = {}
    signals = []
    for model, (lines, squeeze) in states.items():
        for side, line in lines.items():
            distance = ratio(d.close - line, candles.atr20.reindex(d.index))
            output[f"{model}:{side}_near"] = distance.abs().le(.5).where(distance.notna())
            if side != "CENTER" or model == "ENV20":
                for date in d.index[signal(d, line) & d.index.to_series().between("2023-01-01", "2024-12-31")]:
                    signals.append((model, d.code.iloc[0], date, side))
        if squeeze is not None:
            output[f"{model}:squeeze"] = squeeze
            prior = squeeze.shift()
            output[f"{model}:squeeze_start"] = (squeeze.eq(True) & prior.eq(False)).where(
                squeeze.notna() & prior.notna())
    return pd.DataFrame(output, index=d.index), signals


def split_features(f, recent_wave):
    known = f.quiet.notna() & recent_wave.notna()
    flags = {"QUIET": f.quiet, "NR4": f.nr4, "NR7": f.nr7}
    for name, column, cut, direction in [
        ("SMALL_BODY", "body_atr", .35, "le"), ("SMALL_RANGE", "range_atr", .65, "le"),
        ("SMALL_BODIES_4_OF_5", "small_body_days5", 4, "ge"), ("BELOW_MA224", "ma224_distance_pct", 0, "lt"),
        ("POSITIVE_CMF", "cmf20", .10, "ge"), ("POSITIVE_OBV", "obv_delta20", 0, "gt"),
        ("POSITIVE_MFI", "mfi14", 55, "ge"), ("UP_VOLUME_DOMINANT", "volume_up_down20", 1.25, "ge"),
        ("UP_AMOUNT_DOMINANT", "amount_up_down20", 1.25, "ge"), ("RISING_LOWS", "low_change_atr", 0, "gt"),
        ("LOW_DEFENSE", "low_change_atr", -.25, "ge")
    ]:
        value = f[column]
        flags[name] = getattr(value, direction)(cut).where(value.notna())
    flags["HIGH_AMOUNT_SMALL_ADVANCE"] = f.high_amount_small_advance
    quiet = f.quiet.eq(True)
    known_turnover = f[["amount_5_20", "volume_5_20"]].notna().all(axis=1)
    for name, condition in [
        ("QUIET_DRY", f.amount_5_20.le(.8) & f.volume_5_20.le(.8)),
        ("QUIET_ACTIVE", f.amount_5_20.ge(1.1) & f.volume_5_20.ge(1.1)),
        ("QUIET_POSITIVE_CMF", flags["POSITIVE_CMF"].eq(True)),
        ("QUIET_NEGATIVE_CMF", f.cmf20.le(-.10)),
        ("QUIET_RISING_LOWS", flags["RISING_LOWS"].eq(True)),
    ]:
        available = (known_turnover if name in ("QUIET_DRY", "QUIET_ACTIVE")
                     else f.low_change_atr.notna() if name == "QUIET_RISING_LOWS" else f.cmf20.notna())
        flags[name] = condition.where(quiet & available)
    for group in (*GROUPS, *DETAIL):
        net = f.get(f"{group}_net20")
        if net is None:
            continue
        persistent = (net.gt(0) & f[f"{group}_buy_days20"].ge(12)).where(net.notna())
        flags[f"{group}:PERSISTENT"] = persistent
        flags[f"{group}:PERSISTENT_FLAT"] = persistent.where(f.return20_pct.abs().le(5))
        flags[f"{group}:PERSISTENT_DOWN"] = persistent.where(f.return20_pct.lt(-5))
        turning = f[f"{group}_turn_positive"]
        flags[f"{group}:TURN_POSITIVE"] = turning
        strengthening = f[f"{group}_buy_strengthening"]
        flags[f"{group}:QUIET_STRENGTHENING"] = strengthening.where(quiet)
        flags[f"{group}:FLAT_STRENGTHENING"] = strengthening.where(f.return20_pct.abs().le(5))
        flags[f"{group}:COMPRESSED_STRENGTHENING"] = strengthening.where(f.tr_compression.le(.8))
        flags[f"{group}:BELOW224_PERSISTENT"] = persistent.where(flags["BELOW_MA224"].eq(True))
    flags = pd.DataFrame(flags, index=f.index)
    flags.insert(0, "DAILY", True)
    return flags.where(known & recent_wave.eq(False), axis=0)


def summaries(daily, flag_names):
    rows = []
    years = daily.year.to_numpy()
    codes = pd.factorize(daily.code)[0]
    fields = {}
    for n in HORIZONS:
        fields[n] = {field: daily[f"{field}{n}"].to_numpy() for field in (
            "observed", "days_to_wave", "no_wave_path", "net", "exit", "wait_p1_net20_within")}
    phase = daily.first_p1_final_status.fillna("").to_numpy()
    for year in ("ALL", *sorted(pd.unique(years))):
        include = np.ones(len(daily), dtype=bool) if year == "ALL" else years == year
        for flag in flag_names:
            if flag not in daily:
                continue
            groups, values = pd.factorize(daily[flag], sort=True)
            for index, value in enumerate(values):
                state = np.flatnonzero((groups == index) & include)
                if not len(state):
                    continue
                n_codes = len(np.unique(codes[state]))
                for horizon in HORIZONS:
                    outcome = fields[horizon]
                    usable = state[outcome["observed"][state]]
                    path = outcome["no_wave_path"][usable]
                    delay = outcome["days_to_wave"][usable]
                    reached = np.isfinite(delay)
                    confirmed = outcome["wait_p1_net20_within"][usable[reached]]
                    confirmed = confirmed[np.isfinite(confirmed)]
                    status = phase[usable[reached]] if horizon == 60 else np.array([], dtype=str)
                    rows.append(dict(year=year, feature=flag, value=value, days=len(state),
                                     codes=n_codes, horizon=horizon, complete=len(usable),
                                     p1=int(reached.sum()), p1_pct=100 * reached.mean() if len(usable) else np.nan,
                                     mean_delay=delay[reached].mean() if reached.any() else np.nan,
                                     sideways=int((path == "SIDEWAYS").sum()), down=int((path == "DOWN").sum()),
                                     up_without_p1=int((path == "UP").sum()),
                                     next_open_mean_net=outcome["net"][usable].mean() if len(usable) else np.nan,
                                     next_open_tp_pct=100 * (outcome["exit"][usable] == "TP").mean() if len(usable) else np.nan,
                                     wait_p1_closed=len(confirmed), wait_p1_mean_net=confirmed.mean() if len(confirmed) else np.nan,
                                     failed_first_wave=int((status == "P1_BASE_LOW_FAILED").sum()),
                                     failed_pullback=int((status == "P2_BASE_LOW_FAILED").sum())))
    return pd.DataFrame(rows)


def compare_breakouts(f, baseline):
    merged = baseline.merge(f, on=["code", "date"], how="left", validate="many_to_one")
    merged = merged[merged.complete & merged.nonoverlap60]
    rows = []
    for keys, base in merged.groupby(["model", "band", "year"]):
        for feature in ["BASE", "quiet", "nr7", "cmf20", "low_change_atr", "flow_status"]:
            if feature == "BASE":
                groups = [("ALL", base)]
            elif feature in ("cmf20", "low_change_atr"):
                group = base[feature].gt(0).where(base[feature].notna())
                groups = base.groupby(group, observed=True)
            else:
                groups = base.groupby(feature, observed=True)
            for group, sample in groups:
                rows.append(dict(model=keys[0], band=keys[1], year=keys[2], feature=feature,
                                 value=group, n=len(sample), p10_pct=100 * sample.hit.mean(),
                                 mean_net=sample.net_pct.mean(), baseline_n=len(base)))
        for investor in GROUPS:
            field = f"{investor}_net20"
            if field not in base:
                continue
            group = base[field].gt(0).where(base[field].notna())
            for value, sample in base.groupby(group, observed=True):
                rows.append(dict(model=keys[0], band=keys[1], year=keys[2], feature=field, value=value,
                                 n=len(sample), p10_pct=100 * sample.hit.mean(),
                                 mean_net=sample.net_pct.mean(), baseline_n=len(base)))
    return pd.DataFrame(rows)


def baseline_ledgers():
    if BUNDLED:
        return pd.read_csv(REFERENCE / "BREAKOUTS.csv.gz", dtype={"code": str})
    bb = pd.concat([pd.read_csv(path, dtype={"code": str}, low_memory=False) for path in (
        BASE / "results/TRADES.csv.gz", ROOT / "BLUE20_TEN_VARIANTS/results/BB30_TRADES.csv.gz")])
    bb = bb[bb.target_pct.eq(10)].assign(complete=lambda t: t.full60, hit=lambda t: t.status.eq("TP"))
    env = pd.read_csv(ROOT / "BLUE20_ENVELOPE_COMPARE/results/EVENTS.csv.gz", dtype={"code": str})
    env = env[env.model.eq("ENV20") & env.entry_rule.eq("SIGNAL_NEXT_OPEN")].assign(
        complete=lambda t: t.complete60.eq(True), hit=lambda t: t.status.eq("TP10"))
    fields = ["model", "band", "year", "code", "date", "nonoverlap60", "complete", "hit", "net_pct"]
    return pd.concat([bb[fields], env[fields]], ignore_index=True)


def describe_states(contexts, group_columns):
    metrics = ["tr_compression", "amount_5_20", "cmf20", "low_change_atr",
               *[f"{group}_turnover_pct20" for group in (*GROUPS, *DETAIL)]]
    rows = []
    for keys, group in contexts.groupby(group_columns):
        for metric in metrics:
            values = group[metric].dropna() if metric in group else pd.Series(dtype=float)
            rows.append(dict(zip(group_columns, keys)) | dict(metric=metric, rows=len(group),
                             known=len(values), mean=values.mean(), median=values.median()))
    return pd.DataFrame(rows)


def main(manifest, flows, out):
    out.mkdir(parents=True, exist_ok=True)
    if BUNDLED:
        for name, expected in json.loads((REFERENCE / "SHA256.json").read_text()).items():
            actual = hashlib.sha256((REFERENCE / name).read_bytes()).hexdigest()
            if actual != expected:
                raise ValueError(f"Frozen research reference changed: {name}")
    frames = load_prices(manifest)
    frozen = json.loads(((REFERENCE if BUNDLED else BASE / "reference") / "PRICE_SOURCE_HASHES.json").read_text())
    assert set(frames) == set(frozen)
    calendar = pd.Index(sorted({day for d in frames.values() for day in d.date}), name="date")
    early_ref = REFERENCE if BUNDLED else EARLY / "results"
    all_signals = pd.read_csv(early_ref / "SIGNALS.csv.gz", dtype={"code": str}, low_memory=False)
    episodes = pd.read_csv(early_ref / "EPISODES.csv.gz", dtype={"code": str})
    episodes = episodes[episodes.origin_stage.eq("P1")]
    investor = load_flows(flows)
    daily_tables, stage_tables, band_rows, baseline_contexts, ledgers, squeeze_tables = [], [], [], [], [], []
    flag_names = set()
    wave_keys = set()
    for number, (code, observed) in enumerate(frames.items(), 1):
        assert hashlib.sha256(observed.to_csv(index=False, lineterminator="\n").encode("utf-8-sig")).hexdigest() == frozen[code]
        origin = indicators(observed).set_index("date")
        d = observed.set_index("date").reindex(calendar)
        f = candle_features(d).join(investor_features(calendar, investor.get(code)))
        context, keys = line_states(observed.set_index("date"), f)
        band_rows.extend(keys)
        f = f.join(context)
        f["code"], f["year"] = code, calendar.str[:4].astype(int)
        p1 = origin.p1_signal.reindex(calendar, fill_value=False)
        observed_days = d.close.notna() & calendar.to_series().between("2023-01-01", "2024-12-31")
        wave_keys.update((code, date) for date in calendar[p1 & observed_days])
        prior = origin.p1_raw.reindex(calendar, fill_value=False).rolling(21).max().astype("boolean")
        prior = prior.where(d.close.notna().rolling(21).sum().eq(21))
        flags = split_features(f, prior)
        eligible = flags.DAILY.eq(True)
        proximity = context.loc[:, context.columns.str.endswith("_near")].reindex(calendar)
        flags = pd.concat([flags, context.reindex(calendar).where(eligible, axis=0),
                           proximity.where(eligible & f.quiet.eq(True), axis=0).add_suffix(":quiet")], axis=1)
        if "flow_combination20" in f:
            flags["FLOW_COMBINATION20"] = f.flow_combination20.where(flags.DAILY.eq(True))
        flag_names.update(flags.columns)
        outcomes = forward(d, p1, episodes[episodes.code.eq(code)].set_index("p1_date"))
        joined = pd.concat([f[["code", "year", "flow_status"]], flags, outcomes], axis=1).copy()
        daily_tables.append(joined.loc[observed_days].reset_index())
        feature_export = f.loc[observed_days, [c for c in f if c not in context]].reset_index()
        ledgers.append(feature_export)
        stock_events = all_signals.loc[all_signals.code.eq(code), ["code", "date", "stage", "p1_date"]]
        previous = calendar.to_series().shift()
        pre = stock_events[stock_events.stage.eq("P1")].assign(
            date=lambda t: t.date.map(previous), stage="P1_PREVIOUS_SESSION")
        stock_events = pd.concat([stock_events, pre], ignore_index=True)
        stock_events["year"] = stock_events.date.str[:4]
        stage_features = f.drop(columns=["code", "year"])
        stage_tables.append(stock_events.join(stage_features, on="date"))
        squeeze_columns = context.columns[context.columns.str.endswith(":squeeze_start")]
        measurements = [c for c in ["tr_compression", "amount_5_20", "cmf20", "low_change_atr",
            *[f"{group}_turnover_pct20" for group in (*GROUPS, *DETAIL)]] if c in f]
        for column in squeeze_columns:
            anchors = np.flatnonzero((f[column].eq(True) & observed_days).to_numpy())
            anchors = anchors[(anchors >= 5) & (anchors + 5 < len(calendar))]
            anchors = anchors[d.close.notna().rolling(11).sum().iloc[anchors + 5].eq(11).to_numpy()]
            for offset in (-5, 0, 5):
                squeeze_tables.append(f.iloc[anchors + offset][measurements].assign(
                    model=column.split(":")[0], offset=offset, year=calendar[anchors].str[:4]))
        signal_dates = pd.Index([r[2] for r in keys]).unique()
        baseline_contexts.append(f.reindex(signal_dates).drop(columns=["year"]).rename_axis("date").reset_index())
        if number % 100 == 0:
            print(f"Processed {number} / {len(frames)} stocks", flush=True)
    pd.concat(ledgers, ignore_index=True).to_csv(out / "DAILY_FEATURES.csv.gz", index=False)
    baseline = baseline_ledgers()
    assert set(band_rows) == set(baseline[["model", "code", "date", "band"]].itertuples(index=False, name=None))
    archived_p1 = set(all_signals.loc[all_signals.stage.eq("P1"), ["code", "date"]].itertuples(index=False, name=None))
    assert wave_keys == archived_p1, (len(wave_keys - archived_p1), len(archived_p1 - wave_keys))
    daily = pd.concat(daily_tables, ignore_index=True)
    daily.to_csv(out / "DAILY_OUTCOMES.csv.gz", index=False)
    summaries(daily, sorted(flag_names)).to_csv(out / "DAILY_COMPARISON.csv", index=False)
    selected = daily[daily.DAILY.eq(True) & daily.observed60].copy()
    selected["month"] = selected.date.str[:7]
    selected = selected.drop_duplicates(["code", "month"])
    summaries(selected, ["DAILY", "QUIET", "QUIET_DRY", "QUIET_ACTIVE", "QUIET_POSITIVE_CMF",
                         "QUIET_NEGATIVE_CMF", "QUIET_RISING_LOWS"]).to_csv(
        out / "MONTHLY_FIRST_ELIGIBLE_SENSITIVITY.csv", index=False)
    stage = pd.concat(stage_tables, ignore_index=True)
    stage.to_csv(out / "WAVE_STAGE_CONTEXT.csv.gz", index=False)
    describe_states(stage, ["year", "stage"]).to_csv(out / "WAVE_STAGE_COMPARISON.csv", index=False)
    describe_states(pd.concat(squeeze_tables), ["model", "year", "offset"]).to_csv(
        out / "SQUEEZE_PHASE_COMPARISON.csv", index=False)
    compare_breakouts(pd.concat(baseline_contexts, ignore_index=True), baseline).to_csv(
        out / "ORIGINAL_SIGNAL_COMPARISON.csv", index=False)
    status = dict(mode="RESEARCH_ONLY", prices=500, daily_rows=len(daily),
                  eligible_days=int(daily.DAILY.eq(True).sum()), verified_original_breakouts=len(band_rows),
                  verified_p1=len(wave_keys), flow_stock_sources=len(investor),
                  known_flow20=int(daily.flow_status.eq("KNOWN20").sum()),
                  flow_timing="D-1 maximum and actual published_at at or before D0 20:30 KST",
                  validation_2025_2026="NOT_RUN", corporate_actions="UNVERIFIED",
                  github_run_id=os.environ.get("GITHUB_RUN_ID"))
    (out / "STATUS.json").write_text(json.dumps(status, indent=2), encoding="utf-8")
    print(json.dumps(status))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, default=Path(os.environ.get("BLUE20_MANIFEST",
        Path(os.environ.get("TEMP", "/tmp")) / "stock-hunter-blue20-full-replay/research_projects/blue20_multifactor_research/.auto500_work/r118_inputs/manifest.csv")))
    parser.add_argument("--flows", type=Path)
    parser.add_argument("--out", type=Path, default=HERE / "results")
    args = parser.parse_args()
    main(args.manifest, args.flows, args.out)
