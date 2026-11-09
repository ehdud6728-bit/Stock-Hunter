"""Additional daily and paired-stage analyses on the output of study.py."""

from pathlib import Path

import pandas as pd

from study import BASE, BUNDLED, HERE, HORIZONS, REFERENCE, summaries


def consecutive_candles_and_stages(out: Path):
    keys = ["code", "date"]
    features = pd.read_csv(out / "DAILY_FEATURES.csv.gz", usecols=[*keys, "body_atr"],
                           dtype={"code": str})
    calendar = pd.Index(sorted(features.date.unique()))
    session = pd.Series(range(len(calendar)), index=calendar)
    features = features.sort_values(keys)
    features["session"] = features.date.map(session)
    by_code = features.groupby("code", sort=False)
    small = features.body_atr.le(.35).where(features.body_atr.notna())
    previous = small.groupby(features.code, sort=False)
    has_three_days = by_code.session.diff(2).eq(2)
    complete = has_three_days & small.notna() & previous.shift(1).notna() & previous.shift(2).notna()
    features["THREE_CONSECUTIVE_SMALL_BODIES"] = (
        small.eq(True) & previous.shift(1).eq(True) & previous.shift(2).eq(True)).where(complete)
    comparison_flags = ["DAILY", "QUIET", "QUIET_DRY", "QUIET_ACTIVE", "QUIET_POSITIVE_CMF",
                        "QUIET_NEGATIVE_CMF", "QUIET_RISING_LOWS"]
    horizon_fields = [f"{field}{n}" for n in HORIZONS for field in (
        "observed", "wave", "days_to_wave", "no_wave_path", "net", "exit", "wait_p1_net20_within")]
    daily = pd.read_csv(out / "DAILY_OUTCOMES.csv.gz", dtype={"code": str}, low_memory=False,
                        usecols=[*keys, "year", *comparison_flags, *horizon_fields,
                                 "first_p1_date60", "first_p1_final_status", "first_p1_known_date"])
    flag = "THREE_CONSECUTIVE_SMALL_BODIES"
    daily = daily.merge(features[[*keys, flag]], on=keys, validate="one_to_one")
    daily[flag] = daily[flag].where(daily.DAILY.eq(True))
    summaries(daily, [flag]).to_csv(out / "CONSECUTIVE_CANDLES_COMPARISON.csv", index=False)
    examples = []
    quiet = daily[daily.QUIET.eq(True) & daily.observed60].sort_values(["date", "code"])
    for name, mask in {
        "WAVE_AND_EARLY_TARGET": quiet.wave20.eq(True) & quiet.exit20.eq("TP"),
        "P1_LOW_FAILED": quiet.first_p1_final_status.eq("P1_BASE_LOW_FAILED"),
        "PULLBACK_FAILED": quiet.first_p1_final_status.eq("P2_BASE_LOW_FAILED"),
        "SIDEWAYS_WITHOUT_P1": quiet.no_wave_path60.eq("SIDEWAYS"),
        "DOWN_WITHOUT_P1": quiet.no_wave_path60.eq("DOWN"),
        "UP_WITHOUT_P1": quiet.no_wave_path60.eq("UP"),
    }.items():
        columns = ["code", "date", "year", "days_to_wave60", "first_p1_date60",
                   "first_p1_final_status", "first_p1_known_date", "exit20", "net20", "net60"]
        examples.append(quiet.loc[mask, columns].drop_duplicates(["year", "code"])
                        .groupby("year", sort=False).head(2).assign(example=name))
    pd.concat(examples, ignore_index=True).to_csv(out / "CANDIDATE_EXAMPLES.csv", index=False)
    issue_ref = REFERENCE if BUNDLED else BASE / "reference"
    issues = pd.read_csv(issue_ref / "KNOWN_ISSUES.csv", dtype={"code": str})
    # Whole-stock exclusion is intentionally conservative and only a sensitivity check.
    clean = daily[~daily.code.isin(issues.code)]
    summaries(clean, [*comparison_flags, flag]).to_csv(out / "KNOWN_ISSUE_STOCK_EXCLUDED.csv", index=False)
    stage = pd.read_csv(out / "WAVE_STAGE_CONTEXT.csv.gz", dtype={"code": str}, low_memory=False)
    fields = ["cmf20", "amount_5_20", "tr_compression", "low_change_atr",
              *[column for column in stage if column.endswith("_turnover_pct20")]]
    stages = stage[stage.stage.isin(["P1_PREVIOUS_SESSION", "P1", "P2", "P3"])]
    reference = stages[stages.stage.eq("P1_PREVIOUS_SESSION")][["code", "p1_date", *fields]]
    paired = stages[stages.stage.ne("P1_PREVIOUS_SESSION")].merge(reference, on=["code", "p1_date"],
                 suffixes=("", "_pre"), validate="many_to_one")
    rows = []
    for (year, phase), group in paired.groupby(["year", "stage"]):
        for field in fields:
            delta = (group[field] - group[f"{field}_pre"]).dropna()
            rows.append(dict(year=year, stage=phase, metric=field, rows=len(group),
                             known_pairs=len(delta), mean_delta=delta.mean(), median_delta=delta.median()))
    pd.DataFrame(rows).to_csv(out / "WAVE_STAGE_PAIRED_CHANGE.csv", index=False)


if __name__ == "__main__":
    consecutive_candles_and_stages(HERE / "results")
