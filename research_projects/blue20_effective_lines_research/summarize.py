"""Tabulate common opportunities and contemporaneous control groups."""

from pathlib import Path
import numpy as np
import pandas as pd

OUT = Path(__file__).resolve().parent / "results"


def load(name):
    return pd.read_csv(OUT / f"{name}.csv.gz", dtype={"code": str}, low_memory=False)


def add_year(frame, field="date"):
    return frame.assign(year=frame[field].str[:4].astype(int))


def summarize():
    daily = add_year(load("DAILY"))
    daily = daily[daily.market.isin(["STK", "KSQ"]) & daily.prior_amount.gt(0)].copy()
    percentiles = daily.groupby(["date","market"]).prior_amount.rank(method="average", pct=True)
    daily["liquidity"] = np.ceil(3*percentiles).astype(int)
    match = []
    for h in (5,10,20):
        field = f"wave{h}"
        known = daily[daily[field].notna()].copy()
        known[field] = known[field].astype(bool)
        cells = known.groupby(["year","date","market","liquidity","squeeze"])[field].agg(["size","mean"]).unstack("squeeze")
        cells = cells.dropna()
        for year, rows in cells.groupby(level="year"):
            count = rows["size"][True]
            rate = np.average(rows["mean"][True],weights=count)
            control = np.average(rows["mean"][False],weights=count)
            total = int(known.loc[known.year.eq(year),"squeeze"].sum())
            match.append(dict(year=year,horizon=h,matched_squeeze=int(count.sum()),
                              total_known_squeeze=total,controls=int(rows["size"][False].sum()),
                              squeeze_pct=100*rate,matched_control_pct=100*control,delta_pp=100*(rate-control)))
    pd.DataFrame(match).to_csv(OUT/"MATCHED_SQUEEZE.csv",index=False)

    trades = add_year(load("POLICIES"))
    trades[trades.policy.eq("COVERAGE")].groupby("year").size().rename("incomplete").to_csv(OUT/"INCOMPLETE.csv")
    trades = trades[trades.policy.ne("COVERAGE")].copy()
    baseline = trades[trades.policy.eq("A_SQUEEZE")][["code","date","net"]].rename(columns={"net":"baseline"})
    trades = trades.merge(baseline,on=["code","date"],validate="many_to_one")
    fixed = trades[trades.policy.eq("FIXED_BREAK")][["code","date","net"]].rename(columns={"net":"fixed"})
    trades = trades.merge(fixed,on=["code","date"],validate="many_to_one")
    entered = trades.entry_date.notna()
    trades = trades.assign(entered=entered, trade_net=trades.net.where(entered),
                           skipped_loss=~entered & trades.baseline.lt(0),
                           less_loss=entered & trades.baseline.lt(0) & trades.net.gt(trades.baseline),
                           stop=trades.status.str.startswith(("SL","LINE_EXIT")),
                           vs_fixed=trades.net-trades.fixed)
    summary = trades.groupby(["year","policy"]).agg(
        opportunities=("net","size"), entries=("entered","sum"), opportunity_net=("net","mean"),
        mean_trade=("trade_net","mean"), stops=("stop","sum"), skipped_losses=("skipped_loss","sum"),
        mitigated_losses=("less_loss","sum"), missed5=("missed5","sum"), missed10=("missed10","sum"),
        active_line_stops=("line_stop_active","sum"), mean_delta_fixed=("vs_fixed","mean"))
    summary.to_csv(OUT/"POLICY_COMPARISON.csv")
    subset = trades[trades.past_cases.ge(8) & trades.policy.eq("ADAPTIVE_BREAK")][["code","date"]]
    paired = trades[trades.policy.isin(["ADAPTIVE_BREAK","FIXED_BREAK","ENV_BREAK"])].merge(subset,on=["code","date"])
    paired.groupby(["year","policy"]).agg(opportunities=("net","size"), entries=("entered","sum"),
                                         opportunity_net=("net","mean"), missed10=("missed10","sum")).to_csv(
                                             OUT/"ADAPTIVE_KNOWN_ONLY.csv")
    adaptive = trades[trades.policy.eq("ADAPTIVE_BREAK")].copy()
    adaptive = adaptive[adaptive.selected_line.ne("BB40_S18_SHIFT20:UPPER") & adaptive.past_cases.ge(8)]
    adaptive.sort_values(["year","vs_fixed","code","date"]).groupby("year").head(4).to_csv(
        OUT/"ADAPTIVE_NEGATIVE_EXAMPLES.csv",index=False)
    adaptive.sort_values(["year","vs_fixed","code","date"],ascending=[True,False,True,True]).groupby("year").head(4).to_csv(
        OUT/"ADAPTIVE_POSITIVE_EXAMPLES.csv",index=False)

    breaks = add_year(load("BREAKS"))
    breaks["overlap"] = np.select([breaks.families_near.eq(1),breaks.families_near.ge(2)],
                                  ["SINGLE","MULTIPLE"],default="UNKNOWN")
    group = breaks.groupby(["year","line","overlap"])
    group.agg(signals=("wave20","size"), wave_rate=("wave20","mean"), lead_days=("days_to_wave","mean"),
              hit5=("high5","mean"), hit10=("high10","mean"), slipped5=("slipped5","mean"),
              mean_net=("net","mean")).to_csv(OUT/"BREAKOUT_CLUSTER.csv")
    breaks.groupby(["code","year","line"]).agg(signals=("wave20","size"),wave_rate=("wave20","mean"),
                                               lead_days=("days_to_wave","mean"),
                                               net=("net","mean")).to_csv(OUT/"STOCK_BREAKOUTS.csv")
    leads = add_year(load("LEADS"))
    leads.groupby(["year","line","relation"]).agg(
        waves=("code","size"), median_env_lead=("env_lead_market_sessions","median")).to_csv(OUT/"LEAD_ORDER.csv")
    leads.groupby(["code","year","line","relation"]).size().rename("waves").to_csv(OUT/"STOCK_LEAD_ORDER.csv")
    support = add_year(load("SUPPORT"))
    support.groupby(["code","year","line"]).held_to_p3.agg(["size","mean"]).to_csv(OUT/"STOCK_SUPPORT.csv")
    support.groupby(["year","line"]).held_to_p3.agg(["size","mean"]).to_csv(OUT/"SUPPORT_SUMMARY.csv")
    print(summary.round(3).to_string())


if __name__ == "__main__":
    summarize()
