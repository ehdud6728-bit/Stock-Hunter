"""Independent BLUE20 research with dated evidence and one frozen holdout."""

import hashlib
import json
import os
from pathlib import Path
import sys

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
OUT = HERE / "results"
BASE = ROOT / "BLUE20_EIGHT_VARIANTS"
EARLY = ROOT / "BLUE20_EARLY_WAVE"
REF = HERE / "reference"
BUNDLED = REF.is_dir()
if BUNDLED:
    sys.path.insert(0, str(HERE / "vendor"))
    from blue20_bands import bands, signal
else:
    sys.path[:0] = [str(BASE), str(BASE / "vendor"), str(EARLY)]
    from research import bands, signal
from loader import load_prices
from core import indicators

FIXED = "BB40_S18_SHIFT20:UPPER"
CUTOFF = "2024-12-31"
MIN_HISTORY = 8
HORIZONS = (5, 10, 20)
OHLC = ["open", "high", "low", "close"]


def lines_and_hits(d):
    values = {}
    for k, lag in ((2., 26), (1.8, 20)):
        for n in (20, 25, 30, 35, 40):
            model = f"BB{n}" if lag == 26 else f"BB{n}_S18_SHIFT20"
            current = bands(d.close, n, k, lag)
            current["CENTER"] = d.close.rolling(n).mean().shift(lag)
            values.update({f"{model}:{side}": line for side, line in current.items()})
    values["ENV20:CENTER"] = d.close.rolling(20).mean()
    lines = pd.DataFrame(values, index=d.index)
    hits = {key: np.flatnonzero(signal(d, line).to_numpy()) for key, line in values.items()}
    width = (lines[FIXED] - lines["BB40_S18_SHIFT20:LOWER"]) / lines["BB40_S18_SHIFT20:CENTER"]
    reference = width.shift().rolling(20).median()
    squeezed = width.le(.8 * reference)
    starts = squeezed & ~squeezed.shift(fill_value=False) & reference.shift().notna()
    return lines, hits, starts.to_numpy()


def next_in(indices, start, end):
    position = np.searchsorted(indices, start)
    if position < len(indices) and indices[position] <= end:
        return int(indices[position])
    return None


def wave_chain(x, p1, session):
    """Replay the pre-existing P2/P3 observer rules on the extended calendar."""
    op, hi, lo, cl, vol, amount, prior_h, prior_a = x
    p2 = p3 = None
    running_high, floor = hi[p1], np.inf
    status = "RIGHT_CENSORED"
    for j in range(p1 + 1, min(len(cl), p1 + 41)):
        age = j - p1
        if session[j] != session[p1] + age:
            return p2, p3, "MISSING_BARS_UNVERIFIED"
        if cl[j] < lo[p1]:
            return p2, p3, "P2_BASE_LOW_FAILED" if p2 is not None else "P1_BASE_LOW_FAILED"
        previous_low = floor
        running_high = max(running_high, hi[j])
        floor = min(floor, lo[j])
        if (p2 is None and age <= 20 and 1 - lo[j] / running_high >= .03
                and abs(cl[j] - op[j]) <= .5 * (cl[p1] - op[p1]) and vol[j] <= .6 * vol[p1]):
            p2 = j
        elif (p2 is not None and lo[j] >= previous_low and cl[j] > op[j]
              and amount[j] >= 1.2 * prior_a[j] and cl[j] > prior_h[j]):
            return p2, j, "P3_REACHED"
        if p2 is None and age == 20:
            return p2, p3, "NO_P2_20D"
        if age == 40:
            status = "NO_P3_40D"
    return p2, p3, status


def select_known(history, date, fallback=None):
    """Choose only from cases whose full assessment deadline preceded this decision."""
    ranked = []
    for name, records in history.items():
        sample = [success for mature, success in records if mature < date]
        if len(sample) >= MIN_HISTORY:
            # The small common prior damps differences between sparse lanes.
            ranked.append(((sum(sample) + 2) / (len(sample) + 4), len(sample), name))
    if not ranked:
        return fallback, 0
    score, count, name = sorted(ranked, key=lambda row: (-row[0], -row[1], row[2]))[0]
    return name, count


def trade(x, confirm, end, target, fixed_stop=None, dynamic_stop=None):
    if confirm is None or confirm >= end:
        return 0., "NO_ENTRY", None, None
    op, hi, lo, close = x[:4]
    entry = confirm + 1
    buy = op[entry]
    target_price = buy * (1 + target)
    stop = fixed_stop if fixed_stop is not None else .95 * buy
    stop_enabled = dynamic_stop is None
    pending = False
    for j in range(entry, end + 1):
        if pending:
            return 100 * (op[j] / buy - 1) - .25, "LINE_EXIT", entry, j
        if stop_enabled and op[j] <= stop:
            return 100 * (op[j] / buy - 1) - .25, "SL_GAP", entry, j
        if op[j] >= target_price:
            return 100 * (op[j] / buy - 1) - .25, "TP_GAP", entry, j
        if stop_enabled and lo[j] <= stop:
            return 100 * (stop / buy - 1) - .25, "SL", entry, j
        if hi[j] >= target_price:
            return 100 * target - .25, "TP", entry, j
        if dynamic_stop is not None and np.isfinite(dynamic_stop[j]) and close[j] < dynamic_stop[j]:
            pending = True
    return 100 * (close[end] / buy - 1) - .25, "TIME", entry, end


def source_data():
    manifest = Path(os.environ.get("BLUE20_MANIFEST", str(Path(os.environ.get("TEMP","/tmp")) /
        "stock-hunter-blue20-full-replay/research_projects/blue20_multifactor_research/.auto500_work/r118_inputs/manifest.csv")))
    frames = load_prices(manifest)
    base_ref = REF if BUNDLED else BASE / "reference"
    fixed = json.loads((base_ref / "PRICE_SOURCE_HASHES.json").read_text())
    assert set(frames) == set(fixed)
    for code, data in frames.items():
        assert hashlib.sha256(data.to_csv(index=False, lineterminator="\n").encode("utf-8-sig")).hexdigest() == fixed[code]
    oos = []
    calendar = {date for frame in frames.values() for date in frame.date}
    source_hashes = REF / "FROZEN_SHA256.json" if BUNDLED else ROOT / "BLUE20_PIT_MARKET/FROZEN_SHA256.json"
    frozen = json.loads(source_hashes.read_text())["inputs"]
    cols = "Date Code MarketId Open High Low Close Volume Amount".split()
    for year in (2025, 2026):
        name = f"marcap-{year}.parquet"
        path = HERE / "inputs" / name
        assert hashlib.sha256(path.read_bytes()).hexdigest() == frozen[name]
        source = pd.read_parquet(path, columns=cols)
        if "Date" not in source:
            source = source.reset_index()
        source = source[source.MarketId.isin(["STK", "KSQ"])].rename(columns=str.lower)
        source["date"] = pd.to_datetime(source.date).dt.strftime("%Y-%m-%d")
        source["code"] = source.code.astype(str).str.zfill(6)
        calendar.update(source.date.unique())
        source = source[source.code.isin(frames)]
        oos.append(source)
    post = pd.concat(oos, ignore_index=True)
    assert not post.duplicated(["code", "date"]).any()
    mapping = pd.read_csv(base_ref / "MARKET_MAPPING.csv.gz", dtype={"code": str},
                          usecols=["date", "code", "market_id"]).rename(columns={"market_id": "marketid"})
    assert not mapping.duplicated(["code", "date"]).any()
    by_code = {code: group for code, group in post.groupby("code")}
    markets = {code: group.drop(columns="code") for code, group in mapping.groupby("code")}
    for code, frame in frames.items():
        past = frame[frame.date.le(CUTOFF)].merge(
            markets.get(code, pd.DataFrame(columns=["date","marketid"])), on="date", how="left")
        # The base study's inputs retain their original data through 2024.
        later = by_code.get(code, pd.DataFrame(columns=past.columns))
        later = later[later[OHLC].gt(0).all(axis=1)]
        combined = pd.concat([past, later], ignore_index=True) if len(later) else past
        fields = [*OHLC, "volume", "amount"]
        combined[fields] = combined[fields].astype(float)
        frames[code] = combined.sort_values("date").reset_index(drop=True)
    return frames, pd.Index(sorted(calendar))


def stock_replay(d, calendar):
    date = d.date.to_numpy()
    year = np.array([int(day[:4]) for day in date])
    session = calendar.get_indexer(date)
    assert (session >= 0).all()
    origin = indicators(d)
    x = tuple(origin[field].to_numpy() for field in [
        *OHLC, "volume", "amount", "prior_high5", "prior_amount5"])
    atr = origin.prior_atr20.to_numpy()
    lines, hits, squeezed = lines_and_hits(d)
    names = list(lines)
    values = lines.to_numpy()
    families = {key: key.split(":")[0].split("_")[0] for key in names}
    waves = np.flatnonzero(origin.p1_signal.to_numpy())
    no_recent = origin.p1_raw.rolling(21).sum().eq(0).to_numpy()
    lead_history = {name: [] for name in names}
    support_history = {name: [] for name in names}
    break_rows, sequence_rows, support_rows, daily_rows, trades = [], [], [], [], []
    valid_bar = d[OHLC].gt(0).all(axis=1).to_numpy() & d.volume.gt(0).to_numpy()
    bad_prefix = np.r_[0, np.cumsum(~valid_bar)]
    # Daily observations use consecutive market sessions, including future outcome windows.
    def full(j, end):
        return (j >= 0 and end < len(date) and session[end] - session[j] == end - j
                and bad_prefix[end + 1] == bad_prefix[j])

    def split_full(j, end):
        return full(j, end) and (date[end] <= CUTOFF or date[j] > CUTOFF)

    for col, name in enumerate(names):
        previous_training = -100
        for j in hits[name]:
            if year[j] < 2023 or not split_full(j, j + 20):
                continue
            p1 = next_in(waves, j + 1, j + 20)
            successes = p1 is not None
            if session[j] - previous_training > 20:
                lead_history[name].append((date[j + 20], successes))
                previous_training = session[j]
            cluster = {families[key] for key, level in zip(names, values[j], strict=True)
                       if abs(level - values[j, col]) <= .5 * atr[j]}
            net, status, entry, end = trade(x, j, j + 20, .10)
            rise = x[1][j+1:j+21].max() / x[0][j+1] - 1
            break_rows.append((date[j], name, len(cluster), successes,
                               p1-j if successes else None, rise >= .05, rise >= .10,
                               bool((x[3][j+1:j+6] < values[j, col]).any()), net))
    chains = {}
    previous_support = {name: -100 for name in names}
    for p1 in waves:
        if year[p1] < 2023:
            continue
        p2, p3, status = wave_chain(x, p1, session)
        chains[p1] = p2, p3, status
        if p1 >= 20 and full(p1-20, p1):
            first = {name: next_in(indices, p1-20, p1-1) for name, indices in hits.items()}
            env = first["ENV20:CENTER"]
            for name, j in first.items():
                if name == "ENV20:CENTER":
                    continue
                relation = ("NEITHER" if env is None and j is None else "BB_ONLY" if env is None else
                            "ENV_ONLY" if j is None else "SAME_DAY" if env == j else
                            "ENV_FIRST" if env < j else "BB_FIRST")
                sequence_rows.append((date[p1], name, relation, None if j is None else date[j],
                                      None if env is None else date[env],
                                      None if env is None or j is None else session[j]-session[env]))
        if p2 is None or not split_full(p1, p1+40):
            continue
        cutoff = date[p1+40]
        terminal = p3 if p3 is not None else p1+40
        for col, name in enumerate(names):
            line = values[p2, col]
            if not np.isfinite(line) or not (abs(x[2][p2] - line) <= .5 * atr[p2] and x[3][p2] >= line):
                continue
            defended = bool(p3 is not None and (x[3][p2+1:terminal+1] >= values[p2+1:terminal+1,col]).all())
            if session[p1] - previous_support[name] > 40:
                support_history[name].append((cutoff, defended))
                previous_support[name] = session[p1]
            support_rows.append((date[p1], date[p2], name, defended, status))
    frozen_lead, n_lead = select_known(lead_history, "2025-01-01", FIXED)
    frozen_support, n_support = select_known(support_history, "2025-01-01")
    previous_anchor = -100
    for j in range(len(date)):
        if year[j] < 2023 or not no_recent[j] or not full(j-20,j) or not np.isfinite(values[j,names.index(FIXED)]):
            continue
        p1 = next_in(waves, j+1, j+20)
        use = []
        for h in HORIZONS:
            use.append(bool(p1 is not None and p1 <= j+h) if split_full(j,j+h) else None)
        daily_rows.append((date[j], d.marketid.iloc[j], d.prior_amount20.iloc[j], squeezed[j], *use))
        if not squeezed[j] or session[j]-previous_anchor <= 60:
            continue
        previous_anchor = session[j]
        if not split_full(j,j+60):
            trades.append((date[j], "COVERAGE", 0., "INCOMPLETE", None, None, None, None, False, False, False))
            continue
        lead, n = (select_known(lead_history, date[j], FIXED) if year[j] < 2025 else (frozen_lead,n_lead))
        p2,p3,status = chains.get(p1, (None,None,"NO_P1"))
        deadline = j+60
        fixed_hit = next_in(hits[FIXED], j, j+20)
        chosen_hit = next_in(hits[lead], j, j+20)
        envelope_hit = next_in(hits["ENV20:CENTER"], j, j+20)
        low_stop = x[2][p1+1:p3+1].min() if p3 is not None else None
        support, support_n = ((select_known(support_history, date[p2]) if year[j] < 2025 else
                               (frozen_support,n_support)) if p2 is not None else (None,0))
        # Require the selected line to have been touched and held on P2 before using it as a stop.
        dynamic = None
        if support is not None and p3 is not None:
            line = lines[support].to_numpy()
            if abs(x[2][p2] - line[p2]) <= .5*atr[p2] and x[3][p2] >= line[p2] and x[3][p3] >= line[p3]:
                dynamic = line
        fixed_dynamic = None
        if p3 is not None:
            line = lines[FIXED].to_numpy()
            if abs(x[2][p2] - line[p2]) <= .5*atr[p2] and x[3][p2] >= line[p2] and x[3][p3] >= line[p3]:
                fixed_dynamic = line
        base_max = x[1][j+1:deadline+1].max() / x[0][j+1] - 1
        for policy, confirm, stop, moving, selected in [
            ("A_SQUEEZE",j,None,None,None), ("B_P1",p1,None,None,None), ("C_P3",p3,None,None,None),
            ("FIXED_BREAK",fixed_hit,None,None,FIXED), ("ADAPTIVE_BREAK",chosen_hit,None,None,lead),
            ("ENV_BREAK",envelope_hit,None,None,"ENV20:CENTER"),
            ("P3_LOW_STOP",p3,low_stop,None,None), ("P3_FIXED_LINE_STOP",p3,None,fixed_dynamic,FIXED),
            ("P3_ADAPTIVE_LINE_STOP",p3,None,dynamic,support)
        ]:
            net, outcome, entry, exit_day = trade(x, confirm, deadline, .10, stop, moving)
            missed5 = base_max >= .05 and (entry is None or x[1][j+1:entry].max(initial=0)/x[0][j+1]-1 >= .05)
            missed10 = base_max >= .10 and (entry is None or x[1][j+1:entry].max(initial=0)/x[0][j+1]-1 >= .10)
            trades.append((date[j], policy, net, outcome, None if entry is None else date[entry],
                           None if exit_day is None else date[exit_day], selected,
                           n if policy == "ADAPTIVE_BREAK" else support_n if policy == "P3_ADAPTIVE_LINE_STOP" else None,
                           missed5, missed10, moving is not None))
    return (break_rows,sequence_rows,support_rows,daily_rows,trades,
            (frozen_lead,n_lead,frozen_support,n_support), chains)


def main():
    OUT.mkdir(exist_ok=True)
    frames, calendar = source_data()
    records = {name: [] for name in ("BREAKS", "LEADS", "SUPPORT", "DAILY", "POLICIES", "FROZEN_LINES")}
    schemas = {
        "BREAKS": "date line families_near wave20 days_to_wave high5 high10 slipped5 net",
        "LEADS": "date line relation line_date envelope_date env_lead_market_sessions",
        "SUPPORT": "p1_date date line held_to_p3 chain_status",
        "DAILY": "date market prior_amount squeeze wave5 wave10 wave20",
        "POLICIES": "date policy net status entry_date exit_date selected_line past_cases missed5 missed10 line_stop_active",
        "FROZEN_LINES": "lead_line lead_cases support_line support_cases",
    }
    archive = pd.read_csv((REF if BUNDLED else EARLY/"results")/"EPISODES.csv.gz",dtype={"code":str})
    archive = archive[archive.origin_stage.eq("P1")].set_index(["code","p1_date"])
    wave_keys = set()
    comparable_chains = 0
    for n,(code,d) in enumerate(frames.items(),1):
        d["prior_amount20"] = d.amount.rolling(20).mean().shift()
        *sets, frozen, chains = stock_replay(d,calendar)
        for name, rows in zip(schemas, [*sets,[frozen]],strict=True):
            records[name].extend((code,*row) for row in rows)
        for j,(p2,p3,status) in chains.items():
            day = d.date.iloc[j]
            if day > CUTOFF:
                continue
            wave_keys.add((code,day))
            if j+40 < len(d) and d.date.iloc[j+40] <= CUTOFF and (code,day) in archive.index:
                old = archive.loc[(code,day)]
                for index, column in [(p2,"p2_date"),(p3,"p3_date")]:
                    assert (pd.isna(old[column]) if index is None else d.date.iloc[index] == old[column]), (code,day,column)
                assert status == old.final_status, (code,day,status,old.final_status)
                comparable_chains += 1
        if n%100 == 0:
            print(f"Processed {n}/500",flush=True)
    assert wave_keys == set(archive.index), (len(wave_keys), len(archive))
    for name, fields in schemas.items():
        pd.DataFrame(records[name],columns=["code",*fields.split()]).to_csv(OUT/f"{name}.csv.gz",index=False)
    status = dict(mode="RESEARCH_ONLY",stocks=len(frames),calendar_last=calendar[-1],
                  exact_development_p1=len(wave_keys),exact_mature_p2_p3=comparable_chains,
                  selection_minimum=MIN_HISTORY,frozen_through=CUTOFF,corporate_actions="UNVERIFIED",
                  investor_flow="NOT_USED",github_run_id=os.environ.get("GITHUB_RUN_ID"))
    (OUT/"STATUS.json").write_text(json.dumps(status,indent=2),encoding="utf-8")
    print(json.dumps(status))


if __name__ == "__main__":
    main()
