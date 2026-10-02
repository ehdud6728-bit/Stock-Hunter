from pathlib import Path

TEMP = Path(".github/workflows/TEMP_RESEARCH_RUNNER.yml")
RUN = Path(".github/workflows/run_scanner.yml")
FORENSIC = Path("v25_core224_universe_forensic.py")

EXPECTED_TEMP_BLOB = "bcf51b0bc288f1a849fdc2a49d4861f7b9c4a1a9"
EXPECTED_RUN_BLOB = "dbb443cac416f86d5d5ee637d9256df3bde55f8f"

FORENSIC_CONTENT = Path(__file__).with_name("v25_core224_universe_forensic.py").read_text(encoding="utf-8")


def replace_once(text: str, old: str, new: str, label: str) -> str:
    n = text.count(old)
    if n != 1:
        raise SystemExit(f"{label}: expected exactly one target block, found {n}")
    return text.replace(old, new, 1)


def patch_temp(text: str) -> str:
    old = r'''          for _,r in ev.iterrows():
              sd=pd.Timestamp(r.signal_date).normalize(); code=norm_code(r.code)
              if sd not in date_pos: continue
              g=by_code.get(code)
              if g is None: continue
              entry=float(r.snapshot_price)
              pos=date_pos[sd]

              rec=r.to_dict()
              rec["is_spac"]=is_spac(r.get("name",""))
              rec["price_data_last_date"]=last_date.date().isoformat()

              # Audit source snapshot vs official market close if available.
              if sd in g.index:
'''
    new = r'''          for _,r in ev.iterrows():
              sd=pd.Timestamp(r.signal_date).normalize(); code=norm_code(r.code)
              entry=float(r.snapshot_price)

              # Preserve frozen REAL_FULL cohort membership even when the external
              # price parquet has not caught up to the signal date yet.
              rec=r.to_dict()
              rec["is_spac"]=is_spac(r.get("name",""))
              rec["price_data_last_date"]=last_date.date().isoformat()
              rec["price_calendar_has_signal_date"]=bool(sd in date_pos)

              g=by_code.get(code)
              if sd not in date_pos or g is None:
                  rec["signal_market_close"]=np.nan
                  rec["snapshot_vs_market_close_pct"]=np.nan
                  rec["discontinuity45_flag"]=False
                  for h in [1,3,5,10]:
                      rec[f"d{h}_calendar_mature"]=False
                      rec[f"d{h}_date"]=""
                      rec[f"d{h}_close_ret_pct"]=np.nan
                      rec[f"mfe{h}_pct"]=np.nan
                      rec[f"mae{h}_pct"]=np.nan
                  outrows.append(rec)
                  continue

              pos=date_pos[sd]

              # Audit source snapshot vs official market close if available.
              if sd in g.index:
'''
    text = replace_once(text, old, new, "TEMP cohort preservation")

    old = r'''              "raw_rows":len(led),
              "signal_dates":sorted({str(pd.Timestamp(x).date()) for x in led.signal_date}),
'''
    new = r'''              "raw_rows":len(led),
              "source_rows_after_dedup":len(ev),
              "cohort_rows_preserved":bool(len(led)==len(ev)),
              "pending_price_rows":int((~led["price_calendar_has_signal_date"].astype(bool)).sum()),
              "signal_dates":sorted({str(pd.Timestamp(x).date()) for x in led.signal_date}),
'''
    text = replace_once(text, old, new, "TEMP meta guard")

    old = r'''          assert len(q)>0
          assert {"research_level","signal_date","snapshot_price","d1_close_ret_pct","mfe5_pct","mae5_pct"}.issubset(q.columns)
'''
    new = r'''          assert len(q)>0
          assert {"research_level","signal_date","snapshot_price","d1_close_ret_pct","mfe5_pct","mae5_pct","price_calendar_has_signal_date"}.issubset(q.columns)
          assert int(m.get("source_rows_after_dedup",-1))==len(q), ("COHORT_ROW_DROP_DETECTED",m.get("source_rows_after_dedup"),len(q))
          assert m.get("cohort_rows_preserved") is True
'''
    text = replace_once(text, old, new, "TEMP invariant")
    return text


def patch_run(text: str) -> str:
    old = r'''          echo "V25_CURRENT_LIVE_SEED_LISTING_AUTHORITY STOCKHUNTER_PYKRX_FORCE_STUB=0 scope=seed_refresh_subprocess_only"
          env \
            TEST_PROFILE=WEEKLY_BACKTEST \
            STOCKHUNTER_WEEKLY_BACKTEST_ONLY=1 \
            STOCKHUNTER_CORE224_LIVE_ONLY=0 \
            STOCKHUNTER_PYKRX_FORCE_STUB=0 \
            V25_CORE224_LIVE_ENABLE=0 \
            V1081_BACKTEST_SOURCE=DIRECT_REPLAY \
            V1081_DIRECT_DATE_MODE=WEEK_LAST \
            V25_CURRENT_LIVE_END_DATE="$current_live_end" \
            V25_TWO_YEAR_END_DATE="$current_live_end" \
            V1080_BACKTEST_OUTPUT_DIR=reports/v25_live_seed_build \
            V1080_BACKTEST_HOLD_DAYS=0 \
            V1080_BACKTEST_WEEKS=1 \
            V1081_DIRECT_MAX_DATES=1 \
            V23_SHARD_COUNT=1 \
            V23_SHARD_INDEX=0 \
            V23_SHARD_WORKER_ONLY=1 \
            V23_MERGE_ONLY_PARENT=0 \
            V23_ZERO_RECOMPUTE=1 \
            V23_MATERIALIZED_DIR=reports/v25_live_seed_build/v23_materialized \
            V25_COHORT_MODE=D \
            python -u "$SCRIPT"

          echo "V25_CURRENT_LIVE_SEED_LOADER_PREFLIGHT_BEGIN"
'''
    new = r'''          echo "V25_CURRENT_LIVE_SEED_LISTING_AUTHORITY STOCKHUNTER_PYKRX_FORCE_STUB=0 scope=seed_refresh_subprocess_only"
          seed_refresh_log="reports/v25_live_seed_build/core224_seed_refresh_stdout.log"
          set +e
          env \
            TEST_PROFILE=WEEKLY_BACKTEST \
            STOCKHUNTER_WEEKLY_BACKTEST_ONLY=1 \
            STOCKHUNTER_CORE224_LIVE_ONLY=0 \
            STOCKHUNTER_PYKRX_FORCE_STUB=0 \
            V25_CORE224_LIVE_ENABLE=0 \
            V1081_BACKTEST_SOURCE=DIRECT_REPLAY \
            V1081_DIRECT_DATE_MODE=WEEK_LAST \
            V25_CURRENT_LIVE_END_DATE="$current_live_end" \
            V25_TWO_YEAR_END_DATE="$current_live_end" \
            V1080_BACKTEST_OUTPUT_DIR=reports/v25_live_seed_build \
            V1080_BACKTEST_HOLD_DAYS=0 \
            V1080_BACKTEST_WEEKS=1 \
            V1081_DIRECT_MAX_DATES=1 \
            V23_SHARD_COUNT=1 \
            V23_SHARD_INDEX=0 \
            V23_SHARD_WORKER_ONLY=1 \
            V23_MERGE_ONLY_PARENT=0 \
            V23_ZERO_RECOMPUTE=1 \
            V23_MATERIALIZED_DIR=reports/v25_live_seed_build/v23_materialized \
            V25_COHORT_MODE=D \
            python -u "$SCRIPT" 2>&1 | tee "$seed_refresh_log"
          seed_refresh_rc=${PIPESTATUS[0]}
          set -e

          if [ "$seed_refresh_rc" -ne 0 ]; then
            if grep -Fq "NO_DIRECT_UNIVERSE" "$seed_refresh_log"; then
              echo "🔬 CORE224 NO_DIRECT_UNIVERSE detected · running isolated forensic diagnostic"
              if [ -f v25_core224_universe_forensic.py ]; then
                env \
                  STOCKHUNTER_PYKRX_FORCE_STUB=0 \
                  V1081_BACKTEST_SOURCE=DIRECT_REPLAY \
                  V1081_DIRECT_DATE_MODE=WEEK_LAST \
                  V1081_DIRECT_MAX_DATES=1 \
                  python -u v25_core224_universe_forensic.py \
                    --asof "$current_live_end" \
                    --output-dir reports/v25_live_seed_build \
                    --report-dir reports/v25_live_seed_build/core224_universe_forensic || true
              else
                echo "⚠️ CORE224 forensic script missing: v25_core224_universe_forensic.py"
              fi
            fi
            echo "❌ V25_CURRENT_LIVE_SEED_REFRESH failed rc=$seed_refresh_rc; restoring previous seed"
            restore_seed_backup
            rm -rf "$backup_dir"
            trap - ERR
            exit "$seed_refresh_rc"
          fi

          echo "V25_CURRENT_LIVE_SEED_LOADER_PREFLIGHT_BEGIN"
'''
    return replace_once(text, old, new, "run_scanner CORE224 forensic")


def main():
    if not TEMP.exists() or not RUN.exists():
        raise SystemExit("Run this script from the Stock-Hunter repository root.")

    temp = patch_temp(TEMP.read_text(encoding="utf-8"))
    run = patch_run(RUN.read_text(encoding="utf-8"))

    TEMP.write_text(temp, encoding="utf-8")
    RUN.write_text(run, encoding="utf-8")
    FORENSIC.write_text(FORENSIC_CONTENT, encoding="utf-8")

    print("PATCH_APPLIED")
    print("TEMP:", TEMP)
    print("RUN :", RUN)
    print("NEW :", FORENSIC)
    print("Research-only invariants preserved; production search/score/rank/order logic not modified.")


if __name__ == "__main__":
    main()
