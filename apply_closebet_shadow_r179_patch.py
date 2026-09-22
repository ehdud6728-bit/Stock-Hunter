#!/usr/bin/env python3
from pathlib import Path

WF = Path(".github/workflows/closing_bet_live_shadow_board_r175.yml")
OV = Path("closebet_live_shadow_source_capture_r177_overlay.py.txt")

def must_replace(text, old, new, label):
    if old not in text:
        raise SystemExit(f"[FAIL] expected block not found: {label}")
    return text.replace(old, new, 1)

def patch_workflow():
    s = WF.read_text(encoding="utf-8")

    s = must_replace(
        s,
        "name: Closing Bet v49.76 Live Shadow Board R1.7.7 Compact + Manual Test",
        "name: Closing Bet v49.76 Live Shadow Board R1.7.9 Outcome Probability + Human Briefing",
        "workflow name",
    )

    s = must_replace(
        s,
        """mkdir -p source_run source_r14 source_r16_a
          gh run download "${{ steps.source.outputs.run_id }}" -D source_run
          gh run download "35445479856" -D source_r14
          gh run download "35469509910" -D source_r16_a""",
        """mkdir -p source_run source_r13 source_r14 source_r16_a
          gh run download "${{ steps.source.outputs.run_id }}" -D source_run
          gh run download "35443265652" -D source_r13
          gh run download "35445479856" -D source_r14
          gh run download "35469509910" -D source_r16_a""",
        "frozen R1.3 reference download",
    )

    s = must_replace(
        s,
        "- name: Build and optionally send R1.7.7 SHADOW",
        "- name: Build and optionally send R1.7.9 SHADOW",
        "build step name",
    )

    s = must_replace(
        s,
        "python -u closing_bet_live_shadow_board_r177.py \\",
        "CLOSEBET_R13_ROOT=source_r13 python -u closing_bet_live_shadow_board_r179.py \\",
        "R1.7.9 executable",
    )

    old_validate = """          assert m["research_only"]
          assert not m.get("candidate_membership_changed",False)
          assert not m.get("candidate_order_changed",False)
          assert not m.get("score_rank_changed",False)
          assert not m.get("production_logic_changed",False)
          assert m["production_final_message_modified"] is False
          assert m["shadow_message_separate_from_production_final"] is True
          print("R177 PASS")"""
    new_validate = """          assert m["research_only"]
          assert not m.get("candidate_membership_changed",False)
          assert not m.get("candidate_order_changed",False)
          assert not m.get("score_rank_changed",False)
          assert not m.get("production_logic_changed",False)
          assert m["production_final_message_modified"] is False
          assert m["shadow_message_separate_from_production_final"] is True
          if m.get("status") not in ("NO_LIVE_SIDECAR","SKIP_NON_CANDIDATE_LANE"):
              assert m.get("version") == "CLOSEBET_LIVE_SHADOW_BOARD_R179_OUTCOME_PROBABILITY_20260922"
              assert m.get("outcome_probability_enabled") is True
              assert m.get("probability_source_r13") == "35443265652"
              assert m.get("probability_atr_used") is False
              assert m.get("probability_ma224_used") is False
              assert m.get("probability_candidate_order_changed") is False
              assert m.get("probability_score_rank_changed") is False
          print("R179 PASS")"""
    s = must_replace(s, old_validate, new_validate, "R1.7.9 validation")

    s = must_replace(
        s,
        "- name: Upload R1.7.7 artifact",
        "- name: Upload R1.7.9 artifact",
        "artifact step name",
    )
    s = must_replace(
        s,
        "name: closing-bet-live-shadow-board-r177-${{ github.run_id }}",
        "name: closing-bet-live-shadow-board-r179-${{ github.run_id }}",
        "artifact name",
    )

    WF.write_text(s, encoding="utf-8")
    print("[PASS] patched SHADOW workflow to R1.7.9")

def patch_source_capture():
    s = OV.read_text(encoding="utf-8")

    old_mode = """        # Normal exact LIVE authority remains strict.
        denied=any(x in lane for x in ("RESTORE_ONLY","INTRADAY_EXPIRED","AFTER_WINDOW"))
        live_candidate=(not denied) and any(
            x in lane for x in ("PREFINAL","PRE_FINAL","LIVE_SESSION_CONTINUOUS_PRIMARY","AFTER_FINAL","FINAL")
        )
        if live_candidate:
            return "LIVE_EXACT"

        # Explicit manual TEST only. Never grants production/LIVE authority.
        if manual_test:
            return "MANUAL_TEST""""
    new_mode = """        # Explicit manual TEST takes precedence over lane tokens.
        # It never grants production/LIVE authority, even when lane contains AFTER_FINAL/FINAL.
        if manual_test:
            return "MANUAL_TEST"

        # Normal exact LIVE authority remains strict.
        denied=any(x in lane for x in ("RESTORE_ONLY","INTRADAY_EXPIRED","AFTER_WINDOW"))
        live_candidate=(not denied) and any(
            x in lane for x in ("PREFINAL","PRE_FINAL","LIVE_SESSION_CONTINUOUS_PRIMARY","AFTER_FINAL","FINAL")
        )
        if live_candidate:
            return "LIVE_EXACT""""
    s = must_replace(s, old_mode, new_mode, "manual TEST_ONLY precedence")

    old_items = """        decision=dict(globals().get("_V4942_LAST_DECISION",{}) or {})
        items=list(decision.get("items",[]) or [])
        if not items:
            return

        # Mirror the already-existing visible FINAL/decision-board selection.
        top=[]
        quotas=(("ENTER",3),("MANAGE",2),("REPLAY",1),("WAIT",3),("EXCLUDE",1),("HOLD",1))
        for cat,limit in quotas:
            top.extend([x for x in items if str((x or {}).get("decision",""))==cat][:limit])
        try:
            top=sorted(
                top,
                key=lambda x:(int((x or {}).get("rank",9) or 9),-float((x or {}).get("score",0) or 0))
            )[:int(globals().get("CLOSING_BET_V4942_FINAL_TOP_N",6) or 6)]
        except Exception:
            top=top[:int(globals().get("CLOSING_BET_V4942_FINAL_TOP_N",6) or 6)]
        if not top:
            return

        hdf=hits_df.copy() if isinstance(hits_df,pd.DataFrame) else pd.DataFrame(hits_df or [])"""
    new_items = """        decision=dict(globals().get("_V4942_LAST_DECISION",{}) or {})
        items=list(decision.get("items",[]) or [])
        hdf=hits_df.copy() if isinstance(hits_df,pd.DataFrame) else pd.DataFrame(hits_df or [])

        # Prefer the already-existing visible FINAL/decision-board selection.
        # If that board is empty in an AFTER_FINAL/manual run, fall back to an exact,
        # order-preserving copy of already-computed production hits. No recomputation.
        top=[]
        if items:
            quotas=(("ENTER",3),("MANAGE",2),("REPLAY",1),("WAIT",3),("EXCLUDE",1),("HOLD",1))
            for cat,limit in quotas:
                top.extend([x for x in items if str((x or {}).get("decision",""))==cat][:limit])
            try:
                top=sorted(
                    top,
                    key=lambda x:(int((x or {}).get("rank",9) or 9),-float((x or {}).get("score",0) or 0))
                )[:int(globals().get("CLOSING_BET_V4942_FINAL_TOP_N",6) or 6)]
            except Exception:
                top=top[:int(globals().get("CLOSING_BET_V4942_FINAL_TOP_N",6) or 6)]
        elif not hdf.empty:
            top=hdf.to_dict("records")[:int(globals().get("CLOSING_BET_V4942_FINAL_TOP_N",6) or 6)]

        if not top:
            return"""
    s = must_replace(s, old_items, new_items, "copy-only hits fallback")

    s = s.replace(
        '_CLOSEBET_SHADOW_CAPTURE_VERSION = "CLOSEBET_LIVE_SHADOW_SOURCE_CAPTURE_R177_20260921"',
        '_CLOSEBET_SHADOW_CAPTURE_VERSION = "CLOSEBET_LIVE_SHADOW_SOURCE_CAPTURE_R177_FIX1_TEST_PRECEDENCE_COPY_FALLBACK_20260922"',
        1,
    )

    OV.write_text(s, encoding="utf-8")
    print("[PASS] patched research-only source capture helper")
    print("[INFO] production search/score/rank/order/Telegram logic unchanged")

def main():
    if not WF.exists():
        raise SystemExit(f"[FAIL] missing {WF}")
    if not OV.exists():
        raise SystemExit(f"[FAIL] missing {OV}")
    patch_workflow()
    patch_source_capture()
    print("[DONE] R1.7.9 SHADOW patch applied. Review git diff before commit.")

if __name__ == "__main__":
    main()
