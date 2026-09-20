#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, json, os, re, sys
from pathlib import Path
from typing import List, Tuple
import requests

# R1.7 deliberately reuses the validated R1.6.2 renderer.
import closing_bet_live_shadow_board_r162 as r162

VERSION="CLOSEBET_LIVE_SHADOW_BOARD_R17_TELEGRAM_INTEGRATION_20260920"
TELEGRAM_SAFE_LIMIT=3500
LIVE_SOURCE_PREFIXES=("live_shadow_execution_","live_shadow_hits_")
ALLOWED_LANE_TOKENS=(
    "PREFINAL",
    "PRE_FINAL",
    "LIVE_SESSION_CONTINUOUS_PRIMARY",
    "AFTER_FINAL",
    "FINAL",
)
DENY_LANE_TOKENS=("RESTORE_ONLY","INTRADAY_EXPIRED","AFTER_WINDOW")

def _arg_value(flag, default=""):
    try:
        i=sys.argv.index(flag)
        return sys.argv[i+1]
    except Exception:
        return default

def _read_json(p: Path):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _find_lane(source_root: Path) -> str:
    # Prefer explicit live-shadow source metadata when present.
    metas=sorted(source_root.rglob("live_shadow_source_meta_*.json"))
    for p in reversed(metas):
        d=_read_json(p)
        for k in ("effective_lane","lane","source_lane","run_lane"):
            v=str(d.get(k,"")).strip()
            if v:return v

    # Fall back to production schedule-slot authority.
    metas=sorted(source_root.rglob("schedule_slot_authority_*.json"))
    for p in reversed(metas):
        d=_read_json(p)
        for k in ("effective_lane","lane","source_lane"):
            v=str(d.get(k,"")).strip()
            if v:return v
    return ""

def _source_basename(meta: dict) -> str:
    s=str(meta.get("source","")).strip()
    return Path(s).name if s else ""

def _is_exact_live_source(meta: dict) -> bool:
    name=_source_basename(meta)
    return any(name.startswith(p) for p in LIVE_SOURCE_PREFIXES)

def _lane_is_candidate_generating(lane: str) -> bool:
    u=(lane or "").upper()
    if any(t in u for t in DENY_LANE_TOKENS):
        return False
    return any(t in u for t in ALLOWED_LANE_TOKENS)

def _split_header_blocks(text: str) -> Tuple[str,List[str]]:
    chunks=[x.strip() for x in text.strip().split("\n\n") if x.strip()]
    if not chunks:return "",[]
    header=chunks[0]
    blocks=chunks[1:]
    return header,blocks

def _build_telegram_parts(text: str, safe_limit: int=TELEGRAM_SAFE_LIMIT) -> List[str]:
    """
    Preserve each stock block intact.
    Never split a stock block across Telegram messages.
    """
    header,blocks=_split_header_blocks(text)
    if not header:
        return []
    prefix="🧪 SHADOW · RESEARCH ONLY\n"
    fixed=prefix+header

    for b in blocks:
        if len(prefix)+len(b)+20 > safe_limit:
            raise ValueError(f"SINGLE_BLOCK_TOO_LONG:{len(b)}")

    parts=[]
    cur=fixed
    for b in blocks:
        add="\n\n"+b
        if len(cur)+len(add) <= safe_limit:
            cur+=add
        else:
            parts.append(cur)
            cur=prefix+b
    if cur.strip():
        parts.append(cur)

    total=len(parts)
    if total>1:
        labeled=[]
        for i,p in enumerate(parts,1):
            lines=p.splitlines()
            if lines and lines[0].startswith("🧪 SHADOW · RESEARCH ONLY"):
                lines[0]=f"🧪 SHADOW · RESEARCH ONLY [{i}/{total}]"
            else:
                lines.insert(0,f"🧪 SHADOW · RESEARCH ONLY [{i}/{total}]")
            labeled.append("\n".join(lines))
        parts=labeled
    return parts

def _send_parts(parts: List[str]) -> dict:
    token=(os.environ.get("CLOSING_BET_TOKEN") or "").strip()
    chat=(os.environ.get("CLOSING_BET_CHAT_ID") or "").strip()
    if not token or not chat:
        return {"status":"SKIPPED_NO_ROUTE","parts":len(parts),"success":0,"errors":[]}

    ok=0; errors=[]
    url=f"https://api.telegram.org/bot{token}/sendMessage"
    for i,p in enumerate(parts,1):
        try:
            rr=requests.post(url,data={
                "chat_id":chat,
                "text":p,
                "disable_web_page_preview":"true",
            },timeout=20)
            if rr.ok:
                ok+=1
            else:
                errors.append({"part":i,"status_code":rr.status_code,"body":rr.text[:200]})
        except Exception as e:
            errors.append({"part":i,"error":type(e).__name__})
    return {
        "status":"DELIVERED" if parts and ok==len(parts) else "PARTIAL_OR_FAILED",
        "parts":len(parts),"success":ok,"errors":errors
    }

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--source-root",default="source_run")
    ap.add_argument("--r14-root",default="source_r14")
    ap.add_argument("--r16-a-root",default="source_r16_a")
    ap.add_argument("--output-dir",default="reports/live_shadow_board_r17")
    ap.add_argument("--top-n",type=int,default=10)
    ap.add_argument("--source-run-id",default="")
    ap.add_argument("--r14-run-id",default="")
    ap.add_argument("--r16-a-run-id",default="")
    ap.add_argument("--automatic",action="store_true")
    ap.add_argument("--send-telegram",action="store_true")
    ap.add_argument("--allow-historical-test-send",action="store_true")
    args=ap.parse_args()

    # Build the validated R1.6.2 board first, with Telegram disabled in the base renderer.
    saved_argv=sys.argv[:]
    sys.argv=[
        "closing_bet_live_shadow_board_r162.py",
        "--source-root",args.source_root,
        "--r14-root",args.r14_root,
        "--r16-a-root",args.r16_a_root,
        "--output-dir",args.output_dir,
        "--top-n",str(args.top_n),
        "--source-run-id",args.source_run_id,
        "--r14-run-id",args.r14_run_id,
        "--r16-a-run-id",args.r16_a_run_id,
    ]
    try:
        r162.main()
    finally:
        sys.argv=saved_argv

    out=Path(args.output_dir)
    meta_path=out/"meta.json"
    board_path=out/"shadow_board.txt"
    if not meta_path.exists():
        raise SystemExit("R162_META_MISSING")

    meta=_read_json(meta_path)
    lane=_find_lane(Path(args.source_root))
    exact_live=_is_exact_live_source(meta)
    candidate_lane=_lane_is_candidate_generating(lane)

    # Delivery policy.
    reason=""
    eligible=False
    historical_test=False

    if meta.get("status")!="PASS":
        reason=f"BOARD_STATUS_{meta.get('status','UNKNOWN')}"
    elif args.automatic:
        # Automatic delivery is strict: exact live sidecar + candidate-generating lane.
        if not exact_live:
            reason="AUTO_BLOCK_NON_LIVE_SIDECAR"
        elif lane and not candidate_lane:
            reason="AUTO_BLOCK_NON_CANDIDATE_LANE"
        elif not lane:
            # Exact copy-only live sidecar is accepted when lane metadata is absent,
            # but this is explicitly recorded.
            eligible=True
            reason="AUTO_ELIGIBLE_EXACT_LIVE_SIDECAR_LANE_METADATA_MISSING"
        else:
            eligible=True
            reason="AUTO_ELIGIBLE_EXACT_LIVE_SIDECAR_CANDIDATE_LANE"
    elif args.send_telegram:
        if exact_live:
            eligible=True
            reason="MANUAL_ELIGIBLE_EXACT_LIVE_SIDECAR"
        elif args.allow_historical_test_send:
            eligible=True
            historical_test=True
            reason="MANUAL_HISTORICAL_FORMAT_TEST_EXPLICITLY_ALLOWED"
        else:
            reason="MANUAL_BLOCK_NON_LIVE_SIDECAR"
    else:
        reason="NOT_REQUESTED"

    delivery={"status":"NOT_REQUESTED","parts":0,"success":0,"errors":[]}
    parts=[]
    if eligible:
        text=board_path.read_text(encoding="utf-8")
        if historical_test:
            text=("⚠️ HISTORICAL REPLAY FORMAT TEST · NOT LIVE\n"
                  "실전 후보 알림이 아니라 텔레그램 형식 검증용입니다.\n\n"+text)
        parts=_build_telegram_parts(text)
        # Persist exact payloads for audit before attempting network delivery.
        payload_dir=out/"telegram_payloads"
        payload_dir.mkdir(exist_ok=True)
        for i,p in enumerate(parts,1):
            (payload_dir/f"part_{i:02d}.txt").write_text(p,encoding="utf-8")
        delivery=_send_parts(parts)

    delivery.update({
        "policy_reason":reason,
        "automatic":bool(args.automatic),
        "send_requested":bool(args.send_telegram or args.automatic),
        "allow_historical_test_send":bool(args.allow_historical_test_send),
        "historical_test":historical_test,
        "exact_live_sidecar":exact_live,
        "source_basename":_source_basename(meta),
        "source_lane":lane,
        "candidate_generating_lane":candidate_lane,
        "safe_limit_chars":TELEGRAM_SAFE_LIMIT,
        "stock_block_split_forbidden":True,
        "production_final_message_modified":False,
    })
    (out/"telegram_delivery.json").write_text(json.dumps(delivery,ensure_ascii=False,indent=2),encoding="utf-8")

    meta.update({
        "version":VERSION,
        "telegram_integration":True,
        "telegram_auto_policy":"EXACT_LIVE_SIDECAR_AND_CANDIDATE_LANE",
        "telegram_delivery_reason":reason,
        "telegram_delivery_status":delivery.get("status"),
        "telegram_parts":delivery.get("parts",0),
        "telegram_success_parts":delivery.get("success",0),
        "telegram_safe_limit_chars":TELEGRAM_SAFE_LIMIT,
        "telegram_preserves_stock_blocks":True,
        "telegram_historical_fallback_auto_send":False,
        "telegram_restore_only_auto_send":False,
        "telegram_intraday_expired_auto_send":False,
        "production_final_message_modified":False,
        "shadow_message_separate_from_production_final":True,
        "telegram_failure_affects_production":False,
    })
    meta_path.write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")

    print(json.dumps(delivery,ensure_ascii=False,indent=2))
    print(json.dumps(meta,ensure_ascii=False,indent=2))

if __name__=="__main__":
    main()
