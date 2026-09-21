#!/usr/bin/env python3
from __future__ import annotations
import json, sys
from pathlib import Path
import closing_bet_live_shadow_board_r17 as r17
import closing_bet_live_shadow_board_r175 as r175

VERSION="CLOSEBET_LIVE_SHADOW_BOARD_R177_MANUAL_TEST_CAPTURE_20260921"
_TEST_MODE=False
_orig_find_lane=r17._find_lane
_orig_lane_candidate=r17._lane_is_candidate_generating
_orig_compact=r175.compact

def _read_json(p):
    try:return json.loads(Path(p).read_text(encoding="utf-8"))
    except Exception:return {}

def _latest_source_meta(root):
    xs=sorted(Path(root).rglob("live_shadow_source_meta_*.json"))
    for p in reversed(xs):
        d=_read_json(p)
        if d:return d
    return {}

def _source_root_from_argv():
    try:
        i=sys.argv.index("--source-root")
        return sys.argv[i+1]
    except Exception:
        return "source_run"

def _patched_find_lane(root):
    d=_latest_source_meta(root)
    if bool(d.get("test_only")) and bool(d.get("manual_test_authorized")):
        return "MANUAL_TEST_CAPTURE"
    return _orig_find_lane(root)

def _patched_lane_candidate(lane):
    if str(lane or "").upper()=="MANUAL_TEST_CAPTURE":
        return True
    return _orig_lane_candidate(lane)

def _patched_compact(full):
    txt=_orig_compact(full)
    if _TEST_MODE:
        marker=("⚠️ MANUAL SHADOW TEST · NOT LIVE · 실전 authority/성과표본 제외\n"
                "※ 20시 이후 또는 restore-only에서도 형식/전송 검증을 위해 명시적으로 생성된 TEST_ONLY 캡처입니다.\n")
        if marker.strip() not in txt:
            txt=marker+txt
    return txt

def main():
    global _TEST_MODE
    root=_source_root_from_argv()
    meta=_latest_source_meta(root)
    _TEST_MODE=bool(meta.get("test_only")) and bool(meta.get("manual_test_authorized"))

    r17._find_lane=_patched_find_lane
    r17._lane_is_candidate_generating=_patched_lane_candidate
    r175.compact=_patched_compact
    r175.VERSION=VERSION

    r175.main()

    # Persist explicit test semantics into observer meta/delivery audit.
    try:
        out=Path("reports/live_shadow_board_r17")
        try:
            i=sys.argv.index("--output-dir"); out=Path(sys.argv[i+1])
        except Exception: pass

        mp=out/"meta.json"
        if mp.exists():
            m=_read_json(mp)
            m["version"]=VERSION
            m["manual_test_capture"]=bool(_TEST_MODE)
            m["manual_test_not_live"]=bool(_TEST_MODE)
            m["manual_test_excluded_from_live_authority"]=bool(_TEST_MODE)
            mp.write_text(json.dumps(m,ensure_ascii=False,indent=2),encoding="utf-8")

        dp=out/"telegram_delivery.json"
        if dp.exists():
            d=_read_json(dp)
            d["manual_test_capture"]=bool(_TEST_MODE)
            d["manual_test_not_live"]=bool(_TEST_MODE)
            dp.write_text(json.dumps(d,ensure_ascii=False,indent=2),encoding="utf-8")
    except Exception:
        pass

if __name__=="__main__":
    main()
