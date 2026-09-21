#!/usr/bin/env python3
from __future__ import annotations
from pathlib import Path
import argparse, hashlib, py_compile

MARKER="CLOSEBET LIVE SHADOW SOURCE CAPTURE R1.7.6"

def sha256(p: Path):
    return hashlib.sha256(p.read_bytes()).hexdigest()

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("target",nargs="?",default="Closing_bet_scanner_v2.py")
    ap.add_argument("--overlay",default=str(Path(__file__).with_name("closebet_live_shadow_source_capture_r176_overlay.py.txt")))
    a=ap.parse_args()

    target=Path(a.target)
    overlay=Path(a.overlay)
    if not target.exists():
        raise SystemExit(f"[FAIL] target missing: {target}")
    if not overlay.exists():
        raise SystemExit(f"[FAIL] overlay missing: {overlay}")

    text=target.read_text(encoding="utf-8")
    if MARKER in text:
        print("[PASS] R1.7.6 sidecar capture already present")
        print("sha256:",sha256(target))
        return

    required=["def _v4938_build_live_parts","if __name__ == '__main__':"]
    miss=[x for x in required if x not in text]
    if miss:
        raise SystemExit("[FAIL] required anchors missing: "+", ".join(miss))

    anchors=[]
    for s in ("if __name__ == '__main__':",'if __name__ == "__main__":'):
        p=text.rfind(s)
        if p>=0: anchors.append(p)
    if not anchors:
        raise SystemExit("[FAIL] __main__ anchor missing")
    pos=max(anchors)

    ov=overlay.read_text(encoding="utf-8").rstrip()+"\n\n"
    patched=text[:pos]+ov+text[pos:]
    target.write_text(patched,encoding="utf-8")
    py_compile.compile(str(target),doraise=True)
    print("[PASS] R1.7.6 copy-only sidecar overlay injected")
    print("sha256:",sha256(target))

if __name__=="__main__":
    main()
