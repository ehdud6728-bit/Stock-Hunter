#!/usr/bin/env python3
from pathlib import Path
import argparse, hashlib, py_compile

MARKER="LIVE_SHADOW_SOURCE_R11 LOADED"
def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("target",nargs="?",default="Closing_bet_scanner_v2.py")
    ap.add_argument("--overlay",default="live_shadow_source_r11_overlay.py.txt")
    a=ap.parse_args()
    target=Path(a.target); overlay=Path(a.overlay)
    if not target.exists(): raise SystemExit(f"TARGET_MISSING {target}")
    if not overlay.exists(): raise SystemExit(f"OVERLAY_MISSING {overlay}")
    s=target.read_text(encoding="utf-8")
    if MARKER in s:
        print("PASS already patched",sha(target)); return
    ov=overlay.read_text(encoding="utf-8")
    target.write_text(s.rstrip()+"\n\n"+ov.strip()+"\n",encoding="utf-8")
    py_compile.compile(str(target),doraise=True)
    print("PATCHED",target)
    print("sha256",sha(target))
if __name__=="__main__": main()
