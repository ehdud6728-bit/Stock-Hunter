#!/usr/bin/env python3
from __future__ import annotations
import argparse, hashlib, py_compile, shutil
from datetime import datetime
from pathlib import Path

MARKER='# ✅ V49.76.4 FINAL ACTION AUTHORITY GUARD — SAFE OVERLAY'

def sha256(path: Path) -> str:
    h=hashlib.sha256()
    with path.open('rb') as f:
        for c in iter(lambda:f.read(1024*1024),b''): h.update(c)
    return h.hexdigest()

def find_anchor(text: str):
    xs=[]
    for a in ("if __name__ == '__main__':", 'if __name__ == "__main__":'):
        p=text.rfind(a)
        if p>=0: xs.append((p,a))
    return max(xs) if xs else None

def main():
    ap=argparse.ArgumentParser(description='Apply v49.76.4 FINAL ACTION AUTHORITY overlay')
    ap.add_argument('target',nargs='?',default='Closing_bet_scanner_v2.py')
    ap.add_argument('--overlay',default=str(Path(__file__).with_name('v49764_final_action_overlay.py.txt')))
    args=ap.parse_args()
    target=Path(args.target).resolve(); overlay=Path(args.overlay).resolve()
    if not target.exists(): raise SystemExit(f'[FAIL] target not found: {target}')
    if not overlay.exists(): raise SystemExit(f'[FAIL] overlay not found: {overlay}')
    text=target.read_text(encoding='utf-8')
    if MARKER in text:
        print('[PASS] v49.76.4 already present; no duplicate edit')
        print('sha256:',sha256(target)); return
    required=['def _v4938_build_live_parts','def _v4976_execution_board_lines']
    miss=[x for x in required if x not in text]
    if miss: raise SystemExit('[FAIL] v49.76.x anchors missing: '+', '.join(miss))
    anc=find_anchor(text)
    if not anc: raise SystemExit('[FAIL] __main__ anchor missing; refusing guess edit')
    pos,_=anc
    block=overlay.read_text(encoding='utf-8').strip()+'\n\n'
    patched=text[:pos].rstrip()+'\n\n'+block+text[pos:]
    stamp=datetime.now().strftime('%Y%m%d_%H%M%S')
    backup=target.with_name(target.name+f'.pre_v49764_{stamp}.bak')
    shutil.copy2(target,backup); before=sha256(target)
    try:
        target.write_text(patched,encoding='utf-8')
        py_compile.compile(str(target),doraise=True)
    except Exception:
        shutil.copy2(backup,target); raise
    print('[PASS] V49.76.4 FINAL ACTION AUTHORITY applied')
    print('target :',target)
    print('backup :',backup)
    print('before :',before)
    print('after  :',sha256(target))
    print('scope  : user action authority/display guard only; search/rank/ENTRY/EXIT/MARCAP/PIT frozen')

if __name__=='__main__': main()
