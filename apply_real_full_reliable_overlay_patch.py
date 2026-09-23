#!/usr/bin/env python3
from pathlib import Path

P = Path(".github/workflows/run_scanner.yml")
if not P.exists():
    raise SystemExit("run_scanner.yml not found")

s = P.read_text(encoding="utf-8")
orig = s

s = s.replace(
'''    # 한국시간 평일 오후 2:40 = UTC 05:40 — REAL_FULL 광범위 후보 탐색 1차
    - cron: '40 05 * * 1-5'
''',
'''    # REAL_FULL은 전용 REAL_FULL Reliable Trigger가 14:30~15:00 다중 슬롯 + 일일 lock으로 dispatch 합니다.
'''
)

s = s.replace(
'''              "40 05 * * 1-5") profile="REAL_FULL"; real_full_capture_slot="PRIMARY_1440" ;;
''',
''
)

s = s.replace(
'''          echo "SCHEDULE_EXPECTED_KST=09:30 OPEN_0930 / 14:40 REAL_FULL / 15:20 TRUST watchdog / 15:45 CORE224_LIVE weekdays"
''',
'''          echo "SCHEDULE_EXPECTED_KST=09:30 OPEN_0930 / REAL_FULL Reliable Trigger 14:30~15:00 / 15:20 TRUST watchdog / 15:45 CORE224_LIVE weekdays"
'''
)

s = s.replace(
'''          # Forward-causal capture is shared with the only scheduled REAL_FULL run.
          # 09:30 OPEN_0930 stays lightweight; 14:40 REAL_FULL performs the broad capture.
          if [ "$event" = "schedule" ] && [ "$schedule_expr" = "40 05 * * 1-5" ]; then
            catalyst_capture=1
          fi
''',
'''          # REAL_FULL is workflow_dispatch'ed by the dedicated Reliable Trigger.
          # workflow_dispatch AUTO mode below enables forward-causal capture for REAL_FULL.
'''
)

old_manual = '''          if [ "$event" = "workflow_dispatch" ] || [ "$event" = "schedule" ]; then
            force_bt=1
            use_bt_token=1
            if [ "$event" = "workflow_dispatch" ]; then manual=1; else manual=0; fi
          else
'''
new_manual = '''          if [ "$event" = "workflow_dispatch" ] || [ "$event" = "schedule" ]; then
            force_bt=1
            use_bt_token=1
            if [ "$event" = "workflow_dispatch" ]; then manual=1; else manual=0; fi
            case "${real_full_capture_slot:-}" in
              RELIABLE_TRIGGER_*) manual=0 ;;
            esac
          else
'''
if "RELIABLE_TRIGGER_*) manual=0" not in s:
    if old_manual not in s:
        raise SystemExit("manual-mode anchor not found; refusing partial patch")
    s = s.replace(old_manual, new_manual)

if "      - name: Build + send REAL_FULL Research Overlay immediately\n" not in s:
    anchor = '''          PYBRIDGE

      - name: Capture REAL_FULL TRUST R1 prospective snapshot
'''
    block = '''          PYBRIDGE

      - name: Build + send REAL_FULL Research Overlay immediately
        if: success() && env.TEST_PROFILE == 'REAL_FULL'
        shell: bash
        run: |
          set -euo pipefail
          REG="reference/FROZEN_watermelon_longma_signature_registry.csv"
          test -f real_full_research_overlay_r1.py || { echo "OVERLAY_SOURCE_MISSING"; exit 271; }
          test -f reports/real_full_trust_source.csv || { echo "OVERLAY_TRUST_SOURCE_MISSING"; exit 272; }
          test -f reports/real_full_current_universe.csv || { echo "OVERLAY_UNIVERSE_MISSING"; exit 273; }
          test -f "$REG" || { echo "OVERLAY_FROZEN_REGISTRY_MISSING"; exit 274; }
          python -m py_compile real_full_research_overlay_r1.py
          python -u real_full_research_overlay_r1.py \
            --source reports/real_full_trust_source.csv \
            --universe reports/real_full_current_universe.csv \
            --registry "$REG" \
            --output-dir reports/real_full_research_overlay_r1
          python - <<'PYOV'
          import hashlib,json
          from pathlib import Path
          reg=Path("reference/FROZEN_watermelon_longma_signature_registry.csv")
          assert hashlib.sha256(reg.read_bytes()).hexdigest()=="558ff5804134a57d35eb77736f6e064ff925588347919912c3e2b07cb26da51a"
          meta=json.loads(Path("reports/real_full_research_overlay_r1/real_full_research_overlay_meta.json").read_text(encoding="utf-8"))
          assert meta["research_only"] is True
          assert not meta["production_search_changed"]
          assert not meta["production_score_changed"]
          assert not meta["production_rank_changed"]
          assert not meta["production_order_changed"]
          print("REAL_FULL_FAST_OVERLAY_INVARIANTS PASS")
          PYOV
          TOKEN="${TELEGRAM_BACKTEST_TOKEN:-}"
          CHAT="${STOCK_SEARCH_CHAT_ID:-${STOCKHUNTER_CHAT_ID:-${TELEGRAM_DYUL_CHAT_ID:-${TELEGRAM_CHAT_ID:-}}}}"
          FILE="reports/real_full_research_overlay_r1/real_full_research_overlay.txt"
          if [ -n "$TOKEN" ] && [ -n "$CHAT" ] && [ -s "$FILE" ]; then
            python - <<'PYTGOV'
          import os,urllib.parse,urllib.request
          from pathlib import Path
          text=Path("reports/real_full_research_overlay_r1/real_full_research_overlay.txt").read_text(encoding="utf-8")
          chunks=[]
          while text:
              if len(text)<=3500:
                  chunks.append(text); break
              cut=text.rfind("\n",0,3500)
              if cut<1000: cut=3500
              chunks.append(text[:cut]); text=text[cut:].lstrip("\n")
          token=os.environ["TELEGRAM_BACKTEST_TOKEN"]
          chat=os.environ.get("STOCK_SEARCH_CHAT_ID") or os.environ.get("STOCKHUNTER_CHAT_ID") or os.environ.get("TELEGRAM_DYUL_CHAT_ID") or os.environ.get("TELEGRAM_CHAT_ID")
          for i,msg in enumerate(chunks,1):
              data=urllib.parse.urlencode({"chat_id":chat,"text":msg}).encode()
              req=urllib.request.Request(f"https://api.telegram.org/bot{token}/sendMessage",data=data)
              with urllib.request.urlopen(req,timeout=30) as r:
                  assert r.status==200
              print("REAL_FULL_FAST_OVERLAY_TELEGRAM_SENT",i,len(msg))
          PYTGOV
          else
            echo "REAL_FULL_FAST_OVERLAY_TELEGRAM_SKIP token/chat/file unavailable"
          fi

      - name: Capture REAL_FULL TRUST R1 prospective snapshot
'''
    if anchor not in s:
        raise SystemExit("source-bridge anchor not found; refusing partial patch")
    s = s.replace(anchor, block)

if s == orig:
    print("run_scanner already patched; no changes")
else:
    P.write_text(s, encoding="utf-8")
    print("run_scanner patch applied")

q = P.read_text(encoding="utf-8")
assert "cron: '40 05 * * 1-5'" not in q, "direct REAL_FULL cron still present"
assert "RELIABLE_TRIGGER_*) manual=0" in q
assert "Build + send REAL_FULL Research Overlay immediately" in q
assert "558ff5804134a57d35eb77736f6e064ff925588347919912c3e2b07cb26da51a" in q
print("REAL_FULL_RELIABLE_OVERLAY_PATCH_GUARDS PASS")
