from pathlib import Path
import sys
import types
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import real_full_r1c1_runtime_adapter as a


def test_self():
    assert a.self_test() == 0


def test_same_day_pb_is_serializable_but_not_causal_eligible():
    sd = "2026-09-15"
    idx = pd.to_datetime(["2026-09-10", "2026-09-11", "2026-09-12", "2026-09-15"])
    raw = pd.DataFrame({
        "Open": [10, 11, 12, 11],
        "High": [11, 14, 13, 12],
        "Low": [9, 10, 11, 10],
        "Close": [10, 13, 12, 11],
        "Volume": [100, 120, 80, 70],
    }, index=idx)
    anchor = {
        "wave1_low_date": "2026-09-10",
        "wave1_high_date": "2026-09-11",
        "pullback_low_date": "2026-09-15",
    }
    m = a._frozen_volume_metrics(raw, anchor, sd)
    assert m["metric_status"] == "PASS"
    assert m["predictor_causal_invariant"] == "FAIL_PULLBACK_NOT_PRE_SIGNAL"
    assert m["max_predictor_price_date_used"] == sd


def test_build_uses_exact_runtime_cache_without_refetch(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    sd = "2026-09-15"
    dates = pd.to_datetime(["2026-09-09", "2026-09-10", "2026-09-11", "2026-09-12", "2026-09-15"])
    cache = {}
    top = []
    for i in range(15):
        code = f"{i+1:06d}"
        raw = pd.DataFrame({
            "Open": [9, 10, 11, 12, 12],
            "High": [10, 11, 15, 14, 14],
            "Low": [8, 9, 10, 11, 11],
            "Close": [9, 10, 14, 12, 13],
            "Volume": [90+i, 100+i, 120+i, 70+i, 80+i],
        }, index=dates)
        cache[f"{code}_220_20260915"] = raw
        top.append({"종목코드": code, "종목명": f"N{i+1}", "현재가": 13+i})

    def detector(df):
        return {
            "ok": True,
            "score": 100.0,
            "raw_score": 112.0,
            "grade": "A",
            "reason": "TEST",
            "debug": "score=112",
        }

    g = {
        "_v1107_4_5_72_detect_pullback_restart": detector,
        "_fdr_cache": cache,
        "get_indicators": lambda df: df.copy(),
        "__name__": "legacy_test",
        "__file__": "main7_bugfix_2.py",
    }
    fake_frame = types.SimpleNamespace(f_globals=g)

    sfcp = types.ModuleType("search_formula_complete_pipeline")
    def anchor_fn(history, signal_date):
        return {
            "anchor_status": "AVAILABLE",
            "anchor_method": a.ANCHOR_METHOD,
            "signal_date": signal_date,
            "wave1_low_date": "2026-09-10",
            "wave1_low_price": 9.0,
            "wave1_high_date": "2026-09-11",
            "wave1_high_price": 15.0,
            "pullback_low_date": "2026-09-12",
            "pullback_low_price": 11.0,
            "temporal_invariant": "PASS",
        }
    sfcp.causal_anchor_v1 = anchor_fn
    monkeypatch.setitem(sys.modules, "search_formula_complete_pipeline", sfcp)

    assert a.build_from_runtime_frames([fake_frame], pd.DataFrame(top), sd, "PRIMARY_1440") is True
    side = pd.read_csv(tmp_path / "reports/real_full_r1c1_v72_runtime_sidecar.csv", dtype={"code": str})
    anchors = pd.read_csv(tmp_path / "reports/v72_formula_selector_anchor_serialization.csv", dtype={"code": str})
    assert len(side) == 15 and len(anchors) == 15
    assert side["v72_pullback_restart_score"].eq(100).all()
    assert side["network_refetch_used"].eq(False).all()
    assert anchors["runtime_price_provenance"].eq(a.RUNTIME_PRICE_PROVENANCE).all()
    assert anchors["predictor_causal_invariant"].eq("PASS").all()
    assert pd.to_numeric(anchors["pb_volume_vs_wave1"], errors="coerce").notna().all()
