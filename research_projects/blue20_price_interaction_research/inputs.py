"""Verify and rebuild the frozen 500-stock input using existing research loaders."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import numpy as np
import pandas as pd

HERE=Path(__file__).resolve().parent
RESEARCH=Path(os.environ.get("BLUE20_RESEARCH_ROOT",HERE.parent))
LEDGER=HERE/".prepared/EVENTS.csv.gz"


def rebuild():
    spec=importlib.util.spec_from_file_location("interaction_frozen_auto_data",RESEARCH/"r1d118/auto_data.py")
    source=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(source)
    cache=Path(os.environ.get("BLUE20_INPUT_CACHE",RESEARCH/"r1d118/.local_source_cache"))
    annual={}
    for year,digest in source.MARCAP_SHA.items():
        path=cache/f"marcap-{year}.parquet"
        assert source.blob_hash(path.read_bytes())==digest
        original=pd.read_parquet(path)
        if "Date" not in original:original=original.reset_index()
        normalized=source.normalize_marcap(original,year)[0]
        annual[year]=normalized.merge(original[["Code","Date","Changes","Stocks"]],
                                      on=["Code","Date"],validate="one_to_one")
    codes,asof=source.make_cohort(annual,count=500)
    frozen=json.loads((HERE/"reference/frozen_reference.json").read_text(encoding="utf-8"))
    assert set(codes)==set(frozen["cohort_codes"])
    manifest=Path(os.environ["BLUE20_MANIFEST"])
    pins=pd.read_csv(manifest,dtype=str).set_index("code")
    assert set(pins.index)==set(codes)
    all_data=pd.concat(annual.values(),ignore_index=True)
    frames,refs={},{}
    fields=["open","high","low","close","volume","amount"]
    for code,g in all_data[all_data.code.isin(codes)].groupby("code"):
        raw=g.sort_values("date").reset_index(drop=True)
        refs[code]=raw
        d=raw[["code","date",*[c.title() for c in fields]]].copy()
        d.columns=["code","date",*fields]
        saved=(manifest.parent/pins.loc[code,"path"].replace("\\","/")).read_bytes()
        assert hashlib.sha256(saved).hexdigest()==pins.loc[code,"sha256"]
        content=d.to_csv(index=False,lineterminator="\n").encode("utf-8-sig")
        assert content.decode("utf-8-sig")==saved.decode("utf-8-sig").replace("\r\n","\n")
        if code in frozen["event_stock_prices"]:
            assert hashlib.sha256(content).hexdigest()==frozen["event_stock_prices"][code]
        prev=d.close.shift()
        tr=pd.concat([d.high-d.low,(d.high-prev).abs(),(d.low-prev).abs()],axis=1).max(axis=1)
        d["prior_atr20"]=tr.rolling(20).mean().shift()
        frames[code]=d
    return frames,refs,frozen,asof


def prepare():
    import sys
    for module in ("blue20_ma_structure_research","blue20_candle_exhaustion_research"):
        sys.path.insert(0,str(RESEARCH/module))
    from analyze_ma import load_events,load_manifest
    from analyze_candle_exhaustion import build_table,candle_features
    from research import identity_digest
    manifest=Path(os.environ["BLUE20_MANIFEST"])
    events=Path(os.environ["BLUE20_EVENTS"])
    pins=pd.read_csv(manifest,dtype=str)
    frames,_=load_manifest(manifest,set(pins.code))
    base=build_table(load_events(events),{code:candle_features(d) for code,d in frames.items()})
    frozen=json.loads((HERE/"reference/frozen_reference.json").read_text(encoding="utf-8"))
    assert identity_digest(base)==frozen["identity_sha256"]
    LEDGER.parent.mkdir(exist_ok=True)
    base.to_csv(LEDGER,index=False)
    print("Verified original 1804 event identities and baseline trade replay")


if __name__=="__main__":
    import sys
    sys.path.insert(0,str(RESEARCH/"blue20_repeated_resistance_research"))
    prepare()
