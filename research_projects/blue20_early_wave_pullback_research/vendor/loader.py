from pathlib import Path
import hashlib
import pandas as pd
def load_prices(manifest):
    m=pd.read_csv(manifest,dtype=str)
    assert len(m)==500 and m.code.nunique()==500
    frames={}
    for row in m.itertuples(index=False):
        p=manifest.parent/row.path.replace("\\","/")
        assert hashlib.sha256(p.read_bytes()).hexdigest()==row.sha256
        d=pd.read_csv(p,dtype={"code":str,"date":str})
        assert d.code.eq(row.code).all() and not d.date.duplicated().any() and d.date.is_monotonic_increasing
        frames[row.code]=d
    return frames
