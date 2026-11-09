"""Exact BB bands and signal from the frozen eight-variant research."""

def bands(close,n,multiplier=2.,shift=26):
 r=close.rolling(n,min_periods=n);m=r.mean();s=r.std(ddof=0);return {'UPPER':(m+multiplier*s).shift(shift),'LOWER':(m-multiplier*s).shift(shift)}

def signal(d,line):
 b=d.close-d.open
 return (line.lt(line.shift())&d.close.shift().le(line.shift())&d.open.lt(line)&d.close.gt(line)&b.gt(0)&((line-d.open)/b).ge(.2)&((d.close-line)/b).ge(.2)&(100*b/d.open).ge(.5)).fillna(False)
