"""Daily proxies only. Time cutoffs and histogram geometry are explicit."""
import numpy as np

METHODS=('range','typical','close','ohlc4')
FIELDS=('open','high','low','close','volume')

def validate(w):
    a=w[list(FIELDS)].to_numpy(float)
    if not len(a) or not np.isfinite(a).all():raise ValueError('NONFINITE_OR_EMPTY')
    if (a[:,:4]<=0).any() or (a[:,4]<0).any():raise ValueError('BAD_PRICE_OR_VOLUME')
    if ((w.low>w.high)|(w.open<w.low)|(w.open>w.high)|(w.close<w.low)|(w.close>w.high)).any():raise ValueError('INVALID_OHLC')

def snapshot(d,j,n,kind):
    if kind not in ('PAST_ONLY','SIGNAL_CLOSE'):raise ValueError('UNKNOWN_SNAPSHOT')
    end=j if kind=='PAST_ONLY' else j+1
    if end<n:raise ValueError('INSUFFICIENT_PAST')
    return d.iloc[end-n:end].copy()

def edges_for(w,bins):
    validate(w)
    if bins<1:raise ValueError('BAD_BINS')
    lo,hi=float(w.low.min()),float(w.high.max())
    if hi==lo:lo,hi=lo-.5,hi+.5
    return np.linspace(lo,hi,bins+1)

def extend_edges(edges,w):
    width=edges[1]-edges[0]
    left=max(0,int(np.ceil((edges[0]-w.low.min())/width)))
    right=max(0,int(np.ceil((w.high.max()-edges[-1])/width)))
    return edges[0]+np.arange(-left,len(edges)+right)*width

def matrix(w,edges,method):
    validate(w);edges=np.asarray(edges,float)
    if len(edges)<2 or not np.isfinite(edges).all() or not (np.diff(edges)>0).all():raise ValueError('BAD_EDGES')
    tol=max(1.,abs(edges[-1]))*1e-12
    if w.low.min()<edges[0]-tol or w.high.max()>edges[-1]+tol:raise ValueError('OUTSIDE_GRID_NO_CLIPPING')
    low,high=w.low.to_numpy(float),w.high.to_numpy(float);bins=len(edges)-1
    if method=='range':
        span=high-low
        overlap=np.maximum(0,np.minimum(high[:,None],edges[1:])-np.maximum(low[:,None],edges[:-1]))
        a=overlap/np.where(span>0,span,1)[:,None]
        flat=np.flatnonzero(span==0);idx=np.clip(np.searchsorted(edges,low[flat],side='right')-1,0,bins-1);a[flat,idx]=1
    elif method in ('typical','close','ohlc4'):
        if method=='typical':points=((w.high+w.low+w.close)/3).to_numpy()[:,None]
        elif method=='close':points=w.close.to_numpy()[:,None]
        else:points=w[['open','high','low','close']].to_numpy()
        a=np.zeros((len(w),bins));idx=np.clip(np.searchsorted(edges,points,side='right')-1,0,bins-1)
        for k in range(points.shape[1]):np.add.at(a,(np.arange(len(w)),idx[:,k]),1/points.shape[1])
    else:raise ValueError('UNKNOWN_METHOD')
    np.testing.assert_allclose(a.sum(axis=1),1,rtol=1e-10,atol=1e-10)
    return a

def profile(w,edges,method):
    a=matrix(w,edges,method);volume=w.volume.to_numpy(float);weights=(a*volume[:,None]).sum(axis=0)
    total=float(volume.sum())
    if total<=0:raise ValueError('ZERO_VOLUME')
    assert np.isclose(weights.sum(),total,rtol=1e-10,atol=1e-6)
    return weights

def features(weights,edges,close):
    peak=left=right=int(weights.argmax());total=float(weights.sum());mass=float(weights[peak]);bins=len(weights)
    while mass<.7*total:
        take_left=right==bins-1 or (left>0 and weights[left-1]>=weights[right+1])
        if take_left:left-=1;mass+=weights[left]
        else:right+=1;mass+=weights[right]
    local=[i for i,v in enumerate(weights) if v>=.5*weights[peak] and (i==0 or v>weights[i-1]) and (i==bins-1 or v>=weights[i+1])]
    overhead=[i for i in local if edges[i]>close];above=min(overhead,key=lambda i:edges[i]) if overhead else None
    position=lambda i:'ABOVE_CLOSE' if edges[i]>close else 'BELOW_CLOSE' if edges[i+1]<close else 'CLOSE_INSIDE'
    return dict(poc=float((edges[peak]+edges[peak+1])/2),vah=float(edges[right+1]),val=float(edges[left]),
                peak_low=float(edges[peak]),peak_high=float(edges[peak+1]),peak_pct=100*float(weights[peak])/total,
                peak_position=position(peak),hvn_count=len(local),overhead_low=float(edges[above]) if above is not None else np.nan,
                overhead_high=float(edges[above+1]) if above is not None else np.nan,
                overhead_pct=100*float(weights[above])/total if above is not None else np.nan,
                overhead_distance_pct=100*(float(edges[above])/close-1) if above is not None else np.nan)

def query_band(w,lo,hi,method):
    validate(w)
    if not lo<hi:raise ValueError('BAD_BAND')
    if method=='range':
        span=(w.high-w.low).to_numpy(float);low=w.low.to_numpy(float);high=w.high.to_numpy(float)
        f=np.maximum(0,np.minimum(high,hi)-np.maximum(low,lo))/np.where(span>0,span,1)
        f[span==0]=((low[span==0]>=lo)&(low[span==0]<=hi)).astype(float)
    else:
        if method=='typical':p=((w.high+w.low+w.close)/3).to_numpy()[:,None]
        elif method=='close':p=w.close.to_numpy()[:,None]
        elif method=='ohlc4':p=w[['open','high','low','close']].to_numpy()
        else:raise ValueError('UNKNOWN_METHOD')
        f=((p>=lo)&(p<=hi)).mean(axis=1)
    return float(np.dot(f,w.volume.to_numpy(float)))

def partial_rebase(w,signal_date,actions,quantity='RAW'):
    """Partial confirmed reference scenario, NOT certified adjusted OHLCV."""
    out=w.copy();applied=[]
    for a in actions:
        if a['known_date']>signal_date or a['effective_date']>signal_date:continue
        factor=float(a['reference']/a['previous_close']);mask=out.date.lt(a['effective_date'])
        out.loc[mask,['open','high','low','close']]*=factor
        if quantity=='INVERSE_PRICE_SYNTHETIC':out.loc[mask,'volume']/=factor
        elif quantity!='RAW':raise ValueError('BAD_QUANTITY_SCENARIO')
        applied.append(a['effective_date'])
    return out,applied
