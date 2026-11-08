import numpy as np
BINS=32
def volume_profile(window, bins=BINS, method="range"):
    """Approximate per-price volume from historical daily high/low and volume."""
    low, high, volume = (window[c].to_numpy(float) for c in ("low", "high", "volume"))
    if volume.sum() <= 0 or high.max() <= low.min():
        return None
    edges = np.linspace(low.min(), high.max(), bins + 1)
    centers = (edges[:-1] + edges[1:]) / 2
    if method == "typical":
        price = (high + low + window.close.to_numpy(float)) / 3
        idx = np.clip(np.searchsorted(edges, price, side="right") - 1, 0, bins - 1)
        weights = np.bincount(idx, weights=volume, minlength=bins)
    elif method == "range":
        span = high - low
        overlap = np.maximum(0, np.minimum(high[:, None], edges[1:]) -
                             np.maximum(low[:, None], edges[:-1]))
        distributed = overlap / np.where(span > 0, span, 1)[:, None]
        flat = np.flatnonzero(span == 0)
        idx = np.clip(np.searchsorted(edges, low[flat], side="right") - 1, 0, bins - 1)
        distributed[flat, idx] = 1
        weights = volume @ distributed
    else:
        raise ValueError(method)
    peak = left = right = int(weights.argmax())
    mass = weights[peak]
    while mass < 0.70 * weights.sum():
        take_left = right == bins - 1 or (left > 0 and weights[left-1] >= weights[right+1])
        if take_left:
            left -= 1
            mass += weights[left]
        else:
            right += 1
            mass += weights[right]
    hvn = [
        float(centers[i]) for i, w in enumerate(weights)
        if w >= 0.50 * weights[peak]
        and (i == 0 or w > weights[i-1])
        and (i == bins - 1 or w >= weights[i+1])
    ]
    return dict(poc=float(centers[peak]), vah=float(edges[right+1]),
                val=float(edges[left]), hvn=hvn, centers=centers,
                weights=weights, width=float(edges[1] - edges[0]))
