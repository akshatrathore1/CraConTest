#!/usr/bin/env python3
"""Paired per-seed statistical comparison of candidate rankings across workload seeds.

Primary metric : k1 = position of first failing crash state in the ranking (lower is better).
Secondary      : S25 = distinct failing persistence states among the first 25 picks.
Design         : each seed is one paired observation (same recorded run, different ranking).
Primary test   : H1 "DCG k1 < R2 k1", one-sided exact sign test per setting, Holm-corrected across settings.
Effect size    : mean(k1_R2)/mean(k1_DCG) with 95% bootstrap CI (resampling seeds).
"""
import csv, json, glob, sys, math, random, statistics as st, os
from scipy import stats

MUT = {'Write','Create','Rename','Remove','Sync','SyncData','ReuseForWrite','Truncate','Link'}
NAMES = {'dcg': 'DCG', 'dcg-pr': 'DCG-PR', 'dcg-flag-only': 'FlagOnly', 'r2': 'R2', 'r1': 'R1', 'b0': 'B0', 'b2': 'LIFO', 'b7': 'RHG'}

def ld(p):
    with open(p, 'r', encoding='utf-8') as f:
        return json.load(f)

def load_run(d):
    o = ld(f'{d}/outcomes.json')['outcomes']
    fail = {(x['candidate']['event'], x['candidate']['percent'], x['candidate'].get('seed', 0)): x['class'] != 'valid' for x in o}
    t = ld(f'{d}/trace.json')['events']
    anchor, last = {0: 0}, 0
    for e in t:
        if e['kind'] in MUT: last = e['id']
        anchor[e['id']] = last
    return fail, anchor

def rank(d, n):
    p = f'{d}/ranking_{n}.json'
    if not os.path.exists(p):
        return []
    try:
        data = ld(p)
        return [(c['event'], c['percent'], c.get('seed', 0)) for c in data.get('candidates', [])]
    except Exception:
        return []

def metrics(order, fail, anchor):
    k1 = next((i + 1 for i, c in enumerate(order) if fail.get(c, False)), None)
    s25 = len({(anchor.get(c[0], 0), c[1]) for c in order[:25] if fail.get(c, False)})
    return k1, s25

def boot_ratio(a, b, n=10000, seed=1):
    """CI for mean(a)/mean(b) resampling paired seeds."""
    pairs = [(x, y) for x, y in zip(a or [], b or []) if x is not None and y is not None]
    if not pairs:
        return float('nan'), float('nan')
    r = random.Random(seed); m = len(pairs); vals = []
    a_f = [p[0] for p in pairs]
    b_f = [p[1] for p in pairs]
    for _ in range(n):
        idx = [r.randrange(m) for _ in range(m)]
        mb = sum(b_f[i] for i in idx)
        if mb > 0:
            vals.append(sum(a_f[i] for i in idx) / mb)
    if not vals:
        return float('nan'), float('nan')
    vals.sort()
    lo_idx = max(0, min(len(vals) - 1, int(0.025 * len(vals))))
    hi_idx = max(0, min(len(vals) - 1, int(0.975 * len(vals))))
    return vals[lo_idx], vals[hi_idx]

def sign_p_less(x, y):
    """one-sided exact sign test p for H1: x < y (paired); ties dropped."""
    pairs = [(a, b) for a, b in zip(x or [], y or []) if a is not None and b is not None]
    if not pairs:
        return 1.0, 0, 0, 0
    w = sum(a < b for a, b in pairs)
    l = sum(a > b for a, b in pairs)
    t = sum(a == b for a, b in pairs)
    n = w + l
    if n == 0:
        return 1.0, w, l, t
    return stats.binomtest(w, n, 0.5, alternative='greater').pvalue, w, l, t

def wilcoxon_p_less(x, y):
    d = [a - b for a, b in zip(x or [], y or []) if a is not None and b is not None and a != b]
    if len(d) < 5:
        return float('nan')
    try:
        return stats.wilcoxon(d, alternative='less').pvalue
    except Exception:
        return float('nan')

def holm(pvals):
    idx = sorted(range(len(pvals)), key=lambda i: pvals[i]); m = len(pvals); adj = [0] * m; run = 0
    for rnk, i in enumerate(idx):
        run = max(run, (m - rnk) * pvals[i]); adj[i] = min(1.0, run)
    return adj

def safe_mean(seq):
    vals = [x for x in seq if x is not None] if seq else []
    return st.mean(vals) if vals else float('nan')

def analyse(settings, root, strategies=('dcg', 'dcg-pr', 'dcg-flag-only', 'r2', 'r1', 'b0', 'b2'), baselines_for_tests=('r2', 'r1', 'b0')):
    out = []
    if root.endswith('.csv') or (os.path.exists(root) and not os.path.isdir(root)):
        with open(root, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            data_by_fault = {}
            for row in reader:
                fid = row['fault_id']
                seed = row['seed']
                if fid not in data_by_fault:
                    data_by_fault[fid] = {}
                if seed not in data_by_fault[fid]:
                    data_by_fault[fid][seed] = {}
                strat_code = row['strategy_code']
                data_by_fault[fid][seed][strat_code] = row
        active_settings = settings if settings else list(data_by_fault.keys())
        for s in active_settings:
            seeds_map = data_by_fault.get(s, {})
            nd = len(seeds_map)
            K = {n: [] for n in strategies}
            S = {n: [] for n in strategies}
            dens = []
            used = 0
            def seed_key(x):
                try: return int(x)
                except ValueError: return x
            for seed in sorted(seeds_map.keys(), key=seed_key):
                s_rows = seeds_map[seed]
                if not s_rows:
                    continue
                sample = next(iter(s_rows.values()))
                m = int(sample['total_failures']) if sample.get('total_failures') else 0
                univ = int(sample['universe_size']) if sample.get('universe_size') else 0
                if m == 0:
                    continue
                used += 1
                dens.append(m / univ if univ > 0 else 0.0)
                for n in strategies:
                    r_n = s_rows.get(n, {})
                    val_k1 = r_n.get('k1_single')
                    k1 = int(val_k1) if (val_k1 is not None and str(val_k1).strip() != '' and int(val_k1) > 0) else None
                    val_s25 = r_n.get('s25_single')
                    s25 = int(val_s25) if (val_s25 is not None and str(val_s25).strip() != '') else 0
                    K[n].append(k1)
                    S[n].append(s25)
            out.append((s, used, nd, dens, K, S))
    else:
        active_settings = settings if settings else [os.path.basename(p) for p in sorted(glob.glob(f'{root}/*')) if os.path.isdir(p)]
        for s in active_settings:
            dirs = sorted(glob.glob(f'{root}/{s}/*'))
            K = {n: [] for n in strategies}; S = {n: [] for n in strategies}; dens = []; used = 0
            for d in dirs:
                if not os.path.exists(f'{d}/outcomes.json') or not os.path.exists(f'{d}/trace.json'):
                    continue
                fail, anchor = load_run(d)
                m = sum(fail.values())
                if m == 0:
                    continue
                used += 1; dens.append(m / len(fail) if len(fail) > 0 else 0.0)
                for n in strategies:
                    k1, s25 = metrics(rank(d, n), fail, anchor); K[n].append(k1); S[n].append(s25)
            out.append((s, used, len(dirs), dens, K, S))
    return out

def report(settings, root, title):
    res = analyse(settings, root)
    print(f'\n##### {title}')
    print(f"{'setting':14s} {'seeds w/ fail':>13s} {'dens%':>6s} | {'k1 DCG':>7s} {'k1 R2':>7s} {'k1 R1':>7s} {'k1 B0':>7s} {'k1 Flag':>7s} {'k1 LIFO':>8s} | {'R2/DCG [95% CI]':>22s} | DCG<R2 W/L/T  p(sign)  p(Wilcoxon) | S25 DCG/R2")
    ps = []; rows = []
    for s, used, nd, dens, K, S in res:
        if used < 8:
            print(f"{s:14s} {used:>7d}/{nd:<5d} (fewer than 8 seeds with any failure: not testable, reported as null)")
            continue
        p, w, l, t = sign_p_less(K.get('dcg', []), K.get('r2', []))
        pw = wilcoxon_p_less(K.get('dcg', []), K.get('r2', []))
        lo, hi = boot_ratio(K.get('r2', []), K.get('dcg', []))
        ps.append(p)
        rows.append((s, used, nd, dens, K, S, p, pw, w, l, t, lo, hi))
    adj = holm(ps) if ps else []
    for (s, used, nd, dens, K, S, p, pw, w, l, t, lo, hi), pa in zip(rows, adj):
        mean = lambda n: safe_mean(K.get(n, []))
        m_dcg = mean('dcg')
        m_r2 = mean('r2')
        if m_dcg and not math.isnan(m_dcg) and not math.isnan(m_r2) and m_dcg > 0:
            ratio_str = f"{m_r2 / m_dcg:6.1f}x"
        else:
            ratio_str = "   N/Ax"
        dens_mean = 100 * safe_mean(dens)
        s_dcg = safe_mean(S.get('dcg', []))
        s_r2 = safe_mean(S.get('r2', []))
        print(f"{s:14s} {used:>7d}/{nd:<5d} {dens_mean:6.1f} | {mean('dcg'):7.2f} {mean('r2'):7.2f} {mean('r1'):7.2f} {mean('b0'):7.2f} {mean('dcg-flag-only'):7.2f} {mean('b2'):8.1f} | {ratio_str} [{lo:5.1f},{hi:6.1f}] | {w:2d}/{l:2d}/{t:2d}   {p:8.2e}  {pw:8.2e} (Holm {pa:.1e}) | {s_dcg:.1f}/{s_r2:.1f}")
    # worse-than-baseline flags
    def is_worse(k_a, k_b):
        ma, mb = safe_mean(k_a), safe_mean(k_b)
        return not math.isnan(ma) and not math.isnan(mb) and ma > mb

    print('\n  settings where DCG is WORSE than R2 on average k1:', [r[0] for r in rows if is_worse(r[4].get('dcg'), r[4].get('r2'))] or 'none')
    print('  settings where DCG is WORSE than uniform random (B0) on average k1:', [r[0] for r in rows if is_worse(r[4].get('dcg'), r[4].get('b0'))] or 'none')
    return rows

if __name__ == '__main__':
    root = sys.argv[1]; title = sys.argv[2]; settings = sys.argv[3:]
    report(settings, root, title)
