#!/usr/bin/env python3
"""Paired per-seed statistical comparison of candidate rankings across workload seeds.

Primary metric : k1 = position of first failing crash state in the ranking (lower is better).
Secondary      : S25 = distinct failing persistence states among the first 25 picks.
Design         : each seed is one paired observation (same recorded run, different ranking).
Primary test   : H1 "DCG k1 < R2 k1", one-sided exact sign test per setting, Holm-corrected across settings.
                 Sign tests use k1_single (the deterministic single-ranking k1) for reproducibility.
Effect size    : mean(k1_R2)/mean(k1_DCG) with 95% bootstrap CI (resampling seeds).
"""
import argparse
import csv
import glob
import json
import math
import os
import random
import statistics as st
import sys

from scipy import stats

MUT = {'Write', 'WriteAt', 'Create', 'Rename', 'Remove', 'RemoveAll', 'MkdirAll', 'Sync', 'SyncData', 'SyncTo', 'ReuseForWrite', 'Link', 'Preallocate'}
NAMES = {
    'dcg': 'DCG',
    'dcg-pr': 'DCG-PR',
    'dcg-flag-only': 'FlagOnly',
    'r2': 'R2',
    'r1': 'R1',
    'b0': 'B0',
    'b2': 'LIFO',
    'b4': 'Combined',
    'b5': 'Representative',
    'b6': 'SyncBoundary',
    'b7': 'RHG',
}

def ld(p):
    with open(p, 'r', encoding='utf-8') as f:
        return json.load(f)

def is_failure_class(cls):
    # Exclude infrastructure errors (oracle_error) from failure counts;
    # failures are actual consistency violations, crashes, or hangs.
    return cls in ('violation', 'open_error', 'panic', 'hang')

def load_run(d):
    o = ld(f'{d}/outcomes.json')['outcomes']
    fail = {(x['candidate']['event'], x['candidate']['percent'], x['candidate'].get('seed', 0)): is_failure_class(x['class']) for x in o}
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

DEFAULT_IMPUTATION_UNIVERSE = 1000  # Conservative upper bound penalty on rank when candidate universe size is unknown


def impute_rank(val, univ=None, default_impute=DEFAULT_IMPUTATION_UNIVERSE):
    """Impute missing rank (where no bug was found) with universe size or default penalty.

    When a strategy fails to find any bug in a run, its rank is missing (None).
    To avoid survival bias, missing values are imputed using the candidate universe
    size for that seed (or default_impute if universe size is unknown or non-positive).
    """
    if val is not None:
        return float(val)
    if univ is not None and univ > 0:
        return float(univ)
    return float(default_impute)


def boot_ratio(a, b, univs=None, n=10000, seed=1, default_impute=DEFAULT_IMPUTATION_UNIVERSE):
    """CI for mean(a)/mean(b) resampling paired seeds.
    Missing values (None) are imputed using candidate universe size so failed seeds are not dropped.
    """
    if not a or not b or len(a) != len(b):
        return float('nan'), float('nan')
    m = len(a)
    if m < 2:
        return float('nan'), float('nan')
    u = lambda i: univs[i] if (univs and i < len(univs)) else None
    a_f = [impute_rank(a[i], u(i), default_impute) for i in range(m)]
    b_f = [impute_rank(b[i], u(i), default_impute) for i in range(m)]
    r = random.Random(seed)
    vals = []
    for _ in range(n):
        idx = [r.randrange(m) for _ in range(m)]
        mb = sum(b_f[i] for i in idx)
        if mb > 0:
            vals.append(sum(a_f[i] for i in idx) / mb)
        elif mb == 0 and sum(a_f[i] for i in idx) == 0:
            vals.append(1.0)
    if not vals:
        return float('nan'), float('nan')
    vals.sort()
    lo_idx = max(0, min(len(vals) - 1, int(round(0.025 * (len(vals) - 1)))))
    hi_idx = max(0, min(len(vals) - 1, int(round(0.975 * (len(vals) - 1)))))
    return vals[lo_idx], vals[hi_idx]

def sign_p_less(x, y):
    """one-sided exact sign test p for H1: x < y (paired); ties dropped.
    None represents failure to find a bug (imputed as infinity > any valid rank).
    """
    if not x or not y or len(x) != len(y):
        return 1.0, 0, 0, 0
    pairs = list(zip(x, y))
    w = 0
    l = 0
    t = 0
    for a, b in pairs:
        if a is None and b is None:
            t += 1
        elif a is None and b is not None:
            l += 1
        elif a is not None and b is None:
            w += 1
        else:
            if a < b:
                w += 1
            elif a > b:
                l += 1
            else:
                t += 1
    n = w + l
    if n == 0:
        return 1.0, w, l, t
    return stats.binomtest(w, n, 0.5, alternative='greater').pvalue, w, l, t

def wilcoxon_p_less(x, y, univs=None, default_impute=DEFAULT_IMPUTATION_UNIVERSE):
    """One-sided Wilcoxon signed-rank test for paired ranks x and y (H1: x < y).
    Missing ranks are imputed with universe size to avoid survival bias.
    """
    if not x or not y or len(x) != len(y):
        return float('nan')
    m = len(x)
    d = []
    u = lambda i: univs[i] if (univs and i < len(univs)) else None
    for i in range(m):
        vx = impute_rank(x[i], u(i), default_impute)
        vy = impute_rank(y[i], u(i), default_impute)
        diff = vx - vy
        if diff != 0:
            d.append(diff)
    if len(d) < 5:
        return float('nan')
    try:
        return stats.wilcoxon(d, alternative='less').pvalue
    except Exception:
        return float('nan')

def holm(pvals):
    """Compute Holm-Bonferroni step-down family-wise error rate (FWER) adjusted p-values.

    Parameters
    ----------
    pvals : list of float
        Raw unadjusted p-values.

    Returns
    -------
    list of float
        Adjusted p-values in the same order as input.
    """
    if not pvals:
        return []

    m = len(pvals)
    # Order p-values ascending by value
    sorted_indices = sorted(range(m), key=lambda i: pvals[i])

    adjusted = [0.0] * m
    running_max = 0.0

    for rank, orig_idx in enumerate(sorted_indices):
        # Step-down multiplier: (m - rank)
        multiplier = m - rank
        val = multiplier * pvals[orig_idx]
        running_max = max(running_max, val)
        adjusted[orig_idx] = min(1.0, running_max)

    return adjusted

def safe_mean(seq):
    vals = [x for x in seq if x is not None] if seq else []
    return st.mean(vals) if vals else float('nan')

def analyse(settings, root, strategies=('dcg', 'dcg-pr', 'dcg-flag-only', 'r2', 'r1', 'b0', 'b2'), baselines_for_tests=('r2', 'r1', 'b0')):
    out = []
    if root.endswith('.csv') or (os.path.exists(root) and not os.path.isdir(root)):
        with open(root, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or []

            # Detect format: long format has 'strategy_code', wide format has 'k1_dcg' or similar columns
            is_wide = 'strategy_code' not in fieldnames and any(col.startswith('k1_') for col in fieldnames)
            setting_col = next((c for c in ('fault_id', 'workload', 'ops', 'setting', 'preset') if c in fieldnames), None)

            data_by_setting = {}
            for row in reader:
                fid = row[setting_col] if setting_col else 'default'
                seed = row.get('seed', '0')
                if fid not in data_by_setting:
                    data_by_setting[fid] = {}
                if seed not in data_by_setting[fid]:
                    data_by_setting[fid][seed] = {}

                if is_wide:
                    for strat in strategies:
                        norm_strat = strat.replace('-', '_')
                        k1_val = row.get(f'k1_{strat}', row.get(f'k1_{norm_strat}'))
                        s25_val = row.get(f's25_{strat}', row.get(f's25_{norm_strat}'))
                        data_by_setting[fid][seed][strat] = {
                            'k1_single': k1_val,
                            'k1_failure_mean': k1_val,
                            's25_single': s25_val,
                            's25_mean': s25_val,
                            'total_failures': row.get('total_failures', '1' if any(row.get(f'k1_{st}') for st in ('dcg', 'r2', 'r1', 'b0')) else '0'),
                            'universe_size': row.get('universe_size', '0')
                        }
                else:
                    strat_code = row.get('strategy_code', row.get('strategy', ''))
                    data_by_setting[fid][seed][strat_code] = row

        active_settings = settings if settings else list(data_by_setting.keys())
        for s in active_settings:
            seeds_map = data_by_setting.get(s, {})
            nd = len(seeds_map)
            K_single = {n: [] for n in strategies}
            K_mean = {n: [] for n in strategies}
            S_mean = {n: [] for n in strategies}
            dens = []
            univs = []
            used = 0
            def seed_key(x):
                try:
                    return (0, int(x), "")
                except (ValueError, TypeError):
                    return (1, 0, str(x))
            for seed in sorted(seeds_map.keys(), key=seed_key):
                s_rows = seeds_map[seed]
                if not s_rows:
                    continue
                sample = next(iter(s_rows.values()))
                m = int(sample['total_failures']) if sample.get('total_failures') else 0
                univ = int(sample['universe_size']) if sample.get('universe_size') else 0
                if m == 0 and not is_wide:
                    continue
                elif m == 0 and is_wide:
                    has_positive_k1 = any(r_n.get('k1_single') and float(r_n.get('k1_single')) > 0 for r_n in s_rows.values() if isinstance(r_n, dict))
                    if not has_positive_k1:
                        continue
                    m = 1
                used += 1
                dens.append(m / univ if univ > 0 else 0.0)
                univs.append(univ)
                for n in strategies:
                    r_n = s_rows.get(n, {})
                    val_k1_single = r_n.get('k1_single')
                    try:
                        k1_single = float(val_k1_single) if (val_k1_single is not None and str(val_k1_single).strip() != '' and float(val_k1_single) > 0) else None
                    except ValueError:
                        k1_single = None

                    val_k1_mean = r_n.get('k1_failure_mean') if (r_n.get('k1_failure_mean') is not None and str(r_n.get('k1_failure_mean')).strip() != '') else r_n.get('k1_single')
                    try:
                        k1_mean = float(val_k1_mean) if (val_k1_mean is not None and str(val_k1_mean).strip() != '' and float(val_k1_mean) > 0) else None
                    except ValueError:
                        k1_mean = None

                    val_s25 = r_n.get('s25_mean') if (r_n.get('s25_mean') is not None and str(r_n.get('s25_mean')).strip() != '') else r_n.get('s25_single')
                    try:
                        s25 = float(val_s25) if (val_s25 is not None and str(val_s25).strip() != '') else 0.0
                    except ValueError:
                        s25 = 0.0
                    K_single[n].append(k1_single)
                    K_mean[n].append(k1_mean)
                    S_mean[n].append(s25)
            out.append((s, used, nd, dens, K_single, K_mean, S_mean, univs))
    else:
        # Directory mode
        if os.path.exists(f'{root}/outcomes.json'):
            active_settings = [os.path.basename(os.path.normpath(root))]
            dirs_by_setting = {active_settings[0]: [root]}
        else:
            cand_settings = settings if settings else [os.path.basename(p) for p in sorted(glob.glob(f'{root}/*')) if os.path.isdir(p)]
            active_settings = []
            dirs_by_setting = {}
            for s in cand_settings:
                s_path = f'{root}/{s}'
                if os.path.exists(f'{s_path}/outcomes.json'):
                    active_settings.append(s)
                    dirs_by_setting[s] = [s_path]
                else:
                    subdirs = [d for d in sorted(glob.glob(f'{s_path}/*')) if os.path.exists(f'{d}/outcomes.json')]
                    if subdirs or not settings:
                        active_settings.append(s)
                        dirs_by_setting[s] = subdirs

        for s in active_settings:
            dirs = dirs_by_setting.get(s, [])
            K_single = {n: [] for n in strategies}
            K_mean = {n: [] for n in strategies}
            S_mean = {n: [] for n in strategies}
            dens = []
            univs = []
            used = 0
            for d in dirs:
                if not os.path.exists(f'{d}/outcomes.json') or not os.path.exists(f'{d}/trace.json'):
                    continue
                fail, anchor = load_run(d)
                m = sum(fail.values())
                if m == 0:
                    continue
                used += 1
                u_size = len(fail)
                dens.append(m / u_size if u_size > 0 else 0.0)
                univs.append(u_size)
                for n in strategies:
                    k1, s25 = metrics(rank(d, n), fail, anchor)
                    K_single[n].append(k1)
                    K_mean[n].append(k1)
                    S_mean[n].append(s25)
            out.append((s, used, len(dirs), dens, K_single, K_mean, S_mean, univs))
    return out

def report(settings, root, title, default_impute=DEFAULT_IMPUTATION_UNIVERSE):
    res = analyse(settings, root)
    is_pr_casestudy = any(k in root.lower() for k in ('pebble5825', 'checkpoint', 'ingest', 'dcg-pr', 'partial'))
    print(f'\n##### {title}')
    if is_pr_casestudy:
        print(f"{'setting':14s} {'seeds w/ fail':>13s} {'dens%':>6s} | {'k1 DCG-PR':>9s} {'k1 DCG':>7s} {'k1 R2':>7s} {'k1 R1':>7s} {'k1 B0':>7s} {'k1 Flag':>7s} {'k1 LIFO':>8s} | {'R2/DCG-PR [95% CI]':>22s} | DCG-PR<R2 W/L/T  p(sign)  p(Wilcoxon) | S25 PR/R2")
    else:
        print(f"{'setting':14s} {'seeds w/ fail':>13s} {'dens%':>6s} | {'k1 DCG':>7s} {'k1 R2':>7s} {'k1 R1':>7s} {'k1 B0':>7s} {'k1 Flag':>7s} {'k1 LIFO':>8s} | {'R2/DCG [95% CI]':>22s} | DCG<R2 W/L/T  p(sign)  p(Wilcoxon) | S25 DCG/R2")
    ps = []; rows = []
    for s, used, nd, dens, K_single, K_mean, S_mean, univs in res:
        if used < 8:
            print(f"{s:14s} {used:>7d}/{nd:<5d} (fewer than 8 seeds with any failure: not testable, reported as null)")
            continue
        eval_strat = 'dcg-pr' if is_pr_casestudy else 'dcg'
        # Sign test uses K_single (deterministic single-ranking k1) for reproducibility.
        # K_mean is used for display means and bootstrap CI only.
        p, w, l, t = sign_p_less(K_single.get(eval_strat, []), K_single.get('r2', []))
        pw = wilcoxon_p_less(K_single.get(eval_strat, []), K_single.get('r2', []), univs=univs, default_impute=default_impute)
        lo, hi = boot_ratio(K_mean.get('r2', []), K_mean.get(eval_strat, []), univs=univs, default_impute=default_impute)
        ps.append(p)
        rows.append((s, used, nd, dens, K_single, K_mean, S_mean, p, pw, w, l, t, lo, hi))
    adj = holm(ps) if ps else []
    for (s, used, nd, dens, K_single, K_mean, S_mean, p, pw, w, l, t, lo, hi), pa in zip(rows, adj):
        mean = lambda n: safe_mean(K_mean.get(n, []))
        eval_strat = 'dcg-pr' if is_pr_casestudy else 'dcg'
        m_eval = mean(eval_strat)
        m_r2 = mean('r2')
        if m_eval and not math.isnan(m_eval) and not math.isnan(m_r2) and m_eval > 0:
            ratio_str = f"{m_r2 / m_eval:6.1f}x"
        else:
            ratio_str = "   N/Ax"
        dens_mean = 100 * safe_mean(dens)
        s_eval = safe_mean(S_mean.get('dcg-pr' if is_pr_casestudy else 'dcg', []))
        s_r2 = safe_mean(S_mean.get('r2', []))
        if is_pr_casestudy:
            print(f"{s:14s} {used:>7d}/{nd:<5d} {dens_mean:6.1f} | {mean('dcg-pr'):9.2f} {mean('dcg'):7.2f} {mean('r2'):7.2f} {mean('r1'):7.2f} {mean('b0'):7.2f} {mean('dcg-flag-only'):7.2f} {mean('b2'):8.1f} | {ratio_str} [{lo:5.1f},{hi:6.1f}] | {w:2d}/{l:2d}/{t:2d}   {p:8.2e}  {pw:8.2e} (Holm {pa:.1e}) | {s_eval:.1f}/{s_r2:.1f}")
        else:
            print(f"{s:14s} {used:>7d}/{nd:<5d} {dens_mean:6.1f} | {mean('dcg'):7.2f} {mean('r2'):7.2f} {mean('r1'):7.2f} {mean('b0'):7.2f} {mean('dcg-flag-only'):7.2f} {mean('b2'):8.1f} | {ratio_str} [{lo:5.1f},{hi:6.1f}] | {w:2d}/{l:2d}/{t:2d}   {p:8.2e}  {pw:8.2e} (Holm {pa:.1e}) | {s_eval:.1f}/{s_r2:.1f}")
    # worse-than-baseline flags
    def is_worse(k_a, k_b):
        ma, mb = safe_mean(k_a), safe_mean(k_b)
        return not math.isnan(ma) and not math.isnan(mb) and ma > mb

    target_strat = 'dcg-pr' if is_pr_casestudy else 'dcg'
    target_name = 'DCG-PR' if is_pr_casestudy else 'DCG'
    print(f'\n  settings where {target_name} is WORSE than R2 on average k1:', [r[0] for r in rows if is_worse(r[5].get(target_strat), r[5].get('r2'))] or 'none')
    print(f'  settings where {target_name} is WORSE than uniform random (B0) on average k1:', [r[0] for r in rows if is_worse(r[5].get(target_strat), r[5].get('b0'))] or 'none')
    return rows

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Paired per-seed statistical comparison of candidate rankings.',
        usage='%(prog)s <results.csv|directory> <title> [setting ...] [--default-impute N]',
    )
    parser.add_argument('root', help='Path to per-seed CSV file or exploration directory')
    parser.add_argument('title', help='Section title for the report')
    parser.add_argument('settings', nargs='*', help='Fault IDs to include (default: all in CSV)')
    parser.add_argument('--default-impute', type=float, default=DEFAULT_IMPUTATION_UNIVERSE,
                        help=f'Fallback imputation penalty when universe size is missing (default: {DEFAULT_IMPUTATION_UNIVERSE})')
    args = parser.parse_args()
    report(args.settings, args.root, args.title, default_impute=args.default_impute)
