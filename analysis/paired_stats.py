#!/usr/bin/env python3
"""Paired per-seed statistical comparison of candidate rankings across workload seeds.

Primary metric : k1 = position of first failing crash state in the ranking (lower is better).
Secondary      : S25 = distinct failing persistence states among the first 25 picks.
Design         : each seed is one paired observation (same recorded run, different ranking).
Primary test   : H1 "DCG k1 < R2 k1", one-sided exact sign test per setting, Holm-corrected across settings.
                 Primary summary table aligns means, ratios, CIs, and paired tests on k1_failure_mean
                 (imputed on missed failures); when k1_single differs, a separate single-draw table is printed.
Effect size    : mean(k1_R2)/mean(k1_DCG) with 95% bootstrap CI (resampling seeds).
"""
import argparse
import csv
from dataclasses import dataclass
import hashlib
import json
import math
import os
import posixpath
import random
import statistics as st
from typing import Any, Optional, Sequence

from scipy import stats

MUT_ALWAYS = {'Create', 'Link', 'Lock', 'MkdirAll', 'OpenReadWrite', 'Remove', 'RemoveAll', 'Rename', 'ReuseForWrite'}
BARRIER_ALWAYS = {'Sync', 'SyncData'}


def is_mutation(event: dict[str, Any]) -> bool:
    """Mirror trace.Event.IsMutation semantics for directory-mode anchor computation."""
    kind = event.get('kind')
    has_err = bool(event.get('err'))
    if kind in MUT_ALWAYS:
        return True
    if kind in BARRIER_ALWAYS:
        return not has_err
    if kind == 'SyncTo':
        return (not has_err) and bool(event.get('full_sync', False))
    if kind in ('Write', 'WriteAt'):
        return (event.get('len') or 0) > 0
    if kind == 'Preallocate':
        return (event.get('size') or 0) > 0
    return False


def ld(p: str) -> Any:
    with open(p, 'r', encoding='utf-8') as f:
        return json.load(f)


def is_failure_class(cls: Any) -> bool:
    # Exclude infrastructure errors (oracle_error) from failure counts;
    # failures are actual consistency violations, crashes, or hangs.
    return cls in ('violation', 'open_error', 'panic', 'hang')


def _load_state_keys(d: str) -> Optional[dict[int, str]]:
    """Load optional per-event StateKey map if a graph artifact is present in directory d."""
    gp = os.path.join(d, 'graph.json')
    if not os.path.exists(gp):
        return None
    data = ld(gp)
    if not isinstance(data, dict):
        return None
    cuts = data.get('cuts') or data.get('Cuts') or {}
    if not isinstance(cuts, dict):
        return None
    keys = {}
    for k, feat in cuts.items():
        if isinstance(feat, dict):
            sk = feat.get('state_key') or feat.get('StateKey')
            if sk:
                try:
                    keys[int(k)] = sk
                except (TypeError, ValueError):
                    pass
    return keys or None


def _parent_dir(name: str) -> str:
    d = posixpath.dirname(name)
    if d in ('', '.', '/'):
        return ''
    return d


def _add_interval(spans: list[tuple[int, int]], start: int, end: int) -> list[tuple[int, int]]:
    if end <= start:
        return spans
    merged: list[tuple[int, int]] = []
    inserted = False
    ns, ne = start, end
    for cs, ce in spans:
        if ce < ns:
            merged.append((cs, ce))
        elif ne < cs:
            if not inserted:
                merged.append((ns, ne))
                inserted = True
            merged.append((cs, ce))
        else:
            ns = min(ns, cs)
            ne = max(ne, ce)
    if not inserted:
        merged.append((ns, ne))
    return merged


def _compute_state_keys(events: list[dict[str, Any]]) -> Optional[dict[int, str]]:
    """Compute canonical Go-compatible StateKey per cutoff from trace events.

    Returns None if a synthetic trace lacks valid file-lineage events needed by graph.Build.
    """
    if not events:
        return {0: hashlib.sha256(b'{"resources":[],"pending":[],"unknown":[]}').hexdigest()}

    next_id = 0
    resources: dict[int, dict[str, Any]] = {}
    names: dict[str, int] = {}
    handles: dict[int, int] = {}
    pending: list[dict[str, Any]] = []
    unknown: list[int] = []

    def new_resource(is_dir: bool) -> dict[str, Any]:
        nonlocal next_id
        next_id += 1
        r = {'id': next_id, 'directory': is_dir, 'aliases': set(), 'dirty': []}
        resources[next_id] = r
        return r

    def ensure_dir(name: str) -> int:
        if name in names:
            return names[name]
        r = new_resource(True)
        r['aliases'].add(name)
        names[name] = r['id']
        return r['id']

    def set_name(name: str, rid: int) -> None:
        old = names.get(name)
        if old is not None and old != rid:
            resources[old]['aliases'].discard(name)
        names[name] = rid
        resources[rid]['aliases'].add(name)

    def drop_name(name: str) -> None:
        old = names.pop(name, None)
        if old is not None:
            resources[old]['aliases'].discard(name)

    def add_pending(ev_id: int, name: str, kind: str, rid: int) -> None:
        d = _parent_dir(name)
        pending.append({
            'event': ev_id,
            'dir': d,
            'dir_id': ensure_dir(d),
            'path': name,
            'kind': kind,
            'resource': rid,
        })

    def create_res(ev_id: int, name: str) -> int:
        kind = 'replace' if name in names else 'create'
        r = new_resource(False)
        set_name(name, r['id'])
        add_pending(ev_id, name, kind, r['id'])
        return r['id']

    def rename_names(src: str, dst: str) -> None:
        moves = []
        prefix = src + '/'
        for nm in list(names.keys()):
            if nm == src:
                moves.append((nm, dst))
            elif nm.startswith(prefix):
                moves.append((nm, dst + nm[len(src):]))
        moves.sort(key=lambda item: item[0])
        ids = []
        for old_name, _ in moves:
            ids.append(names[old_name])
            drop_name(old_name)
        for (_, new_name), rid in zip(moves, ids):
            set_name(new_name, rid)

    def current_state_key() -> str:
        included: set[int] = {rid for rid, r in resources.items() if r['dirty']}
        pend_list = []
        for entry in pending:
            included.add(entry['resource'])
            pend_list.append({
                'event': entry['event'],
                'dir': entry['dir'],
                'path': entry['path'],
                'kind': entry['kind'],
                'resource': entry['resource'],
            })
        pend_list.sort(key=lambda p: (p['event'], p['dir'], p['path'], p['kind']))
        res_list = []
        for rid in sorted(included):
            r = resources[rid]
            res_list.append({
                'id': rid,
                'aliases': sorted(r['aliases']),
                'dirty': [[s, e] for s, e in r['dirty']],
            })
        state = {
            'resources': res_list,
            'pending': pend_list,
            'unknown': sorted(unknown),
        }
        encoded = json.dumps(state, separators=(',', ':'), ensure_ascii=False).encode('utf-8')
        return hashlib.sha256(encoded).hexdigest()

    keys: dict[int, str] = {0: current_state_key()}
    try:
        for ev in events:
            ev_id = int(ev['id'])
            kind = ev.get('kind', '')
            err = ev.get('err', '') or ''
            file_name = ev.get('file', '') or ''
            file2 = ev.get('file2', '') or ''
            handle = ev.get('handle') or 0

            if kind in ('OpBegin', 'OpEnd', 'EngineEvent', 'Read'):
                pass
            elif kind in ('Create', 'Lock'):
                if err:
                    unknown.append(ev_id)
                else:
                    rid = create_res(ev_id, file_name)
                    handles[handle] = rid
                    ensure_dir(_parent_dir(file_name))
            elif kind == 'OpenReadWrite':
                if err:
                    unknown.append(ev_id)
                else:
                    if ev.get('created'):
                        rid = create_res(ev_id, file_name)
                        ensure_dir(_parent_dir(file_name))
                    else:
                        if file_name not in names:
                            return None
                        rid = names[file_name]
                    handles[handle] = rid
            elif kind == 'ReuseForWrite':
                if err:
                    unknown.append(ev_id)
                else:
                    if file_name not in names:
                        return None
                    rid = names[file_name]
                    resources[rid]['dirty'] = []
                    rename_names(file_name, file2)
                    add_pending(ev_id, file_name, 'rename_from', rid)
                    add_pending(ev_id, file2, 'rename_to', rid)
                    handles[handle] = rid
                    ensure_dir(_parent_dir(file_name))
                    ensure_dir(_parent_dir(file2))
            elif kind in ('Open', 'OpenDir'):
                if not err:
                    if ev.get('dir'):
                        rid = ensure_dir(file_name)
                    else:
                        if file_name not in names:
                            return None
                        rid = names[file_name]
                    handles[handle] = rid
            elif kind == 'Link':
                if err:
                    unknown.append(ev_id)
                else:
                    if file_name not in names:
                        return None
                    rid = names[file_name]
                    set_name(file2, rid)
                    add_pending(ev_id, file2, 'link', rid)
                    ensure_dir(_parent_dir(file2))
            elif kind == 'Rename':
                if err:
                    unknown.append(ev_id)
                else:
                    if file_name not in names:
                        return None
                    rid = names[file_name]
                    rename_names(file_name, file2)
                    add_pending(ev_id, file_name, 'rename_from', rid)
                    add_pending(ev_id, file2, 'rename_to', rid)
                    ensure_dir(_parent_dir(file_name))
                    ensure_dir(_parent_dir(file2))
            elif kind == 'Remove':
                if err:
                    unknown.append(ev_id)
                else:
                    if file_name not in names:
                        return None
                    rid = names[file_name]
                    drop_name(file_name)
                    add_pending(ev_id, file_name, 'remove', rid)
                    ensure_dir(_parent_dir(file_name))
            elif kind == 'RemoveAll':
                if err:
                    unknown.append(ev_id)
                else:
                    is_root = file_name in ('', '.')
                    prefix = file_name.rstrip('/') + '/'
                    removed = sorted(
                        nm for nm in names
                        if is_root or nm == file_name or nm.startswith(prefix)
                    )
                    parent_d = '' if is_root else _parent_dir(file_name)
                    ensure_dir(parent_d)
                    for nm in removed:
                        rid = names[nm]
                        drop_name(nm)
                        add_pending(ev_id, nm, 'remove', rid)
                        ensure_dir(_parent_dir(nm))
            elif kind == 'MkdirAll':
                if err:
                    unknown.append(ev_id)
                else:
                    ensure_dir('')
                    if file_name:
                        parts = file_name.split('/')
                        for idx in range(len(parts)):
                            sub = '/'.join(parts[:idx + 1])
                            if sub not in names:
                                r = new_resource(True)
                                set_name(sub, r['id'])
                                add_pending(ev_id, sub, 'mkdir', r['id'])
                                ensure_dir(_parent_dir(sub))
            elif kind in ('Write', 'WriteAt'):
                length = int(ev.get('len') or 0)
                if length > 0 and handle in handles and handles[handle] != 0:
                    off = int(ev.get('off') or 0)
                    r = resources.get(handles[handle])
                    if r is not None:
                        r['dirty'] = _add_interval(r['dirty'], off, off + length)
            elif kind in ('Sync', 'SyncData', 'SyncTo'):
                if err:
                    return None
                is_barrier = (kind in ('Sync', 'SyncData')) or bool(ev.get('full_sync', False))
                if is_barrier and handle in handles and handles[handle] != 0:
                    r = resources.get(handles[handle])
                    if r is not None:
                        if r['directory']:
                            pending = [entry for entry in pending if entry['dir_id'] != r['id']]
                        else:
                            r['dirty'] = []
            elif kind in ('Close', 'Unlock'):
                handles.pop(handle, None)
            elif kind == 'Preallocate':
                if err:
                    unknown.append(ev_id)
            else:
                return None

            keys[ev_id] = current_state_key()
    except (KeyError, TypeError, ValueError):
        return None
    return keys


def load_run(d: str) -> tuple[dict[tuple[Any, Any, Any], bool], dict[int, int], Optional[dict[int, str]]]:
    o_doc = ld(os.path.join(d, 'outcomes.json'))
    o = (o_doc.get('outcomes') if isinstance(o_doc, dict) else None) or []
    fail = {(x['candidate']['event'], x['candidate']['percent'], x['candidate'].get('seed', 0)): is_failure_class(x['class']) for x in o}
    t_doc = ld(os.path.join(d, 'trace.json'))
    t = (t_doc.get('events') if isinstance(t_doc, dict) else None) or []
    anchor, last = {0: 0}, 0
    for e in t:
        if is_mutation(e):
            last = e['id']
        anchor[e['id']] = last
    state_keys = _load_state_keys(d)
    if state_keys is None:
        state_keys = _compute_state_keys(t)
    return fail, anchor, state_keys


def rank(d: str, n: str) -> list[tuple[Any, Any, Any]]:
    p = os.path.join(d, f'ranking_{n}.json')
    if not os.path.exists(p):
        return []
    data = ld(p)
    cands = (data.get('candidates') if isinstance(data, dict) else None) or []
    return [(c['event'], c['percent'], c.get('seed', 0)) for c in cands]


def metrics(order, fail, anchor, state_keys=None):
    k1 = next((i + 1 for i, c in enumerate(order) if fail.get(c, False)), None)
    s25 = len({
        ((state_keys[c[0]] if (state_keys and c[0] in state_keys) else anchor.get(c[0], 0)), c[1])
        for c in order[:25]
        if fail.get(c, False)
    })
    return k1, s25


DEFAULT_IMPUTATION_UNIVERSE = 1000  # Conservative upper bound penalty on rank when candidate universe size is unknown


def _is_nan(x: Any) -> bool:
    if x is None or isinstance(x, bool):
        return False
    try:
        return math.isnan(float(x))
    except (TypeError, ValueError):
        return False


def _is_finite_num(x: Any) -> bool:
    if x is None or isinstance(x, bool):
        return False
    try:
        return math.isfinite(float(x))
    except (TypeError, ValueError):
        return False


def _validate_positive_rank(val: Any) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, bool):
        raise ValueError(f"rank value cannot be bool: {val!r}")
    fv = float(val)
    if math.isnan(fv):
        raise ValueError(f"rank value cannot be NaN: {val!r}")
    if fv <= 0:
        raise ValueError(f"rank value must be > 0, got {val!r}")
    return fv


def impute_rank(val, univ=None, default_impute=DEFAULT_IMPUTATION_UNIVERSE) -> float:
    """Impute missing rank (where no bug was found) with universe size or default penalty.

    When a strategy fails to find any bug in a run, its rank is missing (None).
    To avoid survival bias, missing values are imputed using the candidate universe
    size for that seed (or default_impute if universe size is unknown or non-positive).
    """
    if default_impute is None or not _is_finite_num(default_impute) or float(default_impute) <= 0:
        raise ValueError(f"default_impute must be a finite positive number, got {default_impute}")
    if val is not None:
        fv = _validate_positive_rank(val)
        assert fv is not None
        return fv
    if univ is not None and _is_finite_num(univ) and float(univ) > 0:
        return float(univ)
    return float(default_impute)


def _univ_at(univs: Optional[Sequence[Any]], idx: int) -> Any:
    return univs[idx] if (univs and idx < len(univs)) else None


def boot_ratio(a, b, univs=None, n=10000, seed=1, default_impute=DEFAULT_IMPUTATION_UNIVERSE):
    """CI for mean(a)/mean(b) resampling paired seeds.
    Missing values (None) are imputed using candidate universe size so failed seeds are not dropped.
    """
    if isinstance(n, bool) or not isinstance(n, int) or n <= 0:
        raise ValueError(f"bootstrap resamples n must be a positive integer, got {n!r}")
    if not a or not b or len(a) != len(b):
        return float('nan'), float('nan')
    m = len(a)
    for x in a:
        if isinstance(x, bool):
            raise ValueError(f"rank value cannot be bool: {x!r}")
    for x in b:
        if isinstance(x, bool):
            raise ValueError(f"rank value cannot be bool: {x!r}")
    if any(_is_nan(x) or (x is not None and (not math.isfinite(float(x)) or float(x) < 0)) for x in a):
        return float('nan'), float('nan')
    if any(_is_nan(x) or (x is not None and (not math.isfinite(float(x)) or float(x) <= 0)) for x in b):
        return float('nan'), float('nan')
    if m < 2:
        return float('nan'), float('nan')
    a_f = [
        (0.0 if (a[i] is not None and float(a[i]) == 0.0) else impute_rank(a[i], _univ_at(univs, i), default_impute))
        for i in range(m)
    ]
    b_f = [impute_rank(b[i], _univ_at(univs, i), default_impute) for i in range(m)]
    r = random.Random(seed)
    vals = []
    for _ in range(n):
        idx = [r.randrange(m) for _ in range(m)]
        mb = sum(b_f[i] for i in idx)
        ma = sum(a_f[i] for i in idx)
        vals.append(ma / mb)
    vals.sort()
    lo_idx = max(0, min(len(vals) - 1, int(round(0.025 * (len(vals) - 1)))))
    hi_idx = max(0, min(len(vals) - 1, int(round(0.975 * (len(vals) - 1)))))
    return vals[lo_idx], vals[hi_idx]


def sign_p_less(x, y):
    """one-sided exact sign test p for H1: x < y (paired); ties dropped.
    None represents failure to find a bug (imputed as infinity > any valid rank).
    """
    if not x or not y or len(x) != len(y):
        return float('nan'), 0, 0, 0
    for a in x:
        if isinstance(a, bool):
            raise ValueError(f"rank value cannot be bool: {a!r}")
        if a is not None and not _is_nan(a) and float(a) <= 0:
            raise ValueError(f"rank value must be > 0, got {a!r}")
    for b in y:
        if isinstance(b, bool):
            raise ValueError(f"rank value cannot be bool: {b!r}")
        if b is not None and not _is_nan(b) and float(b) <= 0:
            raise ValueError(f"rank value must be > 0, got {b!r}")
    w = 0
    l = 0
    t = 0
    for a, b in zip(x, y):
        if _is_nan(a) or _is_nan(b):
            continue
        if a is None and b is None:
            t += 1
        elif a is None and b is not None:
            l += 1
        elif a is not None and b is None:
            w += 1
        else:
            fa, fb = float(a), float(b)
            if fa < fb:
                w += 1
            elif fa > fb:
                l += 1
            else:
                t += 1
    if (w + l + t) == 0:
        return float('nan'), 0, 0, 0
    n = w + l
    if n == 0:
        return 1.0, w, l, t
    return float(stats.binomtest(w, n, 0.5, alternative='greater').pvalue), w, l, t


def wilcoxon_p_less(x, y, univs=None, default_impute=DEFAULT_IMPUTATION_UNIVERSE):
    """One-sided Wilcoxon signed-rank test for paired ranks x and y (H1: x < y).
    Missing ranks are imputed with universe size to avoid survival bias.
    Note: sign_p_less is the primary distribution-free test when imputation induces asymmetric differences.
    """
    if not x or not y or len(x) != len(y):
        return float('nan')
    m = len(x)
    for i in range(m):
        if isinstance(x[i], bool) or isinstance(y[i], bool):
            raise ValueError("rank value cannot be bool")
        if _is_nan(x[i]) or _is_nan(y[i]):
            return float('nan')
    d = []
    for i in range(m):
        xi = float(x[i]) if x[i] is not None else None
        yi = float(y[i]) if y[i] is not None else None
        if xi is not None and math.isinf(xi) and yi is not None and math.isinf(yi) and xi == yi:
            continue
        if (xi is not None and math.isinf(xi)) or (yi is not None and math.isinf(yi)):
            return float('nan')
        vx = impute_rank(x[i], _univ_at(univs, i), default_impute)
        vy = impute_rank(y[i], _univ_at(univs, i), default_impute)
        if vx == vy:
            continue
        d.append(vx - vy)
    if len(d) < 5:
        return float('nan')
    has_ties = len({abs(v) for v in d}) < len(d)
    try:
        if not has_ties and len(d) <= 50:
            return float(stats.wilcoxon(d, alternative='less', method='exact').pvalue)
        return float(stats.wilcoxon(
            d,
            alternative='less',
            method=stats.PermutationMethod(n_resamples=19999, rng=1),
        ).pvalue)
    except ValueError:
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

    m_total = len(pvals)
    finite_indices = []
    adjusted = [float('nan')] * m_total

    for i, p in enumerate(pvals):
        if p is None or _is_nan(p):
            continue
        if isinstance(p, bool):
            raise ValueError(f"p-value cannot be bool: {p}")
        fp = float(p)
        if not math.isfinite(fp) or fp < 0.0 or fp > 1.0:
            raise ValueError(f"p-value out of range [0, 1]: {p}")
        finite_indices.append(i)

    m = len(finite_indices)
    if m == 0:
        return adjusted

    # Order finite p-values ascending by value
    sorted_indices = sorted(finite_indices, key=lambda i: float(pvals[i]))

    running_max = 0.0
    for rank_idx, orig_idx in enumerate(sorted_indices):
        # Step-down multiplier: (m - rank_idx)
        multiplier = m - rank_idx
        val = multiplier * float(pvals[orig_idx])
        running_max = max(running_max, val)
        adjusted[orig_idx] = min(1.0, running_max)

    return adjusted


def safe_mean(seq):
    vals = [float(x) for x in seq if x is not None and _is_finite_num(x)] if seq else []
    return st.mean(vals) if vals else float('nan')


def imputed_mean(seq, univs=None, default_impute=DEFAULT_IMPUTATION_UNIVERSE):
    """Mean after imputing None with candidate universe size (matches boot_ratio estimand)."""
    if not seq:
        return float('nan')
    vals = [impute_rank(v, _univ_at(univs, i), default_impute) for i, v in enumerate(seq)]
    return st.mean(vals) if vals else float('nan')


def _is_positive_num(val):
    if val is None:
        return False
    s = str(val).strip()
    if s == '':
        return False
    try:
        v = float(s)
        return math.isfinite(v) and v > 0
    except ValueError:
        return False


def _parse_int_field(val: Any) -> int:
    s = str(val).strip()
    try:
        return int(s)
    except ValueError:
        fv = float(s)
        if math.isfinite(fv) and fv.is_integer():
            return int(fv)
        raise ValueError(f"invalid integer field: {val!r}")


def _detect_pr_casestudy(root, prefer_pr=None, has_partial_policies=None):
    if prefer_pr is not None:
        return bool(prefer_pr)
    if has_partial_policies is not None:
        return bool(has_partial_policies)
    base = os.path.basename(os.path.normpath(root)).lower()
    return any(k in base for k in ('pebble5825', 'checkpoint', 'dcg-pr', 'partial-retention', 'partial_retention'))


def _fmt_float(val, width, prec):
    if val is None or _is_nan(val):
        return f"{'N/A':>{width}s}"
    fv = float(val)
    if not math.isfinite(fv):
        return f"{('inf' if fv > 0 else '-inf'):>{width}s}"
    return f"{fv:{width}.{prec}f}"


def _parse_pos_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    s = str(val).strip()
    if s == '':
        return None
    try:
        fv = float(s)
    except ValueError:
        return None
    if not math.isfinite(fv) or fv <= 0:
        return None
    return fv


def _parse_finite_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    s = str(val).strip()
    if s == '':
        return None
    try:
        fv = float(s)
    except ValueError:
        return None
    if not math.isfinite(fv):
        return None
    return fv


def _first_nonempty(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        val = row.get(key)
        if val is not None and str(val).strip() != '':
            return val
    return None


def _has_any_data(seq: Sequence[Any]) -> bool:
    return any(x is not None for x in seq)


class _AnalyseTuple(tuple):
    """8-tuple compatible with legacy callers while carrying observation masks and policy metadata."""
    def __new__(cls, items, observed: dict[str, list[bool]], has_partial_policies: Optional[bool] = None):
        obj = super().__new__(cls, items)
        obj.observed = observed
        obj.has_partial_policies = has_partial_policies
        return obj


def _policies_have_partial(raw_policies: Any) -> Optional[bool]:
    if raw_policies is None:
        return None
    if isinstance(raw_policies, str):
        s = raw_policies.strip()
        if not s:
            return None
        parts = [p.strip() for p in s.replace('[', '').replace(']', '').replace(';', ',').split(',') if p.strip()]
        vals = []
        for part in parts:
            try:
                vals.append(int(part))
            except ValueError:
                return None
        return any(0 < p < 100 for p in vals) if vals else None
    if isinstance(raw_policies, (list, tuple)):
        vals = [int(p) for p in raw_policies if isinstance(p, (int, float)) and not isinstance(p, bool)]
        return any(0 < p < 100 for p in vals) if vals else None
    return None


def analyse(settings, root, strategies=('dcg', 'dcg-pr', 'dcg-flag-only', 'r2', 'r1', 'b0', 'b2'), baselines_for_tests=('r2', 'r1', 'b0')):
    del baselines_for_tests  # Handled by report(); kept here for signature compatibility.
    out = []
    if root.lower().endswith('.csv') or (os.path.exists(root) and not os.path.isdir(root)):
        with open(root, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or []
            if 'seed' not in fieldnames:
                raise ValueError(f"CSV missing required 'seed' column: {root}")

            is_wide = (
                'strategy_code' not in fieldnames
                and 'strategy' not in fieldnames
                and any(col.startswith('k1_') for col in fieldnames)
            )
            setting_cols = [c for c in ('fault_id', 'workload', 'ops', 'setting', 'preset') if c in fieldnames]
            has_total_failures_col = 'total_failures' in fieldnames
            has_partial_policies: Optional[bool] = None

            data_by_setting: dict[str, dict[str, dict[str, dict[str, Any]]]] = {}
            for line_num, row in enumerate(reader, start=2):
                if None in row:
                    raise ValueError(f"ragged CSV row at {root}:{line_num} (extra columns)")
                for col in fieldnames:
                    if row.get(col) is None:
                        raise ValueError(f"ragged CSV row at {root}:{line_num} (missing column {col!r})")
                if setting_cols:
                    parts = [str(row[c]).strip() for c in setting_cols]
                    if any(p == '' for p in parts):
                        raise ValueError(f"empty setting column at {root}:{line_num}")
                    fid = '/'.join(parts) if len(parts) > 1 else parts[0]
                else:
                    fid = 'default'
                seed = str(row['seed']).strip()
                if not seed:
                    raise ValueError(f"empty 'seed' at {root}:{line_num}")

                if 'policies' in fieldnames:
                    pp = _policies_have_partial(row.get('policies'))
                    if pp is not None:
                        has_partial_policies = bool(has_partial_policies or pp)

                if fid not in data_by_setting:
                    data_by_setting[fid] = {}
                if seed not in data_by_setting[fid]:
                    data_by_setting[fid][seed] = {}

                if is_wide:
                    if data_by_setting[fid][seed]:
                        raise ValueError(f"duplicate wide CSV row for ({fid!r}, seed={seed!r}) at {root}:{line_num}")
                    raw_tf = row.get('total_failures')
                    tf_val = raw_tf if (has_total_failures_col and str(raw_tf).strip() != '') else None
                    for strat in strategies:
                        norm_strat = strat.replace('-', '_')
                        k1_col = f'k1_{strat}'
                        k1_norm_col = f'k1_{norm_strat}'
                        if k1_col not in fieldnames and k1_norm_col not in fieldnames:
                            continue
                        k1_val = _first_nonempty(row, k1_col, k1_norm_col)
                        s25_val = _first_nonempty(row, f's25_{strat}', f's25_{norm_strat}')
                        data_by_setting[fid][seed][strat] = {
                            'k1_single': k1_val,
                            'k1_failure_mean': k1_val,
                            's25_single': s25_val,
                            's25_mean': s25_val,
                            'total_failures': tf_val,
                            'universe_size': row.get('universe_size', '0'),
                        }
                else:
                    strat_code = _first_nonempty(row, 'strategy_code', 'strategy')
                    if strat_code is None:
                        raise ValueError(f"missing strategy code at {root}:{line_num}")
                    strat_code_str = str(strat_code).strip()
                    if strat_code_str in data_by_setting[fid][seed]:
                        raise ValueError(f"duplicate CSV row for ({fid!r}, seed={seed!r}, strategy={strat_code_str!r}) at {root}:{line_num}")
                    data_by_setting[fid][seed][strat_code_str] = row

        if settings:
            missing_settings = [s for s in settings if s not in data_by_setting]
            if missing_settings:
                raise ValueError(f"requested setting(s) not found in {root}: {missing_settings}")
        active_settings = list(settings) if settings else list(data_by_setting.keys())
        for s in active_settings:
            seeds_map = data_by_setting.get(s, {})
            nd = len(seeds_map)
            K_single = {n: [] for n in strategies}
            K_mean = {n: [] for n in strategies}
            S_mean = {n: [] for n in strategies}
            observed = {n: [] for n in strategies}
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
                raw_tf = sample.get('total_failures')
                m = None
                if raw_tf is not None and str(raw_tf).strip() != '':
                    m = _parse_int_field(raw_tf)
                    if m < 0:
                        raise ValueError(f"negative total_failures {m} for ({s!r}, seed={seed!r})")
                raw_univ = sample.get('universe_size')
                univ = _parse_int_field(raw_univ) if (raw_univ is not None and str(raw_univ).strip() != '') else 0
                if univ < 0:
                    raise ValueError(f"negative universe_size {univ} for ({s!r}, seed={seed!r})")

                if m is not None:
                    if m == 0:
                        continue
                elif is_wide:
                    has_positive_k1 = any(
                        _is_positive_num(r_n.get('k1_single'))
                        for r_n in s_rows.values() if isinstance(r_n, dict)
                    )
                    if not has_positive_k1:
                        continue
                else:
                    continue

                used += 1
                dens.append((m / univ) if (m is not None and univ > 0) else float('nan'))
                univs.append(univ)
                for n in strategies:
                    r_n = s_rows.get(n)
                    if not isinstance(r_n, dict):
                        K_single[n].append(None)
                        K_mean[n].append(None)
                        S_mean[n].append(None)
                        observed[n].append(False)
                        continue
                    k1_single = _parse_pos_float(r_n.get('k1_single'))
                    val_k1_mean = _first_nonempty(r_n, 'k1_failure_mean', 'k1_single')
                    k1_mean = _parse_pos_float(val_k1_mean)
                    val_s25 = _first_nonempty(r_n, 's25_mean', 's25_single')
                    s25 = _parse_finite_float(val_s25)
                    K_single[n].append(k1_single)
                    K_mean[n].append(k1_mean)
                    S_mean[n].append(s25)
                    observed[n].append(True)
            out.append(_AnalyseTuple(
                (s, used, nd, dens, K_single, K_mean, S_mean, univs),
                observed=observed,
                has_partial_policies=has_partial_policies,
            ))
    else:
        # Directory mode
        if not os.path.exists(root):
            raise FileNotFoundError(f"exploration directory not found: {root}")
        has_partial_policies = None
        if os.path.exists(os.path.join(root, 'outcomes.json')):
            active_settings = [os.path.basename(os.path.normpath(root))]
            dirs_by_setting = {active_settings[0]: [root]}
        else:
            with os.scandir(root) as it:
                all_dirs = sorted(entry.name for entry in it if entry.is_dir())
            cand_settings = list(settings) if settings else all_dirs
            active_settings = []
            dirs_by_setting = {}
            for s in cand_settings:
                s_path = os.path.join(root, s)
                if os.path.exists(os.path.join(s_path, 'outcomes.json')):
                    active_settings.append(s)
                    dirs_by_setting[s] = [s_path]
                elif os.path.isdir(s_path):
                    with os.scandir(s_path) as sub_it:
                        subdirs = [
                            entry.path for entry in sorted(sub_it, key=lambda e: e.name)
                            if entry.is_dir() and os.path.exists(os.path.join(entry.path, 'outcomes.json'))
                        ]
                    if subdirs or not settings:
                        active_settings.append(s)
                        dirs_by_setting[s] = subdirs
            if settings:
                missing_settings = [s for s in settings if s not in dirs_by_setting]
                if missing_settings:
                    raise ValueError(f"requested setting(s) not found in {root}: {missing_settings}")

        for s in active_settings:
            dirs = dirs_by_setting.get(s, [])
            K_single = {n: [] for n in strategies}
            K_mean = {n: [] for n in strategies}
            S_mean = {n: [] for n in strategies}
            observed = {n: [] for n in strategies}
            dens = []
            univs = []
            used = 0
            valid_nd = 0
            for d in dirs:
                if not os.path.exists(os.path.join(d, 'outcomes.json')) or not os.path.exists(os.path.join(d, 'trace.json')):
                    continue
                valid_nd += 1
                run_json = os.path.join(d, 'run.json')
                if os.path.exists(run_json):
                    rj = ld(run_json)
                    if isinstance(rj, dict):
                        pp = _policies_have_partial(rj.get('policies'))
                        if pp is not None:
                            has_partial_policies = bool(has_partial_policies or pp)
                fail, anchor, state_keys = load_run(d)
                m = sum(fail.values())
                if m == 0:
                    continue
                used += 1
                u_size = len(fail)
                dens.append(m / u_size if u_size > 0 else 0.0)
                univs.append(u_size)
                for n in strategies:
                    if not os.path.exists(os.path.join(d, f'ranking_{n}.json')):
                        K_single[n].append(None)
                        K_mean[n].append(None)
                        S_mean[n].append(None)
                        observed[n].append(False)
                        continue
                    k1, s25 = metrics(rank(d, n), fail, anchor, state_keys=state_keys)
                    K_single[n].append(k1)
                    K_mean[n].append(k1)
                    S_mean[n].append(s25)
                    observed[n].append(True)
            out.append(_AnalyseTuple(
                (s, used, valid_nd, dens, K_single, K_mean, S_mean, univs),
                observed=observed,
                has_partial_policies=has_partial_policies,
            ))
    return out


def _strategy_recorded(
    K_single: dict[str, list[Any]],
    K_mean: dict[str, list[Any]],
    S_mean: dict[str, list[Any]],
    name: str,
    observed: Optional[dict[str, list[bool]]] = None,
) -> bool:
    if observed is not None and name in observed:
        return any(observed[name])
    return _has_any_data(K_single.get(name, [])) or _has_any_data(K_mean.get(name, [])) or _has_any_data(S_mean.get(name, []))


def _fmt_pval(val: Optional[float], width: int = 8) -> str:
    if val is None or _is_nan(val):
        return f"{'N/A':>{width}s}"
    return f"{float(val):{width}.2e}"


@dataclass
class ReportRow:
    setting: str
    used: int
    nd: int
    dens: list[float]
    K_single: dict[str, list[Any]]
    K_mean: dict[str, list[Any]]
    S_mean: dict[str, list[Any]]
    p_sign: float
    p_wilcoxon: float
    w: int
    l: int
    t: int
    ci_lo: float
    ci_hi: float
    univs: list[int]
    observed: dict[str, list[bool]]
    has_eval: bool
    has_r2: bool
    p_sign_single: float
    p_wilcoxon_single: float
    w_single: int
    l_single: int
    t_single: int


def _paired_observed_subset(
    a: list[Any],
    b: list[Any],
    univs: list[int],
    obs_a: list[bool],
    obs_b: list[bool],
) -> tuple[list[Any], list[Any], list[int]]:
    xa, xb, xu = [], [], []
    for i in range(min(len(a), len(b), len(obs_a), len(obs_b))):
        if obs_a[i] and obs_b[i]:
            xa.append(a[i])
            xb.append(b[i])
            if i < len(univs):
                xu.append(univs[i])
    return xa, xb, xu


def _observed_subset(seq: list[Any], univs: list[int], obs: list[bool]) -> tuple[list[Any], list[int]]:
    xs, xu = [], []
    for i in range(min(len(seq), len(obs))):
        if obs[i]:
            xs.append(seq[i])
            if i < len(univs):
                xu.append(univs[i])
    return xs, xu


def report(
    settings,
    root,
    title,
    default_impute=DEFAULT_IMPUTATION_UNIVERSE,
    prefer_pr=None,
    baselines_for_tests=('r2', 'r1', 'b0'),
):
    if default_impute is None or not _is_finite_num(default_impute) or float(default_impute) <= 0:
        raise ValueError(f"default_impute must be a finite positive number, got {default_impute}")
    res = analyse(settings, root)
    has_partial_policies = next((r.has_partial_policies for r in res if isinstance(r, _AnalyseTuple) and r.has_partial_policies is not None), None)
    is_pr_casestudy = _detect_pr_casestudy(root, prefer_pr=prefer_pr, has_partial_policies=has_partial_policies)
    eval_strat = 'dcg-pr' if is_pr_casestudy else 'dcg'
    target_name = 'DCG-PR' if is_pr_casestudy else 'DCG'
    print(f'\n##### {title}')
    if is_pr_casestudy:
        print(f"{'setting':14s} {'seeds w/ fail':>13s} {'dens%':>6s} | {'k1 DCG-PR':>9s} {'k1 DCG':>7s} {'k1 R2':>7s} {'k1 R1':>7s} {'k1 B0':>7s} {'k1 Flag':>7s} {'k1 LIFO':>8s} | {'R2/DCG-PR [95% CI]':>22s} | DCG-PR<R2 W/L/T  p(sign) [Holm]  p(Wilcoxon) [Holm] | S25 PR/R2")
    else:
        print(f"{'setting':14s} {'seeds w/ fail':>13s} {'dens%':>6s} | {'k1 DCG':>7s} {'k1 R2':>7s} {'k1 R1':>7s} {'k1 B0':>7s} {'k1 Flag':>7s} {'k1 LIFO':>8s} | {'R2/DCG [95% CI]':>22s} | DCG<R2 W/L/T  p(sign) [Holm]  p(Wilcoxon) [Holm] | S25 DCG/R2")

    ps_holm = []
    pws_holm = []
    ps_single_holm = []
    pws_single_holm = []
    rows: list[ReportRow] = []
    has_distinct_single = False

    for item in res:
        s, used, nd, dens, K_single, K_mean, S_mean, univs = item
        observed = item.observed if isinstance(item, _AnalyseTuple) else {
            k: [True] * len(v) for k, v in K_mean.items()
        }
        if used < 8:
            print(f"{s:14s} {used:>7d}/{nd:<5d} (fewer than 8 seeds with any failure: not testable, reported as null)")
            continue
        has_eval = _strategy_recorded(K_single, K_mean, S_mean, eval_strat, observed)
        has_r2 = _strategy_recorded(K_single, K_mean, S_mean, 'r2', observed)
        if has_eval and has_r2:
            obs_eval = observed.get(eval_strat, [])
            obs_r2 = observed.get('r2', [])
            xm_eval, xm_r2, xm_univs = _paired_observed_subset(
                K_mean.get(eval_strat, []), K_mean.get('r2', []), univs, obs_eval, obs_r2
            )
            xs_eval, xs_r2, xs_univs = _paired_observed_subset(
                K_single.get(eval_strat, []), K_single.get('r2', []), univs, obs_eval, obs_r2
            )
            if xm_eval != xs_eval or xm_r2 != xs_r2:
                has_distinct_single = True

            # Primary table aligns all metrics and paired tests on the imputed k1_failure_mean estimand.
            xm_eval_imp = [impute_rank(v, _univ_at(xm_univs, i), default_impute) for i, v in enumerate(xm_eval)]
            xm_r2_imp = [impute_rank(v, _univ_at(xm_univs, i), default_impute) for i, v in enumerate(xm_r2)]
            p, w, l, t = sign_p_less(xm_eval_imp, xm_r2_imp)
            pw = wilcoxon_p_less(xm_eval, xm_r2, univs=xm_univs, default_impute=default_impute)
            lo, hi = boot_ratio(xm_r2, xm_eval, univs=xm_univs, default_impute=default_impute)

            p_s, w_s, l_s, t_s = sign_p_less(xs_eval, xs_r2)
            pw_s = wilcoxon_p_less(xs_eval, xs_r2, univs=xs_univs, default_impute=default_impute)

            # Testable settings with insufficient non-zero differences (<5, e.g. high tie rate)
            # are non-significant evidence against H1 and must remain in the Holm family size m as 1.0.
            ps_holm.append(1.0 if math.isnan(p) else p)
            pws_holm.append(1.0 if math.isnan(pw) else pw)
            ps_single_holm.append(1.0 if math.isnan(p_s) else p_s)
            pws_single_holm.append(1.0 if math.isnan(pw_s) else pw_s)
        else:
            p, w, l, t = float('nan'), 0, 0, 0
            pw = float('nan')
            lo, hi = float('nan'), float('nan')
            p_s, w_s, l_s, t_s = float('nan'), 0, 0, 0
            pw_s = float('nan')
            ps_holm.append(float('nan'))
            pws_holm.append(float('nan'))
            ps_single_holm.append(float('nan'))
            pws_single_holm.append(float('nan'))

        rows.append(ReportRow(
            setting=s, used=used, nd=nd, dens=dens,
            K_single=K_single, K_mean=K_mean, S_mean=S_mean,
            p_sign=p, p_wilcoxon=pw, w=w, l=l, t=t, ci_lo=lo, ci_hi=hi,
            univs=univs, observed=observed, has_eval=has_eval, has_r2=has_r2,
            p_sign_single=p_s, p_wilcoxon_single=pw_s,
            w_single=w_s, l_single=l_s, t_single=t_s,
        ))

    adj = holm(ps_holm) if ps_holm else []
    adj_w = holm(pws_holm) if pws_holm else []
    for r, pa, paw in zip(rows, adj, adj_w):
        def imp_mean(name: str) -> float:
            vals, sub_univs = _observed_subset(r.K_mean.get(name, []), r.univs, r.observed.get(name, []))
            return imputed_mean(vals, univs=sub_univs, default_impute=default_impute)

        if r.has_eval and r.has_r2:
            obs_eval = r.observed.get(eval_strat, [])
            obs_r2 = r.observed.get('r2', [])
            xm_eval, xm_r2, xm_univs = _paired_observed_subset(
                r.K_mean.get(eval_strat, []), r.K_mean.get('r2', []), r.univs, obs_eval, obs_r2
            )
            m_eval = imputed_mean(xm_eval, univs=xm_univs, default_impute=default_impute)
            m_r2 = imputed_mean(xm_r2, univs=xm_univs, default_impute=default_impute)
            if m_eval and not math.isnan(m_eval) and not math.isnan(m_r2) and m_eval > 0:
                ratio_str = f"{m_r2 / m_eval:6.1f}x"
            else:
                ratio_str = "   N/Ax"
        else:
            ratio_str = "   N/Ax"
        dens_raw = safe_mean(r.dens)
        dens_str = f"{100 * dens_raw:6.1f}" if not math.isnan(dens_raw) else "   N/A"
        s_eval = safe_mean(_observed_subset(r.S_mean.get(eval_strat, []), r.univs, r.observed.get(eval_strat, []))[0])
        s_r2 = safe_mean(_observed_subset(r.S_mean.get('r2', []), r.univs, r.observed.get('r2', []))[0])
        s25_str = "N/A" if (not math.isfinite(s_eval) or not math.isfinite(s_r2)) else f"{s_eval:.1f}/{s_r2:.1f}"
        lo_str = _fmt_float(r.ci_lo, 5, 1)
        hi_str = _fmt_float(r.ci_hi, 6, 1)
        wlt_str = f"{r.w:2d}/{r.l:2d}/{r.t:2d}" if (r.has_eval and r.has_r2) else " N/A   "
        p_str = f"{_fmt_pval(r.p_sign)} [{_fmt_pval(pa, 7).strip()}]"
        pw_str = f"{_fmt_pval(r.p_wilcoxon)} [{_fmt_pval(paw, 7).strip()}]"
        if is_pr_casestudy:
            print(f"{r.setting:14s} {r.used:>7d}/{r.nd:<5d} {dens_str} | {_fmt_float(imp_mean('dcg-pr'), 9, 2)} {_fmt_float(imp_mean('dcg'), 7, 2)} {_fmt_float(imp_mean('r2'), 7, 2)} {_fmt_float(imp_mean('r1'), 7, 2)} {_fmt_float(imp_mean('b0'), 7, 2)} {_fmt_float(imp_mean('dcg-flag-only'), 7, 2)} {_fmt_float(imp_mean('b2'), 8, 1)} | {ratio_str} [{lo_str},{hi_str}] | {wlt_str}  {p_str}  {pw_str} | {s25_str}")
        else:
            print(f"{r.setting:14s} {r.used:>7d}/{r.nd:<5d} {dens_str} | {_fmt_float(imp_mean('dcg'), 7, 2)} {_fmt_float(imp_mean('r2'), 7, 2)} {_fmt_float(imp_mean('r1'), 7, 2)} {_fmt_float(imp_mean('b0'), 7, 2)} {_fmt_float(imp_mean('dcg-flag-only'), 7, 2)} {_fmt_float(imp_mean('b2'), 8, 1)} | {ratio_str} [{lo_str},{hi_str}] | {wlt_str}  {p_str}  {pw_str} | {s25_str}")

    if has_distinct_single and rows:
        adj_s = holm(ps_single_holm)
        adj_ws = holm(pws_single_holm)
        print(f"\n  Single-draw (k1_single) paired tests ({target_name}<R2):")
        print(f"  {'setting':14s} | W/L/T     p(sign) [Holm]      p(Wilcoxon) [Holm]")
        for r, pas, paws in zip(rows, adj_s, adj_ws):
            wlt_s = f"{r.w_single:2d}/{r.l_single:2d}/{r.t_single:2d}" if (r.has_eval and r.has_r2) else " N/A   "
            ps_s = f"{_fmt_pval(r.p_sign_single)} [{_fmt_pval(pas, 7).strip()}]"
            pws_s = f"{_fmt_pval(r.p_wilcoxon_single)} [{_fmt_pval(paws, 7).strip()}]"
            print(f"  {r.setting:14s} | {wlt_s}  {ps_s}  {pws_s}")

    # worse-than-baseline flags using the consistent imputed-mean estimand
    def is_worse(row: ReportRow, base_name: str) -> bool:
        if not (_strategy_recorded(row.K_single, row.K_mean, row.S_mean, eval_strat, row.observed)
                and _strategy_recorded(row.K_single, row.K_mean, row.S_mean, base_name, row.observed)):
            return False
        xa, xb, xu = _paired_observed_subset(
            row.K_mean.get(eval_strat, []),
            row.K_mean.get(base_name, []),
            row.univs,
            row.observed.get(eval_strat, []),
            row.observed.get(base_name, []),
        )
        if not xa or not xb:
            return False
        ma = imputed_mean(xa, univs=xu, default_impute=default_impute)
        mb = imputed_mean(xb, univs=xu, default_impute=default_impute)
        return not math.isnan(ma) and not math.isnan(mb) and ma > mb

    baseline_labels = {'r2': 'R2', 'r1': 'R1', 'b0': 'uniform random (B0)', 'b2': 'LIFO (B2)'}
    checks = list(baselines_for_tests) if baselines_for_tests else []
    for required in ('r2', 'b0'):
        if required not in checks:
            checks.append(required)
    print()
    for base_name in checks:
        if base_name == 'r1' and not any(is_worse(r, 'r1') for r in rows):
            continue
        label = baseline_labels.get(base_name, base_name)
        print(f'  settings where {target_name} is WORSE than {label} on average k1:', [r.setting for r in rows if is_worse(r, base_name)] or 'none')
    return rows


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Paired per-seed statistical comparison of candidate rankings.',
        usage='%(prog)s <results.csv|directory> <title> [setting ...] [--default-impute N] [--dcg-pr | --no-dcg-pr]',
    )
    parser.add_argument('root', help='Path to per-seed CSV file or exploration directory')
    parser.add_argument('title', help='Section title for the report')
    parser.add_argument('settings', nargs='*', help='Fault IDs to include (default: all in CSV)')
    parser.add_argument('--default-impute', type=float, default=DEFAULT_IMPUTATION_UNIVERSE,
                        help=f'Fallback imputation penalty when universe size is missing (default: {DEFAULT_IMPUTATION_UNIVERSE})')
    parser.add_argument('--dcg-pr', dest='prefer_pr', action=argparse.BooleanOptionalAction, default=None,
                        help='Force or disable DCG-PR evaluation mode instead of inferring from input filename')
    args = parser.parse_args()
    if not _is_finite_num(args.default_impute) or float(args.default_impute) <= 0:
        parser.error(f"--default-impute must be a finite positive number, got {args.default_impute}")
    try:
        report(args.settings, args.root, args.title, default_impute=args.default_impute, prefer_pr=args.prefer_pr)
    except (ValueError, FileNotFoundError, json.JSONDecodeError) as exc:
        parser.error(str(exc))
