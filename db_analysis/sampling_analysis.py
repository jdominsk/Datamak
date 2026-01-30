#!/usr/bin/env python3
import argparse
import json
import math
import random
import sqlite3
import os
import time
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple


MHD_COLUMNS = [
    "rhoc",
    "Rmaj",
    "R_geo",
    "qinp",
    "shat",
    "shift",
    "akappa",
    "akappri",
    "tri",
    "tripri",
    "betaprim",
    "beta",
]
MHD_ID_COLUMN = "id"


def _as_finite_float(value: object) -> Optional[float]:
    try:
        val = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(val):
        return None
    return val


def _percentile(sorted_values: List[float], pct: float) -> Optional[float]:
    if not sorted_values:
        return None
    idx = int(round((len(sorted_values) - 1) * pct))
    return sorted_values[max(0, min(idx, len(sorted_values) - 1))]


def _column_basic_stats(values: List[float], total_rows: int) -> Dict[str, object]:
    if not values:
        return {
            "count": 0,
            "missing": total_rows,
            "mean": None,
            "std": None,
            "min": None,
            "max": None,
            "median": None,
        }
    count = len(values)
    mean_val = sum(values) / count
    if count > 1:
        variance = sum((val - mean_val) ** 2 for val in values) / (count - 1)
        std_val = math.sqrt(variance)
    else:
        std_val = 0.0
    sorted_vals = sorted(values)
    median_val = _percentile(sorted_vals, 0.5)
    return {
        "count": count,
        "missing": total_rows - count,
        "mean": mean_val,
        "std": std_val,
        "min": sorted_vals[0],
        "max": sorted_vals[-1],
        "median": median_val,
    }


def _filter_complete_rows(
    dataset: List[Dict[str, Optional[float]]]
) -> List[Dict[str, float]]:
    rows: List[Dict[str, float]] = []
    for row in dataset:
        if any(row[col] is None for col in MHD_COLUMNS):
            continue
        complete = {col: float(row[col]) for col in MHD_COLUMNS}
        if row.get(MHD_ID_COLUMN) is not None:
            complete[MHD_ID_COLUMN] = float(row[MHD_ID_COLUMN])
        rows.append(complete)
    return rows


def _standardize_rows(
    rows: List[Dict[str, float]],
) -> Tuple[List[List[float]], Dict[str, float], Dict[str, float]]:
    means: Dict[str, float] = {}
    stds: Dict[str, float] = {}
    for col in MHD_COLUMNS:
        values = [row[col] for row in rows]
        if not values:
            means[col] = 0.0
            stds[col] = 1.0
            continue
        mean_val = sum(values) / len(values)
        if len(values) > 1:
            variance = sum((val - mean_val) ** 2 for val in values) / (len(values) - 1)
            std_val = math.sqrt(variance)
        else:
            std_val = 1.0
        if std_val == 0.0:
            std_val = 1.0
        means[col] = mean_val
        stds[col] = std_val
    vectors: List[List[float]] = []
    for row in rows:
        vectors.append([(row[col] - means[col]) / stds[col] for col in MHD_COLUMNS])
    return vectors, means, stds


def _euclidean(vec_a: List[float], vec_b: List[float]) -> float:
    dist_sq = 0.0
    for a, b in zip(vec_a, vec_b):
        diff = a - b
        dist_sq += diff * diff
    return math.sqrt(dist_sq)


def _mean_vector(vectors: List[List[float]]) -> List[float]:
    if not vectors:
        return [0.0 for _ in range(len(MHD_COLUMNS))]
    dim = len(vectors[0])
    totals = [0.0 for _ in range(dim)]
    for vec in vectors:
        for idx in range(dim):
            totals[idx] += vec[idx]
    return [val / len(vectors) for val in totals]


def _kmeans(
    vectors: List[List[float]],
    k: int,
    max_iter: int = 25,
    seed: int = 0,
) -> Tuple[List[int], List[List[float]]]:
    rng = random.Random(seed)
    n = len(vectors)
    if k >= n:
        assignments = list(range(n))
        centroids = [vec[:] for vec in vectors]
        return assignments, centroids
    init_indices = rng.sample(range(n), k)
    centroids = [vectors[idx][:] for idx in init_indices]
    assignments = [0 for _ in range(n)]
    for _ in range(max_iter):
        changed = False
        clusters: List[List[List[float]]] = [[] for _ in range(k)]
        for i, vec in enumerate(vectors):
            best = 0
            best_dist = None
            for c_idx, centroid in enumerate(centroids):
                dist = _euclidean(vec, centroid)
                if best_dist is None or dist < best_dist:
                    best_dist = dist
                    best = c_idx
            if assignments[i] != best:
                changed = True
            assignments[i] = best
            clusters[best].append(vec)
        for idx in range(k):
            if clusters[idx]:
                centroids[idx] = _mean_vector(clusters[idx])
            else:
                centroids[idx] = vectors[rng.randrange(n)][:]  # re-seed empty cluster
        if not changed:
            break
    return assignments, centroids


def _silhouette_score(
    vectors: List[List[float]],
    assignments: List[int],
    sample_indices: Optional[List[int]] = None,
) -> Optional[float]:
    n = len(vectors)
    if n < 2:
        return None
    if sample_indices is None:
        sample_indices = list(range(n))
    clusters: Dict[int, List[int]] = {}
    for idx in sample_indices:
        clusters.setdefault(assignments[idx], []).append(idx)
    if len(clusters) < 2:
        return None
    scores = []
    for idx in sample_indices:
        own = assignments[idx]
        own_cluster = clusters.get(own, [])
        if len(own_cluster) <= 1:
            continue
        a = sum(_euclidean(vectors[idx], vectors[j]) for j in own_cluster if j != idx)
        a /= max(1, len(own_cluster) - 1)
        b = None
        for cluster_id, members in clusters.items():
            if cluster_id == own or not members:
                continue
            dist = sum(_euclidean(vectors[idx], vectors[j]) for j in members)
            dist /= len(members)
            if b is None or dist < b:
                b = dist
        if b is None:
            continue
        denom = max(a, b)
        if denom == 0.0:
            continue
        scores.append((b - a) / denom)
    if not scores:
        return None
    return sum(scores) / len(scores)


def _davies_bouldin_index(
    vectors: List[List[float]],
    assignments: List[int],
    centroids: List[List[float]],
) -> Optional[float]:
    k = len(centroids)
    if k < 2:
        return None
    clusters: Dict[int, List[int]] = {}
    for idx, cluster_id in enumerate(assignments):
        clusters.setdefault(cluster_id, []).append(idx)
    scatters: List[float] = []
    for cluster_id in range(k):
        members = clusters.get(cluster_id, [])
        if not members:
            scatters.append(0.0)
            continue
        dist = sum(_euclidean(vectors[i], centroids[cluster_id]) for i in members)
        scatters.append(dist / len(members))
    db_values = []
    for i in range(k):
        worst = None
        for j in range(k):
            if i == j:
                continue
            denom = _euclidean(centroids[i], centroids[j])
            if denom == 0.0:
                continue
            val = (scatters[i] + scatters[j]) / denom
            if worst is None or val > worst:
                worst = val
        if worst is not None:
            db_values.append(worst)
    if not db_values:
        return None
    return sum(db_values) / len(db_values)


def _mat_vec_mul(matrix: List[List[float]], vector: List[float]) -> List[float]:
    result = []
    for row in matrix:
        total = 0.0
        for a, b in zip(row, vector):
            total += a * b
        result.append(total)
    return result


def _vec_norm(vector: List[float]) -> float:
    return math.sqrt(sum(val * val for val in vector))


def _normalize(vector: List[float]) -> List[float]:
    norm = _vec_norm(vector)
    if norm == 0.0:
        return vector[:]
    return [val / norm for val in vector]


def _outer(vector: List[float]) -> List[List[float]]:
    return [[a * b for b in vector] for a in vector]


def _deflate(matrix: List[List[float]], eigenvalue: float, eigenvector: List[float]) -> None:
    outer = _outer(eigenvector)
    for i in range(len(matrix)):
        for j in range(len(matrix)):
            matrix[i][j] -= eigenvalue * outer[i][j]


def _power_iteration(matrix: List[List[float]], iters: int = 80) -> Tuple[float, List[float]]:
    rng = random.Random(0)
    vec = [rng.random() for _ in range(len(matrix))]
    vec = _normalize(vec)
    for _ in range(iters):
        vec = _mat_vec_mul(matrix, vec)
        vec = _normalize(vec)
    eig_val = sum(a * b for a, b in zip(vec, _mat_vec_mul(matrix, vec)))
    return eig_val, vec


def _quantile_edges(values: List[float], bins: int) -> Optional[List[float]]:
    if not values:
        return None
    sorted_vals = sorted(values)
    edges = []
    for i in range(1, bins):
        edges.append(_percentile(sorted_vals, i / bins))
    return edges


def _assign_bin(value: float, edges: List[float]) -> int:
    for idx, edge in enumerate(edges):
        if value <= edge:
            return idx
    return len(edges)


def _sample_rows(
    rows: List[Dict[str, float]],
    max_points: int,
    seed: int,
) -> Tuple[List[Dict[str, float]], bool]:
    if max_points <= 0:
        return rows, False
    if len(rows) <= max_points:
        return rows, False
    rng = random.Random(seed)
    return rng.sample(rows, max_points), True


def _analysis_basic(dataset: List[Dict[str, Optional[float]]], total_rows: int) -> Dict[str, object]:
    stats: Dict[str, Dict[str, object]] = {}
    for col in MHD_COLUMNS:
        values = [row[col] for row in dataset if row[col] is not None]
        stats[col] = _column_basic_stats(values, total_rows)
    return {"total_rows": total_rows, "stats": stats}


def _analysis_coverage(
    rows: List[Dict[str, float]],
    max_points: int,
    block_size: int,
    workers: int,
    sample_pairs: int,
) -> Dict[str, object]:
    total = len(rows)
    if total < 2:
        return {
            "total": total,
            "used": total,
            "sampled": False,
            "pairwise": None,
            "nearest": None,
            "block_size": block_size,
            "sample_pairs": sample_pairs,
            "workers": workers,
        }
    rows, sampled = _sample_rows(rows, max_points, seed=0)
    vectors, _, _ = _standardize_rows(rows)
    n = len(vectors)
    if block_size <= 0 or block_size >= n:
        block_size = n
    if sample_pairs > 0:
        pairwise, nearest = _analysis_coverage_sampled(
            vectors, sample_pairs, workers if workers > 1 else 1
        )
    elif workers > 1 and n > 1:
        pairwise, nearest = _analysis_coverage_parallel(vectors, block_size, workers)
    else:
        pairwise, nearest = _analysis_coverage_serial(vectors, block_size)
    pairwise.sort()
    nearest.sort()
    return {
        "total": total,
        "used": len(rows),
        "sampled": sampled,
        "block_size": block_size,
        "sample_pairs": sample_pairs,
        "workers": workers,
        "pairwise": {
            "min": pairwise[0] if pairwise else None,
            "median": _percentile(pairwise, 0.5),
            "p95": _percentile(pairwise, 0.95),
            "max": pairwise[-1] if pairwise else None,
        },
        "nearest": {
            "min": nearest[0] if nearest else None,
            "median": _percentile(nearest, 0.5),
            "p95": _percentile(nearest, 0.95),
            "max": nearest[-1] if nearest else None,
        },
    }


def _analysis_coverage_serial(
    vectors: List[List[float]],
    block_size: int,
) -> Tuple[List[float], List[float]]:
    pairwise: List[float] = []
    nearest: List[float] = []
    n = len(vectors)
    for i0 in range(0, n, block_size):
        i1 = min(i0 + block_size, n)
        for j0 in range(i0, n, block_size):
            j1 = min(j0 + block_size, n)
            for i in range(i0, i1):
                vi = vectors[i]
                min_dist = None
                j_start = j0
                if i0 == j0:
                    j_start = max(j0, i + 1)
                for j in range(j_start, j1):
                    vj = vectors[j]
                    dist = _euclidean(vi, vj)
                    pairwise.append(dist)
                    if min_dist is None or dist < min_dist:
                        min_dist = dist
                if min_dist is not None:
                    nearest.append(min_dist)
    return pairwise, nearest


_COVERAGE_VECTORS: Optional[List[List[float]]] = None


def _init_coverage_worker(vectors: List[List[float]]) -> None:
    global _COVERAGE_VECTORS
    _COVERAGE_VECTORS = vectors


def _coverage_worker(
    start_i: int,
    end_i: int,
    block_size: int,
) -> Tuple[List[float], List[float]]:
    vectors = _COVERAGE_VECTORS or []
    pairwise: List[float] = []
    n = len(vectors)
    nearest_local = [math.inf for _ in range(n)]
    for i in range(start_i, end_i):
        vi = vectors[i]
        for j0 in range(i + 1, n, block_size):
            j1 = min(j0 + block_size, n)
            for j in range(j0, j1):
                vj = vectors[j]
                dist = _euclidean(vi, vj)
                pairwise.append(dist)
                if dist < nearest_local[i]:
                    nearest_local[i] = dist
                if dist < nearest_local[j]:
                    nearest_local[j] = dist
    return pairwise, nearest_local


def _split_i_ranges(n: int, workers: int) -> List[Tuple[int, int]]:
    total_pairs = n * (n - 1) // 2
    target = total_pairs / workers if workers > 0 else total_pairs
    ranges: List[Tuple[int, int]] = []
    start = 0
    accum = 0.0
    for i in range(n):
        accum += n - 1 - i
        if accum >= target and len(ranges) < workers - 1:
            ranges.append((start, i + 1))
            start = i + 1
            accum = 0.0
    ranges.append((start, n))
    return ranges


def _analysis_coverage_parallel(
    vectors: List[List[float]],
    block_size: int,
    workers: int,
) -> Tuple[List[float], List[float]]:
    n = len(vectors)
    ranges = _split_i_ranges(n, workers)
    pairwise_all: List[float] = []
    nearest = [math.inf for _ in range(n)]
    with mp.Pool(processes=workers, initializer=_init_coverage_worker, initargs=(vectors,)) as pool:
        tasks = [(start, end, block_size) for start, end in ranges if start < end]
        for pairwise, nearest_local in pool.starmap(_coverage_worker, tasks):
            pairwise_all.extend(pairwise)
            for idx, val in enumerate(nearest_local):
                if val < nearest[idx]:
                    nearest[idx] = val
    nearest = [val for val in nearest if val != math.inf]
    return pairwise_all, nearest


def _unrank_pair(k: int, n: int) -> Tuple[int, int]:
    a = n - 1
    low = 0
    high = n - 1
    while low < high:
        mid = (low + high) // 2
        cum = (mid + 1) * (2 * a - mid) // 2
        if k < cum:
            high = mid
        else:
            low = mid + 1
    i = low
    cum_before = i * (2 * a - (i - 1)) // 2 if i > 0 else 0
    offset = k - cum_before
    j = i + 1 + offset
    return i, j


def _coverage_sample_worker(
    n: int,
    sample_count: int,
    seed: int,
) -> Tuple[List[float], List[float]]:
    vectors = _COVERAGE_VECTORS or []
    rng = random.Random(seed)
    total_pairs = n * (n - 1) // 2
    pairwise: List[float] = []
    nearest_local = [math.inf for _ in range(n)]
    for _ in range(sample_count):
        k = rng.randrange(total_pairs)
        i, j = _unrank_pair(k, n)
        vi = vectors[i]
        vj = vectors[j]
        dist = _euclidean(vi, vj)
        pairwise.append(dist)
        if dist < nearest_local[i]:
            nearest_local[i] = dist
        if dist < nearest_local[j]:
            nearest_local[j] = dist
    return pairwise, nearest_local


def _analysis_coverage_sampled(
    vectors: List[List[float]],
    sample_pairs: int,
    workers: int,
) -> Tuple[List[float], List[float]]:
    n = len(vectors)
    if n < 2 or sample_pairs <= 0:
        return [], []
    if workers <= 1 or sample_pairs < workers:
        pairwise, nearest_local = _coverage_sample_worker(n, sample_pairs, seed=0)
        nearest = [val for val in nearest_local if val != math.inf]
        return pairwise, nearest
    counts = [sample_pairs // workers for _ in range(workers)]
    for idx in range(sample_pairs % workers):
        counts[idx] += 1
    pairwise_all: List[float] = []
    nearest = [math.inf for _ in range(n)]
    with mp.Pool(processes=workers, initializer=_init_coverage_worker, initargs=(vectors,)) as pool:
        tasks = [(n, counts[i], i) for i in range(workers) if counts[i] > 0]
        for pairwise, nearest_local in pool.starmap(_coverage_sample_worker, tasks):
            pairwise_all.extend(pairwise)
            for idx, val in enumerate(nearest_local):
                if val < nearest[idx]:
                    nearest[idx] = val
    nearest = [val for val in nearest if val != math.inf]
    return pairwise_all, nearest


def _analysis_clustering(rows: List[Dict[str, float]], k: int, max_points: int) -> Dict[str, object]:
    total = len(rows)
    if total < 2:
        return {"total": total, "used": total, "sampled": False, "k": k, "metrics": None}
    rows, sampled = _sample_rows(rows, max_points, seed=1)
    vectors, _, _ = _standardize_rows(rows)
    if k < 2 or k > len(vectors):
        return {"total": total, "used": len(vectors), "sampled": sampled, "k": k, "metrics": None}
    assignments, centroids = _kmeans(vectors, k)
    sample_indices = None
    if len(vectors) > 400:
        rng = random.Random(2)
        sample_indices = rng.sample(range(len(vectors)), 400)
    silhouette = _silhouette_score(vectors, assignments, sample_indices)
    dbi = _davies_bouldin_index(vectors, assignments, centroids)
    sizes: Dict[int, int] = {}
    for cluster_id in assignments:
        sizes[cluster_id] = sizes.get(cluster_id, 0) + 1
    counts = sorted(sizes.values())
    return {
        "total": total,
        "used": len(vectors),
        "sampled": sampled,
        "k": k,
        "metrics": {
            "silhouette": silhouette,
            "davies_bouldin": dbi,
            "min_size": counts[0] if counts else None,
            "median_size": _percentile(counts, 0.5) if counts else None,
            "max_size": counts[-1] if counts else None,
        },
    }


def _analysis_pca(rows: List[Dict[str, float]], max_points: int) -> Dict[str, object]:
    total = len(rows)
    if total < 2:
        return {"total": total, "used": total, "sampled": False, "components": []}
    rows, sampled = _sample_rows(rows, max_points, seed=3)
    vectors, _, _ = _standardize_rows(rows)
    n = len(vectors)
    dim = len(MHD_COLUMNS)
    cov = [[0.0 for _ in range(dim)] for _ in range(dim)]
    for vec in vectors:
        for i in range(dim):
            for j in range(dim):
                cov[i][j] += vec[i] * vec[j]
    denom = max(1, n - 1)
    for i in range(dim):
        for j in range(dim):
            cov[i][j] /= denom
    total_variance = sum(cov[i][i] for i in range(dim))
    eigs: List[Tuple[float, List[float]]] = []
    work = [row[:] for row in cov]
    for _ in range(min(3, dim)):
        eig_val, eig_vec = _power_iteration(work)
        if eig_val <= 0.0:
            break
        eigs.append((eig_val, eig_vec))
        _deflate(work, eig_val, eig_vec)
    comp_rows = []
    cumulative = 0.0
    for idx, (eig_val, _) in enumerate(eigs, start=1):
        ratio = eig_val / total_variance if total_variance else 0.0
        cumulative += ratio
        comp_rows.append(
            {"component": idx, "eigenvalue": eig_val, "ratio": ratio, "cumulative": cumulative}
        )
    return {"total": total, "used": len(vectors), "sampled": sampled, "components": comp_rows}


def _analysis_selection(
    rows: List[Dict[str, float]],
    target: int,
    max_points: int,
) -> Dict[str, object]:
    total = len(rows)
    if total == 0:
        return {"total": total, "used": 0, "sampled": False, "target": target, "metrics": None}
    rows, sampled = _sample_rows(rows, max_points, seed=4)
    vectors, _, _ = _standardize_rows(rows)
    n = len(vectors)
    target = max(1, min(target, n))
    rng = random.Random(0)
    selected = [rng.randrange(n)]
    min_dists = [float("inf") for _ in range(n)]
    for _ in range(1, target):
        best_idx = None
        best_dist = -1.0
        last_idx = selected[-1]
        for idx, vec in enumerate(vectors):
            dist = _euclidean(vec, vectors[last_idx])
            if dist < min_dists[idx]:
                min_dists[idx] = dist
            if min_dists[idx] > best_dist:
                best_dist = min_dists[idx]
                best_idx = idx
        if best_idx is None:
            break
        selected.append(best_idx)
    for idx, vec in enumerate(vectors):
        dist = _euclidean(vec, vectors[selected[-1]])
        if dist < min_dists[idx]:
            min_dists[idx] = dist
    avg_dist = sum(min_dists) / len(min_dists) if min_dists else None
    max_dist = max(min_dists) if min_dists else None
    selected_ids = []
    for idx in selected:
        row_id = rows[idx].get(MHD_ID_COLUMN)
        if row_id is not None:
            selected_ids.append(int(row_id))
    return {
        "total": total,
        "used": n,
        "sampled": sampled,
        "target": target,
        "metrics": {"avg_nearest": avg_dist, "max_nearest": max_dist},
        "selected_ids": selected_ids,
    }


def _analysis_regimes(
    dataset: List[Dict[str, Optional[float]]],
    params: Optional[List[str]] = None,
    bins: int = 3,
) -> Dict[str, object]:
    if params is None:
        params = ["qinp", "shat", "beta"]
    available = [p for p in params if p in MHD_COLUMNS]
    if len(available) != len(params):
        missing = [p for p in params if p not in available]
        return {"params": available, "missing": missing, "coverage": None, "bins": None}
    values_by_param = {p: [row[p] for row in dataset if row[p] is not None] for p in params}
    edges_by_param = {}
    for p in params:
        edges = _quantile_edges(values_by_param[p], bins)
        if not edges:
            return {"params": params, "missing": [], "coverage": None, "bins": None}
        edges_by_param[p] = edges
    counts: Dict[Tuple[int, ...], int] = {}
    for row in dataset:
        if any(row[p] is None for p in params):
            continue
        bin_key = tuple(_assign_bin(row[p], edges_by_param[p]) for p in params)
        counts[bin_key] = counts.get(bin_key, 0) + 1
    total_bins = bins ** len(params)
    non_empty = len(counts)
    bin_counts = sorted(counts.values())
    return {
        "params": params,
        "missing": [],
        "coverage": {
            "non_empty": non_empty,
            "total_bins": total_bins,
            "coverage_pct": (non_empty / total_bins * 100.0) if total_bins else 0.0,
            "min": bin_counts[0] if bin_counts else None,
            "median": _percentile(bin_counts, 0.5) if bin_counts else None,
            "max": bin_counts[-1] if bin_counts else None,
        },
        "bins": sorted(counts.items(), key=lambda item: item[1], reverse=True)[:10],
        "edges": edges_by_param,
    }


def _run_timed(name: str, fn, *args):
    start = time.perf_counter()
    result = fn(*args)
    elapsed = time.perf_counter() - start
    return name, elapsed, result


def _fetch_dataset(conn: sqlite3.Connection, origin_id: Optional[int]) -> Tuple[List[Dict[str, Optional[float]]], int]:
    columns = MHD_COLUMNS
    base_query = f"""
        SELECT gk_input.id, {", ".join(columns)}
        FROM gk_input
        JOIN gk_study ON gk_study.id = gk_input.gk_study_id
        JOIN data_equil ON data_equil.id = gk_study.data_equil_id
    """
    params: List[object] = []
    if origin_id is not None:
        base_query += " WHERE data_equil.data_origin_id = ?"
        params.append(origin_id)
    rows = conn.execute(base_query, params).fetchall()
    dataset: List[Dict[str, Optional[float]]] = []
    for row in rows:
        item: Dict[str, Optional[float]] = {}
        item[MHD_ID_COLUMN] = _as_finite_float(row[0])
        for idx, col in enumerate(columns, start=1):
            item[col] = _as_finite_float(row[idx])
        dataset.append(item)
    return dataset, len(rows)


def run_for_origin(
    conn: sqlite3.Connection,
    origin_id: Optional[int],
    origin_name: Optional[str],
    k: int,
    selection_size: int,
    max_points: int,
    workers: Optional[int],
    analyses: List[int],
    analysis2_block_size: int,
    analysis2_workers: int,
    analysis2_sample_pairs: int,
) -> Dict[str, object]:
    origin_label = f"{origin_id}" if origin_id is not None else "All"
    origin_title = origin_name if origin_name is not None else "All"
    print(f"=== Starting data_origin {origin_label}: {origin_title} ===", flush=True)
    dataset, total_rows = _fetch_dataset(conn, origin_id)
    complete_rows = _filter_complete_rows(dataset)
    used_ids = [int(row[MHD_ID_COLUMN]) for row in complete_rows if MHD_ID_COLUMN in row]
    if not workers or workers <= 1:
        result = {
            "origin_id": origin_id,
            "origin_name": origin_name,
            "total_rows": total_rows,
            "complete_rows": len(complete_rows),
            "gk_input_ids_used": used_ids,
        }
        if 1 in analyses:
            print(f"[origin {origin_label}] starting analysis 1", flush=True)
            name, elapsed, value = _run_timed("analysis_1", _analysis_basic, dataset, total_rows)
            result[name] = value
            print(f"[origin {origin_label}] finished analysis 1 in {elapsed:.2f}s", flush=True)
        if 2 in analyses:
            print(f"[origin {origin_label}] starting analysis 2", flush=True)
            name, elapsed, value = _run_timed(
                "analysis_2",
                _analysis_coverage,
                complete_rows,
                max_points,
                analysis2_block_size,
                analysis2_workers,
                analysis2_sample_pairs,
            )
            result[name] = value
            print(f"[origin {origin_label}] finished analysis 2 in {elapsed:.2f}s", flush=True)
        if 3 in analyses:
            print(f"[origin {origin_label}] starting analysis 3", flush=True)
            name, elapsed, value = _run_timed("analysis_3", _analysis_clustering, complete_rows, k, max_points)
            result[name] = value
            print(f"[origin {origin_label}] finished analysis 3 in {elapsed:.2f}s", flush=True)
        if 4 in analyses:
            print(f"[origin {origin_label}] starting analysis 4", flush=True)
            name, elapsed, value = _run_timed("analysis_4", _analysis_pca, complete_rows, max_points)
            result[name] = value
            print(f"[origin {origin_label}] finished analysis 4 in {elapsed:.2f}s", flush=True)
        if 5 in analyses:
            print(f"[origin {origin_label}] starting analysis 5", flush=True)
            name, elapsed, value = _run_timed(
                "analysis_5", _analysis_selection, complete_rows, selection_size, max_points
            )
            result[name] = value
            print(f"[origin {origin_label}] finished analysis 5 in {elapsed:.2f}s", flush=True)
        if 6 in analyses:
            print(f"[origin {origin_label}] starting analysis 6", flush=True)
            name, elapsed, value = _run_timed("analysis_6", _analysis_regimes, dataset)
            result[name] = value
            print(f"[origin {origin_label}] finished analysis 6 in {elapsed:.2f}s", flush=True)
        return result

    results: Dict[str, object] = {}
    timings: Dict[str, float] = {}
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {}
        if 1 in analyses:
            futures["analysis_1"] = executor.submit(_run_timed, "analysis_1", _analysis_basic, dataset, total_rows)
        if 2 in analyses:
            futures["analysis_2"] = executor.submit(
                _run_timed,
                "analysis_2",
                _analysis_coverage,
                complete_rows,
                max_points,
                analysis2_block_size,
                analysis2_workers,
                analysis2_sample_pairs,
            )
        if 3 in analyses:
            futures["analysis_3"] = executor.submit(
                _run_timed, "analysis_3", _analysis_clustering, complete_rows, k, max_points
            )
        if 4 in analyses:
            futures["analysis_4"] = executor.submit(
                _run_timed, "analysis_4", _analysis_pca, complete_rows, max_points
            )
        if 5 in analyses:
            futures["analysis_5"] = executor.submit(
                _run_timed,
                "analysis_5",
                _analysis_selection,
                complete_rows,
                selection_size,
                max_points,
            )
        if 6 in analyses:
            futures["analysis_6"] = executor.submit(_run_timed, "analysis_6", _analysis_regimes, dataset)
        for analysis_key in futures:
            print(f"[origin {origin_label}] starting {analysis_key}", flush=True)
        for key, future in futures.items():
            name, elapsed, value = future.result()
            results[name] = value
            timings[name] = elapsed
        for analysis_id in analyses:
            name = f"analysis_{analysis_id}"
            if name in timings:
                print(
                    f"[origin {origin_label}] finished {name} in {timings[name]:.2f}s",
                    flush=True,
                )

    return {
        "origin_id": origin_id,
        "origin_name": origin_name,
        "total_rows": total_rows,
        "complete_rows": len(complete_rows),
        "gk_input_ids_used": used_ids,
        **results,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Sampling analysis for gk_input by data_origin.")
    parser.add_argument(
        "--db",
        default="gyrokinetic_simulations.db",
        help="Path to gyrokinetic_simulations.db (default: gyrokinetic_simulations.db)",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Output JSON path (default: db_analysis/sampling_results_TIMESTAMP.json)",
    )
    parser.add_argument("--max-points", type=int, default=1500, help="Max rows per analysis")
    parser.add_argument("--k", type=int, default=6, help="k for k-means")
    parser.add_argument("--selection-size", type=int, default=50, help="Target selection size")
    parser.add_argument("--include-all", action="store_true", help="Include an All entry")
    parser.add_argument("--analysis1", action="store_true", help="Run analysis 1 only")
    parser.add_argument("--analysis2", action="store_true", help="Run analysis 2 only")
    parser.add_argument("--analysis3", action="store_true", help="Run analysis 3 only")
    parser.add_argument("--analysis4", action="store_true", help="Run analysis 4 only")
    parser.add_argument("--analysis5", action="store_true", help="Run analysis 5 only")
    parser.add_argument("--analysis6", action="store_true", help="Run analysis 6 only")
    parser.add_argument(
        "--analyses",
        type=str,
        default="",
        help="Comma-separated list of analyses to run, e.g. 1,2,4",
    )
    parser.add_argument(
        "--analysis2-block-size",
        type=int,
        default=0,
        help="Block size for analysis 2 distance loops (0 = no blocking)",
    )
    parser.add_argument(
        "--analysis2-workers",
        type=int,
        default=None,
        help="Workers for analysis 2 pairwise loop (default: use --workers)",
    )
    parser.add_argument(
        "--analysis2-sample-pairs",
        type=int,
        default=0,
        help="If >0, sample this many pairs for analysis 2 (approximate)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=6,
        help="Workers for analyses within each data_origin (1 = no parallel)",
    )
    args = parser.parse_args()

    selected: List[int] = []
    if args.analyses:
        for token in args.analyses.split(","):
            token = token.strip()
            if not token:
                continue
            if not token.isdigit():
                raise SystemExit(f"Invalid analysis value: {token}")
            val = int(token)
            if val < 1 or val > 6:
                raise SystemExit(f"Analysis out of range (1-6): {val}")
            if val not in selected:
                selected.append(val)
    else:
        selected = [i for i in range(1, 7) if getattr(args, f"analysis{i}")]
    if not selected:
        selected = [1, 2, 3, 4, 5, 6]

    start_total = time.perf_counter()
    output_path = args.output.strip()
    if not output_path:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = f"db_analysis/sampling_results_{stamp}.json"

    analysis2_workers = (
        args.analysis2_workers if args.analysis2_workers and args.analysis2_workers > 0 else args.workers
    )
    analysis2_sample_pairs = args.analysis2_sample_pairs if args.analysis2_sample_pairs > 0 else 0

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    try:
        origins = conn.execute("SELECT id, name FROM data_origin ORDER BY id").fetchall()
        results = []
        if args.include_all:
            results.append(
                run_for_origin(
                    conn,
                    None,
                    "All",
                    args.k,
                    args.selection_size,
                    args.max_points,
                    args.workers,
                    selected,
                    args.analysis2_block_size,
                    analysis2_workers,
                    analysis2_sample_pairs,
                )
            )
        for row in origins:
            results.append(
                run_for_origin(
                    conn,
                    int(row["id"]),
                    str(row["name"]),
                    args.k,
                    args.selection_size,
                    args.max_points,
                    args.workers,
                    selected,
                    args.analysis2_block_size,
                    analysis2_workers,
                    analysis2_sample_pairs,
                )
            )
    finally:
        conn.close()

    payload = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "db_path": args.db,
        "mhd_columns": MHD_COLUMNS,
        "max_points": args.max_points,
        "k": args.k,
        "selection_size": args.selection_size,
        "analysis2_block_size": args.analysis2_block_size,
        "analysis2_workers": analysis2_workers,
        "analysis2_sample_pairs": analysis2_sample_pairs,
        "analyses": selected,
        "results": results,
    }
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    total_elapsed = time.perf_counter() - start_total
    print(f"Total elapsed time: {total_elapsed:.2f}s", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
