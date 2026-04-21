"""
Benchmark harness: runs every cross-backend query against every backend N times,
drops warmup runs, reports median and quartiles.

Calls the FastAPI /api/v1/query endpoint so the timing comes from the same code
path that serves the Comparison Dashboard.

Output: results/bench.json and results/bench_summary.csv
"""

import json
import os
import statistics
import sys
import time
from pathlib import Path

import requests

API = os.environ.get("BENCH_API", "http://localhost:8000")
N = int(os.environ.get("BENCH_N", "7"))
WARMUP = int(os.environ.get("BENCH_WARMUP", "2"))
TIMEOUT = int(os.environ.get("BENCH_TIMEOUT", "180"))
OUT_DIR = Path(__file__).parent.parent / "results"
OUT_DIR.mkdir(parents=True, exist_ok=True)

CROSS_BACKEND_QUERIES = ["Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q7", "Q8", "Q9", "Q10"]
GRAPH_ONLY = ["Q11", "Q12", "Q13"]
BACKENDS = ["postgres", "mongodb", "neo4j"]


def wait_for_api():
    deadline = time.time() + 120
    while time.time() < deadline:
        try:
            r = requests.get(f"{API}/api/v1/health", timeout=5)
            if r.ok:
                data = r.json()
                print(f"health: {data}")
                return True
        except Exception:
            pass
        time.sleep(3)
    return False


def run_one(query_id: str, backend: str) -> dict | None:
    try:
        r = requests.post(
            f"{API}/api/v1/query",
            json={"query_id": query_id, "backend": backend, "params": {}},
            timeout=TIMEOUT,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"  ERROR {query_id}/{backend}: {e}")
        return None


def bench_cell(query_id: str, backend: str) -> dict:
    samples = []
    row_count = None
    error = None
    for i in range(N):
        res = run_one(query_id, backend)
        if res is None:
            error = "request failed"
            continue
        ms = res.get("execution_time_ms")
        if ms is None:
            error = "no execution_time_ms"
            continue
        samples.append(float(ms))
        if row_count is None:
            row_count = res.get("row_count")

    kept = samples[WARMUP:] if len(samples) > WARMUP else samples
    if not kept:
        return {"median_ms": None, "p25_ms": None, "p75_ms": None,
                "runs": samples, "row_count": row_count, "error": error}
    kept_sorted = sorted(kept)
    median_ms = statistics.median(kept_sorted)
    p25 = kept_sorted[max(0, int(0.25 * (len(kept_sorted) - 1)))]
    p75 = kept_sorted[min(len(kept_sorted) - 1, int(0.75 * (len(kept_sorted) - 1)))]
    return {"median_ms": round(median_ms, 2),
            "p25_ms": round(p25, 2),
            "p75_ms": round(p75, 2),
            "runs": [round(s, 2) for s in samples],
            "kept": [round(s, 2) for s in kept],
            "row_count": row_count,
            "error": error}


def fetch_sizes() -> dict:
    sizes = {}
    for be in BACKENDS:
        try:
            r = requests.get(f"{API}/api/v1/metadata/{be}", timeout=30)
            r.raise_for_status()
            sizes[be] = r.json()
        except Exception as e:
            sizes[be] = {"error": str(e)}
    return sizes


def main():
    if not wait_for_api():
        print("API not healthy, aborting")
        sys.exit(1)

    results = {"config": {"N": N, "warmup": WARMUP, "api": API},
               "cross_backend": {}, "graph_only": {}, "sizes": {}}

    print("\n=== Fetching backend sizes ===")
    results["sizes"] = fetch_sizes()

    print("\n=== Cross-backend queries (Q1..Q10) ===")
    for qid in CROSS_BACKEND_QUERIES:
        results["cross_backend"][qid] = {}
        for be in BACKENDS:
            print(f"{qid} / {be} ...")
            cell = bench_cell(qid, be)
            results["cross_backend"][qid][be] = cell
            med = cell["median_ms"]
            print(f"  median={med} ms  rows={cell['row_count']}  samples={cell['runs']}")

    print("\n=== Graph-only queries (Q11..Q13 on Neo4j) ===")
    for qid in GRAPH_ONLY:
        print(f"{qid} / neo4j ...")
        cell = bench_cell(qid, "neo4j")
        results["graph_only"][qid] = cell
        print(f"  median={cell['median_ms']} ms  rows={cell['row_count']}")

    out_json = OUT_DIR / "bench.json"
    out_json.write_text(json.dumps(results, indent=2))
    print(f"\nwrote {out_json}")

    out_csv = OUT_DIR / "bench_summary.csv"
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        f.write("query_id,backend,median_ms,p25_ms,p75_ms,row_count,error\n")
        for qid, by_be in results["cross_backend"].items():
            for be, cell in by_be.items():
                f.write(f"{qid},{be},{cell['median_ms']},{cell['p25_ms']},"
                        f"{cell['p75_ms']},{cell['row_count']},{cell.get('error') or ''}\n")
        for qid, cell in results["graph_only"].items():
            f.write(f"{qid},neo4j,{cell['median_ms']},{cell['p25_ms']},"
                    f"{cell['p75_ms']},{cell['row_count']},{cell.get('error') or ''}\n")
    print(f"wrote {out_csv}")


if __name__ == "__main__":
    main()
