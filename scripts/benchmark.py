"""Pruebas de escalabilidad y robustez de DistriTask.

Cada corrida levanta un clúster nuevo (coordinador + nodos como procesos
independientes), ejecuta un trabajo y guarda sus métricas. Cada configuración
se repite ``--reps`` veces y se reporta la mediana.

Experimentos:
  E1  Escalabilidad vertical: hilos por nodo (1 nodo, 1..16 hilos).
  E2  Escalabilidad horizontal: número de nodos (1..8 nodos x 4 hilos).
  E3  Carga CPU pura: GIL de CPython (hilos vs. procesos vs. nodos).
  E4  Versión 1 vs. versión 2 (ablación de cada ajuste) en 3 escenarios.
  E5  Tolerancia a fallos: caída abrupta y cuelgue de un nodo.
  E6  Nodo degradado: efecto del Circuit Breaker.
  E7  Ajuste fino: sensibilidad al número de créditos por hilo (prefetch).
  E8  Fallos combinados en caliente (validación de la corrección del breaker).

Uso:  python scripts/benchmark.py              (todo, ~8-10 min)
      python scripts/benchmark.py --only E1,E5 --reps 1
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from distritask.chaos import inject  # noqa: E402
from distritask.cluster import LocalCluster  # noqa: E402
from distritask.monitor import subscribe  # noqa: E402
from distritask.protocol import ProtocolError  # noqa: E402
from distritask.tasks import build_log_job, build_pi_job  # noqa: E402

RESULTS = ROOT / "results"

# Perfiles de ablación: de la versión 1 a la versión 2, un ajuste a la vez.
ABLATION = {
    "V1 linea base": ["--strategy", "round_robin", "--transport", "per_task", "--prefetch", "8",
                      "--batch-size", "1", "--no-breaker"],
    "+ conexion persistente": ["--strategy", "round_robin", "--transport", "persistent", "--prefetch", "8",
                               "--batch-size", "1", "--no-breaker"],
    "+ lotes (8)": ["--strategy", "round_robin", "--transport", "persistent", "--prefetch", "8",
                    "--batch-size", "8", "--no-breaker"],
    "V2 (+ least-loaded, creditos=2)": ["--strategy", "least_loaded", "--transport", "persistent",
                                        "--prefetch", "2", "--batch-size", "8", "--breaker",
                                        "--retry-affinity"],
}


def cpu_info() -> str:
    try:
        with open("/proc/cpuinfo", encoding="utf-8") as fh:
            for line in fh:
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or platform.machine()


def run_job(*, workers: int, threads: int, tasks: list[dict[str, Any]], job_type: str,
            profile: str = "optimized", coordinator_args: list[str] | None = None,
            worker_args: dict[str, list[str]] | None = None, executor: str = "thread",
            fault: Callable[[LocalCluster], None] | None = None, fault_at: float = 1.0,
            watch_events: bool = False) -> dict[str, Any]:
    events: list[dict[str, Any]] = []
    with LocalCluster(workers, threads, profile, coordinator_args=coordinator_args or [],
                      worker_args=worker_args, executor=executor) as cluster:
        if watch_events:
            def listen() -> None:
                try:
                    for e in subscribe(cluster.host, cluster.port, ["worker.*", "breaker.*"]):
                        events.append(e)
                except (OSError, ProtocolError):
                    pass  # el clúster se detuvo al terminar la corrida
            threading.Thread(target=listen, daemon=True).start()
            time.sleep(0.2)
        fault_ts: list[float] = []
        if fault:
            def trigger() -> None:
                fault_ts.append(time.time())
                fault(cluster)
            threading.Timer(fault_at, trigger).start()
        with cluster.client() as client:
            summary = client.submit(job_type, tasks, timeout=300)
        time.sleep(0.1)
    out = dict(summary["metrics"])
    if fault_ts:
        downs = [e["ts"] for e in events if e["topic"] == "worker.down" and e["ts"] >= fault_ts[0]]
        out["detection_s"] = round(downs[0] - fault_ts[0], 3) if downs else None
    out["breaker_opens"] = sum(1 for e in events if e["topic"] == "breaker.opened")
    return out


def repeat(reps: int, label: str, **kwargs: Any) -> dict[str, Any]:
    runs = []
    for i in range(reps):
        r = run_job(**kwargs)
        runs.append(r)
        print(f"   {label:<42} rep {i + 1}: {r['throughput_tps']:>7.1f} t/s  makespan={r['makespan_s']:.2f}s"
              f"  p95={r['latency_p95_ms']:.0f}ms  ok={r['done']}/{r['tasks']}", flush=True)
    def med(key: str) -> float:
        vals = [r[key] for r in runs if r.get(key) is not None]
        return round(statistics.median(vals), 3) if vals else None  # type: ignore[return-value]
    keys = ("throughput_tps", "makespan_s", "latency_p50_ms", "latency_p95_ms", "done", "failed",
            "retries", "requeues", "attempts", "duplicates", "detection_s", "breaker_opens")
    return {"label": label, "runs": runs, **{k: med(k) for k in keys},
            "throughput_min": min(r["throughput_tps"] for r in runs),
            "throughput_max": max(r["throughput_tps"] for r in runs),
            "per_worker": runs[-1]["per_worker"], "tasks": runs[0]["tasks"]}


# ---------------------------------------------------------------------------
def e1_threads(reps: int) -> dict[str, Any]:
    rows = []
    for threads in (1, 2, 4, 8, 16):
        r = repeat(reps, f"1 nodo x {threads} hilos", workers=1, threads=threads, job_type="logs",
                   tasks=build_log_job(120, 500, 50))
        rows.append({"threads": threads, **r})
    return {"title": "Escalabilidad vertical (hilos por nodo)",
            "params": "1 nodo, 120 tareas de logs (500 lineas, 50 ms de E/S simulada)", "rows": rows}


def e2_nodes(reps: int) -> dict[str, Any]:
    rows = []
    for nodes in (1, 2, 3, 4, 6, 8):
        r = repeat(reps, f"{nodes} nodos x 4 hilos", workers=nodes, threads=4, job_type="logs",
                   tasks=build_log_job(320, 500, 50))
        rows.append({"nodes": nodes, **r})
    base = rows[0]["throughput_tps"]
    for row in rows:
        row["speedup"] = round(row["throughput_tps"] / base, 2)
        row["efficiency_pct"] = round(100 * row["speedup"] / row["nodes"], 1)
    return {"title": "Escalabilidad horizontal (numero de nodos)",
            "params": "N nodos x 4 hilos, 320 tareas de logs (500 lineas, 50 ms de E/S)", "rows": rows}


def e3_cpu(reps: int) -> dict[str, Any]:
    rows = []
    tasks = build_pi_job(24, 1_000_000)
    for n in (1, 2, 4):
        rows.append({"mode": "hilos (1 nodo)", "n": n, **repeat(
            reps, f"1 nodo x {n} hilos (pi)", workers=1, threads=n, job_type="pi", tasks=tasks)})
    for n in (1, 2, 4):
        rows.append({"mode": "procesos (1 nodo)", "n": n, **repeat(
            reps, f"1 nodo x {n} procesos (pi)", workers=1, threads=n, job_type="pi", tasks=tasks,
            executor="process")})
    for n in (1, 2, 4):
        rows.append({"mode": "nodos x 1 hilo", "n": n, **repeat(
            reps, f"{n} nodos x 1 hilo (pi)", workers=n, threads=1, job_type="pi", tasks=tasks)})
    return {"title": "Carga CPU pura y GIL",
            "params": "24 tareas Monte Carlo de 1.000.000 de muestras", "rows": rows}


def e4_versions(reps: int) -> dict[str, Any]:
    scenarios = {
        "Homogeneo (3 nodos iguales)": dict(workers=3, threads=4, job_type="logs",
                                            tasks=build_log_job(360, 500, 50)),
        "Heterogeneo (w3 4x mas lento)": dict(workers=3, threads=4, job_type="logs",
                                              tasks=build_log_job(360, 500, 50),
                                              worker_args={"w3": ["--slowdown", "4"]}),
        "Tareas finas (2000 x ~0.5 ms)": dict(workers=3, threads=4, job_type="logs",
                                              tasks=build_log_job(2000, 50, 0)),
    }
    rows = []
    for scenario, kwargs in scenarios.items():
        for label, args in ABLATION.items():
            r = repeat(reps, f"{scenario[:18]} | {label}", coordinator_args=args, **kwargs)
            rows.append({"scenario": scenario, "config": label, **r})
    return {"title": "Version 1 vs. version 2 (ablacion)", "params": "3 nodos x 4 hilos", "rows": rows}


def e5_faults(reps: int) -> dict[str, Any]:
    tasks = build_log_job(600, 500, 50)
    faults: dict[str, Callable[[LocalCluster], None] | None] = {
        "Sin fallos": None,
        "Caida abrupta de w2 (kill)": lambda c: c.kill_worker("w2"),
        "Cuelgue de w2 (sin heartbeats)": lambda c: inject((c.host, c.port), "w2", "hang"),
    }
    rows = []
    for profile in ("baseline", "optimized"):
        for name, fault in faults.items():
            r = repeat(reps, f"{profile} | {name}", workers=3, threads=4, job_type="logs", tasks=tasks,
                       profile=profile, fault=fault, fault_at=1.0, watch_events=True)
            rows.append({"profile": profile, "fault": name, **r})
    return {"title": "Tolerancia a fallos de nodo",
            "params": "3 nodos x 4 hilos, 600 tareas, fallo inyectado en t = 1 s, timeout heartbeat 3 s",
            "rows": rows}


def e6_breaker(reps: int) -> dict[str, Any]:
    tasks = build_log_job(360, 500, 50)
    degraded = {"w3": ["--failure-rate", "1.0"]}
    configs = {
        "V1 (round-robin, sin protecciones)": ("baseline", []),
        "V2 sin breaker ni anti-afinidad": ("optimized", ["--no-breaker", "--no-retry-affinity"]),
        "V2 + anti-afinidad de reintentos": ("optimized", ["--no-breaker"]),
        "V2.1 + Circuit Breaker (final)": ("optimized", []),
    }
    rows = []
    for label, (profile, args) in configs.items():
        r = repeat(reps, label, workers=3, threads=4, job_type="logs", tasks=tasks, profile=profile,
                   coordinator_args=args, worker_args=degraded, watch_events=True)
        rows.append({"config": label, **r})
    return {"title": "Nodo degradado (w3 falla el 100 % de sus tareas)",
            "params": "3 nodos x 4 hilos, 360 tareas, max. 3 intentos por tarea", "rows": rows}


def e7_prefetch(reps: int) -> dict[str, Any]:
    """Sensibilidad al número de créditos (tareas en vuelo por hilo del nodo)."""
    scenarios = {
        "Heterogeneo (w3 4x mas lento)": dict(tasks=build_log_job(360, 500, 50),
                                              worker_args={"w3": ["--slowdown", "4"]}),
        "Tareas finas (2000 x ~0.5 ms)": dict(tasks=build_log_job(2000, 50, 0)),
    }
    rows = []
    for scenario, kwargs in scenarios.items():
        for prefetch in (1, 2, 4, 8):
            args = ["--strategy", "least_loaded", "--transport", "persistent", "--prefetch", str(prefetch),
                    "--batch-size", "8"]
            r = repeat(reps, f"{scenario[:18]} | creditos={prefetch}", workers=3, threads=4,
                       job_type="logs", coordinator_args=args, **kwargs)
            rows.append({"scenario": scenario, "prefetch": prefetch, **r})
    return {"title": "Sensibilidad a los creditos por hilo (prefetch)",
            "params": "3 nodos x 4 hilos, least-loaded, conexion persistente, lotes de 8", "rows": rows}


def e8_hot_faults(reps: int) -> dict[str, Any]:
    """Escenario que reveló el defecto del Circuit Breaker (fallos en caliente)."""
    def chaos(c: LocalCluster) -> None:
        inject((c.host, c.port), "w3", "hang")
        time.sleep(0.8)
        inject((c.host, c.port), "w4", "failure_rate", 1.0)
    r = repeat(max(reps, 5), "V2.1 | w3 colgado + w4 degradado en caliente", workers=4, threads=4,
               job_type="logs", tasks=build_log_job(1500, 500, 50), fault=chaos, fault_at=1.0,
               watch_events=True)
    return {"title": "Fallos combinados en caliente (validacion del ajuste v2.1)",
            "params": "4 nodos x 4 hilos, 1500 tareas; t=1 s cuelga w3; t=1,8 s w4 falla el 100 %",
            "before_fix_failed_per_run": [4, 0, 8, 3, 2], "rows": [r]}


EXPERIMENTS = {"E1": e1_threads, "E2": e2_nodes, "E3": e3_cpu, "E4": e4_versions,
               "E5": e5_faults, "E6": e6_breaker, "E7": e7_prefetch,
               "E8": e8_hot_faults}


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmarks de DistriTask")
    parser.add_argument("--only", default=",".join(EXPERIMENTS))
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--out", default=str(RESULTS / "benchmark_results.json"))
    args = parser.parse_args()
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    data: dict[str, Any] = json.loads(out.read_text(encoding="utf-8")) if out.exists() else {}
    data["environment"] = {
        "date": datetime.now().isoformat(timespec="seconds"), "python": platform.python_version(),
        "os": f"{platform.system()} {platform.release()}", "cpu": cpu_info(),
        "logical_cpus": os.cpu_count(), "reps": args.reps,
    }
    for key in [k.strip() for k in args.only.split(",") if k.strip()]:
        print(f"\n=== {key}: {EXPERIMENTS[key].__name__} ===", flush=True)
        started = time.time()
        data[key] = EXPERIMENTS[key](args.reps)
        data[key]["elapsed_s"] = round(time.time() - started, 1)
        out.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nResultados guardados en {out}")


if __name__ == "__main__":
    main()
