"""Demostración guiada de DistriTask en un solo comando (ideal para el video).

Lanza un coordinador y N nodos como procesos independientes, muestra en vivo
los eventos del bus (Observer distribuido), envía un trabajo y, opcionalmente,
inyecta un fallo a mitad de la ejecución.

Ejemplos:
    python scripts/demo.py                              # 3 nodos, análisis de logs
    python scripts/demo.py --dashboard --pause          # abre el panel web y espera Enter
    python scripts/demo.py --kill w2 --at 1.0           # caída abrupta de un nodo
    python scripts/demo.py --hang w2 --at 1.0 --recover-after 4
    python scripts/demo.py --degraded w3                # nodo degradado -> Circuit Breaker
    python scripts/demo.py --profile baseline --slow w3 # versión 1 con un nodo lento
    python scripts/demo.py --job pi --executor process  # carga CPU con pool de procesos
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import threading
import time
import webbrowser
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from distritask import monitor  # noqa: E402
from distritask.chaos import inject  # noqa: E402
from distritask.client import format_summary  # noqa: E402
from distritask.cluster import LocalCluster  # noqa: E402
from distritask.tasks import build_log_job, build_pi_job  # noqa: E402


def banner(text: str) -> None:
    print("\n" + "#" * 70 + f"\n#  {text}\n" + "#" * 70, flush=True)


def main() -> None:
    p = argparse.ArgumentParser(description="Demo de DistriTask")
    p.add_argument("--workers", type=int, default=3)
    p.add_argument("--threads", type=int, default=4)
    p.add_argument("--executor", choices=["thread", "process"], default="thread")
    p.add_argument("--profile", choices=["baseline", "optimized"], default="optimized")
    p.add_argument("--job", choices=["logs", "pi"], default="logs")
    p.add_argument("--chunks", type=int, default=400)
    p.add_argument("--lines", type=int, default=1000)
    p.add_argument("--io-latency", type=int, default=50)
    p.add_argument("--pi-tasks", type=int, default=60)
    p.add_argument("--samples", type=int, default=500_000)
    p.add_argument("--port", type=int, default=5000)
    p.add_argument("--kill", metavar="WORKER", help="mata el proceso de este nodo durante el trabajo")
    p.add_argument("--hang", metavar="WORKER", help="cuelga este nodo durante el trabajo")
    p.add_argument("--recover-after", type=float, help="segundos tras el cuelgue para recuperarlo")
    p.add_argument("--degraded", metavar="WORKER", help="nodo vivo pero con 100%% de fallos")
    p.add_argument("--slow", metavar="WORKER", help="nodo 4 veces más lento")
    p.add_argument("--at", type=float, default=1.0, help="segundo del trabajo en que se inyecta el fallo")
    p.add_argument("--dashboard", action="store_true", help="lanza el panel web en http://127.0.0.1:8080")
    p.add_argument("--pause", action="store_true", help="espera Enter antes de enviar el trabajo")
    p.add_argument("--keep", action="store_true", help="mantiene el clúster vivo al terminar")
    p.add_argument("--stats", action="store_true", help="muestra instantáneas periódicas del clúster")
    args = p.parse_args()

    worker_args: dict[str, list[str]] = {}
    if args.degraded:
        worker_args.setdefault(args.degraded, []).extend(["--failure-rate", "1.0"])
    if args.slow:
        worker_args.setdefault(args.slow, []).extend(["--slowdown", "4"])

    run_dir = ROOT / "runs" / datetime.now().strftime("%Y%m%d-%H%M%S")
    banner(f"Iniciando clúster: 1 coordinador + {args.workers} nodos x {args.threads} "
           f"{'hilos' if args.executor == 'thread' else 'procesos'} | perfil {args.profile}")
    cluster = LocalCluster(args.workers, args.threads, args.profile, worker_args=worker_args,
                           executor=args.executor, port=args.port, log_dir=run_dir, log_level="INFO")
    dashboard = None
    stop_monitor = threading.Event()
    try:
        cluster.start()
        print(f"Coordinador en {cluster.address}. Logs de cada proceso en {run_dir.relative_to(ROOT)}",
              flush=True)
        topics = list(monitor.DEFAULT_TOPICS) + (["stats"] if args.stats else [])
        threading.Thread(target=monitor.run, daemon=True, kwargs=dict(
            host=cluster.host, port=cluster.port, topics=topics,
            printer=lambda s: print(s, flush=True), stop=stop_monitor.is_set)).start()
        if args.dashboard:
            dashboard = subprocess.Popen([sys.executable, "-m", "distritask.dashboard",
                                          "--coordinator", cluster.address], cwd=ROOT,
                                         stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
            time.sleep(0.8)
            webbrowser.open("http://127.0.0.1:8080")
        time.sleep(0.5)
        if args.pause:
            input("\n>>> Clúster listo. Presiona Enter para enviar el trabajo... ")

        if args.job == "logs":
            tasks = build_log_job(args.chunks, args.lines, args.io_latency)
        else:
            tasks = build_pi_job(args.pi_tasks, args.samples)
        banner(f"Enviando trabajo '{args.job}' con {len(tasks)} tareas")

        def chaos() -> None:
            if args.kill:
                print(f"\n>>> [CAOS] Matando el proceso del nodo {args.kill}\n", flush=True)
                cluster.kill_worker(args.kill)
            if args.hang:
                print(f"\n>>> [CAOS] Colgando el nodo {args.hang} (deja de enviar heartbeats)\n", flush=True)
                inject((cluster.host, cluster.port), args.hang, "hang")
                if args.recover_after:
                    time.sleep(args.recover_after)
                    print(f"\n>>> [CAOS] Recuperando el nodo {args.hang}\n", flush=True)
                    inject((cluster.host, cluster.port), args.hang, "recover")

        if args.kill or args.hang:
            threading.Timer(args.at, chaos).start()
        with cluster.client() as client:
            summary = client.submit(args.job, tasks)
        time.sleep(0.3)
        print("\n" + format_summary(summary), flush=True)
        if args.keep:
            input("\n>>> Clúster en ejecución. Presiona Enter para detenerlo... ")
    except KeyboardInterrupt:
        pass
    finally:
        stop_monitor.set()
        if dashboard:
            dashboard.terminate()
        cluster.stop()


if __name__ == "__main__":
    main()
