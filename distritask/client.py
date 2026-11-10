"""Cliente de DistriTask (proxy del coordinador del lado del cliente).

``CoordinatorClient`` encapsula el protocolo: el código de la aplicación solo
llama a ``submit(...)`` y recibe el resultado agregado (fase *reduce*) junto
con las métricas del trabajo.

Ejemplos:
    python -m distritask.client logs --chunks 300 --lines 2000 --io-latency 40
    python -m distritask.client logs --file servidor.log --chunk-lines 5000
    python -m distritask.client pi --tasks 40 --samples 250000
    python -m distritask.client stats
"""
from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from .protocol import (ERROR, JOB_ACCEPTED, JOB_DONE, STATS, STATS_REPLY, SUBMIT_JOB, Connection,
                       ProtocolError, parse_address)
from .tasks import build_log_job, build_log_job_from_file, build_pi_job, build_sleep_job


class CoordinatorClient:
    def __init__(self, host: str = "127.0.0.1", port: int = 5000, timeout: float = 5.0) -> None:
        self.address = (host, port)
        self.timeout = timeout
        self._conn: Connection | None = None

    def _connection(self) -> Connection:
        if self._conn is None or self._conn.closed:
            self._conn = Connection.connect(*self.address, timeout=self.timeout)
        return self._conn

    def submit(self, job_type: str, tasks: list[dict[str, Any]], *, job_id: str | None = None,
               timeout: float | None = None) -> dict[str, Any]:
        conn = self._connection()
        conn.send({"type": SUBMIT_JOB, "job_type": job_type, "tasks": tasks, "job_id": job_id, "wait": True})
        conn.settimeout(timeout)
        try:
            accepted = conn.recv()
            if accepted.get("type") == ERROR:
                raise ProtocolError(accepted.get("error"))
            if accepted.get("type") != JOB_ACCEPTED:
                raise ProtocolError(f"respuesta inesperada: {accepted.get('type')}")
            done = conn.recv()
            if done.get("type") != JOB_DONE:
                raise ProtocolError(f"respuesta inesperada: {done.get('type')}")
            return done
        finally:
            if not conn.closed:
                conn.settimeout(None)

    def stats(self) -> dict[str, Any]:
        conn = self._connection()
        conn.send({"type": STATS})
        reply = conn.recv()
        if reply.get("type") != STATS_REPLY:
            raise ProtocolError(f"respuesta inesperada: {reply.get('type')}")
        return reply

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "CoordinatorClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


# ---------------------------------------------------------------------------
# Presentación en consola (ASCII para que funcione en cualquier terminal)
# ---------------------------------------------------------------------------
def format_summary(summary: dict[str, Any]) -> str:
    m = summary["metrics"]
    lines = [
        "=" * 64,
        f" Trabajo {summary.get('job_id', '')} ({summary['job_type']}) finalizado",
        "=" * 64,
        f" Tareas: {m['done']}/{m['tasks']} completadas, {m['failed']} fallidas",
        f" Tiempo total (makespan): {m['makespan_s']:.3f} s   Throughput: {m['throughput_tps']:.1f} tareas/s",
        f" Latencia por tarea  p50={m['latency_p50_ms']:.0f} ms  p95={m['latency_p95_ms']:.0f} ms"
        f"  p99={m['latency_p99_ms']:.0f} ms",
        f" Intentos={m['attempts']}  reintentos={m['retries']}  re-encoladas={m['requeues']}"
        f"  duplicados={m['duplicates']}",
        " Reparto por nodo: " + ", ".join(f"{k}={v}" for k, v in m["per_worker"].items()),
        "-" * 64,
    ]
    result = summary.get("result", {})
    if summary["job_type"] == "logs" and "lines" in result:
        lines.append(f" Lineas analizadas: {result['lines']:,}  tasa de error: {result['error_rate_pct']}%")
        lines.append(" Niveles: " + ", ".join(f"{k}={v:,}" for k, v in result["levels"].items()))
        lines.append(" Servicio        peticiones  errores  lat.media  lat.max")
        for svc, s in result["services"].items():
            lines.append(f"  {svc:<15}{s['requests']:>10,}{s['errors']:>9,}{s['avg_latency_ms']:>9.1f}ms"
                         f"{s['max_latency_ms']:>7}ms")
        lines.append(" Errores mas frecuentes:")
        for msg, count in result["top_errors"]:
            lines.append(f"  {count:>7,}  {msg}")
    elif summary["job_type"] == "pi" and "pi_estimate" in result:
        lines.append(f" Muestras: {result['samples']:,}  pi ~= {result['pi_estimate']:.6f}"
                     f"  (error {result['abs_error']:.2e})")
    lines.append(" Config: " + json.dumps(summary.get("config", {})))
    return "\n".join(lines)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Cliente de DistriTask")
    parser.add_argument("--coordinator", default="127.0.0.1:5000")
    parser.add_argument("--json", action="store_true", help="imprime la respuesta completa en JSON")
    sub = parser.add_subparsers(dest="command", required=True)
    logs = sub.add_parser("logs", help="análisis distribuido de logs")
    logs.add_argument("--chunks", type=int, default=200)
    logs.add_argument("--lines", type=int, default=2000, help="líneas por chunk (sintético)")
    logs.add_argument("--io-latency", type=int, default=40, help="ms de descarga simulada por chunk")
    logs.add_argument("--file", help="analiza un archivo de log real en lugar de datos sintéticos")
    logs.add_argument("--chunk-lines", type=int, default=5000)
    pi = sub.add_parser("pi", help="estimación de pi por Monte Carlo (CPU)")
    pi.add_argument("--tasks", type=int, default=40)
    pi.add_argument("--samples", type=int, default=200_000)
    sleep = sub.add_parser("sleep", help="tareas de espera (pruebas)")
    sleep.add_argument("--tasks", type=int, default=100)
    sleep.add_argument("--ms", type=int, default=10)
    sub.add_parser("stats", help="estado actual del clúster")
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_arg_parser().parse_args(argv)
    host, port = parse_address(args.coordinator)
    with CoordinatorClient(host, port) as client:
        if args.command == "stats":
            print(json.dumps(client.stats(), indent=2, ensure_ascii=False))
            return
        if args.command == "logs":
            tasks = (build_log_job_from_file(args.file, args.chunk_lines) if args.file
                     else build_log_job(args.chunks, args.lines, args.io_latency))
        elif args.command == "pi":
            tasks = build_pi_job(args.tasks, args.samples)
        else:
            tasks = build_sleep_job(args.tasks, args.ms)
        print(f"Enviando trabajo '{args.command}' con {len(tasks)} tareas a {host}:{port} ...", flush=True)
        summary = client.submit(args.command, tasks)
        print(json.dumps(summary, indent=2, ensure_ascii=False) if args.json else format_summary(summary))


if __name__ == "__main__":
    try:
        main()
    except ConnectionRefusedError:
        sys.exit("No se pudo conectar con el coordinador. ¿Está en ejecución?")
