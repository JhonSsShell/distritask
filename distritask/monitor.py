"""Monitor de consola: observador remoto del bus de eventos del coordinador.

Se suscribe por TCP (mensaje ``SUBSCRIBE``) y muestra los eventos en vivo.
Es la parte "distribuida" del patrón Observer: el sujeto (coordinador) y el
observador (este proceso) pueden estar en máquinas distintas.

Uso:  python -m distritask.monitor --coordinator 127.0.0.1:5000 [--stats]
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime
from typing import Any, Callable, Iterator

from .protocol import EVENT, SUBSCRIBE, Connection, ConnectionClosed, ProtocolError, parse_address

# task.requeued se omite por defecto (el evento worker.down ya informa cuántas);
# úsese --topics o --all para verlas una a una.
DEFAULT_TOPICS = ["worker.*", "breaker.*", "job.*", "task.retry", "task.failed"]

ICONS = {
    "worker.registered": "[+] NODO",
    "worker.down": "[X] NODO CAIDO",
    "worker.recovered": "[~] NODO RECUPERADO",
    "breaker.opened": "[!] CIRCUITO ABIERTO",
    "breaker.half_open": "[?] CIRCUITO SEMIABIERTO",
    "breaker.closed": "[=] CIRCUITO CERRADO",
    "job.submitted": "[>] TRABAJO",
    "job.completed": "[OK] TRABAJO",
    "task.retry": "  reintento",
    "task.requeued": "  re-encolada",
    "task.failed": "  FALLIDA",
}


def subscribe(host: str, port: int, topics: list[str]) -> Iterator[dict[str, Any]]:
    conn = Connection.connect(host, port)
    conn.send({"type": SUBSCRIBE, "topics": topics})
    try:
        while True:
            message = conn.recv()
            if message.get("type") == EVENT:
                yield message
    finally:
        conn.close()


def format_event(event: dict[str, Any]) -> str:
    ts = datetime.fromtimestamp(event["ts"]).strftime("%H:%M:%S.%f")[:-3]
    topic, data = event["topic"], event["data"]
    if topic == "stats":
        workers = " | ".join(
            f"{w['worker_id']}:{w['state']}/{w['breaker'][:4]} {w['inflight']}/{w['limit']} ok={w['completed']}"
            for w in data["workers"])
        return (f"{ts} [stats] {data['throughput_tps']:>7.1f} t/s  pendientes={data['pending']:<5}"
                f" en_vuelo={data['inflight']:<4} | {workers}")
    label = ICONS.get(topic, topic)
    if topic == "job.completed":
        details = (f"{data['job_id']}: {data['done']}/{data['tasks']} en {data['makespan_s']:.2f}s "
                   f"({data['throughput_tps']:.1f} t/s) reintentos={data['retries']} "
                   f"re-encoladas={data['requeues']}")
    else:
        details = " ".join(f"{k}={v}" for k, v in data.items())
    return f"{ts} {label} {details}"


def run(host: str, port: int, topics: list[str], printer: Callable[[str], None] = print,
        stop: Callable[[], bool] = lambda: False, reconnect: bool = True) -> None:
    while not stop():
        try:
            for event in subscribe(host, port, topics):
                printer(format_event(event))
                if stop():
                    return
        except (ConnectionClosed, OSError, ProtocolError):
            if not reconnect or stop():
                return
            printer("... conexión con el coordinador perdida; reintentando")
            time.sleep(1.0)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Monitor de eventos de DistriTask")
    parser.add_argument("--coordinator", default="127.0.0.1:5000")
    parser.add_argument("--topics", default=",".join(DEFAULT_TOPICS))
    parser.add_argument("--stats", action="store_true", help="incluye instantáneas periódicas")
    parser.add_argument("--all", action="store_true", help="todos los eventos (incluye task.completed)")
    args = parser.parse_args(argv)
    topics = ["*"] if args.all else [t.strip() for t in args.topics.split(",") if t.strip()]
    if args.stats and "*" not in topics:
        topics.append("stats")
    host, port = parse_address(args.coordinator)
    print(f"Suscrito a {topics} en {host}:{port}. Ctrl+C para salir.", flush=True)
    try:
        run(host, port, topics, printer=lambda s: print(s, flush=True))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    sys.exit(main())
