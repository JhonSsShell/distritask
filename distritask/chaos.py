"""Herramienta de ingeniería del caos: inyecta fallos en un nodo en caliente.

Consulta al coordinador la dirección del nodo y le envía un mensaje ``CHAOS``.

Ejemplos:
    python -m distritask.chaos w2 hang              # nodo colgado (sin heartbeats)
    python -m distritask.chaos w2 recover           # el nodo vuelve a responder
    python -m distritask.chaos w3 failure_rate 1.0  # nodo degradado: todo falla
    python -m distritask.chaos w1 slowdown 4        # nodo 4 veces más lento
"""
from __future__ import annotations

import argparse
import json
from typing import Any

from .client import CoordinatorClient
from .protocol import CHAOS, Connection, parse_address


def inject(coordinator: tuple[str, int], worker_id: str, action: str, value: Any = None) -> dict[str, Any]:
    with CoordinatorClient(*coordinator) as client:
        workers = {w["worker_id"]: w for w in client.stats()["workers"]}
    if worker_id not in workers:
        raise SystemExit(f"nodo {worker_id!r} no registrado. Disponibles: {sorted(workers)}")
    host, port = parse_address(workers[worker_id]["address"])
    with Connection.connect(host, port, timeout=3.0) as conn:
        conn.send({"type": CHAOS, "action": action, "value": value})
        return conn.recv()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Inyección de fallos en DistriTask")
    parser.add_argument("worker_id")
    parser.add_argument("action", choices=["hang", "recover", "failure_rate", "slowdown"])
    parser.add_argument("value", nargs="?", type=float)
    parser.add_argument("--coordinator", default="127.0.0.1:5000")
    args = parser.parse_args(argv)
    print(json.dumps(inject(parse_address(args.coordinator), args.worker_id, args.action, args.value),
                     ensure_ascii=False))


if __name__ == "__main__":
    main()
