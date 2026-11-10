"""Nodo trabajador de DistriTask.

Cada nodo es un proceso independiente (en producción, una VM o contenedor)
con dos planos de comunicación:

* **Plano de control** (nodo -> coordinador): se registra y envía un
  *heartbeat* cada ``heartbeat_interval`` segundos con sus estadísticas. Si el
  coordinador no está disponible, reintenta con *backoff* exponencial.
* **Plano de datos** (coordinador -> nodo): un servidor TCP recibe lotes de
  tareas (``EXECUTE``) y devuelve un ``RESULT`` por tarea en cuanto termina.

Las tareas se ejecutan en un **pool de hilos** (``ThreadPoolExecutor``) o, para
cargas CPU-intensivas, en un pool de procesos que evita el GIL.

Opciones de inyección de fallos para las pruebas de robustez:
``--slowdown`` (nodo lento), ``--failure-rate`` (nodo degradado) y
``--hang-after`` (nodo colgado / partición de red: deja de enviar heartbeats
y resultados sin cerrar sus conexiones).

Uso:  python -m distritask.worker --id w1 --coordinator 127.0.0.1:5000 --threads 4
"""
from __future__ import annotations

import argparse
import logging
import os
import socket
import threading
import time
from concurrent.futures import Executor, Future, ProcessPoolExecutor, ThreadPoolExecutor
from typing import Any

from .protocol import (CHAOS, CHAOS_ACK, EXECUTE, HEARTBEAT, REGISTER, RESULT, Connection,
                       ConnectionClosed, ProtocolError, parse_address)
from .tasks import run_task, seed_process

log = logging.getLogger("distritask.worker")


class WorkerNode:
    def __init__(self, worker_id: str, coordinator: tuple[str, int], *, bind_host: str = "127.0.0.1",
                 port: int = 0, advertise_host: str | None = None, threads: int = 4,
                 executor: str = "thread", slowdown: float = 1.0, failure_rate: float = 0.0,
                 hang_after: float | None = None, heartbeat_interval: float = 1.0) -> None:
        self.worker_id = worker_id
        self.coordinator = coordinator
        self.bind_host = bind_host
        self.port = port
        self.advertise_host = advertise_host
        self.threads = threads
        self.executor_kind = executor
        self.slowdown = slowdown
        self.failure_rate = failure_rate
        self.hang_after = hang_after
        self.heartbeat_interval = heartbeat_interval
        self._stop = threading.Event()
        self._hung = threading.Event()
        self._stats_lock = threading.Lock()
        self.active = 0
        self.completed = 0
        self.failed = 0
        self._server: socket.socket | None = None
        self._pool: Executor | None = None

    # -- ciclo de vida --------------------------------------------------------
    def start(self) -> "WorkerNode":
        self._server = socket.create_server((self.bind_host, self.port), backlog=512)
        self.port = self._server.getsockname()[1]
        if self.executor_kind == "process":
            self._pool = ProcessPoolExecutor(max_workers=self.threads, initializer=seed_process)
        else:
            self._pool = ThreadPoolExecutor(max_workers=self.threads,
                                            thread_name_prefix=f"{self.worker_id}-exec")
        threading.Thread(target=self._accept_loop, name="data-acceptor", daemon=True).start()
        threading.Thread(target=self._control_loop, name="control", daemon=True).start()
        if self.hang_after:
            timer = threading.Timer(self.hang_after, self._hang)
            timer.daemon = True
            timer.start()
        log.info("nodo %s escuchando en %s:%s | %s x%d | slowdown=%.1f failure_rate=%.2f",
                 self.worker_id, self.bind_host, self.port, self.executor_kind, self.threads,
                 self.slowdown, self.failure_rate)
        return self

    def stop(self) -> None:
        self._stop.set()
        if self._server:
            try:
                self._server.close()
            except OSError:
                pass
        if self._pool:
            self._pool.shutdown(wait=False, cancel_futures=True)

    def serve_forever(self) -> None:
        self.start()
        try:
            while not self._stop.is_set():
                time.sleep(0.5)
        except KeyboardInterrupt:
            log.info("deteniendo nodo %s...", self.worker_id)
        finally:
            self.stop()

    def _hang(self) -> None:
        log.warning("nodo %s COLGADO (simulación): deja de enviar heartbeats y resultados",
                    self.worker_id)
        self._hung.set()

    def apply_chaos(self, message: dict[str, Any]) -> dict[str, Any]:
        """Inyección de fallos en caliente (ingeniería del caos)."""
        action, value = message.get("action"), message.get("value")
        if action == "hang":
            self._hang()
        elif action == "recover":
            log.warning("nodo %s RECUPERADO (simulación)", self.worker_id)
            self._hung.clear()
        elif action == "failure_rate":
            self.failure_rate = float(value)
            log.warning("nodo %s: tasa de fallos = %.2f", self.worker_id, self.failure_rate)
        elif action == "slowdown":
            self.slowdown = float(value)
            log.warning("nodo %s: factor de lentitud = %.1f", self.worker_id, self.slowdown)
        else:
            return {"ok": False, "error": f"acción desconocida: {action}"}
        return {"ok": True, "worker_id": self.worker_id, "action": action, "value": value}

    # -- plano de datos -----------------------------------------------------
    def _accept_loop(self) -> None:
        assert self._server is not None
        while not self._stop.is_set():
            try:
                sock, _ = self._server.accept()
            except OSError:
                break
            threading.Thread(target=self._serve_data, args=(Connection(sock),),
                             name="data-conn", daemon=True).start()

    def _serve_data(self, conn: Connection) -> None:
        try:
            while True:
                message = conn.recv()
                kind = message.get("type")
                if kind == EXECUTE:
                    for task in message.get("tasks", []):
                        self._submit(conn, task)
                elif kind == CHAOS:
                    conn.send({"type": CHAOS_ACK, **self.apply_chaos(message)})
        except (ConnectionClosed, OSError, ProtocolError):
            pass
        finally:
            # En modo una-conexión-por-tarea el coordinador cierra tras recibir
            # el resultado; aquí solo liberamos el socket.
            conn.close()

    def _submit(self, conn: Connection, task: dict[str, Any]) -> None:
        assert self._pool is not None
        with self._stats_lock:
            self.active += 1
        try:
            future = self._pool.submit(run_task, task, self.slowdown, self.failure_rate)
        except RuntimeError as exc:  # pool cerrado
            self._finish(conn, task, None, exc)
            return
        future.add_done_callback(lambda f: self._finish(conn, task, f, None))

    def _finish(self, conn: Connection, task: dict[str, Any], future: Future | None,
                error: Exception | None) -> None:
        message: dict[str, Any] = {"type": RESULT, "task_id": task["task_id"], "worker_id": self.worker_id}
        try:
            if future is None:
                raise error or RuntimeError("tarea no ejecutada")
            outcome = future.result()
            message.update(status="ok", result=outcome["result"], duration_ms=outcome["duration_ms"])
            ok = True
        except Exception as exc:
            message.update(status="error", error=f"{exc.__class__.__name__}: {exc}", duration_ms=0.0)
            ok = False
        with self._stats_lock:
            self.active -= 1
            if ok:
                self.completed += 1
            else:
                self.failed += 1
        if self._hung.is_set():
            return  # partición simulada: el resultado nunca llega
        try:
            conn.send(message)
        except OSError:
            log.debug("no se pudo devolver %s (conexión cerrada)", task["task_id"])

    # -- plano de control ------------------------------------------------------
    def _control_loop(self) -> None:
        backoff = 0.5
        while not self._stop.is_set():
            try:
                with Connection.connect(*self.coordinator, timeout=3.0) as conn:
                    conn.send({"type": REGISTER, "worker_id": self.worker_id,
                               "host": self.advertise_host, "port": self.port,
                               "capacity": self.threads, "executor": self.executor_kind,
                               "pid": os.getpid()})
                    conn.recv()  # REGISTERED
                    log.info("registrado en el coordinador %s:%s", *self.coordinator)
                    backoff = 0.5
                    while not self._stop.is_set():
                        if not self._hung.is_set():
                            conn.send({"type": HEARTBEAT, "worker_id": self.worker_id,
                                       "stats": self.stats()})
                        self._stop.wait(self.heartbeat_interval)
            except (OSError, ProtocolError) as exc:
                if self._stop.is_set():
                    break
                log.warning("coordinador no disponible (%s); reintento en %.1fs",
                            exc.__class__.__name__, backoff)
                self._stop.wait(backoff)
                backoff = min(backoff * 2, 5.0)

    def stats(self) -> dict[str, Any]:
        with self._stats_lock:
            return {"active": self.active, "completed": self.completed, "failed": self.failed,
                    "threads": self.threads, "executor": self.executor_kind}


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Nodo trabajador de DistriTask")
    parser.add_argument("--id", dest="worker_id", default=f"w-{socket.gethostname()}-{os.getpid()}")
    parser.add_argument("--coordinator", default=os.environ.get("COORDINATOR", "127.0.0.1:5000"),
                        help="host:puerto del coordinador")
    parser.add_argument("--bind", default="127.0.0.1", help="interfaz de escucha del plano de datos")
    parser.add_argument("--port", type=int, default=0, help="0 = puerto libre automático")
    parser.add_argument("--advertise-host", default=None,
                        help="host que el coordinador usará para conectarse (por defecto, la IP de origen)")
    parser.add_argument("--threads", type=int, default=4, help="tamaño del pool de hilos/procesos")
    parser.add_argument("--executor", choices=["thread", "process"], default="thread")
    parser.add_argument("--slowdown", type=float, default=1.0, help="factor de lentitud simulada (>1)")
    parser.add_argument("--failure-rate", type=float, default=0.0, help="probabilidad de fallo por tarea")
    parser.add_argument("--hang-after", type=float, default=None, help="segundos hasta simular un cuelgue")
    parser.add_argument("--heartbeat-interval", type=float, default=1.0)
    parser.add_argument("--log-level", default="INFO")
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_arg_parser().parse_args(argv)
    logging.basicConfig(level=args.log_level.upper(),
                        format=f"%(asctime)s %(levelname)-5s [{args.worker_id}] %(message)s",
                        datefmt="%H:%M:%S")
    WorkerNode(args.worker_id, parse_address(args.coordinator), bind_host=args.bind, port=args.port,
               advertise_host=args.advertise_host, threads=args.threads, executor=args.executor,
               slowdown=args.slowdown, failure_rate=args.failure_rate, hang_after=args.hang_after,
               heartbeat_interval=args.heartbeat_interval).serve_forever()


if __name__ == "__main__":
    main()
