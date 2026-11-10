"""Patrón Proxy remoto.

``WorkerProxy`` es el representante local, dentro del coordinador, de un
nodo trabajador remoto. El coordinador lo usa como si fuera un objeto local
(``reserve``/``send``/``free_slots``) y el proxy oculta:

* el transporte TCP y la serialización de mensajes;
* la política de conexiones (``per_task`` = una conexión por tarea, versión 1;
  ``persistent`` = una conexión multiplexada con lotes, versión 2);
* el control de flujo por créditos (máximo de tareas en vuelo);
* el Circuit Breaker que aísla al nodo si se degrada;
* métricas del nodo (completadas, fallidas, latencia EWMA).

``CoordinatorClient`` (en ``client.py``) es el proxy equivalente del lado del
cliente.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable

from .circuit_breaker import CircuitBreaker
from .protocol import EXECUTE, RESULT, Connection, ConnectionClosed, ProtocolError

log = logging.getLogger("distritask.proxy")

ResultCallback = Callable[["WorkerProxy", dict[str, Any]], None]
TransportErrorCallback = Callable[["WorkerProxy", list[str], Exception], None]
DisconnectCallback = Callable[["WorkerProxy", Exception], None]

UP = "UP"
DOWN = "DOWN"


class WorkerProxy:
    def __init__(self, worker_id: str, host: str, port: int, capacity: int, *,
                 transport: str, prefetch: int, breaker: CircuitBreaker,
                 on_result: ResultCallback, on_transport_error: TransportErrorCallback,
                 on_disconnect: DisconnectCallback, connect_timeout: float = 2.0,
                 task_timeout: float = 60.0) -> None:
        if transport not in ("per_task", "persistent"):
            raise ValueError("transport debe ser 'per_task' o 'persistent'")
        self.worker_id = worker_id
        self.host = host
        self.port = port
        self.capacity = max(1, capacity)
        self.transport = transport
        self.prefetch = max(1, prefetch)
        self.breaker = breaker
        self.connect_timeout = connect_timeout
        self.task_timeout = task_timeout
        self._on_result = on_result
        self._on_transport_error = on_transport_error
        self._on_disconnect = on_disconnect
        # Estado protegido por el candado del coordinador:
        self.state = UP
        self.inflight: dict[str, float] = {}
        self.completed = 0
        self.failed = 0
        self.ewma_latency_ms = 0.0
        self.remote_stats: dict[str, Any] = {}
        self.registered_at = time.time()
        # Conexión persistente
        self._conn: Connection | None = None
        self._conn_lock = threading.Lock()
        self._closed = False

    # -- capacidad / control de flujo ---------------------------------------
    @property
    def limit(self) -> int:
        """Créditos: tareas que pueden estar en vuelo hacia este nodo."""
        return self.capacity * self.prefetch

    def free_slots(self) -> int:
        return max(0, self.limit - len(self.inflight))

    def available_slots(self) -> int:
        if self.state != UP:
            return 0
        return self.breaker.allowed_slots(self.free_slots())

    def reserve(self, task_ids: list[str]) -> None:
        now = time.monotonic()
        for task_id in task_ids:
            self.inflight[task_id] = now
        self.breaker.on_dispatch()

    def observe_latency(self, duration_ms: float) -> None:
        alpha = 0.2
        self.ewma_latency_ms = duration_ms if self.ewma_latency_ms == 0 else (
            (1 - alpha) * self.ewma_latency_ms + alpha * duration_ms)

    # -- envío (se invoca FUERA del candado del coordinador) ------------------
    def send(self, tasks: list[dict[str, Any]]) -> None:
        if self.transport == "persistent":
            self._send_persistent(tasks)
        else:
            for task in tasks:
                threading.Thread(target=self._send_one_shot, args=(task,),
                                 name=f"oneshot-{self.worker_id}", daemon=True).start()

    def _send_persistent(self, tasks: list[dict[str, Any]]) -> None:
        try:
            conn = self._ensure_connection()
            conn.send({"type": EXECUTE, "tasks": tasks})
        except (OSError, ProtocolError) as exc:
            self.drop_connection()
            self._on_transport_error(self, [t["task_id"] for t in tasks], exc)

    def _ensure_connection(self) -> Connection:
        with self._conn_lock:
            if self._closed:
                raise ConnectionClosed("proxy cerrado")
            if self._conn is None or self._conn.closed:
                conn = Connection.connect(self.host, self.port, timeout=self.connect_timeout)
                self._conn = conn
                threading.Thread(target=self._reader, args=(conn,),
                                 name=f"reader-{self.worker_id}", daemon=True).start()
            return self._conn

    def _reader(self, conn: Connection) -> None:
        try:
            while True:
                message = conn.recv()
                if message.get("type") == RESULT:
                    self._on_result(self, message)
        except (ConnectionClosed, OSError, ProtocolError) as exc:
            with self._conn_lock:
                lost_current = self._conn is conn and not self._closed
                if self._conn is conn:
                    self._conn = None
            conn.close()
            if lost_current:
                self._on_disconnect(self, exc)

    def _send_one_shot(self, task: dict[str, Any]) -> None:
        """Versión 1: abre, envía, espera y cierra una conexión por tarea."""
        try:
            with Connection.connect(self.host, self.port, timeout=self.connect_timeout) as conn:
                conn.settimeout(self.task_timeout)
                conn.send({"type": EXECUTE, "tasks": [task]})
                message = conn.recv()
            self._on_result(self, message)
        except (OSError, ProtocolError) as exc:
            self._on_transport_error(self, [task["task_id"]], exc)

    def drop_connection(self) -> None:
        with self._conn_lock:
            conn, self._conn = self._conn, None
        if conn:
            conn.close()

    def close(self) -> None:
        with self._conn_lock:
            self._closed = True
            conn, self._conn = self._conn, None
        if conn:
            conn.close()

    # -- representación -------------------------------------------------------
    def snapshot(self) -> dict[str, Any]:
        return {
            "worker_id": self.worker_id,
            "address": f"{self.host}:{self.port}",
            "state": self.state,
            "breaker": self.breaker.state.value,
            "breaker_opens": self.breaker.open_count,
            "capacity": self.capacity,
            "limit": self.limit,
            "inflight": len(self.inflight),
            "completed": self.completed,
            "failed": self.failed,
            "ewma_latency_ms": round(self.ewma_latency_ms, 1),
            "remote": self.remote_stats,
        }

    def __repr__(self) -> str:  # pragma: no cover
        return f"WorkerProxy({self.worker_id}@{self.host}:{self.port}, {self.state})"
