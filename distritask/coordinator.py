"""Coordinador (nodo maestro) de DistriTask - patrón Mediator.

El coordinador es el único componente que conoce a todos los demás. Clientes,
nodos trabajadores y monitores no se comunican entre sí: todo pasa por aquí,
lo que reduce el acoplamiento de *n x m* a *n + m* conexiones.

Hilos del coordinador:

* ``acceptor``      - acepta conexiones TCP y crea un hilo por conexión;
* ``dispatcher``    - productor/consumidor: saca tareas de la cola pendiente y
                       las envía al nodo elegido por la estrategia (Strategy);
* ``heartbeat``     - detecta nodos caídos o colgados por ausencia de latidos;
* ``stats``         - publica cada ``stats_interval`` una instantánea del estado;
* un hilo por conexión de cliente, de nodo (plano de control) y de monitor.

Uso:  python -m distritask.coordinator --port 5000 --profile optimized
"""
from __future__ import annotations

import argparse
import logging
import socket
import threading
import time
import uuid
from collections import deque
from dataclasses import asdict, dataclass, field
from typing import Any

from .circuit_breaker import BreakerState, CircuitBreaker
from .events import EventBus, LogObserver, RemoteSubscriber
from .metrics import MetricsCollector, percentile
from .protocol import (ERROR, HEARTBEAT, JOB_ACCEPTED, JOB_DONE, REGISTER, REGISTERED,
                       STATS, STATS_REPLY, SUBMIT_JOB, SUBSCRIBE, Connection, ConnectionClosed,
                       ProtocolError)
from .proxy import DOWN, UP, WorkerProxy
from .strategies import make_strategy
from .tasks import reduce_results

log = logging.getLogger("distritask.coordinator")

PENDING, DISPATCHED, DONE, FAILED = "PENDING", "DISPATCHED", "DONE", "FAILED"

PROFILES: dict[str, dict[str, Any]] = {
    # Versión 1 (línea base): decisiones "ingenuas" previas a la evaluación.
    "baseline": dict(strategy="round_robin", transport="per_task", prefetch=8,
                     batch_size=1, breaker_enabled=False, retry_affinity=False),
    # Versión 2 (optimizada): ajustes derivados de las pruebas.
    "optimized": dict(strategy="least_loaded", transport="persistent", prefetch=2,
                      batch_size=8, breaker_enabled=True, retry_affinity=True),
}


@dataclass
class CoordinatorConfig:
    host: str = "127.0.0.1"
    port: int = 5000
    strategy: str = "least_loaded"
    transport: str = "persistent"
    prefetch: int = 2
    batch_size: int = 8
    breaker_enabled: bool = True
    retry_affinity: bool = True      # evita reintentar en el nodo donde ya falló
    breaker_threshold: int = 3
    breaker_reset_s: float = 2.0
    heartbeat_timeout_s: float = 3.0
    max_attempts: int = 3
    stats_interval_s: float = 0.5
    task_timeout_s: float = 60.0
    verbose_events: bool = False

    @classmethod
    def from_profile(cls, profile: str, **overrides: Any) -> "CoordinatorConfig":
        values = dict(PROFILES[profile])
        values.update({k: v for k, v in overrides.items() if v is not None})
        return cls(**values)


@dataclass
class TaskRecord:
    task_id: str
    job_id: str
    spec: dict[str, Any]
    submitted_at: float
    status: str = PENDING
    attempts: int = 0
    failures: int = 0
    worker_id: str | None = None
    dispatched_at: float | None = None
    completed_at: float | None = None
    result: Any = None
    error: str | None = None
    failed_on: set[str] = field(default_factory=set)


@dataclass
class JobRecord:
    job_id: str
    job_type: str
    task_ids: list[str]
    submitted_at: float
    done: int = 0
    failed: int = 0
    retries: int = 0
    requeues: int = 0
    duplicates: int = 0
    completed_at: float | None = None
    summary: dict[str, Any] | None = None
    finished: threading.Event = field(default_factory=threading.Event)

    @property
    def total(self) -> int:
        return len(self.task_ids)


class Coordinator:
    def __init__(self, config: CoordinatorConfig) -> None:
        self.config = config
        self.bus = EventBus()
        self.metrics = MetricsCollector()
        self.bus.subscribe(self.metrics)
        if config.verbose_events:
            self.bus.subscribe(LogObserver(log), ["worker.*", "breaker.*", "job.*", "task.requeued"])
        self.strategy = make_strategy(config.strategy)
        self._lock = threading.RLock()
        self._cond = threading.Condition(self._lock)
        self._pending: deque[str] = deque()
        self._tasks: dict[str, TaskRecord] = {}
        self._jobs: dict[str, JobRecord] = {}
        self._workers: dict[str, WorkerProxy] = {}
        self._last_heartbeat: dict[str, float] = {}
        self._subscribers: set[RemoteSubscriber] = set()
        self._running = threading.Event()
        self._server: socket.socket | None = None
        self._threads: list[threading.Thread] = []
        self._started_at = time.time()
        self.port: int | None = None

    # ======================================================================
    # Ciclo de vida
    # ======================================================================
    def start(self) -> "Coordinator":
        self._server = socket.create_server((self.config.host, self.config.port), backlog=512)
        self.port = self._server.getsockname()[1]
        self._running.set()
        for name, target in (("acceptor", self._accept_loop), ("dispatcher", self._dispatch_loop),
                             ("heartbeat", self._heartbeat_loop), ("stats", self._stats_loop)):
            thread = threading.Thread(target=target, name=name, daemon=True)
            thread.start()
            self._threads.append(thread)
        log.info("coordinador escuchando en %s:%s | estrategia=%s transporte=%s prefetch=%s "
                 "lote=%s breaker=%s", self.config.host, self.port, self.config.strategy,
                 self.config.transport, self.config.prefetch, self.config.batch_size,
                 "on" if self.config.breaker_enabled else "off")
        return self

    def stop(self) -> None:
        self._running.clear()
        if self._server:
            try:
                self._server.close()
            except OSError:
                pass
        with self._cond:
            workers = list(self._workers.values())
            subscribers = list(self._subscribers)
            self._cond.notify_all()
        for proxy in workers:
            proxy.close()
        for sub in subscribers:
            sub.close()

    def serve_forever(self) -> None:
        self.start()
        try:
            while self._running.is_set():
                time.sleep(0.5)
        except KeyboardInterrupt:
            log.info("deteniendo coordinador...")
        finally:
            self.stop()

    # ======================================================================
    # Red: aceptación y enrutamiento por rol
    # ======================================================================
    def _accept_loop(self) -> None:
        assert self._server is not None
        while self._running.is_set():
            try:
                sock, _addr = self._server.accept()
            except OSError:
                break
            threading.Thread(target=self._handle_connection, args=(Connection(sock),),
                             name="conn", daemon=True).start()

    def _handle_connection(self, conn: Connection) -> None:
        try:
            first = conn.recv()
            kind = first.get("type")
            if kind == REGISTER:
                self._serve_worker_control(conn, first)
            elif kind in (SUBMIT_JOB, STATS):
                self._serve_client(conn, first)
            elif kind == SUBSCRIBE:
                self._serve_subscriber(conn, first)
                return  # el RemoteSubscriber es dueño de la conexión
            else:
                conn.send({"type": ERROR, "error": f"mensaje inicial inesperado: {kind}"})
        except (ConnectionClosed, OSError, ProtocolError):
            pass
        except Exception:  # pragma: no cover - defensa
            log.exception("error atendiendo conexión de %s", conn.peer)
        conn.close()

    # -- clientes -----------------------------------------------------------
    def _serve_client(self, conn: Connection, message: dict[str, Any]) -> None:
        while True:
            kind = message.get("type")
            if kind == SUBMIT_JOB:
                job = self.submit_job(message.get("job_type", "generic"), message["tasks"],
                                      message.get("job_id"))
                conn.send({"type": JOB_ACCEPTED, "job_id": job.job_id, "tasks": job.total})
                if message.get("wait", True):
                    job.finished.wait()
                    conn.send({"type": JOB_DONE, "job_id": job.job_id, **(job.summary or {})})
            elif kind == STATS:
                conn.send({"type": STATS_REPLY, **self.snapshot()})
            else:
                conn.send({"type": ERROR, "error": f"mensaje de cliente desconocido: {kind}"})
            message = conn.recv()

    # -- nodos: plano de control (registro + heartbeats) ----------------------
    def _serve_worker_control(self, conn: Connection, message: dict[str, Any]) -> None:
        worker_id = message["worker_id"]
        host = message.get("host") or conn.peer[0]
        proxy = self._make_proxy(worker_id, host, int(message["port"]), int(message.get("capacity", 1)))
        self._register_worker(proxy, message)
        conn.send({"type": REGISTERED, "worker_id": worker_id,
                   "heartbeat_timeout_s": self.config.heartbeat_timeout_s})
        try:
            while True:
                beat = conn.recv()
                if beat.get("type") != HEARTBEAT:
                    continue
                revived = False
                with self._cond:
                    if self._workers.get(worker_id) is not proxy:
                        return  # este proxy fue reemplazado por un re-registro
                    self._last_heartbeat[worker_id] = time.monotonic()
                    proxy.remote_stats = beat.get("stats", {})
                    if proxy.state == DOWN:  # el nodo volvió (p. ej., partición sanada)
                        proxy.state = UP
                        revived = True
                        self._cond.notify_all()
                if revived:
                    self.bus.publish("worker.recovered", worker_id=worker_id)
        except (ConnectionClosed, OSError, ProtocolError):
            self._fail_worker(proxy, "conexión de control cerrada")

    def _make_proxy(self, worker_id: str, host: str, port: int, capacity: int) -> WorkerProxy:
        def on_breaker_change(old: BreakerState, new: BreakerState) -> None:
            topic = {BreakerState.OPEN: "breaker.opened", BreakerState.HALF_OPEN: "breaker.half_open",
                     BreakerState.CLOSED: "breaker.closed"}[new]
            self.bus.publish(topic, worker_id=worker_id, previous=old.value)

        breaker = CircuitBreaker(self.config.breaker_threshold, self.config.breaker_reset_s,
                                 enabled=self.config.breaker_enabled, on_change=on_breaker_change)
        return WorkerProxy(worker_id, host, port, capacity, transport=self.config.transport,
                           prefetch=self.config.prefetch, breaker=breaker,
                           on_result=self._on_result, on_transport_error=self._on_transport_error,
                           on_disconnect=self._on_disconnect, task_timeout=self.config.task_timeout_s)

    def _register_worker(self, proxy: WorkerProxy, message: dict[str, Any]) -> None:
        requeued: list[str] = []
        with self._cond:
            old = self._workers.get(proxy.worker_id)
            if old is not None:
                requeued = self._fail_worker_locked(old)
                old.close()
            self._workers[proxy.worker_id] = proxy
            self._last_heartbeat[proxy.worker_id] = time.monotonic()
            self._cond.notify_all()
        self.bus.publish("worker.registered", worker_id=proxy.worker_id,
                         address=f"{proxy.host}:{proxy.port}", capacity=proxy.capacity,
                         executor=message.get("executor", "thread"))
        self._publish_requeued(proxy.worker_id, requeued)

    # -- observadores remotos -------------------------------------------------
    def _serve_subscriber(self, conn: Connection, message: dict[str, Any]) -> None:
        def on_close(sub: RemoteSubscriber) -> None:
            self.bus.unsubscribe(sub)
            with self._lock:
                self._subscribers.discard(sub)

        subscriber = RemoteSubscriber(conn, on_close)
        with self._lock:
            self._subscribers.add(subscriber)
        self.bus.subscribe(subscriber, message.get("topics") or None)
        # Hilo lector: detecta cuándo el monitor se desconecta.
        def watch() -> None:
            try:
                while True:
                    conn.recv()
            except (ConnectionClosed, OSError, ProtocolError):
                subscriber.close()
        threading.Thread(target=watch, name="subscriber-watch", daemon=True).start()

    # ======================================================================
    # Trabajos y planificación
    # ======================================================================
    def submit_job(self, job_type: str, specs: list[dict[str, Any]], job_id: str | None = None) -> JobRecord:
        job_id = job_id or uuid.uuid4().hex[:8]
        now = time.monotonic()
        task_ids = [f"{job_id}-{i:05d}" for i in range(len(specs))]
        job = JobRecord(job_id, job_type, task_ids, now)
        with self._cond:
            for task_id, spec in zip(task_ids, specs):
                self._tasks[task_id] = TaskRecord(
                    task_id, job_id, {"task_id": task_id, "type": spec["type"],
                                      "payload": spec.get("payload", {})}, now)
                self._pending.append(task_id)
            self._jobs[job_id] = job
            self._cond.notify_all()
        self.bus.publish("job.submitted", job_id=job_id, job_type=job_type, tasks=job.total)
        if job.total == 0:
            self._finalize_job(job)
        return job

    def _dispatch_loop(self) -> None:
        """Productor/consumidor: consume la cola de pendientes y alimenta nodos."""
        while self._running.is_set():
            proxy: WorkerProxy | None = None
            batch: list[dict[str, Any]] = []
            with self._cond:
                while self._running.is_set():
                    if self._pending:
                        proxy, batch = self._select_batch_locked()
                        if batch:
                            break
                    # Espera con tiempo límite: los circuitos abiertos pasan a
                    # semiabiertos por tiempo, sin que nadie notifique.
                    self._cond.wait(timeout=0.1)
            if proxy is not None and batch:
                proxy.send(batch)  # E/S de red fuera del candado

    def _select_batch_locked(self) -> tuple[WorkerProxy | None, list[dict[str, Any]]]:
        """Elige nodo (Strategy) y arma un lote respetando créditos y anti-afinidad."""
        candidates = [w for w in self._workers.values() if w.available_slots() > 0]
        up_ids = {w.worker_id for w in self._workers.values() if w.state == UP}
        while candidates:
            proxy = self.strategy.select(candidates)
            if proxy is None:
                break
            batch, skipped = self._take_tasks_locked(proxy, up_ids)
            if batch:
                return proxy, batch
            if not skipped:
                break  # no quedan tareas pendientes válidas
            # Las tareas disponibles ya fallaron en este nodo: probar otro nodo
            # o esperar a que uno sano libere créditos.
            candidates = [c for c in candidates if c is not proxy]
        return None, []

    def _take_tasks_locked(self, proxy: WorkerProxy, up_ids: set[str],
                           scan_limit: int = 256) -> tuple[list[dict[str, Any]], list[str]]:
        room = min(self.config.batch_size, proxy.available_slots())
        now = time.monotonic()
        batch: list[dict[str, Any]] = []
        skipped: list[str] = []
        while self._pending and len(batch) < room and len(skipped) < scan_limit:
            task_id = self._pending.popleft()
            record = self._tasks.get(task_id)
            if record is None or record.status != PENDING:
                continue
            # Anti-afinidad: no reintentar donde ya falló, salvo que haya fallado
            # en todos los nodos activos (evita inanición).
            if (self.config.retry_affinity and proxy.worker_id in record.failed_on
                    and not up_ids <= record.failed_on):
                skipped.append(task_id)
                continue
            record.status = DISPATCHED
            record.attempts += 1
            record.worker_id = proxy.worker_id
            record.dispatched_at = now
            batch.append({**record.spec, "attempt": record.attempts})
        self._pending.extendleft(reversed(skipped))  # conserva el orden original
        if batch:
            proxy.reserve([t["task_id"] for t in batch])
        return batch, skipped

    # -- callbacks del proxy ----------------------------------------------------
    def _on_result(self, proxy: WorkerProxy, message: dict[str, Any]) -> None:
        task_id = message.get("task_id")
        ok = message.get("status") == "ok"
        event: tuple[str, dict[str, Any]] | None = None
        finished: JobRecord | None = None
        with self._cond:
            proxy.inflight.pop(task_id, None)
            if ok:
                proxy.completed += 1
                proxy.observe_latency(float(message.get("duration_ms", 0.0)))
                proxy.breaker.record_success()
            else:
                proxy.failed += 1
                proxy.breaker.record_failure()
            record = self._tasks.get(task_id)
            job = self._jobs.get(record.job_id) if record else None
            if record is None or job is None:
                pass
            elif record.status in (DONE, FAILED):
                job.duplicates += 1  # resultado tardío de un intento ya resuelto
                event = ("task.duplicate", {"task_id": task_id, "worker_id": proxy.worker_id})
            elif ok:
                record.status = DONE
                record.result = message.get("result")
                record.completed_at = time.monotonic()
                record.worker_id = proxy.worker_id
                job.done += 1
                event = ("task.completed", {"task_id": task_id, "worker_id": proxy.worker_id,
                                            "duration_ms": message.get("duration_ms")})
            elif record.status == DISPATCHED and record.worker_id == proxy.worker_id:
                event = self._retry_or_fail_locked(record, job, message.get("error", "error"))
            finished = self._check_job_locked(job) if job else None
            self._cond.notify_all()
        if event:
            self.bus.publish(event[0], **event[1])
        if finished:
            self._finalize_job(finished)

    def _retry_or_fail_locked(self, record: TaskRecord, job: JobRecord, error: str) -> tuple[str, dict[str, Any]]:
        record.failures += 1
        record.error = error
        failed_on = record.worker_id
        if failed_on:
            record.failed_on.add(failed_on)
        if record.failures < self.config.max_attempts:
            record.status = PENDING
            record.worker_id = None
            self._pending.appendleft(record.task_id)
            job.retries += 1
            return ("task.retry", {"task_id": record.task_id, "worker_id": failed_on,
                                   "failures": record.failures, "error": error})
        record.status = FAILED
        record.completed_at = time.monotonic()
        job.failed += 1
        return ("task.failed", {"task_id": record.task_id, "worker_id": failed_on, "error": error})

    def _on_transport_error(self, proxy: WorkerProxy, task_ids: list[str], exc: Exception) -> None:
        events: list[tuple[str, dict[str, Any]]] = []
        finished: list[JobRecord] = []
        with self._cond:
            proxy.breaker.record_failure()
            for task_id in task_ids:
                proxy.inflight.pop(task_id, None)
                record = self._tasks.get(task_id)
                if record and record.status == DISPATCHED and record.worker_id == proxy.worker_id:
                    job = self._jobs[record.job_id]
                    events.append(self._retry_or_fail_locked(record, job, f"transporte: {exc}"))
                    done = self._check_job_locked(job)
                    if done:
                        finished.append(done)
            self._cond.notify_all()
        for topic, data in events:
            self.bus.publish(topic, **data)
        for job in finished:
            self._finalize_job(job)

    def _on_disconnect(self, proxy: WorkerProxy, exc: Exception) -> None:
        self._fail_worker(proxy, f"conexión de datos perdida: {exc.__class__.__name__}")

    # -- fallos de nodo ---------------------------------------------------------
    def _fail_worker(self, proxy: WorkerProxy, reason: str) -> None:
        with self._cond:
            if proxy.state == DOWN:
                return
            requeued = self._fail_worker_locked(proxy)
            self._cond.notify_all()
        proxy.drop_connection()
        self.bus.publish("worker.down", worker_id=proxy.worker_id, reason=reason,
                         requeued=len(requeued))
        self._publish_requeued(proxy.worker_id, requeued)

    def _fail_worker_locked(self, proxy: WorkerProxy) -> list[str]:
        """Marca el nodo como caído y re-encola sus tareas en vuelo."""
        proxy.state = DOWN
        requeued: list[str] = []
        for task_id in list(proxy.inflight):
            record = self._tasks.get(task_id)
            if record and record.status == DISPATCHED and record.worker_id == proxy.worker_id:
                record.status = PENDING
                record.worker_id = None
                self._pending.appendleft(task_id)
                self._jobs[record.job_id].requeues += 1
                requeued.append(task_id)
        proxy.inflight.clear()
        return requeued

    def _publish_requeued(self, worker_id: str, task_ids: list[str]) -> None:
        for task_id in task_ids:
            self.bus.publish("task.requeued", task_id=task_id, worker_id=worker_id)

    def _heartbeat_loop(self) -> None:
        while self._running.is_set():
            time.sleep(0.25)
            now = time.monotonic()
            with self._cond:
                expired = [p for wid, p in self._workers.items()
                           if p.state == UP and now - self._last_heartbeat.get(wid, now)
                           > self.config.heartbeat_timeout_s]
            for proxy in expired:
                self._fail_worker(proxy, f"sin heartbeat por más de {self.config.heartbeat_timeout_s:.1f}s")

    # -- cierre de trabajos ------------------------------------------------------
    def _check_job_locked(self, job: JobRecord) -> JobRecord | None:
        if job.completed_at is None and job.done + job.failed >= job.total:
            job.completed_at = time.monotonic()
            return job
        return None

    def _finalize_job(self, job: JobRecord) -> None:
        if job.completed_at is None:
            job.completed_at = time.monotonic()
        with self._lock:
            records = [self._tasks[t] for t in job.task_ids if t in self._tasks]
        done = [r for r in records if r.status == DONE]
        latencies = [(r.completed_at - r.submitted_at) * 1000 for r in done if r.completed_at]
        makespan = max(1e-9, job.completed_at - job.submitted_at)
        per_worker: dict[str, int] = {}
        for r in done:
            per_worker[r.worker_id or "?"] = per_worker.get(r.worker_id or "?", 0) + 1
        metrics = {
            "tasks": job.total,
            "done": job.done,
            "failed": job.failed,
            "makespan_s": round(makespan, 4),
            "throughput_tps": round(job.done / makespan, 2),
            "latency_p50_ms": round(percentile(latencies, 50), 1),
            "latency_p95_ms": round(percentile(latencies, 95), 1),
            "latency_p99_ms": round(percentile(latencies, 99), 1),
            "attempts": sum(r.attempts for r in records),
            "retries": job.retries,
            "requeues": job.requeues,
            "duplicates": job.duplicates,
            "per_worker": dict(sorted(per_worker.items())),
        }
        try:
            result = reduce_results(job.job_type, [r.result for r in done])
        except Exception as exc:  # pragma: no cover
            log.exception("fallo en reduce")
            result = {"error": str(exc)}
        job.summary = {"job_type": job.job_type, "metrics": metrics, "result": result,
                       "config": {k: v for k, v in asdict(self.config).items()
                                  if k in ("strategy", "transport", "prefetch", "batch_size",
                                           "breaker_enabled", "retry_affinity", "max_attempts")}}
        job.finished.set()
        self.bus.publish("job.completed", job_id=job.job_id, **metrics)
        # Libera memoria: los registros por tarea ya no son necesarios.
        with self._lock:
            for task_id in job.task_ids:
                self._tasks.pop(task_id, None)

    # ======================================================================
    # Observabilidad
    # ======================================================================
    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            workers = [p.snapshot() for p in sorted(self._workers.values(), key=lambda p: p.worker_id)]
            pending = len(self._pending)
            inflight = sum(len(p.inflight) for p in self._workers.values())
            active_jobs = sum(1 for j in self._jobs.values() if not j.finished.is_set())
        return {
            "uptime_s": round(time.time() - self._started_at, 1),
            "pending": pending,
            "inflight": inflight,
            "active_jobs": active_jobs,
            "throughput_tps": round(self.metrics.throughput(), 1),
            "counters": self.metrics.snapshot(),
            "workers": workers,
            "workers_up": sum(1 for w in workers if w["state"] == UP),
            "config": {"strategy": self.config.strategy, "transport": self.config.transport,
                       "prefetch": self.config.prefetch, "batch_size": self.config.batch_size,
                       "breaker": self.config.breaker_enabled,
                       "retry_affinity": self.config.retry_affinity},
        }

    def _stats_loop(self) -> None:
        while self._running.is_set():
            time.sleep(self.config.stats_interval_s)
            with self._lock:
                has_remote_observers = bool(self._subscribers)
            if has_remote_observers:
                self.bus.publish("stats", **self.snapshot())


# ==========================================================================
# CLI
# ==========================================================================
def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Coordinador (maestro) de DistriTask")
    parser.add_argument("--host", default="127.0.0.1", help="interfaz de escucha (0.0.0.0 en contenedores)")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--profile", choices=sorted(PROFILES), default="optimized",
                        help="baseline = versión 1, optimized = versión 2")
    parser.add_argument("--strategy", choices=["round_robin", "least_loaded", "random"])
    parser.add_argument("--transport", choices=["per_task", "persistent"])
    parser.add_argument("--prefetch", type=int, help="créditos por hilo del nodo")
    parser.add_argument("--batch-size", type=int)
    breaker = parser.add_mutually_exclusive_group()
    breaker.add_argument("--breaker", dest="breaker_enabled", action="store_true", default=None)
    breaker.add_argument("--no-breaker", dest="breaker_enabled", action="store_false")
    affinity = parser.add_mutually_exclusive_group()
    affinity.add_argument("--retry-affinity", dest="retry_affinity", action="store_true", default=None,
                          help="no reintentar una tarea en el nodo donde ya falló")
    affinity.add_argument("--no-retry-affinity", dest="retry_affinity", action="store_false")
    parser.add_argument("--breaker-threshold", type=int, default=3)
    parser.add_argument("--breaker-reset", type=float, default=2.0)
    parser.add_argument("--heartbeat-timeout", type=float, default=3.0)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--log-level", default="INFO")
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_arg_parser().parse_args(argv)
    logging.basicConfig(level=args.log_level.upper(),
                        format="%(asctime)s %(levelname)-5s %(threadName)-11s %(message)s",
                        datefmt="%H:%M:%S")
    config = CoordinatorConfig.from_profile(
        args.profile, host=args.host, port=args.port, strategy=args.strategy,
        transport=args.transport, prefetch=args.prefetch, batch_size=args.batch_size,
        breaker_enabled=args.breaker_enabled, retry_affinity=args.retry_affinity,
        breaker_threshold=args.breaker_threshold,
        breaker_reset_s=args.breaker_reset, heartbeat_timeout_s=args.heartbeat_timeout,
        max_attempts=args.max_attempts, verbose_events=True)
    Coordinator(config).serve_forever()


if __name__ == "__main__":
    main()
