"""Cargas de trabajo (fase *map*) y agregación de resultados (fase *reduce*).

Dos tipos de trabajo ilustran los dos perfiles clásicos de carga:

* ``logs`` - análisis distribuido de registros (logs) de una plataforma de
  microservicios. Cada tarea procesa un *chunk* de líneas. Antes de procesar,
  simula la descarga del chunk desde un almacenamiento de objetos en la nube
  (``io_latency_ms``): carga **mixta E/S + CPU**, donde los hilos ayudan.
* ``pi`` - estimación de pi por Monte Carlo: carga **CPU pura**, donde el GIL
  de CPython impide que los hilos de un mismo proceso escalen.

Las funciones de este módulo son de nivel superior (serializables con
``pickle``) para poder ejecutarse también en un ``ProcessPoolExecutor``.
"""
from __future__ import annotations

import os
import random
import re
import time
from collections import Counter
from datetime import datetime, timedelta
from typing import Any, Callable, Iterable

# --------------------------------------------------------------------------
# Generación sintética de logs (determinista por semilla)
# --------------------------------------------------------------------------
LEVELS = ("INFO", "DEBUG", "WARN", "ERROR", "CRITICAL")
LEVEL_WEIGHTS = (70, 12, 12, 5, 1)
SERVICES = ("auth", "payments", "orders", "inventory", "gateway", "notifications")
MESSAGES = {
    "INFO": ("solicitud atendida", "usuario autenticado", "pedido creado", "cache actualizada"),
    "DEBUG": ("payload validado", "consulta preparada", "reintento programado"),
    "WARN": ("respuesta lenta del proveedor", "uso de memoria alto", "cola cerca del limite"),
    "ERROR": ("timeout al conectar con la base de datos", "pago rechazado por el banco",
              "token expirado", "stock insuficiente", "servicio no disponible (503)"),
    "CRITICAL": ("disco lleno en nodo de almacenamiento", "perdida de conexion con el broker"),
}
BASE_TIME = datetime(2026, 11, 1, 0, 0, 0)

LINE_RE = re.compile(
    r'^(?P<ts>\S+)\s+(?P<level>[A-Z]+)\s+\[(?P<service>[^\]]+)\]\s+'
    r'latency=(?P<latency>\d+)ms\s+msg="(?P<msg>[^"]*)"')
LEVEL_RE = re.compile(r"\b(DEBUG|INFO|WARN(?:ING)?|ERROR|CRITICAL|FATAL)\b")


def generate_log_lines(seed: int, n_lines: int) -> list[str]:
    rng = random.Random(seed)
    start = BASE_TIME + timedelta(minutes=seed % 1440)
    lines = []
    for i in range(n_lines):
        level = rng.choices(LEVELS, LEVEL_WEIGHTS)[0]
        service = SERVICES[rng.randrange(len(SERVICES))]
        latency = int(rng.expovariate(1 / (180 if level in ("ERROR", "CRITICAL") else 45)))
        ts = start + timedelta(milliseconds=i * 37)
        msg = MESSAGES[level][rng.randrange(len(MESSAGES[level]))]
        lines.append(f'{ts:%Y-%m-%dT%H:%M:%S} {level} [{service}] latency={latency}ms msg="{msg}"')
    return lines


# --------------------------------------------------------------------------
# Manejadores de tareas (map)
# --------------------------------------------------------------------------
def analyze_logs(payload: dict[str, Any]) -> dict[str, Any]:
    io_latency_ms = payload.get("io_latency_ms", 0)
    if io_latency_ms:
        # Simula la descarga del chunk desde almacenamiento de objetos (E/S).
        time.sleep(io_latency_ms / 1000.0)
    if payload.get("source") == "inline":
        lines: Iterable[str] = payload["lines"]
    else:
        lines = generate_log_lines(payload["seed"], payload["lines"])

    levels: Counter[str] = Counter()
    errors_by_service: Counter[str] = Counter()
    error_messages: Counter[str] = Counter()
    lat_sum: Counter[str] = Counter()
    lat_count: Counter[str] = Counter()
    lat_max: dict[str, int] = {}
    total = unparsed = 0
    for line in lines:
        total += 1
        match = LINE_RE.match(line)
        if match is None:
            generic = LEVEL_RE.search(line)
            if generic:
                levels[generic.group(1).replace("WARNING", "WARN").replace("FATAL", "CRITICAL")] += 1
            else:
                unparsed += 1
            continue
        level, service = match["level"], match["service"]
        latency = int(match["latency"])
        levels[level] += 1
        lat_sum[service] += latency
        lat_count[service] += 1
        if latency > lat_max.get(service, -1):
            lat_max[service] = latency
        if level in ("ERROR", "CRITICAL"):
            errors_by_service[service] += 1
            error_messages[match["msg"]] += 1
    return {
        "chunk": payload.get("chunk"),
        "lines": total,
        "unparsed": unparsed,
        "levels": dict(levels),
        "errors_by_service": dict(errors_by_service),
        "error_messages": dict(error_messages),
        "latency_sum": dict(lat_sum),
        "latency_count": dict(lat_count),
        "latency_max": lat_max,
    }


def monte_carlo_pi(payload: dict[str, Any]) -> dict[str, Any]:
    rng = random.Random(payload["seed"])
    samples = payload["samples"]
    inside = 0
    rand = rng.random
    for _ in range(samples):
        x = rand()
        y = rand()
        if x * x + y * y <= 1.0:
            inside += 1
    return {"inside": inside, "samples": samples}


def sleep_task(payload: dict[str, Any]) -> dict[str, Any]:
    time.sleep(payload.get("ms", 10) / 1000.0)
    return {"slept_ms": payload.get("ms", 10)}


HANDLERS: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {
    "logs": analyze_logs,
    "pi": monte_carlo_pi,
    "sleep": sleep_task,
}


class InjectedFailure(RuntimeError):
    """Fallo provocado a propósito para las pruebas de robustez."""


def run_task(task: dict[str, Any], slowdown: float = 1.0, failure_rate: float = 0.0) -> dict[str, Any]:
    """Ejecuta una tarea en un hilo (o proceso) del pool del nodo.

    ``slowdown`` simula un nodo con menos recursos (VM más pequeña) y
    ``failure_rate`` un nodo degradado que falla parte de sus tareas.
    """
    start = time.perf_counter()
    if failure_rate > 0 and random.random() < failure_rate:
        raise InjectedFailure("fallo inyectado: nodo degradado")
    try:
        handler = HANDLERS[task["type"]]
    except KeyError:
        raise ValueError(f"tipo de tarea desconocido: {task.get('type')!r}") from None
    result = handler(task["payload"])
    if slowdown > 1.0:
        time.sleep((time.perf_counter() - start) * (slowdown - 1.0))
    return {"result": result, "duration_ms": (time.perf_counter() - start) * 1000.0}


def seed_process() -> None:
    """Inicializador de procesos del pool: semillas distintas por proceso."""
    random.seed(int.from_bytes(os.urandom(8), "big"))


# --------------------------------------------------------------------------
# Construcción de trabajos (lado cliente)
# --------------------------------------------------------------------------
def build_log_job(chunks: int, lines_per_chunk: int = 2000, io_latency_ms: int = 40,
                  seed: int = 2026) -> list[dict[str, Any]]:
    return [{"type": "logs", "payload": {"source": "synthetic", "seed": seed + i, "chunk": i,
                                          "lines": lines_per_chunk, "io_latency_ms": io_latency_ms}}
            for i in range(chunks)]


def build_log_job_from_file(path: str, chunk_lines: int = 5000) -> list[dict[str, Any]]:
    tasks: list[dict[str, Any]] = []
    with open(path, encoding="utf-8", errors="replace") as fh:
        buffer: list[str] = []
        for line in fh:
            buffer.append(line.rstrip("\n"))
            if len(buffer) >= chunk_lines:
                tasks.append({"type": "logs", "payload": {"source": "inline", "chunk": len(tasks),
                                                          "lines": buffer}})
                buffer = []
        if buffer:
            tasks.append({"type": "logs", "payload": {"source": "inline", "chunk": len(tasks),
                                                      "lines": buffer}})
    return tasks


def build_pi_job(tasks: int, samples: int = 200_000, seed: int = 7) -> list[dict[str, Any]]:
    return [{"type": "pi", "payload": {"seed": seed + i, "samples": samples}} for i in range(tasks)]


def build_sleep_job(tasks: int, ms: int = 10) -> list[dict[str, Any]]:
    return [{"type": "sleep", "payload": {"ms": ms}} for _ in range(tasks)]


# --------------------------------------------------------------------------
# Agregación (reduce)
# --------------------------------------------------------------------------
def reduce_logs(results: list[dict[str, Any]]) -> dict[str, Any]:
    levels: Counter[str] = Counter()
    errors: Counter[str] = Counter()
    messages: Counter[str] = Counter()
    lat_sum: Counter[str] = Counter()
    lat_count: Counter[str] = Counter()
    lat_max: dict[str, int] = {}
    lines = unparsed = 0
    for r in results:
        lines += r["lines"]
        unparsed += r["unparsed"]
        levels.update(r["levels"])
        errors.update(r["errors_by_service"])
        messages.update(r["error_messages"])
        lat_sum.update(r["latency_sum"])
        lat_count.update(r["latency_count"])
        for svc, value in r["latency_max"].items():
            lat_max[svc] = max(lat_max.get(svc, 0), value)
    error_total = levels.get("ERROR", 0) + levels.get("CRITICAL", 0)
    services = {
        svc: {"requests": lat_count[svc], "errors": errors.get(svc, 0),
              "avg_latency_ms": round(lat_sum[svc] / lat_count[svc], 1) if lat_count[svc] else 0,
              "max_latency_ms": lat_max.get(svc, 0)}
        for svc in sorted(lat_count)
    }
    return {
        "lines": lines,
        "unparsed": unparsed,
        "levels": dict(levels.most_common()),
        "error_rate_pct": round(100 * error_total / lines, 3) if lines else 0.0,
        "services": services,
        "top_errors": messages.most_common(5),
    }


def reduce_pi(results: list[dict[str, Any]]) -> dict[str, Any]:
    inside = sum(r["inside"] for r in results)
    samples = sum(r["samples"] for r in results)
    estimate = 4 * inside / samples if samples else 0.0
    return {"samples": samples, "pi_estimate": estimate,
            "abs_error": abs(estimate - 3.141592653589793)}


def reduce_results(job_type: str, results: list[dict[str, Any]]) -> dict[str, Any]:
    if job_type == "logs":
        return reduce_logs(results)
    if job_type == "pi":
        return reduce_pi(results)
    return {"results": len(results)}
