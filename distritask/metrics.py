"""Métricas del sistema: observador local del bus de eventos y utilidades."""
from __future__ import annotations

import math
import threading
import time
from collections import Counter, deque
from typing import Iterable

from .events import Event


def percentile(values: Iterable[float], p: float) -> float:
    """Percentil con interpolación lineal (p en [0, 100])."""
    data = sorted(values)
    if not data:
        return 0.0
    if len(data) == 1:
        return float(data[0])
    rank = (len(data) - 1) * p / 100.0
    low, high = math.floor(rank), math.ceil(rank)
    if low == high:
        return float(data[int(rank)])
    return data[low] + (data[high] - data[low]) * (rank - low)


class MetricsCollector:
    """Observador que agrega contadores globales y throughput en ventana móvil."""

    COUNTED = ("task.completed", "task.failed", "task.retry", "task.requeued",
               "task.duplicate", "worker.registered", "worker.down", "worker.recovered",
               "breaker.opened", "breaker.half_open", "breaker.closed", "job.submitted",
               "job.completed")

    def __init__(self, window_s: float = 3.0) -> None:
        self.window_s = window_s
        self._lock = threading.Lock()
        self._counters: Counter[str] = Counter()
        self._completions: deque[float] = deque()
        self._started = time.monotonic()

    def notify(self, event: Event) -> None:
        if event.topic not in self.COUNTED:
            return
        now = time.monotonic()
        with self._lock:
            self._counters[event.topic] += 1
            if event.topic == "task.completed":
                self._completions.append(now)

    def throughput(self) -> float:
        now = time.monotonic()
        with self._lock:
            while self._completions and now - self._completions[0] > self.window_s:
                self._completions.popleft()
            span = min(self.window_s, max(1e-6, now - self._started))
            return len(self._completions) / span

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return {k: self._counters.get(k, 0) for k in self.COUNTED}
