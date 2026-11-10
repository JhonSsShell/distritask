"""Patrón Circuit Breaker (Nygard, 2018; Fowler, 2014).

Protege al coordinador de seguir enviando trabajo a un nodo que está vivo
(responde heartbeats) pero degradado (todas sus tareas fallan). Sin este
patrón, la estrategia *least-loaded* agrava el problema: el nodo que falla
rápido siempre parece el menos cargado y se convierte en un "agujero negro"
que consume los reintentos de todo el sistema.

Estados:

* ``CLOSED``   - tráfico normal; se cuentan fallos consecutivos.
* ``OPEN``     - tras ``failure_threshold`` fallos seguidos: no se envía nada
                 durante ``reset_timeout`` segundos.
* ``HALF_OPEN``- pasado ese tiempo se permite **una** tarea de prueba; si
                 tiene éxito el circuito se cierra, si falla se vuelve a abrir.
"""
from __future__ import annotations

import threading
import time
from enum import Enum
from typing import Callable


class BreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


StateListener = Callable[[BreakerState, BreakerState], None]


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 3, reset_timeout: float = 2.0, *,
                 enabled: bool = True, on_change: StateListener | None = None,
                 clock: Callable[[], float] = time.monotonic) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold debe ser >= 1")
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.enabled = enabled
        self._on_change = on_change
        self._clock = clock
        self._lock = threading.Lock()
        self._state = BreakerState.CLOSED
        self._failures = 0
        self._opened_at = 0.0
        self._trial_in_flight = False
        self.open_count = 0

    # -- consulta -----------------------------------------------------------
    @property
    def state(self) -> BreakerState:
        with self._lock:
            change = self._maybe_half_open()
            state = self._state
        self._emit(change)
        return state

    def allowed_slots(self, free_slots: int) -> int:
        """Cuántas tareas nuevas se pueden enviar al nodo ahora mismo."""
        if not self.enabled:
            return free_slots
        with self._lock:
            change = self._maybe_half_open()
            if self._state is BreakerState.CLOSED:
                allowed = free_slots
            elif self._state is BreakerState.HALF_OPEN:
                allowed = 1 if (free_slots > 0 and not self._trial_in_flight) else 0
            else:
                allowed = 0
        self._emit(change)
        return allowed

    # -- registro de resultados -------------------------------------------
    def on_dispatch(self) -> None:
        if not self.enabled:
            return
        with self._lock:
            if self._state is BreakerState.HALF_OPEN:
                self._trial_in_flight = True

    def record_success(self) -> None:
        """Registra un éxito.

        Ajuste v2.1 (hallado con inyección de fallos en caliente): un éxito
        *tardío* de una tarea despachada antes de abrir el circuito NO debe
        cerrarlo. Solo la tarea de prueba en ``HALF_OPEN`` puede cerrarlo; en
        ``OPEN`` los éxitos se ignoran.
        """
        if not self.enabled:
            return
        with self._lock:
            change = None
            if self._state is BreakerState.CLOSED:
                self._failures = 0
            elif self._state is BreakerState.HALF_OPEN:
                self._failures = 0
                self._trial_in_flight = False
                change = self._set(BreakerState.CLOSED)
            # OPEN: se ignora (resultado de una tarea anterior a la apertura)
        self._emit(change)

    def record_failure(self) -> None:
        if not self.enabled:
            return
        with self._lock:
            change = None
            if self._state is BreakerState.OPEN:
                return  # fallos tardíos de tareas en vuelo: ya está abierto
            self._failures += 1
            if self._state is BreakerState.HALF_OPEN or (
                    self._state is BreakerState.CLOSED and self._failures >= self.failure_threshold):
                self._opened_at = self._clock()
                self.open_count += 1
                change = self._set(BreakerState.OPEN)
            self._trial_in_flight = False
        self._emit(change)

    # -- internos -------------------------------------------------------------
    def _maybe_half_open(self) -> tuple[BreakerState, BreakerState] | None:
        if self._state is BreakerState.OPEN and self._clock() - self._opened_at >= self.reset_timeout:
            self._trial_in_flight = False
            return self._set(BreakerState.HALF_OPEN)
        return None

    def _set(self, new: BreakerState) -> tuple[BreakerState, BreakerState] | None:
        old, self._state = self._state, new
        return (old, new) if old is not new else None

    def _emit(self, change: tuple[BreakerState, BreakerState] | None) -> None:
        # Los listeners se invocan fuera del candado para evitar bloqueos mutuos.
        if change and self._on_change:
            self._on_change(*change)
