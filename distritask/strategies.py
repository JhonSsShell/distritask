"""Patrón Strategy: políticas intercambiables de balanceo de carga.

El coordinador (Mediator) delega en una estrategia la decisión de *a qué
nodo* enviar el siguiente lote de tareas. Cambiar de política no requiere
tocar el coordinador (principio abierto/cerrado).
"""
from __future__ import annotations

import random
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Sequence

if TYPE_CHECKING:  # pragma: no cover
    from .proxy import WorkerProxy


class LoadBalancingStrategy(ABC):
    name = "abstract"

    @abstractmethod
    def select(self, candidates: Sequence["WorkerProxy"]) -> "WorkerProxy | None":
        """Elige un nodo entre los candidatos (todos con capacidad libre)."""


class RoundRobinStrategy(LoadBalancingStrategy):
    """Turno rotativo: ignora la carga real de cada nodo (versión 1)."""

    name = "round_robin"

    def __init__(self) -> None:
        self._last: str | None = None

    def select(self, candidates):
        if not candidates:
            return None
        ordered = sorted(candidates, key=lambda w: w.worker_id)
        if self._last is not None:
            for proxy in ordered:
                if proxy.worker_id > self._last:
                    self._last = proxy.worker_id
                    return proxy
        self._last = ordered[0].worker_id
        return ordered[0]


class LeastLoadedStrategy(LoadBalancingStrategy):
    """Menor utilización (tareas en vuelo / capacidad); desempata por latencia."""

    name = "least_loaded"

    def select(self, candidates):
        if not candidates:
            return None
        return min(candidates, key=lambda w: (len(w.inflight) / max(1, w.capacity),
                                              w.ewma_latency_ms, w.worker_id))


class RandomStrategy(LoadBalancingStrategy):
    name = "random"

    def __init__(self, seed: int | None = None) -> None:
        self._rng = random.Random(seed)

    def select(self, candidates):
        return self._rng.choice(list(candidates)) if candidates else None


STRATEGIES: dict[str, type[LoadBalancingStrategy]] = {
    RoundRobinStrategy.name: RoundRobinStrategy,
    LeastLoadedStrategy.name: LeastLoadedStrategy,
    RandomStrategy.name: RandomStrategy,
}


def make_strategy(name: str) -> LoadBalancingStrategy:
    try:
        return STRATEGIES[name]()
    except KeyError:
        raise ValueError(f"estrategia desconocida: {name!r}. Opciones: {sorted(STRATEGIES)}") from None
