"""Patrón Observer distribuido (publicar / suscribir).

``EventBus`` es el *Sujeto*: los componentes del coordinador publican eventos
por tema (``worker.down``, ``task.completed``, ``breaker.opened`` ...) sin
conocer a sus consumidores. Los *Observadores* pueden ser locales
(``MetricsCollector``, ``LogObserver``) o remotos: ``RemoteSubscriber`` es un
proxy de un observador que vive en otro proceso/nodo (monitor de consola o
dashboard web) y le reenvía los eventos por TCP.

Decisión de robustez: un observador remoto lento nunca bloquea al
coordinador. Cada ``RemoteSubscriber`` tiene su propia cola acotada y un
hilo emisor; si la cola se llena, el evento se descarta y se contabiliza.
"""
from __future__ import annotations

import logging
import queue
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Protocol

from .protocol import EVENT, Connection

log = logging.getLogger("distritask.events")


@dataclass(frozen=True)
class Event:
    topic: str
    data: dict[str, Any]
    ts: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return {"topic": self.topic, "data": self.data, "ts": self.ts}


class Observer(Protocol):
    def notify(self, event: Event) -> None:  # pragma: no cover - interfaz
        ...


def topic_matches(topic: str, patterns: Iterable[str] | None) -> bool:
    """``worker.*`` coincide con ``worker.down``; ``*`` coincide con todo."""
    if patterns is None:
        return True
    for pattern in patterns:
        if pattern in ("*", topic):
            return True
        if pattern.endswith(".*") and topic.startswith(pattern[:-1]):
            return True
    return False


class EventBus:
    """Sujeto concreto del patrón Observer (seguro entre hilos)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._subscribers: list[tuple[Observer, frozenset[str] | None]] = []

    def subscribe(self, observer: Observer, topics: Iterable[str] | None = None) -> None:
        with self._lock:
            self._subscribers.append((observer, frozenset(topics) if topics else None))

    def unsubscribe(self, observer: Observer) -> None:
        with self._lock:
            self._subscribers = [(o, t) for o, t in self._subscribers if o is not observer]

    @property
    def subscriber_count(self) -> int:
        with self._lock:
            return len(self._subscribers)

    def publish(self, topic: str, **data: Any) -> Event:
        event = Event(topic, data)
        with self._lock:
            subscribers = list(self._subscribers)
        for observer, topics in subscribers:
            if topic_matches(topic, topics):
                try:
                    observer.notify(event)
                except Exception:  # un observador defectuoso no rompe al resto
                    log.exception("observador %r falló procesando %s", observer, topic)
        return event


class RemoteSubscriber:
    """Proxy de un observador remoto conectado por TCP."""

    def __init__(self, conn: Connection, on_close: Callable[["RemoteSubscriber"], None],
                 max_queue: int = 2000) -> None:
        self.conn = conn
        self.dropped = 0
        self._queue: "queue.Queue[Event | None]" = queue.Queue(maxsize=max_queue)
        self._on_close = on_close
        self._thread = threading.Thread(target=self._pump, name="subscriber-pump", daemon=True)
        self._thread.start()

    def notify(self, event: Event) -> None:
        try:
            self._queue.put_nowait(event)
        except queue.Full:
            self.dropped += 1

    def _pump(self) -> None:
        try:
            while True:
                event = self._queue.get()
                if event is None:
                    break
                self.conn.send({"type": EVENT, **event.to_dict()})
        except OSError:
            pass
        finally:
            self.conn.close()
            self._on_close(self)

    def close(self) -> None:
        try:
            self._queue.put_nowait(None)
        except queue.Full:
            self.conn.close()


class LogObserver:
    """Observador local que escribe los eventos relevantes en el log."""

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self.log = logger or logging.getLogger("distritask.events")

    def notify(self, event: Event) -> None:
        details = " ".join(f"{k}={v}" for k, v in event.data.items() if k != "tasks")
        self.log.info("[evento] %s %s", event.topic, details)
