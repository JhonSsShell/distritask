"""Protocolo de red de DistriTask.

Cada mensaje viaja sobre TCP como un *marco* (frame):

    +----------------------+-------------------------------+
    | longitud (4 bytes,   | cuerpo JSON codificado en     |
    | entero big-endian)   | UTF-8 (``longitud`` bytes)    |
    +----------------------+-------------------------------+

El prefijo de longitud resuelve el problema de delimitar mensajes en un
flujo de bytes (TCP no conserva fronteras). Todos los mensajes son objetos
JSON con un campo ``type``.
"""
from __future__ import annotations

import json
import socket
import struct
import threading
from typing import Any

HEADER = struct.Struct("!I")
MAX_MESSAGE_BYTES = 16 * 1024 * 1024  # 16 MiB: protege contra marcos corruptos

# --- Tipos de mensaje -------------------------------------------------------
# Plano de control (nodo -> coordinador)
REGISTER = "REGISTER"
REGISTERED = "REGISTERED"
HEARTBEAT = "HEARTBEAT"
# Plano de datos (coordinador -> nodo y respuesta)
EXECUTE = "EXECUTE"          # {"tasks": [ {task_id, type, payload}, ... ]}
RESULT = "RESULT"            # {task_id, status, result|error, duration_ms}
# Clientes
SUBMIT_JOB = "SUBMIT_JOB"
JOB_ACCEPTED = "JOB_ACCEPTED"
JOB_DONE = "JOB_DONE"
STATS = "STATS"
STATS_REPLY = "STATS_REPLY"
# Observadores remotos (monitor / dashboard)
SUBSCRIBE = "SUBSCRIBE"
EVENT = "EVENT"
ERROR = "ERROR"
# Ingeniería del caos (pruebas de robustez): se envía al plano de datos de un nodo
CHAOS = "CHAOS"              # {"action": "hang"|"recover"|"failure_rate"|"slowdown", "value": x}
CHAOS_ACK = "CHAOS_ACK"


class ProtocolError(Exception):
    """Marco mal formado o mensaje inesperado."""


class ConnectionClosed(ProtocolError):
    """El extremo remoto cerró la conexión."""


def encode(message: dict[str, Any]) -> bytes:
    body = json.dumps(message, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    if len(body) > MAX_MESSAGE_BYTES:
        raise ProtocolError(f"mensaje demasiado grande ({len(body)} bytes)")
    return HEADER.pack(len(body)) + body


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    buf = bytearray()
    while len(buf) < size:
        chunk = sock.recv(size - len(buf))
        if not chunk:
            raise ConnectionClosed("el extremo remoto cerró la conexión")
        buf.extend(chunk)
    return bytes(buf)


def recv_message(sock: socket.socket) -> dict[str, Any]:
    (length,) = HEADER.unpack(_recv_exact(sock, HEADER.size))
    if length > MAX_MESSAGE_BYTES:
        raise ProtocolError(f"marco de {length} bytes excede el máximo permitido")
    try:
        message = json.loads(_recv_exact(sock, length).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolError(f"JSON inválido: {exc}") from exc
    if not isinstance(message, dict) or "type" not in message:
        raise ProtocolError("el mensaje debe ser un objeto con campo 'type'")
    return message


class Connection:
    """Envoltura de un socket TCP con envío seguro entre hilos.

    Varios hilos pueden llamar a :meth:`send` sobre la misma conexión (por
    ejemplo, los hilos del pool de un nodo devolviendo resultados), por lo
    que la escritura se serializa con un candado. La lectura la hace un solo
    hilo por conexión.
    """

    def __init__(self, sock: socket.socket):
        self.sock = sock
        self._send_lock = threading.Lock()
        self._closed = False
        try:
            # Desactiva el algoritmo de Nagle: los mensajes son pequeños y
            # sensibles a la latencia.
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError:
            pass
        try:
            self.peer = sock.getpeername()
        except OSError:
            self.peer = ("?", 0)

    @classmethod
    def connect(cls, host: str, port: int, timeout: float = 5.0) -> "Connection":
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.settimeout(None)
        return cls(sock)

    def settimeout(self, timeout: float | None) -> None:
        self.sock.settimeout(timeout)

    def send(self, message: dict[str, Any]) -> None:
        data = encode(message)
        with self._send_lock:
            self.sock.sendall(data)

    def recv(self) -> dict[str, Any]:
        return recv_message(self.sock)

    @property
    def closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self.sock.shutdown(socket.SHUT_RDWR)  # desbloquea un recv() pendiente
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def parse_address(value: str, default_port: int = 5000) -> tuple[str, int]:
    """Convierte ``"host:puerto"`` en una tupla. Acepta solo ``host``."""
    if ":" in value:
        host, port = value.rsplit(":", 1)
        return host or "127.0.0.1", int(port)
    return value, default_port


def free_port(host: str = "127.0.0.1") -> int:
    """Devuelve un puerto TCP libre (útil para pruebas y benchmarks)."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, 0))
        return s.getsockname()[1]
