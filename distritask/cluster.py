"""Lanzador de un clúster local: coordinador + N nodos como procesos del SO.

Cada componente corre en su propio proceso de Python y se comunica solo por
TCP, igual que lo haría en máquinas distintas. Se usa en la demo, en las
pruebas de integración y en los benchmarks.
"""
from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Sequence

from .client import CoordinatorClient
from .protocol import free_port

PROJECT_ROOT = Path(__file__).resolve().parent.parent


class LocalCluster:
    def __init__(self, workers: int = 3, threads: int = 4, profile: str = "optimized", *,
                 coordinator_args: Sequence[str] = (), worker_args: dict[str, Sequence[str]] | None = None,
                 executor: str = "thread", host: str = "127.0.0.1", port: int | None = None,
                 log_dir: str | os.PathLike[str] | None = None, log_level: str = "WARNING") -> None:
        self.n_workers = workers
        self.threads = threads
        self.profile = profile
        self.coordinator_args = list(coordinator_args)
        self.worker_args = {k: list(v) for k, v in (worker_args or {}).items()}
        self.executor = executor
        self.host = host
        self.port = port or free_port(host)
        self.log_dir = Path(log_dir) if log_dir else None
        self.log_level = log_level
        self.coordinator: subprocess.Popen[bytes] | None = None
        self.workers: dict[str, subprocess.Popen[bytes]] = {}
        self._logs: list[Any] = []

    # -- utilidades -----------------------------------------------------------
    def _popen(self, name: str, args: list[str]) -> subprocess.Popen[bytes]:
        env = dict(os.environ)
        env["PYTHONPATH"] = str(PROJECT_ROOT) + os.pathsep + env.get("PYTHONPATH", "")
        env["PYTHONUNBUFFERED"] = "1"
        env.setdefault("PYTHONIOENCODING", "utf-8")  # logs con tildes también en Windows
        if self.log_dir:
            self.log_dir.mkdir(parents=True, exist_ok=True)
            out: Any = open(self.log_dir / f"{name}.log", "wb")
            self._logs.append(out)
        else:
            out = subprocess.DEVNULL
        return subprocess.Popen([sys.executable, *args], cwd=PROJECT_ROOT, env=env,
                                stdout=out, stderr=subprocess.STDOUT)

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"

    def client(self) -> CoordinatorClient:
        return CoordinatorClient(self.host, self.port)

    # -- ciclo de vida --------------------------------------------------------
    def start(self, timeout: float = 20.0) -> "LocalCluster":
        self.coordinator = self._popen("coordinator", [
            "-m", "distritask.coordinator", "--host", self.host, "--port", str(self.port),
            "--profile", self.profile, "--log-level", self.log_level, *self.coordinator_args])
        self._wait_port(timeout)
        for i in range(1, self.n_workers + 1):
            self.add_worker(f"w{i}")
        self.wait_for_workers(self.n_workers, timeout)
        return self

    def add_worker(self, worker_id: str, extra: Sequence[str] = ()) -> str:
        args = ["-m", "distritask.worker", "--id", worker_id, "--coordinator", self.address,
                "--threads", str(self.threads), "--executor", self.executor,
                "--log-level", self.log_level, *self.worker_args.get(worker_id, []), *extra]
        self.workers[worker_id] = self._popen(worker_id, args)
        return worker_id

    def kill_worker(self, worker_id: str) -> None:
        """Termina el proceso del nodo abruptamente (simula caída de la VM)."""
        proc = self.workers[worker_id]
        proc.kill()
        proc.wait(timeout=5)

    def _wait_port(self, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.coordinator and self.coordinator.poll() is not None:
                raise RuntimeError("el coordinador terminó inesperadamente")
            try:
                with socket.create_connection((self.host, self.port), timeout=0.5):
                    return
            except OSError:
                time.sleep(0.05)
        raise TimeoutError("el coordinador no abrió su puerto a tiempo")

    def wait_for_workers(self, count: int, timeout: float = 20.0) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        with self.client() as client:
            while time.monotonic() < deadline:
                stats = client.stats()
                if stats["workers_up"] >= count:
                    return stats
                time.sleep(0.1)
        raise TimeoutError(f"solo se registraron {stats['workers_up']} de {count} nodos")

    def stop(self) -> None:
        procs = list(self.workers.values()) + ([self.coordinator] if self.coordinator else [])
        for proc in procs:
            if proc.poll() is None:
                proc.terminate()
        for proc in procs:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        for fh in self._logs:
            fh.close()
        self._logs.clear()

    def __enter__(self) -> "LocalCluster":
        return self.start()

    def __exit__(self, *exc: object) -> None:
        self.stop()
