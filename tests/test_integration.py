"""Pruebas de integración: clúster real de procesos comunicados por TCP."""
import threading
import time
import unittest

from distritask.chaos import inject
from distritask.cluster import LocalCluster
from distritask.monitor import subscribe
from distritask.tasks import build_log_job, build_pi_job, build_sleep_job


class IntegrationTest(unittest.TestCase):
    def test_trabajo_completo_y_resultado_correcto(self):
        for profile in ("baseline", "optimized"):
            with self.subTest(profile=profile), LocalCluster(2, 2, profile) as cluster, cluster.client() as c:
                summary = c.submit("logs", build_log_job(30, 200, io_latency_ms=5))
                self.assertEqual(summary["metrics"]["done"], 30)
                self.assertEqual(summary["result"]["lines"], 6000)
                self.assertEqual(set(summary["metrics"]["per_worker"]), {"w1", "w2"})

    def test_caida_de_nodo_no_pierde_tareas(self):
        with LocalCluster(3, 2, "optimized") as cluster, cluster.client() as c:
            threading.Timer(0.4, cluster.kill_worker, args=("w2",)).start()
            summary = c.submit("sleep", build_sleep_job(150, ms=20))
            m = summary["metrics"]
            self.assertEqual(m["done"], 150)
            self.assertEqual(m["failed"], 0)
            self.assertGreater(m["requeues"] + m["retries"], 0)

    def test_nodo_degradado_aislado_por_circuit_breaker(self):
        with LocalCluster(3, 2, "optimized", worker_args={"w3": ["--failure-rate", "1.0"]}) as cluster, \
                cluster.client() as c:
            summary = c.submit("sleep", build_sleep_job(120, ms=10))
            self.assertEqual(summary["metrics"]["done"], 120)
            self.assertNotIn("w3", summary["metrics"]["per_worker"])
            w3 = next(w for w in c.stats()["workers"] if w["worker_id"] == "w3")
            self.assertGreaterEqual(w3["breaker_opens"], 1)

    def test_degradacion_en_caliente_sin_fallos_permanentes(self):
        with LocalCluster(3, 2, "optimized") as cluster, cluster.client() as c:
            threading.Timer(0.3, inject, args=((cluster.host, cluster.port), "w3", "failure_rate", 1.0)).start()
            summary = c.submit("sleep", build_sleep_job(300, ms=10))
            self.assertEqual(summary["metrics"]["failed"], 0)
            self.assertEqual(summary["metrics"]["done"], 300)

    def test_nodo_colgado_se_detecta_y_se_recupera(self):
        with LocalCluster(2, 2, "optimized") as cluster, cluster.client() as c:
            events = []

            def listen():
                for e in subscribe(cluster.host, cluster.port, ["worker.*"]):
                    events.append(e["topic"])
                    if e["topic"] == "worker.recovered":
                        return

            threading.Thread(target=listen, daemon=True).start()
            time.sleep(0.2)
            inject((cluster.host, cluster.port), "w1", "hang")
            summary = c.submit("sleep", build_sleep_job(200, ms=30))
            self.assertEqual(summary["metrics"]["done"], 200)
            inject((cluster.host, cluster.port), "w1", "recover")
            deadline = time.time() + 5
            while "worker.recovered" not in events and time.time() < deadline:
                time.sleep(0.1)
            self.assertIn("worker.down", events)
            self.assertIn("worker.recovered", events)

    def test_pool_de_procesos(self):
        with LocalCluster(1, 2, "optimized", executor="process") as cluster, cluster.client() as c:
            summary = c.submit("pi", build_pi_job(8, 20000))
            self.assertEqual(summary["metrics"]["done"], 8)
            self.assertLess(summary["result"]["abs_error"], 0.05)


if __name__ == "__main__":
    unittest.main()
