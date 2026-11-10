import os
import tempfile
import unittest

from distritask.events import Event, EventBus, topic_matches
from distritask.metrics import percentile
from distritask.tasks import (analyze_logs, build_log_job, build_log_job_from_file, generate_log_lines,
                              monte_carlo_pi, reduce_logs, reduce_pi, run_task, InjectedFailure)


class TasksTest(unittest.TestCase):
    def test_generacion_determinista(self):
        self.assertEqual(generate_log_lines(42, 50), generate_log_lines(42, 50))
        self.assertNotEqual(generate_log_lines(42, 50), generate_log_lines(43, 50))

    def test_map_reduce_equivale_a_procesamiento_secuencial(self):
        job = build_log_job(6, 300, io_latency_ms=0)
        partial = [analyze_logs(t["payload"]) for t in job]
        all_lines = [line for t in job for line in generate_log_lines(t["payload"]["seed"], 300)]
        whole = analyze_logs({"source": "inline", "lines": all_lines})
        reduced = reduce_logs(partial)
        self.assertEqual(reduced["lines"], 1800)
        self.assertEqual(reduced["levels"], dict(sorted(whole["levels"].items(), key=lambda kv: -kv[1])))

    def test_archivo_real_con_formato_libre(self):
        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".log", encoding="utf-8") as fh:
            fh.write("2026-01-01 ERROR algo falló\n2026-01-01 WARNING disco\nlinea sin nivel\n")
            name = fh.name
        try:
            tasks = build_log_job_from_file(name, chunk_lines=2)
            self.assertEqual(len(tasks), 2)
            result = reduce_logs([analyze_logs(t["payload"]) for t in tasks])
            self.assertEqual(result["levels"], {"ERROR": 1, "WARN": 1})
            self.assertEqual(result["unparsed"], 1)
        finally:
            os.unlink(name)

    def test_pi(self):
        r = reduce_pi([monte_carlo_pi({"seed": s, "samples": 20000}) for s in range(10)])
        self.assertLess(r["abs_error"], 0.05)

    def test_inyeccion_de_fallos(self):
        with self.assertRaises(InjectedFailure):
            run_task({"type": "sleep", "payload": {"ms": 0}}, failure_rate=1.0)
        self.assertIn("duration_ms", run_task({"type": "sleep", "payload": {"ms": 0}}))


class ObserverTest(unittest.TestCase):
    def test_filtrado_por_tema(self):
        self.assertTrue(topic_matches("worker.down", ["worker.*"]))
        self.assertFalse(topic_matches("task.completed", ["worker.*", "job.*"]))
        self.assertTrue(topic_matches("x", None))

    def test_observador_defectuoso_no_afecta_a_otros(self):
        bus, seen = EventBus(), []

        class Bad:
            def notify(self, e):
                raise RuntimeError("boom")

        class Good:
            def notify(self, e: Event):
                seen.append(e.topic)

        bus.subscribe(Bad())
        bus.subscribe(Good(), ["job.*"])
        with self.assertLogs("distritask.events", "ERROR"):
            bus.publish("job.completed", job_id="1")
            bus.publish("worker.down", worker_id="w1")
        self.assertEqual(seen, ["job.completed"])

    def test_percentiles(self):
        self.assertEqual(percentile([1, 2, 3, 4, 5], 50), 3)
        self.assertAlmostEqual(percentile(range(1, 101), 95), 95.05)


if __name__ == "__main__":
    unittest.main()
