import unittest
from types import SimpleNamespace

from distritask.strategies import LeastLoadedStrategy, RoundRobinStrategy, make_strategy


def worker(wid, inflight=0, capacity=4, latency=0.0):
    return SimpleNamespace(worker_id=wid, inflight={str(i): 0 for i in range(inflight)},
                           capacity=capacity, ewma_latency_ms=latency)


class StrategyTest(unittest.TestCase):
    def test_round_robin_rota(self):
        rr = RoundRobinStrategy()
        ws = [worker("w1"), worker("w2"), worker("w3")]
        self.assertEqual([rr.select(ws).worker_id for _ in range(5)], ["w1", "w2", "w3", "w1", "w2"])

    def test_round_robin_salta_nodos_no_disponibles(self):
        rr = RoundRobinStrategy()
        self.assertEqual(rr.select([worker("w1"), worker("w3")]).worker_id, "w1")
        self.assertEqual(rr.select([worker("w1"), worker("w3")]).worker_id, "w3")

    def test_least_loaded_usa_utilizacion(self):
        ll = LeastLoadedStrategy()
        ws = [worker("w1", inflight=4, capacity=4), worker("w2", inflight=4, capacity=8), worker("w3", 3, 4)]
        self.assertEqual(ll.select(ws).worker_id, "w2")

    def test_least_loaded_desempata_por_latencia(self):
        ll = LeastLoadedStrategy()
        ws = [worker("w1", latency=90), worker("w2", latency=30)]
        self.assertEqual(ll.select(ws).worker_id, "w2")

    def test_fabrica(self):
        self.assertIsInstance(make_strategy("least_loaded"), LeastLoadedStrategy)
        with self.assertRaises(ValueError):
            make_strategy("magia")


if __name__ == "__main__":
    unittest.main()
