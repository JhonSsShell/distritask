import unittest

from distritask.circuit_breaker import BreakerState, CircuitBreaker


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


class CircuitBreakerTest(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.changes = []
        self.cb = CircuitBreaker(3, 2.0, clock=self.clock,
                                 on_change=lambda old, new: self.changes.append((old, new)))

    def test_se_abre_tras_umbral_de_fallos_consecutivos(self):
        self.cb.record_failure(); self.cb.record_failure()
        self.assertEqual(self.cb.state, BreakerState.CLOSED)
        self.cb.record_failure()
        self.assertEqual(self.cb.state, BreakerState.OPEN)
        self.assertEqual(self.cb.allowed_slots(8), 0)
        self.assertEqual(self.cb.open_count, 1)

    def test_un_exito_reinicia_el_contador(self):
        self.cb.record_failure(); self.cb.record_failure(); self.cb.record_success()
        self.cb.record_failure(); self.cb.record_failure()
        self.assertEqual(self.cb.state, BreakerState.CLOSED)

    def test_semiabierto_permite_una_sola_prueba(self):
        for _ in range(3):
            self.cb.record_failure()
        self.clock.now = 2.5
        self.assertEqual(self.cb.allowed_slots(8), 1)
        self.cb.on_dispatch()
        self.assertEqual(self.cb.allowed_slots(8), 0)
        self.cb.record_success()
        self.assertEqual(self.cb.state, BreakerState.CLOSED)
        self.assertEqual(self.cb.allowed_slots(8), 8)
        self.assertEqual([n for _, n in self.changes],
                         [BreakerState.OPEN, BreakerState.HALF_OPEN, BreakerState.CLOSED])

    def test_fallo_en_semiabierto_reabre(self):
        for _ in range(3):
            self.cb.record_failure()
        self.clock.now = 3.0
        self.assertEqual(self.cb.state, BreakerState.HALF_OPEN)
        self.cb.on_dispatch(); self.cb.record_failure()
        self.assertEqual(self.cb.state, BreakerState.OPEN)
        self.assertEqual(self.cb.open_count, 2)

    def test_exito_tardio_no_cierra_un_circuito_abierto(self):
        """Regresión v2.1: tareas en vuelo que terminan bien tras la apertura."""
        for _ in range(3):
            self.cb.record_failure()
        self.cb.record_success()  # resultado de una tarea despachada antes de abrir
        self.assertEqual(self.cb.state, BreakerState.OPEN)
        self.assertEqual(self.cb.allowed_slots(8), 0)

    def test_deshabilitado_no_interfiere(self):
        cb = CircuitBreaker(1, 1.0, enabled=False)
        for _ in range(10):
            cb.record_failure()
        self.assertEqual(cb.allowed_slots(5), 5)


if __name__ == "__main__":
    unittest.main()
