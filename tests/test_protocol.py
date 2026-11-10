import socket
import threading
import unittest

from distritask.protocol import (Connection, ConnectionClosed, ProtocolError, encode, parse_address,
                                 recv_message)


class ProtocolTest(unittest.TestCase):
    def test_roundtrip_con_unicode(self):
        a, b = socket.socketpair()
        with Connection(a) as ca, Connection(b) as cb:
            ca.send({"type": "X", "texto": "año, señal, ñandú", "n": [1, 2, 3]})
            self.assertEqual(cb.recv(), {"type": "X", "texto": "año, señal, ñandú", "n": [1, 2, 3]})

    def test_envios_concurrentes_no_se_mezclan(self):
        a, b = socket.socketpair()
        ca, cb = Connection(a), Connection(b)
        payload = "x" * 50_000

        def sender(i):
            for j in range(50):
                ca.send({"type": "M", "id": f"{i}-{j}", "p": payload})

        threads = [threading.Thread(target=sender, args=(i,)) for i in range(4)]
        received = []
        reader = threading.Thread(target=lambda: received.extend(cb.recv() for _ in range(200)))
        reader.start()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        reader.join(timeout=10)
        self.assertEqual(len({m["id"] for m in received}), 200)
        self.assertTrue(all(m["p"] == payload for m in received))
        ca.close(); cb.close()

    def test_cierre_remoto(self):
        a, b = socket.socketpair()
        a.close()
        with self.assertRaises(ConnectionClosed):
            recv_message(b)
        b.close()

    def test_mensaje_sin_tipo_es_invalido(self):
        a, b = socket.socketpair()
        a.sendall(encode({"type": "ok"})[:4] + b"")  # cabecera sin cuerpo completo
        a.close()
        with self.assertRaises(ConnectionClosed):
            recv_message(b)
        a2, b2 = socket.socketpair()
        body = b"[1,2]"
        a2.sendall(len(body).to_bytes(4, "big") + body)
        with self.assertRaises(ProtocolError):
            recv_message(b2)
        for s in (b, a2, b2):
            s.close()

    def test_parse_address(self):
        self.assertEqual(parse_address("10.0.0.5:6000"), ("10.0.0.5", 6000))
        self.assertEqual(parse_address("nodo"), ("nodo", 5000))


if __name__ == "__main__":
    unittest.main()
