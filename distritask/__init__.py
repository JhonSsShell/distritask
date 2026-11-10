"""DistriTask: prototipo de sistema distribuido de procesamiento paralelo.

Arquitectura Maestro-Trabajador (Coordinator / Worker nodes) que reparte
tareas entre nodos de una red TCP; cada nodo ejecuta las tareas en un pool
de hilos. Patrones aplicados: Proxy remoto, Mediator, Observer distribuido
(publicar/suscribir), Strategy (balanceo de carga), Circuit Breaker y
Thread Pool / Productor-Consumidor.
"""

__version__ = "1.0.0"
