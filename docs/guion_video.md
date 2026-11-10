# Guion del video — DistriTask

**Tejiendo redes: arquitectura de software entre hilos y nodos**
Arquitectura de Software · Unidad 2 · Actividad 4

| Dato | Valor |
|---|---|
| Duración objetivo | **≈ 14 minutos** (rango pedido: 12–15) |
| Presentador | [NOMBRE DEL ESTUDIANTE] |
| Diapositivas | `DistriTask_Presentacion.pptx` (18 diapositivas) |
| Demo en vivo | Diapositiva 10 (≈ 2 min 30 s) |
| Publicación | YouTube → Visibilidad: **No listado** |

> Cómo usar este guion: el texto en *"cursiva entre comillas"* es lo que dices. Las líneas con **[ACCIÓN]** son lo que haces en pantalla. Los tiempos son acumulados; si te pasas en una diapositiva, recorta en la siguiente. A ritmo natural (≈140 palabras por minuto) cada bloque cabe en su tiempo.

---

## Antes de grabar (lista de verificación)

1. Abre una terminal (PowerShell o Windows Terminal) **en la carpeta del repositorio** `distritask`.
2. Comprueba Python: `python --version` (3.10 o superior). Si `python` no responde, usa `py` en todos los comandos.
3. Ensayo rápido sin grabar: `python scripts/demo.py` (debe terminar con *400/400 completadas*).
4. Sube el tamaño de letra de la terminal (Ctrl + rueda del ratón) a **18–20 pt**.
5. Cierra notificaciones (Windows: *Asistente de concentración* / *No molestar*).
6. Deja abiertas dos ventanas: **PowerPoint** en modo presentación y la **terminal**. El navegador se abrirá solo con el dashboard.
7. Grabador sugerido: OBS Studio (lienzo 1920×1080, 30 fps, micrófono a −12 dB) o la grabadora de pantalla de PowerPoint.
8. Ten a mano el **plan B**: si algo falla en vivo, la diapositiva 10 ya muestra una captura del dashboard y el informe tiene capturas de la consola.

---

## Diapositiva 1 · Portada — `0:00 – 0:35`

*"Hola, mi nombre es [NOMBRE DEL ESTUDIANTE] y presento el mini proyecto de la Unidad 2 de Arquitectura de Software: **Tejiendo redes, arquitectura de software entre hilos y nodos**. Construí **DistriTask**, un sistema distribuido que reparte el análisis de registros, o logs, de una plataforma de microservicios entre varios nodos que trabajan en red, y donde cada nodo procesa varias tareas a la vez con un pool de hilos. El trabajo lo realicé de forma individual, así que asumí todos los roles: líder, arquitecto, desarrollador, evaluador de calidad y documentador."*

## Diapositiva 2 · Recorrido — `0:35 – 1:00`

*"El recorrido tiene seis partes: primero, qué sistema elegí y qué debía cumplir; luego la arquitectura y sus diagramas UML; después los seis patrones de diseño que apliqué; una demostración en vivo del prototipo; la evaluación de escalabilidad y robustez con los ajustes que hice; y, al final, las conclusiones."*

## Diapositiva 3 · El problema — `1:00 – 1:55`

*"Imaginemos un equipo de operaciones que, después de un incidente, necesita saber en segundos qué servicio concentra los errores. Seis microservicios —autenticación, pagos, pedidos, inventario, pasarela y notificaciones— escriben millones de líneas. Lo interesante es que la mayor parte del tiempo no se va en procesar, sino en esperar: descargar cada bloque desde el almacenamiento en la nube tarda unos 50 milisegundos y procesarlo apenas 2. Por eso el sistema sigue el modelo map–reduce: cada nodo analiza un bloque y el coordinador consolida el resultado.*

*Para elegir el tema hice una lluvia de ideas con cuatro alternativas y cinco criterios ponderados. Ganó el procesamiento paralelo con 4,90 sobre 5, sobre todo porque es el que mejor permite **medir** la escalabilidad y demostrar fallos."*

## Diapositiva 4 · Metas de calidad — `1:55 – 2:35`

*"Desde el diseño escribí los requisitos como escenarios de calidad con una medida concreta. Escalabilidad: crecer casi linealmente al agregar nodos. Rendimiento: que los hilos oculten la espera de E/S. Disponibilidad: no perder tareas si un nodo cae o se cuelga. Robustez: aislar un nodo que sigue vivo pero falla todo. Mantenibilidad: cambiar políticas sin tocar el núcleo. Y observabilidad: ver cada fallo en tiempo real. Abajo de cada tarjeta está el resultado que efectivamente obtuve; los iremos viendo."*

## Diapositiva 5 · Arquitectura Maestro–Trabajador — `2:35 – 3:25`

*"La arquitectura es de tipo Maestro–Trabajador sobre TCP. En el centro está el **coordinador**, que actúa como mediador: es el único que conoce a clientes, nodos y monitores. A la derecha están los **nodos trabajadores**, cada uno con su pool de cuatro hilos, o de procesos para cargas de CPU. A la izquierda, el cliente que envía trabajos y los observadores: un monitor de consola y un dashboard web.*

*Una decisión clave fue separar dos planos de comunicación: el **plano de control**, por donde el nodo se registra y envía un latido cada segundo, y el **plano de datos**, por donde viajan los lotes de tareas y los resultados. Así, si los datos se saturan, los latidos siguen llegando."*

## Diapositiva 6 · Diagrama de componentes — `3:25 – 4:05`

*"En el diagrama de componentes se ven los módulos distribuidos y sus interfaces. El coordinador expone tres interfaces sobre un mismo puerto: envío de trabajos, control de nodos y flujo de eventos; el primer mensaje de cada conexión decide su rol. El despachador no depende de una política concreta, sino de la abstracción de estrategia de balanceo, y habla con cada nodo únicamente a través de su proxy, que además contiene un Circuit Breaker. En el nodo, el servidor de datos y el agente de control son independientes."*

## Diapositiva 7 · Secuencia — `4:05 – 4:55`

*"El diagrama de secuencia muestra la interacción entre nodos e hilos. Uno: el hilo de control del nodo se registra y empieza a latir. Dos: el cliente envía el trabajo y recibe la confirmación de inmediato; el coordinador encola las tareas. Tres: el hilo despachador pregunta a la estrategia cuál es el nodo menos cargado y le envía un lote de hasta ocho tareas, respetando un límite de **créditos**. Cuatro: en el nodo, el fragmento *par* muestra los hilos del pool trabajando en paralelo; cada resultado vuelve por un hilo lector y libera un crédito. Y cinco: al terminar, el coordinador hace el *reduce* y devuelve el resumen."*

## Diapositiva 8 · Despliegue — `4:55 – 5:30`

*"El diagrama de despliegue describe la distribución física en una red de nube: el coordinador y el monitoreo en sus propias máquinas o contenedores, y un grupo de nodos con N réplicas. Los nodos son los que llaman primero al coordinador, así que cada réplica nueva se incorpora sola, sin configurar direcciones. El mismo código corre como procesos locales, con Docker Compose escalando réplicas, o en máquinas virtuales."*

## Diapositiva 9 · Patrones de diseño — `5:30 – 6:40`

*"Apliqué seis patrones, y cada uno resuelve un problema concreto que además pude medir.*
*— **Mediator**: todos hablan con el coordinador y nadie se conoce entre sí; eso centraliza la detección de fallos y el re-encolado: cero tareas perdidas.*
*— **Proxy remoto**: el nodo remoto se usa como un objeto local. Gracias a eso, cambiar el transporte fue un parámetro, y triplicó el rendimiento con tareas pequeñas.*
*— **Observer distribuido**: el coordinador publica eventos por tema y los monitores remotos se suscriben, cada uno con su propia cola para que uno lento no bloquee al sistema.*
*— **Strategy**: el balanceo es intercambiable; pasar de round-robin a least-loaded mejoró 36 % con un nodo lento.*
*— **Circuit Breaker**: aísla al nodo que está vivo pero falla todo; pasamos de 24 a 360 de 360 tareas.*
*— Y **Thread Pool** con productor–consumidor: concurrencia acotada en cada nodo, siete veces más rápido con ocho hilos.*
*Ahora veámoslo funcionando."*

## Diapositiva 10 · Demostración en vivo — `6:40 – 9:10`

**[ACCIÓN]** Cambia a la terminal (Alt + Tab).

*"Este comando levanta un clúster real: un coordinador y tres nodos, cada uno como un proceso independiente que solo se comunica por TCP, como si estuvieran en máquinas distintas."*

**[ACCIÓN]** Ejecuta:

```powershell
python scripts/demo.py --dashboard --pause --chunks 1500 --kill w2 --at 2
```

**[ACCIÓN]** Se abre el navegador con el dashboard. Muestra las tres tarjetas de nodos en estado UP.

*"En el navegador está el dashboard, que es otro observador remoto del bus de eventos. Los tres nodos están arriba, con cuatro hilos cada uno."*

**[ACCIÓN]** Vuelve a la terminal y presiona **Enter**. Regresa al navegador.

*"Envío un trabajo de 1.500 bloques de logs. Fíjense en las barras de créditos: cada nodo tiene como máximo ocho tareas en vuelo. Y a los dos segundos el script va a **matar** el proceso del nodo w2..."*

**[ACCIÓN]** Señala la tarjeta de w2 cuando cambie a **DOWN** y el evento `worker.down` en la lista de eventos.

*"...ahí está: w2 cae, el coordinador lo detecta en milisegundos porque se cierra su conexión, re-encola sus ocho tareas en vuelo y los otros dos nodos absorben la carga. El trabajo continúa."*

**[ACCIÓN]** Cuando termine, vuelve a la terminal y muestra el resumen.

*"Resultado: 1.500 de 1.500 tareas, cero fallidas, y el análisis agregado: niveles, tasa de error, latencias por servicio y los errores más frecuentes; es el mismo resultado que sin fallos."*

**[ACCIÓN]** Ejecuta el segundo escenario:

```powershell
python scripts/demo.py --degraded w3
```

*"Ahora el caso difícil: el nodo w3 está vivo, responde los latidos, pero falla el cien por ciento de sus tareas. Miren el monitor de eventos: tras tres fallos, el **circuito se abre**; a los dos segundos pasa a **semiabierto**, deja pasar una sola tarea de prueba, falla y se vuelve a abrir. El trabajo termina con 400 de 400 tareas."*

> **Opcional si te sobra tiempo (30 s):** `python scripts/demo.py --hang w2 --at 1 --recover-after 4 --keep` — muestra el cuelgue detectado por heartbeat a los ≈3 s y el evento `worker.recovered`. Presiona Enter para cerrar.

**[ACCIÓN]** Vuelve a PowerPoint (diapositiva 11).

## Diapositiva 11 · Escalabilidad — `9:10 – 9:55`

*"Pasemos a las mediciones. Cada configuración se repitió tres veces y reporto la mediana. A la izquierda, un solo nodo: con un hilo procesa 18,8 tareas por segundo y con ocho, 137: siete veces más, porque mientras un hilo espera la descarga otro procesa. Con 16 hilos ya se satura la CPU. A la derecha, agregando nodos: de 73 a 445 tareas por segundo con ocho nodos. La eficiencia baja a 76 %, pero ojo: aquí los ocho nodos comparten solo dos núcleos virtuales; en producción cada nodo trae su propia CPU."*

## Diapositiva 12 · El GIL — `9:55 – 10:30`

*"Con una carga de cálculo puro, una estimación de pi por Monte Carlo, la historia cambia: agregar hilos no mejora nada, de 12,9 baja a 11,8 tareas por segundo. Es el GIL de CPython, que solo deja ejecutar un hilo de Python a la vez. Con procesos o con más nodos sí se duplica el rendimiento, justo el límite de dos núcleos. Por eso cada nodo permite elegir entre pool de hilos o de procesos según la carga."*

## Diapositiva 13 · Robustez — `10:30 – 11:15`

*"En robustez: una caída se detecta en uno a tres milisegundos y un cuelgue en 2,9 segundos, por el timeout del latido. En ambos casos se completaron las 600 tareas. El escenario más revelador es el nodo degradado: sin protección se convierte en un **agujero negro**; como falla al instante, siempre parece el menos ocupado y recibe una y otra vez los reintentos. Solo terminan 24 de 360 tareas. La anti-afinidad de reintentos ya evita perderlas, pero desperdicia 344 intentos; con el Circuit Breaker, 360 de 360 y solo 11 intentos perdidos."*

## Diapositiva 14 · Ajustes V1 → V2.1 — `11:15 – 12:00`

*"Primero construí una versión 1 con decisiones habituales: una conexión por tarea, una tarea por mensaje, ocho créditos por hilo y round-robin. Luego apliqué los ajustes uno por uno para atribuir cada mejora. Con nodos iguales no hay diferencia. Con tareas muy pequeñas, la conexión persistente triplica el rendimiento. Con un nodo cuatro veces más lento, least-loaded con dos créditos mejora un 36 %. Y algo honesto: los lotes no aportaron en localhost, donde la red es casi instantánea; su beneficio aparece con latencia real."*

## Diapositiva 15 · Medir cambió las decisiones — `12:00 – 12:35`

*"Medir también me hizo encontrar un defecto real. Al combinar fallos en caliente, un nodo colgado y otro que empieza a fallar, el circuito se abría y se cerraba en milisegundos: el éxito tardío de una tarea vieja lo cerraba. En la versión 2.1 solo la tarea de prueba puede cerrar el circuito. Antes perdía hasta ocho tareas por corrida; después, cero en cinco de cinco. También vi que los créditos son un compromiso: dos por hilo es lo mejor con nodos lentos."*

## Diapositiva 16 · Cuellos de botella — `12:35 – 13:00`

*"Este es el resumen de cuellos de botella: la espera de E/S, resuelta con hilos; el GIL, con procesos o nodos; la conexión por tarea, con conexiones persistentes; el round-robin con nodos lentos, con least-loaded; y el nodo degradado, con el Circuit Breaker. El que queda pendiente es el coordinador centralizado, que es un punto único de fallo."*

## Diapositiva 17 · Conclusiones — `13:00 – 13:45`

*"Concluyo con cuatro ideas. Uno: hilos y nodos se complementan; los hilos ocultan la espera y los procesos o nodos escalan el cálculo. Dos: los patrones de diseño tienen un impacto medible en robustez y mantenibilidad. Tres: la robustez se diseña en capas; ningún mecanismo solo cubrió todos los fallos. Y cuatro: medir cambia las decisiones. El mayor desafío fue la concurrencia; la regla que más ayudó fue nunca hacer operaciones de red con un candado tomado. Como trabajo futuro: replicar el coordinador con elección de líder, créditos adaptativos y desplegarlo en Kubernetes."*

## Diapositiva 18 · Cierre — `13:45 – 14:00`

*"El código, las pruebas y los resultados están en el repositorio cuyo enlace aparece en pantalla y en el informe. Muchas gracias por su atención."*

---

## Después de grabar

1. Sube el video a YouTube → **Visibilidad: No listado** → copia el enlace.
2. Pega el enlace en: portada del informe Word, Anexo A del informe, diapositiva 18 y el área de entrega del LMS.
3. Pega también el enlace del repositorio de GitHub en esos mismos lugares.
4. Abre el enlace del video en una ventana privada del navegador para comprobar que se ve.
5. En Word, actualiza el índice: clic derecho sobre el índice → *Actualizar campos* → *Actualizar toda la tabla*.

## Campos por reemplazar (resaltados en amarillo en los archivos)

`[NOMBRE DEL ESTUDIANTE]` · `[CÓDIGO / ID]` · `[NOMBRE DE LA INSTITUCIÓN]` · `[Facultad / Escuela / Programa académico]` · `[NOMBRE DEL DOCENTE]` · `[GRUPO]` · `[CIUDAD]` · `[FECHA DE ENTREGA]` · `[ENLACE AL VIDEO]` · `[ENLACE AL REPOSITORIO GITHUB]` · fechas del cronograma `[dd/mm – dd/mm]` · `[editor de video]`.
