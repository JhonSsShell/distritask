"""Dashboard web en tiempo real (observador remoto + Server-Sent Events).

Se suscribe al bus de eventos del coordinador por TCP y retransmite los
eventos a los navegadores conectados mediante SSE. Solo usa la biblioteca
estándar y no depende de recursos externos (funciona sin Internet).

Uso:  python -m distritask.dashboard --coordinator 127.0.0.1:5000 --http-port 8080
      y abrir http://127.0.0.1:8080
"""
from __future__ import annotations

import argparse
import json
import logging
import queue
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from .monitor import subscribe
from .protocol import ConnectionClosed, ProtocolError, parse_address

log = logging.getLogger("distritask.dashboard")

TOPICS = ["stats", "worker.*", "breaker.*", "job.*", "task.retry", "task.requeued", "task.failed"]


class Broadcaster:
    """Reparte cada evento recibido a todas las colas de los navegadores."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._clients: list[queue.Queue[str]] = []
        self.last_stats: str | None = None

    def add(self) -> "queue.Queue[str]":
        q: queue.Queue[str] = queue.Queue(maxsize=500)
        with self._lock:
            self._clients.append(q)
        if self.last_stats:
            q.put_nowait(self.last_stats)
        return q

    def remove(self, q: "queue.Queue[str]") -> None:
        with self._lock:
            if q in self._clients:
                self._clients.remove(q)

    def publish(self, payload: dict[str, Any]) -> None:
        data = json.dumps(payload, ensure_ascii=False)
        if payload.get("topic") == "stats":
            self.last_stats = data
        with self._lock:
            clients = list(self._clients)
        for q in clients:
            try:
                q.put_nowait(data)
            except queue.Full:
                pass


def pump_events(host: str, port: int, hub: Broadcaster) -> None:
    while True:
        try:
            for event in subscribe(host, port, TOPICS):
                hub.publish({"topic": event["topic"], "data": event["data"], "ts": event["ts"]})
        except (ConnectionClosed, OSError, ProtocolError):
            hub.publish({"topic": "dashboard.disconnected", "data": {}, "ts": time.time()})
            time.sleep(1.0)


def make_handler(hub: Broadcaster, coordinator: str) -> type[BaseHTTPRequestHandler]:
    page = PAGE.replace("{{COORDINATOR}}", coordinator).encode("utf-8")

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: Any) -> None:  # silencia el log HTTP
            pass

        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/events":
                self._stream()
            elif self.path in ("/", "/index.html"):
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(page)))
                self.end_headers()
                self.wfile.write(page)
            else:
                self.send_error(404)

        def _stream(self) -> None:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.end_headers()
            q = hub.add()
            try:
                while True:
                    try:
                        data = q.get(timeout=10)
                        self.wfile.write(f"data: {data}\n\n".encode("utf-8"))
                    except queue.Empty:
                        self.wfile.write(b": keep-alive\n\n")
                    self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
                pass
            finally:
                hub.remove(q)

    return Handler


PAGE = r"""<!doctype html>
<html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>DistriTask - Dashboard</title>
<style>
:root{--bg:#0f1720;--panel:#16212d;--line:#243446;--text:#e6edf3;--muted:#8ea0b4;--ok:#2fbf71;--bad:#ef5350;--warn:#f5a524;--accent:#3ba7ff}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:14px/1.45 "Segoe UI",system-ui,-apple-system,sans-serif}
header{display:flex;align-items:center;justify-content:space-between;padding:14px 22px;border-bottom:1px solid var(--line)}
h1{font-size:18px;margin:0;font-weight:600;letter-spacing:.2px}h1 span{color:var(--accent)}
.conn{font-size:12px;color:var(--muted)}.dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--bad);margin-right:6px}.dot.on{background:var(--ok)}
main{padding:18px 22px;display:grid;gap:16px}
.kpis{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:12px}
.kpi{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:12px 14px}
.kpi .v{font-size:26px;font-weight:650;font-variant-numeric:tabular-nums}.kpi .l{color:var(--muted);font-size:12px;text-transform:uppercase;letter-spacing:.6px}
.row{display:grid;grid-template-columns:2fr 1fr;gap:16px}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:14px}
.panel h2{font-size:13px;margin:0 0 10px;color:var(--muted);font-weight:600;text-transform:uppercase;letter-spacing:.6px}
.workers{display:grid;grid-template-columns:repeat(auto-fill,minmax(210px,1fr));gap:12px}
.w{border:1px solid var(--line);border-radius:9px;padding:11px;background:#121b25}
.w.down{border-color:var(--bad);opacity:.75}.w .top{display:flex;justify-content:space-between;align-items:center;margin-bottom:6px}
.w .id{font-weight:650;font-size:15px}.badge{font-size:11px;padding:2px 8px;border-radius:99px;font-weight:600}
.UP{background:rgba(47,191,113,.15);color:var(--ok)}.DOWN{background:rgba(239,83,80,.15);color:var(--bad)}
.closed{color:var(--muted)}.open{color:var(--bad)}.half_open{color:var(--warn)}
.bar{height:8px;background:#0b121a;border-radius:6px;overflow:hidden;margin:6px 0}.bar i{display:block;height:100%;background:var(--accent);transition:width .3s}
.meta{display:flex;justify-content:space-between;color:var(--muted);font-size:12px;font-variant-numeric:tabular-nums}
canvas{width:100%;height:220px;display:block}
.log{height:260px;overflow:auto;font:12px/1.5 Consolas,"Cascadia Mono",monospace}
.log div{padding:2px 0;border-bottom:1px dashed #1e2b39}.log .t{color:var(--muted)}
.e-worker\.down,.e-breaker\.opened,.e-task\.failed{color:var(--bad)}.e-worker\.registered,.e-job\.completed,.e-breaker\.closed,.e-worker\.recovered{color:var(--ok)}
.e-breaker\.half_open,.e-task\.retry,.e-task\.requeued{color:var(--warn)}
.cfg{color:var(--muted);font-size:12px}
@media (max-width:900px){.kpis{grid-template-columns:repeat(3,1fr)}.row{grid-template-columns:1fr}}
</style></head><body>
<header><h1>Distri<span>Task</span> &middot; panel del cl&uacute;ster</h1>
<div class="conn"><span id="dot" class="dot"></span><span id="conn">conectando a {{COORDINATOR}}...</span></div></header>
<main>
<section class="kpis">
<div class="kpi"><div class="v" id="k-tps">0</div><div class="l">Tareas / s</div></div>
<div class="kpi"><div class="v" id="k-done">0</div><div class="l">Completadas</div></div>
<div class="kpi"><div class="v" id="k-pending">0</div><div class="l">Pendientes</div></div>
<div class="kpi"><div class="v" id="k-inflight">0</div><div class="l">En vuelo</div></div>
<div class="kpi"><div class="v" id="k-retry">0</div><div class="l">Reintentos</div></div>
<div class="kpi"><div class="v" id="k-requeue">0</div><div class="l">Re-encoladas</div></div>
</section>
<section class="panel"><h2>Nodos trabajadores <span class="cfg" id="cfg"></span></h2><div class="workers" id="workers"></div></section>
<section class="row">
<div class="panel"><h2>Throughput (tareas/s, &uacute;ltimos 60 s)</h2><canvas id="chart" width="900" height="220"></canvas></div>
<div class="panel"><h2>Eventos</h2><div class="log" id="log"></div></div>
</section>
</main>
<script>
const $=id=>document.getElementById(id);const series=[];const MAX=120;
function fmt(n){return Number(n).toLocaleString('es-CO')}
function draw(){const c=$('chart'),x=c.getContext('2d'),W=c.width,H=c.height;x.clearRect(0,0,W,H);
const max=Math.max(10,...series)*1.15;x.strokeStyle='#243446';x.lineWidth=1;x.fillStyle='#8ea0b4';x.font='12px Segoe UI';
for(let i=0;i<=4;i++){const y=H-20-(H-30)*i/4;x.beginPath();x.moveTo(40,y);x.lineTo(W,y);x.stroke();x.fillText(Math.round(max*i/4),4,y+4)}
if(series.length<2)return;x.beginPath();series.forEach((v,i)=>{const px=40+(W-40)*i/(MAX-1),py=H-20-(H-30)*v/max;i?x.lineTo(px,py):x.moveTo(px,py)});
x.strokeStyle='#3ba7ff';x.lineWidth=2.5;x.stroke();x.lineTo(40+(W-40)*(series.length-1)/(MAX-1),H-20);x.lineTo(40,H-20);x.closePath();x.fillStyle='rgba(59,167,255,.12)';x.fill()}
function stats(d){$('k-tps').textContent=d.throughput_tps.toFixed(1);$('k-done').textContent=fmt(d.counters['task.completed']);
$('k-pending').textContent=fmt(d.pending);$('k-inflight').textContent=fmt(d.inflight);$('k-retry').textContent=fmt(d.counters['task.retry']);
$('k-requeue').textContent=fmt(d.counters['task.requeued']);
$('cfg').textContent=`· estrategia=${d.config.strategy} · transporte=${d.config.transport} · lote=${d.config.batch_size} · breaker=${d.config.breaker?'on':'off'} · anti-afinidad=${d.config.retry_affinity?'on':'off'}`;
$('workers').innerHTML=d.workers.map(w=>{const pct=Math.min(100,100*w.inflight/Math.max(1,w.limit));
return `<div class="w ${w.state==='DOWN'?'down':''}"><div class="top"><span class="id">${w.worker_id}</span><span class="badge ${w.state}">${w.state}</span></div>
<div class="meta"><span>${w.address}</span><span class="${w.breaker}">circuito: ${w.breaker.replace('_','-')}</span></div>
<div class="bar"><i style="width:${pct}%"></i></div><div class="meta"><span>en vuelo ${w.inflight}/${w.limit}</span><span>${w.capacity} hilos</span></div>
<div class="meta"><span>ok ${fmt(w.completed)} · fallos ${fmt(w.failed)}</span><span>${w.ewma_latency_ms} ms</span></div></div>`}).join('');
series.push(d.throughput_tps);if(series.length>MAX)series.shift();draw()}
function logEvent(e){const t=new Date(e.ts*1000).toLocaleTimeString('es-CO',{hour12:false});const d=e.data;let msg;
if(e.topic==='job.completed')msg=`trabajo ${d.job_id}: ${d.done}/${d.tasks} tareas en ${d.makespan_s.toFixed(2)} s (${d.throughput_tps} t/s)`;
else if(e.topic==='job.submitted')msg=`trabajo ${d.job_id} (${d.job_type}) con ${d.tasks} tareas`;
else msg=Object.entries(d).map(([k,v])=>`${k}=${v}`).join(' ');
const el=document.createElement('div');el.className='e-'+e.topic;el.innerHTML=`<span class="t">${t}</span> <b>${e.topic}</b> ${msg}`;
const log=$('log');log.prepend(el);while(log.children.length>200)log.lastChild.remove()}
function connect(){const es=new EventSource('/events');
es.onopen=()=>{$('dot').className='dot on';$('conn').textContent='conectado a {{COORDINATOR}}'};
es.onerror=()=>{$('dot').className='dot';$('conn').textContent='reconectando...'};
es.onmessage=m=>{const e=JSON.parse(m.data);if(e.topic==='stats'){stats(e.data);$('dot').className='dot on'}
else if(e.topic==='dashboard.disconnected'){$('dot').className='dot';$('conn').textContent='coordinador no disponible'}else logEvent(e)}}
draw();connect();
</script></body></html>
"""


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Dashboard web de DistriTask")
    parser.add_argument("--coordinator", default="127.0.0.1:5000")
    parser.add_argument("--http-host", default="127.0.0.1")
    parser.add_argument("--http-port", type=int, default=8080)
    args = parser.parse_args(argv)
    host, port = parse_address(args.coordinator)
    hub = Broadcaster()
    threading.Thread(target=pump_events, args=(host, port, hub), daemon=True).start()
    server = ThreadingHTTPServer((args.http_host, args.http_port), make_handler(hub, f"{host}:{port}"))
    server.daemon_threads = True
    print(f"Dashboard en http://{args.http_host}:{args.http_port}  (coordinador {host}:{port})", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
