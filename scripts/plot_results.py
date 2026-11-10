"""Genera las gráficas de resultados (PNG) a partir de results/benchmark_results.json.

Uso:  python scripts/plot_results.py [--input results/benchmark_results.json] [--out docs/img]
Requiere matplotlib (pip install matplotlib).
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import MaxNLocator  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent

# Paleta categórica validada (orden fijo) y tinta del gráfico
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300"]
GOOD, CRITICAL = "#0ca30c", "#d03b3b"
SURFACE, INK, INK2, MUTED, GRID, AXIS = "#fcfcfb", "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"

plt.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 10.5, "axes.facecolor": SURFACE,
    "figure.facecolor": SURFACE, "axes.edgecolor": AXIS, "axes.labelcolor": INK2,
    "xtick.color": MUTED, "ytick.color": MUTED, "axes.grid": True, "grid.color": GRID,
    "grid.linewidth": 0.8, "axes.axisbelow": True, "axes.spines.top": False,
    "axes.spines.right": False, "axes.titleweight": "bold", "axes.titlesize": 12.5,
    "axes.titlecolor": INK, "axes.titlelocation": "left", "legend.frameon": False,
    "savefig.dpi": 200, "savefig.bbox": "tight",
})


PRETTY = {
    "V1 linea base": "V1 línea base", "+ conexion persistente": "+ conexión persistente",
    "+ lotes (8)": "+ lotes de 8 tareas", "V2 (+ least-loaded, creditos=2)": "V2 (+ least-loaded, 2 créditos/hilo)",
    "Homogeneo (3 nodos iguales)": "Homogéneo (3 nodos iguales)",
    "Heterogeneo (w3 4x mas lento)": "Heterogéneo (w3 4x más lento)",
    "Tareas finas (2000 x ~0.5 ms)": "Tareas finas (2000 x ~0,5 ms)",
    "Caida abrupta de w2 (kill)": "Caída abrupta de w2 (kill)",
}


def pretty(text: str) -> str:
    return PRETTY.get(text, text)


def subtitle(ax, text: str) -> None:
    ax.text(0, 1.02, text, transform=ax.transAxes, fontsize=9.5, color=INK2, va="bottom")


def line_with_ideal(ax, xs, ys, ideal, label, color):
    ax.plot(xs, ideal, color=MUTED, lw=1.5, ls=(0, (4, 3)), label="Escalado lineal ideal")
    ax.plot(xs, ys, color=color, lw=2.2, marker="o", ms=7, mec=SURFACE, mew=2, label=label,
            solid_capstyle="round")
    ax.annotate(f"{ys[-1]:.0f} t/s", (xs[-1], ys[-1]), textcoords="offset points", xytext=(6, -4),
                color=INK, fontsize=9.5, fontweight="bold")


def bar_label(ax, rects, fmt="{:.0f}", color=INK):
    for r in rects:
        h = r.get_height()
        ax.annotate(fmt.format(h), (r.get_x() + r.get_width() / 2, h), textcoords="offset points",
                    xytext=(0, 3), ha="center", fontsize=8.5, color=color)


def plot_e1(d, out):
    rows = d["rows"]
    xs = [r["threads"] for r in rows]
    ys = [r["throughput_tps"] for r in rows]
    fig, ax = plt.subplots(figsize=(7.2, 3.9))
    line_with_ideal(ax, xs, ys, [ys[0] * x for x in xs], "Throughput medido (mediana)", SERIES[0])
    ax.set_xticks(xs, [str(x) for x in xs])
    ax.set_xlabel("Hilos en el pool del nodo")
    ax.set_ylabel("Tareas por segundo")
    ax.set_ylim(0, max(max(ys), ys[0] * xs[-1]) * 1.08)
    ax.set_title("E1. Escalabilidad vertical: hilos por nodo", pad=22)
    subtitle(ax, d["params"])
    ax.legend(loc="upper left")
    fig.savefig(out / "e1_hilos.png")
    plt.close(fig)


def plot_e2(d, out):
    rows = d["rows"]
    xs = [r["nodes"] for r in rows]
    ys = [r["throughput_tps"] for r in rows]
    eff = [100 * y / (ys[0] * x) for x, y in zip(xs, ys)]
    fig, (ax, ax2) = plt.subplots(1, 2, figsize=(10.5, 3.9), gridspec_kw={"width_ratios": [1.6, 1]})
    line_with_ideal(ax, xs, ys, [ys[0] * x for x in xs], "Throughput medido (mediana)", SERIES[0])
    ax.set_xticks(xs)
    ax.set_xlabel("Nodos trabajadores (4 hilos c/u)")
    ax.set_ylabel("Tareas por segundo")
    ax.set_ylim(0, ys[0] * xs[-1] * 1.08)
    ax.set_title("E2. Escalabilidad horizontal: número de nodos", pad=22)
    subtitle(ax, d["params"])
    ax.legend(loc="upper left")
    rects = ax2.bar([str(x) for x in xs], eff, width=0.55, color=SERIES[0])
    bar_label(ax2, rects, "{:.0f} %")
    ax2.set_ylim(0, 115)
    ax2.set_xlabel("Nodos")
    ax2.set_ylabel("Eficiencia (speedup / nodos)")
    ax2.set_title("Eficiencia paralela", pad=22)
    subtitle(ax2, "100 % = escalado perfecto")
    fig.tight_layout()
    fig.savefig(out / "e2_nodos.png")
    plt.close(fig)


def plot_e3(d, out):
    fig, ax = plt.subplots(figsize=(7.2, 3.9))
    modes = []
    for r in d["rows"]:
        if r["mode"] not in modes:
            modes.append(r["mode"])
    labels = {"hilos (1 nodo)": "1 nodo, N hilos (GIL)", "procesos (1 nodo)": "1 nodo, N procesos",
              "nodos x 1 hilo": "N nodos x 1 hilo"}
    for i, mode in enumerate(modes):
        rows = [r for r in d["rows"] if r["mode"] == mode]
        xs = [r["n"] for r in rows]
        ys = [r["throughput_tps"] for r in rows]
        ax.plot(xs, ys, color=SERIES[i], lw=2.2, marker="o", ms=7, mec=SURFACE, mew=2,
                label=labels.get(mode, mode))
        ax.annotate(labels.get(mode, mode).split(",")[-1].strip(), (xs[-1], ys[-1]),
                    textcoords="offset points", xytext=(6, -3), fontsize=8.5, color=INK2)
    ax.set_xticks([1, 2, 4])
    ax.set_xlabel("N (hilos, procesos o nodos)")
    ax.set_ylabel("Tareas por segundo")
    ax.set_ylim(bottom=0)
    ax.set_xlim(0.8, 5.2)
    ax.set_title("E3. Carga CPU pura: el GIL limita a los hilos", pad=22)
    subtitle(ax, d["params"] + " (host de 2 vCPU)")
    ax.legend(loc="center right", bbox_to_anchor=(1.0, 0.62))
    fig.savefig(out / "e3_cpu_gil.png")
    plt.close(fig)


def plot_e4(d, out):
    rows = d["rows"]
    scenarios = list(dict.fromkeys(r["scenario"] for r in rows))
    configs = list(dict.fromkeys(r["config"] for r in rows))
    fig, ax = plt.subplots(figsize=(10.5, 4.2))
    width = 0.19
    for j, cfg in enumerate(configs):
        vals = []
        for sc in scenarios:
            base = next(r for r in rows if r["scenario"] == sc and r["config"] == configs[0])["throughput_tps"]
            cur = next(r for r in rows if r["scenario"] == sc and r["config"] == cfg)["throughput_tps"]
            vals.append(cur / base)
        xs = [i + (j - (len(configs) - 1) / 2) * (width + 0.02) for i in range(len(scenarios))]
        rects = ax.bar(xs, vals, width=width, color=SERIES[j], label=pretty(cfg))
        bar_label(ax, rects, "{:.2f}x")
    ax.axhline(1.0, color=AXIS, lw=1)
    ax.set_xticks(range(len(scenarios)), [pretty(sc) for sc in scenarios], color=INK2)
    ax.set_ylabel("Throughput relativo a V1 (x)")
    ax.set_title("E4. De la versión 1 a la versión 2: efecto de cada ajuste", pad=22)
    subtitle(ax, "3 nodos x 4 hilos; mediana de 3 corridas; 1.0x = línea base")
    ax.legend(loc="upper left", ncol=2, fontsize=9)
    ax.set_ylim(0, max(ax.get_ylim()[1], 2.0) * 1.12)
    fig.savefig(out / "e4_versiones.png")
    plt.close(fig)


def plot_e5(d, out):
    rows = d["rows"]
    faults = list(dict.fromkeys(r["fault"] for r in rows))
    fig, ax = plt.subplots(figsize=(8.6, 3.9))
    width = 0.34
    for j, (profile, label) in enumerate((("baseline", "V1 línea base"), ("optimized", "V2 optimizada"))):
        vals = [next(r for r in rows if r["profile"] == profile and r["fault"] == f)["makespan_s"] for f in faults]
        xs = [i + (j - 0.5) * (width + 0.03) for i in range(len(faults))]
        rects = ax.bar(xs, vals, width=width, color=SERIES[j], label=label)
        bar_label(ax, rects, "{:.2f} s")
    ax.set_xticks(range(len(faults)), [pretty(f) for f in faults], color=INK2)
    ax.set_ylabel("Tiempo total del trabajo (s)")
    ax.set_title("E5. Tolerancia a fallos: 600/600 tareas completadas en todos los casos", pad=22)
    subtitle(ax, d["params"])
    ax.legend(loc="upper left")
    ax.set_ylim(0, ax.get_ylim()[1] * 1.12)
    fig.savefig(out / "e5_fallos.png")
    plt.close(fig)


def plot_e6(d, out):
    rows = d["rows"]
    labels = [r["config"] for r in rows]
    done = [r["done"] for r in rows]
    failed = [r["failed"] for r in rows]
    fig, ax = plt.subplots(figsize=(9.6, 3.9))
    y = range(len(rows))
    ax.barh(y, done, height=0.52, color=GOOD, label="Completadas")
    ax.barh(y, failed, left=[d_ + 2 for d_ in done], height=0.52, color=CRITICAL,
            label="Fallidas (agotaron 3 intentos)")
    for i, r in enumerate(rows):
        wasted = r["attempts"] - r["done"]
        ax.text(r["done"] + r["failed"] + 8, i, f"{r['done']:.0f} ok · {r['failed']:.0f} fallidas\n"
                f"{wasted:.0f} intentos desperdiciados", va="center", fontsize=8.8, color=INK)
    ax.set_yticks(list(y), labels, color=INK2)
    ax.invert_yaxis()
    ax.set_xlim(0, rows[0]["tasks"] * 1.45)
    ax.set_xlabel(f"Tareas (de {rows[0]['tasks']})")
    ax.grid(axis="y", visible=False)
    ax.set_title("E6. Nodo degradado: anti-afinidad y Circuit Breaker evitan el \"agujero negro\"", pad=22)
    subtitle(ax, d["params"] + "; mediana de 3 corridas")
    ax.legend(loc="upper center", bbox_to_anchor=(0.4, -0.18), ncol=2, fontsize=9)
    fig.savefig(out / "e6_circuit_breaker.png")
    plt.close(fig)


def plot_e8(d, out):
    before = d["before_fix_failed_per_run"]
    after = [r["failed"] for r in d["rows"][0]["runs"]][:len(before)]
    fig, ax = plt.subplots(figsize=(7.4, 3.5))
    xs = range(len(before))
    w = 0.36
    r1 = ax.bar([x - w / 2 - 0.01 for x in xs], before, width=w, color=CRITICAL, label="v2.0 (antes de la corrección)")
    r2 = ax.bar([x + w / 2 + 0.01 for x in xs], after, width=w, color=SERIES[0], label="v2.1 (después)")
    bar_label(ax, r1, "{:.0f}")
    bar_label(ax, r2, "{:.0f}")
    ax.set_xticks(list(xs), [f"Corrida {i + 1}" for i in xs], color=INK2)
    ax.set_ylabel("Tareas perdidas (fallidas)")
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    ax.set_ylim(0, max(before) * 1.3)
    ax.set_title("E8. Fallos combinados en caliente: corrección del Circuit Breaker", pad=22)
    subtitle(ax, "4 nodos, 1500 tareas; w3 se cuelga y w4 empieza a fallar durante el trabajo")
    ax.legend(loc="upper right")
    fig.savefig(out / "e8_hot_faults.png")
    plt.close(fig)


def plot_e7(d, out):
    rows = d["rows"]
    scenarios = list(dict.fromkeys(r["scenario"] for r in rows))
    fig, axes = plt.subplots(1, 2, figsize=(10.5, 3.8))
    for ax, sc in zip(axes, scenarios):
        sub = [r for r in rows if r["scenario"] == sc]
        xs = [str(r["prefetch"]) for r in sub]
        ys = [r["throughput_tps"] for r in sub]
        colors = [SERIES[0] if r["prefetch"] == 2 else "#9ec5f4" for r in sub]
        rects = ax.bar(xs, ys, width=0.55, color=colors)
        bar_label(ax, rects, "{:,.0f}")
        ax.set_xlabel("Créditos por hilo (prefetch)")
        ax.set_title(pretty(sc), pad=8, fontsize=11)
        ax.set_ylim(0, max(ys) * 1.15)
    axes[0].set_ylabel("Tareas por segundo")
    fig.suptitle("E7. Ajuste fino de créditos: compromiso entre heterogeneidad y granularidad",
                 x=0.01, ha="left", fontweight="bold", fontsize=12.5, color=INK)
    fig.text(0.01, 0.905, "Barra oscura = valor elegido para la V2 (2 créditos por hilo)", fontsize=9.5,
             color=INK2)
    fig.tight_layout(rect=(0, 0, 1, 0.9))
    fig.savefig(out / "e7_creditos.png")
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(ROOT / "results" / "benchmark_results.json"))
    parser.add_argument("--out", default=str(ROOT / "docs" / "img"))
    args = parser.parse_args()
    data = json.loads(Path(args.input).read_text(encoding="utf-8"))
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    for key, fn in (("E1", plot_e1), ("E2", plot_e2), ("E3", plot_e3), ("E4", plot_e4),
                    ("E5", plot_e5), ("E6", plot_e6), ("E7", plot_e7), ("E8", plot_e8)):
        if key in data:
            fn(data[key], out)
            print(f"{key} -> ok")


if __name__ == "__main__":
    main()
