#!/usr/bin/env python3
"""Generate report tables/figures from results/*.csv|json and compile report/report.tex.

Every number in the PDF comes from the results files (nothing is typed by hand).
  python scripts/build_report.py            # needs pdflatex + matplotlib
"""
import csv
import json
import os
import statistics as st
import subprocess

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
R = os.path.join(ROOT, "results")
REP = os.path.join(ROOT, "report")


def tex_bool(b):
    return "yes" if str(b) in ("True", "true", "1") else "no"


def sim_table():
    with open(os.path.join(R, "sim_outages.csv")) as f:
        rows = list(csv.DictReader(f))
    out = [r"\begin{tabular}{lrrrrrrrrc}", r"\toprule",
           r"Run & Outage (s) & Cap. & Generated & Peak & Evicted & Lost & Loss (\%) & Recon. (s) & State conv.\\",
           r"\midrule"]
    for r in rows:
        out.append(" & ".join([
            r["name"].replace("_", r"\_").replace("(", "(").replace("%", r"\%"),
            r["outage_s"], r["capacity"], r["generated"], r["peak_backlog"],
            r["evicted"], r["lost_events"], r["loss_pct"],
            r["drain_s_after_reconnect"], tex_bool(r["twin_state_converged"])]) + r"\\")
    out += [r"\bottomrule", r"\end{tabular}"]
    open(os.path.join(REP, "sim_table.tex"), "w").write("\n".join(out))


def sim_figure():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    with open(os.path.join(R, "sim_backlog.json")) as f:
        d = json.load(f)
    cap = d["params"]["capacity"]
    fig, ax = plt.subplots(figsize=(6.4, 3.2))
    for name in ("short", "medium", "long"):
        s = d["series"][name]
        ax.plot([x[0] for x in s], [x[1] for x in s], label=name, lw=1.3)
    ax.axhline(cap, color="k", ls="--", lw=.8, label=f"capacity = {cap}")
    ax.set_xlabel("virtual time (s)"); ax.set_ylabel("unacked outbox events")
    ax.legend(fontsize=8); ax.grid(alpha=.3); fig.tight_layout()
    fig.savefig(os.path.join(REP, "sim_backlog.png"), dpi=200)


def docker_tables():
    path = os.path.join(R, "summary.json")
    if not os.path.exists(path):
        for n in ("docker_table.tex", "conflict_table.tex"):
            p = os.path.join(REP, n)
            if os.path.exists(p):
                os.remove(p)
        return False
    with open(path) as f:
        d = json.load(f)
    groups = {}
    for r in d["outages"]:
        groups.setdefault(r["run"].rsplit("-", 1)[0], []).append(r)
    out = [r"\begin{tabular}{lrrrrrrrr}", r"\toprule",
           r"Run & n & Outage (s) & Peak & Buffer full & Evicted & Lost & Loss (\%) & Recon. latency (s) mean [min--max]\\",
           r"\midrule"]
    for k, rs in groups.items():
        lat = [r["reconcile_latency_s"] for r in rs if r["reconcile_latency_s"] not in (None, "")]
        lat = [float(x) for x in lat]
        latc = f"{st.mean(lat):.1f} [{min(lat):.1f}--{max(lat):.1f}]" if lat else "n/a"
        m = lambda key: st.mean(float(r[key]) for r in rs)
        out.append(" & ".join([k, str(len(rs)), str(rs[0]["outage_s"]),
                               f"{m('peak_backlog'):.0f}",
                               f"{sum(bool(r['buffer_filled']) for r in rs)}/{len(rs)}",
                               f"{m('evicted'):.0f}", f"{m('lost_events'):.0f}",
                               f"{m('loss_pct'):.1f}", latc]) + r"\\")
    out += [r"\bottomrule", r"\end{tabular}"]
    open(os.path.join(REP, "docker_table.tex"), "w").write("\n".join(out))
    c = [r"\begin{tabular}{lllllc}", r"\toprule",
         r"Scenario & During outage (edge/cloud) & Final edge & Final cloud & Correct winner & Conv. (s)\\",
         r"\midrule"]
    for r in d["conflicts"]:
        c.append(" & ".join([r["scenario"].replace("_", r"\_"),
                             f"{r['during_outage_edge']}/{r['during_outage_cloud']}",
                             str(r["final_edge"]), str(r["final_cloud"]),
                             "n/a (tie-break)" if r["correct_winner"] is None
                             else tex_bool(r["correct_winner"]),
                             str(r["convergence_latency_s"])]) + r"\\")
    c += [r"\bottomrule", r"\end{tabular}"]
    open(os.path.join(REP, "conflict_table.tex"), "w").write("\n".join(c))
    fig = os.path.join(R, "backlog.png")
    if os.path.exists(fig):
        import shutil
        shutil.copy(fig, os.path.join(REP, "docker_backlog.png"))
    return True


def main():
    sim_table(); sim_figure(); have = docker_tables()
    print("Docker results included:", have)
    for _ in range(2):
        subprocess.run(["pdflatex", "-interaction=nonstopmode", "report.tex"],
                       cwd=REP, check=True, stdout=subprocess.DEVNULL)
    print("report/report.pdf built")


if __name__ == "__main__":
    main()
