#!/usr/bin/env python3
"""Generate benchmark plots for the dissertation."""

import argparse
import csv
import os
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

# ---------------------------------------------------------------------------
# Global style — applied once at import time
# ---------------------------------------------------------------------------
plt.rcParams.update({
    'font.family': 'serif',
    'font.size': 10,
    'axes.titlesize': 12,
    'axes.labelsize': 11,
    'xtick.labelsize': 9,
    'ytick.labelsize': 9,
    'legend.fontsize': 9,
    'figure.figsize': (7, 4.5),
    'figure.dpi': 300,
    'axes.grid': True,
    'grid.alpha': 0.3,
    'grid.linestyle': '--',
    'axes.spines.top': False,
    'axes.spines.right': False,
})

# Colorblind-friendly palette (Tableau 10)
COLORS = ['#4e79a7', '#f28e2b', '#e15759', '#76b7b2', '#59a14f',
          '#edc948', '#b07aa1', '#ff9da7', '#9c755f', '#bab0ac']

PROTOCOL_LABELS = {
    'mascot': 'MASCOT',
    'shamir': 'Shamir',
    'rep_ring': 'Replicated Ring',
    'replicated_ring': 'Replicated Ring',
}


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

def load_csv(path: Path) -> list[dict]:
    """Load CSV into a list of dicts with numeric conversion where possible."""
    path = Path(path)
    if not path.exists():
        return []
    rows = []
    try:
        with open(path, newline='', encoding='utf-8') as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                converted = {}
                for k, v in row.items():
                    v = v.strip()
                    try:
                        converted[k] = int(v)
                    except ValueError:
                        try:
                            converted[k] = float(v)
                        except ValueError:
                            converted[k] = v
                rows.append(converted)
    except Exception as exc:
        print(f"  Warning: could not read {path}: {exc}", file=sys.stderr)
    return rows


def aggregate(rows: list[dict], group_keys: list[str], value_key: str) -> dict:
    """Group rows by composite key, return {group_tuple: (mean, std, n)}.

    group_keys: list of column names whose values form the key.
    value_key:  column to aggregate.
    Returns dict mapping tuple(group_values) -> (mean, std, n).
    """
    buckets: dict = defaultdict(list)
    for row in rows:
        try:
            key = tuple(row[k] for k in group_keys)
            val = float(row[value_key])
            buckets[key].append(val)
        except (KeyError, ValueError, TypeError):
            continue
    result = {}
    for key, vals in buckets.items():
        arr = np.array(vals, dtype=float)
        result[key] = (float(np.mean(arr)), float(np.std(arr, ddof=1) if len(arr) > 1 else 0.0), len(arr))
    return result


def filter_rows(rows: list[dict], **conditions) -> list[dict]:
    """Return rows where each condition key equals its value (string or numeric)."""
    out = []
    for row in rows:
        if all(str(row.get(k, '')).lower() == str(v).lower() for k, v in conditions.items()):
            out.append(row)
    return out


def numeric_values(rows: list[dict], key: str) -> list[float]:
    """Extract float values for a column, skipping missing/non-numeric entries."""
    result = []
    for row in rows:
        try:
            result.append(float(row[key]))
        except (KeyError, ValueError, TypeError):
            pass
    return result


def save_fig(fig: plt.Figure, out_dir: str, filename: str) -> None:
    path = os.path.join(out_dir, filename)
    fig.savefig(path, bbox_inches='tight')
    plt.close(fig)
    print(f"  Generated {filename}")


# ---------------------------------------------------------------------------
# Plot 1: Protocol Comparison (N=3)
# ---------------------------------------------------------------------------

def plot_protocol_comparison(direct_data: list[dict], out_dir: str) -> None:
    """Grouped bar chart: wall-clock time per protocol at N=3."""
    n3 = filter_rows(direct_data, num_parties=3)
    if not n3:
        print("  Skipping protocol_comparison.pdf — no data for num_parties=3")
        return

    # Collect protocols present in the data
    protocols_raw = sorted({str(r.get('protocol', '')).lower() for r in n3 if r.get('protocol')})
    if not protocols_raw:
        print("  Skipping protocol_comparison.pdf — no protocol column found")
        return

    time_stats = aggregate(n3, ['protocol'], 'wall_clock_secs')
    comm_stats = aggregate(n3, ['protocol'], 'data_sent_mb')

    labels = [PROTOCOL_LABELS.get(p, p.upper()) for p in protocols_raw]
    means = []
    stds = []
    comm_means = []
    for p in protocols_raw:
        key = (p,)
        t = time_stats.get(key, (0.0, 0.0, 0))
        c = comm_stats.get(key, (0.0, 0.0, 0))
        means.append(t[0])
        stds.append(t[1])
        comm_means.append(c[0])

    x = np.arange(len(labels))
    bar_width = 0.5

    fig, ax = plt.subplots()
    bars = ax.bar(x, means, bar_width, yerr=stds, capsize=4,
                  color=COLORS[:len(labels)], alpha=0.85, error_kw={'linewidth': 1.2})

    # Annotate each bar with data sent
    for bar, cm in zip(bars, comm_means):
        if cm > 0:
            ax.annotate(
                f'{cm:.1f} MB',
                xy=(bar.get_x() + bar.get_width() / 2, bar.get_height()),
                xytext=(0, 6), textcoords='offset points',
                ha='center', va='bottom', fontsize=8,
            )

    ax.set_title('MPC Protocol Comparison (N=3 parties)')
    ax.set_ylabel('Wall-clock time (s)')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.yaxis.set_minor_locator(ticker.AutoMinorLocator())
    plt.tight_layout()
    save_fig(fig, out_dir, 'protocol_comparison.pdf')


# ---------------------------------------------------------------------------
# Plot 2: Computation Time vs Party Count
# ---------------------------------------------------------------------------

def plot_party_scaling_time(direct_data: list[dict], out_dir: str) -> None:
    """Line chart with error bands: wall-clock time vs num_parties per protocol."""
    if not direct_data:
        print("  Skipping party_scaling_time.pdf — no direct_mpc data")
        return

    protocols_raw = sorted({str(r.get('protocol', '')).lower() for r in direct_data if r.get('protocol')})
    stats = aggregate(direct_data, ['protocol', 'num_parties'], 'wall_clock_secs')

    # Gather all party counts
    all_parties = sorted({int(r['num_parties']) for r in direct_data
                          if 'num_parties' in r and str(r['num_parties']).isdigit()})
    if not all_parties:
        print("  Skipping party_scaling_time.pdf — no num_parties column")
        return

    fig, ax = plt.subplots()

    for idx, proto in enumerate(protocols_raw):
        color = COLORS[idx % len(COLORS)]
        label = PROTOCOL_LABELS.get(proto, proto.upper())

        xs, ys, errs = [], [], []
        for n in all_parties:
            key = (proto, n)
            if key in stats:
                mean, std, cnt = stats[key]
                xs.append(n)
                ys.append(mean)
                errs.append(std)

        if not xs:
            continue

        xs_arr = np.array(xs)
        ys_arr = np.array(ys)
        errs_arr = np.array(errs)

        if len(xs) == 1:
            # Single data point — plot as marker only
            ax.errorbar(xs_arr, ys_arr, yerr=errs_arr, fmt='o', color=color,
                        capsize=4, label=label, markersize=7)
        else:
            ax.plot(xs_arr, ys_arr, '-o', color=color, label=label, markersize=5)
            ax.fill_between(xs_arr, ys_arr - errs_arr, ys_arr + errs_arr,
                            alpha=0.15, color=color)

    ax.set_title('MPC Computation Time vs Party Count')
    ax.set_xlabel('Number of parties')
    ax.set_ylabel('Wall-clock time (s)')
    ax.set_xticks(all_parties)
    ax.legend()
    ax.yaxis.set_minor_locator(ticker.AutoMinorLocator())
    plt.tight_layout()
    save_fig(fig, out_dir, 'party_scaling_time.pdf')


# ---------------------------------------------------------------------------
# Plot 3: Communication vs Party Count
# ---------------------------------------------------------------------------

def plot_party_scaling_comm(direct_data: list[dict], out_dir: str) -> None:
    """Line chart: data sent per party vs num_parties per protocol."""
    if not direct_data:
        print("  Skipping party_scaling_comm.pdf — no direct_mpc data")
        return

    protocols_raw = sorted({str(r.get('protocol', '')).lower() for r in direct_data if r.get('protocol')})
    stats = aggregate(direct_data, ['protocol', 'num_parties'], 'data_sent_mb')
    all_parties = sorted({int(r['num_parties']) for r in direct_data
                          if 'num_parties' in r and str(r['num_parties']).isdigit()})
    if not all_parties:
        print("  Skipping party_scaling_comm.pdf — no num_parties column")
        return

    fig, ax = plt.subplots()

    for idx, proto in enumerate(protocols_raw):
        color = COLORS[idx % len(COLORS)]
        label = PROTOCOL_LABELS.get(proto, proto.upper())

        xs, ys, errs = [], [], []
        for n in all_parties:
            key = (proto, n)
            if key in stats:
                mean, std, cnt = stats[key]
                xs.append(n)
                ys.append(mean)
                errs.append(std)

        if not xs:
            continue

        xs_arr = np.array(xs)
        ys_arr = np.array(ys)
        errs_arr = np.array(errs)

        if len(xs) == 1:
            ax.errorbar(xs_arr, ys_arr, yerr=errs_arr, fmt='o', color=color,
                        capsize=4, label=label, markersize=7)
        else:
            ax.plot(xs_arr, ys_arr, '-o', color=color, label=label, markersize=5)
            ax.fill_between(xs_arr, ys_arr - errs_arr, ys_arr + errs_arr,
                            alpha=0.15, color=color)

    ax.set_title('MPC Communication vs Party Count')
    ax.set_xlabel('Number of parties')
    ax.set_ylabel('Data sent per party (MB)')
    ax.set_xticks(all_parties)
    ax.legend()
    ax.yaxis.set_minor_locator(ticker.AutoMinorLocator())
    plt.tight_layout()
    save_fig(fig, out_dir, 'party_scaling_comm.pdf')


# ---------------------------------------------------------------------------
# Plot 4: Veilid Privacy Overhead
# ---------------------------------------------------------------------------

def plot_veilid_overhead(direct_data: list[dict], veilid_data: list[dict], out_dir: str) -> None:
    """Grouped stacked bar: Direct TCP vs Veilid at N=3, showing routing overhead."""
    # Direct TCP: mascot, N=3
    direct_n3 = filter_rows(direct_data, protocol='mascot', num_parties=3)
    if not direct_n3:
        # Try case-insensitive protocol match
        direct_n3 = [r for r in direct_data
                     if str(r.get('protocol', '')).lower() == 'mascot'
                     and str(r.get('num_parties', '')) == '3']
    veilid_n3 = [r for r in veilid_data if str(r.get('num_parties', '')) == '3']

    if not direct_n3 and not veilid_n3:
        print("  Skipping veilid_overhead.pdf — no N=3 data in either dataset")
        return

    fig, ax = plt.subplots()

    bar_width = 0.45
    positions = []
    tick_labels = []
    bottom_vals = []
    top_vals = []
    bar_colors_bottom = []
    bar_colors_top = []

    if direct_n3:
        wc_vals = numeric_values(direct_n3, 'wall_clock_secs')
        mpc_vals = numeric_values(direct_n3, 'mpc_time_secs')
        if not mpc_vals:
            mpc_vals = wc_vals  # fall back to wall_clock if mpc_time not present
        wc_mean = float(np.mean(wc_vals)) if wc_vals else 0.0
        mpc_mean = float(np.mean(mpc_vals)) if mpc_vals else wc_mean
        comp_time = min(mpc_mean, wc_mean)
        overhead = max(0.0, wc_mean - comp_time)
        positions.append(0)
        tick_labels.append('Direct TCP\n(MASCOT)')
        bottom_vals.append(comp_time)
        top_vals.append(overhead)
        bar_colors_bottom.append(COLORS[0])
        bar_colors_top.append(COLORS[1])

    if veilid_n3:
        total_vals = numeric_values(veilid_n3, 'total_secs')
        self_vals = numeric_values(veilid_n3, 'mpc_self_secs')
        total_mean = float(np.mean(total_vals)) if total_vals else 0.0
        self_mean = float(np.mean(self_vals)) if self_vals else 0.0
        routing_overhead = max(0.0, total_mean - self_mean)
        x_pos = 1 if direct_n3 else 0
        positions.append(x_pos)
        tick_labels.append('Veilid\n(MASCOT)')
        bottom_vals.append(self_mean)
        top_vals.append(routing_overhead)
        bar_colors_bottom.append(COLORS[0])
        bar_colors_top.append(COLORS[2])

    x_arr = np.array(positions, dtype=float)

    # Draw stacked bars
    b1 = ax.bar(x_arr, bottom_vals, bar_width,
                color=bar_colors_bottom, alpha=0.85, label='MPC computation')
    b2 = ax.bar(x_arr, top_vals, bar_width, bottom=bottom_vals,
                color=bar_colors_top, alpha=0.85, label='Routing / tunnel overhead')

    # Annotate overhead ratio if both datasets present
    if direct_n3 and veilid_n3:
        direct_total = bottom_vals[0] + top_vals[0]
        veilid_total = bottom_vals[1] + top_vals[1]
        if direct_total > 0:
            ratio = veilid_total / direct_total
            ax.annotate(
                f'{ratio:.1f}× overhead',
                xy=(positions[1], veilid_total),
                xytext=(0, 8), textcoords='offset points',
                ha='center', va='bottom', fontsize=9,
                color=COLORS[2],
            )

    ax.set_title('Veilid Privacy Overhead (N=3 parties, MASCOT)')
    ax.set_ylabel('Time (s)')
    ax.set_xticks(x_arr)
    ax.set_xticklabels(tick_labels)
    ax.legend(loc='upper left')
    ax.yaxis.set_minor_locator(ticker.AutoMinorLocator())
    plt.tight_layout()
    save_fig(fig, out_dir, 'veilid_overhead.pdf')


# ---------------------------------------------------------------------------
# Plot 5: End-to-End Auction Time vs Party Count
# ---------------------------------------------------------------------------

def plot_veilid_party_scaling(veilid_data: list[dict], out_dir: str) -> None:
    """Line chart: total auction time vs num_parties, grouped by protocol."""
    if not veilid_data:
        print("  Skipping veilid_party_scaling.pdf — no veilid_auction data")
        return

    protocols_raw = sorted({str(r.get('protocol', 'mascot')).lower() for r in veilid_data})
    stats = aggregate(veilid_data, ['protocol', 'num_parties'], 'total_secs')
    all_parties = sorted({int(r['num_parties']) for r in veilid_data
                          if 'num_parties' in r and str(r['num_parties']).isdigit()})

    if not stats or not all_parties:
        print("  Skipping veilid_party_scaling.pdf — no total_secs data")
        return

    fig, ax = plt.subplots()

    for idx, proto in enumerate(protocols_raw):
        color = COLORS[idx % len(COLORS)]
        label = PROTOCOL_LABELS.get(proto, proto.upper())

        xs, ys, errs = [], [], []
        for n in all_parties:
            key = (proto, n)
            if key in stats:
                mean, std, cnt = stats[key]
                xs.append(n)
                ys.append(mean)
                errs.append(std)

        if not xs:
            continue

        xs_arr = np.array(xs)
        ys_arr = np.array(ys)
        errs_arr = np.array(errs)

        if len(xs) == 1:
            ax.errorbar(xs_arr, ys_arr, yerr=errs_arr, fmt='o', color=color,
                        capsize=4, label=label, markersize=7)
        else:
            ax.plot(xs_arr, ys_arr, '-o', color=color, label=label, markersize=5)
            ax.fill_between(xs_arr, ys_arr - errs_arr, ys_arr + errs_arr,
                            alpha=0.15, color=color)

    ax.set_title('End-to-End Auction Time vs Party Count (Veilid)')
    ax.set_xlabel('Number of parties')
    ax.set_ylabel('Total auction time (s)')
    ax.set_xticks(all_parties)
    ax.legend()
    ax.yaxis.set_minor_locator(ticker.AutoMinorLocator())
    plt.tight_layout()
    save_fig(fig, out_dir, 'veilid_party_scaling.pdf')


# ---------------------------------------------------------------------------
# Plot 6: Auction Phase Breakdown
# ---------------------------------------------------------------------------

def plot_phase_breakdown(veilid_data: list[dict], out_dir: str) -> None:
    """Horizontal stacked bar chart: phase times per (protocol, party-count) configuration."""
    if not veilid_data:
        print("  Skipping phase_breakdown.pdf — no veilid_auction data")
        return

    protocols_raw = sorted({str(r.get('protocol', 'mascot')).lower() for r in veilid_data})
    party_counts = sorted({int(r['num_parties']) for r in veilid_data
                           if 'num_parties' in r and str(r['num_parties']).isdigit()})
    if not party_counts:
        print("  Skipping phase_breakdown.pdf — no num_parties column")
        return

    route_stats = aggregate(veilid_data, ['protocol', 'num_parties'], 'route_exchange_secs')
    mpc_wall_stats = aggregate(veilid_data, ['protocol', 'num_parties'], 'mpc_wall_secs')
    mpc_self_stats = aggregate(veilid_data, ['protocol', 'num_parties'], 'mpc_self_secs')
    total_stats = aggregate(veilid_data, ['protocol', 'num_parties'], 'total_secs')

    # Build labels and data for each (protocol, party_count) pair
    labels = []
    route_means, mpc_self_means, tunnel_means, other_means = [], [], [], []

    for proto in protocols_raw:
        proto_label = PROTOCOL_LABELS.get(proto, proto.upper())
        for n in party_counts:
            key = (proto, n)
            if key not in total_stats:
                continue

            labels.append(f'{proto_label}\nN={n}')
            route_m = route_stats.get(key, (0.0, 0.0, 0))[0]
            mpc_wall_m = mpc_wall_stats.get(key, (0.0, 0.0, 0))[0]
            mpc_self_m = mpc_self_stats.get(key, (0.0, 0.0, 0))[0]
            total_m = total_stats.get(key, (0.0, 0.0, 0))[0]

            tunnel_m = max(0.0, mpc_wall_m - mpc_self_m)
            accounted = route_m + mpc_self_m + tunnel_m
            other_m = max(0.0, total_m - accounted)

            route_means.append(route_m)
            mpc_self_means.append(mpc_self_m)
            tunnel_means.append(tunnel_m)
            other_means.append(other_m)

    if not labels:
        print("  Skipping phase_breakdown.pdf — no matching data")
        return

    y = np.arange(len(labels))
    height = 0.5

    fig, ax = plt.subplots(figsize=(7, max(3.0, len(labels) * 0.9 + 1.5)))

    ax.barh(y, route_means, height, color=COLORS[0], alpha=0.85, label='Route exchange')
    ax.barh(y, mpc_self_means, height, left=route_means,
            color=COLORS[1], alpha=0.85, label='MPC self-time')
    left2 = np.array(route_means) + np.array(mpc_self_means)
    ax.barh(y, tunnel_means, height, left=left2,
            color=COLORS[2], alpha=0.85, label='Tunnel overhead')
    left3 = left2 + np.array(tunnel_means)
    ax.barh(y, other_means, height, left=left3,
            color=COLORS[3], alpha=0.85, label='Other')

    ax.set_title('Auction Phase Breakdown (Veilid)')
    ax.set_xlabel('Time (s)')
    ax.set_yticks(y)
    ax.set_yticklabels(labels)
    ax.xaxis.set_minor_locator(ticker.AutoMinorLocator())
    ax.legend(loc='lower right')
    plt.tight_layout()
    save_fig(fig, out_dir, 'phase_breakdown.pdf')


# ---------------------------------------------------------------------------
# Plot 7: Devnet Size Scaling
# ---------------------------------------------------------------------------

def plot_devnet_scaling(veilid_data: list[dict], out_dir: str) -> None:
    """Line chart: auction time vs devnet size (network density), fixed party count."""
    if not veilid_data:
        print("  Skipping devnet_scaling.pdf — no veilid_auction data")
        return

    devnet_sizes = sorted({int(r['devnet_nodes']) for r in veilid_data
                           if 'devnet_nodes' in r and str(r['devnet_nodes']).isdigit()})
    if len(devnet_sizes) < 2:
        print("  Skipping devnet_scaling.pdf — need multiple devnet sizes")
        return

    # Group by (devnet_nodes, num_parties) — show one line per party count
    party_counts = sorted({int(r['num_parties']) for r in veilid_data
                           if 'num_parties' in r and str(r['num_parties']).isdigit()})

    stats = aggregate(veilid_data, ['devnet_nodes', 'num_parties'], 'total_secs')

    fig, ax = plt.subplots()

    for idx, n in enumerate(party_counts):
        color = COLORS[idx % len(COLORS)]

        xs, ys, errs = [], [], []
        for ds in devnet_sizes:
            key = (ds, n)
            if key in stats:
                mean, std, cnt = stats[key]
                xs.append(ds)
                ys.append(mean)
                errs.append(std)

        if len(xs) < 2:
            continue

        xs_arr = np.array(xs)
        ys_arr = np.array(ys)
        errs_arr = np.array(errs)

        ax.plot(xs_arr, ys_arr, '-o', color=color, label=f'N={n} parties', markersize=5)
        ax.fill_between(xs_arr, ys_arr - errs_arr, ys_arr + errs_arr,
                        alpha=0.15, color=color)

    ax.set_title('Auction Time vs Devnet Size (Network Density)')
    ax.set_xlabel('Number of devnet nodes')
    ax.set_ylabel('Total auction time (s)')
    ax.set_xticks(devnet_sizes)
    ax.legend()
    ax.yaxis.set_minor_locator(ticker.AutoMinorLocator())
    plt.tight_layout()
    save_fig(fig, out_dir, 'devnet_scaling.pdf')


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description='Generate benchmark plots for the dissertation')
    parser.add_argument('--results-dir', default='bench-results',
                        help='Directory containing CSV files (default: bench-results)')
    parser.add_argument('--output-dir', default='bench-results/plots',
                        help='Output directory for PDF plots (default: bench-results/plots)')
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    direct_path = Path(args.results_dir) / 'direct_mpc.csv'
    veilid_path = Path(args.results_dir) / 'veilid_auction.csv'

    print(f"Loading data from {args.results_dir}/")
    direct_data = load_csv(direct_path)
    veilid_data = load_csv(veilid_path)

    if not direct_data:
        print(f"  direct_mpc.csv not found or empty — skipping direct-MPC plots")
    else:
        print(f"  Loaded {len(direct_data)} rows from direct_mpc.csv")

    if not veilid_data:
        print(f"  veilid_auction.csv not found or empty — skipping Veilid plots")
    else:
        print(f"  Loaded {len(veilid_data)} rows from veilid_auction.csv")

    print(f"\nGenerating plots into {args.output_dir}/")

    if direct_data:
        plot_protocol_comparison(direct_data, args.output_dir)
        plot_party_scaling_time(direct_data, args.output_dir)
        plot_party_scaling_comm(direct_data, args.output_dir)

    if direct_data and veilid_data:
        plot_veilid_overhead(direct_data, veilid_data, args.output_dir)

    if veilid_data:
        plot_veilid_party_scaling(veilid_data, args.output_dir)
        plot_phase_breakdown(veilid_data, args.output_dir)
        plot_devnet_scaling(veilid_data, args.output_dir)

    print("\nDone.")


if __name__ == '__main__':
    main()
