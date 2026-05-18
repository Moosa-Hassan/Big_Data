"""Dependency-free HTML/SVG visualizations for query-evaluation results.

The notebook is the main journal artifact, but this module gives the project a
repeatable command that can render key figures on machines without Jupyter or
Matplotlib installed.
"""

from __future__ import annotations

import argparse
import csv
import html
import json
from pathlib import Path
from typing import Any

MODE_ORDER = [
    "decompressed_text",
    "full_decompression",
    "minor_optimization",
    "static_bloom",
    "static_qgram_index",
    "static_qgram_index_mmap",
    "grep_plaintext",
    "ripgrep_plaintext",
    "static_qgram_index_mmap_compact",
    "static_qgram_index_mmap_cpp",
]
MODE_LABELS = {
    "decompressed_text": "Decompressed Text",
    "full_decompression": "Full Decompression",
    "minor_optimization": "Minor Optimization",
    "static_bloom": "Static Bloom",
    "static_qgram_index": "Static Q-Gram JSON",
    "static_qgram_index_mmap": "Static Q-Gram mmap",
    "grep_plaintext": "grep Plaintext",
    "ripgrep_plaintext": "ripgrep Plaintext",
    "static_qgram_index_mmap_compact": "Static Q-Gram qidx3",
    "static_qgram_index_mmap_cpp": "Static Q-Gram qidx3 C++",
}
MODE_COLORS = {
    "decompressed_text": "#4C78A8",
    "full_decompression": "#F58518",
    "minor_optimization": "#54A24B",
    "static_bloom": "#B279A2",
    "static_qgram_index": "#E45756",
    "static_qgram_index_mmap": "#72B7B2",
    "grep_plaintext": "#9D755D",
    "ripgrep_plaintext": "#BAB0AC",
    "static_qgram_index_mmap_compact": "#EECA3B",
    "static_qgram_index_mmap_cpp": "#FF9DA6",
}
QUERY_LABELS = {
    "common_token": "Common Token",
    "medium_token": "Medium Token",
    "rare_token": "Rare Token",
    "common_phrase": "Common Phrase",
    "selective_phrase": "Selective Phrase",
    "numeric_identifier": "Numeric ID",
    "conjunctive": "Conjunctive",
    "bloom_stress_substring": "Stress Substring",
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m query_eval.visualize_results")
    parser.add_argument("--results-directory", required=True)
    parser.add_argument("--title", default="Query Evaluation")
    args = parser.parse_args(argv)

    results_directory = Path(args.results_directory)
    outputs = build_visualization_report(results_directory, title=args.title)
    print(json.dumps({key: str(path) for key, path in outputs.items()}, sort_keys=True))
    return 0


def build_visualization_report(results_directory: Path, title: str) -> dict[str, Path]:
    if not results_directory.exists():
        raise FileNotFoundError(f"Results directory not found: {results_directory}")

    manifest = json.loads((results_directory / "manifest.json").read_text(encoding="utf-8"))
    suite_rows = _read_csv(results_directory / "suite_summary.csv")
    cell_rows = _read_csv(results_directory / "cell_level_aggregate.csv")
    static_rows = _read_csv(results_directory / "static_bloom_summary.csv")
    complete_summary_rows = _read_csv(results_directory / "complete_evaluation_summary.csv")

    figure_directory = results_directory / "journal_figures"
    figure_directory.mkdir(parents=True, exist_ok=True)

    figures: dict[str, Path] = {}
    figures["suite_wall_time"] = _write(
        figure_directory / "suite_wall_time.svg",
        _bar_chart(
            title="Median Wall Time by Mode",
            rows=[
                {
                    "label": _mode_label(row["mode_name"]),
                    "value": _to_float(row["median_wall_time_ms"]),
                    "color": MODE_COLORS.get(row["mode_name"], "#777777"),
                }
                for row in _ordered_mode_rows(suite_rows)
            ],
            value_label="ms",
        ),
    )
    figures["speedup_vs_full"] = _write(
        figure_directory / "speedup_vs_full.svg",
        _bar_chart(
            title="Wall-Time Speedup vs Full Decompression",
            rows=_suite_speedup_rows(suite_rows),
            value_label="x",
            baseline=1.0,
        ),
    )
    figures["exact_cells"] = _write(
        figure_directory / "exact_cells.svg",
        _bar_chart(
            title="Exact Cells by Mode",
            rows=_exact_cell_rows(cell_rows),
            value_label="cells",
        ),
    )
    if complete_summary_rows:
        for query_group in ("all_queries", "token_safe_queries", "stress_queries"):
            group_rows = [row for row in complete_summary_rows if row.get("query_group") == query_group]
            if not group_rows:
                continue
            figures[f"{query_group}_wall_time"] = _write(
                figure_directory / f"{query_group}_wall_time.svg",
                _bar_chart(
                    title=f"Median Wall Time: {query_group.replace('_', ' ').title()}",
                    rows=[
                        {
                            "label": _mode_label(row["mode_name"]),
                            "value": _to_float(row["median_wall_time_ms"]),
                            "color": MODE_COLORS.get(row["mode_name"], "#777777"),
                        }
                        for row in _ordered_mode_rows(group_rows)
                    ],
                    value_label="ms",
                ),
            )

    minor_rows = [row for row in cell_rows if row["mode_name"] == "minor_optimization"]
    if minor_rows:
        figures["minor_recall_heatmap"] = _write(
            figure_directory / "minor_recall_heatmap.svg",
            _heatmap("Minor Optimization Recall", minor_rows, "median_recall"),
        )
        figures["minor_speedup_recall"] = _write(
            figure_directory / "minor_speedup_recall.svg",
            _scatter_plot(
                title="Minor Optimization: Speedup vs Recall",
                rows=_minor_tradeoff_rows(cell_rows),
                x_field="speedup",
                y_field="recall",
                x_label="Speedup vs Full Decompression (x)",
                y_label="Recall",
                color=MODE_COLORS["minor_optimization"],
            ),
        )

    if static_rows:
        figures["static_skip_rate"] = _write(
            figure_directory / "static_skip_rate.svg",
            _heatmap(
                "Static Bloom Skip Rate",
                [
                    {
                        "dataset_slug": row["dataset_slug"],
                        "query_id": row["query_id"],
                        "median_bloom_skip_rate": row["bloom_skip_rate"],
                    }
                    for row in static_rows
                ],
                "median_bloom_skip_rate",
            ),
        )
        figures["static_speedup"] = _write(
            figure_directory / "static_speedup.svg",
            _heatmap(
                "Static Bloom Speedup vs Full Decompression",
                [
                    {
                        "dataset_slug": row["dataset_slug"],
                        "query_id": row["query_id"],
                        "speedup": row["speedup_vs_full_decompression"],
                    }
                    for row in static_rows
                ],
                "speedup",
                high_is_green=True,
            ),
        )

    report_path = results_directory / "complete_16_dataset_visualization_report.html"
    _write(report_path, _html_report(title, manifest, suite_rows, cell_rows, static_rows, figures, results_directory))
    return {"report": report_path, **figures}


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write(path: Path, content: str) -> Path:
    path.write_text(content, encoding="utf-8")
    return path


def _ordered_mode_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    rank = {mode: index for index, mode in enumerate(MODE_ORDER)}
    return sorted(rows, key=lambda row: rank.get(row.get("mode_name", ""), 999))


def _mode_label(mode_name: str) -> str:
    return MODE_LABELS.get(mode_name, mode_name)


def _query_label(query_id: str) -> str:
    return QUERY_LABELS.get(query_id, query_id)


def _to_float(raw_value: Any, default: float = 0.0) -> float:
    try:
        if raw_value in ("", None):
            return default
        return float(raw_value)
    except (TypeError, ValueError):
        return default


def _to_bool(raw_value: Any) -> bool:
    return str(raw_value).strip().lower() == "true"


def _suite_speedup_rows(suite_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    full_row = next((row for row in suite_rows if row["mode_name"] == "full_decompression"), None)
    full_wall = _to_float(full_row["median_wall_time_ms"]) if full_row else 0.0
    rows = []
    for row in _ordered_mode_rows(suite_rows):
        wall = _to_float(row["median_wall_time_ms"])
        rows.append(
            {
                "label": _mode_label(row["mode_name"]),
                "value": full_wall / wall if wall else 0.0,
                "color": MODE_COLORS.get(row["mode_name"], "#777777"),
            }
        )
    return rows


def _exact_cell_rows(cell_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    by_mode: dict[str, dict[str, int]] = {}
    for row in cell_rows:
        mode = row["mode_name"]
        by_mode.setdefault(mode, {"cells": 0, "exact": 0})
        by_mode[mode]["cells"] += 1
        by_mode[mode]["exact"] += int(_to_bool(row["all_runs_exact_set_match"]))
    rows = []
    for mode in MODE_ORDER:
        if mode not in by_mode:
            continue
        rows.append(
            {
                "label": f"{_mode_label(mode)} ({by_mode[mode]['exact']}/{by_mode[mode]['cells']})",
                "value": by_mode[mode]["exact"],
                "color": MODE_COLORS.get(mode, "#777777"),
            }
        )
    return rows


def _minor_tradeoff_rows(cell_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    full_by_key = {
        (row["dataset_slug"], row["query_id"]): row
        for row in cell_rows
        if row["mode_name"] == "full_decompression"
    }
    rows = []
    for row in cell_rows:
        if row["mode_name"] != "minor_optimization":
            continue
        full = full_by_key.get((row["dataset_slug"], row["query_id"]))
        full_wall = _to_float(full["median_wall_time_ms"]) if full else 0.0
        minor_wall = _to_float(row["median_wall_time_ms"])
        rows.append(
            {
                "label": f"{row['dataset_slug']} / {_query_label(row['query_id'])}",
                "speedup": full_wall / minor_wall if minor_wall else 0.0,
                "recall": _to_float(row["median_recall"]),
            }
        )
    return rows


def _bar_chart(
    title: str,
    rows: list[dict[str, Any]],
    value_label: str,
    width: int = 940,
    height: int = 430,
    baseline: float | None = None,
) -> str:
    margin_left = 210
    margin_right = 42
    margin_top = 52
    margin_bottom = 38
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom
    max_value = max([row["value"] for row in rows] + ([baseline] if baseline else [0.0]) + [1.0]) * 1.12
    bar_gap = 10
    bar_height = max(16, min(38, (plot_height - bar_gap * max(len(rows) - 1, 0)) / max(len(rows), 1)))

    parts = [_svg_open(width, height), _svg_title(title, width)]
    axis_x = margin_left
    axis_y = margin_top + plot_height
    parts.append(f'<line x1="{axis_x}" y1="{margin_top}" x2="{axis_x}" y2="{axis_y}" stroke="#333" />')
    parts.append(f'<line x1="{axis_x}" y1="{axis_y}" x2="{width - margin_right}" y2="{axis_y}" stroke="#333" />')
    if baseline is not None:
        x = margin_left + (baseline / max_value) * plot_width
        parts.append(f'<line x1="{x:.1f}" y1="{margin_top}" x2="{x:.1f}" y2="{axis_y}" stroke="#222" stroke-dasharray="4 4" />')
        parts.append(f'<text x="{x + 4:.1f}" y="{margin_top + 14}" font-size="11" fill="#222">1.0x</text>')

    for index, row in enumerate(rows):
        y = margin_top + index * (bar_height + bar_gap)
        bar_width = (row["value"] / max_value) * plot_width if max_value else 0
        parts.append(
            f'<text x="{margin_left - 8}" y="{y + bar_height * 0.68:.1f}" text-anchor="end" '
            f'font-size="12">{html.escape(str(row["label"]))}</text>'
        )
        parts.append(
            f'<rect x="{margin_left}" y="{y:.1f}" width="{bar_width:.1f}" height="{bar_height:.1f}" '
            f'fill="{row.get("color", "#777777")}" rx="3" />'
        )
        parts.append(
            f'<text x="{margin_left + bar_width + 6:.1f}" y="{y + bar_height * 0.68:.1f}" '
            f'font-size="12">{row["value"]:.2f} {html.escape(value_label)}</text>'
        )
    parts.append("</svg>")
    return "\n".join(parts)


def _heatmap(
    title: str,
    rows: list[dict[str, str]],
    value_field: str,
    high_is_green: bool = False,
) -> str:
    datasets = sorted({row["dataset_slug"] for row in rows})
    queries = sorted({row["query_id"] for row in rows})
    cell_size = 66
    left = 128
    top = 94
    width = left + len(queries) * cell_size + 36
    height = top + len(datasets) * cell_size + 48
    value_by_key = {
        (row["dataset_slug"], row["query_id"]): _to_float(row[value_field])
        for row in rows
    }
    max_value = max(value_by_key.values() or [1.0])
    parts = [_svg_open(width, height), _svg_title(title, width)]
    for index, query_id in enumerate(queries):
        x = left + index * cell_size + cell_size / 2
        parts.append(
            f'<text x="{x:.1f}" y="74" font-size="10" text-anchor="middle" '
            f'transform="rotate(-30 {x:.1f} 74)">{html.escape(_query_label(query_id))}</text>'
        )
    for row_index, dataset in enumerate(datasets):
        y = top + row_index * cell_size
        parts.append(f'<text x="{left - 10}" y="{y + 38}" font-size="11" text-anchor="end">{html.escape(dataset)}</text>')
        for query_index, query_id in enumerate(queries):
            x = left + query_index * cell_size
            value = value_by_key.get((dataset, query_id), 0.0)
            normalized = value / max_value if high_is_green and max_value else value
            color = _green_red(normalized)
            parts.append(f'<rect x="{x}" y="{y}" width="{cell_size - 3}" height="{cell_size - 3}" fill="{color}" rx="4" />')
            parts.append(
                f'<text x="{x + cell_size / 2:.1f}" y="{y + 37}" text-anchor="middle" '
                f'font-size="11" fill="#111">{value:.2f}</text>'
            )
    parts.append("</svg>")
    return "\n".join(parts)


def _scatter_plot(
    title: str,
    rows: list[dict[str, Any]],
    x_field: str,
    y_field: str,
    x_label: str,
    y_label: str,
    color: str,
) -> str:
    width = 960
    height = 540
    left = 80
    right = 240
    top = 54
    bottom = 64
    plot_width = width - left - right
    plot_height = height - top - bottom
    max_x = max([_to_float(row[x_field]) for row in rows] + [1.0]) * 1.12
    min_y = 0.0
    max_y = 1.05
    parts = [_svg_open(width, height), _svg_title(title, width)]
    parts.append(f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_height}" stroke="#333" />')
    parts.append(f'<line x1="{left}" y1="{top + plot_height}" x2="{left + plot_width}" y2="{top + plot_height}" stroke="#333" />')
    baseline_x = left + (1.0 / max_x) * plot_width
    parts.append(f'<line x1="{baseline_x:.1f}" y1="{top}" x2="{baseline_x:.1f}" y2="{top + plot_height}" stroke="#222" stroke-dasharray="4 4" />')
    parts.append(f'<line x1="{left}" y1="{top}" x2="{left + plot_width}" y2="{top}" stroke="#222" stroke-dasharray="4 4" />')
    for row in rows:
        x_value = _to_float(row[x_field])
        y_value = _to_float(row[y_field])
        x = left + (x_value / max_x) * plot_width
        y = top + plot_height - ((y_value - min_y) / (max_y - min_y)) * plot_height
        parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5" fill="{color}" opacity="0.86" />')
        if y_value < 0.95 or x_value > 1.6:
            parts.append(f'<text x="{x + 7:.1f}" y="{y + 4:.1f}" font-size="9">{html.escape(row["label"])}</text>')
    parts.append(f'<text x="{left + plot_width / 2:.1f}" y="{height - 18}" text-anchor="middle" font-size="13">{html.escape(x_label)}</text>')
    parts.append(f'<text x="18" y="{top + plot_height / 2:.1f}" text-anchor="middle" font-size="13" transform="rotate(-90 18 {top + plot_height / 2:.1f})">{html.escape(y_label)}</text>')
    parts.append("</svg>")
    return "\n".join(parts)


def _green_red(value: float) -> str:
    value = max(0.0, min(1.0, value))
    red = int(226 - 118 * value)
    green = int(92 + 95 * value)
    blue = int(87 + 44 * value)
    return f"rgb({red},{green},{blue})"


def _svg_open(width: int, height: int) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img">'
        '<rect width="100%" height="100%" fill="#ffffff" />'
    )


def _svg_title(title: str, width: int) -> str:
    return f'<text x="{width / 2:.1f}" y="28" text-anchor="middle" font-size="18" font-weight="700">{html.escape(title)}</text>'


def _html_report(
    title: str,
    manifest: dict[str, Any],
    suite_rows: list[dict[str, str]],
    cell_rows: list[dict[str, str]],
    static_rows: list[dict[str, str]],
    figures: dict[str, Path],
    results_directory: Path,
) -> str:
    raw_runs_path = results_directory / "raw_runs.jsonl"
    raw_run_count = sum(1 for _ in raw_runs_path.open("r", encoding="utf-8")) if raw_runs_path.exists() else 0
    suite_table = _table(
        [
            {
                "Mode": _mode_label(row["mode_name"]),
                "Cells": row["cell_count"],
                "Median Wall (ms)": f'{_to_float(row["median_wall_time_ms"]):.3f}',
                "Median CPU (ms)": f'{_to_float(row["median_cpu_time_ms"]):.3f}',
                "Median RSS (MB)": f'{_to_float(row["median_peak_rss_mb"]):.2f}',
                "Median Recall": f'{_to_float(row["median_recall"]):.3f}',
                "Median F1": f'{_to_float(row["median_f1"]):.3f}',
                "All Exact": row["all_cells_exact_set_match"],
            }
            for row in _ordered_mode_rows(suite_rows)
        ]
    )
    exact_table = _table(_mode_exact_summary(cell_rows))
    static_table = _table(_static_group_summary(static_rows))
    figure_markup = "\n".join(
        f'<section><h2>{html.escape(path.stem.replace("_", " ").title())}</h2>'
        f'<img src="journal_figures/{html.escape(path.name)}" alt="{html.escape(path.stem)}" /></section>'
        for path in figures.values()
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>{html.escape(title)}</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 32px; color: #1f2933; }}
    h1 {{ margin-bottom: 0.2rem; }}
    .meta {{ color: #52606d; margin-top: 0; }}
    section {{ margin: 32px 0; }}
    img {{ max-width: 100%; border: 1px solid #d9e2ec; border-radius: 8px; background: white; }}
    table {{ border-collapse: collapse; width: 100%; margin-top: 12px; font-size: 14px; }}
    th, td {{ border: 1px solid #d9e2ec; padding: 8px 10px; text-align: left; }}
    th {{ background: #f0f4f8; }}
    code {{ background: #f0f4f8; padding: 2px 5px; border-radius: 4px; }}
  </style>
</head>
<body>
  <h1>{html.escape(title)}</h1>
  <p class="meta">Created from <code>{html.escape(str(results_directory))}</code></p>
  <section>
    <h2>Run Metadata</h2>
    <p>Created at: <strong>{html.escape(str(manifest.get("created_at", "")))}</strong></p>
    <p>Datasets: {html.escape(", ".join(manifest.get("datasets", [])))}</p>
    <p>Queries: {html.escape(", ".join(manifest.get("queries", [])))}</p>
    <p>Modes: {html.escape(", ".join(manifest.get("modes", [])))}</p>
    <p>Raw run records: <strong>{raw_run_count}</strong>; cell rows: <strong>{len(cell_rows)}</strong>.</p>
  </section>
  <section><h2>Suite Summary</h2>{suite_table}</section>
  <section><h2>Correctness Summary</h2>{exact_table}</section>
  <section><h2>Static Bloom Group Summary</h2>{static_table}</section>
  {figure_markup}
</body>
</html>
"""


def _mode_exact_summary(cell_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    grouped: dict[str, dict[str, float]] = {}
    for row in cell_rows:
        mode = row["mode_name"]
        grouped.setdefault(mode, {"cells": 0, "exact": 0, "fn": 0.0, "fp": 0.0})
        grouped[mode]["cells"] += 1
        grouped[mode]["exact"] += int(_to_bool(row["all_runs_exact_set_match"]))
        grouped[mode]["fn"] += _to_float(row["median_fn"])
        grouped[mode]["fp"] += _to_float(row["median_fp"])
    rows = []
    for mode in MODE_ORDER:
        if mode not in grouped:
            continue
        data = grouped[mode]
        rows.append(
            {
                "Mode": _mode_label(mode),
                "Exact Cells": f'{int(data["exact"])}/{int(data["cells"])}',
                "Median FN Sum": f'{data["fn"]:.0f}',
                "Median FP Sum": f'{data["fp"]:.0f}',
            }
        )
    return rows


def _static_group_summary(static_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    grouped: dict[str, dict[str, float]] = {}
    for row in static_rows:
        group = "stress" if _to_bool(row.get("is_stress_query")) else "token_safe"
        grouped.setdefault(group, {"cells": 0, "exact": 0, "fp": 0.0, "fn": 0.0, "skip_rates": [], "speedups": []})
        grouped[group]["cells"] += 1
        grouped[group]["exact"] += int(_to_bool(row["exact_set_match"]))
        grouped[group]["fp"] += _to_float(row["false_positives"])
        grouped[group]["fn"] += _to_float(row["false_negatives"])
        grouped[group]["skip_rates"].append(_to_float(row["bloom_skip_rate"]))
        grouped[group]["speedups"].append(_to_float(row["speedup_vs_full_decompression"]))
    rows = []
    for group in ("token_safe", "stress"):
        if group not in grouped:
            continue
        data = grouped[group]
        rows.append(
            {
                "Query Group": group,
                "Exact Cells": f'{int(data["exact"])}/{int(data["cells"])}',
                "False Positives": f'{data["fp"]:.0f}',
                "False Negatives": f'{data["fn"]:.0f}',
                "Median Skip Rate": f'{_median(data["skip_rates"]):.2%}',
                "Median Speedup": f'{_median(data["speedups"]):.2f}x',
            }
        )
    return rows


def _median(values: list[float]) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2


def _table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "<p>No rows.</p>"
    headers = list(rows[0].keys())
    header_html = "".join(f"<th>{html.escape(str(header))}</th>" for header in headers)
    body = []
    for row in rows:
        body.append("<tr>" + "".join(f"<td>{html.escape(str(row.get(header, '')))}</td>" for header in headers) + "</tr>")
    return "<table><thead><tr>" + header_html + "</tr></thead><tbody>" + "".join(body) + "</tbody></table>"


if __name__ == "__main__":
    raise SystemExit(main())
