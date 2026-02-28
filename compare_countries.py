import json
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_theme(style="whitegrid")
plt.rcParams.update({
    "figure.dpi": 150,
    "axes.titlesize": 11,
    "axes.labelsize": 10,
    "xtick.labelsize": 9,
    "ytick.labelsize": 9, 
    "legend.fontsize": 9,
    "savefig.bbox": "tight"
})

PLOT_DIR = Path("plots/countries")
PLOT_DIR.mkdir(parents=True, exist_ok=True)

def load_scorecards(base_dir="out") -> pd.DataFrame:
    rows = []
    for path in Path(base_dir).rglob("rules_scorecard.json"):
        try:
            d = json.loads(path.read_text())
            d["country"] = path.parent.name
            rows.append(d)
        except Exception:
            continue
    return pd.DataFrame(rows)

def plot_metric(df, metric, title, ylabel, filename):
    if metric not in df.columns:
        print(f"[INFO] Skipping metric {metric} (missing)")
        return

    data = df[["country", metric]].dropna().copy()
    if data.empty:
        print(f"[INFO] No data for metric {metric}")
        return

    data = data.sort_values(metric, ascending=False)

    plt.figure(figsize=(8, 4))
    sns.barplot(data=data, x="country", y=metric, color="steelblue")

    plt.title(title)
    plt.ylabel(ylabel)
    plt.xlabel("")
    plt.xticks(rotation=45, ha="right")

    if metric.endswith("%"):
        plt.ylim(0, 105)
        plt.axhline(100, color="grey", linestyle="--", linewidth=1)

    plt.tight_layout()
    plt.savefig(PLOT_DIR / filename, dpi=200)
    plt.close()

def plot_heatmap(df):
    """
    Create a red–green quality-score heatmap.
    All values are transformed so that higher = better (0–100 %).
    """

    metric_config = {
        "R4_osm_within_threshold_%": ("OSM aligned within threshold", "positive"),
        "R5_osm_type_compatible_%": ("OSM type compatibility", "positive"),
        "R7_recency_ops_%": ("Recency OPs", "positive"),
        "R7_recency_sols_%": ("Recency SoLs", "positive"),
        "R8_osm_only_share_%": ("OSM-only coverage gap", "negative")
    }

    needed_cols = list(metric_config.keys())
    sub_df = df[["country"] + needed_cols].copy()
    sub_df = sub_df.dropna(how="all", subset=needed_cols)

    if sub_df.empty:
        print("[WARN] Heatmap: no usable data.")
        return

    sub_df = sub_df.set_index("country")

    qual = pd.DataFrame(index=sub_df.index)

    for col, (label, polarity) in metric_config.items():
        if col not in sub_df.columns:
            continue

        vals = pd.to_numeric(sub_df[col], errors="coerce")

        if polarity == "positive":
            q = vals.clip(0, 100)
        else:
            q = (100 - vals).clip(0, 100)

        qual[f"{label} (quality %)"] = q

    qual = qual.dropna(how="all")
    if qual.empty:
        print("[WARN] Heatmap: quality table is empty.")
        return

    qual = qual.reindex(qual.mean(axis=1).sort_values(ascending=False).index)

    height = 0.4 * len(qual.index) + 2
    plt.figure(figsize=(10, height))

    sns.heatmap(
        qual,
        annot=True,
        fmt=".1f",
        cmap="RdYlGn",
        vmin=0,
        vmax=100,
        linewidths=0.4,
        linecolor="white",
        cbar_kws={"label": "Quality score (%)"}
    )

    plt.title("Cross-country quality score overview", fontsize=13, pad=10)
    plt.ylabel("Country")
    plt.xlabel("Metric")

    plt.tight_layout()
    plt.savefig(PLOT_DIR / "quality_heatmap.png", dpi=200)
    plt.close()

    print("[OK] Saved: quality_heatmap.png")

def main():
    df = load_scorecards()
    if df.empty:
        print("[WARN] No scorecards found.")
        return

    metrics = {
        "R1_sols_endpoints_exist_%": "SoL endpoints exist (%)",
        "R2_min_distance_violations": "Min-distance violations (count)",
        "R3_bbox_violations": "OPs outside national boundaries (count)",
        "R4_osm_within_threshold_%": "OSM match ≤100 m (%)",
        "R5_osm_type_compatible_%": "OSM type compatibility (%)",
        "R6_missing_reverse_edges_%": "Missing reverse edges (%)",
        "R7_recency_ops_%": "Recency OPs (%)",
        "R7_recency_sols_%": "Recency SoLs (%)",
        "R8_osm_only_share_%": "OSM-only share (%)"
    }

    for metric, title in metrics.items():
        ylabel = "%" if metric.endswith("%") else "count"
        filename = f"{metric.replace('%','').replace(' ','_').lower()}.png"
        plot_metric(df, metric, title, ylabel, filename)
        print(f"[OK] Saved: {filename}")
    
    plot_heatmap(df)

    print(f"[DONE] All plots in {PLOT_DIR.resolve()}")

if __name__ == "__main__":
    main()