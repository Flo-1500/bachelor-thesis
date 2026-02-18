import json
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
import seaborn as sns
import geopandas as gpd
from shapely.geometry import Point
from typing import Tuple, Dict

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

# =======================================================================================
#                          ISO Mapping (correct EU codes)
# =======================================================================================

COUNTRY_TO_ISO2 = {
    "austria": "AT", "germany": "DE", "france": "FR", "italy": "IT",
    "switzerland": "CH", "slovakia": "SK", "slovenia": "SI",
    "hungary": "HU", "czechia": "CZ", "belgium": "BE", "poland": "PL",
    "romania": "RO", "spain": "ES", "sweden": "SE", "finland": "FI",
    "norway": "NO", "denmark": "DK", "luxembourg": "LU",
    "netherlands": "NL", "bulgaria": "BG", "croatia": "HR",
    "greece": "EL", "latvia": "LV", "lithuania": "LT", "estonia": "EE",
    "ireland": "IE", "portugal": "PT"
}

# =======================================================================================
#                               Helper function
# =======================================================================================

def safe_read_csv(path: Path) -> pd.DataFrame:
    """Safely read CSV, returning empty DataFrame if unreadable or empty."""
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
        return df if not df.empty else pd.DataFrame()
    except Exception:
        return pd.DataFrame()
    
def distance_to_border_stats(pts_wgs84: gpd.GeoDataFrame, country_poly_wg84: gpd.GeoDataFrame, metric_epsg: int = 3035,) -> Tuple[pd.Series, Dict[str, float]]:
    """Compute point-to-border distances (in meters) for points outside a country's boundary."""
    pts = pts_wgs84.copy()
    poly = country_poly_wg84.copy()

    # Project to a metric CRS (meters)
    pts_m = pts.to_crs(epsg=metric_epsg)
    poly_m = poly.to_crs(epsg=metric_epsg)

    # Union country geometry (handles MultiPolygons)
    geom = poly_m.geometry.union_all()
    border = geom.boundary

    d = pts_m.geometry.distance(border)
    d = pd.to_numeric(d, errors="coerce")

    stats = {
        "min_m": float(d.min()) if len(d) else float("nan"),
        "mean_m": float(d.mean()) if len(d) else float("nan"),
        "max_m": float(d.max()) if len(d) else float("nan"),
        "n": int(len(d)),
        "metric_epsg": int(metric_epsg),
    }
    return d, stats

# =======================================================================================
#                               Visualization per country
# =======================================================================================

def visualize_country(country: str) -> None:
    base = Path("out") / country
    plot_dir = Path("plots") / country
    plot_dir.mkdir(parents=True, exist_ok=True)

    print(f"[INFO] Visualizing {country}...")

    # -------------------------------------------------------------------
    # 1. Minimum distance histogram
    # -------------------------------------------------------------------
    df = safe_read_csv(base / "ops_min_distance_pairs.csv")
    if not df.empty and "distance_m" in df.columns:
        max_show = 500  # Fokus auf den kritischen Bereich 0–500 m

        plt.figure(figsize=(7, 4))
        sns.histplot(
            df[df["distance_m"] <= max_show],
            x="distance_m",
            bins=25,
            color="steelblue"
        )
        # Schwellwert für „zu nah“ visualisieren
        plt.axvline(50, color="red", linestyle="--", linewidth=1)

        plt.title(f"Distances between neighbouring OPs ({country})")
        plt.xlabel("Distance between OPs (m)")
        plt.ylabel("Number of OP pairs")
        plt.xlim(0, max_show)

        plt.tight_layout()
        plt.savefig(plot_dir / "min_distance_distribution.png", dpi=200)
        plt.close()

    # -------------------------------------------------------------------
    # 2. Map of boundary violations
    # -------------------------------------------------------------------
    shp = Path("shapes/CNTR_RG_10M_2024_4326.shp")
    df_viol = safe_read_csv(base / "ops_bbox_violations.csv")
    iso2 = COUNTRY_TO_ISO2.get(country.lower())

    if shp.exists() and not df_viol.empty and iso2:
        try:
            eu = gpd.read_file(shp).to_crs(4326)
            poly = eu[eu["CNTR_ID"] == iso2]
            if not poly.empty:
                gdf = gpd.GeoDataFrame(
                    df_viol,
                    geometry=gpd.points_from_xy(df_viol["lon"], df_viol["lat"]),
                    crs=4326
                )

                n = len(gdf)

                try:
                    dist_m, stats = distance_to_border_stats(gdf, poly, metric_epsg=3035)
                    gdf["distance_to_border_m"] = dist_m.round(2)

                    # Persist detailed distances for later analysis / appendix tables
                    out_csv = base / "ops_bbox_violations_with_border_distance.csv"
                    gdf.drop(columns=["geometry"]).to_csv(out_csv, index=False)

                except Exception as _ex:
                    dist_m, stats = None, None

                fig, ax = plt.subplots(figsize=(7, 6))
                eu.plot(ax=ax, color="lightgrey", edgecolor="white", linewidth=0.3)
                poly.plot(ax=ax, color="none", edgecolor="black", linewidth=1)
                gdf.plot(ax=ax, color="red", markersize=20, alpha=0.8)

                bounds = poly.total_bounds
                ax.set_xlim(bounds[0] - 1, bounds[2] + 1)
                ax.set_ylim(bounds[1] - 1, bounds[3] + 1)

                ax.set_axis_off()

                legend_handles = [
                    Patch(
                        facecolor="none",
                        edgecolor="black",
                        linewidth=1,
                        label=country.upper()
                    ),
                    Line2D(
                        [0], [0],
                        marker="o",
                        color="w",
                        markerfacecolor="red",
                        markersize=6,
                        label="OP outside boundary"
                    )
                ]
                ax.legend(handles=legend_handles, loc="lower left")

                if stats is not None and pd.notna(stats.get("mean_m")):
                    txt = (
                        "Distance to border (m)\n"
                        f"min:  {stats['min_m']:.1f}\n"
                        f"mean: {stats['mean_m']:.1f}\n"
                        f"max:  {stats['max_m']:.1f}"
                    )
                    ax.text(
                        0.02, 0.98,
                        txt,
                        transform=ax.transAxes,
                        ha="left",
                        va="top",
                        fontsize=9,
                        bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.85, edgecolor="none")
                    )

                ax.set_axis_off()

                plt.title(f"OPs outside national boundary — {country.title()} (N = {n})")
                plt.tight_layout()
                plt.savefig(plot_dir / "bbox_violations_map.png", dpi=250)
                plt.close()

        except Exception as ex:
            print(f"[WARN] Could not create map for {country}: {ex}")

    # -------------------------------------------------------------------
    # 3. Completeness barplot
    # -------------------------------------------------------------------
    df = safe_read_csv(base / "ops_completeness.csv")
    if not df.empty and "completeness_%" in df.columns:
        df_sorted = df.sort_values("completeness_%", ascending=True)

        fig, ax = plt.subplots(figsize=(6, 4))
        sns.barplot(
            data=df_sorted,
            x="completeness_%",
            y="field",
            color="steelblue",
            ax=ax
        )
        ax.set_title(f"Attribute completeness of OPs ({country})")
        ax.set_xlabel("Completeness (%)")
        ax.set_ylabel("Field")
        ax.set_xlim(0, 105)

        # Prozentwerte an Balken annotieren
        for p in ax.patches:
            width = p.get_width()
            ax.text(
                width + 1,
                p.get_y() + p.get_height() / 2,
                f"{width:.1f}%",
                va="center"
            )

        plt.tight_layout()
        plt.savefig(plot_dir / "ops_completeness.png", dpi=200)
        plt.close()

    # -------------------------------------------------------------------
    # 4. OSM distance histogram
    # -------------------------------------------------------------------
    df = safe_read_csv(base / "ops_osm_overpass.csv")
    if not df.empty and "min_distance_m" in df.columns:
        df_valid = df.dropna(subset=["min_distance_m"])
        if not df_valid.empty:
            within_share = (df_valid["min_distance_m"] <= 100).mean() * 100
            max_show = 500  # Focus area

            plt.figure(figsize=(7, 4))
            sns.histplot(
                df_valid[df_valid["min_distance_m"] <= max_show],
                x="min_distance_m",
                bins=30,
                color="darkorange"
            )
            plt.axvline(100, color="red", linestyle="--", linewidth=1)

            plt.title(f"OSM distance to nearest railway feature ({country})")
            plt.xlabel("Distance (m)")
            plt.ylabel("Number of OPs")
            plt.xlim(0, max_show)

            ymax = plt.ylim()[1]
            plt.text(
                max_show * 0.98,
                ymax * 0.9,
                f"{within_share:.1f}% ≤ 100 m",
                ha="right",
                va="top"
            )

            plt.tight_layout()
            plt.savefig(plot_dir / "osm_distance_hist.png", dpi=200)
            plt.close()

        # -------------------------------------------------------------------
        # 5. Recency overview (stacked bar for OPs / SoLs)
        # -------------------------------------------------------------------
        df_rec = safe_read_csv(base / "recency_breakdown.csv")
        if not df_rec.empty and "type" in df_rec.columns:
            share_cols = [
                "current_share_%",
                "expired_share_%",
                "unknown_share_%",
                "not_yet_valid_share_%"
            ]

            missing_cols = [c for c in share_cols if c not in df_rec.columns]
            if len(missing_cols) == 0:
                rec = df_rec.set_index("type")[share_cols].copy()
                rec = rec.fillna(0)

                types_order = [t for t in ["ops", "sols"] if t in rec.index]
                if not types_order:
                    types_order = list(rec.index)

                rec = rec.loc[types_order]

                colors = {
                    "current_share_%": "#2ca02c",
                    "expired_share_%": "#d62728",
                    "unknown_share_%": "#7f7f7f",
                    "not_yet_valid_share_%": "#9467bd"
                }
                labels = {
                    "current_share_%": "current",
                    "expired_share_%": "expired",
                    "unknown_share_%": "unknown",
                    "not_yet_valid_share_%": "not yet valid"
                }

                fig, ax = plt.subplots(figsize=(6, 4))
                left = [0.0] * len(rec)

                for col in share_cols:
                    vals = rec[col].values
                    ax.barh(
                        rec.index,
                        vals,
                        left=left,
                        color=colors[col],
                        label=labels[col]
                    )
                    left = [l + v for l, v in zip(left, vals)]

                ax.set_xlabel("Share of records (%)")
                ax.set_ylabel("")
                ax.set_xlim(0, 100)
                ax.set_title(f"Validity status of OPs and SoLs ({country})")

                ax.legend(loc="lower center", bbox_to_anchor=(0.5, -0.25),
                        ncol=2, frameon=True)

                plt.tight_layout()
                plt.savefig(plot_dir / "recency_status.png", dpi=200)
                plt.close()

# =======================================================================================
#                                      Main
# =======================================================================================

def main() -> None:
    out = Path("out")
    if not out.exists():
        print("[ERROR] No 'out' directory found.")
        return

    for country_dir in sorted([p for p in out.iterdir() if p.is_dir()]):
        visualize_country(country_dir.name)

if __name__ == "__main__":
    main()