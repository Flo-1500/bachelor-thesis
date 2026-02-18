import os, sys, json, math, time, datetime
from pathlib import Path
from collections import Counter, defaultdict
from typing import Optional, Dict, Any, List, Tuple
from shapely.strtree import STRtree
from shapely.geometry import Point

import pandas as pd
from rdflib import Graph, URIRef
from rdflib.namespace import RDF, Namespace

# =======================================================================================
#                             Configuration / Flags
# =======================================================================================

ASSUME_BIDIRECTIONAL = True
MIN_OP_DISTANCE_M = 50

USE_OVERPASS = True             # OSM via Overpass (requires Internet connection)
OVERPASS_URL = "https://overpass-api.de/api/interpreter"
OVERPASS_RADIUS_M = 150
MAX_OSM_DISTANCE_M = 100

# QLever as in paper (ID + distance) – alternative to Overpass (use only one)
USE_QLEVER = False
QLEVER_ENDPOINT = "https://osm-qlever.cs.uni-freiburg.de/api"
ALIGN_DIST_M = 200              # alignment radius for candidate search
VALIDATE_DIST_M = 100           # stricter validation threshold
COUNTRY_REL_ID = { "austria": 16239 }  # example for QLever

# Country bounding boxes (fallback if shapefile is not available)
COUNTRY_BBOX = {
    "austria": (46.372, 9.530, 49.020, 17.162),
    "germany": (47.27, 5.87, 55.06, 15.04),
    "czechia": (48.55, 12.09, 51.06, 18.87),
    "slovakia": (47.73, 16.84, 49.61, 22.56),
    "slovenia": (45.42, 13.38, 46.88, 16.61),
    "hungary": (45.74, 16.11, 48.59, 22.90),
    "switzerland": (45.82, 5.96, 47.81, 10.49),
    "italy": (36.62, 6.62, 47.10, 18.52),
    "france": (41.34, -5.14, 51.09, 9.56),
    "poland": (49.00, 14.12, 55.03, 24.15),
    "netherlands": (50.75, 3.36, 53.67, 7.23),
    "belgium": (49.50, 2.50, 51.51, 6.41),
    "spain": (27.6, -18.2, 43.8, 4.3),
    "portugal": (36.8, -9.6, 42.2, -6.2),
    "denmark": (54.6, 8.1, 57.8, 15.2),
    "sweden": (55.3, 11.0, 69.1, 24.2),
    "norway": (57.9, 4.5, 71.2, 31.1),
    "finland": (59.5, 20.6, 70.1, 31.6),
    "estonia": (57.5, 21.8, 59.6, 28.2),
    "latvia": (55.7, 20.9, 58.1, 28.2),
    "lithuania": (53.9, 20.9, 56.4, 26.9),
    "luxembourg": (49.4, 5.7, 50.2, 6.5),
    "romania": (43.6, 20.3, 48.3, 29.7),
    "bulgaria": (41.2, 22.3, 44.3, 28.6),
    "greece": (34.8, 19.5, 41.8, 28.3),
    "croatia": (42.4, 13.4, 46.9, 19.4),
    "united kingdom": (49.9, -8.6, 60.9, 1.8),
    "ireland": (51.4, -10.6, 55.4, -5.4)
}

# Namespaces
GEO = Namespace("http://www.opengis.net/ont/geosparql#")
WGS84 = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
ERA = Namespace("http://data.europa.eu/949/")
HAS_ABSTR = URIRef(str(ERA) + "hasAbstraction")  # OP -> topology/netElements/<uopid>

# =======================================================================================
#                             Loading RDF graphs
# =======================================================================================

def load_data_from_folder(folder: Path) -> Graph:
    """
    Load all RDF-like files in a folder into an rdflib.Graph.
    Supports several formats (xml, turtle, n3, nt).
    """
    g = Graph()
    if not folder.exists():
        print(f"[WARN] Folder is missing: {folder}")
        return g
    files = [f for f in folder.iterdir() if f.suffix.lower() in (".rdf", ".xml", ".ttl", ".n3", ".nt")]
    if not files:
        print(f"[WARN] No RDF files found in {folder}.")
        return g
    for f in files:
        ok = False
        for fmt in ("xml", "application/rdf+xml", "turtle", "n3", "nt"):
            try:
                g.parse(str(f), format=fmt)
                ok = True
                break
            except Exception:
                continue
        if not ok:
            print(f"[WARN] Not able to parse: {f.name}")
    print(f"[INFO] Files loaded from {folder} — Triples: {len(g)} (Files: {len(files)})")
    return g

def localname(u: URIRef | str) -> str:
    s = str(u)
    if "#" in s:
        return s.split("#")[-1]
    return s.rstrip("/").split("/")[-1]

# =======================================================================================
#                            Geometry helpers
# =======================================================================================

def haversine_m(lat1, lon1, lat2, lon2):
    """Compute great-circle distance (meters) between two WGS84 points."""
    try:
        if None in (lat1, lon1, lat2, lon2):
            return None
        lat1 = float(lat1); lon1 = float(lon1)
        lat2 = float(lat2); lon2 = float(lon2)
    except (TypeError, ValueError):
        return None

    R = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlmb/2)**2
    c = 2 * math.asin(math.sqrt(a))
    return R * c

# =======================================================================================
#                            STEP 1 — Mapping (Setup)
# =======================================================================================

def summary_types_preds(g: Graph) -> Tuple[Counter, Counter]:
    """Return counts of rdf:types and predicates used in the graph."""
    type_cnt, pred_cnt = Counter(), Counter()
    for s, p, o in g.triples((None, RDF.type, None)):
        type_cnt[o] += 1
    for s, p, o in g:
        pred_cnt[p] += 1
    return type_cnt, pred_cnt

def guess_by_tokens(counter: Counter, tokens: List[str]) -> Optional[URIRef]:
    """
    Heuristic: try to find a URI whose localname or full URI contains any token.
    """
    items = sorted(counter.items(), key=lambda x: -x[1])
    tok = [t.lower() for t in tokens]

    # Try localname match first
    for uri, _ in items:
        ln = localname(uri).lower()
        if any(t in ln for t in tok):
            return URIRef(str(uri))

    # Fallback: full URI contains token
    for uri, _ in items:
        if any(t in str(uri).lower() for t in tok):
            return URIRef(str(uri))
    return None

def mapping_from_graphs(g_op: Graph, g_sol: Graph, overrides: Dict[str, str] | None = None) -> Dict[str, URIRef | None]:
    """
    Build a predicate/class mapping from the graphs by guessing common tokens,
    with optional manual overrides from JSON.
    """
    op_types, op_preds = summary_types_preds(g_op) if g_op else (Counter(), Counter())
    sol_types, sol_preds = summary_types_preds(g_sol) if g_sol else (Counter(), Counter())

    OP_CLASS = guess_by_tokens(op_types, ["operational", "point", "station", "op"])
    SOL_CLASS = guess_by_tokens(sol_types, ["section", "line", "segment", "track", "sol"])

    P_ID = guess_by_tokens(op_preds, ["uopid", "id", "identifier", "uuid", "code"])
    P_NAME = guess_by_tokens(op_preds, ["name", "label", "denomination", "designation", "title"])
    P_LAT = guess_by_tokens(op_preds, ["lat", "latitude"])
    P_LON = guess_by_tokens(op_preds, ["lon", "long", "longitude"])
    P_OPTYP = guess_by_tokens(op_preds, ["optype", "op_type", "type", "category", "kind"])
    P_CNTRY = guess_by_tokens(op_preds, ["country", "inCountry", "memberstate", "member_state"])

    P_LOCATION = (guess_by_tokens(op_preds, ["location", "hasGeometry", "geometry", "position"])
                  or URIRef("http://www.w3.org/2003/01/geo/wgs84_pos#location"))

    P_START = guess_by_tokens(sol_preds, ["start", "from", "begin", "source"])
    P_END = guess_by_tokens(sol_preds, ["end", "to", "destination", "target"])
    P_SOLID = guess_by_tokens(sol_preds, ["id", "identifier", "uuid", "code"])

    mp = {
        "OP_CLASS": OP_CLASS,
        "SOL_CLASS": SOL_CLASS,
        "P_ID": P_ID,
        "P_NAME": P_NAME,
        "P_LAT": P_LAT,
        "P_LON": P_LON,
        "P_LOCATION": P_LOCATION,
        "P_OPTYP": P_OPTYP,
        "P_CNTRY": P_CNTRY,
        "P_START": P_START,
        "P_END": P_END,
        "P_SOLID": P_SOLID
    }

    overrides = overrides or {}
    for k, v in overrides.items():
        if v is None:
            mp[k] = None
        elif isinstance(v, str) and v:
            mp[k] = URIRef(v)
    return mp

# =======================================================================================
#                      OP location resolver (WKT / WGS84 / URL pattern)
# =======================================================================================

def _wkt_to_latlon(wkt: str):
    # "POINT(lon lat)" -> (lat, lon)
    if not isinstance(wkt, str) or "POINT" not in wkt:
        return None, None
    try:
        inside = wkt.split("POINT(")[1].split(")")[0].strip()
        lon, lat = inside.split()
        return float(lat), float(lon)
    except Exception:
        return None, None

def _parse_latlon_from_location_uri(uri_str: str):
    # Example: .../locations/%2B16.490825/47.789052  -> lon=+16.490825, lat=47.789052
    if not isinstance(uri_str, str):
        return None, None
    try:
        from urllib.parse import unquote
        s = unquote(uri_str)
        parts = s.strip("/").split("/")
        if len(parts) < 2:
            return None, None
        lon_s, lat_s = parts[-2], parts[-1]
        lon = float(lon_s)
        lat = float(lat_s)
        return lat, lon
    except Exception:
        return None, None

def resolve_op_lat_lon(g: Graph, s: URIRef, p_location: Optional[URIRef], p_lat: Optional[URIRef], p_lon: Optional[URIRef]):
    """
    Preferred order:
    1) Direct literals on OP (p_lat/p_lon), if present
    2) geosparql:hasGeometry -> geo:asWKT
    3) wgs84:lat/long on the geometry node
    4) Fallback: parse coordinates from the geometry URI
    """
    # 1) Direct literals on OP
    if p_lat and p_lon:
        lat_lit = g.value(s, p_lat)
        lon_lit = g.value(s, p_lon) or g.value(s, WGS84.long) or g.value(s, WGS84.lon)
        if lat_lit and lon_lit:
            try:
                return float(str(lat_lit)), float(str(lon_lit))
            except Exception:
                pass

    if not p_location:
        return None, None

    loc = g.value(s, p_location)
    if not loc:
        return None, None

    # 2) GeoSPARQL WKT
    wkt = g.value(loc, GEO.asWKT)
    if wkt:
        lat, lon = _wkt_to_latlon(str(wkt))
        if lat is not None:
            return lat, lon

    # 3) WGS84 lat/long on the location node
    lat_lit = g.value(loc, WGS84.lat)
    lon_lit = g.value(loc, WGS84.long) or g.value(loc, WGS84.lon)
    if lat_lit and lon_lit:
        try:
            return float(str(lat_lit)), float(str(lon_lit))
        except Exception:
            pass

    # 4) Fallback: URI pattern
    return _parse_latlon_from_location_uri(str(loc))

# =======================================================================================
#                          STEP 2 — Extracting into DataFrames
# =======================================================================================

def extract_ops(g: Graph, mp: Dict[str, URIRef | None]) -> pd.DataFrame:
    """
    Extract Operational Points from the OP graph into a flat table.
    """
    oc, pid, pname = mp.get("OP_CLASS"), mp.get("P_ID"), mp.get("P_NAME")
    plat, plon = mp.get("P_LAT"), mp.get("P_LON")
    ploc = mp.get("P_LOCATION")
    ptyp, pcnty = mp.get("P_OPTYP"), mp.get("P_CNTRY")

    rows = []
    if not (g and oc and pid and (ploc or (plat and plon))):
        return pd.DataFrame(columns=["uri", "id", "name", "lat", "lon", "op_type", "op_type_label", "country"])

    for s in g.subjects(RDF.type, oc):
        op_id = g.value(s, pid)
        name = g.value(s, pname) if pname else None
        lat, lon = resolve_op_lat_lon(g, s, ploc, plat, plon)
        typ = g.value(s, ptyp) if ptyp else None
        ctry = g.value(s, pcnty) if pcnty else None

        CANON = URIRef("http://data.europa.eu/949/canonicalURI")
        canon = g.value(s, CANON)

        typ_label = localname(typ) if typ else None

        rows.append({
            "uri": str(s),
            "uri_canonical": (str(canon) if canon else None),
            "id":  str(op_id) if op_id is not None else None,
            "name": str(name) if name is not None else None,
            "lat": lat,
            "lon": lon,
            "op_type": str(typ) if typ is not None else None,
            "op_type_label": typ_label,
            "country": str(ctry) if ctry is not None else None
        })
    return pd.DataFrame(rows)

OP_TYPE_MAP = {
    "10": "station",
    "20": "small station",
    "30": "passenger terminal",
    "40": "freight terminal",
    "50": "depot or workshop",
    "60": "train technical services",
    "70": "passenger stop",
    "80": "junction",
    "90": "border point",
    "100": "shunting yard",
    "110": "technical change",
    "120": "switch",
    "130": "private siding",
    "140": "domestic border point"
}

def map_op_type_label(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize op_type labels using numeric code suffixes if present.
    """
    df = df.copy()
    if "op_type_label" in df.columns:
        df["op_type_code"] = df["op_type_label"].str.extract(r"(\d+)$")
        df["op_type_name"] = df["op_type_code"].map(OP_TYPE_MAP).fillna(df["op_type_label"])
    else:
        df["op_type_name"] = None
    return df

# NetElement Mapping (OP <-> NE)
def build_netElement_maps(g_op: Graph, op_class: URIRef):
    """
    Build maps between OP URIs and their abstracted NetElements (hasAbstraction).
    """
    op2ne, ne2op = {}, {}
    for s in g_op.subjects(RDF.type, op_class):
        ne = g_op.value(s, HAS_ABSTR)
        if ne:
            op2ne[str(s)] = str(ne)
            ne2op[str(ne)] = str(s)
    return op2ne, ne2op

# Heuristic: find SoL predicates that point to NetElements
def find_sol_endpoint_preds_via_ne(g_sol: Graph, sol_class: URIRef, ne_set: set[str], limit=5000):
    from collections import Counter
    c = Counter()
    samples = {}
    i = 0
    for s in g_sol.subjects(RDF.type, sol_class):
        for _, p, o in g_sol.triples((s, None, None)):
            if str(o) in ne_set:
                c[p] += 1
                samples.setdefault(p, (s, o))
        i += 1
        if i >= limit:
            break
    preds = [p for p, _ in c.most_common(2)]
    return preds

def extract_sols(g_sol: Graph, g_op: Graph, mp: Dict[str, URIRef | None]) -> pd.DataFrame:
    """
    Extract SoLs with start/end references (either directly to OPs or via NetElements).
    """
    sc, ps, pe, sid = mp.get("SOL_CLASS"), mp.get("P_START"), mp.get("P_END"), mp.get("P_SOLID")
    rows = []
    if not (g_sol and sc):
        return pd.DataFrame()

    # Map: NetElement <-> OP
    op2ne, ne2op = build_netElement_maps(g_op, mp["OP_CLASS"])
    ne_set = set(ne2op.keys())

    # Direct mode: SoL -> OP via P_START/P_END
    direct_mode = bool(ps and pe)

    # Otherwise: try to infer endpoints via NetElements
    if not direct_mode:
        cand_preds = find_sol_endpoint_preds_via_ne(g_sol, sc, ne_set)
        P_START_NE = cand_preds[0] if len(cand_preds) > 0 else None
        P_END_NE = cand_preds[1] if len(cand_preds) > 1 else None
    else:
        P_START_NE = P_END_NE = None

    for s in g_sol.subjects(RDF.type, sc):
        sol_id = g_sol.value(s, sid) if sid else None

        if direct_mode:
            st = g_sol.value(s, ps)
            en = g_sol.value(s, pe)
            start_op = ne2op.get(str(st), str(st)) if st else None
            end_op = ne2op.get(str(en), str(en)) if en else None
        else:
            ne_st = g_sol.value(s, P_START_NE) if P_START_NE else None
            ne_en = g_sol.value(s, P_END_NE) if P_END_NE else None
            start_op = ne2op.get(str(ne_st)) if ne_st else None
            end_op = ne2op.get(str(ne_en)) if ne_en else None

        rows.append({
            "uri": str(s),
            "sol_id": str(sol_id) if sol_id else None,
            "start": start_op,
            "end":   end_op
        })
    return pd.DataFrame(rows)

# =======================================================================================
#                          STEP 3 — Availability & Completeness
# =======================================================================================

def availability(df_ops, df_sols) -> Dict[str, Any]:
    return {
        "ops_count": int(len(df_ops)),
        "sols_count": int(len(df_sols)),
        "ops_empty": bool(df_ops.empty),
        "sols_empty": bool(df_sols.empty)
    }

def completeness_report(df: pd.DataFrame, fields: List[str]) -> pd.DataFrame:
    """
    For each field, compute non-null count and percentage completeness.
    """
    n = len(df)
    recs = []
    for f in fields:
        miss = int(df[f].isna().sum()) if f in df.columns else n
        non = n - miss
        pct = round(100.0 * non / n, 2) if n else 0.0
        recs.append({
            "field": f,
            "non_null": non,
            "total": n,
            "completeness_%": pct
        })
    return pd.DataFrame(recs)

def empty_columns(df: pd.DataFrame) -> List[str]:
    """
    Return a list of columns that are entirely empty (all NaN).
    """
    return [c for c in df.columns if df[c].isna().all()]

def mostly_empty_rows(df: pd.DataFrame, important_cols: List[str], allow_non_null: int = 2) -> pd.DataFrame:
    """
    Rows with very few non-null values in the selected important columns.
    """
    if df.empty:
        return df
    mask = (df[important_cols].isna().sum(axis=1) >= (len(important_cols) - allow_non_null))
    return df[mask]

# =======================================================================================
#                          STEP 4 — Structural (OP)
# =======================================================================================

def structural_ops(df_ops: pd.DataFrame) -> Dict[str, int]:
    """
    Structural checks for OPs:
    - duplicate IDs
    - missing any of the required fields in a row
    """
    dup = int(df_ops["id"].dropna().duplicated().sum()) if "id" in df_ops else 0
    req_cols = ["id", "lat", "lon", "op_type", "country"]
    missing_req = df_ops[df_ops[req_cols].isna().any(axis=1)] if not df_ops.empty else pd.DataFrame()
    return {"duplicate_ids": dup, "missing_required_rows": int(len(missing_req))}

# =======================================================================================
#                          STEP 5 — Cross-Reference SoL -> OP
# =======================================================================================

def crossrefs_sols_ops(df_ops: pd.DataFrame, df_sols: pd.DataFrame) -> Dict[str, int]:
    """
    Check whether SoL endpoints (start/end) exist in OPs (by uri, canonical uri, or id).
    """
    if df_sols is None or df_sols.empty:
        return {"start_in_ops_true": 0, "end_in_ops_true": 0, "total_sols": 0}

    op_uri = set(df_ops["uri"]) if "uri" in df_ops else set()
    if "uri_canonical" in df_ops:
        op_uri |= set(df_ops["uri_canonical"].dropna())
    op_ids = set(df_ops["id"].dropna()) if "id" in df_ops else set()

    def in_ops(v: Optional[str]) -> bool:
        if not v:
            return False
        return (v in op_uri) or (v in op_ids)

    start_true = int(df_sols["start"].map(in_ops).sum()) if "start" in df_sols else 0
    end_true   = int(df_sols["end"].map(in_ops).sum())   if "end" in df_sols else 0
    return {"start_in_ops_true": start_true, "end_in_ops_true": end_true, "total_sols": int(len(df_sols))}

# =======================================================================================
#                          STEP 6 — Topology (Graph)
# =======================================================================================

def normalize_sols_endpoints(df_ops: pd.DataFrame, df_sols: pd.DataFrame) -> pd.DataFrame:
    """
    Map start/end to df_ops['uri'] via canonicalURI or id → for graph consistency.
    """
    if df_ops.empty or df_sols.empty:
        return df_sols.copy()
    canon2uri = {}
    if "uri_canonical" in df_ops.columns:
        canon2uri = {c: u for u, c in df_ops[["uri", "uri_canonical"]].dropna().itertuples(index=False, name=None)}
    id2uri = {}
    if "id" in df_ops.columns:
        id2uri = {i: u for u, i in df_ops[["uri", "id"]].dropna().itertuples(index=False, name=None)}
    op_uri_set = set(df_ops["uri"]) if "uri" in df_ops.columns else set()

    def to_primary(v: str | None) -> str | None:
        if not isinstance(v, str):
            return None
        if v in op_uri_set:
            return v
        if v in canon2uri:
            return canon2uri[v]
        if v in id2uri:
            return id2uri[v]
        return v

    out = df_sols.copy()
    if "start" in out:
        out["start"] = out["start"].map(to_primary)
    if "end" in out:
        out["end"] = out["end"].map(to_primary)
    return out

def topology_checks(df_ops: pd.DataFrame, df_sols: pd.DataFrame, assume_bidirectional: bool) -> Tuple[Dict[str, Any], List[str], List[str], List[set]]:
    """
    Build a directed graph from SoLs and compute:
    - component stats, isolates, nodes without edges
    - optionally, missing reverse edges if bidirectionality is assumed
    """
    try:
        import networkx as nx
    except ImportError:
        print("[HINT] pip install networkx for topology checks")
        return {}, [], [], []

    G = nx.DiGraph()
    for u in df_ops["uri"]:
        G.add_node(u)
    op_set = set(df_ops["uri"])
    for _, row in df_sols.iterrows():
        s, t = row.get("start"), row.get("end")
        if pd.notna(s) and pd.notna(t) and (s in op_set) and (t in op_set):
            G.add_edge(s, t)

    UG = G.to_undirected()
    comps = list(nx.connected_components(UG)) if UG.number_of_nodes() else []
    isolates = list(nx.isolates(UG))
    deg = dict(UG.degree())
    ops_without_sols = [n for n, d in deg.items() if d == 0]

    result = {
        "nodes": int(G.number_of_nodes()),
        "edges": int(G.number_of_edges()),
        "weak_components": int(len(comps)),
        "isolated_nodes": int(len(isolates)),
        "ops_without_sols_count": int(len(ops_without_sols)),
    }

    if assume_bidirectional and not df_sols.empty:
        missing_reverse = 0
        for s, t in G.edges():
            if not G.has_edge(t, s):
                missing_reverse += 1
        result["missing_reverse_edges"] = int(missing_reverse)

    comp_sizes = [len(c) for c in comps]
    result["component_sizes"] = sorted(comp_sizes, reverse=True)
    return result, isolates, ops_without_sols, comps

# =======================================================================================
#                         STEP 7 — Geographic & OSM checks
# =======================================================================================

def bbox_check(df_ops: pd.DataFrame, country: str) -> pd.DataFrame:
    """
    Simple bounding box check as fallback when shapefile is not available.
    """
    if country not in COUNTRY_BBOX or df_ops.empty:
        return pd.DataFrame()
    min_lat, min_lon, max_lat, max_lon = COUNTRY_BBOX[country]
    mask = ~((df_ops["lat"].between(min_lat, max_lat)) & (df_ops["lon"].between(min_lon, max_lon)))
    return df_ops[mask][["id", "name", "lat", "lon", "uri", "country"]]

def _normalize_country_query(user_token: str, shapes) -> Tuple[str, str] | None:
    """
    Normalize user country string to match the EU shapefile columns.
    Returns (column_name, value) to filter the shapes (prefers ISO-2 in CNTR_ID).
    """
    tok = (user_token or "").strip()
    if not tok:
        return None
    
    if len(tok) == 2 and tok.isalpha():
        iso2 = tok.upper()
        if "CNTR_ID" in shapes.columns and (shapes["CNTR_ID"] == iso2).any():
            return ("CNTR_ID", iso2)

    alias2iso = {
        "austria": "AT", "österreich": "AT",
        "belgium": "BE", "belgien": "BE",
        "bulgaria": "BG", "bulgarien": "BG",
        "croatia": "HR", "kroatien": "HR",
        "cyprus": "CY", "zypern": "CY",
        "czechia": "CZ", "czech republic": "CZ", "tschechien": "CZ",
        "denmark": "DK", "dänemark": "DK",
        "estonia": "EE", "estland": "EE",
        "finland": "FI", "finnland": "FI",
        "france": "FR", "frankreich": "FR",
        "germany": "DE", "deutschland": "DE",
        "greece": "EL", "griechenland": "EL",
        "hungary": "HU", "ungarn": "HU",
        "ireland": "IE", "irland": "IE",
        "italy": "IT", "italien": "IT",
        "latvia": "LV", "lettland": "LV",
        "lithuania": "LT", "litauen": "LT",
        "luxembourg": "LU", "luxemburg": "LU",
        "netherlands": "NL", "niederlande": "NL",
        "norway": "NO", "norwegen": "NO",
        "poland": "PL", "polen": "PL",
        "portugal": "PT",
        "romania": "RO", "rumänien": "RO",
        "slovakia": "SK", "slowakei": "SK",
        "slovenia": "SI", "slowenien": "SI",
        "spain": "ES", "spanien": "ES",
        "sweden": "SE", "schweden": "SE",
        "switzerland": "CH", "schweiz": "CH",
        "united kingdom": "UK", "uk": "UK", "vereinigtes königreich": "UK"
    }

    key = tok.lower()
    if key in alias2iso and "CNTR_ID" in shapes.columns:
        iso2 = alias2iso[key]
        if (shapes["CNTR_ID"] == iso2).any():
            return ("CNTR_ID", iso2)

    name_cols = [c for c in ["NAME_ENGL", "NAME_GERM", "CNTR_NAME"] if c in shapes.columns]
    for col in name_cols:
        if (shapes[col].str.lower() == key).any():
            return (col, tok.title())

    for col in name_cols:
        s = shapes[col].str.lower()
        m = s.str.startswith(key) | s.str.contains(rf"\b{key}\b", regex=True)
        if m.any():
            return (col, shapes.loc[m, col].iloc[0])

    return None

def shapefile_check(df_ops: pd.DataFrame, country: str, shp_path: Path) -> pd.DataFrame:
    """
    Prefer exact national boundaries via EU shapefile; fallback to bbox if needed.
    """
    try:
        import geopandas as gpd
    except Exception:
        return bbox_check(df_ops, country)

    if df_ops.empty or not shp_path.exists():
        return bbox_check(df_ops, country)

    shapes = gpd.read_file(shp_path).to_crs(4326)
    norm = _normalize_country_query(country, shapes)
    if norm is None:
        if "CNTR_ID" in shapes.columns and (shapes["CNTR_ID"] == country.upper()).any():
            name_col, value = "CNTR_ID", country.upper()
        else:
            return bbox_check(df_ops, country)
    else:
        name_col, value = norm

    country_poly = shapes[shapes[name_col] == value]
    if country_poly.empty:
        return bbox_check(df_ops, country)

    df_valid = df_ops.dropna(subset=["lat", "lon"]).copy()
    pts = gpd.GeoDataFrame(
        df_valid,
        geometry=gpd.points_from_xy(df_ops["lon"], df_ops["lat"]),
        crs=4326
    )
    mask = ~pts.within(country_poly.union_all())
    return df_valid.loc[mask, ["id", "name", "lat", "lon", "uri", "country"]]

def min_distance_pairs(df_ops: pd.DataFrame, thresh_m: float) -> pd.DataFrame:
    """
    Return pairs of OPs closer than thresh_m (approximation using STRtree).
    """
    if df_ops.empty:
        return pd.DataFrame()

    df_valid = df_ops.dropna(subset=["lat", "lon"]).copy()
    if df_valid.empty:
        return pd.DataFrame()

    # Create Shapely Points
    points = [Point(xy) for xy in zip(df_valid["lon"], df_valid["lat"])]
    tree = STRtree(points)
    idx_by_id = {id(pt): i for i, pt in enumerate(points)}

    rows = []
    for i, p in enumerate(points):
        # find all neighbors within threshold using a bounding circle (deg -> m)
        nearby = tree.query(p.buffer(thresh_m / 111320))
        for q in nearby:
            j = idx_by_id.get(id(q))
            if j is None or j <= i:
                continue
            # Approximate distance (degrees to meters)
            d = p.distance(q) * 111320
            if d < thresh_m:
                r1, r2 = df_valid.iloc[i], df_valid.iloc[j]
                rows.append({
                    "id_1": r1.get("id"),
                    "name_1": r1.get("name"),
                    "uri_1": r1.get("uri"),
                    "id_2": r2.get("id"),
                    "name_2": r2.get("name"),
                    "uri_2": r2.get("uri"),
                    "distance_m": round(d, 1)
                })
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values("distance_m")

# --------------------------- Overpass helpers ------------------------------------------

def overpass_query(lat: float, lon: float, radius_m: int = 150) -> Dict[str, Any]:
    q = f"""
[out:json][timeout:25];
(
  node(around:{radius_m},{lat},{lon})["railway"];
  way(around:{radius_m},{lat},{lon})["railway"];
  relation(around:{radius_m},{lat},{lon})["railway"];
);
out center;
"""
    import requests
    r = requests.post(OVERPASS_URL, data={"data": q}, timeout=30)
    r.raise_for_status()
    return r.json()

def osm_type_compatible(op_type: Optional[str], osm_tags: Dict[str, Any]) -> bool:
    """
    Simple rule-of-thumb compatibility:
    - 'station'/'passenger'/'halt' in OP type -> OSM railway tag in {station, halt, stop_position, platform, stop}
      (exclude tram stops)
    - 'junction' in OP type -> OSM railway tag in {yard, siding, switch}
    Otherwise: assume compatible (conservative).
    """
    if not op_type:
        return True
    op = op_type.lower()
    rw = (osm_tags.get("railway") or "").lower()
    if any(k in op for k in ["station", "passenger", "halt"]):
        return rw in {"station", "halt", "stop_position", "platform", "stop"} and (osm_tags.get("tram") != "yes")
    if any(k in op for k in ["junction", "branch", "split"]):
        return rw in {"yard", "siding", "switch"}
    return True

def osm_check_ops_overpass(df_ops: pd.DataFrame, radius_m: int, max_rows: int = 200) -> pd.DataFrame:
    """
    For a sample of OPs, query Overpass and compute min distance to any railway element
    and a simple type compatibility flag.
    """
    if df_ops.empty or not USE_OVERPASS:
        return pd.DataFrame()
    sample = df_ops.dropna(subset=["lat", "lon"]).sort_values("id").head(max_rows).copy()
    rows = []
    for _, row in sample.iterrows():
        try:
            resp = overpass_query(row["lat"], row["lon"], radius_m=radius_m)
            el = resp.get("elements", [])
            min_d = None
            type_ok = None
            for e in el:
                if "lat" in e and "lon" in e:
                    lat2, lon2 = e["lat"], e["lon"]
                elif "center" in e and e["center"]:
                    lat2, lon2 = e["center"].get("lat"), e["center"].get("lon")
                else:
                    continue
                d = haversine_m(row["lat"], row["lon"], lat2, lon2)
                if d is None:
                    continue
                if (min_d is None) or (d < min_d):
                    min_d = d
                tags = e.get("tags", {})
                op_type_for_osm = row.get("op_type_name") or row.get("op_type_label") or row.get("op_type")
                comp = osm_type_compatible(op_type_for_osm, tags)
                type_ok = comp if type_ok is None else (type_ok or comp)
            rows.append({
                "id": row["id"],
                "name": row["name"],
                "lat": row["lat"],
                "lon": row["lon"],
                "osm_elements": len(el),
                "min_distance_m": (round(min_d, 1) if min_d is not None else None),
                "within_threshold": (min_d is not None and min_d <= MAX_OSM_DISTANCE_M),
                "osm_type_compatible": (bool(type_ok) if min_d is not None else None)
            })
            time.sleep(1.0)
        except Exception as e:
            rows.append({
                "id": row["id"], "name": row["name"], "lat": row["lat"], "lon": row["lon"],
                "osm_elements": -1, "error": str(e)[:200]
            })
    return pd.DataFrame(rows)

def overpass_fetch_osm_stations(country: str, shp_path: Path | None = None) -> pd.DataFrame:
    """
    Fetch OSM railway nodes for a country:
    - Prefer polygon query from EU shapefile (more precise)
    - Fallback to bounding box from COUNTRY_BBOX
    """
    import requests
    import geopandas as gpd
    from shapely.geometry import MultiPolygon, Polygon

    # Polygon query (preferred)
    if shp_path and shp_path.exists():
        try:
            shapes = gpd.read_file(shp_path).to_crs(4326)
            if "CNTR_ID" in shapes.columns:
                iso = country.upper() if len(country) == 2 else None
                if not iso:
                    alias2iso = {
                        "austria": "AT", "österreich": "AT",
                        "belgium": "BE", "belgien": "BE",
                        "bulgaria": "BG", "bulgarien": "BG",
                        "croatia": "HR", "kroatien": "HR",
                        "cyprus": "CY", "zypern": "CY",
                        "czechia": "CZ", "czech republic": "CZ", "tschechien": "CZ",
                        "denmark": "DK", "dänemark": "DK",
                        "estonia": "EE", "estland": "EE",
                        "finland": "FI", "finnland": "FI",
                        "france": "FR", "frankreich": "FR",
                        "germany": "DE", "deutschland": "DE",
                        "greece": "EL", "griechenland": "EL",
                        "hungary": "HU", "ungarn": "HU",
                        "ireland": "IE", "irland": "IE",
                        "italy": "IT", "italien": "IT",
                        "latvia": "LV", "lettland": "LV",
                        "lithuania": "LT", "litauen": "LT",
                        "luxembourg": "LU", "luxemburg": "LU",
                        "netherlands": "NL", "niederlande": "NL",
                        "norway": "NO", "norwegen": "NO",
                        "poland": "PL", "polen": "PL",
                        "portugal": "PT",
                        "romania": "RO", "rumänien": "RO",
                        "slovakia": "SK", "slowakei": "SK",
                        "slovenia": "SI", "slowenien": "SI",
                        "spain": "ES", "spanien": "ES",
                        "sweden": "SE", "schweden": "SE",
                        "switzerland": "CH", "schweiz": "CH",
                        "united kingdom": "UK", "uk": "UK", "vereinigtes königreich": "UK"
                    }
                    iso = alias2iso.get(country.lower())
                if iso:
                    country_poly = shapes[shapes["CNTR_ID"] == iso]
                else:
                    country_poly = pd.DataFrame()
            else:
                country_poly = shapes[shapes.iloc[:, 0].str.lower() == country.lower()]

            if not country_poly.empty:
                geom = country_poly.geometry.union_all()
                if isinstance(geom, Polygon):
                    polygons = [geom]
                elif isinstance(geom, MultiPolygon):
                    polygons = list(geom.geoms)
                else:
                    print(f"[WARN] Unsupported geometry type: {geom.geom_type}")
                    polygons = []

                if polygons:
                    coord_strs = []
                    for poly in polygons:
                        coords = " ".join(f"{y} {x}" for x, y in poly.exterior.coords)
                        coord_strs.append(coords)
                    coord_str = " ".join(coord_strs)

                    q = f"""
[out:json][timeout:300];
(
  node["railway"~"^(station|halt|stop|stop_position)$"](poly:"{coord_str}");
);
out body;
"""
                    print(f"[INFO] Using polygon query for {country} with {len(polygons)} polygon(s)")
                    r = requests.post(OVERPASS_URL, data={"data": q}, timeout=300)
                    r.raise_for_status()
                    data = r.json().get("elements", [])
                    rows = []
                    for e in data:
                        if e.get("type") != "node":
                            continue
                        tags = e.get("tags", {}) or {}
                        rows.append({
                            "osm_id": f'node/{e.get("id")}',
                            "name": tags.get("name"),
                            "type": tags.get("railway"),
                            "ref":  tags.get("ref") or tags.get("railway:ref"),
                            "lat":  e.get("lat"),
                            "lon":  e.get("lon")
                        })
                    return pd.DataFrame(rows)
                else:
                    print(f"[WARN] No valid polygons found for {country}, fallback to BBOX")
        except Exception as ex:
            print(f"[ERROR] Polygon Overpass failed for {country}, fallback to BBOX. Reason: {ex}")

    # Fallback: bounding box
    if country not in COUNTRY_BBOX:
        print(f"[ERROR] No BBOX for {country}")
        return pd.DataFrame()
    min_lat, min_lon, max_lat, max_lon = COUNTRY_BBOX[country]
    q = f"""
[out:json][timeout:120];
(
  node["railway"~"^(station|halt|stop|stop_position)$"]({min_lat},{min_lon},{max_lat},{max_lon});
);
out body;
"""
    print(f"[INFO] Using BBOX query for {country}")
    try:
        r = requests.post(OVERPASS_URL, data={"data": q}, timeout=180)
        r.raise_for_status()
        data = r.json().get("elements", [])
        rows = []
        for e in data:
            if e.get("type") != "node":
                continue
            tags = e.get("tags", {}) or {}
            rows.append({
                "osm_id": f'node/{e.get("id")}',
                "name": tags.get("name"),
                "type": tags.get("railway"),
                "ref":  tags.get("ref") or tags.get("railway:ref"),
                "lat":  e.get("lat"),
                "lon":  e.get("lon")
            })
        return pd.DataFrame(rows)
    except Exception as ex:
        print(f"[ERROR] Overpass (bbox) failed for {country}: {ex}")
        return pd.DataFrame()

# (Optional) QLever (per paper)
def _wkt_point_to_latlon(wkt: str):
    if not isinstance(wkt, str) or "POINT(" not in wkt:
        return None, None
    try:
        inside = wkt.split("POINT(")[1].split(")")[0].strip()
        lon, lat = inside.split()
        return float(lat), float(lon)
    except Exception:
        return None, None

def qlever_query(endpoint: str, sparql: str) -> dict:
    import requests
    r = requests.post(endpoint, data={"query": sparql}, timeout=60)
    r.raise_for_status()
    return r.json()

def qlever_fetch_stations(country: str) -> pd.DataFrame:
    rel = COUNTRY_REL_ID.get(country)
    if not rel:
        print(f"[WARN] No country relation id for {country}")
        return pd.DataFrame()
    sparql = f"""
PREFIX ogc: <http://www.opengis.net/rdf#>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX osm: <https://www.openstreetmap.org/>
PREFIX osmkey: <https://www.openstreetmap.org/wiki/Key:>
PREFIX osmrel: <https://www.openstreetmap.org/relation/>

SELECT ?osm_id ?name ?type ?ref ?wkt WHERE {{
  osmrel:{rel} ogc:sfContains ?osm_id .
  ?osm_id a osm:node .
  ?osm_id osmkey:railway ?type .
  FILTER(?type = "station" || ?type = "stop" || ?type = "halt")
  FILTER NOT EXISTS {{ ?osm_id osmkey:subway "yes" }}
  OPTIONAL {{ ?osm_id osmkey:name ?name }}
  OPTIONAL {{ ?osm_id osmkey:railway:ref ?ref }}
  ?osm_id geo:hasGeometry/geo:asWKT ?wkt .
}}
"""
    try:
        data = qlever_query(QLEVER_ENDPOINT, sparql)
        rows = []
        for b in data.get("results", {}).get("bindings", []):
            osm_id = b.get("osm_id", {}).get("value")
            name = b.get("name", {}).get("value")
            typ = b.get("type", {}).get("value")
            ref = b.get("ref", {}).get("value")
            wkt = b.get("wkt", {}).get("value")
            lat, lon = _wkt_point_to_latlon(wkt)
            rows.append({"osm_id": osm_id, "name": name, "type": typ, "ref": ref, "lat": lat, "lon": lon})
        return pd.DataFrame(rows)
    except Exception as e:
        print("[ERROR] QLever query failed:", e)
        return pd.DataFrame()

def _uopid_to_ref(uopid: str | None) -> str | None:
    if not uopid:
        return None
    s = str(uopid)
    if len(s) >= 3 and s[:2].isalpha():
        return s[2:]
    return s

def align_ops_to_osm(ops_df: pd.DataFrame, osm_df: pd.DataFrame, align_dist_m: int = 200, validate_dist_m: int = 100) -> tuple[pd.DataFrame, dict]:
    """
    Align OPs to OSM stations:
    1) ID-based join via ref
    2) Geospatial nearest candidate within align_dist_m
    """
    if ops_df.empty or osm_df.empty:
        return pd.DataFrame(), {}
    ops = ops_df.copy()
    ops["ref_guess"] = ops["id"].map(_uopid_to_ref)
    id_links = pd.merge(
        ops[["uri", "id", "name", "lat", "lon", "op_type", "ref_guess"]],
        osm_df[["osm_id", "name", "type", "ref", "lat", "lon"]],
        left_on="ref_guess", right_on="ref", how="inner", suffixes=("_era", "_osm")
    )
    id_links["method"] = "id_ref"
    id_links["distance_m"] = id_links.apply(lambda r: haversine_m(r["lat_era"], r["lon_era"], r["lat_osm"], r["lon_osm"]), axis=1)
    id_links["within_validate_threshold"] = id_links["distance_m"] <= validate_dist_m

    matched_uris = set(id_links["uri"])
    remaining = ops[~ops["uri"].isin(matched_uris)].dropna(subset=["lat", "lon"]).copy()

    cell_deg = 0.002  # ~220 m
    buckets = defaultdict(list)
    for j, r in osm_df.dropna(subset=["lat", "lon"]).iterrows():
        key = (int(r["lat"]/cell_deg), int(r["lon"]/cell_deg))
        buckets[key].append((r["osm_id"], r["name"], r["type"], r["ref"], r["lat"], r["lon"]))

    rows = []
    for i, r in remaining.iterrows():
        key = (int(r["lat"]/cell_deg), int(r["lon"]/cell_deg))
        cands = []
        for di in (-1, 0, 1):
            for dj in (-1, 0, 1):
                cands.extend(buckets.get((key[0]+di, key[1]+dj), []))
        best = None
        best_d = None
        for osm_id, oname, otype, oref, olat, olon in cands:
            d = haversine_m(r["lat"], r["lon"], olat, olon)
            if d is None:
                continue
            if d <= align_dist_m and (best_d is None or d < best_d):
                best_d = d
                best = (osm_id, oname, otype, oref, olat, olon)
        if best is not None:
            rows.append({
                "uri": r["uri"], "id": r["id"], "name_era": r["name"], "lat_era": r["lat"], "lon_era": r["lon"], "op_type": r["op_type"],
                "osm_id": best[0], "name_osm": best[1], "type": best[2], "ref": best[3], "lat_osm": best[4], "lon_osm": best[5],
                "distance_m": round(best_d, 1), "method": "geo_200m",
                "within_validate_threshold": best_d <= validate_dist_m
            })
    geo_links = pd.DataFrame(rows)

    links = pd.concat([id_links[[
        "uri", "id", "name_era", "lat_era", "lon_era", "op_type", "osm_id", "name_osm", "type", "ref", "lat_osm", "lon_osm", "distance_m", "within_validate_threshold", "method"
    ]], geo_links], ignore_index=True)

    def type_match(op_type, osm_type):
        if not isinstance(op_type, str) or not isinstance(osm_type, str):
            return True
        op = op_type.lower(); ot = osm_type.lower()
        if any(k in op for k in ["station", "passenger", "halt"]):
            return ot in {"station", "halt", "stop"}
        return True

    links["type_compatible"] = links.apply(lambda r: type_match(r.get("op_type"), r.get("type")), axis=1)
    n_ops = len(ops_df)
    matched = links["uri"].nunique()
    summary = {
        "ops_total": n_ops,
        "matched_total": int(matched),
        "matched_share_%": round(100*matched/n_ops, 2) if n_ops else 0.0,
        "by_id_ref": int(links[links["method"] == "id_ref"]["uri"].nunique()),
        "by_geo_200m": int(links[links["method"] == "geo_200m"]["uri"].nunique()),
        "within_100m_share_%": round(100*links["within_validate_threshold"].mean(), 1) if len(links) else None,
        "type_compatible_share_%": round(100*links["type_compatible"].mean(), 1) if len(links) else None,
        "notes": {"alignment_distance_m": ALIGN_DIST_M, "validation_distance_m": VALIDATE_DIST_M}
    }
    return links, summary

def osm_only_candidates(osm_df: pd.DataFrame, ops_df: pd.DataFrame, align_dist_m: int = 200) -> tuple[pd.DataFrame, dict]:
    """
    OSM nodes that do not have any OP within align_dist_m (recall perspective).
    Also add nearest OP and distance for context.
    """
    if osm_df.empty or ops_df.empty:
        return pd.DataFrame(), {"osm_total": int(len(osm_df)), "osm_only": int(len(osm_df))}

    ops = ops_df.dropna(subset=["lat", "lon"]).copy()
    osm = osm_df.dropna(subset=["lat", "lon"]).copy()
    if ops.empty or osm.empty:
        return pd.DataFrame(), {"osm_total": int(len(osm_df)), "osm_only": int(len(osm_df))}

    cell_deg = 0.002  # ~220 m
    buckets = defaultdict(list)
    for i, r in ops.iterrows():
        key = (int(r["lat"]/cell_deg), int(r["lon"]/cell_deg))
        buckets[key].append((r["uri"], r["id"], r["name"], r["lat"], r["lon"]))

    rows = []
    for j, r in osm.iterrows():
        key = (int(r["lat"]/cell_deg), int(r["lon"]/cell_deg))
        cands = []
        for di in (-1, 0, 1):
            for dj in (-1, 0, 1):
                cands.extend(buckets.get((key[0]+di, key[1]+dj), []))
        best = None; best_d = None
        for uri, oid, oname, olat, olon in cands:
            d = haversine_m(r["lat"], r["lon"], olat, olon)
            if d is None:
                continue
            if best_d is None or d < best_d:
                best_d = d; best = (uri, oid, oname, olat, olon)
        if best_d is None or best_d > align_dist_m:
            rows.append({
                "osm_id": r.get("osm_id"),
                "name_osm": r.get("name"),
                "type_osm": r.get("type"),
                "ref_osm":  r.get("ref"),
                "lat_osm":  r.get("lat"),
                "lon_osm":  r.get("lon"),
                "nearest_op_uri": (best[0] if best else None),
                "nearest_op_id":  (best[1] if best else None),
                "nearest_op_name":(best[2] if best else None),
                "nearest_distance_m": (round(best_d, 1) if best_d is not None else None)
            })
    out = pd.DataFrame(rows).sort_values(["nearest_distance_m", "name_osm"], na_position="last")
    summary = {"osm_total": int(len(osm_df)), "osm_only": int(len(out)), "align_distance_m": align_dist_m}
    return out, summary

# =======================================================================================
#                          STEP 8 — Recency check
# =======================================================================================

DATE_TOKENS = ["date", "datetime", "modified", "updated", "lastchange", "last_update", "validfrom", "validuntil", "timestamp", "created"]

def find_date_predicates(g: Graph) -> List[URIRef]:
    _, preds = summary_types_preds(g)
    hits = []
    for p, _ in preds.most_common(200):
        ln = localname(p).lower()
        full = str(p).lower()
        if any(t in ln for t in DATE_TOKENS) or any(t in full for t in DATE_TOKENS):
            hits.append(p)
    return hits[:8]

def parse_any_date(val: Any) -> Optional[datetime.datetime]:
    s = str(val)
    fmts = ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y/%m/%d", "%d.%m.%Y")
    for fmt in fmts:
        try:
            return datetime.datetime.strptime(s, fmt)
        except Exception:
            pass
    try:
        return datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def recency_check(g: Graph, subjects: List[str]) -> Dict[str, Any]:
    """
    Classify each subject by validity window:
    - current: today is <= ValidTo AND (ValidFrom missing OR today >= ValidFrom)
    - expired: today > ValidTo
    - unknown: ValidTo missing or unparsable
    - not_yet_valid: ValidFrom exists and today < ValidFrom
    Returns counts and shares.
    """
    VALID_FROM_CANDS = [
    URIRef(str(ERA) + "validityStartDate"),
    URIRef(str(ERA) + "validFrom"),
    URIRef(str(ERA) + "isValidFrom")
    ]
    VALID_TO_CANDS = [
        URIRef(str(ERA) + "validityEndDate"),
        URIRef(str(ERA) + "validTo"),
        URIRef(str(ERA) + "isValidTo")
    ]


    def _parse_dt(val: Any) -> Optional[datetime.datetime]:
        if val is None:
            return None
        s = str(val).replace("Z", "+00:00")
        for fmt in ("%Y-%m-%d","%d.%m.%Y","%Y/%m/%d","%Y-%m-%dT%H:%M:%S","%Y-%m-%dT%H:%M:%S%z"):
            try:
                return datetime.datetime.strptime(s, fmt)
            except Exception:
                continue
        try:
            return datetime.datetime.fromisoformat(s)
        except Exception:
            return None

    today = datetime.datetime.utcnow()
    subj_set = set(subjects or [])

    counts = {"current": 0, "expired": 0, "unknown": 0, "not_yet_valid": 0}
    used_from_preds, used_to_preds = set(), set()

    for s_str in subj_set:
        s = URIRef(s_str)

        # pick first available ValidTo
        ve_lit = None
        for p in VALID_TO_CANDS:
            v = g.value(s, p)
            if v is not None:
                ve_lit = v
                used_to_preds.add(str(p))
                break

        # pick first available ValidFrom
        vs_lit = None
        for p in VALID_FROM_CANDS:
            v = g.value(s, p)
            if v is not None:
                vs_lit = v
                used_from_preds.add(str(p))
                break

        dt_ve = _parse_dt(ve_lit) if ve_lit is not None else None
        dt_vs = _parse_dt(vs_lit) if vs_lit is not None else None

        if dt_ve is None:
            counts["unknown"] += 1
            continue

        if dt_vs is not None and today < dt_vs:
            counts["not_yet_valid"] += 1
            continue

        if today <= dt_ve:
            counts["current"] += 1
        else:
            counts["expired"] += 1

    total = sum(counts.values()) if subj_set else 0
    shares = {k + "_share_%": (round(100.0 * v / total, 2) if total else None) for k, v in counts.items()}
    preds_list = sorted(list(used_to_preds | used_from_preds)) if (used_to_preds or used_from_preds) else [str(ERA)+"validFrom", str(ERA)+"validTo"]

    return {
        "date_predicates": preds_list,
        "total": total,
        **counts,
        **shares
    }

# =======================================================================================
#                                    Scorecard
# =======================================================================================

def rules_scorecard(
    country: str,
    cross_norm: dict,
    near_pairs_df: pd.DataFrame,
    bbox_viol_df: pd.DataFrame,
    overpass_hits_df: pd.DataFrame | None,
    topo: dict,
    rec_ops: dict, rec_sols: dict,
    osm_only_summary: dict | None = None
) -> dict:
    out = {}

    # R1: SoL endpoints exist
    tot = cross_norm.get("total_sols", 0)
    out["R1_sols_endpoints_exist_%"] = round(100.0 * ((cross_norm.get("start_in_ops_true", 0) + cross_norm.get("end_in_ops_true", 0)) / (2 * tot)), 2) if tot else None

    # R2: Minimum distance OP↔OP
    if near_pairs_df is not None and not near_pairs_df.empty:
        out["R2_min_distance_violations"] = int((near_pairs_df["distance_m"] < MIN_OP_DISTANCE_M).sum())
        out["R2_zero_distance_pairs"] = int((near_pairs_df["distance_m"] == 0.0).sum())
    else:
        out["R2_min_distance_violations"] = 0
        out["R2_zero_distance_pairs"] = 0

    # R3: OPs within national boundary (strict)
    out["R3_bbox_violations"] = int(len(bbox_viol_df)) if bbox_viol_df is not None else None

    # R4/R5: OSM position consistency and type compatibility
    if overpass_hits_df is not None and not overpass_hits_df.empty:
        ok = overpass_hits_df["within_threshold"].infer_objects(copy=False).astype(bool).mean()
        out["R4_osm_within_threshold_%"] = round(100.0 * ok, 1)
        typok = overpass_hits_df["osm_type_compatible"].dropna().mean() if "osm_type_compatible" in overpass_hits_df else None
        out["R5_osm_type_compatible_%"] = (round(100.0 * typok, 1) if typok is not None else None)
    else:
        out["R4_osm_within_threshold_%"] = None
        out["R5_osm_type_compatible_%"] = None

    # R6: Bidirectionality
    if topo and topo.get("edges", 0):
        out["R6_missing_reverse_edges_%"] = round(100.0 * topo.get("missing_reverse_edges", 0) / topo["edges"], 2)

    # R7: Recency
    out["R7_recency_ops_%"] = rec_ops.get("current_share_%")
    out["R7_recency_sols_%"] = rec_sols.get("current_share_%")
    out["R7_ops_expired_%"] = rec_ops.get("expired_share_%")
    out["R7_ops_unknown_%"] = rec_ops.get("unknown_share_%")
    out["R7_sols_expired_%"] = rec_sols.get("expired_share_%")
    out["R7_sols_unknown_%"] = rec_sols.get("unknown_share_%")

    # R8: Coverage (OSM lists stations missing in RINF)
    if osm_only_summary:
        tot = max(1, int(osm_only_summary.get("osm_total", 0)))
        miss = int(osm_only_summary.get("osm_only", 0))
        out["R8_osm_only_count"] = miss
        out["R8_osm_total"] = int(osm_only_summary.get("osm_total", 0))
        out["R8_osm_only_share_%"] = round(100.0 * miss / tot, 2)
        out["R8_align_distance_m"] = int(osm_only_summary.get("align_distance_m", ALIGN_DIST_M))
    else:
        out["R8_osm_only_count"] = None
        out["R8_osm_total"] = None
        out["R8_osm_only_share_%"] = None
        out["R8_align_distance_m"] = ALIGN_DIST_M

    out["notes"] = {
        "country": country,
        "min_op_distance_m": MIN_OP_DISTANCE_M,
        "max_osm_distance_m": MAX_OSM_DISTANCE_M,
        "assume_bidirectional": ASSUME_BIDIRECTIONAL
    }
    return out

# =======================================================================================
#                             Extra: Missing reverse edges CSV
# =======================================================================================

def save_missing_reverse_edges(df_sols: pd.DataFrame, out_dir: Path) -> None:
    """
    Save a CSV with directed edges that do not have a reverse edge.
    """
    if df_sols.empty:
        return
    rows = []
    for _, r in df_sols.iterrows():
        s = r.get("start"); t = r.get("end")
        if pd.notna(s) and pd.notna(t):
            rev_exists = ((df_sols["start"] == t) & (df_sols["end"] == s)).any()
            if not rev_exists:
                rows.append({"start": s, "end": t})
    if rows:
        pd.DataFrame(rows).drop_duplicates().to_csv(out_dir / "missing_reverse_edges.csv", index=False)

# =======================================================================================
#                                      Main
# =======================================================================================

def main() -> None:
    # Country input
    country = (sys.argv[1] if len(sys.argv) > 1 else input("Please input any EU-Country (e.g., austria, germany): ").strip().lower())
    base = Path("data")
    op_path  = base / f"op_{country}"
    sol_path = base / f"sol_{country}"

    out_dir = Path("out") / country
    out_dir.mkdir(parents=True, exist_ok=True)

    # Load RDF
    g_op = load_data_from_folder(op_path)
    g_sol = load_data_from_folder(sol_path)

    # Mapping (with optional overrides from JSON)
    overrides_path = Path("mapping_overrides.json")
    overrides = json.loads(overrides_path.read_text()) if overrides_path.exists() else {}
    mp = mapping_from_graphs(g_op, g_sol, overrides)

    # Extract
    ops_df = extract_ops(g_op, mp)
    ops_df = map_op_type_label(ops_df)

    sols_df = extract_sols(g_sol, g_op, mp)

    ops_df.to_csv(out_dir / "ops_extract.csv", index=False)
    sols_df.to_csv(out_dir / "sols_extract.csv", index=False)
    sols_df_norm = normalize_sols_endpoints(ops_df, sols_df)

    # STEP 3 — Availability & Completeness
    avail = availability(ops_df, sols_df)
    pd.DataFrame([avail]).to_csv(out_dir / "availability.csv", index=False)

    ops_comp = completeness_report(ops_df, ["id", "name", "lat", "lon", "op_type", "country"])
    sols_comp = completeness_report(sols_df, ["start", "end", "sol_id"])
    ops_comp.to_csv(out_dir / "ops_completeness.csv", index=False)
    sols_comp.to_csv(out_dir / "sols_completeness.csv", index=False)

    pd.Series(empty_columns(ops_df)).to_csv(out_dir / "ops_empty_columns.csv", index=False, header=["empty_column"])
    pd.Series(empty_columns(sols_df)).to_csv(out_dir / "sols_empty_columns.csv", index=False, header=["empty_column"])

    mostly_empty_ops = mostly_empty_rows(ops_df, ["id", "lat", "lon", "name", "op_type", "country"], allow_non_null=1)
    mostly_empty_ops.to_csv(out_dir / "ops_mostly_empty.csv", index=False)

    # STEP 4 — Structural (OP)
    struct = structural_ops(ops_df)
    Path(out_dir / "ops_structural.json").write_text(json.dumps(struct, indent=2, ensure_ascii=False))

    # STEP 5 — Cross-references SoL -> OP
    cross_raw = crossrefs_sols_ops(ops_df, sols_df)
    cross_norm = crossrefs_sols_ops(ops_df, sols_df_norm)
    Path(out_dir / "sols_crossrefs_raw.json").write_text(json.dumps(cross_raw, indent=2, ensure_ascii=False))
    Path(out_dir / "sols_crossrefs_norm.json").write_text(json.dumps(cross_norm, indent=2, ensure_ascii=False))

    # STEP 6 — Topology
    topo, isolates, ops_wo, comps = topology_checks(ops_df, sols_df_norm, ASSUME_BIDIRECTIONAL)
    Path(out_dir / "topology.json").write_text(json.dumps(topo, indent=2, ensure_ascii=False))
    save_missing_reverse_edges(sols_df_norm, out_dir)

    # STEP 7 — Geographic checks (prefer shapefile, otherwise BBOX)
    shapefile_path = Path("shapes/CNTR_RG_10M_2024_4326.shp")
    if shapefile_path.exists():
        bbox_viol = shapefile_check(ops_df, country, shapefile_path)
    else:
        bbox_viol = bbox_check(ops_df, country)
    bbox_viol.to_csv(out_dir / "ops_bbox_violations.csv", index=False)

    near_pairs = min_distance_pairs(ops_df, MIN_OP_DISTANCE_M)
    near_pairs.to_csv(out_dir / "ops_min_distance_pairs.csv", index=False)

    if USE_OVERPASS:
        overpass_hits = osm_check_ops_overpass(ops_df, OVERPASS_RADIUS_M)
        overpass_hits.to_csv(out_dir / "ops_osm_overpass.csv", index=False)

    # OSM coverage
    osm_nodes_bbox = pd.DataFrame()
    if USE_OVERPASS:
        shapefile_path = Path("shapes/CNTR_RG_10M_2024_4326.shp")
        osm_nodes_bbox = overpass_fetch_osm_stations(country, shp_path=shapefile_path)

        if not osm_nodes_bbox.empty:
            osm_nodes_bbox.to_csv(out_dir / "osm_nodes_bbox.csv", index=False)
            osm_only_df, osm_only_summary = osm_only_candidates(osm_nodes_bbox, ops_df, ALIGN_DIST_M)
            osm_only_df.to_csv(out_dir / "osm_only_nodes.csv", index=False)
            Path(out_dir / "osm_only_summary.json").write_text(json.dumps(osm_only_summary, indent=2, ensure_ascii=False))

    if USE_QLEVER:
        osm_nodes = qlever_fetch_stations(country)
        osm_nodes.to_csv(out_dir / "osm_qlever_nodes.csv", index=False)
        links_df, links_summary = align_ops_to_osm(ops_df, osm_nodes, ALIGN_DIST_M, VALIDATE_DIST_M)
        links_df.to_csv(out_dir / "osm_alignment.csv", index=False)
        Path(out_dir / "osm_alignment_summary.json").write_text(json.dumps(links_summary, indent=2, ensure_ascii=False))

    # STEP 8 — Recency
    rec_ops = recency_check(g_op, subjects=ops_df["uri"].tolist())
    rec_sols = recency_check(g_sol, subjects=sols_df["uri"].tolist())
    Path(out_dir / "recency_ops.json").write_text(json.dumps(rec_ops, indent=2, ensure_ascii=False))
    Path(out_dir / "recency_sols.json").write_text(json.dumps(rec_sols, indent=2, ensure_ascii=False))

    # CSV-summary per datatype
    pd.DataFrame([{
        "type": "ops",
        **{k: rec_ops.get(k) for k in ["total","current","expired","unknown","not_yet_valid",
                                        "current_share_%","expired_share_%","unknown_share_%","not_yet_valid_share_%"]}
    },
    {
        "type": "sols",
        **{k: rec_sols.get(k) for k in ["total","current","expired","unknown","not_yet_valid",
                                        "current_share_%","expired_share_%","unknown_share_%","not_yet_valid_share_%"]}
    }]).to_csv(out_dir / "recency_breakdown.csv", index=False)

    # Scorecard
    overpass_hits = pd.read_csv(out_dir / "ops_osm_overpass.csv") if (USE_OVERPASS and (out_dir / "ops_osm_overpass.csv").exists()) else pd.DataFrame()
    if (out_dir / "osm_only_summary.json").exists():
        osm_only_summary = json.loads(Path(out_dir / "osm_only_summary.json").read_text())
    else:
        osm_only_summary = {}

    score = rules_scorecard(
        country=country,
        cross_norm=cross_norm,
        near_pairs_df=near_pairs,
        bbox_viol_df=bbox_viol,
        overpass_hits_df=overpass_hits,
        topo=topo,
        rec_ops=rec_ops,
        rec_sols=rec_sols,
        osm_only_summary=osm_only_summary
    )
    Path(out_dir / "rules_scorecard.json").write_text(json.dumps(score, indent=2, ensure_ascii=False))

    # Summary
    summary = {
        "country": country,
        "mapping_used": {k: (str(v) if v else None) for k, v in mp.items()},
        "availability": avail,
        "ops_structural": struct,
        "sols_crossrefs_raw": cross_raw,
        "sols_crossrefs_norm": cross_norm,
        "topology_norm": topo,
        "rules_scorecard": score,
        "notes": {
            "assume_bidirectional": ASSUME_BIDIRECTIONAL,
            "min_op_distance_m": MIN_OP_DISTANCE_M,
            "use_overpass": USE_OVERPASS,
            "use_qlever": USE_QLEVER,
            "overpass_radius_m": OVERPASS_RADIUS_M,
            "max_osm_distance_m": MAX_OSM_DISTANCE_M,
            "alignment_distance_m": ALIGN_DIST_M,
            "validation_distance_m": VALIDATE_DIST_M
        }
    }
    Path(out_dir / "validation_summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False))

    print("[OK] Done. Reports in:", out_dir.resolve())
    print("- ops_extract.csv / sols_extract.csv")
    print("- ops_completeness.csv, sols_completeness.csv")
    print("- ops_empty_columns.csv, sols_empty_columns.csv")
    print("- ops_mostly_empty.csv")
    print("- ops_structural.json")
    print("- sols_crossrefs_raw.json, sols_crossrefs_norm.json")
    print("- topology.json, missing_reverse_edges.csv")
    print("- ops_bbox_violations.csv, ops_min_distance_pairs.csv")
    if USE_OVERPASS:
        print("- ops_osm_overpass.csv")
        print("- osm_nodes_bbox.csv, osm_only_nodes.csv, osm_only_summary.json")
    if USE_QLEVER:
        print("- osm_qlever_nodes.csv, osm_alignment.csv, osm_alignment_summary.json")
    print("- recency_ops.json, recency_sols.json")
    print("- rules_scorecard.json")
    print("- validation_summary.json")

if __name__ == "__main__":
    main()