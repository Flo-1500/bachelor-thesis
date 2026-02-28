# RINF Data Quality Validation Framework (Bachelor Thesis)

This repository contains the Python-based validation framework developed in the context of a bachelor thesis to assess the **data quality** and **practical usability** of the European Union Agency for Railways (ERA) **Register of Infrastructure (RINF)**.

The framework focuses on the two core RINF components:
- **Operational Points (OPs)**
- **Sections of Line (SoLs)**

It extends purely formal validation by applying rule-based checks relevant for real analytical workflows, including:
- geographical plausibility (incl. country boundary checks)
- topology / graph consistency
- semantic plausibility signals (optional OpenStreetMap/Overpass checks)
- temporal validity / recency indicators

**Scope:** Only OP and SoL are validated in this repository.

---

## Repository Contents

- `Validation.py`  
  Runs the validation rules for a given country and writes per-country outputs.
- `compare_countries.py`  
  Aggregates per-country scorecards and generates cross-country comparison plots.
- `visualise_additional.py`  
  Generates additional per-country diagnostic plots.
- `out/`  
  Created automatically; stores validation outputs.
- `plots/`  
  Created automatically; stores generated figures.

---

## Data Sources (Download Links)

### 1) RINF (ERA)

RINF is provided by the European Union Agency for Railways.

- RINF landing page (ERA):  
  https://www.era.europa.eu/domains/registers/rinf_en

- ERA data access portal (web app):  
  https://data-interop.era.europa.eu/

**What you need for this framework:**  
For each country, download/export the RINF datasets for:
- Operational Points (OP)
- Sections of Line (SoL)

In the thesis workflow, OP and SoL data were exported/downloaded as **RDF** and stored locally per country (see “Input Folder Structure”).

---

### 2) Eurostat / GISCO country boundaries (shapefile)

For accurate point-in-country checks, the framework can use the Eurostat/GISCO “Countries 2024” dataset.

- Dataset overview:  
  https://gisco-services.ec.europa.eu/distribution/v1/countries-2024.html

- Direct download (recommended for this project):  
  https://gisco-services.ec.europa.eu/distribution/v2/countries/shp/CNTR_RG_10M_2024_4326.shp.zip

After downloading, unzip the archive and place the shapefile files under `shapes/` (see below).

---

## Requirements

Recommended Python version: **3.10+**

Python packages used by the scripts:
- `pandas`
- `geopandas`
- `shapely`
- `matplotlib`
- `seaborn`
- `rdflib`
- `requests`
- `networkx`

Note: Installing GeoPandas may require OS-level dependencies (GDAL/GEOS/PROJ) depending on your platform.

---

## Installation

Create and activate a virtual environment (recommended):

python -m venv .venv

Windows:

.venv\Scripts\activate

macOS / Linux:

source .venv/bin/activate

Install dependencies (no requirements.txt required):

pip install pandas geopandas shapely matplotlib seaborn rdflib

## Input Folder Structure

This project expects a consistent local structure for OP/SoL data (RDF) per country.

Example:

	project-root/
		Validation.py
		compare_countries.py
		visualise_additional.py
		data/
	  	op_austria/
				<RDF files...>
			sol_austria/
				<RDF files...>
			op_germany
				<RDF files...>
			sol_germany/
				<RDF files...>
	  shapes/
	    CNTR_RG_10M_2024_4326.shp
	    CNTR_RG_10M_2024_4326.dbf
	    CNTR_RG_10M_2024_4326.shx
	    CNTR_RG_10M_2024_4326.prj
	    ... (other shapefile sidecar files)
	  out/        (auto-created)
	  plots/      (auto-created)

Country identifiers are expected in lowercase and must match your folder naming convention (e.g., austria, germany, croatia).

## Quick Start

Run validation for a single country:

python Validation.py <country>

Example:

	python Validation.py austria

Outputs will be written to:

	out/austria/

Aggregate and compare multiple countries (after validating several countries):

	python compare_countries.py

This generates cross-country plots (typically) under:

	plots/countries/

Generate additional per-country diagnostic plots:

	python visualise_additional.py

This generates additional plots under:

	plots/

## Notes on Optional External Checks (OSM / Overpass)

Some rules may query OpenStreetMap via the Overpass API, depending on the configuration inside the scripts. 

**Important:** In the current scripts, Overpass checks are enabled by default (e.g., USE_OVERPASS = True in Validation.py).

This can be slow or temporarily unavailable, and results may vary due to OSM updates. If you require strict reproducibility, disable Overpass-dependent rules or implement caching for Overpass responses.

## License

Licensed under the MIT License.

## Acknowledgements

- ERA RINF: European Union Agency for Railways (ERA)
- Country boundaries: Eurostat / GISCO
- OpenStreetMap contributors (if OSM checks are enabled)

