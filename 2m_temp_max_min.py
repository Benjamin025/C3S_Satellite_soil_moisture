"""
ERA5 2M TEMPERATURE MAX/MIN — AFRICA DOWNLOAD PIPELINE
=======================================================
Downloads ERA5 hourly 2m temperature data over Africa month-by-month,
then aggregates to produce:
  - Monthly MAXIMUM 2m temperature  (highest hourly value in the month)
  - Monthly MINIMUM 2m temperature  (lowest  hourly value in the month)

This exactly replicates the methodology used by Google Earth Engine in the
ECMWF/ERA5/MONTHLY dataset, which states:
  "monthly minimum and maximum air temperature at 2m has been calculated
   based on the hourly 2m air temperature data."

Dataset  : reanalysis-era5-single-levels
Variable : 2m_temperature (t2m)
Period   : 1980 – present  (configurable)
Region   : Africa  (N=40, W=-20, S=-40, E=55)
Format   : GRIB (hourly) → aggregated GeoTIFFs (°C)
Units    : Kelvin in raw GRIB → converted to °C on export

WHY HOURLY AND NOT THE MONTHLY-MEANS PRODUCT?
---------------------------------------------
The CDS monthly-means product (`reanalysis-era5-single-levels-monthly-means`)
only provides the TIME-AVERAGED mean temperature. It does NOT provide monthly
max/min. To get true monthly max/min you must download the full hourly data
and reduce it yourself — exactly as GEE does.

STRATEGY
--------
Requests are chunked by MONTH (one request per month) to keep GRIB file
sizes manageable (~150–200 MB per month at 0.25° over Africa) and to allow
resumable, parallel-friendly downloads.

Each month produces:
  - One raw hourly GRIB        (raw_grib/<year>/<month>/era5_t2m_hourly_<year>_<MM>.grib)
  - One MAX GeoTIFF  in °C     (geotiffs/max/<year>/era5_t2m_max_africa_<year>_<MM>.tif)
  - One MIN GeoTIFF  in °C     (geotiffs/min/<year>/era5_t2m_min_africa_<year>_<MM>.tif)
  - Two preview PNGs           (previews/max|min/<year>/...)
  - One metadata JSON          (metadata/<year>/era5_t2m_maxmin_<year>_<MM>.json)

REQUIREMENTS
------------
    pip install cdsapi cfgrib xarray rasterio numpy matplotlib tqdm

CDS CREDENTIALS
---------------
Create ~/.cdsapirc:
    url: https://cds.climate.copernicus.eu/api
    key: <YOUR-CDS-API-KEY>
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
import traceback
import warnings
from datetime import datetime as dt
from pathlib import Path
from typing import List, Optional

warnings.filterwarnings("ignore")

# ---------------------------------------------------------------------------
# Optional heavy dependencies — checked at runtime
# ---------------------------------------------------------------------------
try:
    import cdsapi
    HAS_CDSAPI = True
except ImportError:
    HAS_CDSAPI = False

try:
    import cfgrib
    import xarray as xr
    HAS_XARRAY = True
except ImportError:
    HAS_XARRAY = False

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False

try:
    import rasterio
    from rasterio.transform import from_bounds
    HAS_RASTERIO = True
except ImportError:
    HAS_RASTERIO = False

try:
    import matplotlib.pyplot as plt
    HAS_MPL = True
except ImportError:
    HAS_MPL = False

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False


# =============================================================================
# CONFIGURATION
# =============================================================================

# Africa bounding box [N, W, S, E] — CDS convention (integer degrees)
AFRICA_AREA = [40, -20, -40, 55]

# Precise pixel-edge extents (WSEN) matching the actual ERA5 0.25° grid
AFRICA_BOUNDS = dict(
    left=-20.0,
    bottom=-40.0000011920928955,
    right=55.0000011175870895,
    top=40.0,
)

DATASET  = "reanalysis-era5-single-levels"      # HOURLY product
VARIABLE = "2m_temperature"
KELVIN_OFFSET = 273.15                           # K → °C

# All 24 hours of the day
ALL_HOURS = [f"{h:02d}:00" for h in range(24)]  # 00:00 … 23:00

# All days per month (CDS ignores invalid day/month combos automatically)
ALL_DAYS = [f"{d:02d}" for d in range(1, 32)]

ERA5_START_YEAR = 1980
ERA5_END_YEAR   = dt.now().year


# =============================================================================
# LOGGING
# =============================================================================

def _build_logger(log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("ERA5_MaxMin")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    fh = logging.FileHandler(
        log_dir / f"era5_maxmin_{dt.now().strftime('%Y%m%d')}.log"
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    if not logger.handlers:
        logger.addHandler(ch)
        logger.addHandler(fh)

    return logger


# =============================================================================
# DEPENDENCY CHECK
# =============================================================================

def check_dependencies() -> bool:
    ok = True
    missing = []
    if not HAS_CDSAPI:   missing.append("cdsapi")
    if not HAS_XARRAY:   missing.append("cfgrib xarray")
    if not HAS_NUMPY:    missing.append("numpy")
    if not HAS_RASTERIO: missing.append("rasterio")

    if missing:
        print("❌ Missing required packages:")
        for p in missing:
            print(f"   pip install {p}")
        ok = False
    else:
        print("✅ All core dependencies available")

    if not HAS_MPL:   print("⚠️  matplotlib not found — previews will be skipped")
    if not HAS_TQDM:  print("⚠️  tqdm not found — no progress bars")
    return ok


# =============================================================================
# MAIN WORKFLOW CLASS
# =============================================================================

class ERA5AfricaMaxMinWorkflow:
    """
    Downloads ERA5 hourly 2m temperature for Africa and aggregates to
    monthly maximum and minimum GeoTIFFs.

    Aggregation method (replicating GEE ECMWF/ERA5/MONTHLY):
      - Monthly MAX = max  of all hourly t2m values in the calendar month
      - Monthly MIN = min  of all hourly t2m values in the calendar month

    One GRIB per month is downloaded (all 24 hours × all days).
    The raw GRIB is then reduced in memory and two GeoTIFFs are written.
    Raw GRIBs can optionally be deleted after conversion to save disk space.
    """

    def __init__(
        self,
        base_dir: Optional[Path | str] = None,
        start_year:      int  = ERA5_START_YEAR,
        end_year:        int  = ERA5_END_YEAR,
        create_previews: bool = True,
        keep_grib:       bool = False,       # hourly GRIBs are large — default off
        retry_limit:     int  = 3,
        retry_wait:      int  = 60,
    ):
        self.start_year      = start_year
        self.end_year        = end_year
        self.create_previews = create_previews
        self.keep_grib       = keep_grib
        self.retry_limit     = retry_limit
        self.retry_wait      = retry_wait

        # ---- Directories ---------------------------------------------------
        if base_dir is None:
            self.base_dir = (
                Path.home() / "Documents" / "Benjamin" /
                "ERA5" / "Africa" / "T2M_MaxMin"
            )
        else:
            self.base_dir = Path(base_dir)

        self.dirs = {
            "raw_grib":    self.base_dir / "raw_grib",
            "geotiffs_max": self.base_dir / "geotiffs" / "max",
            "geotiffs_min": self.base_dir / "geotiffs" / "min",
            "previews_max": self.base_dir / "previews"  / "max",
            "previews_min": self.base_dir / "previews"  / "min",
            "metadata":    self.base_dir / "metadata",
            "logs":        self.base_dir / "logs",
            "status":      self.base_dir / "status",
        }
        for path in self.dirs.values():
            path.mkdir(parents=True, exist_ok=True)

        # ---- Logger --------------------------------------------------------
        self.log = _build_logger(self.dirs["logs"])

        # ---- Status tracker ------------------------------------------------
        self.status_file = self.dirs["status"] / "progress.json"
        self.status      = self._load_status()

        # ---- CDS client ----------------------------------------------------
        self._cds = None

        self.log.info("=" * 70)
        self.log.info("ERA5 Africa 2m Temperature — Monthly MAX / MIN Pipeline")
        self.log.info(f"Method     : Hourly t2m → max/min per calendar month")
        self.log.info(f"Period     : {start_year} – {end_year}")
        self.log.info(f"Region     : N={AFRICA_AREA[0]} W={AFRICA_AREA[1]} "
                      f"S={AFRICA_AREA[2]} E={AFRICA_AREA[3]}")
        self.log.info(f"Output dir : {self.base_dir}")
        self.log.info("=" * 70)

    # ------------------------------------------------------------------
    # STATUS TRACKER
    # ------------------------------------------------------------------

    def _load_status(self) -> dict:
        if self.status_file.exists():
            try:
                with open(self.status_file) as f:
                    return json.load(f)
            except Exception:
                pass
        return {"done": [], "failed": []}

    def _save_status(self) -> None:
        with open(self.status_file, "w") as f:
            json.dump(self.status, f, indent=2)

    def _key(self, year: int, month: int) -> str:
        return f"{year}-{month:02d}"

    def _is_done(self, year: int, month: int) -> bool:
        return self._key(year, month) in self.status["done"]

    def _mark_done(self, year: int, month: int) -> None:
        k = self._key(year, month)
        if k not in self.status["done"]:
            self.status["done"].append(k)
        if k in self.status["failed"]:
            self.status["failed"].remove(k)
        self._save_status()

    def _mark_failed(self, year: int, month: int) -> None:
        k = self._key(year, month)
        if k not in self.status["failed"]:
            self.status["failed"].append(k)
        self._save_status()

    # ------------------------------------------------------------------
    # PATH HELPERS
    # ------------------------------------------------------------------

    def _grib_path(self, year: int, month: int) -> Path:
        d = self.dirs["raw_grib"] / str(year)
        d.mkdir(exist_ok=True)
        return d / f"era5_t2m_hourly_{year}_{month:02d}.grib"

    def _tif_path(self, kind: str, year: int, month: int) -> Path:
        """kind: 'max' or 'min'"""
        d = self.dirs[f"geotiffs_{kind}"] / str(year)
        d.mkdir(exist_ok=True)
        return d / f"era5_t2m_{kind}_africa_{year}_{month:02d}.tif"

    def _preview_path(self, kind: str, year: int, month: int) -> Path:
        d = self.dirs[f"previews_{kind}"] / str(year)
        d.mkdir(exist_ok=True)
        return d / f"era5_t2m_{kind}_africa_{year}_{month:02d}_preview.png"

    def _meta_path(self, year: int, month: int) -> Path:
        d = self.dirs["metadata"] / str(year)
        d.mkdir(exist_ok=True)
        return d / f"era5_t2m_maxmin_{year}_{month:02d}.json"

    # ------------------------------------------------------------------
    # CDS CLIENT
    # ------------------------------------------------------------------

    def _get_cds_client(self):
        if self._cds is None:
            self.log.info("Initialising CDS API client…")
            self._cds = cdsapi.Client(quiet=False, progress=True)
        return self._cds

    # ------------------------------------------------------------------
    # BUILD CDS REQUEST  — one month of hourly data
    # ------------------------------------------------------------------

    def _build_request(self, year: int, month: int) -> dict:
        return {
            "product_type": "reanalysis",
            "variable":     VARIABLE,
            "year":         str(year),
            "month":        f"{month:02d}",
            "day":          ALL_DAYS,
            "time":         ALL_HOURS,
            "data_format":  "grib",
            "download_format": "unarchived",
            "area":         AFRICA_AREA,     # [N, W, S, E]
        }

    # ------------------------------------------------------------------
    # STEP 1 — DOWNLOAD ONE MONTH OF HOURLY GRIB
    # ------------------------------------------------------------------

    def download_month(
        self, year: int, month: int, force: bool = False
    ) -> Optional[Path]:
        grib_path = self._grib_path(year, month)

        # Already on disk?
        if not force and grib_path.exists() and grib_path.stat().st_size > 0:
            size_mb = grib_path.stat().st_size / 1e6
            self.log.info(
                f"[{year}-{month:02d}] GRIB on disk ({size_mb:.0f} MB) — reusing"
            )
            return grib_path

        request = self._build_request(year, month)
        self.log.info(f"[{year}-{month:02d}] Submitting hourly CDS request …")
        self.log.debug(f"Request: {json.dumps(request, indent=2)}")

        for attempt in range(1, self.retry_limit + 1):
            try:
                client   = self._get_cds_client()
                tmp_path = grib_path.with_suffix(".downloading")
                client.retrieve(DATASET, request, str(tmp_path))

                if not tmp_path.exists() or tmp_path.stat().st_size == 0:
                    raise RuntimeError("Downloaded file is empty or missing")

                tmp_path.rename(grib_path)
                size_mb = grib_path.stat().st_size / 1e6
                self.log.info(
                    f"[{year}-{month:02d}] ✅ Downloaded {size_mb:.0f} MB → {grib_path.name}"
                )
                return grib_path

            except Exception as exc:
                self.log.warning(
                    f"[{year}-{month:02d}] Attempt {attempt}/{self.retry_limit} failed: {exc}"
                )
                tmp = grib_path.with_suffix(".downloading")
                if tmp.exists():
                    tmp.unlink()
                if attempt < self.retry_limit:
                    wait = self.retry_wait * attempt
                    self.log.info(f"[{year}-{month:02d}] Waiting {wait}s …")
                    time.sleep(wait)

        self.log.error(
            f"[{year}-{month:02d}] ❌ All {self.retry_limit} attempts failed"
        )
        return None

    # ------------------------------------------------------------------
    # STEP 2 — GRIB → MAX & MIN GeoTIFFs
    # ------------------------------------------------------------------

    def grib_to_maxmin_geotiffs(
        self, year: int, month: int, grib_path: Path
    ) -> dict:
        """
        Open the hourly GRIB, reduce over the time dimension:
          - MAX  = np.nanmax across all hourly slices  → monthly max
          - MIN  = np.nanmin across all hourly slices  → monthly min

        Writes two GeoTIFFs (°C) and returns their paths as a dict.
        Returns {} on failure.
        """
        if not HAS_XARRAY or not HAS_RASTERIO:
            self.log.error("cfgrib/xarray or rasterio not available")
            return {}

        self.log.info(f"[{year}-{month:02d}] Opening hourly GRIB …")

        try:
            # cfgrib.open_datasets returns a list — one dataset per
            # unique combination of GRIB edition / level type / etc.
            datasets = cfgrib.open_datasets(str(grib_path))
        except Exception as exc:
            self.log.error(f"[{year}-{month:02d}] GRIB open failed: {exc}")
            traceback.print_exc()
            return {}

        # Locate the dataset that holds t2m
        ds_t2m = None
        for ds in datasets:
            if "t2m" in ds:
                ds_t2m = ds
                break

        if ds_t2m is None:
            self.log.error(f"[{year}-{month:02d}] 't2m' not found in GRIB")
            for ds in datasets:
                ds.close()
            return {}

        # Identify the time dimension name
        time_dim = None
        for candidate in ("valid_time", "time", "step"):
            if candidate in ds_t2m.dims:
                time_dim = candidate
                break

        if time_dim is None:
            self.log.error(
                f"[{year}-{month:02d}] Cannot identify time dimension. "
                f"Available dims: {list(ds_t2m.dims)}"
            )
            for ds in datasets:
                ds.close()
            return {}

        t2m_da = ds_t2m["t2m"]
        n_steps = t2m_da.sizes[time_dim]
        self.log.info(
            f"[{year}-{month:02d}] Loaded {n_steps} hourly steps — "
            f"reducing to max/min …"
        )

        lats = ds_t2m["latitude"].values
        lons = ds_t2m["longitude"].values
        n_lat, n_lon = len(lats), len(lons)
        lat_desc = lats[0] > lats[-1]   # ERA5: latitudes are top→bottom (desc)

        # ---- Build rasterio transform from precise Africa extents ----------
        transform = from_bounds(
            AFRICA_BOUNDS["left"], AFRICA_BOUNDS["bottom"],
            AFRICA_BOUNDS["right"], AFRICA_BOUNDS["top"],
            n_lon, n_lat,
        )

        # ---- Load full 3-D array (time, lat, lon) into memory --------------
        # For one month of hourly ERA5 at 0.25° over Africa this is
        # ~720 steps × 320 lat × 300 lon × 4 bytes ≈ ~275 MB — manageable.
        self.log.info(f"[{year}-{month:02d}] Loading all hourly values into memory …")
        data_k = t2m_da.values.astype("float32")   # shape: (time, lat, lon)

        # Mask fill/missing values
        fill_val = getattr(t2m_da, "_FillValue", 9.999e20)
        data_k[data_k >= fill_val * 0.1] = np.nan

        # Ensure latitude axis is top→bottom (descending) for rasterio
        if not lat_desc:
            data_k = data_k[:, ::-1, :]

        # ---- AGGREGATION ---------------------------------------------------
        # GEE method: max / min across ALL hourly values in the month
        max_k = np.nanmax(data_k, axis=0)   # shape: (lat, lon)
        min_k = np.nanmin(data_k, axis=0)   # shape: (lat, lon)

        # K → °C
        max_c = max_k - KELVIN_OFFSET
        min_c = min_k - KELVIN_OFFSET

        # Replace remaining NaN with standard nodata sentinel
        max_c = np.where(np.isfinite(max_c), max_c, -9999.0).astype("float32")
        min_c = np.where(np.isfinite(min_c), min_c, -9999.0).astype("float32")

        # Free the large 3-D array immediately
        del data_k

        # ---- Write GeoTIFFs ------------------------------------------------
        profile = {
            "driver":     "GTiff",
            "dtype":      "float32",
            "width":      n_lon,
            "height":     n_lat,
            "count":      1,
            "crs":        "EPSG:4326",
            "transform":  transform,
            "nodata":     -9999.0,
            "compress":   "lzw",
            "tiled":      True,
            "blockxsize": 256,
            "blockysize": 256,
            "predictor":  2,
        }

        result = {}

        for kind, arr in (("max", max_c), ("min", min_c)):
            tif_path = self._tif_path(kind, year, month)
            try:
                with rasterio.open(tif_path, "w", **profile) as dst:
                    dst.write(arr, 1)
                size_kb = tif_path.stat().st_size / 1024
                self.log.info(
                    f"[{year}-{month:02d}] ✅ {kind.upper()} → "
                    f"{tif_path.name} ({size_kb:.0f} KB)"
                )
                result[kind] = tif_path

                if self.create_previews and HAS_MPL:
                    self._create_preview(kind, year, month, arr, transform, n_lon, n_lat)

            except Exception as exc:
                self.log.error(
                    f"[{year}-{month:02d}] Writing {kind} TIF failed: {exc}"
                )

        # Close datasets
        for ds in datasets:
            ds.close()

        return result

    # ------------------------------------------------------------------
    # STEP 3 — VALIDATE & METADATA
    # ------------------------------------------------------------------

    def validate_month(
        self, year: int, month: int, tif_paths: dict
    ) -> dict:
        """
        Read both GeoTIFFs, compute statistics, write metadata JSON.
        tif_paths: {'max': Path, 'min': Path}
        """
        meta: dict = {
            "year": year, "month": f"{month:02d}",
            "generated_at": dt.utcnow().isoformat(),
            "method": (
                "Monthly max/min aggregated from ERA5 hourly 2m temperature. "
                "MAX = max of all hourly values; MIN = min of all hourly values. "
                "Replicates GEE ECMWF/ERA5/MONTHLY methodology."
            ),
        }

        for kind in ("max", "min"):
            tif_path = tif_paths.get(kind)
            if tif_path is None or not tif_path.exists():
                self.log.warning(f"[{year}-{month:02d}] {kind} TIF missing — skip")
                continue
            try:
                with rasterio.open(tif_path) as src:
                    data   = src.read(1)
                    nodata = src.nodata if src.nodata is not None else -9999.0
                    valid  = data[data != nodata]
                    valid  = valid[np.isfinite(valid)]

                if len(valid) == 0:
                    self.log.warning(f"[{year}-{month:02d}] {kind}: no valid pixels")
                    continue

                stats = {
                    "min_degC":     float(np.min(valid)),
                    "max_degC":     float(np.max(valid)),
                    "mean_degC":    float(np.mean(valid)),
                    "std_degC":     float(np.std(valid)),
                    "valid_pixels": int(len(valid)),
                }
                meta[kind] = stats
                self.log.info(
                    f"[{year}-{month:02d}] {kind.upper():3s}  "
                    f"mean={stats['mean_degC']:.2f}°C  "
                    f"range=[{stats['min_degC']:.2f}, {stats['max_degC']:.2f}]°C"
                )
            except Exception as exc:
                self.log.error(f"[{year}-{month:02d}] Validate {kind} error: {exc}")

        with open(self._meta_path(year, month), "w") as f:
            json.dump(meta, f, indent=2)

        return meta

    # ------------------------------------------------------------------
    # PREVIEW
    # ------------------------------------------------------------------

    def _create_preview(
        self,
        kind: str,
        year: int,
        month: int,
        arr: "np.ndarray",
        transform,
        n_lon: int,
        n_lat: int,
    ) -> None:
        import calendar

        try:
            plot = arr.copy().astype("float32")
            plot[plot == -9999.0] = np.nan
            plot[~np.isfinite(plot)] = np.nan

            vmin = np.nanpercentile(plot, 2)
            vmax = np.nanpercentile(plot, 98)

            # Reconstruct extent from transform
            left   = transform.c
            top    = transform.f
            right  = left + transform.a * n_lon
            bottom = top  + transform.e * n_lat
            extent = [left, right, bottom, top]

            cmap_name = "YlOrRd" if kind == "max" else "YlGnBu_r"
            label     = f"{'Maximum' if kind == 'max' else 'Minimum'} 2m Temperature (°C)"

            fig, ax = plt.subplots(figsize=(9, 8))
            cmap = plt.get_cmap(cmap_name)
            cmap.set_bad("#cccccc")

            im = ax.imshow(
                plot, cmap=cmap, extent=extent,
                interpolation="nearest", aspect="auto",
                vmin=vmin, vmax=vmax,
            )
            cbar = plt.colorbar(im, ax=ax, pad=0.02, fraction=0.046, extend="both")
            cbar.set_label(label, fontsize=11)

            month_name = calendar.month_name[month]
            ax.set_title(
                f"ERA5 {label}\nAfrica — {month_name} {year}",
                fontsize=13, fontweight="bold",
            )
            ax.set_xlabel("Longitude", fontsize=10)
            ax.set_ylabel("Latitude",  fontsize=10)
            ax.grid(True, alpha=0.3, linewidth=0.5)

            plt.tight_layout()
            out = self._preview_path(kind, year, month)
            plt.savefig(out, dpi=150, bbox_inches="tight")
            plt.close(fig)
            self.log.debug(f"[{year}-{month:02d}] {kind} preview → {out.name}")

        except Exception as exc:
            self.log.warning(f"[{year}-{month:02d}] Preview ({kind}) failed: {exc}")

    # ------------------------------------------------------------------
    # PROCESS ONE MONTH
    # ------------------------------------------------------------------

    def process_month(
        self, year: int, month: int, force: bool = False
    ) -> bool:
        """
        Full pipeline for one calendar month:
          download hourly GRIB → aggregate max/min → write GeoTIFFs → validate.
        Returns True on success.
        """
        tag = f"[{year}-{month:02d}]"
        self.log.info(f"\n{'─'*60}")
        self.log.info(f"Processing {year}-{month:02d}")
        self.log.info(f"{'─'*60}")

        # Skip if already done (unless force)
        if not force and self._is_done(year, month):
            max_tif = self._tif_path("max", year, month)
            min_tif = self._tif_path("min", year, month)
            if max_tif.exists() and min_tif.exists():
                self.log.info(f"{tag} Already done — skipping")
                return True

        # 1. Download
        grib_path = self.download_month(year, month, force=force)
        if grib_path is None:
            self._mark_failed(year, month)
            return False

        # 2. Aggregate → GeoTIFFs
        tif_paths = self.grib_to_maxmin_geotiffs(year, month, grib_path)
        if not tif_paths:
            self.log.error(f"{tag} Aggregation produced no output")
            self._mark_failed(year, month)
            return False

        # 3. Validate + metadata
        self.validate_month(year, month, tif_paths)

        # 4. Optionally remove large hourly GRIB
        if not self.keep_grib and grib_path.exists():
            grib_path.unlink()
            self.log.info(f"{tag} Hourly GRIB deleted (keep_grib=False)")

        self._mark_done(year, month)
        self.log.info(f"{tag} ✅ Complete")
        return True

    # ------------------------------------------------------------------
    # PROCESS A FULL YEAR
    # ------------------------------------------------------------------

    def process_year(self, year: int, force: bool = False) -> dict:
        results = {"success": [], "failed": []}
        months_iter = range(1, 13)
        if HAS_TQDM:
            months_iter = tqdm(months_iter, desc=f"Year {year}", unit="mo")
        for month in months_iter:
            ok = self.process_month(year, month, force=force)
            (results["success"] if ok else results["failed"]).append(month)
        return results

    # ------------------------------------------------------------------
    # FULL PIPELINE RUN
    # ------------------------------------------------------------------

    def run(
        self,
        start_year:     Optional[int] = None,
        end_year:       Optional[int] = None,
        force:          bool = False,
    ) -> dict:
        sy = start_year or self.start_year
        ey = end_year   or self.end_year

        self.log.info(f"\n{'='*70}")
        self.log.info(
            f"ERA5 Africa T2M Max/Min — {sy}–{ey} "
            f"({(ey - sy + 1) * 12} months total)"
        )
        self.log.info(f"{'='*70}\n")

        overall_success, overall_failed = [], []

        for year in range(sy, ey + 1):
            res = self.process_year(year, force=force)
            overall_success.extend([f"{year}-{m:02d}" for m in res["success"]])
            overall_failed.extend( [f"{year}-{m:02d}" for m in res["failed"]])

        self.log.info(f"\n{'='*70}")
        self.log.info("PIPELINE COMPLETE")
        self.log.info(f"{'='*70}")
        self.log.info(f"  Successful months : {len(overall_success)}")
        self.log.info(f"  Failed    months  : {len(overall_failed)}")
        if overall_failed:
            self.log.warning(f"  Failed: {overall_failed}")

        summary = {
            "run_at":  dt.utcnow().isoformat(),
            "period":  f"{sy}–{ey}",
            "success": overall_success,
            "failed":  overall_failed,
        }
        summary_path = (
            self.dirs["status"] /
            f"run_summary_{dt.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        with open(summary_path, "w") as f:
            json.dump(summary, f, indent=2)
        self.log.info(f"  Summary: {summary_path}")
        return summary

    # ------------------------------------------------------------------
    # INVENTORY
    # ------------------------------------------------------------------

    def inventory(self) -> None:
        print(f"\n{'─'*70}")
        print(f"{'YR-MO':>7}  {'GRIB':>8}  {'MAX TIF':>8}  {'MIN TIF':>8}  {'DONE':>6}")
        print(f"{'─'*70}")
        for year in range(self.start_year, self.end_year + 1):
            for month in range(1, 13):
                grib  = self._grib_path(year, month).exists()
                maxt  = self._tif_path("max", year, month).exists()
                mint  = self._tif_path("min", year, month).exists()
                done  = self._is_done(year, month)
                print(
                    f"{year}-{month:02d}  "
                    f"{'✅' if grib  else '—':>8}  "
                    f"{'✅' if maxt  else '—':>8}  "
                    f"{'✅' if mint  else '—':>8}  "
                    f"{'✅' if done  else '—':>6}"
                )
        print(f"{'─'*70}\n")


# =============================================================================
# INTERACTIVE MENU
# =============================================================================

def interactive_menu() -> None:
    print("\n" + "=" * 70)
    print("🌍  ERA5 Africa 2m Temperature MAX/MIN — Interactive Menu")
    print("=" * 70)
    print(
        "NOTE: This downloads HOURLY ERA5 data and aggregates it.\n"
        "      Each month is ~150–250 MB of GRIB before aggregation.\n"
        "      With keep_grib=False (default), only the small GeoTIFFs\n"
        "      (~50–100 KB each) are kept on disk after processing.\n"
    )

    sy = input(f"Start year [{ERA5_START_YEAR}]: ").strip()
    ey = input(f"End year   [{ERA5_END_YEAR}]:   ").strip()
    sy = int(sy) if sy.isdigit() else ERA5_START_YEAR
    ey = int(ey) if ey.isdigit() else ERA5_END_YEAR

    keep_grib = input("Keep raw hourly GRIBs? (y/n) [n]: ").strip().lower() == "y"
    previews  = input("Create preview PNGs?   (y/n) [y]: ").strip().lower() != "n"

    wf = ERA5AfricaMaxMinWorkflow(
        start_year=sy, end_year=ey,
        keep_grib=keep_grib, create_previews=previews,
    )

    while True:
        print("\n📋 MENU")
        print("  1. Run full pipeline (all years/months)")
        print("  2. Process a single year")
        print("  3. Process a single month")
        print("  4. Show disk inventory")
        print("  5. Exit")

        choice = input("Choice: ").strip()

        if choice == "1":
            force = input("Force re-process? (y/n) [n]: ").strip().lower() == "y"
            wf.run(force=force)

        elif choice == "2":
            y = input("Year: ").strip()
            if y.isdigit():
                wf.process_year(int(y))
            else:
                print("Invalid year.")

        elif choice == "3":
            y = input("Year:  ").strip()
            m = input("Month: ").strip()
            if y.isdigit() and m.isdigit():
                wf.process_month(int(y), int(m))
            else:
                print("Invalid input.")

        elif choice == "4":
            wf.inventory()

        elif choice == "5":
            print("Goodbye!")
            break
        else:
            print("Invalid choice.")


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    if not check_dependencies():
        sys.exit(1)

    print("\nSelect mode:")
    print("  1. Quick test  (single month: 2020-01)")
    print("  2. Full run    (1980–present, all months)")
    print("  3. Interactive menu")

    mode = input("Mode [3]: ").strip() or "3"

    if mode == "1":
        wf = ERA5AfricaMaxMinWorkflow(start_year=2020, end_year=2020)
        wf.process_month(2020, 1)

    elif mode == "2":
        wf = ERA5AfricaMaxMinWorkflow(
            start_year=ERA5_START_YEAR,
            end_year=ERA5_END_YEAR,
            keep_grib=False,
        )
        wf.run()

    else:
        interactive_menu()