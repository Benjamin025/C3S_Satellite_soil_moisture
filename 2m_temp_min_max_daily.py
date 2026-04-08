"""
ERA5 2M TEMPERATURE MAX/MIN — AFRICA DAILY-STATISTICS PIPELINE
===============================================================
Downloads ERA5 daily-statistics 2m temperature data over Africa,
month-by-month, using the `derived-era5-single-levels-daily-statistics`
CDS dataset, then aggregates to produce:

  - Monthly MAXIMUM 2m temperature  (highest daily-max value in the month)
  - Monthly MINIMUM 2m temperature  (lowest  daily-min value in the month)

WHY THE DAILY-STATISTICS PRODUCT?
----------------------------------
The dataset `derived-era5-single-levels-daily-statistics` provides:
  • maximum_2m_temperature_since_previous_post_processing  → daily TX
  • minimum_2m_temperature_since_previous_post_processing  → daily TN

These are the true daily max / min as ERA5 defines them (i.e. the max/min
of all post-processing time steps within each UTC day).  Reducing these
to monthly max/min is then trivial:
  - Monthly MAX = max  of all daily TX values in the calendar month
  - Monthly MIN = min  of all daily TN values in the calendar month

COMPARISON WITH THE HOURLY PIPELINE
-------------------------------------
The companion hourly pipeline downloads all 24 h per day and uses
np.nanmax / np.nanmin across every hourly slice.  The daily-statistics
pipeline is:
  • ~24× faster to download (one value per day instead of 24)
  • Produces equivalent monthly max/min with identical spatial coverage
  • Each monthly NetCDF is ≈6–10 MB vs ≈150–250 MB for the hourly product

Dataset   : derived-era5-single-levels-daily-statistics
Variables : maximum_2m_temperature_since_previous_post_processing  (mx2t)
            minimum_2m_temperature_since_previous_post_processing  (mn2t)
Period    : 1980 – present  (configurable)
Region    : Africa  (N=40, W=-20, S=-40, E=55)
Format    : NetCDF (daily) → aggregated GeoTIFFs (°C)
Units     : Kelvin in raw NetCDF → converted to °C on export

CDS REQUEST FORMAT — IMPORTANT DIFFERENCES FROM HOURLY
-------------------------------------------------------
The daily-statistics dataset requires specific parameters:
  "daily_statistic" : "daily_maximum" | "daily_minimum"
      → We request BOTH maximum and minimum in SEPARATE requests using:
         "variable": ["maximum_2m_temperature..."] for daily_maximum
         "variable": ["minimum_2m_temperature..."] for daily_minimum
  "format"          : "netcdf"  → More reliable than GRIB for this dataset

REQUIREMENTS
------------
    pip install cdsapi xarray rasterio numpy matplotlib tqdm

CDS CREDENTIALS
---------------
Create ~/.cdsapirc:
    url: https://cds.climate.copernicus.eu/api
    key: <YOUR-CDS-API-KEY>

OUTPUT STRUCTURE
----------------
Each month produces:
  raw_nc/<year>/era5_daily_tx_<year>_<MM>.nc   ← daily TX (°K)
  raw_nc/<year>/era5_daily_tn_<year>_<MM>.nc   ← daily TN (°K)
  geotiffs/max/<year>/era5_t2m_max_africa_<year>_<MM>.tif   ← monthly max (°C)
  geotiffs/min/<year>/era5_t2m_min_africa_<year>_<MM>.tif   ← monthly min (°C)
  previews/max|min/<year>/...preview.png
  metadata/<year>/era5_t2m_maxmin_<year>_<MM>.json
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import warnings
from datetime import datetime as dt
from pathlib import Path
from typing import Dict, List, Optional, Tuple

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

# Africa bounding box [N, W, S, E] — CDS convention
AFRICA_AREA = [40, -20, -40, 55]

# Pixel-edge extents (WSEN) matching ERA5 0.25° grid over Africa
AFRICA_BOUNDS = dict(
    left=-20.0,
    bottom=-40.0000011920928955,
    right=55.0000011175870895,
    top=40.0,
)

# CDS dataset for pre-computed daily statistics
DATASET = "derived-era5-single-levels-daily-statistics"

# Variable names exactly as CDS expects them
VAR_MAX = "maximum_2m_temperature_since_previous_post_processing"
VAR_MIN = "minimum_2m_temperature_since_previous_post_processing"

KELVIN_OFFSET = 273.15

# All days — CDS ignores any day/month combos that don't exist
ALL_DAYS = [f"{d:02d}" for d in range(1, 32)]

ERA5_START_YEAR = 1980
ERA5_END_YEAR = dt.now().year


# =============================================================================
# LOGGING
# =============================================================================

def _build_logger(log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("ERA5_Daily_MaxMin")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    fh = logging.FileHandler(
        log_dir / f"era5_daily_maxmin_{dt.now().strftime('%Y%m%d')}.log"
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
    if not HAS_CDSAPI:
        missing.append("cdsapi")
    if not HAS_XARRAY:
        missing.append("xarray")
    if not HAS_NUMPY:
        missing.append("numpy")
    if not HAS_RASTERIO:
        missing.append("rasterio")

    if missing:
        print("❌ Missing required packages:")
        for p in missing:
            print(f"   pip install {p}")
        ok = False
    else:
        print("✅ All core dependencies available")

    if not HAS_MPL:
        print("⚠️  matplotlib not found — previews will be skipped")
    if not HAS_TQDM:
        print("⚠️  tqdm not found — no progress bars")

    return ok


# =============================================================================
# MAIN WORKFLOW CLASS
# =============================================================================

class ERA5AfricaDailyMaxMinWorkflow:
    """
    Downloads ERA5 daily-statistics 2m temperature for Africa and
    aggregates to monthly maximum and minimum GeoTIFFs.

    Aggregation:
      Monthly MAX = max of all daily TX (daily-maximum t2m) in the month
      Monthly MIN = min of all daily TN (daily-minimum t2m) in the month

    Two small NetCDF files are downloaded per month (~6-10 MB each at 0.25° over Africa).
    After aggregation the NetCDF files can be deleted (keep_nc=False, default).
    """

    def __init__(
        self,
        base_dir: Optional[Path | str] = None,
        start_year: int = ERA5_START_YEAR,
        end_year: int = ERA5_END_YEAR,
        create_previews: bool = True,
        keep_nc: bool = False,
        retry_limit: int = 3,
        retry_wait: int = 60,
    ):
        self.start_year = start_year
        self.end_year = end_year
        self.create_previews = create_previews
        self.keep_nc = keep_nc
        self.retry_limit = retry_limit
        self.retry_wait = retry_wait

        # ---- Directories ---------------------------------------------------
        if base_dir is None:
            self.base_dir = (
                Path.home() / "Documents" / "Benjamin" /
                "ERA5" / "Africa" / "T2M_MaxMin_Daily"
            )
        else:
            self.base_dir = Path(base_dir)

        self.dirs = {
            "raw_nc": self.base_dir / "raw_nc",
            "geotiffs_max": self.base_dir / "geotiffs" / "max",
            "geotiffs_min": self.base_dir / "geotiffs" / "min",
            "previews_max": self.base_dir / "previews" / "max",
            "previews_min": self.base_dir / "previews" / "min",
            "metadata": self.base_dir / "metadata",
            "logs": self.base_dir / "logs",
            "status": self.base_dir / "status",
        }
        for path in self.dirs.values():
            path.mkdir(parents=True, exist_ok=True)

        # ---- Logger --------------------------------------------------------
        self.log = _build_logger(self.dirs["logs"])

        # ---- Status tracker ------------------------------------------------
        self.status_file = self.dirs["status"] / "progress.json"
        self.status = self._load_status()

        # ---- CDS client ----------------------------------------------------
        self._cds = None

        self.log.info("=" * 70)
        self.log.info("ERA5 Africa 2m Temperature — Monthly MAX/MIN (Daily Stats Pipeline)")
        self.log.info(f"Dataset    : {DATASET}")
        self.log.info(f"Method     : Daily TX/TN → monthly max/min per calendar month")
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

    def _nc_path(self, kind: str, year: int, month: int) -> Path:
        """kind: 'tx' (daily max) or 'tn' (daily min)"""
        d = self.dirs["raw_nc"] / str(year)
        d.mkdir(exist_ok=True)
        return d / f"era5_daily_{kind}_{year}_{month:02d}.nc"

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
    # BUILD CDS REQUESTS — one month, separate TX and TN requests
    # ------------------------------------------------------------------

    def _build_request(
        self, year: int, month: int, variable: str, daily_statistic: str
    ) -> dict:
        """
        Build a CDS request for the daily-statistics dataset.

        Parameters
        ----------
        variable         : VAR_MAX or VAR_MIN
        daily_statistic  : "daily_maximum" or "daily_minimum"

        IMPORTANT: This dataset requires that:
          1. variable is a list (even with one element)
          2. daily_statistic matches the variable type
          3. format="netcdf" for reliable reading
        """
        return {
            "product_type": "reanalysis",
            "variable": [variable],  # Must be a list!
            "year": str(year),
            "month": f"{month:02d}",
            "day": ALL_DAYS,
            "daily_statistic": daily_statistic,
            "time_zone": "utc+00:00",
            "frequency": "1_hourly",
            "area": AFRICA_AREA,  # [N, W, S, E]
            "format": "netcdf",  # Use NetCDF instead of GRIB (more reliable)
        }

    # ------------------------------------------------------------------
    # STEP 1 — DOWNLOAD DAILY TX OR TN NetCDF FOR ONE MONTH
    # ------------------------------------------------------------------

    def _download_one_nc(
        self,
        year: int,
        month: int,
        nc_kind: str,  # 'tx' | 'tn'
        variable: str,  # CDS variable name
        daily_statistic: str,  # 'daily_maximum' | 'daily_minimum'
        force: bool = False,
    ) -> Optional[Path]:
        """Download a single NetCDF file (TX or TN) for the given month."""
        nc_path = self._nc_path(nc_kind, year, month)
        tag = f"[{year}-{month:02d}][{nc_kind.upper()}]"

        if not force and nc_path.exists():
            self.log.info(f"{tag} NetCDF already exists — reusing")
            return nc_path

        request = self._build_request(year, month, variable, daily_statistic)
        self.log.info(f"{tag} Submitting CDS request ({daily_statistic}) …")
        self.log.debug(f"Request: {json.dumps(request, indent=2)}")

        for attempt in range(1, self.retry_limit + 1):
            try:
                client = self._get_cds_client()
                tmp_path = nc_path.with_suffix(".downloading")
                client.retrieve(DATASET, request, str(tmp_path))

                if not tmp_path.exists() or tmp_path.stat().st_size == 0:
                    raise RuntimeError("Downloaded file is empty or missing")

                tmp_path.rename(nc_path)
                size_mb = nc_path.stat().st_size / 1e6
                self.log.info(f"{tag} ✅ Downloaded {size_mb:.1f} MB → {nc_path.name}")
                return nc_path

            except Exception as exc:
                self.log.warning(f"{tag} Attempt {attempt}/{self.retry_limit} failed: {exc}")
                tmp = nc_path.with_suffix(".downloading")
                if tmp.exists():
                    tmp.unlink()
                if attempt < self.retry_limit:
                    wait = self.retry_wait * attempt
                    self.log.info(f"{tag} Waiting {wait}s before retry…")
                    time.sleep(wait)

        self.log.error(f"{tag} ❌ All {self.retry_limit} download attempts failed")
        return None

    def download_month(
        self, year: int, month: int, force: bool = False
    ) -> Tuple[Optional[Path], Optional[Path]]:
        """Download both TX and TN NetCDF files for the given month."""
        tx_path = self._download_one_nc(
            year, month, "tx", VAR_MAX, "daily_maximum", force=force
        )
        tn_path = self._download_one_nc(
            year, month, "tn", VAR_MIN, "daily_minimum", force=force
        )
        return tx_path, tn_path

    # ------------------------------------------------------------------
    # STEP 2 — NetCDF → NUMPY ARRAY
    # ------------------------------------------------------------------

    def _nc_to_array(self, nc_path: Path, tag: str) -> Tuple[Optional[np.ndarray], Optional[object], Optional[np.ndarray], Optional[np.ndarray]]:
        """
        Open a daily-statistics NetCDF file and return:
          (data_array_3d_K, transform, lats, lons)
        data_array_3d_K has shape (n_days, n_lat, n_lon) in Kelvin.
        Returns (None, None, None, None) on failure.
        """
        try:
            # Open the NetCDF file
            ds = xr.open_dataset(nc_path)

            # Find the temperature variable
            temp_var = None
            possible_names = ['t2m', '2t', 'temperature', 'mx2t', 'mn2t',
                            'maximum_2m_temperature_since_previous_post_processing',
                            'minimum_2m_temperature_since_previous_post_processing']

            for var in ds.data_vars:
                var_lower = var.lower()
                for name in possible_names:
                    if name.lower() in var_lower:
                        temp_var = var
                        break
                if temp_var:
                    break

            # If still not found, take the first data variable
            if temp_var is None and len(ds.data_vars) > 0:
                temp_var = list(ds.data_vars)[0]
                self.log.info(f"{tag} Using first variable: {temp_var}")

            if temp_var is None:
                self.log.error(f"{tag} No data variables found in {nc_path}")
                ds.close()
                return None, None, None, None

            # Get the data array
            da = ds[temp_var]

            # Handle dimensions - ensure time is first dimension
            if 'time' in da.dims:
                if da.dims[0] != 'time':
                    da = da.transpose('time', ...)
                data_k = da.values.astype(np.float32)
            elif 'valid_time' in da.dims:
                da = da.transpose('valid_time', ...)
                data_k = da.values.astype(np.float32)
            else:
                # Single time step
                data_k = da.values.astype(np.float32)[np.newaxis, ...]

            # Handle missing values
            data_k = np.where(np.abs(data_k) > 1e10, np.nan, data_k)

            # Get lat/lon coordinates
            lats = ds.coords['latitude'].values
            lons = ds.coords['longitude'].values
            n_lat, n_lon = len(lats), len(lons)

            # Check latitude orientation (should be north to south)
            if lats[0] < lats[-1]:
                # Latitude is increasing (south to north), flip it
                data_k = data_k[:, ::-1, :] if data_k.ndim == 3 else data_k[::-1, :]
                lats = lats[::-1]

            # Create transform
            transform = from_bounds(
                AFRICA_BOUNDS["left"], AFRICA_BOUNDS["bottom"],
                AFRICA_BOUNDS["right"], AFRICA_BOUNDS["top"],
                n_lon, n_lat,
            )

            n_days = data_k.shape[0]
            self.log.info(f"{tag} [NetCDF] {n_days} daily steps — grid {n_lat}×{n_lon}")

            ds.close()
            return data_k, transform, lats, lons

        except Exception as e:
            self.log.error(f"{tag} Failed to read NetCDF {nc_path.name}: {e}")
            import traceback
            self.log.debug(traceback.format_exc())
            return None, None, None, None

    # ------------------------------------------------------------------
    # STEP 3 — AGGREGATE TX/TN → MONTHLY MAX/MIN GeoTIFFs
    # ------------------------------------------------------------------

    def nc_to_maxmin_geotiffs(
        self,
        year: int,
        month: int,
        tx_path: Path,
        tn_path: Path,
    ) -> dict:
        """
        Open the two daily-statistics NetCDF files and reduce:
          Monthly MAX = np.nanmax(daily TX values)   → monthly-maximum
          Monthly MIN = np.nanmin(daily TN values)   → monthly-minimum

        Writes two GeoTIFFs (°C).  Returns {'max': Path, 'min': Path} or {}.
        """
        if not HAS_XARRAY or not HAS_RASTERIO:
            self.log.error("xarray or rasterio not available")
            return {}

        tag = f"[{year}-{month:02d}]"
        result = {}
        transform = None
        n_lon = n_lat = None

        # ---- Process TX (daily maximum → monthly max) ----------------------
        self.log.info(f"{tag} Reading daily TX NetCDF …")
        tx_k, transform, lats, lons = self._nc_to_array(tx_path, f"{tag}[TX]")
        if tx_k is None:
            self.log.error(f"{tag} TX NetCDF processing failed")
            return {}
        n_lat, n_lon = tx_k.shape[1], tx_k.shape[2]

        # Monthly MAX = max of all daily TX values in the month
        max_k = np.nanmax(tx_k, axis=0)  # (lat, lon)
        max_c = (max_k - KELVIN_OFFSET).astype("float32")
        max_c = np.where(np.isfinite(max_c), max_c, -9999.0).astype("float32")
        del tx_k

        # ---- Process TN (daily minimum → monthly min) ----------------------
        self.log.info(f"{tag} Reading daily TN NetCDF …")
        tn_k, _, _, _ = self._nc_to_array(tn_path, f"{tag}[TN]")
        if tn_k is None:
            self.log.error(f"{tag} TN NetCDF processing failed")
            return {}

        # Monthly MIN = min of all daily TN values in the month
        min_k = np.nanmin(tn_k, axis=0)  # (lat, lon)
        min_c = (min_k - KELVIN_OFFSET).astype("float32")
        min_c = np.where(np.isfinite(min_c), min_c, -9999.0).astype("float32")
        del tn_k

        # ---- Write GeoTIFFs ------------------------------------------------
        profile = {
            "driver": "GTiff",
            "dtype": "float32",
            "width": n_lon,
            "height": n_lat,
            "count": 1,
            "crs": "EPSG:4326",
            "transform": transform,
            "nodata": -9999.0,
            "compress": "lzw",
            "tiled": True,
            "blockxsize": 256,
            "blockysize": 256,
            "predictor": 2,
        }

        for kind, arr in (("max", max_c), ("min", min_c)):
            tif_path = self._tif_path(kind, year, month)
            try:
                with rasterio.open(tif_path, "w", **profile) as dst:
                    dst.write(arr, 1)
                size_kb = tif_path.stat().st_size / 1024
                self.log.info(
                    f"{tag} ✅ {kind.upper()} → "
                    f"{tif_path.name} ({size_kb:.0f} KB)"
                )
                result[kind] = tif_path

                if self.create_previews and HAS_MPL:
                    self._create_preview(
                        kind, year, month, arr, transform, n_lon, n_lat
                    )
            except Exception as exc:
                self.log.error(f"{tag} Writing {kind} TIF failed: {exc}")

        return result

    # ------------------------------------------------------------------
    # STEP 4 — VALIDATE & METADATA
    # ------------------------------------------------------------------

    def validate_month(
        self, year: int, month: int, tif_paths: dict
    ) -> dict:
        meta: dict = {
            "year": year,
            "month": f"{month:02d}",
            "generated_at": dt.utcnow().isoformat(),
            "source_dataset": DATASET,
            "method": (
                "Monthly max/min from ERA5 daily statistics. "
                "MAX = max of daily TX (maximum_2m_temperature_since_previous_post_processing); "
                "MIN = min of daily TN (minimum_2m_temperature_since_previous_post_processing). "
                "Units: °C."
            ),
        }

        for kind in ("max", "min"):
            tif_path = tif_paths.get(kind)
            if tif_path is None or not tif_path.exists():
                self.log.warning(
                    f"[{year}-{month:02d}] {kind} TIF missing — skipping stats"
                )
                continue
            try:
                with rasterio.open(tif_path) as src:
                    data = src.read(1)
                    nodata = src.nodata if src.nodata is not None else -9999.0
                    valid = data[data != nodata]
                    valid = valid[np.isfinite(valid)]

                if len(valid) == 0:
                    self.log.warning(
                        f"[{year}-{month:02d}] {kind}: no valid pixels"
                    )
                    continue

                stats = {
                    "min_degC": float(np.min(valid)),
                    "max_degC": float(np.max(valid)),
                    "mean_degC": float(np.mean(valid)),
                    "std_degC": float(np.std(valid)),
                    "valid_pixels": int(len(valid)),
                }
                meta[kind] = stats
                self.log.info(
                    f"[{year}-{month:02d}] {kind.upper():3s}  "
                    f"mean={stats['mean_degC']:.2f}°C  "
                    f"range=[{stats['min_degC']:.2f}, {stats['max_degC']:.2f}]°C"
                )
            except Exception as exc:
                self.log.error(
                    f"[{year}-{month:02d}] Validate {kind} error: {exc}"
                )

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

            left = transform.c
            top = transform.f
            right = left + transform.a * n_lon
            bottom = top + transform.e * n_lat
            extent = [left, right, bottom, top]

            cmap_name = "YlOrRd" if kind == "max" else "YlGnBu_r"
            label = (
                f"{'Maximum' if kind == 'max' else 'Minimum'} "
                f"2m Temperature (°C)"
            )

            fig, ax = plt.subplots(figsize=(9, 8))
            cmap = plt.get_cmap(cmap_name)
            cmap.set_bad("#cccccc")

            im = ax.imshow(
                plot, cmap=cmap, extent=extent,
                interpolation="nearest", aspect="auto",
                vmin=vmin, vmax=vmax,
            )
            cbar = plt.colorbar(
                im, ax=ax, pad=0.02, fraction=0.046, extend="both"
            )
            cbar.set_label(label, fontsize=11)

            month_name = calendar.month_name[month]
            ax.set_title(
                f"ERA5 {label}\nAfrica — {month_name} {year}\n"
                f"(source: daily statistics)",
                fontsize=13, fontweight="bold",
            )
            ax.set_xlabel("Longitude", fontsize=10)
            ax.set_ylabel("Latitude", fontsize=10)
            ax.grid(True, alpha=0.3, linewidth=0.5)

            plt.tight_layout()
            out = self._preview_path(kind, year, month)
            plt.savefig(out, dpi=150, bbox_inches="tight")
            plt.close(fig)
            self.log.debug(
                f"[{year}-{month:02d}] {kind} preview → {out.name}"
            )

        except Exception as exc:
            self.log.warning(
                f"[{year}-{month:02d}] Preview ({kind}) failed: {exc}"
            )

    # ------------------------------------------------------------------
    # PROCESS ONE MONTH
    # ------------------------------------------------------------------

    def process_month(
        self, year: int, month: int, force: bool = False
    ) -> bool:
        """
        Full pipeline for one calendar month:
          1. Download daily TX NetCDF (daily_maximum variable)
          2. Download daily TN NetCDF (daily_minimum variable)
          3. Aggregate to monthly max/min → write GeoTIFFs
          4. Validate + write metadata JSON
          5. Optionally delete raw NetCDF files
        Returns True on success.
        """
        tag = f"[{year}-{month:02d}]"
        self.log.info(f"\n{'─'*60}")
        self.log.info(f"Processing {year}-{month:02d}")
        self.log.info(f"{'─'*60}")

        # Skip if already complete (unless forced)
        if not force and self._is_done(year, month):
            max_tif = self._tif_path("max", year, month)
            min_tif = self._tif_path("min", year, month)
            if max_tif.exists() and min_tif.exists():
                self.log.info(f"{tag} Already done — skipping")
                return True

        # 1 & 2. Download TX and TN
        tx_path, tn_path = self.download_month(year, month, force=force)

        if tx_path is None or tn_path is None:
            self.log.error(
                f"{tag} Download failed (TX={'ok' if tx_path else '❌'}, "
                f"TN={'ok' if tn_path else '❌'})"
            )
            self._mark_failed(year, month)
            return False

        # 3. Aggregate → GeoTIFFs
        tif_paths = self.nc_to_maxmin_geotiffs(
            year, month, tx_path, tn_path
        )
        if not tif_paths:
            self.log.error(f"{tag} Aggregation produced no output")
            self._mark_failed(year, month)
            return False

        # 4. Validate + metadata
        self.validate_month(year, month, tif_paths)

        # 5. Optionally delete raw NetCDF files
        if not self.keep_nc:
            for nc_path in (tx_path, tn_path):
                if nc_path.exists():
                    nc_path.unlink()
            self.log.info(f"{tag} Daily NetCDF files deleted (keep_nc=False)")

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
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        force: bool = False,
    ) -> dict:
        sy = start_year or self.start_year
        ey = end_year or self.end_year

        self.log.info(f"\n{'='*70}")
        self.log.info(
            f"ERA5 Africa T2M Max/Min (Daily Stats) — {sy}–{ey} "
            f"({(ey - sy + 1) * 12} months total)"
        )
        self.log.info(f"{'='*70}\n")

        overall_success, overall_failed = [], []

        for year in range(sy, ey + 1):
            res = self.process_year(year, force=force)
            overall_success.extend([f"{year}-{m:02d}" for m in res["success"]])
            overall_failed.extend([f"{year}-{m:02d}" for m in res["failed"]])

        self.log.info(f"\n{'='*70}")
        self.log.info("PIPELINE COMPLETE")
        self.log.info(f"{'='*70}")
        self.log.info(f"  Successful months : {len(overall_success)}")
        self.log.info(f"  Failed    months  : {len(overall_failed)}")
        if overall_failed:
            self.log.warning(f"  Failed: {overall_failed}")

        summary = {
            "run_at": dt.utcnow().isoformat(),
            "period": f"{sy}–{ey}",
            "success": overall_success,
            "failed": overall_failed,
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
        print(
            f"{'YR-MO':>7}  {'TX NC':>8}  {'TN NC':>8}  "
            f"{'MAX TIF':>8}  {'MIN TIF':>8}  {'DONE':>6}"
        )
        print(f"{'─'*70}")
        for year in range(self.start_year, self.end_year + 1):
            for month in range(1, 13):
                tx = self._nc_path("tx", year, month).exists()
                tn = self._nc_path("tn", year, month).exists()
                maxt = self._tif_path("max", year, month).exists()
                mint = self._tif_path("min", year, month).exists()
                done = self._is_done(year, month)
                print(
                    f"{year}-{month:02d}  "
                    f"{'✅' if tx else '—':>8}  "
                    f"{'✅' if tn else '—':>8}  "
                    f"{'✅' if maxt else '—':>8}  "
                    f"{'✅' if mint else '—':>8}  "
                    f"{'✅' if done else '—':>6}"
                )
        print(f"{'─'*70}\n")


# =============================================================================
# INTERACTIVE MENU
# =============================================================================

def interactive_menu() -> None:
    print("\n" + "=" * 70)
    print("🌍  ERA5 Africa 2m Temperature MAX/MIN — Daily Statistics Pipeline")
    print("=" * 70)
    print(
        "INFO: Uses 'derived-era5-single-levels-daily-statistics'.\n"
        "      Downloads TWO small NetCDF files per month (~6–10 MB each):\n"
        "        TX = daily_maximum (max_2m_temp_since_post_processing)\n"
        "        TN = daily_minimum (min_2m_temp_since_post_processing)\n"
        "      Monthly max = max(all TX); Monthly min = min(all TN).\n"
        "      Much faster and smaller than the hourly product.\n"
    )

    sy = input(f"Start year [{ERA5_START_YEAR}]: ").strip()
    ey = input(f"End year   [{ERA5_END_YEAR}]:   ").strip()
    sy = int(sy) if sy.isdigit() else ERA5_START_YEAR
    ey = int(ey) if ey.isdigit() else ERA5_END_YEAR

    keep_nc = input("Keep raw daily NetCDF files? (y/n) [n]: ").strip().lower() == "y"
    previews = input("Create preview PNGs?  (y/n) [y]: ").strip().lower() != "n"

    wf = ERA5AfricaDailyMaxMinWorkflow(
        start_year=sy, end_year=ey,
        keep_nc=keep_nc, create_previews=previews,
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
                wf.process_year(int(y), force=True)
            else:
                print("Invalid year.")

        elif choice == "3":
            y = input("Year:  ").strip()
            m = input("Month: ").strip()
            if y.isdigit() and m.isdigit():
                wf.process_month(int(y), int(m), force=True)
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

    # Clean up any existing small/corrupt GRIB files from previous runs
    base_dir = Path.home() / "Documents" / "Benjamin" / "ERA5" / "Africa" / "T2M_MaxMin_Daily"
    if base_dir.exists():
        grib_dir = base_dir / "raw_grib"
        if grib_dir.exists():
            print("\nCleaning up old GRIB files (switching to NetCDF format)...")
            import shutil
            shutil.rmtree(grib_dir)
            print("  Removed raw_grib directory")

    print("\nSelect mode:")
    print("  1. Quick test  (single month: 2020-01)")
    print("  2. Full run    (1980–present, all months)")
    print("  3. Interactive menu")

    mode = input("Mode [3]: ").strip() or "3"

    if mode == "1":
        wf = ERA5AfricaDailyMaxMinWorkflow(start_year=2020, end_year=2020, keep_nc=False)
        wf.process_month(2020, 1, force=True)

    elif mode == "2":
        wf = ERA5AfricaDailyMaxMinWorkflow(
            start_year=ERA5_START_YEAR,
            end_year=ERA5_END_YEAR,
            keep_nc=False,
        )
        wf.run(force=True)

    else:
        interactive_menu()