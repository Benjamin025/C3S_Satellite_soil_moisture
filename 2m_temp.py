"""
ERA5 2M TEMPERATURE — AFRICA DOWNLOAD PIPELINE
===============================================
Production-grade automation for ERA5 monthly 2m temperature data over Africa.

Dataset  : reanalysis-era5-single-levels-monthly-means
Variable : 2m_temperature (t2m)
Period   : 1940 – present
Region   : Africa  (N=37, W=-25, S=-34, E=51)
Format   : GRIB → converted to GeoTIFF (°C)
Units    : Kelvin in raw GRIB → converted to °C on export

REQUIREMENTS
------------
    pip install cdsapi cfgrib xarray rasterio numpy matplotlib tqdm

CDS CREDENTIALS
---------------
Create ~/.cdsapirc with:
    url: https://cds.climate.copernicus.eu/api
    key: <YOUR-CDS-API-KEY>

Or set environment variables:
    export CDSAPI_URL=https://cds.climate.copernicus.eu/api
    export CDSAPI_KEY=<YOUR-CDS-API-KEY>

STRATEGY
--------
ERA5 monthly requests are chunked by YEAR to stay well within CDS size limits
and to allow resumable downloads. Each year produces:
  - One raw GRIB file   (raw_grib/<year>/era5_t2m_africa_<year>.grib)
  - 12 GeoTIFFs in °C  (geotiffs/<year>/era5_t2m_africa_<year>_<MM>.tif)
  - 12 preview PNGs     (previews/<year>/...)
  - One metadata file   (metadata/<year>/...)
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
from typing import List, Optional, Tuple

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
    import matplotlib.colors as mcolors
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

# Precise rasterio bounds (W, S, E, N) matching the actual grid pixel edges
# WSEN: -20.0, -40.0000011920928955, 55.0000011175870895, 40.0
AFRICA_BOUNDS = dict(
    left=-20.0,
    bottom=-40.0000011920928955,
    right=55.0000011175870895,
    top=40.0,
)

DATASET   = "reanalysis-era5-single-levels-monthly-means"
VARIABLE  = "2m_temperature"
PRODUCT   = "monthly_averaged_reanalysis"
TIME_STEP = "00:00"

# Years available in ERA5
ERA5_START_YEAR = 1980
ERA5_END_YEAR   = dt.now().year          # always up-to-date

KELVIN_OFFSET   = 273.15                 # K → °C

# CDS allows up to 1 000 fields per request; 12 months/year is always safe.
CHUNK_SIZE_YEARS = 1                     # download 1 year at a time (safest)


# =============================================================================
# LOGGING
# =============================================================================

def _build_logger(log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("ERA5_Africa")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    # File handler — rotating daily name
    fh = logging.FileHandler(
        log_dir / f"era5_t2m_{dt.now().strftime('%Y%m%d')}.log"
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
        missing.append("cfgrib xarray")
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

class ERA5AfricaT2MWorkflow:
    """
    Production ERA5 monthly 2m-temperature pipeline for Africa.

    Workflow per year
    -----------------
    1. Submit CDS request (12 months, Africa bbox)
    2. Download GRIB to  raw_grib/<year>/
    3. Open with cfgrib
    4. Convert K → °C
    5. Write one GeoTIFF per month to geotiffs/<year>/
    6. Validate & create metadata + preview PNG
    """

    # ------------------------------------------------------------------
    def __init__(
        self,
        base_dir: Optional[Path | str] = None,
        start_year: int = ERA5_START_YEAR,
        end_year:   int = ERA5_END_YEAR,
        create_previews: bool = True,
        keep_grib: bool = True,
        retry_limit: int = 3,
        retry_wait:  int = 60,        # seconds between CDS retries
    ):
        self.start_year      = start_year
        self.end_year        = end_year
        self.create_previews = create_previews
        self.keep_grib       = keep_grib
        self.retry_limit     = retry_limit
        self.retry_wait      = retry_wait

        # ---- Directories ---------------------------------------------------
        if base_dir is None:
            self.base_dir = Path.home() / "Documents" / "ERA5" / "Africa" / "T2M"
        else:
            self.base_dir = Path(base_dir)

        self.dirs = {
            "raw_grib": self.base_dir / "raw_grib",
            "geotiffs": self.base_dir / "geotiffs",
            "previews": self.base_dir / "previews",
            "metadata": self.base_dir / "metadata",
            "logs":     self.base_dir / "logs",
            "status":   self.base_dir / "status",   # JSON progress tracker
        }
        for path in self.dirs.values():
            path.mkdir(parents=True, exist_ok=True)

        # ---- Logger --------------------------------------------------------
        self.log = _build_logger(self.dirs["logs"])

        # ---- Status file (survives crashes) --------------------------------
        self.status_file = self.dirs["status"] / "progress.json"
        self.status      = self._load_status()

        # ---- CDS client (lazy — created on first download) -----------------
        self._cds: Optional[cdsapi.Client] = None

        self.log.info("=" * 70)
        self.log.info("ERA5 Africa 2m-Temperature Workflow")
        self.log.info(f"Period     : {start_year} – {end_year}")
        self.log.info(f"Region     : N={AFRICA_AREA[0]} W={AFRICA_AREA[1]} "
                      f"S={AFRICA_AREA[2]} E={AFRICA_AREA[3]}")
        self.log.info(f"Output dir : {self.base_dir}")
        self.log.info("=" * 70)

    # ------------------------------------------------------------------
    # STATUS / PROGRESS TRACKER
    # ------------------------------------------------------------------

    def _load_status(self) -> dict:
        if self.status_file.exists():
            try:
                with open(self.status_file) as f:
                    return json.load(f)
            except Exception:
                pass
        return {"downloaded_years": [], "failed_years": [], "validated_years": []}

    def _save_status(self) -> None:
        with open(self.status_file, "w") as f:
            json.dump(self.status, f, indent=2)

    def _mark_downloaded(self, year: int) -> None:
        if year not in self.status["downloaded_years"]:
            self.status["downloaded_years"].append(year)
        if year in self.status["failed_years"]:
            self.status["failed_years"].remove(year)
        self._save_status()

    def _mark_failed(self, year: int) -> None:
        if year not in self.status["failed_years"]:
            self.status["failed_years"].append(year)
        self._save_status()

    def _mark_validated(self, year: int) -> None:
        if year not in self.status["validated_years"]:
            self.status["validated_years"].append(year)
        self._save_status()

    def _is_downloaded(self, year: int) -> bool:
        return year in self.status["downloaded_years"]

    def _is_validated(self, year: int) -> bool:
        return year in self.status["validated_years"]

    # ------------------------------------------------------------------
    # CDS CLIENT
    # ------------------------------------------------------------------

    def _get_cds_client(self) -> "cdsapi.Client":
        if self._cds is None:
            self.log.info("Initialising CDS API client…")
            self._cds = cdsapi.Client(
                quiet=False,
                progress=True,
            )
        return self._cds

    # ------------------------------------------------------------------
    # BUILD CDS REQUEST
    # ------------------------------------------------------------------

    def _build_request(self, year: int) -> dict:
        return {
            "product_type": [PRODUCT],
            "variable":     [VARIABLE],
            "year":         [str(year)],
            "month":        [f"{m:02d}" for m in range(1, 13)],
            "time":         [TIME_STEP],
            "data_format":  "grib",
            "download_format": "unarchived",
            "area":         AFRICA_AREA,        # [N, W, S, E]
        }

    # ------------------------------------------------------------------
    # GRIB PATH HELPERS
    # ------------------------------------------------------------------

    def _grib_path(self, year: int) -> Path:
        d = self.dirs["raw_grib"] / str(year)
        d.mkdir(exist_ok=True)
        return d / f"era5_t2m_africa_{year}.grib"

    def _tif_path(self, year: int, month: int) -> Path:
        d = self.dirs["geotiffs"] / str(year)
        d.mkdir(exist_ok=True)
        return d / f"era5_t2m_africa_{year}_{month:02d}.tif"

    def _preview_path(self, year: int, month: int) -> Path:
        d = self.dirs["previews"] / str(year)
        d.mkdir(exist_ok=True)
        return d / f"era5_t2m_africa_{year}_{month:02d}_preview.png"

    def _meta_path(self, year: int) -> Path:
        d = self.dirs["metadata"] / str(year)
        d.mkdir(exist_ok=True)
        return d / f"era5_t2m_africa_{year}_metadata.json"

    # ------------------------------------------------------------------
    # STEP 1 — DOWNLOAD GRIB
    # ------------------------------------------------------------------

    def download_year(self, year: int, force: bool = False) -> Optional[Path]:
        """
        Download a single year of ERA5 monthly t2m as GRIB.

        Returns the GRIB path on success, None on failure.
        """
        grib_path = self._grib_path(year)

        # Already complete?
        if not force and grib_path.exists() and self._is_downloaded(year):
            size_mb = grib_path.stat().st_size / 1e6
            self.log.info(f"[{year}] GRIB exists ({size_mb:.1f} MB) — skipping download")
            return grib_path

        # GRIB file exists on disk but wasn't marked downloaded (e.g. conversion
        # failed previously) — skip the network request and reuse the file.
        if not force and grib_path.exists() and grib_path.stat().st_size > 0:
            size_mb = grib_path.stat().st_size / 1e6
            self.log.info(
                f"[{year}] GRIB found on disk ({size_mb:.1f} MB) but not in status — "
                f"reusing without re-download"
            )
            self._mark_downloaded(year)
            return grib_path

        request = self._build_request(year)
        self.log.info(f"[{year}] Submitting CDS request …")
        self.log.debug(f"[{year}] Request: {json.dumps(request, indent=2)}")

        for attempt in range(1, self.retry_limit + 1):
            try:
                client = self._get_cds_client()

                # CDS downloads to a temp name then we rename for atomicity
                tmp_path = grib_path.with_suffix(".downloading")
                client.retrieve(DATASET, request, str(tmp_path))

                if not tmp_path.exists() or tmp_path.stat().st_size == 0:
                    raise RuntimeError("Downloaded file is empty or missing")

                tmp_path.rename(grib_path)
                size_mb = grib_path.stat().st_size / 1e6
                self.log.info(f"[{year}] ✅ Downloaded {size_mb:.1f} MB → {grib_path.name}")
                self._mark_downloaded(year)
                return grib_path

            except Exception as exc:
                self.log.warning(
                    f"[{year}] Attempt {attempt}/{self.retry_limit} failed: {exc}"
                )
                # Clean up partial file
                tmp = grib_path.with_suffix(".downloading")
                if tmp.exists():
                    tmp.unlink()

                if attempt < self.retry_limit:
                    wait = self.retry_wait * attempt
                    self.log.info(f"[{year}] Waiting {wait}s before retry …")
                    time.sleep(wait)

        self.log.error(f"[{year}] ❌ All {self.retry_limit} download attempts failed")
        self._mark_failed(year)
        return None

    # ------------------------------------------------------------------
    # STEP 2 — GRIB → GeoTIFF (K → °C)
    # ------------------------------------------------------------------

    def grib_to_geotiffs(self, year: int, grib_path: Path) -> List[Path]:
        """
        Open a GRIB file with cfgrib, convert K→°C, and write one
        GeoTIFF per month.

        Uses cfgrib.open_datasets() directly — compatible with all
        xarray versions (xr.open_datasets plural was never released).

        Returns list of successfully written TIF paths.
        """
        if not HAS_XARRAY or not HAS_RASTERIO:
            self.log.error("cfgrib/xarray or rasterio not available — cannot convert")
            return []

        self.log.info(f"[{year}] Opening GRIB with cfgrib …")

        try:
            # cfgrib.open_datasets() returns a list of xarray Datasets,
            # one per unique set of GRIB keys. This is the correct API
            # and works regardless of xarray version.
            datasets = cfgrib.open_datasets(str(grib_path))
        except Exception as exc:
            self.log.error(f"[{year}] Failed to open GRIB: {exc}")
            traceback.print_exc()
            return []

        # Find the dataset that contains t2m
        ds_t2m = None
        for ds in datasets:
            if "t2m" in ds:
                ds_t2m = ds
                break

        if ds_t2m is None:
            self.log.error(f"[{year}] Variable 't2m' not found in GRIB datasets")
            for ds in datasets:
                ds.close()
            return []

        # ERA5 monthly mean: dims are typically (valid_time, latitude, longitude)
        # Normalise dimension name
        time_dim = None
        for candidate in ("valid_time", "time", "forecast_reference_time"):
            if candidate in ds_t2m.dims:
                time_dim = candidate
                break

        if time_dim is None:
            self.log.error(f"[{year}] Cannot find time dimension in dataset")
            for ds in datasets:
                ds.close()
            return []

        t2m_da  = ds_t2m["t2m"]
        lats    = ds_t2m["latitude"].values
        lons    = ds_t2m["longitude"].values

        # ERA5 has latitudes descending (90 → -90); rasterio expects top→bottom
        lat_desc = lats[0] > lats[-1]

        n_lat = len(lats)
        n_lon = len(lons)
        res   = abs(float(lats[1] - lats[0]))          # grid spacing in degrees

        # Affine transform using the precise pixel-edge extents defined in
        # AFRICA_BOUNDS (WSEN: -20, -40.0000011920928955, 55.0000011175870895, 40)
        # from_bounds signature: (west, south, east, north, width, height)
        transform = from_bounds(
            AFRICA_BOUNDS["left"], AFRICA_BOUNDS["bottom"],
            AFRICA_BOUNDS["right"], AFRICA_BOUNDS["top"],
            n_lon, n_lat,
        )

        written_tifs: List[Path] = []
        times = ds_t2m[time_dim].values

        months_iter = range(len(times))
        if HAS_TQDM:
            months_iter = tqdm(months_iter, desc=f"  {year} months", unit="mo")

        for i in months_iter:
            try:
                ts    = times[i]
                # numpy datetime64 → python datetime
                ts_dt = dt.utcfromtimestamp(
                    (ts - np.datetime64("1970-01-01T00:00:00")) /
                    np.timedelta64(1, "s")
                )
                month = ts_dt.month

                tif_path = self._tif_path(year, month)
                if tif_path.exists():
                    self.log.debug(f"[{year}-{month:02d}] TIF exists, skipping")
                    written_tifs.append(tif_path)
                    continue

                # Extract 2-D slice (K) and convert to °C
                slice_k = t2m_da.isel({time_dim: i}).values.astype("float32")
                if not lat_desc:
                    slice_k = np.flipud(slice_k)    # ensure top→bottom
                slice_c = slice_k - KELVIN_OFFSET

                # Replace ERA5 fill value with standard nodata
                fill = getattr(t2m_da, "_FillValue", 9.999e20)
                slice_c[np.abs(slice_c - (fill - KELVIN_OFFSET)) < 1e10] = -9999.0

                profile = {
                    "driver":    "GTiff",
                    "dtype":     "float32",
                    "width":     n_lon,
                    "height":    n_lat,
                    "count":     1,
                    "crs":       "EPSG:4326",
                    "transform": transform,
                    "nodata":    -9999.0,
                    "compress":  "lzw",
                    "tiled":     True,
                    "blockxsize": 256,
                    "blockysize": 256,
                    "predictor": 2,
                }

                with rasterio.open(tif_path, "w", **profile) as dst:
                    dst.write(slice_c, 1)

                size_kb = tif_path.stat().st_size / 1024
                self.log.info(
                    f"[{year}-{month:02d}] ✅ Written {tif_path.name} ({size_kb:.0f} KB)"
                )
                written_tifs.append(tif_path)

            except Exception as exc:
                self.log.error(f"[{year} month-{i}] Conversion error: {exc}")
                traceback.print_exc()

        # Close all datasets cleanly
        for ds in datasets:
            ds.close()

        return written_tifs

    # ------------------------------------------------------------------
    # STEP 3 — VALIDATE & METADATA
    # ------------------------------------------------------------------

    def validate_year(self, year: int, tif_paths: List[Path]) -> dict:
        """
        Compute per-month statistics and write a JSON metadata file.
        Optionally create preview PNGs.
        """
        if not tif_paths:
            self.log.warning(f"[{year}] No TIFs to validate")
            return {}

        year_stats: dict = {"year": year, "months": {}}

        for tif_path in sorted(tif_paths):
            match = re.search(r"_(\d{4})_(\d{2})\.tif", tif_path.name)
            if not match:
                continue
            month = int(match.group(2))

            try:
                with rasterio.open(tif_path) as src:
                    data = src.read(1)
                    nodata = src.nodata if src.nodata is not None else -9999.0
                    valid  = data[data != nodata]
                    valid  = valid[np.isfinite(valid)]

                    if len(valid) == 0:
                        self.log.warning(f"[{year}-{month:02d}] No valid pixels!")
                        continue

                    stats = {
                        "min_degC":     float(np.min(valid)),
                        "max_degC":     float(np.max(valid)),
                        "mean_degC":    float(np.mean(valid)),
                        "std_degC":     float(np.std(valid)),
                        "valid_pixels": int(len(valid)),
                        "crs":          str(src.crs),
                        "shape":        list(src.shape),
                        "bounds":       list(src.bounds),
                    }
                    year_stats["months"][f"{month:02d}"] = stats

                    self.log.info(
                        f"[{year}-{month:02d}] mean={stats['mean_degC']:.2f}°C  "
                        f"range=[{stats['min_degC']:.2f}, {stats['max_degC']:.2f}]°C"
                    )

                    if self.create_previews and HAS_MPL:
                        self._create_preview(tif_path, year, month, data, src.bounds, nodata)

            except Exception as exc:
                self.log.error(f"[{year}-{month:02d}] Validation error: {exc}")

        # Save JSON metadata
        meta_path = self._meta_path(year)
        year_stats["generated_at"] = dt.utcnow().isoformat()
        with open(meta_path, "w") as f:
            json.dump(year_stats, f, indent=2)
        self.log.info(f"[{year}] Metadata → {meta_path.name}")
        self._mark_validated(year)

        return year_stats

    # ------------------------------------------------------------------
    # PREVIEW IMAGES
    # ------------------------------------------------------------------

    def _create_preview(
        self, tif_path: Path, year: int, month: int,
        data: "np.ndarray", bounds, nodata: float
    ) -> None:
        import calendar

        try:
            plot_data = data.astype("float32").copy()
            plot_data[plot_data == nodata]        = np.nan
            plot_data[~np.isfinite(plot_data)]    = np.nan

            vmin = np.nanpercentile(plot_data, 2)
            vmax = np.nanpercentile(plot_data, 98)

            fig, ax = plt.subplots(figsize=(9, 8))
            cmap = plt.get_cmap("RdYlBu_r")
            cmap.set_bad("#cccccc")

            extent = [bounds.left, bounds.right, bounds.bottom, bounds.top]
            im = ax.imshow(
                plot_data, cmap=cmap, extent=extent,
                interpolation="nearest", aspect="auto",
                vmin=vmin, vmax=vmax,
            )

            cbar = plt.colorbar(im, ax=ax, pad=0.02, fraction=0.046, extend="both")
            cbar.set_label("2m Temperature (°C)", fontsize=11)

            month_name = calendar.month_name[month]
            ax.set_title(
                f"ERA5 2m Temperature — Africa\n{month_name} {year}",
                fontsize=13, fontweight="bold",
            )
            ax.set_xlabel("Longitude", fontsize=10)
            ax.set_ylabel("Latitude",  fontsize=10)
            ax.grid(True, alpha=0.3, linewidth=0.5)

            plt.tight_layout()
            out = self._preview_path(year, month)
            plt.savefig(out, dpi=150, bbox_inches="tight")
            plt.close(fig)
            self.log.debug(f"[{year}-{month:02d}] Preview → {out.name}")

        except Exception as exc:
            self.log.warning(f"[{year}-{month:02d}] Preview failed: {exc}")

    # ------------------------------------------------------------------
    # ORCHESTRATOR — single year
    # ------------------------------------------------------------------

    def process_year(self, year: int, force: bool = False) -> bool:
        """
        Full pipeline for one year: download → convert → validate.
        Returns True if all 12 months were written successfully.
        """
        self.log.info(f"\n{'─'*60}")
        self.log.info(f"Processing year: {year}")
        self.log.info(f"{'─'*60}")

        # ------ Download ----------------------------------------------------
        grib_path = self.download_year(year, force=force)
        if grib_path is None:
            return False

        # ------ Convert -----------------------------------------------------
        tif_paths = self.grib_to_geotiffs(year, grib_path)
        if not tif_paths:
            self.log.error(f"[{year}] No TIFs produced — aborting year")
            return False

        # ------ Validate ----------------------------------------------------
        self.validate_year(year, tif_paths)

        # ------ Optionally remove raw GRIB ----------------------------------
        if not self.keep_grib and grib_path.exists():
            grib_path.unlink()
            self.log.info(f"[{year}] GRIB deleted (keep_grib=False)")

        ok = len(tif_paths) == 12
        self.log.info(
            f"[{year}] {'✅ Complete' if ok else '⚠️ Partial'} "
            f"({len(tif_paths)}/12 months)"
        )
        return ok

    # ------------------------------------------------------------------
    # ORCHESTRATOR — full range
    # ------------------------------------------------------------------

    def run(
        self,
        start_year: Optional[int] = None,
        end_year:   Optional[int] = None,
        force:      bool = False,
        skip_validated: bool = True,
    ) -> dict:
        """
        Run the full pipeline for a range of years.

        Parameters
        ----------
        start_year : int, optional
            Override self.start_year.
        end_year : int, optional
            Override self.end_year.
        force : bool
            Re-download even if GRIB already exists.
        skip_validated : bool
            Skip years that have already been fully validated.

        Returns
        -------
        dict with keys 'success', 'failed', 'skipped'.
        """
        sy = start_year or self.start_year
        ey = end_year   or self.end_year
        years = list(range(sy, ey + 1))

        self.log.info(f"\n{'='*70}")
        self.log.info(f"ERA5 Africa T2M — Processing {sy}–{ey} ({len(years)} years)")
        self.log.info(f"{'='*70}\n")

        success, failed, skipped = [], [], []

        for year in years:
            if skip_validated and not force and self._is_validated(year):
                # Double-check files actually exist
                expected = [self._tif_path(year, m) for m in range(1, 13)]
                if all(p.exists() for p in expected):
                    self.log.info(f"[{year}] Already validated — skipping")
                    skipped.append(year)
                    continue

            ok = self.process_year(year, force=force)
            (success if ok else failed).append(year)

        # ---- Final report --------------------------------------------------
        self.log.info(f"\n{'='*70}")
        self.log.info("PIPELINE COMPLETE")
        self.log.info(f"{'='*70}")
        self.log.info(f"  Successful : {len(success)} years")
        self.log.info(f"  Failed     : {len(failed)} years")
        self.log.info(f"  Skipped    : {len(skipped)} years (already done)")

        if failed:
            self.log.warning(f"  Failed years: {failed}")

        # Write final summary
        summary = {
            "run_at":    dt.utcnow().isoformat(),
            "period":    f"{sy}–{ey}",
            "success":   success,
            "failed":    failed,
            "skipped":   skipped,
        }
        summary_path = self.dirs["status"] / f"run_summary_{dt.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(summary_path, "w") as f:
            json.dump(summary, f, indent=2)
        self.log.info(f"  Summary    : {summary_path}")

        return summary

    # ------------------------------------------------------------------
    # UTILITY: list what's on disk
    # ------------------------------------------------------------------

    def inventory(self) -> None:
        """Print a table of downloaded / validated years."""
        print(f"\n{'─'*60}")
        print(f"{'YEAR':>6}  {'GRIB':>6}  {'TIFs':>5}  {'VALIDATED':>10}")
        print(f"{'─'*60}")

        for year in range(self.start_year, self.end_year + 1):
            grib_exists = self._grib_path(year).exists()
            tif_count   = len(list((self.dirs["geotiffs"] / str(year)).glob("*.tif"))) \
                          if (self.dirs["geotiffs"] / str(year)).exists() else 0
            validated   = self._is_validated(year)
            grib_str    = "✅" if grib_exists else "—"
            val_str     = "✅" if validated   else "—"
            print(f"{year:>6}  {grib_str:>6}  {tif_count:>5}  {val_str:>10}")

        print(f"{'─'*60}\n")


# =============================================================================
# INTERACTIVE MENU
# =============================================================================

def interactive_menu() -> None:
    print("\n" + "=" * 70)
    print("🌍  ERA5 Africa 2m Temperature — Interactive Menu")
    print("=" * 70)

    sy = input(f"Start year [{ERA5_START_YEAR}]: ").strip()
    ey = input(f"End year   [{ERA5_END_YEAR}]:   ").strip()
    sy = int(sy) if sy.isdigit() else ERA5_START_YEAR
    ey = int(ey) if ey.isdigit() else ERA5_END_YEAR

    keep_grib = input("Keep raw GRIB files? (y/n) [y]: ").strip().lower() != "n"
    previews  = input("Create preview PNGs?  (y/n) [y]: ").strip().lower() != "n"

    wf = ERA5AfricaT2MWorkflow(
        start_year=sy, end_year=ey,
        keep_grib=keep_grib, create_previews=previews,
    )

    while True:
        print("\n📋 MENU")
        print("  1. Run full pipeline (download → convert → validate)")
        print("  2. Download GRIB only")
        print("  3. Convert existing GRIBs to GeoTIFF")
        print("  4. Validate existing GeoTIFFs")
        print("  5. Show disk inventory")
        print("  6. Exit")

        choice = input("Choice: ").strip()

        if choice == "1":
            force = input("Force re-download? (y/n) [n]: ").strip().lower() == "y"
            wf.run(force=force)

        elif choice == "2":
            for year in range(sy, ey + 1):
                wf.download_year(year)

        elif choice == "3":
            for year in range(sy, ey + 1):
                grib = wf._grib_path(year)
                if grib.exists():
                    wf.grib_to_geotiffs(year, grib)
                else:
                    print(f"  [{year}] No GRIB found — skipping")

        elif choice == "4":
            for year in range(sy, ey + 1):
                tifs = sorted((wf.dirs["geotiffs"] / str(year)).glob("*.tif")) \
                       if (wf.dirs["geotiffs"] / str(year)).exists() else []
                if tifs:
                    wf.validate_year(year, tifs)
                else:
                    print(f"  [{year}] No TIFs found — skipping")

        elif choice == "5":
            wf.inventory()

        elif choice == "6":
            print("Goodbye!")
            break
        else:
            print("Invalid choice.")


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    # 1 — dependency gate
    if not check_dependencies():
        sys.exit(1)

    print("\nSelect mode:")
    print("  1. Quick test  (2020–2022, 3 years)")
    print("  2. Full run    (1980–present)")
    print("  3. Interactive menu")

    mode = input("Mode [3]: ").strip() or "3"

    if mode == "1":
        wf = ERA5AfricaT2MWorkflow(start_year=2020, end_year=2022)
        wf.run()

    elif mode == "2":
        wf = ERA5AfricaT2MWorkflow(
            start_year=ERA5_START_YEAR,
            end_year=ERA5_END_YEAR,
        )
        wf.run()

    else:
        interactive_menu()