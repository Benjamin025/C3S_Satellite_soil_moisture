#!/usr/bin/env python3
"""
ESA SOIL MOISTURE DATA PROCESSOR - PRODUCTION READY (FIXED)
============================================================
Processes downloaded soil moisture ZIP files and extracts NetCDF bands.

FIXES:
- Handles numbered RZSM variables (rzsm_1, rzsm_2, rzsm_3)
- Uses ds.sizes instead of ds.dims to avoid FutureWarning
- Properly extracts all soil layers

Features:
- Extracts SSM, RZSM (all layers), and Freeze/Thaw variables from ZIP files
- Organizes by variable type and date
- Quality control and metadata extraction
- Progress tracking and resume capability
- Handles both daily and monthly aggregations
- Creates summary statistics and data catalog

Author: Production Ready
Date: February 2026
"""

import os
import sys
import json
import logging
import argparse
import zipfile
import shutil
from pathlib import Path
from datetime import datetime
import time
import re

try:
    import xarray as xr
    import numpy as np
    import pandas as pd
except ImportError:
    print("ERROR: Required packages not installed")
    print("Install with: pip install xarray netCDF4 pandas numpy")
    sys.exit(1)

try:
    from tqdm import tqdm
except ImportError:
    print("WARNING: tqdm not installed. Progress bars will be disabled.")
    tqdm = None

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('soil_moisture_processing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class SoilMoistureProcessor:
    """
    Production-ready soil moisture data processor
    
    Processes downloaded ESA CCI Soil Moisture data:
    - Extracts NetCDF files from ZIP archives
    - Separates variables (SSM, RZSM, Freeze/Thaw)
    - Handles numbered RZSM layers (rzsm_1, rzsm_2, rzsm_3)
    - Organizes by date and variable type
    - Generates metadata and statistics
    """
    
    # Variable mapping (internal name → possible NetCDF variable names)
    # UPDATED: Now includes patterns to match numbered variables
    VARIABLE_MAPPING = {
        'SSM': ['sm'],                    # Surface soil moisture
        'RZSM': ['rzsm'],                 # Root zone soil moisture (base name)
        'freeze_thaw': ['ft', 'flag']     # Freeze/thaw classification
    }
    
    # Full variable names for documentation
    VARIABLE_NAMES = {
        'SSM': 'surface_soil_moisture_volumetric',
        'RZSM': 'root_zone_soil_moisture_volumetric',
        'freeze_thaw': 'freeze_thaw_classification'
    }
    
    def __init__(self, base_dir="./data/soil_moisture"):
        """
        Initialize processor
        
        Parameters:
        -----------
        base_dir : str
            Base directory containing raw/ subdirectory
        """
        self.base_dir = Path(base_dir)
        self.raw_dir = self.base_dir / "raw"
        self.processed_dir = self.base_dir / "processed"
        self.metadata_dir = self.base_dir / "metadata"
        
        # Create directories
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        
        # Create variable directories
        for var in ['SSM', 'RZSM', 'freeze_thaw']:
            (self.processed_dir / var).mkdir(exist_ok=True)
        
        # Progress tracking
        self.progress_file = self.base_dir / "processing_progress.json"
        self.progress = self.load_progress()
        
        # Processing statistics
        self.stats = {
            'processed': 0,
            'skipped': 0,
            'failed': 0,
            'total_size_mb': 0,
            'variables_extracted': {var: 0 for var in ['SSM', 'RZSM', 'freeze_thaw']},
            'rzsm_layers_found': set()  # Track which RZSM layers we've seen
        }
        
        logger.info("✅ Processor initialized")
        logger.info(f"Raw directory: {self.raw_dir}")
        logger.info(f"Processed directory: {self.processed_dir}")
    
    def load_progress(self):
        """Load processing progress"""
        if self.progress_file.exists():
            with open(self.progress_file, 'r') as f:
                return json.load(f)
        return {'completed': [], 'failed': []}
    
    def save_progress(self):
        """Save processing progress"""
        with open(self.progress_file, 'w') as f:
            json.dump(self.progress, f, indent=2)
    
    def is_processed(self, zip_path):
        """Check if file already processed"""
        return str(zip_path) in self.progress['completed']
    
    def mark_completed(self, zip_path):
        """Mark file as processed"""
        path_str = str(zip_path)
        if path_str not in self.progress['completed']:
            self.progress['completed'].append(path_str)
            self.save_progress()
    
    def mark_failed(self, zip_path, error):
        """Mark file as failed"""
        self.progress['failed'].append({
            'file': str(zip_path),
            'error': str(error),
            'timestamp': datetime.now().isoformat()
        })
        self.save_progress()
    
    def add_crs_information(self, ds):
        """
        Add CRS (Coordinate Reference System) information to dataset
        
        Parameters:
        -----------
        ds : xarray.Dataset
            Dataset to add CRS information to
        
        Returns:
        --------
        xarray.Dataset : Dataset with CRS information added
        """
        # Add CRS as a variable (CF conventions)
        ds['crs'] = xr.DataArray(
            data=0,  # Dummy value
            attrs={
                'grid_mapping_name': 'latitude_longitude',
                'longitude_of_prime_meridian': 0.0,
                'semi_major_axis': 6378137.0,
                'inverse_flattening': 298.257223563,
                'spatial_ref': 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]',
                'crs_wkt': 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]',
                'epsg_code': 'EPSG:4326',
                'proj4': '+proj=longlat +datum=WGS84 +no_defs',
                'GeoTransform': 'None'
            }
        )
        
        # Add grid_mapping attribute to all data variables
        for var in ds.data_vars:
            if var != 'crs':
                ds[var].attrs['grid_mapping'] = 'crs'
        
        # Ensure lat/lon have proper attributes
        if 'lat' in ds.coords:
            ds['lat'].attrs.update({
                'standard_name': 'latitude',
                'long_name': 'latitude',
                'units': 'degrees_north',
                'axis': 'Y'
            })
        
        if 'lon' in ds.coords:
            ds['lon'].attrs.update({
                'standard_name': 'longitude',
                'long_name': 'longitude',
                'units': 'degrees_east',
                'axis': 'X'
            })
        
        # Add global CRS attributes
        ds.attrs.update({
            'crs': 'EPSG:4326',
            'coordinate_system': 'WGS84',
            'proj4': '+proj=longlat +datum=WGS84 +no_defs'
        })
        
        logger.debug("  ✓ Added CRS information (EPSG:4326)")
        
        return ds
    
    def find_rzsm_variables(self, ds):
        """
        Find all RZSM variables in dataset (including numbered ones like rzsm_1, rzsm_2, rzsm_3)
        
        Parameters:
        -----------
        ds : xarray.Dataset
            Dataset to search
        
        Returns:
        --------
        list : List of RZSM variable names found
        """
        rzsm_vars = []
        
        # Check for numbered RZSM variables (rzsm_1, rzsm_2, rzsm_3, etc.)
        for var in ds.data_vars:
            if re.match(r'^rzsm(_\d+)?$', var):
                rzsm_vars.append(var)
        
        return sorted(rzsm_vars)
    
    def scan_raw_files(self, year=None):
        """
        Scan for ZIP files to process
        
        Parameters:
        -----------
        year : int, optional
            Process only specific year
        
        Returns:
        --------
        list : List of Path objects to ZIP files
        """
        if not self.raw_dir.exists():
            logger.error(f"❌ Raw directory not found: {self.raw_dir}")
            return []
        
        zip_files = []
        
        if year:
            year_dir = self.raw_dir / str(year)
            if year_dir.exists():
                zip_files = list(year_dir.glob("*.zip"))
        else:
            # Scan all year directories
            for year_dir in sorted(self.raw_dir.iterdir()):
                if year_dir.is_dir() and year_dir.name.isdigit():
                    zip_files.extend(year_dir.glob("*.zip"))
        
        logger.info(f"📁 Found {len(zip_files)} ZIP file(s) to process")
        return sorted(zip_files)
    
    def extract_metadata_from_filename(self, filename):
        """
        Extract metadata from filename
        
        Format: soil_moisture_YYYY_MM_sensor_timeagg.zip
        
        Returns:
        --------
        dict : Metadata
        """
        parts = filename.stem.split('_')
        
        try:
            metadata = {
                'year': int(parts[2]),
                'month': int(parts[3]),
                'sensor': parts[4],
                'time_agg': parts[5]
            }
            return metadata
        except (IndexError, ValueError) as e:
            logger.warning(f"⚠️  Could not parse filename: {filename}")
            return None
    
    def process_zip_file(self, zip_path, skip_if_exists=True):
        """
        Process a single ZIP file
        
        Parameters:
        -----------
        zip_path : Path
            Path to ZIP file
        skip_if_exists : bool
            Skip if already processed
        
        Returns:
        --------
        dict : Processing results
        """
        
        # Check if already processed
        if skip_if_exists and self.is_processed(zip_path):
            logger.info(f"⏭️  Skipping {zip_path.name} (already processed)")
            self.stats['skipped'] += 1
            return {'status': 'skipped'}
        
        logger.info("="*70)
        logger.info(f"PROCESSING: {zip_path.name}")
        logger.info("="*70)
        
        # Extract metadata from filename
        metadata = self.extract_metadata_from_filename(zip_path)
        if not metadata:
            logger.error("❌ Invalid filename format")
            return {'status': 'failed', 'error': 'Invalid filename'}
        
        year = metadata['year']
        month = metadata['month']
        time_agg = metadata['time_agg']
        
        logger.info(f"Year: {year}, Month: {month:02d}")
        logger.info(f"Time aggregation: {time_agg}")
        logger.info(f"File size: {zip_path.stat().st_size / (1024**2):.2f} MB")
        
        # Create temporary extraction directory
        temp_dir = self.base_dir / f"temp_extract_{year}_{month:02d}"
        temp_dir.mkdir(exist_ok=True)
        
        results = {
            'status': 'success',
            'zip_file': str(zip_path),
            'year': year,
            'month': month,
            'time_agg': time_agg,
            'variables_found': [],
            'output_files': []
        }
        
        try:
            # Extract ZIP file
            logger.info("📦 Extracting ZIP file...")
            with zipfile.ZipFile(zip_path, 'r') as zf:
                # List contents
                nc_files = [f for f in zf.namelist() if f.endswith('.nc')]
                logger.info(f"Found {len(nc_files)} NetCDF file(s) in archive")
                
                if not nc_files:
                    logger.warning("⚠️  No NetCDF files found in ZIP")
                    return {'status': 'failed', 'error': 'No NetCDF files'}
                
                # Extract all NetCDF files
                for nc_file in nc_files:
                    zf.extract(nc_file, temp_dir)
                    logger.info(f"  ✓ Extracted: {nc_file}")
            
            # Process each NetCDF file
            for nc_file in nc_files:
                nc_path = temp_dir / nc_file
                
                logger.info(f"\n📊 Processing: {nc_file}")
                
                # Open with xarray
                try:
                    ds = xr.open_dataset(nc_path)
                    
                    logger.info(f"Variables in file: {list(ds.data_vars)}")
                    logger.info(f"Dimensions: {dict(ds.sizes)}")  # FIXED: Use ds.sizes instead of ds.dims
                    
                    # 1. Process SSM (Surface Soil Moisture)
                    if 'sm' in ds.data_vars:
                        logger.info(f"  ✓ Found SSM (sm)")
                        
                        var_ds = ds[['sm']]
                        var_ds = self.add_crs_information(var_ds)
                        
                        var_dir = self.processed_dir / 'SSM' / str(year)
                        var_dir.mkdir(parents=True, exist_ok=True)
                        
                        output_name = f"SSM_{year}_{month:02d}_{time_agg}.nc"
                        output_path = var_dir / output_name
                        
                        logger.info(f"  💾 Saving to: {output_path}")
                        var_ds.to_netcdf(output_path)
                        
                        var_data = ds['sm']
                        valid_pixels = np.isfinite(var_data).sum().values
                        
                        logger.info(f"  📈 Valid pixels: {valid_pixels:,}")
                        logger.info(f"  📏 Shape: {var_data.shape}")
                        logger.info(f"  📊 Min: {float(var_data.min().values):.4f}")
                        logger.info(f"  📊 Max: {float(var_data.max().values):.4f}")
                        logger.info(f"  📊 Mean: {float(var_data.mean().values):.4f}")
                        
                        results['variables_found'].append('SSM')
                        results['output_files'].append(str(output_path))
                        self.stats['variables_extracted']['SSM'] += 1
                    
                    # 2. Process RZSM (Root Zone Soil Moisture) - ALL LAYERS
                    rzsm_vars = self.find_rzsm_variables(ds)
                    
                    if rzsm_vars:
                        logger.info(f"  ✓ Found RZSM variables: {rzsm_vars}")
                        
                        # Extract ALL RZSM variables into one file
                        var_ds = ds[rzsm_vars]
                        var_ds = self.add_crs_information(var_ds)
                        
                        var_dir = self.processed_dir / 'RZSM' / str(year)
                        var_dir.mkdir(parents=True, exist_ok=True)
                        
                        output_name = f"RZSM_{year}_{month:02d}_{time_agg}.nc"
                        output_path = var_dir / output_name
                        
                        logger.info(f"  💾 Saving to: {output_path}")
                        var_ds.to_netcdf(output_path)
                        
                        # Track which layers we've found
                        for rzsm_var in rzsm_vars:
                            self.stats['rzsm_layers_found'].add(rzsm_var)
                            
                            var_data = ds[rzsm_var]
                            valid_pixels = np.isfinite(var_data).sum().values
                            
                            logger.info(f"  📈 {rzsm_var}: Valid pixels: {valid_pixels:,}")
                            logger.info(f"     Shape: {var_data.shape}")
                            logger.info(f"     Min: {float(var_data.min().values):.4f}")
                            logger.info(f"     Max: {float(var_data.max().values):.4f}")
                            logger.info(f"     Mean: {float(var_data.mean().values):.4f}")
                        
                        results['variables_found'].append('RZSM')
                        results['output_files'].append(str(output_path))
                        self.stats['variables_extracted']['RZSM'] += 1
                    else:
                        logger.info(f"  ⚠️  RZSM not found in file")
                    
                    # 3. Process Freeze/Thaw
                    freeze_thaw_var = None
                    for possible_name in ['ft', 'flag']:
                        if possible_name in ds.data_vars:
                            freeze_thaw_var = possible_name
                            break
                    
                    if freeze_thaw_var:
                        logger.info(f"  ✓ Found freeze_thaw ({freeze_thaw_var})")
                        
                        var_ds = ds[[freeze_thaw_var]]
                        var_ds = self.add_crs_information(var_ds)
                        
                        var_dir = self.processed_dir / 'freeze_thaw' / str(year)
                        var_dir.mkdir(parents=True, exist_ok=True)
                        
                        output_name = f"freeze_thaw_{year}_{month:02d}_{time_agg}.nc"
                        output_path = var_dir / output_name
                        
                        logger.info(f"  💾 Saving to: {output_path}")
                        var_ds.to_netcdf(output_path)
                        
                        var_data = ds[freeze_thaw_var]
                        valid_pixels = np.isfinite(var_data).sum().values
                        
                        logger.info(f"  📈 Valid pixels: {valid_pixels:,}")
                        logger.info(f"  📏 Shape: {var_data.shape}")
                        
                        results['variables_found'].append('freeze_thaw')
                        results['output_files'].append(str(output_path))
                        self.stats['variables_extracted']['freeze_thaw'] += 1
                    
                    ds.close()
                    
                except Exception as e:
                    logger.error(f"❌ Error processing {nc_file}: {e}")
                    continue
            
            # Success
            logger.info("="*70)
            logger.info("✅ PROCESSING SUCCESSFUL!")
            logger.info(f"Variables extracted: {results['variables_found']}")
            logger.info(f"Output files: {len(results['output_files'])}")
            logger.info("="*70)
            
            self.mark_completed(zip_path)
            self.stats['processed'] += 1
            self.stats['total_size_mb'] += zip_path.stat().st_size / (1024**2)
            
        except Exception as e:
            logger.error("="*70)
            logger.error("❌ PROCESSING FAILED!")
            logger.error(f"Error: {e}")
            logger.error("="*70)
            
            results['status'] = 'failed'
            results['error'] = str(e)
            self.mark_failed(zip_path, e)
            self.stats['failed'] += 1
        
        finally:
            # Clean up temp directory
            if temp_dir.exists():
                shutil.rmtree(temp_dir)
                logger.info(f"🧹 Cleaned up temp directory")
        
        return results
    
    def process_all(self, year=None, skip_if_exists=True):
        """
        Process all ZIP files
        
        Parameters:
        -----------
        year : int, optional
            Process only specific year
        skip_if_exists : bool
            Skip already processed files
        
        Returns:
        --------
        list : List of processing results
        """
        zip_files = self.scan_raw_files(year)
        
        if not zip_files:
            logger.warning("⚠️  No ZIP files found to process")
            return []
        
        logger.info(f"\n{'='*70}")
        logger.info(f"PROCESSING {len(zip_files)} FILE(S)")
        logger.info(f"{'='*70}\n")
        
        all_results = []
        
        # Process with progress bar if available
        iterator = tqdm(zip_files, desc="Processing") if tqdm else zip_files
        
        for zip_file in iterator:
            result = self.process_zip_file(zip_file, skip_if_exists)
            all_results.append(result)
            
            time.sleep(0.5)
        
        return all_results
    
    def generate_data_catalog(self):
        """Generate a catalog of all processed data"""
        logger.info("\n📋 Generating data catalog...")
        
        records = []
        
        for var_type in ['SSM', 'RZSM', 'freeze_thaw']:
            var_dir = self.processed_dir / var_type
            
            if not var_dir.exists():
                continue
            
            for nc_file in sorted(var_dir.rglob("*.nc")):
                try:
                    ds = xr.open_dataset(nc_file)
                    
                    # Get all data variables (excluding 'crs')
                    data_vars = [v for v in ds.data_vars if v != 'crs']
                    
                    for var_name in data_vars:
                        var_data = ds[var_name]
                        
                        record = {
                            'variable_type': var_type,
                            'variable_name': var_name,
                            'file_path': str(nc_file.relative_to(self.base_dir)),
                            'year': None,
                            'month': None,
                            'time_agg': None,
                            'spatial_resolution': None,
                            'temporal_coverage': None,
                            'valid_pixels': int(np.isfinite(var_data).sum().values),
                            'total_pixels': int(var_data.size),
                            'file_size_mb': nc_file.stat().st_size / (1024**2)
                        }
                        
                        # Parse filename
                        parts = nc_file.stem.split('_')
                        if len(parts) >= 4:
                            record['year'] = int(parts[1])
                            record['month'] = int(parts[2])
                            record['time_agg'] = parts[3]
                        
                        # Get spatial resolution
                        if 'lat' in ds.dims and 'lon' in ds.dims:
                            lat_res = abs(float(ds.lat[1] - ds.lat[0]))
                            lon_res = abs(float(ds.lon[1] - ds.lon[0]))
                            record['spatial_resolution'] = f"{lat_res:.4f}° x {lon_res:.4f}°"
                        
                        # Get time coverage
                        if 'time' in ds.dims:
                            time_vals = pd.to_datetime(ds.time.values)
                            record['temporal_coverage'] = f"{time_vals[0]} to {time_vals[-1]}"
                        
                        # Stats for non-categorical variables
                        if var_type != 'freeze_thaw':
                            record['min_value'] = float(var_data.min().values)
                            record['max_value'] = float(var_data.max().values)
                            record['mean_value'] = float(var_data.mean().values)
                            record['std_value'] = float(var_data.std().values)
                        
                        records.append(record)
                    
                    ds.close()
                    
                except Exception as e:
                    logger.warning(f"⚠️  Could not process {nc_file}: {e}")
                    continue
        
        if records:
            df = pd.DataFrame(records)
            
            catalog_path = self.metadata_dir / "data_catalog.csv"
            df.to_csv(catalog_path, index=False)
            logger.info(f"✅ Data catalog saved: {catalog_path}")
            logger.info(f"   Total entries: {len(df)}")
            
            return df
        else:
            logger.warning("⚠️  No processed files found for catalog")
            return None
    
    def generate_statistics(self):
        """Generate and save processing statistics"""
        logger.info("\n📊 Generating statistics...")
        
        stats_summary = {
            'processing_stats': {
                **self.stats,
                'rzsm_layers_found': list(self.stats['rzsm_layers_found'])
            },
            'timestamp': datetime.now().isoformat(),
            'processed_directory': str(self.processed_dir),
            'variable_counts': {}
        }
        
        # Count files per variable
        for var_type in ['SSM', 'RZSM', 'freeze_thaw']:
            var_dir = self.processed_dir / var_type
            if var_dir.exists():
                nc_files = list(var_dir.rglob("*.nc"))
                stats_summary['variable_counts'][var_type] = len(nc_files)
        
        stats_path = self.metadata_dir / "statistics_summary.json"
        with open(stats_path, 'w') as f:
            json.dump(stats_summary, f, indent=2)
        
        logger.info(f"✅ Statistics saved: {stats_path}")
        
        return stats_summary
    
    def print_summary(self):
        """Print processing summary"""
        print("\n" + "="*70)
        print("PROCESSING SUMMARY")
        print("="*70)
        print(f"✅ Processed: {self.stats['processed']} file(s)")
        print(f"⏭️  Skipped: {self.stats['skipped']} file(s)")
        print(f"❌ Failed: {self.stats['failed']} file(s)")
        print(f"📦 Total size: {self.stats['total_size_mb']:.2f} MB")
        print("\nVariables extracted:")
        for var, count in self.stats['variables_extracted'].items():
            print(f"  {var}: {count} file(s)")
        
        if self.stats['rzsm_layers_found']:
            print(f"\nRZSM layers found: {sorted(self.stats['rzsm_layers_found'])}")
        
        print("="*70)


def main():
    """Command-line interface"""
    
    parser = argparse.ArgumentParser(
        description='Process ESA Soil Moisture Data - FIXED VERSION',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all downloaded files
  python %(prog)s
  
  # Process specific year
  python %(prog)s --year 2022
  
  # Process and generate catalog/stats
  python %(prog)s --stats
  
  # Resume interrupted processing
  python %(prog)s --resume
  
  # Force reprocess everything
  python %(prog)s --no-resume

FIXES:
  - Handles numbered RZSM variables (rzsm_1, rzsm_2, rzsm_3)
  - Uses ds.sizes instead of ds.dims
  - Properly extracts all soil layers
        """
    )
    
    parser.add_argument('--base-dir', type=str, default='./data/soil_moisture',
                       help='Base directory (default: ./data/soil_moisture)')
    parser.add_argument('--year', type=int, default=None,
                       help='Process only specific year')
    parser.add_argument('--stats', action='store_true',
                       help='Generate statistics and data catalog')
    parser.add_argument('--resume', action='store_true',
                       help='Resume from previous processing (skip completed)')
    parser.add_argument('--no-resume', action='store_true',
                       help='Reprocess everything (ignore progress)')
    
    args = parser.parse_args()
    
    print("="*70)
    print("ESA SOIL MOISTURE PROCESSOR - FIXED VERSION")
    print("="*70)
    print(f"Base directory: {args.base_dir}")
    print(f"Year filter: {args.year if args.year else 'all'}")
    print(f"Generate stats: {args.stats}")
    print(f"Resume mode: {not args.no_resume}")
    print("="*70)
    
    try:
        processor = SoilMoistureProcessor(base_dir=args.base_dir)
    except Exception as e:
        print(f"\n❌ Failed to initialize: {e}")
        return 1
    
    try:
        results = processor.process_all(
            year=args.year,
            skip_if_exists=not args.no_resume
        )
        
        processor.print_summary()
        
        if args.stats:
            catalog = processor.generate_data_catalog()
            stats = processor.generate_statistics()
            
            if catalog is not None:
                print(f"\n📋 Data catalog: {len(catalog)} entries")
        
        print(f"\nProcessed data: {args.base_dir}/processed/")
        print(f"Metadata: {args.base_dir}/metadata/")
        print("="*70)
        
        return 0 if processor.stats['failed'] == 0 else 1
        
    except KeyboardInterrupt:
        print("\n\n❌ Processing interrupted by user")
        print("Progress has been saved. Run with --resume to continue.")
        return 1
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())