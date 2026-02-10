#!/usr/bin/env python3
"""
ESA SOIL MOISTURE DATA PROCESSOR - PRODUCTION READY
===================================================
Processes downloaded soil moisture ZIP files and extracts NetCDF bands.

Features:
- Extracts SSM, RZSM, and Freeze/Thaw variables from ZIP files
- Organizes by variable type and date
- Quality control and metadata extraction
- Progress tracking and resume capability
- Handles both daily and monthly aggregations
- Creates summary statistics and data catalog

Processing workflow:
1. Scans raw ZIP files
2. Extracts NetCDF files
3. Separates variables into organized structure
4. Generates metadata and statistics
5. Creates data catalog

Output structure:
data/soil_moisture/
├── raw/                          # Original ZIP files (input)
│   ├── 2022/
│   │   ├── soil_moisture_2022_01_combined_daily.zip
│   │   └── soil_moisture_2022_02_combined_daily.zip
│   └── 2023/
├── processed/                    # Processed NetCDF files (output)
│   ├── SSM/                      # Surface Soil Moisture
│   │   ├── 2022/
│   │   │   ├── SSM_2022_01_daily.nc
│   │   │   └── SSM_2022_02_daily.nc
│   │   └── 2023/
│   ├── RZSM/                     # Root Zone Soil Moisture
│   │   ├── 2022/
│   │   └── 2023/
│   └── freeze_thaw/              # Freeze/Thaw Classification
│       ├── 2022/
│       └── 2023/
├── metadata/                     # Metadata and catalogs
│   ├── data_catalog.csv
│   ├── processing_log.json
│   └── statistics_summary.json
└── processing_progress.json

Requirements:
pip install netCDF4 xarray pandas numpy tqdm

Usage:
# Process all downloaded files
python soil_moisture_processor.py

# Process specific year
python soil_moisture_processor.py --year 2022

# Process and generate statistics
python soil_moisture_processor.py --stats

# Resume interrupted processing
python soil_moisture_processor.py --resume

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
    - Organizes by date and variable type
    - Generates metadata and statistics
    """
    
    # Variable mapping (internal name → possible NetCDF variable names)
    # Each variable type can have multiple possible names in source files
    VARIABLE_MAPPING = {
        'SSM': ['sm'],                    # Surface soil moisture
        'RZSM': ['rzsm'],                 # Root zone soil moisture  
        'freeze_thaw': ['ft', 'flag']     # Freeze/thaw classification (can be 'ft' or 'flag')
    }
    
    # Full variable names for documentation
    VARIABLE_NAMES = {
        'SSM': 'surface_soil_moisture_volumetric',
        'RZSM': 'root_zone_soil_moisture_volumetric',
        'freeze_thaw': 'freeze_thaw_classification'
    }
    
    # File patterns to identify variable types
    # Some variables come in separate NetCDF files within the ZIP
    FILE_PATTERNS = {
        'SSM': ['SSMV', 'SSM'],           # Surface soil moisture files
        'RZSM': ['RZSM'],                 # Root zone files
        'freeze_thaw': ['FT', 'FREEZE']   # Freeze/thaw files
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
            'variables_extracted': {var: 0 for var in ['SSM', 'RZSM', 'freeze_thaw']}
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
        
        Adds EPSG:4326 (WGS84) CRS attributes to ensure proper georeferencing.
        
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
                'GeoTransform': 'None'  # Will be set based on actual grid
            }
        )
        
        # Add grid_mapping attribute to all data variables
        for var in ds.data_vars:
            if var != 'crs':  # Don't add to CRS itself
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
            # NOTE: ZIP files may contain multiple NetCDF files, each with different variables:
            # - SSM files: contain 'sm' variable (surface soil moisture)
            # - RZSM files: contain 'rzsm' variable (root zone soil moisture)  
            # - FT files: contain 'ft' or 'flag' variable (freeze/thaw classification)
            # We process all NetCDF files and extract the relevant variables from each
            for nc_file in nc_files:
                nc_path = temp_dir / nc_file
                
                logger.info(f"\n📊 Processing: {nc_file}")
                
                # Open with xarray
                try:
                    ds = xr.open_dataset(nc_path)
                    
                    logger.info(f"Variables in file: {list(ds.data_vars)}")
                    logger.info(f"Dimensions: {dict(ds.dims)}")
                    
                    # Extract each variable type
                    for var_type, possible_var_names in self.VARIABLE_MAPPING.items():
                        # Find which variable name exists in this file
                        nc_var = None
                        for possible_name in possible_var_names:
                            if possible_name in ds.data_vars:
                                nc_var = possible_name
                                break
                        
                        if nc_var:
                            logger.info(f"  ✓ Found {var_type} ({nc_var})")
                            
                            # Extract this variable
                            var_ds = ds[[nc_var]]
                            
                            # Add CRS information (EPSG:4326)
                            var_ds = self.add_crs_information(var_ds)
                            logger.info(f"  🌍 Added CRS: EPSG:4326")
                            
                            # Create year directory for this variable
                            var_dir = self.processed_dir / var_type / str(year)
                            var_dir.mkdir(parents=True, exist_ok=True)
                            
                            # Output filename
                            output_name = f"{var_type}_{year}_{month:02d}_{time_agg}.nc"
                            output_path = var_dir / output_name
                            
                            # Save with CRS information
                            logger.info(f"  💾 Saving to: {output_path}")
                            var_ds.to_netcdf(output_path)

                            
                            # Get stats
                            var_data = ds[nc_var]
                            valid_pixels = np.isfinite(var_data).sum().values
                            
                            logger.info(f"  📈 Valid pixels: {valid_pixels:,}")
                            logger.info(f"  📏 Shape: {var_data.shape}")
                            
                            if var_type != 'freeze_thaw':  # Freeze/thaw is categorical
                                logger.info(f"  📊 Min: {float(var_data.min().values):.4f}")
                                logger.info(f"  📊 Max: {float(var_data.max().values):.4f}")
                                logger.info(f"  📊 Mean: {float(var_data.mean().values):.4f}")
                            
                            results['variables_found'].append(var_type)
                            results['output_files'].append(str(output_path))
                            self.stats['variables_extracted'][var_type] += 1
                        else:
                            # Show which variable names were searched for
                            searched_names = ', '.join(possible_var_names)
                            logger.info(f"  ⚠️  {var_type} (searched: {searched_names}) not found in file")
                    
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
            
            # Small delay between files
            time.sleep(0.5)
        
        return all_results
    
    def generate_data_catalog(self):
        """
        Generate a catalog of all processed data
        
        Returns:
        --------
        pd.DataFrame : Data catalog
        """
        logger.info("\n📋 Generating data catalog...")
        
        records = []
        
        for var_type in ['SSM', 'RZSM', 'freeze_thaw']:
            var_dir = self.processed_dir / var_type
            
            if not var_dir.exists():
                continue
            
            for nc_file in sorted(var_dir.rglob("*.nc")):
                try:
                    # Open file
                    ds = xr.open_dataset(nc_file)
                    
                    # Find which variable name exists in this file
                    nc_var = None
                    possible_var_names = self.VARIABLE_MAPPING[var_type]
                    for possible_name in possible_var_names:
                        if possible_name in ds.data_vars:
                            nc_var = possible_name
                            break
                    
                    if nc_var and nc_var in ds.data_vars:
                        var_data = ds[nc_var]
                        
                        # Extract metadata
                        record = {
                            'variable_type': var_type,
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
                        
                        # Get spatial resolution from attributes
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
        
        # Create DataFrame
        if records:
            df = pd.DataFrame(records)
            
            # Save catalog
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
            'processing_stats': self.stats,
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
        
        # Save statistics
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
        print("="*70)


def main():
    """Command-line interface"""
    
    parser = argparse.ArgumentParser(
        description='Process ESA Soil Moisture Data - Production Ready',
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

Output structure:
  processed/
    SSM/          # Surface Soil Moisture
    RZSM/         # Root Zone Soil Moisture
    freeze_thaw/  # Freeze/Thaw Classification
  metadata/
    data_catalog.csv
    statistics_summary.json
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
    
    # Print configuration
    print("="*70)
    print("ESA SOIL MOISTURE PROCESSOR - PRODUCTION")
    print("="*70)
    print(f"Base directory: {args.base_dir}")
    print(f"Year filter: {args.year if args.year else 'all'}")
    print(f"Generate stats: {args.stats}")
    print(f"Resume mode: {not args.no_resume}")
    print("="*70)
    
    # Initialize processor
    try:
        processor = SoilMoistureProcessor(base_dir=args.base_dir)
    except Exception as e:
        print(f"\n❌ Failed to initialize: {e}")
        return 1
    
    # Process files
    try:
        results = processor.process_all(
            year=args.year,
            skip_if_exists=not args.no_resume
        )
        
        # Print summary
        processor.print_summary()
        
        # Generate catalog and stats if requested
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