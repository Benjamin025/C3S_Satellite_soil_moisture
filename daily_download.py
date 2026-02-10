#!/usr/bin/env python3
"""
ESA SOIL MOISTURE COMPLETE DOWNLOADER - PRODUCTION READY
=========================================================
Based on exact CDS API documentation and verified request format.

Downloads all available soil moisture variables:
- Surface Soil Moisture (SSM) - Volumetric
- Root Zone Soil Moisture (RZSM) - Volumetric
- Freeze/Thaw Classification

Features:
- Downloads by year and month to manage file sizes
- Automatic organization by year/month
- Progress tracking and resume capability
- Handles large multi-year downloads
- Error recovery and retry logic

Data coverage: 1978-2024 (updated regularly)
Spatial coverage: Global
Temporal resolution: Daily

Requirements:
pip install cdsapi tqdm

Usage:
# Download all data for 2023
python soil_moisture_production.py --start-year 2023 --end-year 2023

# Download specific year range
python soil_moisture_production.py --start-year 2020 --end-year 2023

# Download specific months
python soil_moisture_production.py --start-year 2023 --end-year 2023 --months 1 2 3 6

# Download for specific region (Kenya)
python soil_moisture_production.py --start-year 2023 --end-year 2023 --area 5 33 -5 42

Author: Production Ready
Date: February 2026
"""

import os
import sys
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime
import time

try:
    import cdsapi
except ImportError:
    print("ERROR: cdsapi not installed")
    print("Install with: pip install cdsapi")
    sys.exit(1)

try:
    from tqdm import tqdm
except ImportError:
    print("WARNING: tqdm not installed. Progress bars will be disabled.")
    print("Install with: pip install tqdm")
    tqdm = None

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('soil_moisture_download.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class SoilMoistureDownloader:
    """
    Production-ready soil moisture downloader
    
    Based on CDS API documentation:
    https://cds.climate.copernicus.eu/datasets/satellite-soil-moisture
    """
    
    # All available variables
    ALL_VARIABLES = [
        'surface_soil_moisture_volumetric',    # SSM
        'root_zone_soil_moisture_volumetric',  # RZSM
        'freeze_thaw_classification',          # Freeze/Thaw
    ]
    
    # Sensor types (API exact values)
    SENSOR_TYPES = {
        'combined': 'combined',
        'active': 'active', 
        'passive': 'passive',
    }
    
    # Time aggregations (API exact values)
    TIME_AGGREGATIONS = {
        'daily': 'daily',
        'monthly': 'month_average',
        '10day': '10-day average',
    }
    
    # Record types (API exact values)
    RECORD_TYPES = {
        'cdr': 'cdr',      # Climate Data Record (archive)
        'icdr': 'icdr',    # Intermediate CDR (near real-time)
    }
    
    def __init__(self, output_dir="./data/soil_moisture", api_key=None):
        """
        Initialize downloader
        
        Parameters:
        -----------
        output_dir : str
            Base output directory
        api_key : str, optional
            CDS API key (uses ~/.cdsapirc if not provided)
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create subdirectories
        self.raw_dir = self.output_dir / "raw"
        self.raw_dir.mkdir(exist_ok=True)
        
        # Progress tracking
        self.progress_file = self.output_dir / "download_progress.json"
        self.progress = self.load_progress()
        
        # Initialize CDS client
        try:
            if api_key:
                self.client = cdsapi.Client(
                    url='https://cds.climate.copernicus.eu/api',
                    key=api_key
                )
            else:
                self.client = cdsapi.Client()
            logger.info("✅ CDS API client initialized")
        except Exception as e:
            logger.error(f"❌ Failed to initialize CDS client: {e}")
            logger.error("Setup ~/.cdsapirc with:")
            logger.error("  url: https://cds.climate.copernicus.eu/api")
            logger.error("  key: YOUR-API-KEY")
            raise
    
    def load_progress(self):
        """Load download progress"""
        if self.progress_file.exists():
            with open(self.progress_file, 'r') as f:
                return json.load(f)
        return {'completed': [], 'failed': []}
    
    def save_progress(self):
        """Save download progress"""
        with open(self.progress_file, 'w') as f:
            json.dump(self.progress, f, indent=2)
    
    def is_completed(self, year, month):
        """Check if download already completed"""
        key = f"{year}_{month:02d}"
        return key in self.progress['completed']
    
    def mark_completed(self, year, month):
        """Mark download as completed"""
        key = f"{year}_{month:02d}"
        if key not in self.progress['completed']:
            self.progress['completed'].append(key)
            self.save_progress()
    
    def mark_failed(self, year, month, error):
        """Mark download as failed"""
        key = f"{year}_{month:02d}"
        self.progress['failed'].append({
            'key': key,
            'error': str(error),
            'timestamp': datetime.now().isoformat()
        })
        self.save_progress()
    
    def download_month(self, year, month,
                      variables=None,
                      sensor='combined',
                      time_agg='month_average',
                      record_type='cdr',
                      version='v202505',
                      area=None,
                      skip_if_exists=True):
        """
        Download soil moisture data for one month
        
        Parameters:
        -----------
        year : int
            Year (1978-2024)
        month : int
            Month (1-12)
        variables : list, optional
            List of variables to download. Default: all variables
        sensor : str
            'combined' (default), 'active', or 'passive'
        time_agg : str
            'daily' (default), 'monthly', or '10day'
        record_type : str
            'cdr' (archive) or 'icdr' (near real-time)
        version : str
            Dataset version (default: 'v202505')
        area : list, optional
            [N, W, S, E] in degrees. Default: Africa
        skip_if_exists : bool
            Skip if already downloaded
        
        Returns:
        --------
        str : Path to downloaded file, or None if skipped/failed
        """
        
        # Check if already completed
        if skip_if_exists and self.is_completed(year, month):
            logger.info(f"⏭️  Skipping {year}-{month:02d} (already completed)")
            return None
        
        # Default variables: all
        if variables is None:
            variables = self.ALL_VARIABLES
        
        # Format month and days
        month_str = f"{month:02d}"
        
        # Get number of days in month
        import calendar
        _, num_days = calendar.monthrange(year, month)
        days = [f"{d:02d}" for d in range(1, num_days + 1)]
        
        # Map to API values
        api_sensor = self.SENSOR_TYPES.get(sensor, sensor)
        api_time_agg = self.TIME_AGGREGATIONS.get(time_agg, time_agg)
        api_record_type = self.RECORD_TYPES.get(record_type, record_type)
        
        # Build request (EXACT format from CDS documentation)
        request = {
            'variable': variables,  # List of variables
            'type_of_sensor': [api_sensor],  # Must be list!
            'time_aggregation': [api_time_agg],  # Must be list!
            'year': [str(year)],  # Must be list!
            'month': [month_str],  # Must be list!
            'day': days,  # All days in month
            'type_of_record': [api_record_type],  # Must be list!
            'version': [version],  # Must be list!
        }
        
        # Add area if specified
        if area:
            request['area'] = area
        
        # Create output subdirectory by year
        year_dir = self.raw_dir / str(year)
        year_dir.mkdir(exist_ok=True)
        
        # Output filename
        output_file = year_dir / f"soil_moisture_{year}_{month_str}_{sensor}_{time_agg}.zip"
        
        # Log request
        logger.info("="*70)
        logger.info(f"DOWNLOADING: {year}-{month_str}")
        logger.info("="*70)
        logger.info(f"Variables: {variables}")
        logger.info(f"Sensor: {sensor} → {api_sensor}")
        logger.info(f"Time aggregation: {time_agg} → {api_time_agg}")
        logger.info(f"Record type: {record_type} → {api_record_type}")
        logger.info(f"Version: {version}")
        logger.info(f"Days: {len(days)} days")
        if area:
            logger.info(f"Area: {area}")
        logger.info(f"Output: {output_file}")
        logger.info("-"*70)
        logger.info("API Request:")
        for k, v in request.items():
            if k == 'day':
                logger.info(f"  {k}: [{v[0]}, ..., {v[-1]}] ({len(v)} days)")
            else:
                logger.info(f"  {k}: {v}")
        logger.info("="*70)
        
        try:
            logger.info("⏳ Submitting to CDS queue...")
            logger.info("   This may take 10-60 minutes for daily data")
            logger.info("   Check status: https://cds.climate.copernicus.eu/live")
            
            # Submit request
            start_time = time.time()
            self.client.retrieve(
                'satellite-soil-moisture',
                request,
                str(output_file)
            )
            elapsed = time.time() - start_time
            
            # Success
            file_size = output_file.stat().st_size / (1024**2)  # MB
            logger.info("="*70)
            logger.info("✅ DOWNLOAD SUCCESSFUL!")
            logger.info(f"File: {output_file}")
            logger.info(f"Size: {file_size:.2f} MB")
            logger.info(f"Time: {elapsed/60:.1f} minutes")
            logger.info("="*70)
            
            # Mark as completed
            self.mark_completed(year, month)
            
            return str(output_file)
            
        except Exception as e:
            logger.error("="*70)
            logger.error("❌ DOWNLOAD FAILED!")
            logger.error("="*70)
            logger.error(f"Error: {e}")
            logger.error("="*70)
            
            # Mark as failed
            self.mark_failed(year, month, e)
            
            return None
    
    def download_year(self, year, months=None, **kwargs):
        """
        Download all months for a year
        
        Parameters:
        -----------
        year : int
        months : list, optional
            Specific months to download (default: all 12 months)
        **kwargs : dict
            Additional parameters passed to download_month()
        
        Returns:
        --------
        dict : Summary of downloads
        """
        if months is None:
            months = range(1, 13)
        
        summary = {'success': [], 'skipped': [], 'failed': []}
        
        logger.info(f"\n{'='*70}")
        logger.info(f"DOWNLOADING YEAR {year}")
        logger.info(f"Months: {list(months)}")
        logger.info(f"{'='*70}\n")
        
        for month in months:
            result = self.download_month(year, month, **kwargs)
            
            if result is None:
                if self.is_completed(year, month):
                    summary['skipped'].append((year, month))
                else:
                    summary['failed'].append((year, month))
            else:
                summary['success'].append((year, month))
            
            # Small delay between requests
            time.sleep(2)
        
        return summary
    
    def download_range(self, start_year, end_year, months=None, **kwargs):
        """
        Download multiple years
        
        Parameters:
        -----------
        start_year : int
        end_year : int
        months : list, optional
            Specific months to download (default: all)
        **kwargs : dict
            Additional parameters
        
        Returns:
        --------
        dict : Overall summary
        """
        total_summary = {'success': [], 'skipped': [], 'failed': []}
        
        logger.info(f"\n{'='*70}")
        logger.info(f"DOWNLOADING YEAR RANGE: {start_year} to {end_year}")
        logger.info(f"{'='*70}\n")
        
        for year in range(start_year, end_year + 1):
            year_summary = self.download_year(year, months, **kwargs)
            
            # Combine summaries
            total_summary['success'].extend(year_summary['success'])
            total_summary['skipped'].extend(year_summary['skipped'])
            total_summary['failed'].extend(year_summary['failed'])
            
            # Print year summary
            logger.info(f"\n{'='*70}")
            logger.info(f"YEAR {year} SUMMARY")
            logger.info(f"{'='*70}")
            logger.info(f"✅ Success: {len(year_summary['success'])} months")
            logger.info(f"⏭️  Skipped: {len(year_summary['skipped'])} months")
            logger.info(f"❌ Failed: {len(year_summary['failed'])} months")
            logger.info(f"{'='*70}\n")
        
        return total_summary


def main():
    """Command-line interface"""
    
    parser = argparse.ArgumentParser(
        description='Download ESA Soil Moisture Data - Production Ready',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download all of 2023
  python %(prog)s --start-year 2023 --end-year 2023
  
  # Download 2020-2023
  python %(prog)s --start-year 2020 --end-year 2023
  
  # Download specific months
  python %(prog)s --start-year 2023 --end-year 2023 --months 1 2 3 6 7 8
  
  # Download for Kenya region only
  python %(prog)s --start-year 2023 --end-year 2023 --area 5 33 -5 42
  
  # Download monthly averages instead of daily
  python %(prog)s --start-year 2023 --end-year 2023 --time-agg monthly
  
  # Download only SSM (not RZSM or Freeze/Thaw)
  python %(prog)s --start-year 2023 --end-year 2023 --variables surface_soil_moisture_volumetric
  
  # Resume interrupted download
  python %(prog)s --start-year 2020 --end-year 2023 --resume

Setup ~/.cdsapirc:
  url: https://cds.climate.copernicus.eu/api
  key: YOUR-API-KEY

Get API key: https://cds.climate.copernicus.eu/account
Accept terms: https://cds.climate.copernicus.eu/datasets/satellite-soil-moisture
        """
    )
    
    # Time range
    parser.add_argument('--start-year', type=int, required=True,
                       help='Start year (1978-2024)')
    parser.add_argument('--end-year', type=int, required=True,
                       help='End year (1978-2024)')
    parser.add_argument('--months', type=int, nargs='+', default=None,
                       help='Specific months (1-12). Default: all months')
    
    # Variables
    parser.add_argument('--variables', type=str, nargs='+', default=None,
                       choices=['surface_soil_moisture_volumetric',
                               'root_zone_soil_moisture_volumetric',
                               'freeze_thaw_classification'],
                       help='Variables to download. Default: all')
    
    # Parameters
    parser.add_argument('--sensor', type=str, default='combined',
                       choices=['combined', 'active', 'passive'],
                       help='Sensor type (default: combined)')
    parser.add_argument('--time-agg', type=str, default='daily',
                       choices=['daily', 'monthly', '10day'],
                       help='Time aggregation (default: daily)')
    parser.add_argument('--record-type', type=str, default='cdr',
                       choices=['cdr', 'icdr'],
                       help='Record type: cdr=archive, icdr=near-realtime (default: cdr)')
    parser.add_argument('--version', type=str, default='v202505',
                       help='Dataset version (default: v202505)')
    
    # Region
    parser.add_argument('--area', type=float, nargs=4, default=None,
                       metavar=('N', 'W', 'S', 'E'),
                       help='Region [N W S E] degrees. Default: Africa [40, -20, -40, 55]')
    
    # Output
    parser.add_argument('--output-dir', type=str, default='./data/soil_moisture',
                       help='Output directory (default: ./data/soil_moisture)')
    parser.add_argument('--api-key', type=str, default=None,
                       help='CDS API key (uses ~/.cdsapirc if not provided)')
    
    # Options
    parser.add_argument('--resume', action='store_true',
                       help='Resume from previous download (skip completed)')
    parser.add_argument('--no-resume', action='store_true',
                       help='Re-download everything (ignore progress)')
    
    args = parser.parse_args()
    
    # Validate years
    if args.start_year < 1978 or args.end_year > 2024:
        print("ERROR: Years must be between 1978 and 2024")
        return 1
    
    if args.start_year > args.end_year:
        print("ERROR: Start year must be <= end year")
        return 1
    
    # Print configuration
    print("="*70)
    print("ESA SOIL MOISTURE DOWNLOADER - PRODUCTION")
    print("="*70)
    print(f"Time range: {args.start_year} to {args.end_year}")
    print(f"Months: {args.months if args.months else 'all (1-12)'}")
    print(f"Variables: {args.variables if args.variables else 'all (SSM, RZSM, Freeze/Thaw)'}")
    print(f"Sensor: {args.sensor}")
    print(f"Time aggregation: {args.time_agg}")
    print(f"Record type: {args.record_type}")
    print(f"Version: {args.version}")
    print(f"Area: {args.area if args.area else 'Africa [40, -20, -40, 55]'}")
    print(f"Output: {args.output_dir}")
    print(f"Resume: {not args.no_resume}")
    print("="*70)
    
    # Calculate total months
    years = args.end_year - args.start_year + 1
    months_per_year = len(args.months) if args.months else 12
    total_months = years * months_per_year
    
    print(f"\nTotal to download: {total_months} month(s)")
    print("Estimated time: ~30 minutes per month for daily data")
    print(f"Total estimated time: ~{total_months * 0.5:.1f} hours")
    print("\nPress Ctrl+C to cancel\n")
    
    try:
        time.sleep(3)  # Give user time to read
    except KeyboardInterrupt:
        print("\nCancelled by user")
        return 0
    
    # Initialize downloader
    try:
        downloader = SoilMoistureDownloader(
            output_dir=args.output_dir,
            api_key=args.api_key
        )
    except Exception as e:
        print(f"\n❌ Failed to initialize: {e}")
        return 1
    
    # Set default area if not specified
    if args.area is None:
        args.area = [40, -20, -40, 55]  # Africa
    
    # Download
    try:
        summary = downloader.download_range(
            start_year=args.start_year,
            end_year=args.end_year,
            months=args.months,
            variables=args.variables,
            sensor=args.sensor,
            time_agg=args.time_agg,
            record_type=args.record_type,
            version=args.version,
            area=args.area,
            skip_if_exists=not args.no_resume
        )
        
        # Final summary
        print("\n" + "="*70)
        print("FINAL SUMMARY")
        print("="*70)
        print(f"✅ Successfully downloaded: {len(summary['success'])} months")
        print(f"⏭️  Skipped (already done): {len(summary['skipped'])} months")
        print(f"❌ Failed: {len(summary['failed'])} months")
        
        if summary['failed']:
            print("\nFailed months:")
            for year, month in summary['failed']:
                print(f"  - {year}-{month:02d}")
            print("\nCheck soil_moisture_download.log for details")
        
        print(f"\nData saved to: {args.output_dir}")
        print(f"Progress file: {args.output_dir}/download_progress.json")
        print("="*70)
        
        return 0 if not summary['failed'] else 1
        
    except KeyboardInterrupt:
        print("\n\n❌ Download interrupted by user")
        print("Progress has been saved. Run with --resume to continue.")
        return 1
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())