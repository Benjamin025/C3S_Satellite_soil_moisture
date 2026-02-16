#!/usr/bin/env python3
"""
ESA SOIL MOISTURE DOWNLOADER - MONTHLY AVERAGES (FIXED)
========================================================
Downloads monthly average soil moisture data for pre-configured years.

FIXED: Added required 'day' parameter for monthly average requests

Variables downloaded:
- Surface Soil Moisture (SSM) - Volumetric
- Root Zone Soil Moisture (RZSM) - Volumetric  
- Freeze/Thaw Classification

Configuration:
- Years: Set in DOWNLOAD_YEARS below (default: 2020-2024)
- Time aggregation: Month average
- Sensor: Combined (active + passive)
- Region: Africa (can be changed in AREA_BOUNDS)

Data is automatically organized by year in the output directory.

Requirements:
pip install cdsapi

Usage:
python soil_moisture_monthly_fixed.py

Author: Production Ready
Date: February 2026
"""

import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime
import time

try:
    import cdsapi
except ImportError:
    print("ERROR: cdsapi not installed")
    print("Install with: pip install cdsapi")
    sys.exit(1)

# ============================================================================
# CONFIGURATION - EDIT THESE SETTINGS
# ============================================================================

# Years to download (edit this list)
# DOWNLOAD_YEARS = [1978, 1979, 1980, 1981, 1982, 1983, 1984, 1985, 1986, 
# 1987, 1988, 1989, 1990, 1991, 1992, 1993, 1994, 1995, 1996, 1997, 1998,
# 1999, 2000, 2001,2002, 2003, 2004,2005, 2006, 2007,2008, 2009, 2010,2011, 
# 2012, 2013,2014, 2015,2016, 2017, 2018, 2019,2020, 2021, 2022,2023, 2024,2025],

DOWNLOAD_YEARS = [2024]

# Months to download (1-12, or None for all months)
DOWNLOAD_MONTHS = None  # None = all months, or [1, 2, 3, 6, 7, 8] for specific months

# Region bounding box [North, West, South, East] in degrees
AREA_BOUNDS = [40, -20, -40, 55]  # Africa
# AREA_BOUNDS = [5, 33, -5, 42]  # Kenya
# AREA_BOUNDS = [15, 33, 3, 48]  # Ethiopia
# AREA_BOUNDS = None  # Global (very large files!)

# Variables to download
VARIABLES = [
    'surface_soil_moisture_volumetric',    # SSM
    'root_zone_soil_moisture_volumetric',  # RZSM
    'freeze_thaw_classification',          # Freeze/Thaw
]

# Sensor type
SENSOR_TYPE = 'combined'  # 'combined', 'active', or 'passive'

# Record type
RECORD_TYPE = 'cdr'  # 'cdr' (archive quality) or 'icdr' (near real-time)

# Dataset version
VERSION = 'v202505'  # Latest version

# Output directory
OUTPUT_DIR = './data/soil_moisture_monthly'

# Resume previous downloads (skip completed months)
RESUME_DOWNLOADS = True

# ============================================================================
# DO NOT EDIT BELOW THIS LINE
# ============================================================================

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('soil_moisture_monthly.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class MonthlyDownloader:
    """Downloads monthly average soil moisture data"""
    
    def __init__(self):
        """Initialize downloader"""
        
        # Create output directories
        self.output_dir = Path(OUTPUT_DIR)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.raw_dir = self.output_dir / "raw"
        self.raw_dir.mkdir(exist_ok=True)
        
        logger.info(f"📁 Output directory: {self.output_dir}")
        
        # Progress tracking
        self.progress_file = self.output_dir / "monthly_progress.json"
        self.progress = self.load_progress()
        
        # Initialize CDS client
        try:
            self.client = cdsapi.Client()
            logger.info("✅ CDS API client initialized")
        except Exception as e:
            logger.error(f"❌ Failed to initialize CDS client: {e}")
            logger.error("\nSetup ~/.cdsapirc with:")
            logger.error("  url: https://cds.climate.copernicus.eu/api")
            logger.error("  key: YOUR-API-KEY")
            logger.error("\nGet API key: https://cds.climate.copernicus.eu/account")
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
    
    def download_month(self, year, month):
        """
        Download monthly average data for one month
        
        Parameters:
        -----------
        year : int
        month : int
        
        Returns:
        --------
        str : Path to downloaded file, or None if failed/skipped
        """
        
        # Check if already completed
        if RESUME_DOWNLOADS and self.is_completed(year, month):
            logger.info(f"⏭️  Skipping {year}-{month:02d} (already completed)")
            return None
        
        # Format month
        month_str = f"{month:02d}"
        
        # Build request - CRITICAL: All parameters must be lists!
        request = {
            'variable': VARIABLES,
            'type_of_sensor': [SENSOR_TYPE],
            'time_aggregation': ['month_average'],  # EXACT value - lowercase with underscore!
            'year': [str(year)],
            'month': [month_str],
            'day': ['01'],  # REQUIRED for monthly averages (always set to '01')
            'type_of_record': [RECORD_TYPE],
            'version': [VERSION],
        }
        
        # Add area if specified
        if AREA_BOUNDS:
            request['area'] = AREA_BOUNDS
        
        # Create year subdirectory
        year_dir = self.raw_dir / str(year)
        year_dir.mkdir(exist_ok=True)
        
        # Output filename
        output_file = year_dir / f"soil_moisture_monthly_{year}_{month_str}.zip"
        
        # Log request
        logger.info("="*70)
        logger.info(f"📥 DOWNLOADING: {year}-{month_str}")
        logger.info("="*70)
        logger.info(f"Variables: {len(VARIABLES)} variables")
        for var in VARIABLES:
            logger.info(f"  - {var}")
        logger.info(f"Sensor: {SENSOR_TYPE}")
        logger.info(f"Time aggregation: Month average")
        logger.info(f"Day: 01 (required for monthly)")
        logger.info(f"Record type: {RECORD_TYPE}")
        logger.info(f"Version: {VERSION}")
        if AREA_BOUNDS:
            logger.info(f"Area: {AREA_BOUNDS}")
        logger.info(f"Output: {output_file}")
        logger.info("-"*70)
        logger.info("API Request:")
        for k, v in request.items():
            logger.info(f"  {k}: {v}")
        logger.info("="*70)
        
        try:
            logger.info("⏳ Submitting to CDS queue...")
            logger.info("   Monthly data typically takes 5-15 minutes")
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
            logger.info(f"📦 File: {output_file.name}")
            logger.info(f"💾 Size: {file_size:.2f} MB")
            logger.info(f"⏱️  Time: {elapsed/60:.1f} minutes")
            logger.info("="*70)
            
            # Mark as completed
            self.mark_completed(year, month)
            
            return str(output_file)
            
        except Exception as e:
            logger.error("="*70)
            logger.error("❌ DOWNLOAD FAILED!")
            logger.error("="*70)
            logger.error(f"Error: {e}")
            logger.error("-"*70)
            logger.error("Common issues:")
            logger.error("1. Terms not accepted at CDS website")
            logger.error("2. Invalid API key in ~/.cdsapirc")
            logger.error("3. CDS queue is full - try again later")
            logger.error("4. Missing 'day' parameter (now fixed in this version)")
            logger.error("="*70)
            
            # Mark as failed
            self.mark_failed(year, month, e)
            
            return None
    
    def download_year(self, year, months=None):
        """
        Download all months for a year
        
        Parameters:
        -----------
        year : int
        months : list, optional
            Specific months to download
        
        Returns:
        --------
        dict : Summary
        """
        
        if months is None:
            months = range(1, 13)  # All 12 months
        
        summary = {'success': [], 'skipped': [], 'failed': []}
        
        logger.info(f"\n{'='*70}")
        logger.info(f"📅 DOWNLOADING YEAR {year}")
        logger.info(f"Months: {list(months)}")
        logger.info(f"{'='*70}\n")
        
        for month in months:
            result = self.download_month(year, month)
            
            if result is None:
                if self.is_completed(year, month):
                    summary['skipped'].append((year, month))
                else:
                    summary['failed'].append((year, month))
            else:
                summary['success'].append((year, month))
            
            # Small delay between requests
            time.sleep(2)
        
        # Year summary
        logger.info(f"\n{'='*70}")
        logger.info(f"YEAR {year} SUMMARY")
        logger.info(f"{'='*70}")
        logger.info(f"✅ Success: {len(summary['success'])} months")
        logger.info(f"⏭️  Skipped: {len(summary['skipped'])} months")
        logger.info(f"❌ Failed: {len(summary['failed'])} months")
        logger.info(f"{'='*70}\n")
        
        return summary
    
    def download_all(self):
        """Download all configured years"""
        
        total_summary = {'success': [], 'skipped': [], 'failed': []}
        
        logger.info("\n" + "="*70)
        logger.info("🚀 STARTING MONTHLY SOIL MOISTURE DOWNLOAD")
        logger.info("="*70)
        logger.info(f"Years: {DOWNLOAD_YEARS}")
        logger.info(f"Months: {DOWNLOAD_MONTHS if DOWNLOAD_MONTHS else 'All (1-12)'}")
        logger.info(f"Variables: {len(VARIABLES)}")
        for var in VARIABLES:
            logger.info(f"  - {var}")
        logger.info(f"Sensor: {SENSOR_TYPE}")
        logger.info(f"Record type: {RECORD_TYPE}")
        logger.info(f"Version: {VERSION}")
        logger.info(f"Area: {AREA_BOUNDS if AREA_BOUNDS else 'Global'}")
        logger.info(f"Output: {self.output_dir}")
        logger.info(f"Resume: {RESUME_DOWNLOADS}")
        logger.info("="*70)
        
        # Calculate total
        total_years = len(DOWNLOAD_YEARS)
        months_per_year = len(DOWNLOAD_MONTHS) if DOWNLOAD_MONTHS else 12
        total_months = total_years * months_per_year
        
        logger.info(f"\n📊 Total to download: {total_months} months")
        logger.info(f"⏱️  Estimated time: ~{total_months * 0.2:.1f} hours")
        logger.info("\n⚠️  Press Ctrl+C to cancel\n")
        
        try:
            time.sleep(3)
        except KeyboardInterrupt:
            logger.info("\n❌ Cancelled by user")
            return None
        
        # Download each year
        for year in DOWNLOAD_YEARS:
            year_summary = self.download_year(year, DOWNLOAD_MONTHS)
            
            # Combine summaries
            total_summary['success'].extend(year_summary['success'])
            total_summary['skipped'].extend(year_summary['skipped'])
            total_summary['failed'].extend(year_summary['failed'])
        
        return total_summary


def main():
    """Main function"""
    
    # Print banner
    print("\n" + "="*70)
    print("ESA SOIL MOISTURE DOWNLOADER - MONTHLY AVERAGES (FIXED)")
    print("="*70)
    print("\nConfiguration:")
    print(f"  Years: {DOWNLOAD_YEARS}")
    print(f"  Months: {DOWNLOAD_MONTHS if DOWNLOAD_MONTHS else 'All (1-12)'}")
    print(f"  Variables: {len(VARIABLES)}")
    for var in VARIABLES:
        print(f"    - {var.replace('_', ' ').title()}")
    print(f"  Sensor: {SENSOR_TYPE.title()}")
    print(f"  Region: {AREA_BOUNDS if AREA_BOUNDS else 'Global'}")
    print(f"  Output: {OUTPUT_DIR}")
    print("\n✅ FIX: Added required 'day' parameter for monthly averages")
    print("="*70 + "\n")
    
    # Initialize downloader
    try:
        downloader = MonthlyDownloader()
    except Exception as e:
        logger.error(f"\n❌ Initialization failed: {e}")
        return 1
    
    # Download
    try:
        summary = downloader.download_all()
        
        if summary is None:
            return 0
        
        # Final summary
        print("\n" + "="*70)
        print("🎉 FINAL SUMMARY")
        print("="*70)
        print(f"✅ Successfully downloaded: {len(summary['success'])} months")
        print(f"⏭️  Skipped (already done): {len(summary['skipped'])} months")
        print(f"❌ Failed: {len(summary['failed'])} months")
        
        if summary['success']:
            print(f"\n📁 Downloaded files saved to:")
            print(f"   {OUTPUT_DIR}/raw/")
        
        if summary['failed']:
            print("\n❌ Failed months:")
            for year, month in summary['failed']:
                print(f"  - {year}-{month:02d}")
            print("\n💡 Check soil_moisture_monthly.log for details")
            print("💡 Run again to retry failed downloads")
        
        print(f"\n📄 Progress saved to: {OUTPUT_DIR}/monthly_progress.json")
        print(f"📋 Log file: soil_moisture_monthly.log")
        print("="*70 + "\n")
        
        return 0 if not summary['failed'] else 1
        
    except KeyboardInterrupt:
        print("\n\n❌ Download interrupted by user")
        print("💾 Progress has been saved")
        print("🔄 Run the script again to resume")
        return 1
    except Exception as e:
        logger.error(f"\n❌ Fatal error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())