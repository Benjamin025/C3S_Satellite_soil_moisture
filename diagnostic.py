#!/usr/bin/env python3
"""
ZIP CONTENTS INVESTIGATOR
=========================
Scans all ZIP files and reports what NetCDF files and variables they contain.
This helps understand the data structure before processing.

Usage:
    python investigate_zip_contents.py
    python investigate_zip_contents.py --base-dir /path/to/data
"""

import os
import sys
import zipfile
import argparse
from pathlib import Path
from collections import defaultdict
import json

try:
    import xarray as xr
    import pandas as pd
except ImportError:
    print("ERROR: Required packages not installed")
    print("Install with: pip install xarray netCDF4 pandas")
    sys.exit(1)


def investigate_zip_file(zip_path):
    """
    Investigate contents of a single ZIP file
    
    Returns:
        dict: Information about NetCDF files and variables
    """
    results = {
        'zip_file': str(zip_path),
        'netcdf_files': [],
        'variables_found': defaultdict(list)
    }
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            # Find all NetCDF files
            nc_files = [f for f in zf.namelist() if f.endswith('.nc')]
            
            if not nc_files:
                return results
            
            for nc_file in nc_files:
                nc_info = {
                    'filename': nc_file,
                    'variables': [],
                    'dimensions': {},
                    'coordinates': []
                }
                
                # Extract to temporary location and read
                temp_path = f"/tmp/{Path(nc_file).name}"
                zf.extract(nc_file, '/tmp')
                
                try:
                    # Open with xarray
                    with xr.open_dataset(temp_path) as ds:
                        # Get all data variables
                        nc_info['variables'] = list(ds.data_vars)
                        nc_info['coordinates'] = list(ds.coords)
                        nc_info['dimensions'] = {k: v for k, v in ds.dims.items()}
                        
                        # Categorize variables
                        for var in ds.data_vars:
                            # Track each unique variable name
                            results['variables_found'][var].append(nc_file)
                
                except Exception as e:
                    nc_info['error'] = str(e)
                
                finally:
                    # Clean up temp file
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                
                results['netcdf_files'].append(nc_info)
    
    except Exception as e:
        results['error'] = str(e)
    
    return results


def scan_all_zips(base_dir):
    """
    Scan all ZIP files in the raw directory
    """
    raw_dir = Path(base_dir) / "raw"
    
    if not raw_dir.exists():
        print(f"❌ Raw directory not found: {raw_dir}")
        return None
    
    # Find all ZIP files
    zip_files = list(raw_dir.rglob("*.zip"))
    
    print("="*80)
    print(f"INVESTIGATING {len(zip_files)} ZIP FILE(S)")
    print("="*80)
    
    if not zip_files:
        print("No ZIP files found!")
        return None
    
    all_results = []
    all_variables = defaultdict(int)
    variable_to_files = defaultdict(set)
    
    # Process each ZIP
    for i, zip_file in enumerate(sorted(zip_files), 1):
        print(f"\n[{i}/{len(zip_files)}] {zip_file.relative_to(base_dir)}")
        
        result = investigate_zip_file(zip_file)
        all_results.append(result)
        
        # Show what's in this ZIP
        if result['netcdf_files']:
            print(f"  NetCDF files: {len(result['netcdf_files'])}")
            
            for nc_info in result['netcdf_files']:
                print(f"    📄 {nc_info['filename']}")
                print(f"       Variables: {', '.join(nc_info['variables'])}")
                print(f"       Dimensions: {nc_info['dimensions']}")
                
                # Track variables
                for var in nc_info['variables']:
                    all_variables[var] += 1
                    variable_to_files[var].add(zip_file.name)
        else:
            print("  ⚠️  No NetCDF files found")
    
    # Print summary
    print("\n" + "="*80)
    print("SUMMARY - ALL UNIQUE VARIABLES FOUND")
    print("="*80)
    
    for var, count in sorted(all_variables.items(), key=lambda x: -x[1]):
        print(f"\n'{var}':")
        print(f"  Occurrences: {count} file(s)")
        print(f"  Found in ZIP files: {len(variable_to_files[var])}")
        print(f"  Example ZIPs: {', '.join(list(variable_to_files[var])[:3])}")
    
    # Create detailed report
    print("\n" + "="*80)
    print("VARIABLE CATEGORIZATION")
    print("="*80)
    
    # Categorize by likely type
    categories = {
        'Surface Soil Moisture': [],
        'Root Zone Soil Moisture': [],
        'Freeze/Thaw': [],
        'Quality Flags': [],
        'Metadata': [],
        'Other': []
    }
    
    for var in all_variables.keys():
        var_lower = var.lower()
        if 'sm' in var_lower and 'rzsm' not in var_lower:
            categories['Surface Soil Moisture'].append(var)
        elif 'rzsm' in var_lower:
            categories['Root Zone Soil Moisture'].append(var)
        elif 'freeze' in var_lower or 'thaw' in var_lower or var == 'flag':
            categories['Freeze/Thaw'].append(var)
        elif 'flag' in var_lower or 'uncertainty' in var_lower or 'error' in var_lower:
            categories['Quality Flags'].append(var)
        elif var in ['sensor', 'mode', 'freqbandID', 't0', 'dnflag']:
            categories['Metadata'].append(var)
        else:
            categories['Other'].append(var)
    
    for category, variables in categories.items():
        if variables:
            print(f"\n{category}:")
            for var in sorted(variables):
                print(f"  - {var} (in {all_variables[var]} files)")
    
    # Check for RZSM variants
    print("\n" + "="*80)
    print("RZSM VARIABLE INVESTIGATION")
    print("="*80)
    
    rzsm_vars = [v for v in all_variables.keys() if 'rzsm' in v.lower()]
    if rzsm_vars:
        print(f"Found {len(rzsm_vars)} RZSM-related variable(s):")
        for var in sorted(rzsm_vars):
            print(f"\n  '{var}':")
            print(f"    Count: {all_variables[var]}")
            print(f"    In ZIPs: {sorted(list(variable_to_files[var]))[:5]}")
    else:
        print("⚠️  No RZSM variables found!")
        print("Searching for variables with 'rz' or 'root'...")
        
        possible_rzsm = [v for v in all_variables.keys() 
                         if 'rz' in v.lower() or 'root' in v.lower()]
        if possible_rzsm:
            print(f"Possible candidates: {', '.join(possible_rzsm)}")
    
    # Save detailed report
    report = {
        'total_zips': len(zip_files),
        'all_variables': {k: v for k, v in all_variables.items()},
        'variable_to_files': {k: list(v) for k, v in variable_to_files.items()},
        'categories': categories,
        'detailed_results': all_results
    }
    
    return report


def main():
    parser = argparse.ArgumentParser(
        description='Investigate ZIP file contents for soil moisture data'
    )
    parser.add_argument('--base-dir', type=str, 
                       default='./data/soil_moisture_monthly',
                       help='Base directory containing raw/ subdirectory')
    parser.add_argument('--save-report', type=str,
                       help='Save detailed report to JSON file')
    
    args = parser.parse_args()
    
    print("ESA SOIL MOISTURE - ZIP CONTENTS INVESTIGATOR")
    print(f"Base directory: {args.base_dir}\n")
    
    # Scan all ZIPs
    report = scan_all_zips(args.base_dir)
    
    if report and args.save_report:
        with open(args.save_report, 'w') as f:
            json.dump(report, f, indent=2)
        print(f"\n📄 Detailed report saved to: {args.save_report}")
    
    print("\n" + "="*80)
    print("INVESTIGATION COMPLETE")
    print("="*80)


if __name__ == "__main__":
    main()