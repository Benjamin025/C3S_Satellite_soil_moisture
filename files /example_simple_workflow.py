#!/usr/bin/env python3
"""
Simple Example: ESA Soil Moisture Download and Analysis
=========================================================

This script demonstrates a complete workflow from download to visualization.
Perfect for getting started with the ESA soil moisture dataset.

Author: Your Name
Date: February 2026
"""

import warnings
warnings.filterwarnings('ignore')

from pathlib import Path
import xarray as xr
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from esa_soil_moisture_workflow import SoilMoistureDownloader, SoilMoistureProcessor


def main():
    """
    Simple workflow example
    """
    
    print("="*70)
    print("ESA SOIL MOISTURE WORKFLOW - SIMPLE EXAMPLE")
    print("="*70)
    
    # -------------------------------------------------------------------------
    # CONFIGURATION
    # -------------------------------------------------------------------------
    
    # What to download
    YEAR = 2023
    MONTH = 6  # June
    PRODUCT = 'combined'  # Best quality
    VARIABLE = 'volumetric_surface_soil_moisture'
    RESOLUTION = 'monthly'
    
    # Where to focus (East Africa in this example)
    REGION = {
        'name': 'East Africa',
        'bbox': {'north': 15, 'south': -12, 'east': 52, 'west': 28}
    }
    
    # Drought threshold (m³/m³)
    DROUGHT_THRESHOLD = 0.15
    
    print(f"\nConfiguration:")
    print(f"  Date: {YEAR}-{MONTH:02d}")
    print(f"  Product: {PRODUCT}")
    print(f"  Region: {REGION['name']}")
    print(f"  Drought threshold: {DROUGHT_THRESHOLD} m³/m³")
    
    # -------------------------------------------------------------------------
    # STEP 1: DOWNLOAD DATA
    # -------------------------------------------------------------------------
    
    print("\n" + "="*70)
    print("STEP 1: DOWNLOADING DATA")
    print("="*70)
    
    # Initialize downloader
    downloader = SoilMoistureDownloader(output_dir="./data/raw")
    
    # Download data
    print(f"\nDownloading {YEAR}-{MONTH:02d} data...")
    print("This may take 5-15 minutes depending on CDS queue...")
    
    # UNCOMMENT TO ACTUALLY DOWNLOAD
    # downloaded_file = downloader.download_soil_moisture(
    #     year=YEAR,
    #     month=MONTH,
    #     product_type=PRODUCT,
    #     variable=VARIABLE,
    #     temporal_resolution=RESOLUTION,
    #     bbox=REGION['bbox']
    # )
    # print(f"✓ Download complete: {downloaded_file}")
    
    print("(Download section commented out - uncomment to execute)")
    
    # -------------------------------------------------------------------------
    # STEP 2: LOAD AND EXPLORE DATA
    # -------------------------------------------------------------------------
    
    print("\n" + "="*70)
    print("STEP 2: LOADING DATA")
    print("="*70)
    
    # Find downloaded NetCDF files
    nc_files = list(Path("./data/raw").glob("*.nc"))
    
    if not nc_files:
        print("\n⚠ No NetCDF files found in ./data/raw/")
        print("Please uncomment the download section above and run again.")
        return
    
    # Load the first file
    nc_file = nc_files[0]
    print(f"\nLoading: {nc_file.name}")
    
    ds = xr.open_dataset(nc_file)
    
    # Display information
    print("\nDataset Information:")
    print(f"  Dimensions: {dict(ds.dims)}")
    print(f"  Variables: {list(ds.data_vars)}")
    print(f"  Coordinates: {list(ds.coords)}")
    
    # Identify the soil moisture variable
    # (name may vary, so we check)
    possible_sm_vars = ['sm', 'volumetric_surface_soil_moisture', 'soil_moisture']
    sm_var = None
    for var in possible_sm_vars:
        if var in ds.data_vars:
            sm_var = var
            break
    
    if sm_var is None:
        print(f"\n⚠ Could not find soil moisture variable.")
        print(f"Available variables: {list(ds.data_vars)}")
        print("Please update the variable name in the script.")
        return
    
    print(f"\nUsing variable: '{sm_var}'")
    
    # -------------------------------------------------------------------------
    # STEP 3: BASIC STATISTICS
    # -------------------------------------------------------------------------
    
    print("\n" + "="*70)
    print("STEP 3: CALCULATING STATISTICS")
    print("="*70)
    
    sm_data = ds[sm_var]
    
    # Calculate statistics
    stats = {
        'Mean': float(sm_data.mean()),
        'Std': float(sm_data.std()),
        'Min': float(sm_data.min()),
        'Max': float(sm_data.max()),
        'Median': float(sm_data.median())
    }
    
    print(f"\n{REGION['name']} - Soil Moisture Statistics:")
    for stat_name, value in stats.items():
        print(f"  {stat_name:8s}: {value:.4f} m³/m³")
    
    # -------------------------------------------------------------------------
    # STEP 4: DROUGHT ANALYSIS
    # -------------------------------------------------------------------------
    
    print("\n" + "="*70)
    print("STEP 4: DROUGHT ANALYSIS")
    print("="*70)
    
    # Create drought mask
    drought_mask = sm_data < DROUGHT_THRESHOLD
    
    # Calculate drought extent
    drought_percent = (drought_mask.sum() / drought_mask.size) * 100
    
    print(f"\nDrought Analysis (threshold < {DROUGHT_THRESHOLD} m³/m³):")
    print(f"  Drought extent: {drought_percent:.2f}% of region")
    
    if drought_percent > 50:
        print("  ⚠ SEVERE DROUGHT CONDITIONS")
    elif drought_percent > 25:
        print("  ⚠ Moderate drought conditions")
    elif drought_percent > 10:
        print("  ⚠ Mild drought conditions")
    else:
        print("  ✓ Normal moisture conditions")
    
    # -------------------------------------------------------------------------
    # STEP 5: VISUALIZATION
    # -------------------------------------------------------------------------
    
    print("\n" + "="*70)
    print("STEP 5: CREATING VISUALIZATIONS")
    print("="*70)
    
    # Create figures directory
    Path("./figures").mkdir(exist_ok=True)
    
    # Get data for plotting (handle time dimension)
    if 'time' in sm_data.dims:
        plot_data = sm_data.isel(time=0)
    else:
        plot_data = sm_data
    
    # Figure 1: Soil Moisture Map
    print("\nCreating soil moisture map...")
    fig1 = plt.figure(figsize=(14, 10))
    ax1 = plt.axes(projection=ccrs.PlateCarree())
    
    im = plot_data.plot(
        ax=ax1,
        transform=ccrs.PlateCarree(),
        cmap='YlGnBu',
        vmin=0,
        vmax=0.5,
        cbar_kwargs={'label': 'Soil Moisture (m³/m³)', 'shrink': 0.7}
    )
    
    ax1.add_feature(cfeature.BORDERS, linewidth=0.5, alpha=0.7)
    ax1.add_feature(cfeature.COASTLINE, linewidth=0.8)
    ax1.add_feature(cfeature.LAKES, alpha=0.3)
    
    # Set extent to region
    bbox = REGION['bbox']
    ax1.set_extent([bbox['west'], bbox['east'], bbox['south'], bbox['north']])
    
    gl = ax1.gridlines(draw_labels=True, alpha=0.3)
    gl.top_labels = False
    gl.right_labels = False
    
    plt.title(f"Surface Soil Moisture - {REGION['name']}\n{YEAR}-{MONTH:02d}", 
             fontsize=14, fontweight='bold', pad=20)
    
    map_file = f"./figures/soil_moisture_{REGION['name'].replace(' ', '_').lower()}.png"
    plt.savefig(map_file, dpi=300, bbox_inches='tight')
    print(f"  ✓ Saved: {map_file}")
    plt.close()
    
    # Figure 2: Drought Map
    print("Creating drought map...")
    fig2 = plt.figure(figsize=(14, 10))
    ax2 = plt.axes(projection=ccrs.PlateCarree())
    
    if 'time' in drought_mask.dims:
        drought_plot = drought_mask.isel(time=0)
    else:
        drought_plot = drought_mask
    
    drought_plot.plot(
        ax=ax2,
        transform=ccrs.PlateCarree(),
        cmap='RdYlGn_r',
        add_colorbar=True,
        cbar_kwargs={'label': 'Drought (1=Yes, 0=No)', 'shrink': 0.7}
    )
    
    ax2.add_feature(cfeature.BORDERS, linewidth=0.5, alpha=0.7)
    ax2.add_feature(cfeature.COASTLINE, linewidth=0.8)
    ax2.add_feature(cfeature.LAKES, alpha=0.3)
    
    ax2.set_extent([bbox['west'], bbox['east'], bbox['south'], bbox['north']])
    
    gl = ax2.gridlines(draw_labels=True, alpha=0.3)
    gl.top_labels = False
    gl.right_labels = False
    
    plt.title(f"Drought Areas - {REGION['name']}\n" + 
             f"(SM < {DROUGHT_THRESHOLD} m³/m³) - {YEAR}-{MONTH:02d}", 
             fontsize=14, fontweight='bold', pad=20)
    
    drought_file = f"./figures/drought_{REGION['name'].replace(' ', '_').lower()}.png"
    plt.savefig(drought_file, dpi=300, bbox_inches='tight')
    print(f"  ✓ Saved: {drought_file}")
    plt.close()
    
    # -------------------------------------------------------------------------
    # STEP 6: EXPORT DATA
    # -------------------------------------------------------------------------
    
    print("\n" + "="*70)
    print("STEP 6: EXPORTING DATA")
    print("="*70)
    
    # Create output directory
    Path("./data/output").mkdir(parents=True, exist_ok=True)
    
    # Export statistics to text file
    stats_file = "./data/output/statistics_summary.txt"
    with open(stats_file, 'w') as f:
        f.write(f"Soil Moisture Statistics - {REGION['name']}\n")
        f.write(f"Date: {YEAR}-{MONTH:02d}\n")
        f.write(f"{'='*50}\n\n")
        
        f.write("Basic Statistics:\n")
        for stat_name, value in stats.items():
            f.write(f"  {stat_name:8s}: {value:.4f} m³/m³\n")
        
        f.write(f"\nDrought Analysis:\n")
        f.write(f"  Threshold: {DROUGHT_THRESHOLD} m³/m³\n")
        f.write(f"  Drought extent: {drought_percent:.2f}%\n")
    
    print(f"  ✓ Statistics saved: {stats_file}")
    
    # Export to CSV (optional - uncomment if needed)
    # csv_file = "./data/output/soil_moisture_data.csv"
    # df = sm_data.to_dataframe().reset_index()
    # df.to_csv(csv_file, index=False)
    # print(f"  ✓ Data exported to CSV: {csv_file}")
    
    # -------------------------------------------------------------------------
    # SUMMARY
    # -------------------------------------------------------------------------
    
    print("\n" + "="*70)
    print("WORKFLOW COMPLETE!")
    print("="*70)
    
    print("\nFiles created:")
    print(f"  📊 Figures:")
    print(f"     - {map_file}")
    print(f"     - {drought_file}")
    print(f"  📄 Statistics:")
    print(f"     - {stats_file}")
    
    print("\nNext steps:")
    print("  1. Review the generated maps in ./figures/")
    print("  2. Check statistics in ./data/output/")
    print("  3. Modify parameters to analyze different regions/dates")
    print("  4. Use the Jupyter notebook for interactive analysis")
    print("  5. Integrate with GPM precipitation data for comprehensive analysis")
    
    print("\n" + "="*70)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nTroubleshooting:")
        print("  1. Ensure CDS API is configured (~/.cdsapirc)")
        print("  2. Check that required packages are installed")
        print("  3. Verify NetCDF files are in ./data/raw/")
        print("  4. See SETUP_GUIDE.md for detailed instructions")
