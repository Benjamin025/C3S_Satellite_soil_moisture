#!/usr/bin/env python3
"""
SOIL MOISTURE VISUALIZATION AND TREND ANALYSIS
==============================================
Visualizes and analyzes SSM, RZSM, and Freeze/Thaw data over Africa.

Features:
- Time series analysis for all three variables
- Spatial maps showing average conditions
- Seasonal trends and patterns
- Monthly comparisons
- Statistical summaries
- Interactive and static plots

Usage:
    python visualize_soil_moisture.py
    python visualize_soil_moisture.py --year 2020
    python visualize_soil_moisture.py --variable SSM
    python visualize_soil_moisture.py --output-dir ./plots

Author: Production Ready
Date: February 2026
"""

import os
import sys
import argparse
import warnings
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.gridspec import GridSpec
import seaborn as sns

try:
    import xarray as xr
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature
except ImportError:
    print("ERROR: Required packages not installed")
    print("Install with: pip install xarray netCDF4 matplotlib cartopy seaborn pandas")
    sys.exit(1)

warnings.filterwarnings('ignore')

# Set style
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")


class SoilMoistureVisualizer:
    """
    Comprehensive visualization tool for soil moisture data
    """
    
    def __init__(self, base_dir="./data/soil_moisture", output_dir="./plots"):
        """
        Initialize visualizer
        
        Parameters:
        -----------
        base_dir : str
            Base directory containing processed/ subdirectory
        output_dir : str
            Directory to save plots
        """
        self.base_dir = Path(base_dir)
        self.processed_dir = self.base_dir / "processed"
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        print("="*70)
        print("SOIL MOISTURE VISUALIZER - INITIALIZED")
        print("="*70)
        print(f"Data directory: {self.processed_dir}")
        print(f"Output directory: {self.output_dir}")
        print("="*70)
        
        # Data containers
        self.data = {
            'SSM': None,
            'RZSM': None,
            'freeze_thaw': None
        }
        
        # Metadata
        self.spatial_extent = None
        self.time_range = None
    
    def load_data(self, variable, year=None):
        """
        Load all data for a specific variable
        
        Parameters:
        -----------
        variable : str
            'SSM', 'RZSM', or 'freeze_thaw'
        year : int, optional
            Load only specific year
        
        Returns:
        --------
        xarray.Dataset : Combined dataset
        """
        var_dir = self.processed_dir / variable
        
        if not var_dir.exists():
            print(f"⚠️  No data found for {variable}")
            return None
        
        # Find all NetCDF files
        if year:
            nc_files = sorted((var_dir / str(year)).glob("*.nc"))
        else:
            nc_files = sorted(var_dir.rglob("*.nc"))
        
        if not nc_files:
            print(f"⚠️  No NetCDF files found for {variable}")
            return None
        
        print(f"\n📊 Loading {variable} data...")
        print(f"   Found {len(nc_files)} file(s)")
        
        # Load all files
        datasets = []
        for nc_file in nc_files:
            try:
                ds = xr.open_dataset(nc_file)
                datasets.append(ds)
            except Exception as e:
                print(f"   ⚠️  Could not load {nc_file.name}: {e}")
        
        if not datasets:
            return None
        
        # Concatenate along time dimension
        try:
            combined = xr.concat(datasets, dim='time')
            combined = combined.sortby('time')
            
            print(f"   ✓ Loaded {len(datasets)} file(s)")
            print(f"   Time range: {pd.to_datetime(combined.time.values[0]).strftime('%Y-%m-%d')} to {pd.to_datetime(combined.time.values[-1]).strftime('%Y-%m-%d')}")
            print(f"   Spatial extent: {float(combined.lat.min()):.2f}°N to {float(combined.lat.max()):.2f}°N, {float(combined.lon.min()):.2f}°E to {float(combined.lon.max()):.2f}°E")
            
            self.data[variable] = combined
            return combined
            
        except Exception as e:
            print(f"   ❌ Error concatenating datasets: {e}")
            return None
    
    def load_all_variables(self, year=None):
        """Load all three variables"""
        print("\n" + "="*70)
        print("LOADING ALL VARIABLES")
        print("="*70)
        
        for var in ['SSM', 'RZSM', 'freeze_thaw']:
            self.load_data(var, year)
        
        # Get spatial extent from any loaded data
        for var in ['SSM', 'RZSM', 'freeze_thaw']:
            if self.data[var] is not None:
                ds = self.data[var]
                self.spatial_extent = {
                    'lat_min': float(ds.lat.min()),
                    'lat_max': float(ds.lat.max()),
                    'lon_min': float(ds.lon.min()),
                    'lon_max': float(ds.lon.max())
                }
                break
        
        print("\n✓ Data loading complete")
    
    def plot_time_series(self, variable='SSM', roi=None, save=True):
        """
        Plot time series of mean values
        
        Parameters:
        -----------
        variable : str
            Variable to plot
        roi : dict, optional
            Region of interest: {'lat_min': ..., 'lat_max': ..., 'lon_min': ..., 'lon_max': ...}
        save : bool
            Save plot
        """
        ds = self.data[variable]
        if ds is None:
            print(f"⚠️  No data loaded for {variable}")
            return
        
        print(f"\n📈 Creating time series plot for {variable}...")
        
        # Get the main variable(s)
        if variable == 'SSM':
            var_names = ['sm']
        elif variable == 'RZSM':
            # Find all RZSM variables (rzsm_1, rzsm_2, rzsm_3)
            var_names = [v for v in ds.data_vars if v.startswith('rzsm') and v != 'rzsm']
            if not var_names:
                var_names = ['rzsm'] if 'rzsm' in ds.data_vars else []
        else:  # freeze_thaw
            var_names = ['flag'] if 'flag' in ds.data_vars else ['ft']
        
        if not var_names:
            print(f"⚠️  No variables found in {variable} dataset")
            return
        
        # Subset to ROI if specified
        if roi:
            ds_subset = ds.sel(
                lat=slice(roi['lat_min'], roi['lat_max']),
                lon=slice(roi['lon_min'], roi['lon_max'])
            )
        else:
            ds_subset = ds
        
        # Create figure
        fig, axes = plt.subplots(len(var_names), 1, figsize=(14, 4*len(var_names)))
        if len(var_names) == 1:
            axes = [axes]
        
        for i, var_name in enumerate(var_names):
            ax = axes[i]
            
            # Calculate spatial mean for each time step
            data = ds_subset[var_name]
            
            if variable == 'freeze_thaw':
                # For freeze/thaw, show percentage of different states
                time_vals = pd.to_datetime(data.time.values)
                
                # Assuming freeze/thaw flags: 0=thaw, 1=freeze
                # Calculate percentage of frozen pixels
                frozen_pct = []
                for t in range(len(time_vals)):
                    vals = data.isel(time=t).values
                    valid_mask = np.isfinite(vals)
                    if valid_mask.sum() > 0:
                        frozen = (vals[valid_mask] == 1).sum() / valid_mask.sum() * 100
                        frozen_pct.append(frozen)
                    else:
                        frozen_pct.append(np.nan)
                
                ax.plot(time_vals, frozen_pct, linewidth=2, label='Frozen (%)', color='#2E86AB')
                ax.fill_between(time_vals, frozen_pct, alpha=0.3, color='#2E86AB')
                ax.set_ylabel('Frozen Area (%)', fontsize=12, fontweight='bold')
                ax.set_ylim(0, 100)
                
            else:
                # For SSM and RZSM, show mean values
                mean_vals = data.mean(dim=['lat', 'lon']).values
                std_vals = data.std(dim=['lat', 'lon']).values
                time_vals = pd.to_datetime(data.time.values)
                
                # Plot mean with confidence interval
                ax.plot(time_vals, mean_vals, linewidth=2, label=f'Mean {var_name}', color='#A23B72')
                ax.fill_between(time_vals, 
                               mean_vals - std_vals, 
                               mean_vals + std_vals, 
                               alpha=0.3, 
                               color='#A23B72',
                               label='±1 Std Dev')
                
                # Add labels
                if variable == 'SSM':
                    ax.set_ylabel('Soil Moisture (m³/m³)', fontsize=12, fontweight='bold')
                    ax.set_ylim(0, 0.5)
                else:
                    layer_name = var_name.replace('rzsm_', 'Layer ')
                    ax.set_ylabel(f'{layer_name} Moisture (m³/m³)', fontsize=12, fontweight='bold')
                    ax.set_ylim(0, 0.5)
            
            # Format x-axis
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
            ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
            
            # Grid and legend
            ax.grid(True, alpha=0.3)
            ax.legend(loc='upper right', fontsize=10)
            
            # Title for first subplot
            if i == 0:
                title = f"{variable} Time Series Analysis"
                if roi:
                    title += f"\nROI: {roi['lat_min']:.1f}°-{roi['lat_max']:.1f}°N, {roi['lon_min']:.1f}°-{roi['lon_max']:.1f}°E"
                else:
                    title += "\nFull Study Area"
                ax.set_title(title, fontsize=14, fontweight='bold', pad=15)
        
        axes[-1].set_xlabel('Date', fontsize=12, fontweight='bold')
        
        plt.tight_layout()
        
        if save:
            filename = f"{variable}_timeseries.png"
            filepath = self.output_dir / filename
            plt.savefig(filepath, dpi=300, bbox_inches='tight')
            print(f"   ✓ Saved: {filepath}")
        
        return fig
    
    def plot_spatial_average(self, variable='SSM', save=True):
        """
        Plot spatial map of average conditions
        
        Parameters:
        -----------
        variable : str
            Variable to plot
        save : bool
            Save plot
        """
        ds = self.data[variable]
        if ds is None:
            print(f"⚠️  No data loaded for {variable}")
            return
        
        print(f"\n🗺️  Creating spatial map for {variable}...")
        
        # Get the main variable(s)
        if variable == 'SSM':
            var_names = ['sm']
        elif variable == 'RZSM':
            var_names = [v for v in ds.data_vars if v.startswith('rzsm') and v != 'rzsm']
            if not var_names:
                var_names = ['rzsm'] if 'rzsm' in ds.data_vars else []
        else:
            var_names = ['flag'] if 'flag' in ds.data_vars else ['ft']
        
        if not var_names:
            print(f"⚠️  No variables found")
            return
        
        # Create figure with subplots
        n_vars = len(var_names)
        fig = plt.figure(figsize=(15, 5*n_vars))
        
        for i, var_name in enumerate(var_names, 1):
            # Calculate temporal mean
            mean_data = ds[var_name].mean(dim='time')
            
            # Create subplot with Cartopy projection
            ax = fig.add_subplot(n_vars, 1, i, projection=ccrs.PlateCarree())
            
            # Add map features
            ax.add_feature(cfeature.BORDERS, linewidth=0.5, edgecolor='black')
            ax.add_feature(cfeature.COASTLINE, linewidth=0.5)
            ax.add_feature(cfeature.LAND, facecolor='lightgray', alpha=0.3)
            ax.add_feature(cfeature.OCEAN, facecolor='lightblue', alpha=0.3)
            
            # Plot data
            if variable == 'freeze_thaw':
                # For freeze/thaw, show categorical data
                im = ax.pcolormesh(
                    mean_data.lon, mean_data.lat, mean_data,
                    transform=ccrs.PlateCarree(),
                    cmap='RdYlBu',
                    vmin=0, vmax=1,
                    shading='auto'
                )
                cbar_label = 'Freeze/Thaw State'
            else:
                # For SSM/RZSM, show continuous data
                im = ax.pcolormesh(
                    mean_data.lon, mean_data.lat, mean_data,
                    transform=ccrs.PlateCarree(),
                    cmap='YlGnBu',
                    vmin=0, vmax=0.4,
                    shading='auto'
                )
                cbar_label = 'Soil Moisture (m³/m³)'
            
            # Add colorbar
            cbar = plt.colorbar(im, ax=ax, orientation='horizontal', pad=0.05, shrink=0.8)
            cbar.set_label(cbar_label, fontsize=11, fontweight='bold')
            
            # Set extent
            ax.set_extent([
                self.spatial_extent['lon_min'],
                self.spatial_extent['lon_max'],
                self.spatial_extent['lat_min'],
                self.spatial_extent['lat_max']
            ], crs=ccrs.PlateCarree())
            
            # Gridlines
            gl = ax.gridlines(draw_labels=True, linewidth=0.5, alpha=0.5, linestyle='--')
            gl.top_labels = False
            gl.right_labels = False
            
            # Title
            if variable == 'RZSM':
                layer_name = var_name.replace('rzsm_', 'Layer ')
                title = f"{variable} - {layer_name} - Temporal Average"
            else:
                title = f"{variable} - Temporal Average"
            
            ax.set_title(title, fontsize=13, fontweight='bold', pad=10)
        
        plt.tight_layout()
        
        if save:
            filename = f"{variable}_spatial_average.png"
            filepath = self.output_dir / filename
            plt.savefig(filepath, dpi=300, bbox_inches='tight')
            print(f"   ✓ Saved: {filepath}")
        
        return fig
    
    def plot_monthly_comparison(self, variable='SSM', year=2020, save=True):
        """
        Plot monthly comparison for a specific year
        
        Parameters:
        -----------
        variable : str
            Variable to plot
        year : int
            Year to analyze
        save : bool
            Save plot
        """
        ds = self.data[variable]
        if ds is None:
            print(f"⚠️  No data loaded for {variable}")
            return
        
        print(f"\n📅 Creating monthly comparison for {variable} ({year})...")
        
        # Filter to specific year
        ds_year = ds.sel(time=str(year))
        
        if len(ds_year.time) == 0:
            print(f"⚠️  No data for year {year}")
            return
        
        # Get variable name
        if variable == 'SSM':
            var_name = 'sm'
        elif variable == 'RZSM':
            var_names = [v for v in ds.data_vars if v.startswith('rzsm') and v != 'rzsm']
            var_name = var_names[0] if var_names else 'rzsm'
        else:
            var_name = 'flag' if 'flag' in ds.data_vars else 'ft'
        
        # Create 4x3 grid for 12 months
        fig = plt.figure(figsize=(20, 16))
        gs = GridSpec(4, 3, figure=fig, hspace=0.3, wspace=0.3)
        
        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                      'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        
        for month in range(1, 13):
            try:
                # Select data for this month
                ds_month = ds_year.sel(time=f"{year}-{month:02d}")
                
                # If multiple time steps in month, take mean
                if len(ds_month.time) > 1:
                    monthly_data = ds_month[var_name].mean(dim='time')
                else:
                    monthly_data = ds_month[var_name].isel(time=0)
                
                # Create subplot
                row = (month - 1) // 3
                col = (month - 1) % 3
                ax = fig.add_subplot(gs[row, col], projection=ccrs.PlateCarree())
                
                # Add map features
                ax.add_feature(cfeature.BORDERS, linewidth=0.3)
                ax.add_feature(cfeature.COASTLINE, linewidth=0.3)
                
                # Plot
                if variable == 'freeze_thaw':
                    im = ax.pcolormesh(
                        monthly_data.lon, monthly_data.lat, monthly_data,
                        transform=ccrs.PlateCarree(),
                        cmap='RdYlBu', vmin=0, vmax=1, shading='auto'
                    )
                else:
                    im = ax.pcolormesh(
                        monthly_data.lon, monthly_data.lat, monthly_data,
                        transform=ccrs.PlateCarree(),
                        cmap='YlGnBu', vmin=0, vmax=0.4, shading='auto'
                    )
                
                # Set extent
                ax.set_extent([
                    self.spatial_extent['lon_min'],
                    self.spatial_extent['lon_max'],
                    self.spatial_extent['lat_min'],
                    self.spatial_extent['lat_max']
                ])
                
                # Title
                ax.set_title(f"{month_names[month-1]} {year}", fontsize=11, fontweight='bold')
                
            except (KeyError, IndexError):
                # Month not available
                ax = fig.add_subplot(gs[row, col])
                ax.text(0.5, 0.5, f"{month_names[month-1]}\nNo Data", 
                       ha='center', va='center', fontsize=12)
                ax.axis('off')
        
        # Add overall title
        fig.suptitle(f"{variable} - Monthly Comparison {year}", 
                    fontsize=16, fontweight='bold', y=0.98)
        
        # Add single colorbar
        cbar_ax = fig.add_axes([0.92, 0.15, 0.02, 0.7])
        cbar = fig.colorbar(im, cax=cbar_ax)
        if variable == 'freeze_thaw':
            cbar.set_label('Freeze/Thaw State', fontsize=11, fontweight='bold')
        else:
            cbar.set_label('Soil Moisture (m³/m³)', fontsize=11, fontweight='bold')
        
        if save:
            filename = f"{variable}_monthly_{year}.png"
            filepath = self.output_dir / filename
            plt.savefig(filepath, dpi=300, bbox_inches='tight')
            print(f"   ✓ Saved: {filepath}")
        
        return fig
    
    def plot_seasonal_trends(self, save=True):
        """
        Compare all three variables in seasonal trends
        """
        print(f"\n🌍 Creating seasonal trends comparison...")
        
        # Check which data is available
        available_vars = [v for v in ['SSM', 'RZSM', 'freeze_thaw'] 
                         if self.data[v] is not None]
        
        if len(available_vars) < 2:
            print("⚠️  Need at least 2 variables loaded")
            return
        
        fig, axes = plt.subplots(len(available_vars), 1, figsize=(14, 5*len(available_vars)))
        if len(available_vars) == 1:
            axes = [axes]
        
        for i, var in enumerate(available_vars):
            ds = self.data[var]
            ax = axes[i]
            
            # Get variable name
            if var == 'SSM':
                var_name = 'sm'
            elif var == 'RZSM':
                var_names = [v for v in ds.data_vars if v.startswith('rzsm') and v != 'rzsm']
                var_name = var_names[0] if var_names else 'rzsm'
            else:
                var_name = 'flag' if 'flag' in ds.data_vars else 'ft'
            
            # Calculate spatial mean
            mean_vals = ds[var_name].mean(dim=['lat', 'lon']).values
            time_vals = pd.to_datetime(ds.time.values)
            
            # Create DataFrame for seasonal analysis
            df = pd.DataFrame({
                'time': time_vals,
                'value': mean_vals,
                'month': [t.month for t in time_vals]
            })
            
            # Group by month to show seasonal pattern
            monthly_mean = df.groupby('month')['value'].agg(['mean', 'std'])
            months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                     'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            
            # Plot
            x = np.arange(1, 13)
            ax.plot(x, monthly_mean['mean'].values, marker='o', linewidth=2, 
                   markersize=8, label=f'{var} Mean')
            ax.fill_between(x, 
                           monthly_mean['mean'] - monthly_mean['std'],
                           monthly_mean['mean'] + monthly_mean['std'],
                           alpha=0.3)
            
            ax.set_xticks(x)
            ax.set_xticklabels(months)
            ax.set_xlabel('Month', fontsize=12, fontweight='bold')
            
            if var == 'freeze_thaw':
                ax.set_ylabel('Freeze/Thaw Index', fontsize=12, fontweight='bold')
            else:
                ax.set_ylabel('Soil Moisture (m³/m³)', fontsize=12, fontweight='bold')
            
            ax.set_title(f"{var} - Seasonal Pattern (All Years)", 
                        fontsize=13, fontweight='bold')
            ax.grid(True, alpha=0.3)
            ax.legend()
        
        plt.tight_layout()
        
        if save:
            filename = "seasonal_trends_comparison.png"
            filepath = self.output_dir / filename
            plt.savefig(filepath, dpi=300, bbox_inches='tight')
            print(f"   ✓ Saved: {filepath}")
        
        return fig
    
    def generate_statistics_report(self):
        """
        Generate statistical summary report
        """
        print(f"\n📊 Generating statistics report...")
        
        stats = {}
        
        for var in ['SSM', 'RZSM', 'freeze_thaw']:
            if self.data[var] is None:
                continue
            
            ds = self.data[var]
            
            # Get variable names
            if var == 'SSM':
                var_names = ['sm']
            elif var == 'RZSM':
                var_names = [v for v in ds.data_vars if v.startswith('rzsm') and v != 'rzsm']
                if not var_names:
                    var_names = ['rzsm'] if 'rzsm' in ds.data_vars else []
            else:
                var_names = ['flag'] if 'flag' in ds.data_vars else ['ft']
            
            stats[var] = {}
            
            for var_name in var_names:
                data = ds[var_name].values
                valid_data = data[np.isfinite(data)]
                
                stats[var][var_name] = {
                    'min': float(np.min(valid_data)),
                    'max': float(np.max(valid_data)),
                    'mean': float(np.mean(valid_data)),
                    'median': float(np.median(valid_data)),
                    'std': float(np.std(valid_data)),
                    'valid_pixels': int(len(valid_data)),
                    'total_pixels': int(data.size),
                    'coverage': float(len(valid_data) / data.size * 100)
                }
        
        # Print report
        print("\n" + "="*70)
        print("STATISTICAL SUMMARY")
        print("="*70)
        
        for var, var_stats in stats.items():
            print(f"\n{var}:")
            for var_name, metrics in var_stats.items():
                print(f"  {var_name}:")
                print(f"    Range: {metrics['min']:.4f} to {metrics['max']:.4f}")
                print(f"    Mean ± Std: {metrics['mean']:.4f} ± {metrics['std']:.4f}")
                print(f"    Median: {metrics['median']:.4f}")
                print(f"    Coverage: {metrics['coverage']:.1f}%")
        
        # Save to file
        stats_file = self.output_dir / "statistics_summary.txt"
        with open(stats_file, 'w') as f:
            f.write("="*70 + "\n")
            f.write("SOIL MOISTURE STATISTICAL SUMMARY\n")
            f.write("="*70 + "\n\n")
            
            for var, var_stats in stats.items():
                f.write(f"\n{var}:\n")
                for var_name, metrics in var_stats.items():
                    f.write(f"  {var_name}:\n")
                    f.write(f"    Range: {metrics['min']:.4f} to {metrics['max']:.4f}\n")
                    f.write(f"    Mean ± Std: {metrics['mean']:.4f} ± {metrics['std']:.4f}\n")
                    f.write(f"    Median: {metrics['median']:.4f}\n")
                    f.write(f"    Coverage: {metrics['coverage']:.1f}%\n")
        
        print(f"\n✓ Statistics saved to: {stats_file}")
        
        return stats


def main():
    """Command-line interface"""
    
    parser = argparse.ArgumentParser(
        description='Visualize Soil Moisture Data - Production Ready',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate all visualizations
  python %(prog)s
  
  # Specific year
  python %(prog)s --year 2020
  
  # Specific variable
  python %(prog)s --variable SSM
  
  # Custom output directory
  python %(prog)s --output-dir ./my_plots
  
  # Generate only time series
  python %(prog)s --plots timeseries
        """
    )
    
    parser.add_argument('--base-dir', type=str, 
                       default='./data/soil_moisture',
                       help='Base directory (default: ./data/soil_moisture)')
    parser.add_argument('--output-dir', type=str,
                       default='./plots',
                       help='Output directory for plots (default: ./plots)')
    parser.add_argument('--year', type=int, default=None,
                       help='Analyze specific year')
    parser.add_argument('--variable', type=str, 
                       choices=['SSM', 'RZSM', 'freeze_thaw', 'all'],
                       default='all',
                       help='Specific variable to analyze')
    parser.add_argument('--plots', type=str,
                       choices=['timeseries', 'spatial', 'monthly', 'seasonal', 'all'],
                       default='all',
                       help='Types of plots to generate')
    
    args = parser.parse_args()
    
    print("\n" + "="*70)
    print("SOIL MOISTURE VISUALIZATION")
    print("="*70)
    print(f"Data directory: {args.base_dir}")
    print(f"Output directory: {args.output_dir}")
    print(f"Year filter: {args.year if args.year else 'all'}")
    print(f"Variables: {args.variable}")
    print(f"Plot types: {args.plots}")
    print("="*70)
    
    # Initialize visualizer
    viz = SoilMoistureVisualizer(
        base_dir=args.base_dir,
        output_dir=args.output_dir
    )
    
    # Load data
    if args.variable == 'all':
        viz.load_all_variables(year=args.year)
        variables = ['SSM', 'RZSM', 'freeze_thaw']
    else:
        viz.load_data(args.variable, year=args.year)
        variables = [args.variable]
    
    # Generate plots
    print("\n" + "="*70)
    print("GENERATING VISUALIZATIONS")
    print("="*70)
    
    plot_year = args.year if args.year else 2020
    
    for var in variables:
        if viz.data[var] is None:
            continue
        
        if args.plots in ['timeseries', 'all']:
            viz.plot_time_series(variable=var, save=True)
        
        if args.plots in ['spatial', 'all']:
            viz.plot_spatial_average(variable=var, save=True)
        
        if args.plots in ['monthly', 'all']:
            viz.plot_monthly_comparison(variable=var, year=plot_year, save=True)
    
    if args.plots in ['seasonal', 'all'] and args.variable == 'all':
        viz.plot_seasonal_trends(save=True)
    
    # Generate statistics
    viz.generate_statistics_report()
    
    print("\n" + "="*70)
    print("✅ VISUALIZATION COMPLETE")
    print("="*70)
    print(f"Output directory: {args.output_dir}")
    print(f"Check the plots folder for all visualizations!")
    print("="*70 + "\n")


if __name__ == "__main__":
    main()