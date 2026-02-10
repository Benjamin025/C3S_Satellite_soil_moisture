# Quick Reference Guide - ESA Soil Moisture Workflow

## 🚀 Quick Commands

### Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Create directories
mkdir -p data/{raw,processed,output} figures
```

### Configure CDS API
```bash
# Create config file
echo "url: https://cds.climate.copernicus.eu/api" > ~/.cdsapirc
echo "key: YOUR_UID:YOUR_API_KEY" >> ~/.cdsapirc
```

## 📥 Download Data

### Single Month
```python
from esa_soil_moisture_workflow import SoilMoistureDownloader

downloader = SoilMoistureDownloader(output_dir="./data/raw")

file = downloader.download_soil_moisture(
    year=2023,
    month=6,
    product_type='combined',
    variable='volumetric_surface_soil_moisture',
    temporal_resolution='monthly'
)
```

### Multiple Months
```python
files = downloader.download_multiple_months(
    start_date="2023-01",
    end_date="2023-12",
    product_type='combined',
    variable='volumetric_surface_soil_moisture'
)
```

### Custom Region
```python
custom_bbox = {
    'north': 15,    # East Africa
    'south': -12,
    'east': 52,
    'west': 28
}

file = downloader.download_soil_moisture(
    year=2023, month=6,
    bbox=custom_bbox
)
```

## 🔧 Process Data

### Basic Processing
```python
from esa_soil_moisture_workflow import SoilMoistureProcessor
from pathlib import Path

processor = SoilMoistureProcessor()

nc_file = Path("./data/raw/your_file.nc")
ds = processor.process_workflow(
    nc_file=nc_file,
    variables=['sm'],
    calculate_stats=True,
    export_csv=True
)
```

### Load NetCDF
```python
import xarray as xr

ds = xr.open_dataset("./data/raw/your_file.nc")
print(ds)  # View structure
print(list(ds.data_vars))  # View variables
```

### Extract Variables
```python
sm_data = ds['sm']  # Surface soil moisture
uncertainty = ds['sm_uncertainty']  # Uncertainty
```

### Subset Region
```python
# Africa subset
africa = ds.sel(
    lat=slice(40, -35),
    lon=slice(-20, 55)
)

# East Africa
east_africa = ds.sel(
    lat=slice(15, -12),
    lon=slice(28, 52)
)
```

## 📊 Analysis

### Basic Statistics
```python
# Mean
mean_sm = ds['sm'].mean()

# Spatial mean (over time)
temporal_mean = ds['sm'].mean(dim='time')

# Regional mean
regional_mean = ds['sm'].sel(
    lat=slice(15, -12),
    lon=slice(28, 52)
).mean()
```

### Time Series
```python
# Extract point
point_ts = ds['sm'].sel(lat=0, lon=35, method='nearest')

# Plot
point_ts.plot()
```

### Drought Detection
```python
# Define threshold
threshold = 0.15  # m³/m³

# Create drought mask
drought = ds['sm'] < threshold

# Calculate percentage
drought_pct = (drought.sum() / drought.size) * 100
print(f"Drought area: {drought_pct:.2f}%")
```

### Anomaly
```python
# Calculate climatology
clim = ds['sm'].sel(time=slice('2000', '2020')).mean(dim='time')

# Calculate anomaly
anom = ds['sm'] - clim
```

## 🗺️ Visualization

### Spatial Map
```python
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature

fig = plt.figure(figsize=(12, 8))
ax = plt.axes(projection=ccrs.PlateCarree())

ds['sm'].isel(time=0).plot(
    ax=ax,
    transform=ccrs.PlateCarree(),
    cmap='YlGnBu',
    cbar_kwargs={'label': 'Soil Moisture (m³/m³)'}
)

ax.add_feature(cfeature.BORDERS)
ax.add_feature(cfeature.COASTLINE)
ax.set_extent([-20, 55, -35, 40])
plt.title('Surface Soil Moisture - Africa')
plt.show()
```

### Time Series Plot
```python
# Extract location
ts = ds['sm'].sel(lat=0, lon=35, method='nearest')

# Plot
fig, ax = plt.subplots(figsize=(12, 6))
ts.plot(ax=ax, marker='o')
ax.set_ylabel('Soil Moisture (m³/m³)')
ax.grid(True)
plt.show()
```

## 💾 Export Data

### To CSV
```python
# Convert to DataFrame
df = ds['sm'].to_dataframe().reset_index()
df.to_csv('./data/output/soil_moisture.csv', index=False)
```

### To GeoTIFF
```python
import rioxarray

# Add CRS
ds['sm'].rio.write_crs("EPSG:4326", inplace=True)

# Export
ds['sm'].isel(time=0).rio.to_raster('./data/output/sm_map.tif')
```

### To NetCDF
```python
# Save subset
ds.to_netcdf('./data/processed/sm_processed.nc')
```

## 🔍 Common Patterns

### Monthly Climatology
```python
# Group by month
monthly_clim = ds['sm'].groupby('time.month').mean(dim='time')

# Plot
monthly_clim.plot(col='month', col_wrap=4)
```

### Regional Comparison
```python
regions = {
    'East': {'lat': slice(15, -12), 'lon': slice(28, 52)},
    'West': {'lat': slice(25, 0), 'lon': slice(-20, 20)},
    'South': {'lat': slice(-5, -35), 'lon': slice(10, 55)}
}

for name, bbox in regions.items():
    mean = ds['sm'].sel(**bbox).mean()
    print(f"{name} Africa: {mean.values:.3f} m³/m³")
```

### Masking by Quality
```python
# Apply quality mask
good_quality = ds['sm'].where(ds['flag'] == 0)

# Count valid pixels
valid_pct = (good_quality.notnull().sum() / good_quality.size) * 100
print(f"Valid data: {valid_pct:.2f}%")
```

## 📋 Variable Reference

### Common Variable Names in NetCDF

| Variable | Description | Unit |
|----------|-------------|------|
| `sm` | Surface soil moisture | m³/m³ |
| `sm_uncertainty` | SM uncertainty | m³/m³ |
| `flag` | Quality flag | - |
| `sensor` | Sensor identifier | - |
| `rzsm` | Root-zone SM | m³/m³ |
| `rzsm_uncertainty` | RZSM uncertainty | m³/m³ |
| `freeze_thaw` | Freeze/thaw state | 0/1 |

### Coordinate Names
- `lat` or `latitude`
- `lon` or `longitude`
- `time`

## ⚙️ Product Options

### Product Types
```python
# Active (scatterometer)
product_type = 'active'

# Passive (radiometer)
product_type = 'passive'

# Combined (recommended)
product_type = 'combined'
```

### Variables
```python
# Surface soil moisture (volumetric)
variable = 'volumetric_surface_soil_moisture'

# Surface soil moisture (saturation %)
variable = 'saturation_surface_soil_moisture'

# Root-zone soil moisture
variable = 'root_zone_soil_moisture'

# Freeze/thaw
variable = 'freeze_thaw_classification'
```

### Temporal Resolution
```python
temporal_resolution = 'daily'     # Daily
temporal_resolution = '10_daily'  # 10-day
temporal_resolution = 'monthly'   # Monthly
```

## 🎯 Use Cases

### 1. Drought Monitoring
```python
# Load data
ds = xr.open_dataset('file.nc')

# Define drought threshold
threshold = 0.15

# Identify drought
drought = ds['sm'] < threshold
drought_area = (drought.sum() / drought.size) * 100

print(f"Drought extent: {drought_area:.1f}%")
```

### 2. Agricultural Analysis
```python
# Growing season (e.g., March-May)
growing_season = ds['sm'].sel(time=slice('2023-03', '2023-05'))

# Average moisture
avg_moisture = growing_season.mean()

# Check if adequate (>0.2 m³/m³)
adequate = avg_moisture > 0.2
```

### 3. Climate Trend Analysis
```python
# Annual mean
annual_mean = ds['sm'].resample(time='1Y').mean()

# Linear trend
from scipy.stats import linregress

x = range(len(annual_mean.time))
y = annual_mean.values

slope, intercept, r, p, se = linregress(x, y)
print(f"Trend: {slope:.6f} m³/m³/year (p={p:.3f})")
```

## 🔗 Useful Links

- **CDS Dataset**: https://cds.climate.copernicus.eu/datasets/satellite-soil-moisture
- **API Docs**: https://cds.climate.copernicus.eu/api-how-to
- **ESA CCI**: https://www.esa-soilmoisture-cci.org/
- **Forum**: https://forum.ecmwf.int

## 💡 Tips

1. **Start small**: Download 1 month first to test
2. **Check queue**: CDS can be busy, downloads take time
3. **Use 'combined'**: Best quality data
4. **Monthly resolution**: Easier to work with than daily
5. **Quality masking**: Always check quality flags
6. **Memory**: Use Dask for large datasets
7. **Backup**: Save processed data to avoid re-downloading

## ⚠️ Common Gotchas

1. Variable names may differ from documentation
2. CDS downloads include metadata files (unzip to get .nc)
3. Coordinates may be named `lat/lon` or `latitude/longitude`
4. Time dimension format varies
5. Quality flags are essential for reliable analysis
6. Large downloads can fail - use retry logic

## 🆘 Quick Fixes

### Check what's in your NetCDF
```python
ds = xr.open_dataset('file.nc')
print(ds.data_vars)
print(ds.coords)
print(ds.dims)
```

### Find actual variable names
```python
print(list(ds.data_vars))
# Use these names instead of assumed ones
```

### Handle missing time dimension
```python
if 'time' in ds.dims:
    data = ds['sm'].isel(time=0)
else:
    data = ds['sm']
```

### Deal with large files
```python
# Lazy load with Dask
ds = xr.open_dataset('file.nc', chunks={'time': 10})
```
