# ESA Soil Moisture Data Processing Workflow for Africa

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![CDS](https://img.shields.io/badge/Data-Copernicus%20CDS-orange)](https://cds.climate.copernicus.eu/)

Complete workflow for downloading, processing, and analyzing ESA soil moisture satellite data over Africa using the Copernicus Climate Data Store (CDS) API.

## 📋 Overview

This project provides a Python-based workflow similar to GPM precipitation data processing, specifically designed for ESA Climate Change Initiative (CCI) soil moisture data. The workflow includes:

- **Data Download**: Automated retrieval from CDS API
- **Data Processing**: NetCDF handling, quality control, and filtering
- **Spatial Analysis**: Regional statistics and mapping
- **Temporal Analysis**: Time series extraction and anomaly detection
- **Visualization**: Maps, plots, and drought monitoring
- **Export Options**: CSV, GeoTIFF, and NetCDF formats

## 🌍 Dataset Information

**Source**: [Copernicus Climate Data Store - Satellite Soil Moisture](https://cds.climate.copernicus.eu/datasets/satellite-soil-moisture)

### Key Features:
- **Temporal Coverage**: 1978 - present (varies by product)
- **Spatial Resolution**: 0.25° × 0.25°
- **Spatial Coverage**: Global (this workflow focuses on Africa)
- **Update Frequency**: Daily to monthly
- **Format**: NetCDF-4

### Available Products:

| Product | Description | Temporal Range |
|---------|-------------|----------------|
| **ACTIVE** | Multi-scatterometer | Aug 1991 - present |
| **PASSIVE** | Multi-radiometer | Nov 1978 - present |
| **COMBINED** | Merged all sources (recommended) | Nov 1978 - present |

### Available Variables:

| Variable | Description | Unit | Depth |
|----------|-------------|------|-------|
| **SSM (Volumetric)** | Surface soil moisture | m³/m³ | 2-5 cm |
| **SSM (Saturation)** | Surface soil moisture | % | 2-5 cm |
| **RZSM** | Root-zone soil moisture | m³/m³ | 0-10, 10-40, 40-100, 0-100 cm |
| **F/T** | Freeze/Thaw classification | binary | Surface |

## 📁 Project Structure

```
.
├── esa_soil_moisture_workflow.py   # Main workflow script
├── requirements.txt                 # Python dependencies
├── config.ini                       # Configuration file
├── SETUP_GUIDE.md                   # Detailed setup instructions
├── soil_moisture_analysis.ipynb     # Jupyter notebook for analysis
├── README.md                        # This file
│
├── data/
│   ├── raw/                        # Downloaded NetCDF files
│   ├── processed/                  # Processed data
│   └── output/                     # Analysis outputs
│
└── figures/                        # Generated plots and maps
```

## 🚀 Quick Start

### 1. Prerequisites

- Python 3.8 or higher
- CDS API account (free registration)
- VS Code or Jupyter Notebook

### 2. Installation

```bash
# Clone or download this repository
git clone <repository-url>
cd esa-soil-moisture-workflow

# Install dependencies
pip install -r requirements.txt

# Or use conda
conda create -n soil_moisture python=3.10
conda activate soil_moisture
pip install -r requirements.txt
```

### 3. CDS API Setup

1. Register at [CDS](https://cds.climate.copernicus.eu/)
2. Accept the dataset terms of use
3. Get your API key from your [profile](https://cds.climate.copernicus.eu/profile)
4. Create `~/.cdsapirc` file:

```ini
url: https://cds.climate.copernicus.eu/api
key: YOUR_UID:YOUR_API_KEY
```

**See [SETUP_GUIDE.md](SETUP_GUIDE.md) for detailed instructions.**

### 4. Run Your First Download

```python
from esa_soil_moisture_workflow import SoilMoistureDownloader

# Initialize
downloader = SoilMoistureDownloader(output_dir="./data/raw")

# Download data for Africa
file = downloader.download_soil_moisture(
    year=2023,
    month=6,
    product_type='combined',
    variable='volumetric_surface_soil_moisture',
    temporal_resolution='monthly',
    bbox={'north': 40, 'south': -35, 'east': 55, 'west': -20}
)
```

## 💻 Usage Examples

### Example 1: Download Multiple Months

```python
from esa_soil_moisture_workflow import SoilMoistureDownloader

downloader = SoilMoistureDownloader()

files = downloader.download_multiple_months(
    start_date="2023-01",
    end_date="2023-06",
    product_type='combined',
    variable='volumetric_surface_soil_moisture',
    temporal_resolution='monthly'
)
```

### Example 2: Process Downloaded Data

```python
from esa_soil_moisture_workflow import SoilMoistureProcessor
from pathlib import Path

processor = SoilMoistureProcessor(
    input_dir="./data/raw",
    output_dir="./data/processed"
)

# Process a file
nc_file = Path("./data/raw/your_file.nc")
processed_ds = processor.process_workflow(
    nc_file=nc_file,
    variables=['sm'],
    calculate_stats=True,
    export_csv=True
)
```

### Example 3: Regional Analysis

```python
# Load data
import xarray as xr
ds = xr.open_dataset("./data/processed/your_file.nc")

# Extract East Africa
east_africa = ds.sel(
    lat=slice(15, -12),
    lon=slice(28, 52)
)

# Calculate statistics
mean_sm = east_africa['sm'].mean()
print(f"Mean soil moisture: {mean_sm.values:.3f} m³/m³")
```

## 📊 Analysis Capabilities

### Spatial Analysis
- Regional averages and statistics
- Drought identification and monitoring
- Spatial patterns and anomalies
- Sub-regional comparisons

### Temporal Analysis
- Time series extraction
- Trend analysis
- Seasonal cycles
- Anomaly calculations

### Visualization
- Spatial maps with Cartopy
- Time series plots
- Statistical summaries
- Drought extent maps

## 🗺️ Regional Coverage

Pre-defined regions for Africa:

| Region | Bounding Box |
|--------|--------------|
| **Full Africa** | N: 40°, S: -35°, E: 55°, W: -20° |
| **East Africa** | N: 15°, S: -12°, E: 52°, W: 28° |
| **West Africa** | N: 25°, S: 0°, E: 20°, W: -20° |
| **Southern Africa** | N: -5°, S: -35°, E: 55°, W: 10° |
| **North Africa** | N: 40°, S: 15°, E: 55°, W: -20° |

Customize bounding boxes in `config.ini` or directly in code.

## 📖 Documentation

### Main Classes

#### `SoilMoistureDownloader`
Handles data download from CDS API.

**Methods**:
- `download_soil_moisture()`: Download single month
- `download_multiple_months()`: Download date range

#### `SoilMoistureProcessor`
Processes NetCDF files and extracts information.

**Methods**:
- `load_netcdf()`: Load NetCDF file
- `extract_variables()`: Select specific variables
- `subset_region()`: Spatial subsetting
- `calculate_statistics()`: Temporal statistics
- `export_to_csv()`: Export to CSV
- `export_to_geotiff()`: Export to GeoTIFF
- `process_workflow()`: Complete processing pipeline

### Configuration Options

Edit `config.ini` to customize:
- Data paths
- Download parameters
- Regional boundaries
- Processing options
- Visualization settings

## 🔧 Advanced Features

### Quality Control
```python
# Mask low-quality data
ds_masked = processor.mask_low_quality(
    ds,
    variable='sm',
    quality_var='flag',
    valid_flags=[0]
)
```

### Drought Analysis
```python
# Identify drought areas
drought_threshold = 0.15  # m³/m³
drought_mask = ds['sm'] < drought_threshold
drought_area_pct = (drought_mask.sum() / drought_mask.size) * 100
```

### Anomaly Calculation
```python
# Calculate anomaly from climatology
climatology = ds['sm'].sel(time=slice('2000', '2020')).mean(dim='time')
anomaly = ds['sm'] - climatology
```

## 🔄 Integration with Other Datasets

This workflow can be integrated with:
- **GPM Precipitation**: For soil moisture-precipitation relationships
- **NDVI/EVI**: For vegetation-soil moisture coupling
- **ERA5**: For meteorological context
- **CHIRPS**: For additional precipitation analysis

## 📝 Interactive Notebook

Use the included Jupyter notebook (`soil_moisture_analysis.ipynb`) for:
- Interactive data exploration
- Custom visualizations
- Regional analysis
- Drought monitoring

```bash
jupyter notebook soil_moisture_analysis.ipynb
```

## 🐛 Troubleshooting

### Common Issues

**1. CDS API Authentication Error**
```
Solution: Check ~/.cdsapirc file format and credentials
```

**2. Download Takes Too Long**
```
Solution: CDS queue can be busy. Downloads may take 10-30 minutes.
Check status: https://cds.climate.copernicus.eu/live
```

**3. NetCDF Variable Not Found**
```
Solution: Check actual variable names in downloaded file:
ds = xr.open_dataset('file.nc')
print(list(ds.data_vars))
```

**4. Memory Issues with Large Files**
```
Solution: Use Dask for lazy loading:
ds = xr.open_dataset('file.nc', chunks={'time': 1})
```

See [SETUP_GUIDE.md](SETUP_GUIDE.md) for more troubleshooting tips.

## 📚 Resources

### Documentation
- [CDS API Documentation](https://cds.climate.copernicus.eu/api-how-to)
- [Dataset Documentation](https://cds.climate.copernicus.eu/datasets/satellite-soil-moisture?tab=documentation)
- [ESA CCI Soil Moisture](https://www.esa-soilmoisture-cci.org/)

### Python Libraries
- [xarray](http://xarray.pydata.org/): NetCDF handling
- [cdsapi](https://github.com/ecmwf/cdsapi): CDS API client
- [cartopy](https://scitools.org.uk/cartopy/): Mapping
- [rioxarray](https://corteva.github.io/rioxarray/): Raster I/O

## 🤝 Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Submit a pull request

## 📄 License

This project is licensed under the MIT License.

## 🙏 Acknowledgments

- **ESA Climate Change Initiative** for soil moisture algorithms
- **Copernicus Climate Change Service (C3S)** for data provision
- **TU Wien, Planet, and EODC GmbH** for dataset production

## 📧 Contact

For questions or issues:
- Open an issue on GitHub
- Check the [CDS Forum](https://forum.ecmwf.int)

## 🔖 Citation

If you use this workflow in your research, please cite:

```
ESA CCI Soil Moisture Dataset:
DOI: 10.24381/cds.d7782f18
```

---

**Last Updated**: February 2026  
**Version**: 1.0.0
