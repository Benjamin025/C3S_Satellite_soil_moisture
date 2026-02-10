# ESA Soil Moisture Data Workflow - Setup Guide

This guide will help you set up the environment and configure the CDS API for downloading ESA soil moisture data.

## Prerequisites

- Python 3.8 or higher
- VS Code (recommended)
- CDS API account

## Step 1: CDS API Registration

1. Go to https://cds.climate.copernicus.eu/
2. Click "Register" (top right)
3. Fill in the registration form
4. Verify your email address
5. Accept the license terms:
   - Go to https://cds.climate.copernicus.eu/datasets/satellite-soil-moisture?tab=download
   - Accept the "Terms of use" and "Licence to use Copernicus Products"

## Step 2: Get Your API Key

1. Log in to CDS
2. Go to your user profile: https://cds.climate.copernicus.eu/profile
3. Copy your UID and API key from the "API key" section

## Step 3: Configure CDS API

### Option A: Manual Configuration (Recommended)

Create a file named `.cdsapirc` in your home directory:

**Linux/Mac:**
```bash
touch ~/.cdsapirc
nano ~/.cdsapirc
```

**Windows:**
```powershell
New-Item -Path "$env:USERPROFILE\.cdsapirc" -ItemType File
notepad "$env:USERPROFILE\.cdsapirc"
```

Add the following content (replace with your credentials):
```
url: https://cds.climate.copernicus.eu/api
key: YOUR_UID:YOUR_API_KEY
```

Example:
```
url: https://cds.climate.copernicus.eu/api
key: 12345:abcdef12-3456-7890-abcd-ef1234567890
```

### Option B: Python Configuration

You can also configure programmatically:

```python
import os
from pathlib import Path

# Your CDS credentials
uid = "12345"
api_key = "abcdef12-3456-7890-abcd-ef1234567890"

# Create .cdsapirc file
cdsapirc_path = Path.home() / ".cdsapirc"
with open(cdsapirc_path, 'w') as f:
    f.write(f"url: https://cds.climate.copernicus.eu/api\n")
    f.write(f"key: {uid}:{api_key}\n")

print(f"CDS API configured at: {cdsapirc_path}")
```

## Step 4: Install Python Dependencies

### Using pip:
```bash
pip install -r requirements.txt
```

### Using conda:
```bash
conda create -n soil_moisture python=3.10
conda activate soil_moisture
pip install -r requirements.txt
```

### Core packages:
```bash
pip install cdsapi xarray netCDF4 numpy pandas matplotlib cartopy rioxarray
```

## Step 5: Test Your Setup

Create a test script `test_setup.py`:

```python
import cdsapi

# Test CDS API connection
try:
    client = cdsapi.Client()
    print("✓ CDS API configured successfully!")
    print(f"  URL: {client.url}")
except Exception as e:
    print(f"✗ CDS API configuration failed: {e}")

# Test required packages
packages = ['xarray', 'numpy', 'pandas', 'netCDF4', 'matplotlib']
for package in packages:
    try:
        __import__(package)
        print(f"✓ {package} installed")
    except ImportError:
        print(f"✗ {package} not installed")
```

Run the test:
```bash
python test_setup.py
```

## Step 6: Directory Structure

Create the necessary directories:

```bash
mkdir -p data/raw data/processed data/output figures
```

Or in Python:
```python
from pathlib import Path

directories = ['data/raw', 'data/processed', 'data/output', 'figures']
for directory in directories:
    Path(directory).mkdir(parents=True, exist_ok=True)
```

## Step 7: Run Your First Download

Try downloading a single month of data:

```python
from esa_soil_moisture_workflow import SoilMoistureDownloader

# Initialize downloader
downloader = SoilMoistureDownloader(output_dir="./data/raw")

# Download January 2023 data for Africa
downloaded_file = downloader.download_soil_moisture(
    year=2023,
    month=1,
    product_type='combined',
    variable='volumetric_surface_soil_moisture',
    temporal_resolution='monthly',
    bbox={'north': 40, 'south': -35, 'east': 55, 'west': -20}
)

print(f"Downloaded: {downloaded_file}")
```

## Troubleshooting

### Error: "Missing/incomplete configuration file"
- Ensure `.cdsapirc` is in your home directory
- Check file permissions (should be readable)
- Verify the format (url and key on separate lines)

### Error: "Invalid API key"
- Double-check your UID and API key
- Ensure there are no extra spaces
- The format should be: `key: UID:API_KEY`

### Error: "Terms not accepted"
- Log in to CDS
- Go to the dataset page
- Accept the terms of use

### Download is very slow
- This is normal for CDS downloads
- Downloads can take several minutes to hours
- The CDS queue may be busy
- Check the CDS status: https://cds.climate.copernicus.eu/live

### Error: "Request too large"
- Reduce the temporal range
- Download one month at a time
- Reduce the spatial extent

## Dataset Information

### Available Variables:

**Surface Soil Moisture (SSM):**
- `volumetric_surface_soil_moisture`: m³/m³ (0-0.6)
- `saturation_surface_soil_moisture`: percentage (0-100%)

**Root-Zone Soil Moisture (RZSM):**
- Available depths: 0-10cm, 10-40cm, 40-100cm, 0-1m
- Only available with 'combined' product type

**Freeze/Thaw:**
- `freeze_thaw_classification`: binary (0=unfrozen, 1=frozen)

### Product Types:
- **active**: Multi-scatterometer data
- **passive**: Multi-radiometer data
- **combined**: Merges all sources (recommended)

### Temporal Resolution:
- **daily**: Daily observations
- **10_daily**: 10-day aggregations
- **monthly**: Monthly aggregations

### Spatial Resolution:
- 0.25° × 0.25° global grid

### Temporal Coverage:
- SSM (COMBINED/PASSIVE): November 1978 - present
- RZSM: January 1980 - present
- SSM (ACTIVE): August 1991 - present

## Additional Resources

- CDS API Documentation: https://cds.climate.copernicus.eu/api-how-to
- Dataset Documentation: https://cds.climate.copernicus.eu/datasets/satellite-soil-moisture?tab=documentation
- Python API Reference: https://github.com/ecmwf/cdsapi
- ESA CCI Soil Moisture: https://www.esa-soilmoisture-cci.org/

## Next Steps

1. Review the `esa_soil_moisture_workflow.py` script
2. Check the `config.ini` for configuration options
3. Explore the example notebook for data analysis
4. Customize the bounding box for your region of interest
5. Set up your download schedule

## Support

If you encounter issues:
1. Check the CDS Forum: https://forum.ecmwf.int
2. Review the dataset documentation
3. Check GitHub issues for cdsapi package
