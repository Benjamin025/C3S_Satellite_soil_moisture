"""
ESA Soil Moisture Data Download and Processing Workflow
========================================================
Downloads soil moisture data from Copernicus Climate Data Store (CDS)
and processes it for African region analysis.

Dataset: Satellite Soil Moisture from ESA CCI
Source: https://cds.climate.copernicus.eu/datasets/satellite-soil-moisture
Variables: SSM (Surface Soil Moisture), RZSM (Root-Zone Soil Moisture), F/T (Freeze/Thaw)
"""

import cdsapi
import xarray as xr
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Optional, Tuple
import warnings
warnings.filterwarnings('ignore')


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SoilMoistureDownloader:
    """
    Class to handle downloading soil moisture data from CDS API
    """
    
    def __init__(self, output_dir: str = "./data/raw"):
        """
        Initialize the downloader
        
        Parameters:
        -----------
        output_dir : str
            Directory to save downloaded files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize CDS API client
        try:
            self.client = cdsapi.Client()
            logger.info("CDS API client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize CDS API client: {e}")
            logger.info("Please ensure you have ~/.cdsapirc file with your credentials")
            raise
    
    def download_soil_moisture(
        self,
        year: int,
        month: int,
        product_type: str = 'combined',
        variable: str = 'volumetric_surface_soil_moisture',
        temporal_resolution: str = 'monthly',
        bbox: Optional[Dict[str, float]] = None,
        output_filename: Optional[str] = None
    ) -> Path:
        """
        Download soil moisture data from CDS
        
        Parameters:
        -----------
        year : int
            Year to download
        month : int
            Month to download (1-12)
        product_type : str
            Type of product: 'active', 'passive', or 'combined' (default: 'combined')
        variable : str
            Variable to download. Options:
            - 'volumetric_surface_soil_moisture' (SSM in m3/m3)
            - 'saturation_surface_soil_moisture' (SSM in %)
            - 'root_zone_soil_moisture' (RZSM - requires combined product)
            - 'freeze_thaw_classification'
        temporal_resolution : str
            'daily', '10_daily', or 'monthly'
        bbox : dict
            Bounding box with keys: 'north', 'south', 'east', 'west'
            Default is Africa: {'north': 40, 'south': -35, 'east': 55, 'west': -20}
        output_filename : str
            Custom output filename (optional)
            
        Returns:
        --------
        Path : Path to downloaded file
        """
        
        # Default to Africa region
        if bbox is None:
            bbox = {
                'north': 40,
                'south': -35,
                'east': 55,
                'west': -20
            }
        
        # Construct request parameters
        request_params = {
            'version': 'v202505',
            'variable': variable,
            'type_of_sensor': product_type,
            'time_aggregation': temporal_resolution,
            'year': str(year),
            'month': f'{month:02d}',
            'area': [bbox['north'], bbox['west'], bbox['south'], bbox['east']],
            'data_format': 'zip'
        }
        
        # Generate output filename
        if output_filename is None:
            output_filename = (
                f"soil_moisture_{product_type}_{variable.replace('_', '')}_{year}_{month:02d}_"
                f"{temporal_resolution}_africa.zip"
            )
        
        output_path = self.output_dir / output_filename
        
        logger.info(f"Downloading soil moisture data for {year}-{month:02d}")
        logger.info(f"Product: {product_type}, Variable: {variable}")
        logger.info(f"Output: {output_path}")
        
        try:
            self.client.retrieve(
                'satellite-soil-moisture',
                request_params,
                str(output_path)
            )
            logger.info(f"Download completed: {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Download failed: {e}")
            raise
    
    def download_multiple_months(
        self,
        start_date: str,
        end_date: str,
        product_type: str = 'combined',
        variable: str = 'volumetric_surface_soil_moisture',
        temporal_resolution: str = 'monthly',
        bbox: Optional[Dict[str, float]] = None
    ) -> List[Path]:
        """
        Download data for multiple months
        
        Parameters:
        -----------
        start_date : str
            Start date in format 'YYYY-MM'
        end_date : str
            End date in format 'YYYY-MM'
        product_type : str
            Type of product
        variable : str
            Variable to download
        temporal_resolution : str
            Temporal resolution
        bbox : dict
            Bounding box
            
        Returns:
        --------
        List[Path] : List of downloaded file paths
        """
        
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        
        # Generate list of months
        date_range = pd.date_range(start=start, end=end, freq='MS')
        
        downloaded_files = []
        
        for date in date_range:
            try:
                file_path = self.download_soil_moisture(
                    year=date.year,
                    month=date.month,
                    product_type=product_type,
                    variable=variable,
                    temporal_resolution=temporal_resolution,
                    bbox=bbox
                )
                downloaded_files.append(file_path)
                
            except Exception as e:
                logger.error(f"Failed to download {date.year}-{date.month:02d}: {e}")
                continue
        
        logger.info(f"Downloaded {len(downloaded_files)} files successfully")
        return downloaded_files


class SoilMoistureProcessor:
    """
    Class to process soil moisture NetCDF files
    """
    
    def __init__(self, input_dir: str = "./data/raw", output_dir: str = "./data/processed"):
        """
        Initialize the processor
        
        Parameters:
        -----------
        input_dir : str
            Directory with raw NetCDF files
        output_dir : str
            Directory to save processed files
        """
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def load_netcdf(self, file_path: Path) -> xr.Dataset:
        """
        Load NetCDF file
        
        Parameters:
        -----------
        file_path : Path
            Path to NetCDF file
            
        Returns:
        --------
        xr.Dataset : Loaded dataset
        """
        logger.info(f"Loading NetCDF file: {file_path}")
        ds = xr.open_dataset(file_path)
        logger.info(f"Dataset loaded. Variables: {list(ds.data_vars)}")
        logger.info(f"Dimensions: {dict(ds.dims)}")
        return ds
    
    def extract_variables(
        self,
        ds: xr.Dataset,
        variables: List[str] = None
    ) -> xr.Dataset:
        """
        Extract specific variables from dataset
        
        Parameters:
        -----------
        ds : xr.Dataset
            Input dataset
        variables : List[str]
            List of variables to extract. If None, keeps all variables
            Common variables:
            - 'sm' : Surface soil moisture (volumetric)
            - 'sm_uncertainty' : Uncertainty estimate
            - 'rzsm' : Root-zone soil moisture (different depths)
            - 'rzsm_uncertainty' : RZSM uncertainty
            - 'flag' : Quality flags
            
        Returns:
        --------
        xr.Dataset : Dataset with selected variables
        """
        if variables is None:
            logger.info("Keeping all variables")
            return ds
        
        # Filter variables that exist in dataset
        available_vars = [v for v in variables if v in ds.data_vars]
        
        if not available_vars:
            logger.warning(f"None of the requested variables found. Available: {list(ds.data_vars)}")
            return ds
        
        logger.info(f"Extracting variables: {available_vars}")
        return ds[available_vars]
    
    def subset_region(
        self,
        ds: xr.Dataset,
        bbox: Dict[str, float]
    ) -> xr.Dataset:
        """
        Subset dataset to specific region
        
        Parameters:
        -----------
        ds : xr.Dataset
            Input dataset
        bbox : dict
            Bounding box with keys: 'north', 'south', 'east', 'west'
            
        Returns:
        --------
        xr.Dataset : Subsetted dataset
        """
        logger.info(f"Subsetting to region: {bbox}")
        
        # Handle different possible coordinate names
        lat_name = 'lat' if 'lat' in ds.dims else 'latitude'
        lon_name = 'lon' if 'lon' in ds.dims else 'longitude'
        
        ds_subset = ds.sel(
            {lat_name: slice(bbox['north'], bbox['south']),
             lon_name: slice(bbox['west'], bbox['east'])}
        )
        
        logger.info(f"Subset dimensions: {dict(ds_subset.dims)}")
        return ds_subset
    
    def calculate_statistics(
        self,
        ds: xr.Dataset,
        variable: str,
        time_dim: str = 'time'
    ) -> Dict[str, xr.DataArray]:
        """
        Calculate basic statistics over time
        
        Parameters:
        -----------
        ds : xr.Dataset
            Input dataset
        variable : str
            Variable to calculate statistics for
        time_dim : str
            Name of time dimension
            
        Returns:
        --------
        dict : Dictionary with mean, std, min, max
        """
        logger.info(f"Calculating statistics for {variable}")
        
        stats = {
            'mean': ds[variable].mean(dim=time_dim),
            'std': ds[variable].std(dim=time_dim),
            'min': ds[variable].min(dim=time_dim),
            'max': ds[variable].max(dim=time_dim)
        }
        
        return stats
    
    def mask_low_quality(
        self,
        ds: xr.Dataset,
        variable: str,
        quality_var: str = 'flag',
        valid_flags: List[int] = [0]
    ) -> xr.Dataset:
        """
        Mask data based on quality flags
        
        Parameters:
        -----------
        ds : xr.Dataset
            Input dataset
        variable : str
            Variable to mask
        quality_var : str
            Quality flag variable name
        valid_flags : List[int]
            List of valid flag values
            
        Returns:
        --------
        xr.Dataset : Dataset with masked variable
        """
        if quality_var not in ds.data_vars:
            logger.warning(f"Quality variable {quality_var} not found. Skipping masking.")
            return ds
        
        logger.info(f"Masking {variable} based on {quality_var}")
        
        # Create mask
        mask = ds[quality_var].isin(valid_flags)
        
        # Apply mask
        ds[variable] = ds[variable].where(mask)
        
        masked_percent = (1 - mask.sum() / mask.size) * 100
        logger.info(f"Masked {masked_percent:.2f}% of data")
        
        return ds
    
    def export_to_csv(
        self,
        ds: xr.Dataset,
        variable: str,
        output_path: Path,
        include_coords: bool = True
    ):
        """
        Export data to CSV format
        
        Parameters:
        -----------
        ds : xr.Dataset
            Input dataset
        variable : str
            Variable to export
        output_path : Path
            Output CSV file path
        include_coords : bool
            Whether to include coordinate columns
        """
        logger.info(f"Exporting {variable} to CSV: {output_path}")
        
        # Convert to DataFrame
        df = ds[variable].to_dataframe().reset_index()
        
        # Save to CSV
        df.to_csv(output_path, index=False)
        logger.info(f"CSV exported: {output_path}")
    
    def export_to_geotiff(
        self,
        ds: xr.Dataset,
        variable: str,
        output_path: Path,
        time_index: int = 0
    ):
        """
        Export single time slice to GeoTIFF
        
        Parameters:
        -----------
        ds : xr.Dataset
            Input dataset
        variable : str
            Variable to export
        output_path : Path
            Output GeoTIFF file path
        time_index : int
            Time slice index to export
        """
        try:
            import rioxarray
            
            logger.info(f"Exporting {variable} to GeoTIFF: {output_path}")
            
            # Select time slice
            if 'time' in ds[variable].dims:
                data = ds[variable].isel(time=time_index)
            else:
                data = ds[variable]
            
            # Add CRS information
            data.rio.write_crs("EPSG:4326", inplace=True)
            
            # Export to GeoTIFF
            data.rio.to_raster(output_path)
            logger.info(f"GeoTIFF exported: {output_path}")
            
        except ImportError:
            logger.error("rioxarray not installed. Install with: pip install rioxarray")
            raise
    
    def process_workflow(
        self,
        nc_file: Path,
        variables: List[str] = ['sm'],
        bbox: Optional[Dict[str, float]] = None,
        calculate_stats: bool = True,
        export_csv: bool = True,
        export_geotiff: bool = False
    ) -> xr.Dataset:
        """
        Complete processing workflow
        
        Parameters:
        -----------
        nc_file : Path
            Path to NetCDF file
        variables : List[str]
            Variables to process
        bbox : dict
            Bounding box for subsetting
        calculate_stats : bool
            Whether to calculate statistics
        export_csv : bool
            Whether to export to CSV
        export_geotiff : bool
            Whether to export to GeoTIFF
            
        Returns:
        --------
        xr.Dataset : Processed dataset
        """
        logger.info(f"Starting processing workflow for {nc_file.name}")
        
        # Load data
        ds = self.load_netcdf(nc_file)
        
        # Extract variables of interest
        ds = self.extract_variables(ds, variables)
        
        # Subset region if specified
        if bbox:
            ds = self.subset_region(ds, bbox)
        
        # Calculate statistics if requested
        if calculate_stats:
            for var in variables:
                if var in ds.data_vars:
                    stats = self.calculate_statistics(ds, var)
                    
                    # Add statistics to dataset
                    for stat_name, stat_data in stats.items():
                        ds[f'{var}_{stat_name}'] = stat_data
        
        # Export processed data
        base_name = nc_file.stem
        
        # Save processed NetCDF
        output_nc = self.output_dir / f"{base_name}_processed.nc"
        ds.to_netcdf(output_nc)
        logger.info(f"Processed NetCDF saved: {output_nc}")
        
        # Export to CSV if requested
        if export_csv:
            for var in variables:
                if var in ds.data_vars:
                    csv_path = self.output_dir / f"{base_name}_{var}.csv"
                    self.export_to_csv(ds, var, csv_path)
        
        # Export to GeoTIFF if requested
        if export_geotiff:
            for var in variables:
                if var in ds.data_vars:
                    tiff_path = self.output_dir / f"{base_name}_{var}.tif"
                    self.export_to_geotiff(ds, var, tiff_path)
        
        logger.info("Processing workflow completed")
        return ds


def main():
    """
    Main execution function demonstrating the workflow
    """
    
    # Configuration
    START_DATE = "2023-01"
    END_DATE = "2023-03"
    PRODUCT_TYPE = "combined"  # Options: 'active', 'passive', 'combined'
    VARIABLE = "volumetric_surface_soil_moisture"  # SSM in m3/m3
    TEMPORAL_RES = "monthly"
    
    # Africa bounding box
    AFRICA_BBOX = {
        'north': 40,
        'south': -35,
        'east': 55,
        'west': -20
    }
    
    # Variables to process
    VARIABLES_TO_PROCESS = ['sm']  # Will vary based on actual NetCDF structure
    
    # Step 1: Download data
    logger.info("=" * 60)
    logger.info("STEP 1: DOWNLOADING SOIL MOISTURE DATA")
    logger.info("=" * 60)
    
    downloader = SoilMoistureDownloader(output_dir="./data/raw")
    
    # Download single month (example)
    # downloaded_file = downloader.download_soil_moisture(
    #     year=2023,
    #     month=1,
    #     product_type=PRODUCT_TYPE,
    #     variable=VARIABLE,
    #     temporal_resolution=TEMPORAL_RES,
    #     bbox=AFRICA_BBOX
    # )
    
    # Or download multiple months
    downloaded_files = downloader.download_multiple_months(
         start_date=START_DATE,
         end_date=END_DATE,
         product_type=PRODUCT_TYPE,
         variable=VARIABLE,
         temporal_resolution=TEMPORAL_RES,
         bbox=AFRICA_BBOX
     )
    
    # Step 2: Process data
    logger.info("=" * 60)
    logger.info("STEP 2: PROCESSING DOWNLOADED DATA")
    logger.info("=" * 60)
    
    processor = SoilMoistureProcessor(
        input_dir="./data/raw",
        output_dir="./data/processed"
    )
    
    # Process each downloaded file
    # for nc_file in Path("./data/raw").glob("*.nc"):
    #     processed_ds = processor.process_workflow(
    #         nc_file=nc_file,
    #         variables=VARIABLES_TO_PROCESS,
    #         bbox=AFRICA_BBOX,
    #         calculate_stats=True,
    #         export_csv=True,
    #         export_geotiff=False
    #     )
    
    logger.info("=" * 60)
    logger.info("WORKFLOW COMPLETED")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
