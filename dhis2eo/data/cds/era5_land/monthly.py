import json
import logging
import os
from pathlib import Path

import earthkit.data
import xarray as xr

from ...utils import force_logging
from ....utils.types import DateLike, BBox

logger = logging.getLogger(__name__)
force_logging(logger)

# Try to fix CDS cache issue by setting download threads to 1
config = earthkit.data.config
config.set("number-of-download-threads", 1)


# Internal function to fetch data from the CDS API
def fetch_years(years, months, bbox, variables):
    """Download monthly era5-land data"""

    # extract the coordinates from input bounding box
    xmin, ymin, xmax, ymax = map(float, bbox)

    # construct the query parameters
    params = {
        "product_type": ["monthly_averaged_reanalysis"],
        "variable": variables,
        "year": [str(year) for year in years],
        "month": [str(month).zfill(2) for month in months],
        "time": ["00:00"],
        "area": [ymax, xmin, ymin, xmax],  # notice how we reordered the bbox coordinate sequence
        "data_format": "netcdf",
        "download_format": "unarchived",
    }

    # download the data
    logger.info("Downloading data from CDS API...")
    logger.info(f"Request parameters: \n{json.dumps(params)}")
    data = earthkit.data.from_source("cds", "reanalysis-era5-land-monthly-means", **params)

    # load lazily from disk using xarray
    ds = xr.open_dataset(data.path)

    # clean unnecessary data
    ds = ds.drop_vars(['number', 'expver'])

    # return
    return ds

# Public API to retrieve data for bbox between start and end date
def download(
    start: DateLike,
    end: DateLike,
    bbox: BBox,
    dirname: str,
    prefix: str,
    variables: list[str],
    overwrite: bool = False,
):
    """
    Retrieves ERA5-Land monthly climate data for a given bbox, variables, and start/end dates.
    Saves to disk in a single file for the entire period, as specified by dirname and prefix.
    Returns list with a single file path entry where data was downloaded, e.g. to open directly with xr.open_dataset().
    """
    os.makedirs(dirname, exist_ok=True)

    start_year = int(start)
    end_year = int(end)
    years = range(start_year, end_year+1)
    months = range(1, 12+1)

    files = []

    # Determine the save path
    save_file = f'{prefix}_{start_year}-{end_year}.nc'
    save_path = (Path(dirname) / save_file).resolve()
    files.append(save_path)

    # Download or use existing file
    if overwrite is False and save_path.exists():
        # File already exist, load from file instead
        logger.info(f'File already downloaded: {save_path}')
    
    else:
        # Download the data
        ds = fetch_years(years=years, months=months, bbox=bbox, variables=variables)
            
        # Save to target path
        ds.to_netcdf(save_path)

    # return list of all file downloads
    return files
