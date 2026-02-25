import json
import logging
import os
from pathlib import Path

from ecmwf.datastores import Client
import xarray as xr

from ...utils import force_logging
from ....utils.types import DateLike, BBox

logger = logging.getLogger(__name__)
force_logging(logger)


# Internal function to fetch data from the CDS API
def request_years(client, years, months, bbox, variables):
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
    remote = client.submit(
        "reanalysis-era5-land-monthly-means",
        params
    )

    # return
    return remote

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

    # Parse dates
    start_year = int(start)
    end_year = int(end)
    years = range(start_year, end_year+1)
    months = range(1, 12+1)

    # Create ecmwf client
    client = Client()
    client.check_authentication()

    # Determine the save path
    files = []
    save_file = f'{prefix}_{start_year}-{end_year}.nc'
    save_path = (Path(dirname) / save_file).resolve()
    files.append(save_path)

    # Download or use existing file
    if overwrite is False and save_path.exists():
        # File already exist, load from file instead
        logger.info(f'File already downloaded: {save_path}')
    
    else:
        # Submit job request
        remote = request_years(client=client, years=years, months=months, bbox=bbox, variables=variables)
            
        # Wait for results and save to target path
        remote.download(save_path)

    # return list of all file downloads
    return files
