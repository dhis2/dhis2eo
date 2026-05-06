import calendar
import json
import logging
from pathlib import Path
import os
from datetime import date, timedelta
import time

from ecmwf.datastores import Client
import xarray as xr

from ...utils import force_logging
from ....utils.time import iter_months
from ....utils.types import BBox, DateLike

logger = logging.getLogger(__name__)
force_logging(logger)


# Internal function to fetch data from the API
def request_years(client, years, months, bbox, variables, use_server_cache):
    # extract the coordinates from input bounding box
    xmin, ymin, xmax, ymax = map(float, bbox)

    # construct the query parameters
    params = {
        "product_type": "monthly_averaged_reanalysis",
        "variable": variables,
        "year": [str(year) for year in years],
        "month": [str(month).zfill(2) for month in months],
        "time": ["00:00"],
        "area": [ymax, xmin, ymin, xmax],  # notice how we reordered the bbox coordinate sequence
        "data_format": "netcdf",
        "download_format": "unarchived",
    }

    # if use_server_cache is False, add tiny numeric flag to invalidate request hash
    # see: https://forum.ecmwf.int/t/how-to-avoid-the-cds-cache-issue/905/2
    if not use_server_cache:
        unique_numeric_string = str(int(time.time()))
        params['nocache'] = unique_numeric_string

    # download the data
    logger.info("Downloading data from CDS API...")
    logger.info(f"Request parameters: \n{json.dumps(params)}")
    remote = client.submit(
        "reanalysis-era5-single-levels-monthly-means",
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
    use_server_cache: bool = True,
    overwrite: bool = False,
):
    """
    Retrieves ERA5 monthly climate data for a given bbox, variables, and start/end dates.
    Saves to disk in a single file for the entire period, as specified by dirname and prefix.
    Returns list with a single file path entry where data was downloaded, e.g. to open directly with xr.open_dataset().
    """
    os.makedirs(dirname, exist_ok=True)

    # Parse dates
    start_year, start_month = map(int, start.split('-')[:2])
    end_year, end_month = map(int, end.split('-')[:2])

    # Set correct years months
    years = list(range(start_year, end_year+1))
    if len(years) == 1:
        months = list(range(start_month, end_month+1))
    else:
        months = list(range(1, 12+1))

    # Create ecmwf client
    client = Client()
    client.check_authentication()

    # Determine the save path
    save_file = f'{prefix}.nc'
    save_path = (Path(dirname) / save_file).resolve()
    files = [save_path]

    # Download or use existing file
    if overwrite is False and save_path.exists():
        # File already exist, load from file instead
        logger.info(f'File already downloaded: {save_path}')
    
    else:
        # Submit job request
        remote = request_years(client=client, years=years, months=months, bbox=bbox, variables=variables, use_server_cache=use_server_cache)

        # Wait for results and save to target path
        remote.download(save_path)

    # return list of all file downloads
    return files
