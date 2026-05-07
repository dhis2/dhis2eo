import json
import logging
from pathlib import Path
import os
from datetime import date, timedelta
import time
import tempfile
import zipfile
import shutil

from ecmwf.datastores import Client
import xarray as xr

from ...utils import force_logging
from ....utils.time import iter_months
from ....utils.types import BBox, DateLike

logger = logging.getLogger(__name__)
force_logging(logger)


# Internal function to fetch data from the API
def save_month(client, save_path, year, month, leadtimes, bbox, variables, model, system, use_server_cache):
    # extract the coordinates from input bounding box
    xmin, ymin, xmax, ymax = map(float, bbox)

    # construct the query parameters
    # NOTE: for now we force only downloading forecasts from the first of month
    # since most models indeed only have forecasts on 1st of month
    # however many models also have new forecasts every single day, but just not
    # sure how users should select which day between start-end dates. it would become 
    # very slow to download all days since CDS request size limit peaks at ca 1 day per download
    params = {
        "originating_centre": model,
        "system": str(system),
        "variable": variables,
        "year": str(year),
        "month": [str(month).zfill(2)],
        "day": ["01"],
        "leadtime_hour": [str(hour) for hour in leadtimes],
        "area": [ymax, xmin, ymin, xmax],  # notice how we reordered the bbox coordinate sequence
        "data_format": "netcdf",
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
        "seasonal-original-single-levels",
        params
    )
    remote.download(save_path)


# Public API to retrieve data for bbox between start and end date
def download(
    start: DateLike,
    end: DateLike,
    bbox: BBox,
    dirname: str,
    prefix: str,
    variables: list[str],
    model: str,
    system: str,
    use_server_cache: bool = True,
    overwrite: bool = False,
):
    """
    Retrieves C3S seasonal forecast data at 6-hourly intervals for a given bbox, variables, and start/end dates.
    Saves to disk in monthly files, as specified by dirname and prefix.
    Returns list of file paths where data was downloaded, e.g. to use with xr.open_mfdataset().
    """
    os.makedirs(dirname, exist_ok=True)

    # Parse dates
    start_year, start_month = map(int, start.split('-')[:2])
    end_year, end_month = map(int, end.split('-')[:2])

    # Include all possible leadtimes for seas5
    leadtimes = list(range(0, 5160+1, 6))

    # Create ecmwf client
    client = Client()
    client.check_authentication()

    # Begin downloads
    # NOTE: Although ecmwf-datastores allows asynch jobs, seasonal dataset is limited to max 1 runnning job
    # ...so just doing it regular synchronously
    files = []
    for year, month in iter_months(start_year, start_month, end_year, end_month):
        logger.info(f'Month {year}-{month}')

        # Determine the save path
        save_file = f'{prefix}_{year}-{month}.nc'
        save_path = (Path(dirname) / save_file).resolve()
        files.append(save_path)

        # Download or use existing file
        if overwrite is False and save_path.exists():
            # File already exist, load from file instead
            logger.info(f'File already downloaded: {save_path}')
        
        else:
            # Submit job request and save to file
            save_month(client=client, save_path=save_path, year=year, month=month, leadtimes=leadtimes, bbox=bbox, variables=variables, model=model, system=system, use_server_cache=use_server_cache)

    # return list of all file downloads
    return files