import calendar
import json
import logging
from pathlib import Path
import os

import earthkit.data
import xarray as xr

from ...utils import force_logging
from ....utils.time import iter_months
from ....utils.types import BBox, DateLike

logger = logging.getLogger(__name__)
force_logging(logger)

DEFAULT_VARIABLES = [
    "2m_temperature",
    "total_precipitation",
]


# Try to fix CDS cache issue by setting download threads to 1
config = earthkit.data.config
config.set("number-of-download-threads", 1)


# Internal function to execute a single monthly file download (API only allows one month at a time)
def fetch_month(save_path, year, month, bbox, variables=None):
    # get default variables
    variables = variables or DEFAULT_VARIABLES

    # extract the coordinates from input bounding box
    xmin, ymin, xmax, ymax = map(float, bbox)

    # construct the query parameters
    _, last_day = calendar.monthrange(year, month)
    days = [day for day in range(1, last_day + 1)]
    days = [str(day).zfill(2) for day in days]
    params = {
        "variable": variables,
        "year": str(year),
        "month": [str(month).zfill(2)],
        "day": days,
        "time": [f"{str(h).zfill(2)}:00" for h in range(0, 23 + 1)],
        "area": [ymax, xmin, ymin, xmax],  # notice how we reordered the bbox coordinate sequence
        "data_format": "netcdf",
        "download_format": "unarchived",
    }

    # download the data with earthkit
    logger.info("Downloading data from CDS API...")
    logger.info(f"Request parameters: \n{json.dumps(params)}")
    data = earthkit.data.from_source("cds", "reanalysis-era5-land", **params)

    # load lazily from disk using xarray
    ds = xr.open_dataset(data.path)

    # clean unnecessary data
    ds = ds.drop_vars(['number', 'expver'])

    # return
    return ds


# Public API to retrieve data for bbox between start and end date
def retrieve(
    start: DateLike,
    end: DateLike,
    bbox: BBox,
    dirname: str,
    prefix: str,
    skip_existing=True,
    variables=None,
):
    """Retrieves hourly ERA5-Land data for a given bbox, variables, and start/end dates,
    saving the file downloads to a target directory and returning the list of filepaths."""
    os.makedirs(dirname, exist_ok=True)

    start_year, start_month = map(int, start.split('-')[:2])
    end_year, end_month = map(int, end.split('-')[:2])

    files = []
    for year, month in iter_months(start_year, start_month, end_year, end_month):
        logger.info(f'Month {year}-{month}')

        # determine the save path
        save_file = f'{prefix}_{year}-{str(month).zfill(2)}.nc'
        save_path = (Path(dirname) / save_file).resolve()
        files.append(save_path)

        # Skip if already exist
        # TODO: should not skip if this is the current month
        if skip_existing and save_path.exists():
            logger.info(f'File already downloaded: {save_path}')
            continue
        
        # Download the data
        ds = fetch_month(save_path, year=year, month=month, 
                            bbox=bbox, variables=variables)
            
        # Save to target path
        ds.to_netcdf(save_path)

    # return list of all file downloads
    return files
