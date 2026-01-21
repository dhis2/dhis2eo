import calendar
import json
import logging
from pathlib import Path
import os
from datetime import date, timedelta

import earthkit.data
import xarray as xr

from ...utils import force_logging
from ....utils.time import iter_months
from ....utils.types import BBox, DateLike

logger = logging.getLogger(__name__)
force_logging(logger)


# Try to fix CDS cache issue by setting download threads to 1
config = earthkit.data.config
config.set("number-of-download-threads", 1)


# Internal function to execute a single monthly file download (API only allows one month at a time)
def fetch_month(year, month, bbox, variables):
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
    Retrieves ERA5-Land hourly climate data for a given bbox, variables, and start/end dates.
    Saves to disk in monthly files, as specified by dirname and prefix.
    Returns list of file paths where data was downloaded, e.g. to use with xr.open_mfdataset().
    """
    os.makedirs(dirname, exist_ok=True)

    start_year, start_month = map(int, start.split('-')[:2])
    end_year, end_month = map(int, end.split('-')[:2])

    # Determine last date for which we can expect ERA5-Land to be complete
    # ERA5-Land seems to have roughly 6-7 days of lag
    # Meaning only on the 7th of a new month, can we expect that the previous month contains all days
    current_date = date.today()
    last_updated_date = current_date - timedelta(days=7)

    files = []
    for year, month in iter_months(start_year, start_month, end_year, end_month):
        logger.info(f'Month {year}-{month}')

        # Skip if month is expected to be incomplete
        # Technically speaking CDS allows us to download monthly files with only some of the days.
        # However, this introduces the issue of caching partial monthly downloads, where we would have to check if
        # ...each file contains all days.
        # As a simple solution, we instead check if the month is expected to be complete (based on the reported publishing lag of ERA5-Land)
        # ...and issue a warning that we don't download incomplete months. 
        # I think this should be fine in the DHIS2 context where reporting tends to happen for each month.
        if (year,month) >= (last_updated_date.year, last_updated_date.month):
            logger.warning(
                f'Skipping downloads for months that are expected to be incomplete (~7 days of lag).'
                f'Latest available date expected in ERA5-Land: {last_updated_date.isoformat()}'
            )
            continue

        # Determine the save path
        save_file = f'{prefix}_{year}-{str(month).zfill(2)}.nc'
        save_path = (Path(dirname) / save_file).resolve()
        files.append(save_path)

        # Download or use existing file
        if overwrite is False and save_path.exists():
            # File already exist, load from file instead
            logger.info(f'File already downloaded: {save_path}')
        
        else:
            # Download the data
            ds = fetch_month(year=year, month=month, bbox=bbox, variables=variables)
                
            # Save to target path
            ds.to_netcdf(save_path)

    # return list of all file downloads
    return files
