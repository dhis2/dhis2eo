import calendar
import logging
from pathlib import Path
import os
from datetime import date, timedelta

import xarray as xr

from ...utils import force_logging, open_zarr, select_ds_region, select_ds_month
from ....utils.time import iter_months
from ....utils.types import BBox, DateLike

logger = logging.getLogger(__name__)
force_logging(logger)


# Requirements: 
# Access to DestinE datasets requires registering an account with DestinE Earth Data Hub.
# Free accounts have a monthly request limit of 500,000, and can be checked on user account page.
# Authentication is handled by xarray and aiohttp and requires a .netrc (unix) or _netrc (windows) file
# in user's home folder. 
# see: https://earthdatahub.destine.eu/getting-started

# Note:
# Full dataset details and list of variables:
# https://earthdatahub.destine.eu/collections/era5/datasets/reanalysis-era5-single-levels


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
    Retrieves ERA5 hourly climate data for a given bbox, variables, and start/end dates.
    Saves to disk in monthly files, as specified by dirname and prefix.
    Returns list of file paths where data was downloaded, e.g. to use with xr.open_mfdataset().
    """
    os.makedirs(dirname, exist_ok=True)

    # Zarr url
    zarr_path = "https://data.earthdatahub.destine.eu/era5/reanalysis-era5-single-levels-v0.zarr"

    # Parse dates
    start_year, start_month = map(int, start.split('-')[:2])
    end_year, end_month = map(int, end.split('-')[:2])

    # Determine last date for which we can expect ERA5-Land to be complete
    # DestinE's ERA5-Land seems to have roughly 15 days of lag
    # Meaning only on the 15th of a new month, can we expect that the previous month contains all days
    current_date = date.today()
    last_updated_date = current_date - timedelta(days=15)

    # Begin monthly downloads
    zarr = None
    files = []
    for year, month in iter_months(start_year, start_month, end_year, end_month):
        logger.info(f'Month {year}-{month}')

        # Skip if month is expected to be incomplete
        # DestinE updates the full previous month on the 15th
        if (year,month) >= (last_updated_date.year, last_updated_date.month):
            logger.warning(
                f'Skipping downloads for months that are expected to be incomplete (~15 days of lag). '
                f'Latest available date expected in ERA5: {last_updated_date.isoformat()}'
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
            if zarr is None:
                # Open the full zarr archive
                zarr = open_zarr(zarr_path, variables)

                # Restrict to bbox
                zarr = select_ds_region(zarr, bbox, x_dim='longitude', y_dim='latitude', fix_360_longitude=True)

            # Fetch zarr data for the month
            ds = select_ds_month(zarr, year=year, month=month, time_dim='valid_time')

            # Save to target path
            logger.info("Saving data from DestinE Earth Data Hub...")
            logger.info(f'--> {ds.dims}')
            ds.to_netcdf(save_path)

    # return list of all file downloads
    return files
