import calendar
import logging
from pathlib import Path
import os
from datetime import date, timedelta

import xarray as xr

from ...utils import force_logging
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
# https://earthdatahub.destine.eu/collections/era5/datasets/reanalysis-era5-land


# Internal functions to get data from zarr
def open_zarr(variables):
    # zarr path
    zarr_path = "https://data.earthdatahub.destine.eu/era5/reanalysis-era5-land-no-antartica-v0.zarr"

    # open with xarray
    logger.info(f'Opening zarr archive from {zarr_path}')
    ds = xr.open_dataset(
        zarr_path,
        storage_options={"client_kwargs":{"trust_env":True}},
        chunks={},
        engine="zarr",
    )

    # subset to only the specified variables
    ds = ds[variables]

    return ds

def get_zarr_region(ds, bbox):
    # convert ds longitudes from 0 to 360 to -180 to 180
    logger.info('Correcting longitude coords to range -180 to 180')
    ds = ds.assign_coords(longitude=((ds.longitude + 180) % 360 - 180)).sortby("longitude")

    # extract the coordinates from input bounding box
    logger.info(f'Computing bbox to use for subsetting')
    xmin, ymin, xmax, ymax = map(float, bbox)

    # add padding to include edge pixels
    xres = abs(float(ds.longitude.diff('longitude').median()))
    yres = abs(float(ds.latitude.diff('latitude').median()))
    xmin, xmax = xmin - xres, xmax + xres
    ymin, ymax = ymin - yres, ymax + yres

    # filter to bbox coords
    logger.info(f'Subsetting zarr archive to bbox with padding: {xmin} {ymin} {xmax} {ymax}')
    ds = ds.sel(longitude=slice(xmin, xmax), latitude=slice(ymax, ymin))

    return ds

def get_zarr_month(ds, year, month):
    # turn time query parameters into xarray selection
    _, last_day = calendar.monthrange(year, month)
    from_date = f"{year}-{month:02d}-01"
    to_date = f"{year}-{month:02d}-{last_day:02d}"
    
    logger.info(f'Subsetting zarr archive to date range: {from_date} to {to_date}')
    ds = ds.sel(valid_time=slice(from_date, to_date))

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
            if zarr is None:
                # Open the full zarr archive
                zarr = open_zarr(variables)

                # Restrict to bbox
                zarr = get_zarr_region(zarr, bbox)

            # Fetch zarr data for the month
            ds = get_zarr_month(zarr, year=year, month=month)

            # Save to target path
            logger.info("Saving data from DestinE Earth Data Hub...")
            logger.info(f'--> {ds.dims}')
            ds.to_netcdf(save_path)

    # return list of all file downloads
    return files
