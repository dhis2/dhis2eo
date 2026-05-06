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
# https://earthdatahub.destine.eu/collections/copernicus-dem/datasets/GLO-30


# Internal functions to get data from zarr
def open_zarr(variables):
    # zarr path
    zarr_path = "https://data.earthdatahub.destine.eu/copernicus-dem/GLO-30-v0.zarr"

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
    #logger.info('Correcting longitude coords to range -180 to 180')
    #ds = ds.assign_coords(longitude=((ds.longitude + 180) % 360 - 180)).sortby("longitude")

    # extract the coordinates from input bounding box
    logger.info(f'Computing bbox to use for subsetting')
    xmin, ymin, xmax, ymax = map(float, bbox)

    # add padding to include edge pixels
    xres = abs(float(ds.lon.diff('lon').median()))
    yres = abs(float(ds.lat.diff('lat').median()))
    xmin, xmax = xmin - xres, xmax + xres
    ymin, ymax = ymin - yres, ymax + yres

    # filter to bbox coords
    logger.info(f'Subsetting zarr archive to bbox with padding: {xmin} {ymin} {xmax} {ymax}')
    ds = ds.sel(lon=slice(xmin, xmax), lat=slice(ymin, ymax))  # NOTE: had to switch ymin ymax order to work

    return ds

# def get_zarr_month(ds, year, month):
#     # turn time query parameters into xarray selection
#     _, last_day = calendar.monthrange(year, month)
#     from_date = f"{year}-{month:02d}-01"
#     to_date = f"{year}-{month:02d}-{last_day:02d}"
    
#     logger.info(f'Subsetting zarr archive to date range: {from_date} to {to_date}')
#     ds = ds.sel(valid_time=slice(from_date, to_date))

#     return ds


# Public API to retrieve data for bbox between start and end date
def download(
    bbox: BBox,
    dirname: str,
    prefix: str,
    overwrite: bool = False,
):
    """
    Retrieves ERA5 hourly climate data for a given bbox, variables, and start/end dates.
    Saves to disk in monthly files, as specified by dirname and prefix.
    Returns list of file paths where data was downloaded, e.g. to use with xr.open_mfdataset().
    """
    os.makedirs(dirname, exist_ok=True)

    # Determine the save path
    save_file = f'{prefix}.nc'
    save_path = (Path(dirname) / save_file).resolve()
    files = [save_path]

    # Download or use existing file
    if overwrite is False and save_path.exists():
        # File already exist, load from file instead
        logger.info(f'File already downloaded: {save_path}')
    
    else:
        # Open the full zarr archive
        variables = ['dsm']
        zarr = open_zarr(variables)

        # Restrict to bbox
        ds = get_zarr_region(zarr, bbox)

        # Save to target path
        logger.info("Saving data from DestinE Earth Data Hub...")
        logger.info(f'--> {ds.dims}')
        ds.to_netcdf(save_path)

    # return list of all file downloads
    return files
