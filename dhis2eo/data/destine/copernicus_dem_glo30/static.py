import calendar
import logging
from pathlib import Path
import os
from datetime import date, timedelta

import xarray as xr

from ...utils import force_logging, open_zarr, select_ds_region
from ....utils.time import iter_months
from ....utils.types import BBox, DateLike

logger = logging.getLogger(__name__)
force_logging(logger)


# Note:
# Full dataset details and list of variables:
# https://earthdatahub.destine.eu/collections/copernicus-dem/datasets/GLO-30


# Public API to retrieve data for bbox between start and end date
def download(
    bbox: BBox,
    dirname: str,
    prefix: str,
    overwrite: bool = False,
):
    """
    Retrieves Copernicus DEM GLO-30 static elevation data for a given bbox.
    Saves to disk in a single file, as specified by dirname and prefix.
    Returns list of the single file path where data was downloaded, e.g. to use with xr.open_dataset().
    """
    os.makedirs(dirname, exist_ok=True)

    # Zarr url
    zarr_path = "https://data.earthdatahub.destine.eu/copernicus-dem/GLO-30-v0.zarr"

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
        ds = open_zarr(zarr_path, variables)

        # Restrict to bbox
        ds = select_ds_region(ds, bbox, x_dim='lon', y_dim='lat')

        # Save to target path
        logger.info("Saving data from DestinE Earth Data Hub...")
        logger.info(f'--> {ds.dims}')
        ds.to_netcdf(save_path)

    # return list of all file downloads
    return files
