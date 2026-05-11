import os
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import time
import tempfile

import numpy as np
import xarray as xr

from ..shared import connect_s3, connect_stac, group_stac_items_by_year, save_stac_asset
from ....utils.types import BBox
from ....data.utils import force_logging

logger = logging.getLogger(__name__)
force_logging(logger)


def fetch_year(fs, items, year, bbox):
    with tempfile.TemporaryDirectory(delete=True) as tmpdir:

        # loop and download all item assets
        # each modis tile can be downloaded as a .hdf file from the 'data' asset
        asset_name = 'data'
        for item in items:
            logger.info(item.assets)
            save_stac_asset(fs, item, asset_name, tmpdir)

        # open all downloaded assets as single xarray
        ds = xr.open_mfdataset(Path(tmpdir) / '*.hdf')
        logger.info(ds)

        # crop to bbox
        xmin,ymin,xmax,ymax = bbox
        ds = ds.sel(longitude=slice(xmin, xmax), latitude=slice(ymax, ymin))
        ds = ds.load()  # loads into memory so tile folder can be safely deleted
        logger.info(ds)

    # Ensure nodata value is masked and added to metadata
    #nodata = 251 # this should be the correct nodata value
    #da = da.where(da != nodata) # this adds nans where nodata for plotting
    #da.rio.write_nodata(nodata, encoded=True, inplace=True) # should write to metadata for future saving

    # # Convert to dataset
    # ds = da.to_dataset(name=var_name)

    # # Remove unnecessary band dim
    # ds = ds.squeeze("band", drop=True)

    # Add year constant
    ds = ds.expand_dims(time=[np.datetime64(f'{year}-01')])

    # Return
    return ds

def download(
    start: str,
    end: str,
    bbox: BBox,
    dirname: str,
    prefix: str,
    overwrite: bool = False,
) -> list[Path]:
    """
    Retrieves MODIS Land Cover 500m yearly data for a given start/end year, and bbox.
    Saves files to disk, as specified by dirname and prefix.
    """
    os.makedirs(dirname, exist_ok=True)

    # connect to s3
    fs = connect_s3()

    # connect to copernicus stac catalog
    catalog = connect_stac()

    # find all stac tiles for given bbox
    collection_id = "modis-terraaqua-mcd12q1"
    search = catalog.search(
        collections=[collection_id],
        bbox=bbox,
        datetime=[start, end],
    )

    # process each tile
    files = []
    for year, items in group_stac_items_by_time(search.items(), 'year'):
        logger.info(f'Year {year}')

        # Determine the save path
        save_file = f'{prefix}_{year}.nc'
        save_path = (Path(dirname) / save_file).resolve()
        files.append(save_path)

        # Download or use existing file
        if overwrite is False and save_path.exists():
            # File already exist, load from file instead
            logger.info(f'File already downloaded: {save_path}')

        else:
            # Download the data
            ds = fetch_year(fs, items, year=year, bbox=bbox)

            # Save to file
            ds.to_netcdf(save_path)

    return files
