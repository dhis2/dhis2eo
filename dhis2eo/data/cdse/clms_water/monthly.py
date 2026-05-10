import os
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import time

import numpy as np

from ..shared import connect_stac, read_rioxarray_window
from ....utils.types import BBox
from ....data.utils import force_logging

logger = logging.getLogger(__name__)
force_logging(logger)

def fetch_month(url, year, month, bbox, var_name):
    logger.info(f"Reading {year}-{month} -> {url}")
    t = time.time()

    # read bbox of cloud hosted raster
    da = read_rioxarray_window(url, bbox)

    # Ensure nodata value is masked and added to metadata
    #nodata = 251 # this should be the correct nodata value
    #da = da.where(da != nodata) # this adds nans where nodata for plotting
    #da.rio.write_nodata(nodata, encoded=True, inplace=True) # should write to metadata for future saving

    # Convert to dataset
    ds = da.to_dataset(name=var_name)

    # Remove unnecessary band dim
    ds = ds.squeeze("band", drop=True)

    # Add day constant
    ds = ds.expand_dims(time=[np.datetime64(f'{year}-{month:02d}')])

    # Return
    #logger.info(f'Downloaded {year}-{month} in {time.time()-t} seconds')
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
    Retrieves CLMS Waterbodies 100m monthly data for a given bbox.
    Saves files to disk, as specified by dirname and prefix.
    """
    os.makedirs(dirname, exist_ok=True)

    # connect to copernicus stac catalog
    catalog = connect_stac()

    # find all tiles for given bbox
    collection_id = "clms_wb_global_100m_monthly_v1_cog"
    search = catalog.search(
        collections=[collection_id],
        bbox=bbox,
        datetime=[start, end],
    )

    # process each tile
    variable = "wb100_wb"  # or "wb100_qual" for level of water occurence, but for simplicity dont give user that option
    files = []
    for item in search.items():
        logger.info(f'Tile {item}')

        # Get info from tile
        url = item.assets[variable].href
        year, month = item.datetime.year, item.datetime.month

        # Determine the save path
        save_file = f'{prefix}_{year}-{month}.tif'
        save_path = (Path(dirname) / save_file).resolve()
        files.append(save_path)

        # Download or use existing file
        if overwrite is False and save_path.exists():
            # File already exist, load from file instead
            logger.info(f'File already downloaded: {save_path}')

        else:
            # Download the data
            ds = fetch_month(url, year=year, month=month, bbox=bbox, var_name=variable)

            # Save to file
            ds.to_netcdf(save_path)

    return files
