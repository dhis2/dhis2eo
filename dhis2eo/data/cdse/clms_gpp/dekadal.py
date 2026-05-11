import os
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import xarray as xr

from ..shared import connect_stac, group_stac_items_by_month, read_rioxarray_window
from ....utils.types import BBox
from ...utils import force_logging

logger = logging.getLogger(__name__)
force_logging(logger)

def fetch_dekad(url, year, month, day, bbox, var_name):
    #logger.info(f"Reading {year}-{month}-{day} -> {url}")
    #t = time.time()

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
    ds = ds.expand_dims(time=[np.datetime64(f'{year}-{month:02d}-{day:02d}')])

    # Return
    #logger.info(f'Downloaded {year}-{month} in {time.time()-t} seconds')
    return ds

def fetch_month(items, year, month, bbox, var_name):
    # loop and download all month dekads
    ds_list = []
    for item in items:
        # Get info from item
        day = item.datetime.day
        url = item.assets[f'gpp300_{var_name}'].href
        
        # Get dekad from url
        ds = fetch_dekad(url, year, month, day, bbox, var_name)
        ds_list.append(ds)
    
    # merge dekads into month ds
    ds = xr.merge(ds_list)

    # return
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
    Retrieves CLMS GPP 300m 10-daily data for a given start/end date, and bbox.
    Saves files to disk, as specified by dirname and prefix.
    """
    os.makedirs(dirname, exist_ok=True)

    # connect to copernicus stac catalog
    catalog = connect_stac()

    # find all tiles for given bbox
    collection_id = "clms_gpp_global_300m_10daily_v2_cog"
    search = catalog.search(
        collections=[collection_id],
        bbox=bbox,
        datetime=[start, end],
    )

    # process each tile
    variable = "gpp"  # or "qflag" for quality flag, but for simplicity dont give user that option
    files = []
    for (year, month), items in group_stac_items_by_month(search.items()):
        logger.info(f'Month {year}-{month}')

        # Determine the save path
        save_file = f'{prefix}_{year}-{month:02d}.nc'
        save_path = (Path(dirname) / save_file).resolve()
        files.append(save_path)

        # Download or use existing file
        if overwrite is False and save_path.exists():
            # File already exist, load from file instead
            logger.info(f'File already downloaded: {save_path}')

        else:
            # Download the data
            ds = fetch_month(items, year=year, month=month, bbox=bbox, var_name=variable)

            # Save to file
            ds.to_netcdf(save_path)

    return files
