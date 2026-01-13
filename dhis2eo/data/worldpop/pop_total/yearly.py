import logging
import os
import sys
import tempfile
from pathlib import Path
from io import BytesIO

import requests
import xarray as xr
import rioxarray
import numpy as np

from ...utils import force_logging
from ....utils.types import BBox, DateLike

logger = logging.getLogger(__name__)
force_logging(logger)


# Internal logic and helpers

def url_country_for_year(year, country_code, version):
    # generate url to download country geotiff
    if version == 'global1':
        # the older version dataset covers the period 2000 to 2020
        # of the multiple possible variants, this uses the UN adjusted unconstrained dataset
        # as this seems most similar in methodology to the global2 version
        # see https://hub.worldpop.org/Global1_2000-2020
        filename = f"{country_code.lower()}_ppp_{year}_UNadj.tif"
        url = f"https://data.worldpop.org/GIS/Population/Global_2000_2020/{year}/{country_code.upper()}/{filename}"
    elif version == 'global2':
        # the newest version dataset covers the period 2015 to 2030
        # see https://hub.worldpop.org/project/categories?id=3
        filename = f"{country_code.lower()}_pop_{year}_CN_100m_R2025A_v1.tif"
        url = f"https://data.worldpop.org/GIS/Population/Global_2015_2030/R2025A/{year}/{country_code.upper()}/v1/100m/constrained/{filename}"
    return url

def fetch_country_year(year, country_code, version):
    var_name = 'pop_total'

    # get country file url based on the year
    url = url_country_for_year(year, country_code, version)
    logger.info(f"Reading {year} -> {url}")

    # Download country file data
    resp = requests.get(url)
    resp.raise_for_status()
    geotiff_data = resp.content

    # Connect to country dataset
    fobj = BytesIO(geotiff_data)
    da = rioxarray.open_rasterio(fobj)

    # Ensure nodata value is masked and added to metadata
    #nodata = -99999.0 # this should be the worldpop nodata value
    #da = da.where(da != nodata) # this adds nans where nodata for plotting
    #da.rio.write_nodata(nodata, encoded=True, inplace=True) # should write to metadata for future saving

    # Convert to dataset
    ds = da.to_dataset(name=var_name)

    # Remove unnecessary band dim
    ds = ds.squeeze("band", drop=True)

    # Add year constant
    ds = ds.expand_dims(time=[np.datetime64(str(year))])

    # Return
    return ds

# def url_global_for_year(year):
#     # generate url to download global geotiff
#     url = f'https://data.worldpop.org/GIS/Population/Global_2015_2030/R2025A/{year}/0_Mosaicked/v1/1km_ua/constrained/global_pop_{year}_CN_1km_R2025A_UA_v1.tif'
#     return url

# def fetch_global_year(year, bbox, save_path):
#     # get global file url based on the year
#     url = url_global_for_year(year)
#     logger.info(f"Reading {year} -> {url}")

#     # Connect to global dataset lazily
#     da = rioxarray.open_rasterio(
#         url,
#         chunks={'x': 1024, 'y': 1024}  # lazy Dask arrays
#     )
#     logger.info(da)

#     # Read only the bbox window
#     xmin, ymin, xmax, ymax = bbox
#     da = da.rio.clip_box(minx=xmin, miny=ymin, maxx=xmax, maxy=ymax)
#     logger.info(da)


# Public api

def download(start: DateLike, 
             end: DateLike, 
             country_code: str, 
             dirname: str,
             prefix: str,
             version: str = 'global2',
             skip_existing=True
):
    """
    Retrieves WorldPop yearly population count data for a given bbox, variables, and start/end dates.
    Saves to disk in yearly files, as specified by dirname and prefix.
    Returns list of file paths where data was downloaded, e.g. to use with xr.open_mfdataset().
    """
    os.makedirs(dirname, exist_ok=True)

    # Retrieve country geotiff
    files = []
    for year in range(int(start), int(end)+1):
        logger.info(f'Year {year}')

        # Determine the save path
        save_file = f'{prefix}_{year}.nc'
        save_path = (Path(dirname) / save_file).resolve()
        files.append(save_path)

        # Download or use existing file
        if skip_existing and save_path.exists():
            # File already exist, load from file instead
            logger.info(f'File already downloaded: {save_path}')
        
        else:
            # Download the data
            ds = fetch_country_year(year, country_code, version)

            # Save to target path
            ds.to_netcdf(save_path)
    
    # Return downloaded files
    return files

# def retrieve_global(start: DateLike, 
#              end: DateLike, 
#              bbox: BBox, 
#              dirname: str,
#              prefix: str,
#              skip_existing=True
# ):
#     os.makedirs(dirname, exist_ok=True)

#     # Retrieve bbox slice from the global geotiff
#     # But not yet working because not supported by the worldpop server
#     downloads = []
#     for year in range(int(start), int(end)+1):
#         logger.info(f'Year {year}')

#         # Determine the save path
#         save_file = f'{prefix}_{year}.nc'
#         save_path = (Path(dirname) / save_file).resolve()
#         downloads.append(save_path)

#         # download the data if doesnt exist
#         if skip_existing and save_path.exists():
#             logger.info(f'File already downloaded: {save_path}')
#         else:
#             fetch_global_year(year, bbox, save_path)

#     # Return downloaded files
#     return downloads
