import os
import logging
from itertools import groupby
from pathlib import Path

import rasterio
import rioxarray
import pystac_client
import fsspec

from ..utils import force_logging

logger = logging.getLogger(__name__)
force_logging(logger)

STAC_URL = "https://stac.dataspace.copernicus.eu/v1"

S3_URL = "eodata.dataspace.copernicus.eu"  # without https:// or s3 prefix
S3_PROFILE = 'cdse'  # profile name containing the s3 credentials in ~/.aws/credentials


# raw aws s3 files

def save_s3_file(fs, fs_path, save_path):
    logger.info(f'Downloading file {fs_path} to {save_path}')

    logger.info(f"Testing fs.ls on eodata: {fs.ls('eodata')}")
    logger.info(f"Testing fs.exists on path: {fs.exists(fs_path)}")
    
    fs.get(fs_path, save_path)

def connect_s3():
    # connect and authenticate with s3 storage
    logger.info(f'Connecting to s3 {S3_URL}')
    fs = fsspec.filesystem(
        "s3",
        client_kwargs={"endpoint_url": f'https://{S3_URL}'},
        config_kwargs={"s3": {"addressing_style": "path"}},
        profile=S3_PROFILE,
    )
    return fs


# rasterio

def get_rasterio_s3_env():
    """
    Build a rasterio Env that authenticates against your custom S3 endpoint.
    GDAL's /vsis3/ driver reads this env to authenticate range requests,
    which is what makes lazy/windowed reads possible without downloading
    the full file.
    """
    return rasterio.Env(
        AWS_S3_ENDPOINT=S3_URL,
        AWS_PROFILE='cdse',
        AWS_VIRTUAL_HOSTING="FALSE",  # prevents bucket.endpoint → endpoint/bucket
        AWS_HTTPS="YES",
    )

def read_rioxarray_window(url, bbox):
    # get rasterio s3 env with authentication
    s3_env = get_rasterio_s3_env()

    # Connect to global dataset lazily
    with s3_env:
        da = rioxarray.open_rasterio(
            url,
            chunks=None, # disable dask, not needed and actually slows things down
            masked=False,
            lock=False,
        )
    
    # Read only the bbox window
    xmin, ymin, xmax, ymax = bbox
    da = da.rio.clip_box(minx=xmin, miny=ymin, maxx=xmax, maxy=ymax)
    da = da.load()
    
    return da


# stac

def connect_stac():
    logger.info(f'Connecting to STAC {STAC_URL}')
    catalog = pystac_client.Client.open(STAC_URL)
    return catalog

def group_stac_items_by_year(items):
    key = lambda item: item.datetime.year
    for year,subitems in groupby(sorted(items, key=key), key=key):
        yield year, list(subitems)

def save_stac_asset(fs, item, asset_name, save_folder):
    url = item.assets[asset_name].href
    filename = Path(url).name
    save_path = Path(save_folder) / filename
    save_s3_file(fs, url, save_path)
