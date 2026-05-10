import os
import logging

import rasterio
from rasterio.session import AWSSession
import rioxarray
import pystac_client

from ..utils import force_logging

logger = logging.getLogger(__name__)
force_logging(logger)

STAC_URL = "https://stac.dataspace.copernicus.eu/v1"

S3_URL = "eodata.dataspace.copernicus.eu"
#S3_URL = "https://eodata.dataspace.copernicus.eu"
#S3_ACCESS_KEY = os.getenv('CDSE_S3_ACCESS_KEY')
#S3_SECRET_KEY = os.getenv('CDSE_S3_SECRET_KEY')

def get_rasterio_s3_env():
    """
    Build a rasterio Env that authenticates against your custom S3 endpoint.
    GDAL's /vsis3/ driver reads this env to authenticate range requests,
    which is what makes lazy/windowed reads possible without downloading
    the full file.
    """
    #aws_session = AWSSession(endpoint_url=S3_URL, aws_access_key_id=S3_ACCESS_KEY, aws_secret_access_key=S3_SECRET_KEY)
    
    return rasterio.Env(
        #session=aws_session,
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

def connect_stac():
    logger.info(f'Connecting to STAC {STAC_URL}')
    catalog = pystac_client.Client.open(STAC_URL)
    return catalog
