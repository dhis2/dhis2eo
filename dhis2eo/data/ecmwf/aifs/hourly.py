import logging
from pathlib import Path
import os

from ecmwf.opendata import Client
import xarray as xr

from ..shared.opendata import fetch_hres_day
from ...utils import force_logging
from ....utils.time import iter_days, ensure_date
from ....utils.types import BBox, DateLike

logger = logging.getLogger(__name__)
force_logging(logger)

'''
https://github.com/ecmwf/ecmwf-opendata

Parameter	Description	Units
10u	10 metre U wind component	m s-1
10v	10 metre V wind component	m s-1
2t	2 metre temperature	K
msl	Mean sea level pressure	Pa
ro	Runoff	m
skt	Skin temperature	K
sp	Surface pressure	Pa
st	Soil Temperature	K
stl1	Soil temperature level 1	K
tcwv	Total column vertically-integrated water vapour	kg m-2
tp	Total Precipitation	m
'''
# see also https://confluence.ecmwf.int/display/DAC/ECMWF+open+data%3A+real-time+forecasts+from+IFS+and+AIFS


# Public download function
def download(
    start: DateLike, 
    end: DateLike, 
    bbox: BBox, 
    dirname: str, 
    prefix: str, 
    variables: list[str], 
    server: str = "aws", 
    overwrite: bool = False,
):
    os.makedirs(dirname, exist_ok=True)
    
    # about the server param
    # "ecmwf" is their own server but that one only has last 4 days and regularly gets throttled
    # more reliable options with longer historical archive: aws, google, azure

    # model type
    model = 'aifs-single'

    # init the client
    client = Client(
        source=server,
        model=model,
        resol="0p25",
        preserve_request_order=False,
        infer_stream_keyword=True,
    )

    start = ensure_date(start)
    end = ensure_date(end)

    files = []
    for day in iter_days(start, end):
        logger.info(f'Day {day}')
        
        # Determine the save path
        save_file = f'{prefix}_{day}.nc'
        save_path = (Path(dirname) / save_file).resolve()
        files.append(save_path)

        # Download or use existing file
        if overwrite is False and save_path.exists():
            # File already exist, load from file instead
            logger.info(f'File already downloaded: {save_path}')
        
        else:
            # Download the data
            ds = fetch_hres_day(client, model, day, bbox, variables)
                
            # Save to target path
            ds.to_netcdf(save_path)

    # return
    return files
