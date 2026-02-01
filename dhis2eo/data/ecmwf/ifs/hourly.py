import calendar
import json
import logging
from pathlib import Path
import os
from datetime import date, timedelta
from tempfile import TemporaryDirectory

from ecmwf.opendata import Client
import xarray as xr

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


# Internal function to fetch a single forecast step file download (API only allows one forecast step at a time)
def fetch_forecast_step(client, day, step, variables, dirname):    
    # forecasts are actually available every 6 hours, but for simplicity we only return the midnight one
    run_time = 0

    # download to temp file
    temp_file = Path(dirname) / f'file_{day}-{step}.grib2'
    params = dict(
        date=day,
        time=run_time,
        stream="oper",
        type="fc",
        step=step,
        param=variables,
        target=temp_file,
    )
    client.retrieve(params)

    # return
    return temp_file


# Internal function to fetch all forecast steps for a single day
def fetch_day(client, day, bbox, variables):
    # define the full range of forecast steps: 0 to 144 hours every 3h, then 144 to 360 every 6h
    steps = list(range(0, 144 + 3, 3)) + list(range(150, 360 + 6, 6))

    # downloads go inside temp dir which handles the cleanup of ALL files generated inside it
    # TODO: might be best to wrap the tempdir in the fetch_day function to speed things up
    with TemporaryDirectory(prefix=f'forecast_{day}', delete=True) as tmpdir:

        # fetch one step at a time
        logger.info(f'Downloading to temporary folder {tmpdir}')
        files = []
        for step in steps:
            file = fetch_forecast_step(client, day, step, variables, dirname=tmpdir)
            files.append(file)

        # lazy open all global files
        logger.info('Cropping to bbox')
        with xr.open_mfdataset(files, combine='nested', concat_dim='time') as ds:
            # extract only the bbox and load to memory
            xmin,ymin,xmax,ymax = map(float, bbox)
            crop = ds.sel(longitude=slice(xmin, xmax), latitude=slice(ymax, ymin))
            crop.load()

    # return
    return crop


# Public download function
def download(start, end, bbox, dirname, prefix, variables, server="aws", overwrite=False):
    os.makedirs(dirname, exist_ok=True)
    
    # about the server param
    # "ecmwf" is their own server but that one only has last 4 days and regularly gets throttled
    # more reliable options with longer historical archive: aws, google, azure

    # init the client
    client = Client(
        source=server,
        model="ifs",
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
            ds = fetch_day(client, day, bbox, variables)
                
            # Save to target path
            ds.to_netcdf(save_path)

    # return
    return files
