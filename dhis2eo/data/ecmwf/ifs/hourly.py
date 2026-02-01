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
def fetch_forecast_step(client, day, step, bbox, variables):
    # unpack bbox
    xmin,ymin,xmax,ymax = map(float, bbox)
    
    # forecasts are actually available every 6 hours, but for simplicity we only return the midnight one
    run_time = 0

    # downloads go inside temp dir which handles the cleanup of ALL files generated inside it
    # TODO: might be best to wrap the tempdir in the fetch_day function to speed things up
    with TemporaryDirectory(prefix=f'forecast_{day}-{step}') as tmpdir:
        # download to temp file
        temp_file = Path(tmpdir) / f'file_{day}-{step}.grib2'
        params = dict(
            date=day,
            time=run_time,
            stream="oper",
            type="fc",
            step=step,
            param=variables,
            target=temp_file,
        )
        logger.info(params)
        client.retrieve(params)

        # lazy load crop region from global file, and release file lock
        with xr.open_dataset(temp_file) as ds:
        
            # subset to bbox and load to memory
            # NOTE: loading to memory might not be ideal for a large country, should maybe leave on disk and instead load from files
            crop = ds.sel(longitude=slice(xmin, xmax), latitude=slice(ymax, ymin))
            crop.load()

    # return
    return crop


# Internal function to fetch all forecast steps for a single day
def fetch_day(client, day, bbox, variables):
    # define the full range of forecast steps: 0 to 144 hours every 3h, then 144 to 360 every 6h
    steps = list(range(0, 144 + 3, 3)) + list(range(150, 360 + 6, 6))

    # fetch one step at a time
    step_list = []
    for step in steps:
        logger.info(f'Fetching step {step}')
        ds_step = fetch_forecast_step(client, day, step, bbox, variables)
        step_list.append(ds_step)

    # merge together
    ds = xr.concat(step_list, dim='time')
    
    # return
    return ds


# Public download function
def download(start, end, bbox, dirname, prefix, variables, overwrite=False):
    os.makedirs(dirname, exist_ok=True)
    
    # choose where to get the data
    # "ecmwf" is their own server but that one only has last 4 days and regularly gets throttled
    # more reliable options with longer historical archive: aws, google, azure
    server = "aws"

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
