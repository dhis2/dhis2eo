import os
import json
import logging
from calendar import monthrange
from pathlib import Path
from datetime import date, timedelta
from tempfile import NamedTemporaryFile

import xarray as xr
from ecmwfapi import ECMWFDataServer

from ...utils import force_logging
from ....utils.time import iter_months
from ....utils.types import BBox, DateLike, MaybeString

logger = logging.getLogger(__name__)
force_logging(logger)


# Define variable codes used by ECMWF API
# TODO: Need to add more... 
VARIABLE_TO_CODE = {
    'total_precipitation': '228228',
    '2m_temperature': '167'
}


# Internal function to execute a single monthly file download (API only allows one month at a time)
def fetch_month(year, month, bbox, variables, last_updated_date, resolution=None):
    # FINAL WORKING PARAMS
    variable_codes = [VARIABLE_TO_CODE[varname] for varname in variables]
    xmin,ymin,xmax,ymax = map(float, bbox)
    
    _, days_in_month = monthrange(year, month)
    fromdate = date(year, month, 1)
    todate = min(date(year, month, days_in_month), last_updated_date) # dont request further than last updated date
    
    resolution = resolution or 0.5  # native TIGGE resolution is 0.5, finer resolutions will be interpolated on the server

    # file is first downloaded to temporary location, before modifying and saving to final output
    with NamedTemporaryFile(delete=True) as tmp:
        # params
        temp_target = f'{tmp.name}.grib'
        params = {
            "class": "ti",             # TIGGE class
            "dataset": "tigge",        # Dataset identifier
            "date": f"{fromdate.isoformat()}/to/{todate.isoformat()}",
            "expver": "prod",          # Production version
            "grid": f"{resolution:.2f}/{resolution:.2f}",       # Default and best is 0.5 grid resolution for ecmwf origin
            "area": f'{ymax}/{xmin}/{ymin}/{xmax}', #[ymax, xmin, ymin, xmax], #"50/10/40/20",  # N/W/S/E (subsetting coordinates)
            "levtype": "sfc",          # Surface level
            "origin": "ecmf",          # Forecasting center (ECMWF)
            "param": "/".join(variable_codes),            # Parameter code (e.g., 2m temperature)
            "time": "00",              # Only take the midnight forecast
            "step": "0/6/12/18/24/30/36/42/48/54/60/66/72/78/84/90/96/102/108/114/120/126/132/138/144/150/156/162/168/174/180/186/192/198/204/210/216/222/228/234/240/246/252/258/264/270/276/282/288/294/300/306/312/318/324/330/336/342/348/354/360",
            "type": "cf",              # Control forecast (best estimate)
            "target": temp_target,
        }

        # download the data to temp file
        logger.info("Downloading data from ECMWF API...")
        logger.info(f"Request parameters: \n{json.dumps(params)}")
        server = ECMWFDataServer()
        server.retrieve(params)

        # open as xarray
        ds = xr.open_dataset(temp_target)
        ds.load()
        
    # fix longitudes from 0 to 360 to -180 to 180
    ds.coords['longitude'] = (ds.coords['longitude'] + 180) % 360 - 180

    # return
    return ds


# Public API to retrieve data for bbox between start and end date
def download(
    start: DateLike,
    end: DateLike,
    bbox: BBox,
    dirname: str,
    prefix: str,
    variables: list[str],
    resolution: MaybeString = None,
    overwrite: bool = False,
):
    """
    Retrieves TIGGE 6-hourly climate forecasts for a given bbox, variables, and start/end dates.
    Saves to disk in monthly files, as specified by dirname and prefix.
    Returns list of file paths where data was downloaded, e.g. to use with xr.open_mfdataset().
    """
    os.makedirs(dirname, exist_ok=True)

    start_year, start_month = map(int, start.split('-')[:2])
    end_year, end_month = map(int, end.split('-')[:2])

    # Determine last date for which we can expect TIGGE to be complete
    # TIGGE data seems to have roughly 2 days of lag (48 hours)
    # Meaning only on the 3th of a new month, can we expect that the previous month contains all days
    current_date = date.today()
    last_updated_date = current_date - timedelta(days=3)

    files = []
    for year, month in iter_months(start_year, start_month, end_year, end_month):
        logger.info(f'Month {year}-{month}')

        # Check if month is expected to be incomplete
        month_is_complete = True
        if (year,month) >= (last_updated_date.year, last_updated_date.month):
            logger.warning(
                f'Month is expected to be incomplete (~2 days of lag) and will be downloaded regardless of cache.'
                f'Latest available date expected in TIGGE: {last_updated_date.isoformat()}'
            )
            month_is_complete = False

        # Determine the save path
        save_file = f'{prefix}_{year}-{str(month).zfill(2)}.nc'
        save_path = (Path(dirname) / save_file).resolve()
        files.append(save_path)

        # Download or use existing file
        if overwrite is False and save_path.exists() and month_is_complete:
            # File already exist, load from file instead
            logger.info(f'File already downloaded: {save_path}')
        
        else:
            # Download the data
            ds = fetch_month(year=year, month=month, bbox=bbox, 
                             variables=variables, 
                             last_updated_date=last_updated_date,
                             resolution=resolution)
                
            # Save to target path
            ds.to_netcdf(save_path)

    # return list of all file downloads
    return files
