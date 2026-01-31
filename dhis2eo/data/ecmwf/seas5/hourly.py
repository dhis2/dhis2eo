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
    'total_precipitation': '228',
    '2m_temperature': '167',
}


# Internal function to execute a single monthly file download (API only allows one month at a time)
def fetch_month(year, month, bbox, variables, last_updated_date, resolution=None):
    # FINAL WORKING PARAMS
    variable_codes = [VARIABLE_TO_CODE[varname] for varname in variables]
    xmin,ymin,xmax,ymax = map(float, bbox)
    
    _, days_in_month = monthrange(year, month)
    fromdate = date(year, month, 1)
    todate = min(date(year, month, days_in_month), last_updated_date) # dont request further than last updated date
    
    resolution = resolution or 0.25  # native SEAS5 resolution is approx 0.25, finer resolutions will be interpolated on the server

    # TODO: Almost there but cant figure out how to set the dataset name
    # the retrieve function fails when it tries to get info endpoint for provided dataset/service name
    # since it works with tigge data, the following url should be accurate
    # https://api.ecmwf.int/v1/datasets/tigge/info

    # NOTE: 
    # All ecmwf datasets: https://www.ecmwf.int/en/forecasts/datasets
    # Set V - Seasonal 7-month forecast (SEAS) seems to be part of ECMWF real time products which are supported to be on a CC4 license: https://www.ecmwf.int/en/forecasts/datasets/catalogue-ecmwf-real-time-products
    # Clicking that gets you to the dataset description page: https://www.ecmwf.int/en/forecasts/datasets/set-v#V-v-a
    # You can also select it from the shopping cart: https://products.ecmwf.int/shopping-cart/orders/new
    # It's even possible create the mars query and check the availability which seems accessible for my logged in user
    # https://apps.ecmwf.int/archive-catalogue/?origin=ecmf&stream=mmsf&levtype=sfc&time=00%3A00%3A00&system=5&expver=1&month=nov&method=1&year=2025&date=2025-11-01&type=fc&class=od
    # But still no hint as to what the dataset or service name should be in the api
    # It might look like the only datasets that are still supported via the non-mars service are the TIGGE and S2S datasets
    # https://apps.ecmwf.int/datasets/ 
    # Very good resource for how to use the client here
    # https://confluence.ecmwf.int/display/WEBAPI/Access+ECMWF+Public+Datasets
    # Also very good resource and the definite guide to SEAS5 is the user guide
    # https://www.ecmwf.int/sites/default/files/elibrary/2021/81237-seas5-user-guide_1.pdf

    # NOTE:
    # More resources
    # Using the python client with mars: https://confluence.ecmwf.int/display/WEBAPI/Access+MARS#AccessMARS-key
    # About historical datasets and access: https://www.ecmwf.int/en/forecasts/accessing-forecasts/order-historical-datasets
    # The full "archive catalog" browsing: https://apps.ecmwf.int/archive-catalogue/
    # Interesting but think it's about the CDS API: https://forum.ecmwf.int/t/ecmwf-apis-faq-api-data-documentation/6880

    # file is first downloaded to temporary location, before modifying and saving to final output
    with NamedTemporaryFile(delete=True) as tmp:
        # params
        temp_target = f'{tmp.name}.grib'
        hours_in_7_months = 24 * 30 * 7
        params = {
            "dataset": "seas5", #seasonal",
            "class": "od",             # Operational Datasets class
            "stream": "mmsf",           # direct subdaily forecast values
            "system": "5",              # Seas5?
            "method": "1",              # only the first 7 month forecasts, set to "3" for 13 month forecasts
            "expver": "1",
            "type": "em",               # Ensemble mean instead of individual members
            "date": f"{fromdate.isoformat()}/to/{todate.isoformat()}",
            "grid": f"{resolution:.2f}/{resolution:.2f}",       # Default and best is 0.5 grid resolution for ecmwf origin
            "area": f'{ymax}/{xmin}/{ymin}/{xmax}', #[ymax, xmin, ymin, xmax], #"50/10/40/20",  # N/W/S/E (subsetting coordinates)
            "levtype": "sfc",          # Surface level
            "param": "/".join(variable_codes),            # Parameter code (e.g., 2m temperature)
            "time": "00",              # Only take the midnight forecast
            "step": f"0/to/{hours_in_7_months}/by/6",     # 3 months at 6 hour intervals
            "target": temp_target,
        }

        # params = {
        #     "class": "od",
        #     "stream": "mmsf",
        #     "expver": "001",
        #     "system": "5",
        #     "method": "1",
        #     "type": "pf",                # or "em"
        #     "param": "167",
        #     "date": "20260101/to/20260127",
        #     "time": "00:00:00",
        #     "step": "0/to/5040/by/6",
        #     "area": [10.0004, -13.3035, 6.9176, -10.2658],
        #     "grid": "0.25/0.25",
        #     "target": "seas5_t2m.grib"
        # }

        # download the data to temp file
        logger.info("Downloading data from ECMWF API...")
        logger.info(f"Request parameters: \n{json.dumps(params)}")
        server = ECMWFDataServer()
        server.retrieve(params)
        # alternative download
        # https://confluence.ecmwf.int/display/WEBAPI/Access+MARS#AccessMARS-availability
        #del params['dataset']
        #del params['target']
        #from ecmwfapi import ECMWFService
        #server = ECMWFService()
        #server.execute(params, temp_target)

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
    Retrieves SEAS5 6-hourly climate forecasts for a given bbox, variables, and start/end dates.
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
