import calendar
import json
import logging
import os
import time
from datetime import date, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

from ecmwf.datastores import Client
import xarray as xr

from ...utils import force_logging
from ....utils.time import iter_months
from ....utils.types import BBox, DateLike

logger = logging.getLogger(__name__)
force_logging(logger)


# Internal function to execute a single monthly file download (API only allows one month at a time)
def submit_month(client, year, month, bbox, variables):
    # extract the coordinates from input bounding box
    xmin, ymin, xmax, ymax = map(float, bbox)

    # construct the query parameters
    _, last_day = calendar.monthrange(year, month)
    days = [day for day in range(1, last_day + 1)]
    days = [str(day).zfill(2) for day in days]
    params = {
        "variable": variables,
        "year": str(year),
        "month": [str(month).zfill(2)],
        "day": days,
        "time": [f"{str(h).zfill(2)}:00" for h in range(0, 23 + 1)],
        "area": [ymax, xmin, ymin, xmax],  # notice how we reordered the bbox coordinate sequence
        "data_format": "netcdf",
        "download_format": "unarchived",
    }

    # download the data with earthkit
    logger.info("Downloading data from CDS API...")
    logger.info(f"Request parameters: \n{json.dumps(params)}")
    remote = client.submit(
        "reanalysis-era5-land",
        params
    )

    return remote


def submit(client, start, end, bbox, dirname, prefix, variables, overwrite):
    os.makedirs(dirname, exist_ok=True)

    # Parse dates
    start_year, start_month = map(int, start.split('-')[:2])
    end_year, end_month = map(int, end.split('-')[:2])

    # Determine last date for which we can expect ERA5-Land to be complete
    # ERA5-Land seems to have roughly 6-7 days of lag
    # Meaning only on the 7th of a new month, can we expect that the previous month contains all days
    current_date = date.today()
    last_updated_date = current_date - timedelta(days=7)

    files = []
    request_ids = []
    for year, month in iter_months(start_year, start_month, end_year, end_month):
        logger.info(f'Month {year}-{month}')

        # Skip if month is expected to be incomplete
        # Technically speaking CDS allows us to download monthly files with only some of the days.
        # However, this introduces the issue of caching partial monthly downloads, where we would have to check if
        # ...each file contains all days.
        # As a simple solution, we instead check if the month is expected to be complete (based on the reported publishing lag of ERA5-Land)
        # ...and issue a warning that we don't download incomplete months. 
        # I think this should be fine in the DHIS2 context where reporting tends to happen for each month.
        if (year,month) >= (last_updated_date.year, last_updated_date.month):
            logger.warning(
                f'Skipping downloads for months that are expected to be incomplete (~7 days of lag).'
                f'Latest available date expected in ERA5-Land: {last_updated_date.isoformat()}'
            )
            continue

        # Determine the save path
        save_file = f'{prefix}_{year}-{str(month).zfill(2)}.nc'
        save_path = (Path(dirname) / save_file).resolve()
        files.append(save_path)

        # Download or use existing file
        if overwrite is False and save_path.exists():
            # File already exist, load from file instead
            logger.info(f'File already downloaded: {save_path}')
            request_ids.append(None)
        
        else:
            # Submit month request
            remote = submit_month(client, year=year, month=month, bbox=bbox, variables=variables)
            request_ids.append(remote.request_id)

    # return
    results = list(zip(files, request_ids))
    return results


def download_to_path(remote, filepath):
    logger.info('Request ready, downloading to', filepath)

    # download to path
    remote.download(filepath)

    # finished
    logger.info('Finished downloading to', filepath)


# Public API to retrieve data for bbox between start and end date
def download(
    start: DateLike,
    end: DateLike,
    bbox: BBox,
    dirname: str,
    prefix: str,
    variables: list[str],
    overwrite: bool = False,
):
    """
    Retrieves ERA5-Land hourly climate data for a given bbox, variables, and start/end dates.
    Saves to disk in monthly files, as specified by dirname and prefix.
    Returns list of file paths where data was downloaded, e.g. to use with xr.open_mfdataset().
    """
    # create ecmwf client
    client = Client()
    client.check_authentication()

    # submit or get from cache
    results = submit(client, start, end, bbox, dirname, prefix, variables, overwrite=overwrite)
    
    # check how many files to download
    total_downloads = sum([1 for filepath,request_id in results if request_id])

    # download files if needed
    if total_downloads:
        max_downloads = 5
        multi_downloader = ThreadPoolExecutor(max_workers=max_downloads)

        # continuously check and collect results
        while True:
            # check for any remaining request ids
            remaining = [
                (i, filepath, client.get_remote(request_id)) 
                for i,(filepath,request_id) in enumerate(results)
                if request_id
            ]

            if remaining:
                logger.info(f'Progress: {total_downloads - len(remaining)} of {total_downloads} job requests finished')

                # get list of ready results
                ready = [
                    (i, filepath, remote)
                    for i,filepath,remote in remaining
                    if remote.results_ready
                ]

                # process first ready results if available
                if ready:
                    # get first of the ready
                    i, filepath, remote = ready[0]
                    
                    # download the file
                    multi_downloader.submit(download_to_path, remote, filepath)
                
                    # set request id to None to indicate that job is no longer running
                    results[i] = (filepath, None)

                else:
                    # all remaining are still processing
                    # take a break before checking again
                    time.sleep(15)
                    
            else:
                # stop checking if no remaining request ids
                logger.info(f'All {total_downloads} job requests completed, waiting for downloads to finish')
                multi_downloader.shutdown(wait=True)
                break
    
    # return all local filepaths to user
    filepaths = [filepath for filepath,request_id in results]
    return filepaths
