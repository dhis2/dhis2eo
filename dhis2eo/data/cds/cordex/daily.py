import json
import time
import zipfile
import shutil
import logging
import tempfile
import traceback
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import xarray as xr
import numpy as np
from ecmwf.datastores import Client

logger = logging.getLogger(__name__)

# TODO: handle below RCMs... 
SKIP_RCMS = [
    'ictp_regcm4_7', # yearly downloads rather than 5-yearly
    'ictp_regcm4_6', # yearly downloads rather than 5-yearly
    'ipsl_wrf381p', # single download for all years, start 2006 end 2100
]
ALLOWED_START_YEARS = list(range(2006, 2091+1, 5))
ALLOWED_END_YEARS = list(range(2010, 2095+1, 5))

def model_name_to_stub(name):
    return name.replace('-', '_').lower()

def stub_to_file(stub):
    return stub.replace('_', '-')

def submit_model_years(client, start_years, end_years, region, variables, scenario, resolution, gcm_model, rcm_model, ensemble_member, prefix_full, results, results_lock):
    '''Submits job request for a model combination and the full range of years'''
    try:
        # construct the query parameters
        params = {
            "domain": region,
            "experiment": scenario,
            "horizontal_resolution": resolution,
            "temporal_resolution": "daily_mean",
            "variable": variables,
            "gcm_model": model_name_to_stub(gcm_model),
            "rcm_model": model_name_to_stub(rcm_model),
            "ensemble_member": ensemble_member,
            "start_year": [str(y) for y in start_years],
            "end_year": [str(y) for y in end_years],
            #"download_format": "unarchived", # seems like this is unsupported, so all files are retrieved as zipfiles and have to be extracted
        }

        # download the data
        logger.info(f'Requesting climate projection data from CDS API...')
        logger.info(f'Request parameters: \n{json.dumps(params)}')
        remote = client.submit(
            "projections-cordex-domains-single-levels",
            params
        )

        # add to submitted requests
        request = {'id': remote.request_id, 'file_prefix': prefix_full}
        with results_lock:
            results[remote.request_id] = request
    
    except:
        logger.error(traceback.format_exc())

def submit(client, start_date, end_date, region, dirname, prefix, variables, scenario, resolution, models, bbox=None, overwrite=False):
    '''
    Submits all requested year and model combinations as separate jobs.
    Returns immediately with list of (filename,job_id) pairs.
    Jobs will subsequently have to be checked and results downloaded.
    '''
    start_year = int(str(start_date)[:4])
    end_year = int(str(end_date)[:4])
    start_years = [str(y) for y in ALLOWED_START_YEARS if start_year <= y <= end_year]
    end_years = [str(y) for y in ALLOWED_END_YEARS if start_year <= y <= end_year]

    max_submissions = 2
    multi_submitter = ThreadPoolExecutor(max_workers=max_submissions)
    results_lock = threading.Lock()

    files = []
    requests = {}
    for model in models:
        gcm_model = model_name_to_stub(model['gcm'])
        rcm_model = model_name_to_stub(model['rcm'])
        ensemble_member = model['ens'].lower()
        logger.info(f'GCM model {gcm_model}')
        logger.info(f'RCM model {rcm_model}')
        logger.info(f'Ensemble {ensemble_member}')

        # HACKY SKIP FOR NOW, SKIP_RCMS IS GLOBAL VARIABLE
        if rcm_model in SKIP_RCMS:
            logger.info('Skipping RCM...')
            continue
        
        # determine which years are missing or needs to be downloaded
        prefix_full = f'{prefix}_{stub_to_file(scenario)}_{stub_to_file(gcm_model)}_{stub_to_file(rcm_model)}_{stub_to_file(ensemble_member)}'
        missing_start_years = []
        missing_end_years = []
        for start_year, end_year in zip(start_years, end_years):
            out_name = f'{prefix_full}_{start_year}-{end_year}.nc'
            out_path = Path(dirname) / out_name
            
            if overwrite is False and out_path.exists():
                logger.info(f'File already exists, reusing from cache {out_path}')
                files.append(out_path)
                continue
            
            else:
                missing_start_years.append(start_year)
                missing_end_years.append(end_year)

        # submit job for all missing years in one request
        if missing_start_years:
            logger.info(f'Submitting download request for years {missing_end_years[0]} to {missing_end_years[-1]}')
            multi_submitter.submit(submit_model_years, client, missing_start_years, missing_end_years, region, variables, scenario, resolution, gcm_model, rcm_model, ensemble_member, prefix_full, requests, results_lock)

    # wait for all to be submitted
    multi_submitter.shutdown(wait=True)

    # return
    # files contain already cached files, and requests are necessary downloads
    return files, requests

def extract_file(zip_archive, zip_member, tempdir, dirname, filename_prefix, results, results_lock, clean, bbox):
    '''Extract and process a single file from a zip archive'''
    # Determine filename and paths from zip filename
    logger.info(f'Extracting and processing from zipfile: {zip_member}')
    date_part = zip_member.split('_')[-1]
    start_date, end_date = date_part.replace('.nc', '').split('-')
    start_year, end_year = start_date[:4], end_date[:4]
    filename = f'{filename_prefix}_{start_year}-{end_year}.nc'
    filepath = Path(dirname) / filename
    temppath = Path(tempdir) / filename

    # Read and write directly to the temporary folder
    with zip_archive.open(zip_member) as source, open(temppath, "wb") as target:
        shutil.copyfileobj(source, target)

    # cleaning or bbox crop requires opening with xarray
    if clean or bbox is not None:
        # open dataset
        with xr.open_dataset(temppath) as d:

            # fix datasets with lon 0-360
            # always for bbox crop, or if cleaning
            # TODO: this likely won't detect 0-360 for South American domain? 
            if clean or bbox is not None:
                lon_var = 'lon' if 'lon' in d else 'longitude'
                if d[lon_var].max() > 180:
                    logger.info('Converting longitude coordinates from 0 to 360, to -180 to 180')
                    lon = ((d[lon_var] + 180) % 360) - 180
                    d = d.assign_coords({lon_var: lon})
            
            if clean:
                logger.info('Cleaning and optimizing file')
                # rename to rlat/rlon vars
                if 'rlat' not in d:
                    d = d.rename({'y': 'rlat', 'x': 'rlon'})

                # rename to lat/lon vars
                if 'lat' not in d:
                    d = d.rename({'latitude': 'lat', 'longitude': 'lon'})

                # round dataset coordinates due to minor cross-model decimal differences
                # actually no: too dangerous and too much various, needs ref grid or manual handling
                # digits = 6
                # d = d.assign_coords(
                #     rlon=np.round(d.rlon, digits),
                #     rlat=np.round(d.rlat, digits),
                #     lon=np.round(d.lon, digits),
                #     lat=np.round(d.lat, digits),
                # )

                # remove unnecessary datavar
                dropnames = [
                    varname
                    for varname in 'time_bnds time_bounds rlat_bnds rlon_bnds bounds_lon bounds_lat rotated_latitude_longitude lat_vertices lon_vertices'.split()
                    if varname in d.data_vars
                ]
                if dropnames:
                    d = d.drop_vars(dropnames)

                # prevent rotated pole scalar from being broadcast and balooning file size
                # convert from datavar to coordinate
                if 'rotated_pole' in d.data_vars:
                    d = d.set_coords('rotated_pole')
                if 'Lambert_Conformal' in d.data_vars:
                    d = d.set_coords('Lambert_Conformal')

            # subset to bbox incl edge padding
            if bbox is not None:
                logger.info('Cropping to bbox')
                xmin,ymin,xmax,ymax = bbox
                xres = abs(d.lon.max() - d.lon.min()) / len(d.rlon)
                yres = abs(d.lat.max() - d.lat.min()) / len(d.rlat)
                mask = (
                    (d.lat >= (ymin-yres)) & (d.lat <= (ymax+yres)) &
                    (d.lon >= (xmin-xres))  & (d.lon <= (xmax+xres))
                ).compute()
                d = d.where(mask, drop=True).compute()

            # add compression for smaller filesize
            encoding = {var: {'zlib': True, 'complevel': 4} for var in d.data_vars}

            # save to final path
            d.to_netcdf(filepath, encoding=encoding)

            # add to list of result files
            with results_lock:
                results.append(filepath)

    else:
        # no cleaning or bbox requested
        # move the extracted file from tempfolder to final path
        shutil.move(temppath, filepath)

        # add to list of result files
        with results_lock:
            results.append(filepath)

def download_and_extract_files(remote, dirname, filename_prefix, results, results_lock, clean=True, bbox=None):
    logger.info(f'Request ready, downloading and extracting to {dirname}/{filename_prefix}_*')

    try:
        # create temporary folder that will be deleted after
        with tempfile.TemporaryDirectory(delete=True) as tempdir:
            # download zipfile to temporary folder
            tempdir = Path(tempdir)
            temppath = tempdir / filename_prefix
            temppath_zip = temppath.with_suffix('.zip')
            try:
                remote.download(temppath_zip)
            except:
                logger.error(traceback.format_exc())
                return

            # extract from zipfile to temporary folder
            with zipfile.ZipFile(temppath_zip, 'r') as zobj:
                # Loop and extract each netcdf file in the archive
                for name in zobj.namelist():
                    if name.endswith('.nc'):
                        extract_file(zobj, name, tempdir, dirname, filename_prefix, results, results_lock, clean=clean, bbox=bbox)
    
    except:
        logger.error(traceback.format_exc())

    # finished
    logger.info(f'Finished downloading to {dirname}/{filename_prefix}_*')

def download(start_date, end_date, region, dirname, prefix, variables, scenario, resolution, models, clean=True, bbox=None, overwrite=False):
    # valid variable names
    # - "2m_air_temperature"
    # - "maximum_2m_temperature_in_the_last_24_hours"
    # - "minimum_2m_temperature_in_the_last_24_hours"
    # - "2m_relative_humidity"
    # - "mean_precipitation_flux"

    # create ecmwf client
    client = Client()
    client.check_authentication()

    # submit or get from cache
    files_list, requests = submit(client, start_date, end_date, region, dirname, prefix, variables, scenario, resolution, models, overwrite=overwrite)
    
    # check how many files to download
    total_downloads = len(requests)

    # download files if needed
    if total_downloads:
        max_downloads = 10
        multi_downloader = ThreadPoolExecutor(max_workers=max_downloads)
        files_list_lock = threading.Lock()

        # continuously check and collect results
        while True:
            # if any remaining requests
            if requests:
                logger.info(f'Progress: {total_downloads - len(requests)} of {total_downloads} job requests finished')

                # get first of ready results if any
                ready = None
                for request in requests.values():
                    remote = client.get_remote(request['id'])
                    if remote.results_ready:
                        request['remote'] = remote
                        ready = request
                        break

                # process first ready results if available
                if ready:
                    # download and extract the file
                    #extract_file(remote, filepath, clean, bbox)
                    multi_downloader.submit(download_and_extract_files, ready['remote'], dirname, ready['file_prefix'], files_list, files_list_lock, clean, bbox)
                
                    # remove request from dict of running requests
                    del requests[ready['id']]

                else:
                    # all remaining are still processing
                    # take a break before checking again
                    time.sleep(60)
                    
            else:
                # stop checking if no remaining request ids
                logger.info(f'All {total_downloads} job requests completed, waiting for downloads to finish')
                multi_downloader.shutdown(wait=True)
                break
    
    # return all local filepaths to user
    return files_list