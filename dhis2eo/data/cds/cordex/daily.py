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

def prepare_submission_params(start_year, end_year, region, variables, scenario, resolution, gcm_model, rcm_model, ensemble_member):
    '''Create submission params for a single year block in order to get better control over downloaded file names'''
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
        "start_year": [str(start_year)],
        "end_year": [str(end_year)],
        #"download_format": "unarchived", # seems like this is unsupported, so all files are retrieved as zipfiles and have to be extracted
    }
    return params

def prepare_download_lists(client, start_date, end_date, region, dirname, prefix, variables, scenario, resolution, models, overwrite=False):
    '''
    Processes all requested year and model combinations.
    Returns which files exist in cache, and which should be downloaded.
    Job requests will subsequently have to be submitted and checked.
    '''
    start_year = int(str(start_date)[:4])
    end_year = int(str(end_date)[:4])
    start_years = [str(y) for y in ALLOWED_START_YEARS if start_year <= y <= end_year]
    end_years = [str(y) for y in ALLOWED_END_YEARS if start_year <= y <= end_year]

    files = []
    requests = []
    for model in models:
        gcm_model = model_name_to_stub(model['gcm'])
        rcm_model = model_name_to_stub(model['rcm'])
        ensemble_member = model['ens'].lower()
        logger.info(f'GCM model {gcm_model}')
        logger.info(f'RCM model {rcm_model}')
        logger.info(f'Ensemble {ensemble_member}')

        if rcm_model in SKIP_RCMS:
            # HACKY SKIP FOR NOW, SKIP_RCMS IS GLOBAL VARIABLE
            logger.info('Skipping RCM...')
            continue
        
        prefix_full = f'{prefix}_{stub_to_file(scenario)}_{stub_to_file(gcm_model)}_{stub_to_file(rcm_model)}_{stub_to_file(ensemble_member)}'
        for start_year, end_year in zip(start_years, end_years):
            logger.info(f'Years {start_year} to {end_year}')
            
            out_name = f'{prefix_full}_{start_year}-{end_year}.nc'
            out_path = Path(dirname).resolve() / out_name
            
            if overwrite is False and out_path.exists():
                logger.info(f'File already exists, reusing from cache {out_path}')
                files.append(out_path)
                continue
            
            else:
                params = prepare_submission_params(start_year, end_year, region, variables, scenario, resolution, gcm_model, rcm_model, ensemble_member)
                request = {'params': params, 'filepath': out_path}
                requests.append(request)

    # return
    # files contain already cached files, and requests are necessary downloads
    return files, requests

def submit_job(client, params):
    '''Submits single year block as separate job in order to get better control over downloaded file names'''
    # submit download request
    logger.info(f'Requesting climate projection data from CDS API...')
    logger.info(f'Request parameters: \n{json.dumps(params)}')
    remote = client.submit(
        "projections-cordex-domains-single-levels",
        params
    )

    # return
    return remote

# def submit_jobs(client, request):
#     # threaded submissions
#     max_submissions = 2
#     multi_submitter = ThreadPoolExecutor(max_workers=max_submissions)
#     results_lock = threading.Lock()

#     # submit n jobs at a time
#     if requests:
#         ...

#         multi_submitter.submit(submit_job, client, request)

#     # wait for all to be submitted
#     multi_submitter.shutdown(wait=True)

def download_and_extract_file(remote, filepath, clean, bbox):
    logger.info(f'Waiting for results and downloading to {filepath}')

    # create temporary folder that will be deleted after
    with tempfile.TemporaryDirectory(delete=True) as tempdir:
        filepath = Path(filepath)
        filename = filepath.name

        # download zipfile to temporary folder
        tempdir = Path(tempdir)
        temppath = tempdir / filename
        temppath_zip = temppath.with_suffix('.zip')
        remote.download(temppath_zip)

        # extract from zipfile to temporary folder
        with zipfile.ZipFile(temppath_zip, 'r') as zobj:
            # Grab the name of the first (and only) member
            first_file = zobj.namelist()[0]
            
            # Read and write directly to the temporary folder
            with zobj.open(first_file) as source, open(temppath, "wb") as target:
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

        else:
            # no cleaning or bbox requested
            # move the extracted file from tempfolder to final path
            shutil.move(temppath, filepath)

    # finished
    logger.info(f'Finished downloading to {filepath}')

def submit_download_and_extract_file(client, request, clean, bbox, results, results_lock, i, total_downloads):
    try:
        logger.info(f'Processing download request {i+1} of {total_downloads}')

        # submit job
        remote = submit_job(client, request['params'])

        # download and extract the file
        download_and_extract_file(remote, request['filepath'], clean, bbox)

        # add to results list
        with results_lock:
            results.append(request['filepath'])

    except:
        logger.error(traceback.format_exc())

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

    # prepare list of cache files or downloads to request
    files, requests = prepare_download_lists(client, start_date, end_date, region, dirname, prefix, variables, scenario, resolution, models, overwrite=overwrite)
    
    # check how many files to download
    total_downloads = len(requests)

    # download files if needed
    if total_downloads:
        max_downloads = 10
        multi_downloader = ThreadPoolExecutor(max_workers=max_downloads)
        results_lock = threading.Lock()

        # loop all requests
        for i, request in enumerate(requests):
            # add to multi downloader and let it handle concurrent jobs
            multi_downloader.submit(submit_download_and_extract_file, client, request, clean, bbox, files, results_lock, i, total_downloads)

        # wait for all downloads to finish
        multi_downloader.shutdown(wait=True)
        logger.info(f'All {total_downloads} downloads completed')

    # return all local filepaths to user
    return files
