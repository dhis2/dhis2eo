import json
import time
import zipfile
import shutil
from pathlib import Path

from ecmwf.datastores import Client

# TODO: rcm 'ICTP-RegCM4-7' uses yearly rather than 5-yearly downloads, need to handle differently... 
SKIP_RCMS = ['ictp_regcm4_7']
ALLOWED_START_YEARS = list(range(2006, 2091+1, 5))
ALLOWED_END_YEARS = list(range(2010, 2095+1, 5))

def model_name_to_stub(name):
    return name.replace('-', '_').lower()

def submit_year_block(client, start_year, end_year, region, save_path, variables, scenario, resolution, gcm_model, rcm_model):
    '''Submits single year block as separate job in order to get better control over downloaded file names'''
    # construct the query parameters
    params = {
        "domain": region,
        "experiment": scenario,
        "horizontal_resolution": resolution,
        "temporal_resolution": "daily_mean",
        "variable": variables,
        "gcm_model": model_name_to_stub(gcm_model),
        "rcm_model": model_name_to_stub(rcm_model),
        "ensemble_member": "r1i1p1",
        "start_year": [str(start_year)],
        "end_year": [str(end_year)],
        #"download_format": "unarchived", # seems like this is unsupported, so all files are retrieved as zipfiles and have to be extracted
    }

    # download the data
    print(f'Requesting climate projection data from CDS API...')
    print(f'Request parameters: \n{json.dumps(params)}')
    remote = client.submit(
        "projections-cordex-domains-single-levels",
        params
    )

    return remote

def submit(client, start_date, end_date, region, dirname, prefix, variables, scenario, resolution, models, overwrite=False):
    '''
    Submits all requested year and model combinations as separate jobs.
    Returns immediately with list of (filename,job_id) pairs.
    Jobs will subsequently have to be checked and results downloaded.
    '''
    start_year = int(str(start_date)[:4])
    end_year = int(str(end_date)[:4])
    start_years = [str(y) for y in ALLOWED_START_YEARS if start_year <= y <= end_year]
    end_years = [str(y) for y in ALLOWED_END_YEARS if start_year <= y <= end_year]

    files = []
    request_ids = []
    for model in models:
        gcm_model = model['gcm']
        rcm_model = model['rcm']
        print(f'GCM model {gcm_model}')
        print(f'RCM model {rcm_model}')

        if model_name_to_stub(rcm_model) in SKIP_RCMS:
            # HACKY SKIP FOR NOW, SKIP_RCMS IS GLOBAL VARIABLE
            print('Skipping RCM...')
            continue
        
        for start_year, end_year in zip(start_years, end_years):
            print(f'Years {start_year} to {end_year}')
            
            # TODO: download file should use standardized model stub names
            out_path = Path(dirname).resolve() / f'{prefix}_{gcm_model}_{rcm_model}_{start_year}-{end_year}.nc'
            
            if overwrite is False and out_path.exists():
                print('File already exists, reusing from cache', out_path)
                files.append(out_path)
                request_ids.append(None)
                continue
            
            else:
                files.append(out_path)
                remote = submit_year_block(client, start_year, end_year, region, out_path, variables, scenario, resolution, gcm_model, rcm_model)
                request_ids.append(remote.request_id)

    # return
    results = list(zip(files, request_ids))
    return results

def download_to_path(remote, filepath):
    # download as a zipfile
    filepath_zip = Path(filepath).with_suffix('.zip')
    remote.download(filepath_zip)

    # extract file from zipfile to target filename
    with zipfile.ZipFile(filepath_zip, 'r') as zobj:
        # Grab the name of the first (and only) member
        first_file = zobj.namelist()[0]
        
        # Read and write directly to the new path
        with zobj.open(first_file) as source, open(filepath, "wb") as target:
            shutil.copyfileobj(source, target)

    # delete the zipfile afterwards
    filepath_zip.unlink()

def get(start_date, end_date, region, dirname, prefix, variables, scenario, resolution, models, overwrite=False):
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
    results = submit(client, start_date, end_date, region, dirname, prefix, variables, scenario, resolution, models, overwrite=overwrite)
    
    # check how many files to download
    total_downloads = sum([1 for filepath,request_id in results if request_id])

    # download files if needed
    if total_downloads:
        # continuously check and collect results
        while True:
            # check for any remaining request ids
            remaining = [
                (i, filepath, client.get_remote(request_id)) 
                for i,(filepath,request_id) in enumerate(results)
                if request_id
            ]

            if remaining:
                print(f'Progress: {total_downloads - len(remaining)} of {total_downloads} downloads finished')

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
                    print('Request ready, downloading to', filepath)
                    
                    # download the file
                    download_to_path(remote, filepath)
                    print('Finished downloading to', filepath)
                
                    # set requiest id to None to indicate that job is no longer running
                    results[i] = (filepath, None)

                else:
                    # all remaining are still processing
                    # take a break before checking again
                    time.sleep(15)
                    
            else:
                # stop checking if no remaining request ids
                print(f'All {total_downloads} downloads finished')
                break
    
    # return all local filepaths to user
    filepaths = [filepath for filepath,request_id in results]
    return filepaths