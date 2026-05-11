# API Authentication for CDSE

Data from Copernicus Data Space Ecosystem (CDSE) is accessed by direct access to their data hosted on AWS S3. This requires authentication by following the steps below.

For more details or troubleshooting, see the [official CDSE guide for authenticating with S3](https://documentation.dataspace.copernicus.eu/APIs/S3.html). 

### 1. Create a CDSE account

The first step is to [create a CDSE user](https://dataspace.copernicus.eu/). 

### 2. Generate S3 user credentials

CDSE hosts their data on AWS S3, and require separate S3 credentials to access the data from S3: 

- Visit the CDSE [S3 credentials page](https://eodata-s3keysmanager.dataspace.copernicus.eu/), and login to your account. 
- Click "Add Credential" and "Confirm" to generate your credentials.
- Copy the displayed Access key and Secret key and keep them somewhere safe (these are only shown once). 

### 3. Create or update your AWS credentials file

Create an AWS credentials file at `$HOME/.aws/credentials` with the contents below:

    [cdse]
    aws_access_key_id = <ACCESS-KEY>
    aws_secret_access_key = <SECRET-KEY>

If you already have this file for other S3 credentials, add the lines to the end of the file. 
