# dhis2eo

[![Test status](https://github.com/dhis2/dhis2eo/actions/workflows/tests.yml/badge.svg)](https://github.com/dhis2/dhis2eo/actions/workflows/tests.yml)
[![Latest version](https://img.shields.io/github/v/release/dhis2/dhis2eo)](https://github.com/dhis2/dhis2eo/releases)

Dhis2eo is a small, focused Python library for DHIS2 users who want to integrate Earth Observation (EO) and climate data into their workflows. It provides lightweight helpers and tools bridging the Python geoscience and DHIS2 ecosystems.

- [What is dhis2eo?](#what-is-dhis2eo)
- [Installation](#installation)
- [Data providers](#data-providers) *(\*setup required)*
- [Contributing](#contributing)

---

## What is dhis2eo?

The purpose of dhis2eo is to help DHIS2 users incorporate earth observation and climate data into their workflows without adding unnecessary complexity. It focuses on the intersection of DHIS2 and geoscience rather than trying to be a full earth observation library.

Key points about dhis2eo:

- **Integration with earth observation tools**:
  Dhis2eo is a thin utility layer that connects DHIS2 workflows with climate data sources and geospatial libraries like **xarray** and **earthkit**. It provides helpers for common climate and earth observation tasks while avoiding the overhead of a full-featured earth observation or geospatial library.

- **Generic functionality across local and national contexts**:
  Dhis2eo is designed to provide generic solutions for DHIS2 users worldwide, while also allowing users to extract and work with data relevant to specific country contexts.

- **Data translation for DHIS2**:
  Dhis2eo supports translating between DHIS2 and earth observation domains, including conversion of data formats and values. It is not meant for general interaction with DHIS2; for that, use the separately maintained [dhis2-python-client](https://github.com/dhis2/dhis2-python-client).

---

## Installation

Install the latest released version from PyPI:

```bash
pip install dhis2eo
```

To test unreleased changes directly from GitHub:

```bash
pip install git+https://github.com/dhis2/dhis2eo
```

For contributors (to get linting and testing tools):

```bash
pip install -e ".[dev]"
```

## Data providers

dhis2eo integrates with major European open-access data platforms and connects directly to data producers at 
research institutes and universities, providing access to a wide range of earth observation datasets through modern cloud-native formats and APIs. 

⚠️ **Important:** Registration and authentication is required for several of the data providers - see linked setup instructions.

### Climate Data Store (CDS)

[Authentication instructions](./dhis2eo/data/cds/AUTHENTICATION.md) | [Browse the catalog](https://cds.climate.copernicus.eu/datasets)

- ERA5-Land
  
  Various climate indicators. Hourly + monthly data, 9km resolution, 1940-Present. 

      from dhis2eo.data.cds import era5_land
      era5_land.hourly.download(...)

- ERA5-HEAT

  Heat related indictors such as UTCI. Hourly data, 25km resolution, 1940-Present. 

      from dhis2eo.data.cds import era5_heat
      era5_heat.hourly.download(...)

- ERA5-Drought

  Drought indices like SPI and SPEI. Monthly data, 25km resolution, 1940-Present. 

      from dhis2eo.data.cds import era5_drought
      era5_drought.monthly.download(...)

- ESA CCI Land Cover

  Land cover classification. Yearly data, 300m resolution, 1992-Recent. 

      from dhis2eo.data.cds import esa_landcover
      esa_landcover.yearly.download(...)

### Atmosphere Data Store (ADS)

[Authentication instructions](./dhis2eo/data/ads/AUTHENTICATION.md) | [Browse the catalog](https://ads.atmosphere.copernicus.eu/datasets). 

- CAMS

  Atmosphere composition and air quality data. 3-hourly data, 75km resolution, 2003-Present (~6 month delay). 

        from dhis2eo.data.ads import cams
        cams.hourly.download(...)

### Destination Earth (DestinE)

[Authentication instructions](./dhis2eo/data/destine/AUTHENTICATION.md) | [Browse the catalog](https://earthdatahub.destine.eu/catalogue)

- ERA5

  Same as the data hosted at CDS, but much faster cloud-optimized access. Hourly data, 25km resolution, 1940-Present. 

      from dhis2eo.data.destine import era5
      era5.hourly.download(...)

- ERA5-Land

  Same as the data hosted at CDS, but much faster cloud-optimized access. Hourly data, 9km resolution, 1940-Present. 

      from dhis2eo.data.destine import era5_land
      era5_land.hourly.download(...)

- Copernicus DEM GLO-30

  Elevation data. Very fast cloud-optimized data access. Static data, 30m resolution. 

      from dhis2eo.data.destine import copernicus_dem_glo30
      copernicus_dem_glo30.static.download(...)

### Copernicus Data Space Ecosystem (CDSE)

[Authentication instructions](./dhis2eo/data/cdse/AUTHENTICATION.md) | [Browse the catalog](https://browser.stac.dataspace.copernicus.eu/)

- CLMS NDVI: Normalized Difference Vegetation Index. 10-daily data, 300m resolution, 2014-Present. 

      from dhis2eo.data.cdse import clms_ndvi
      clms_ndvi.dekadal.download(...)

- CLMS GPP

  Gross primary productivity indicator for vegetation health and stress. 10-daily data, 300m resolution, 2013-Present. 

      from dhis2eo.data.cdse import clms_gpp
      clms_gpp.dekadal.download(...)

### WorldPop at the University of Southampton

No authentication required | [Browse the catalog](https://www.worldpop.org/datacatalog/)

- Population count

  Total count of population per pixel. Yearly data, 100m resolution, 2015-2030. 

      from dhis2eo.data.worldpop import pop_total
      pop_total.yearly.download(...)

### Climate Hazards Center (CHC) at UC Santa Barbara

No authentication required | [Browse the catalog](https://www.chc.ucsb.edu/data). 

- CHIRPS v3

  Precipitation data. Daily data, 5km resolution, 1981-Present. 

      from dhis2eo.data.chc import chirps3
      chirps3.daily.download(...)


## Contributing

Contributions are welcome, whether they are bug fixes, improvements, or new features.

### Design principles

When contributing, please keep in mind that dhis2eo is meant to be:

- Easy to read
- Easy to run
- Easy to maintain
- Easy to integrate into other projects

This leads to a few guiding principles:

- **Fewer things over more things**:
  Every new function, dependency, or tool has a long-term cost.

- **Core libraries over wrappers**:
  If something can be done directly with xarray, earthkit, or standard Python, we usually prefer that over adding a dhis2eo wrapper.

- **Keep it simple**:
  Code should be short, easy to read, debug, and explain to someone new to the project.

- **Lightweight Python tooling**:
  The project is designed to be easy for developers to install, test, and run, with minimal extra dependencies and broad support across operating systems and Python versions.

These principles help keep the project approachable and durable over time.

### Tests

Tests are written with pytest and can be run directly:

```bash
pytest -v
```

To avoid unnecessary computation and server load, tests for the `data` integrations are marked with `@pytest.mark.integration` and are skipped by default. To run them manually: 

```bash
pytest -v -m integration
```

### Code formatting

Code style and linting are handled by ruff and can be run directly:

```bash
ruff check .
ruff format .
```
