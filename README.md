# dhis2eo

Dhis2eo is a small, focused Python library for DHIS2 users who want to integrate earth observation and climate data into their workflows. It provides lightweight helpers and tools bridging the Python geoscience and DHIS2 ecosystems.

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

## Design philosophy

More generally, dhis2eo as a library is meant to be:
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
  Installing, testing, and running the project should work with widely used Python tools and conventions and support a wide range of operating environments and Python versions, without requiring a suite of extra developer tools. 

These principles help keep the project approachable and durable over time.

---

## Installation

```bash
pip install git+https://github.com/dhis2/dhis2eo
```

For contributors:

```bash
pip install -e .
```

## Running tests

Tests are written with pytest and can be run directly:

```bash
pytest -v
```

## Code formatting

Code style and linting are handled by ruff and can be run directly:

```bash
ruff check .
ruff format .
```

## Contributing

Contributions are welcome, whether they are bug fixes, improvements, or new features.

When contributing, please try to align with the design philosophy above. In particular, we aim to keep:

- The public API small
- Dependencies minimal
- Tooling simple and standard
