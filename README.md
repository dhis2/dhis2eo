# dhis2eo

A small, focused Python library for DHIS2 users working with Earth observation and climate data.

dhis2eo bridges the Python geoscience ecosystem and DHIS2, providing helpers and integration tools to make it easier for DHIS2 users to incorporate Earth observation and climate data into their workflows.

---

## Design philosophy

dhis2eo is meant to be:
- Easy to read
- Easy to run
- Easy to maintain
- Easy to integrate into other projects

We try to keep it a **thin, connecting utility layer** that helps integrate DHIS2 with sources of climate data and established geoscience libraries like **xarray** and **earthkit**, rather than a large framework of its own.

This leads to a few guiding principles:

- **Fewer things over more things**:
  Every new function, dependency, or tool has a long-term cost.

- **Core libraries over wrappers**:
  If something can be done directly with xarray, earthkit, or standard Python, we usually prefer that over adding a dhis2eo wrapper.

- **Keep it simple**:
  Code should be short, easy to read, debug, and explain to someone new to the project.

- **Lightweight Python tooling**:
  Installing, testing, and running the project should work with widely used Python tools and conventions, without requiring project-specific command layers or extra developer tools.

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
