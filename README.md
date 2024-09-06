[![GitHub release; latest by date](https://img.shields.io/github/v/release/SETI/rms-filecache)](https://github.com/SETI/rms-filecache/releases)
[![GitHub Release Date](https://img.shields.io/github/release-date/SETI/rms-filecache)](https://github.com/SETI/rms-filecache/releases)
[![Test Status](https://img.shields.io/github/actions/workflow/status/SETI/rms-filecache/run-tests.yml?branch=main)](https://github.com/SETI/rms-filecache/actions)
[![Documentation Status](https://readthedocs.org/projects/rms-filecache/badge/?version=latest)](https://rms-filecache.readthedocs.io/en/latest/?badge=latest)
[![Code coverage](https://img.shields.io/codecov/c/github/SETI/rms-filecache/main?logo=codecov)](https://codecov.io/gh/SETI/rms-filecache)
<br />
[![PyPI - Version](https://img.shields.io/pypi/v/rms-filecache)](https://pypi.org/project/rms-filecache)
[![PyPI - Format](https://img.shields.io/pypi/format/rms-filecache)](https://pypi.org/project/rms-filecache)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/rms-filecache)](https://pypi.org/project/rms-filecache)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/rms-filecache)](https://pypi.org/project/rms-filecache)
<br />
[![GitHub commits since latest release](https://img.shields.io/github/commits-since/SETI/rms-filecache/latest)](https://github.com/SETI/rms-filecache/commits/main/)
[![GitHub commit activity](https://img.shields.io/github/commit-activity/m/SETI/rms-filecache)](https://github.com/SETI/rms-filecache/commits/main/)
[![GitHub last commit](https://img.shields.io/github/last-commit/SETI/rms-filecache)](https://github.com/SETI/rms-filecache/commits/main/)
<br />
[![Number of GitHub open issues](https://img.shields.io/github/issues-raw/SETI/rms-filecache)](https://github.com/SETI/rms-filecache/issues)
[![Number of GitHub closed issues](https://img.shields.io/github/issues-closed-raw/SETI/rms-filecache)](https://github.com/SETI/rms-filecache/issues)
[![Number of GitHub open pull requests](https://img.shields.io/github/issues-pr-raw/SETI/rms-filecache)](https://github.com/SETI/rms-filecache/pulls)
[![Number of GitHub closed pull requests](https://img.shields.io/github/issues-pr-closed-raw/SETI/rms-filecache)](https://github.com/SETI/rms-filecache/pulls)
<br />
![GitHub License](https://img.shields.io/github/license/SETI/rms-filecache)
[![Number of GitHub stars](https://img.shields.io/github/stars/SETI/rms-filecache)](https://github.com/SETI/rms-filecache/stargazers)
![GitHub forks](https://img.shields.io/github/forks/SETI/rms-filecache)

# Introduction

`filecache` is a Python module that provides filecache flux density from a variety of
models. These models are currently supported:

| Name       | Wavelength range (microns) |
| ---------- | -------------------------- |
| Colina     | 0.1195 to 2.5              |
| Kurucz     | 0.15 to 300                |
| Rieke      | 0.2 to 30                  |
| STIS       | 0.1195 to 2.7              |
| STIS_Rieke | 0.1195 to 30               |

`filecache` is a product of the [PDS Ring-Moon Systems Node](https://pds-rings.seti.org).

# Installation

The `filecache` module is available via the `rms-filecache` package on PyPI and can be
installed with:

```sh
pip install rms-filecache
```

# Getting Started

The `filecache` module provides five functions:

- [`flux_density`](https://rms-filecache.readthedocs.io/en/latest/module.html#filecache.flux_density):
  Compute the flux density of a filecache model in the specified units.
- [`bandpass_flux_density`](https://rms-filecache.readthedocs.io/en/latest/module.html#filecache.bandpass_flux_density):
  Compute the average filecache flux density over a filter bandpass.
- [`mean_flux_density`](https://rms-filecache.readthedocs.io/en/latest/module.html#filecache.mean_flux_density):
  Compute average filecache flux density over the bandpass of a "boxcar" filter.
- [`bandpass_f`](https://rms-filecache.readthedocs.io/en/latest/module.html#filecache.bandpass_f):
  Compute the filecache F averaged over a filter bandpass.
- [`mean_f`](https://rms-filecache.readthedocs.io/en/latest/module.html#filecache.mean_f):
  Compute average filecache F over the bandpass of a "boxcar" filter.

These functions take or return `Tabulation` objects. For more information on `Tabulation`
objects see the [`rms-tabulation`](https://github.com/SETI/rms-tabulation) package.

Details of each function are available in the [module documentation](https://rms-filecache.readthedocs.io/en/latest/module.html).

Here is an example that plots the filecache flux density for the visual range of 400
to 700 nm using the Rieke model at 2 AU in units of nm for wavelength and
W/m^2/nm for flux:

```python
import matplotlib.pyplot as plt
import filecache

flux = filecache.flux_density(model='rieke', xunits='nm', units='W/m^2/nm', filecache_range=2)
flux = flux.clip(400, 700)
plt.plot(flux.x, flux.y)
plt.show()
```

# Contributing

Information on contributing to this package can be found in the
[Contributing Guide](https://github.com/SETI/rms-filecache/blob/main/CONTRIBUTING.md).

# Links

- [Documentation](https://rms-filecache.readthedocs.io)
- [Repository](https://github.com/SETI/rms-filecache)
- [Issue tracker](https://github.com/SETI/rms-filecache/issues)
- [PyPi](https://pypi.org/project/rms-filecache)

# Licensing

This code is licensed under the [Apache License v2.0](https://github.com/SETI/rms-filecache/blob/main/LICENSE).
