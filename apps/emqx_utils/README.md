# Erlang utility library for EMQX

## Overview

`emqx_utils` is a collection of utility functions for EMQX, organized into
several modules. It provides various functionalities to make it easier to work
with EMQX, such as binary manipulations, maps, JSON en- and decoding, ets table
handling, data conversions, and more.

## Features

- `emqx_utils`: unsorted helper functions, formerly known as `emqx_misc` - NEEDS WORK
- `emqx_utils_api`: collection of helper functions for API responses
- `emqx_utils_binary`: binary reverse, join, trim etc
- `emqx_utils_ets`: convenience functions for creating and looking up data in ets tables.
- `emqx_utils_json`: JSON encoding and decoding
- `emqx_utils_maps`: convenience functions for map lookup and manipulation like
  deep_get etc.
- `emqx_metrics`: counters, gauges, slides

## Contributing

Please see our [contributing guidelines](../../CONTRIBUTING.md) for information
on how to contribute to `emqx_utils`. We welcome bug reports, feature requests,
and pull requests.
