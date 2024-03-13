# EMQX Bit swizzle

`emqx_bitswizzle` is a NIF that rearranges data in the Erlang binaries
on the bit level according to a schema specified in form of a sparse
matrix.

The name comes from https://en.wikipedia.org/wiki/Swizzling_(computer_graphics)

# Features

This library provides an Erlang interface, as well as plain C
interface that could be used from another NIF.

# Limitation

Size of the input and output binaries must be aligned to 8 bytes.
It's up to the API consumer to pad the inputs with zeros, and to slice
the result to cut away the excess.

This NIF is optimized for the transformations described by matrices
where the majority of non-zero elements are located on the diagonals
pointing in the southeast direction. Both memory and compute
complexity depend linearly on the number of such non-zero diagonals.

It should be reasonably efficient for other kinds of transformations
as well (such as mapping bits onto a z-order curve), but not as fast
as a hand-rolled implementation.

# Documentation links

See `doc/src/swizzle.tex` for the in-depth explanations.

# Usage

Currently it's used by the `emqx_ds_storage_bitfield_lts` module to
produce the MQTT message keys.

# Configurations

None

# HTTP APIs

None

# Other TBD

# Contributing

Please see our [contributing.md](../../CONTRIBUTING.md).

Unit tests for the C API use [Criterion](https://github.com/Snaipe/Criterion) library.
It must be present in the system in order to verify the changes with address sanitizer.
