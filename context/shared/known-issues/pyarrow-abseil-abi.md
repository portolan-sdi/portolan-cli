# Issue: PyArrow v22+ ABI incompatibility with abseil

## Symptom

Importing `geoparquet_io` (or any PyArrow-dependent library) fails with:

```
ImportError: .../pyarrow/libarrow_substrait.so.2200: undefined symbol: _ZN4absl12lts_2025012718container_internal24GetHashRefForEmptyHasherERKNS1_12CommonFieldsE
```

## Root cause

PyArrow 22.0.0 was compiled against a newer version of the abseil (absl) C++ library than what's available on some Linux systems. The symbol `GetHashRefForEmptyHasher` was added in a recent abseil release, causing a linker failure when the system has an older abseil.

This primarily affects:
- Ubuntu 22.04 and older
- Systems with system-installed abseil that conflicts with the bundled version
- Environments where DuckDB or gRPC also link against abseil

## Workaround

Pin PyArrow to versions before 22.0.0 in `pyproject.toml`:

```toml
"pyarrow>=12.0.0,<22.0.0",  # v22+ has ABI incompatibility with abseil on some systems
```

## References

- PyArrow 22.0.0 release: https://github.com/apache/arrow/releases/tag/apache-arrow-22.0.0
- Similar issues in DuckDB: https://github.com/duckdb/duckdb/issues
- abseil ABI stability discussion: https://github.com/abseil/abseil-cpp/issues

## Regression test

No automated test — this is an environment/binary compatibility issue. Tested manually by verifying `import geoparquet_io` succeeds after pinning.
