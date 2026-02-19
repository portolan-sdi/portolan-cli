# ADR-0019: COG Optimization Defaults

## Status
Accepted

## Context

Cloud-Optimized GeoTIFFs have many tunable parameters (compression, tile size, predictor, resampling). Different settings suit different data types (imagery vs elevation vs categorical).

## Decision

**Single opinionated default** for all COG conversions:

| Setting | Value | Rationale |
|---------|-------|-----------|
| Compression | DEFLATE | Lossless, universal compatibility |
| Predictor | 2 (horizontal differencing) | Improves compression for all types |
| Tile size | 512×512 | Matches rio-cogeo default; fewer HTTP requests |
| Overview resampling | nearest | Safe for categorical, elevation, and imagery |

**Power users** who need fine-tuned control (WEBP for imagery, LERC for elevation) should use `rio_cogeo.cog_translate()` directly. Portolan is for batch workflows, not per-file optimization.

## Consequences

### Benefits
- Zero configuration for typical users
- Consistent output across all conversions
- Works acceptably for all data types

### Trade-offs
- Not optimal for any specific use case
- Larger file sizes than WEBP/JPEG for RGB imagery
- Slower compression than LZW

## References

- [Cloud Native Geo Guide](https://guide.cloudnativegeo.org/cloud-optimized-geotiffs/cogs-details.html)
- [Koko Alberti's compression guide](https://kokoalberti.com/articles/geotiff-compression-optimization-guide/)
