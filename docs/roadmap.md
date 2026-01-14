# Cerulio Roadmap

## Shipping Now (Q1 2026)

**Cerulio Spec**
Best practices for sharing cloud-native geospatial data, e.g., formats, thumbnails, default styling, metadata, and what makes a dataset actually useful when someone finds it.

**Cerulio CLI**
Convert data to cloud-native formats (GeoParquet, COG) and upload to any cloud bucket. Includes a validator that checks spec compliance and recommends improvements.

**QGIS Plugin**
Browse Cerulio nodes, pull data into local GIS, edit STAC metadata, check best practices.

## How We're Building

- Spec, CLI, and validator developed together in a tight feedback loop
- Development coupled with [geoparquet-io](https://github.com/geoparquet/geoparquet-io)
- Prioritizing building out concrete examples and real use cases along with tooling
- Documentation-first; concise over comprehensive

## On the Horizon

- **Cerulio Uploader** — API + drag-and-drop web interface for converting diverse files to cloud-native formats
- **Browser/Map UI** — Web interfaces for exploring Cerulio nodes
- **Cerulio Node** — A complete, valid CN-SDI with all tooling to make it useful
- **Global Data Bootstrapper** — Subset global datasets (buildings, roads, land cover) to bootstrap local maps

---

*Cerulio is an open source project under [Radiant Earth](https://radiant.earth).*