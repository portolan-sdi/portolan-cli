# portolan Roadmap

## Shipping Now (Q1 2026)

**portolan Spec**
Best practices for sharing cloud-native geospatial data, e.g., formats, thumbnails, default styling, metadata, and what makes a dataset actually useful when someone finds it.

**portolan CLI**
Convert data to cloud-native formats (GeoParquet, COG) and upload to any cloud bucket. Includes a validator that checks spec compliance and recommends improvements.

**QGIS Plugin**
Browse portolan nodes, pull data into local GIS, edit STAC metadata, check best practices.

## How We're Building

- Spec, CLI, and validator developed together in a tight feedback loop
- Development coupled with [geoparquet-io](https://github.com/geoparquet/geoparquet-io)
- Prioritizing building out concrete examples and real use cases along with tooling
- Documentation-first; concise over comprehensive

## On the Horizon

- **portolan Uploader** — API + drag-and-drop web interface for converting diverse files to cloud-native formats
- **Browser/Map UI** — Web interfaces for exploring portolan nodes
- **portolan Node** — A complete, valid CN-SDI with all tooling to make it useful
- **Global Data Bootstrapper** — Subset global datasets (buildings, roads, land cover) to bootstrap local maps

---

*portolan is an open source project under [Radiant Earth](https://radiant.earth).*