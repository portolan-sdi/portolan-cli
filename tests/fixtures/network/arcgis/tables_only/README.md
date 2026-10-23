# FeatureServer with only tables (#812)

A FeatureServer that advertises one table under `tables` and no layer under
`layers`. The extraction must exit non-zero and name the tables (#1034).

- Source: the shape of
  `https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/zoning_descriptions/FeatureServer`
  in #812. That service answers `400 Invalid URL` on 2026-10-23, so the
  service document is authored.
