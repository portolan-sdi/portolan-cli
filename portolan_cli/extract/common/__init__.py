"""Code that the layer-based extractors (ArcGIS, Carto, WFS) share.

``source`` holds the ``LayerSource`` protocol and the driver,
``run_layer_extraction``. The other modules are the steps that the driver
runs: ``filters``, ``retry``, ``report``, ``orchestrator_base`` (the catalog
build), ``metadata_seeding``, ``styles``, and ``urls`` (query merging and the
published-string sanitizer).
"""
