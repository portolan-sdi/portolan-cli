"""Visualization assets: thumbnails, Mapbox GL styles, and PMTiles helpers.

Groups the modules that produce or classify visual/tiling artifacts for a
catalog (see ADR-0043/0045 styles, ADR-0050 PMTiles). This package is a leaf:
it must not import the ``scan`` or ``sync`` layers (enforced by import-linter).
"""
