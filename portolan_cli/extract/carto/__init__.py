"""Carto extract functionality.

Handles extraction of vector tables from Carto SQL API accounts into Portolan
catalogs. geoparquet-io performs the per-table extraction; this package adds
table discovery (CDB_UserTables), filtering, reporting, and catalog auto-init.
"""
