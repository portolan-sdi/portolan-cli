"""The ``example`` extractor: each GeoJSON file in a directory is a layer.

It implements ``LayerSource``. ``portolan_cli.api.extractor_from`` gives it
filters, resume, retry, the report, and catalog setup.
"""

from __future__ import annotations

from pathlib import Path

from portolan_cli.api import (
    Discovery,
    ExtractedMetadata,
    LayerMetadata,
    LayerRef,
    LayerSourceOptions,
    SourceLink,
)


class GeoJSONDirectory:
    """Layers from the ``*.geojson`` files in the directory ``source``."""

    name = "example"
    summary = "Layers from a directory of GeoJSON files."

    def discover(self, source: str, options: LayerSourceOptions) -> Discovery:
        """One layer per file, in name order. The directory publishes no license."""
        files = sorted(Path(source).glob("*.geojson"))
        return Discovery(
            source_url=source,
            layers=[
                LayerRef(id=index, name=path.stem, data=path.resolve())
                for index, path in enumerate(files)
            ],
            catalog_seed=ExtractedMetadata(
                source_type=self.name,
                source_url=Path(source).resolve().as_uri(),
                description=f"GeoJSON files from the directory {Path(source).name}.",
            ),
        )

    def fetch(self, source: str, layer: LayerRef, dest: Path) -> int:
        """Convert the layer's file to GeoParquet with geoparquet-io."""
        import geoparquet_io as gpio  # type: ignore[import-untyped]

        table = gpio.convert(str(layer.data))
        table.write(str(dest))
        return int(table.num_rows)

    def layer_metadata(self, source: str, layer: LayerRef) -> LayerMetadata:
        """Title each collection after its file."""
        return LayerMetadata(title=layer.name.replace("_", " ").capitalize())

    def styles(self, source: str, layer: LayerRef, collection_dir: Path) -> None:
        """The example writes no styles."""

    def source_link(self, source: str, layer: LayerRef) -> SourceLink:
        """Link each collection to its file."""
        return SourceLink(
            href=Path(layer.data).as_uri(), title=f"Source file: {layer.name}.geojson"
        )
