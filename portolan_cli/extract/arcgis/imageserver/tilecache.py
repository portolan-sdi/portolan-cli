"""ArcGIS ImageServer tile cache reader."""

from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

import httpx
import numpy as np

from portolan_cli.extract.arcgis.imageserver.tiling import TileSpec
from portolan_cli.extract.common.urls import redact_credentials

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Sequence
    from pathlib import Path

    from numpy.typing import NDArray
from http import HTTPStatus

_GRAY_NDIM = 2
_MULTIBAND_NDIM = 3
_LERC_NODATA_INDEX = 3

# Tile cache formats this module can decode. LERC2D is what a hosted tiled
# imagery layer stores. The rest are the image formats a map cache stores.
LERC_FORMATS = frozenset({"LERC", "LERC2D"})
IMAGE_FORMATS = frozenset({"PNG", "PNG8", "PNG24", "PNG32", "JPG", "JPEG", "JPEGPLUS", "MIXED"})

# Cache tiles to request at the same time inside one output tile.
DEFAULT_CACHE_CONCURRENCY = 8

# Capability that marks a service as cache-only.
TILES_ONLY_CAPABILITY = "tilesonly"

# The extent and the cache origin disagree by nanometres. Snap a bound this close to a
# whole pixel, or a row of 1-pixel tiles appears across the top of the grid.
PIXEL_SNAP_TOLERANCE = 1e-6


class TileCacheError(Exception):
    """Error while reading an ImageServer tile cache."""

    def __init__(self, message: str, status_code: int | None = None) -> None:
        """Build the error with the HTTP status of the request, if one exists."""
        super().__init__(message)
        self.status_code = status_code


class TileCacheRateLimitError(TileCacheError):
    """The cache answered HTTP 429, so the reader must wait."""

    def __init__(self, url: str, retry_after: float | None = None) -> None:
        """Build the error."""
        self.retry_after = retry_after
        super().__init__(f"Cache read rate limited: HTTP 429 for {url}.")


@dataclass(frozen=True)
class LevelOfDetail:
    """One zoom level of a tile cache."""

    level: int
    resolution: float


@dataclass(frozen=True)
class TileCacheInfo:
    """Grid of an ImageServer tile cache, read from ``tileInfo``."""

    tile_width: int
    tile_height: int
    tile_format: str
    origin_x: float
    origin_y: float
    lods: tuple[LevelOfDetail, ...]
    spatial_reference: dict[str, Any]

    @property
    def is_lerc(self) -> bool:
        """Report whether the cache stores LERC tiles."""
        return self.tile_format in LERC_FORMATS

    def crs_string(self) -> str:
        """Return the cache CRS."""
        wkid = self.spatial_reference.get("latestWkid") or self.spatial_reference.get("wkid")
        if wkid:
            return f"EPSG:{wkid}"
        wkt = self.spatial_reference.get("wkt") or self.spatial_reference.get("wkt2")
        if wkt:
            return str(wkt)
        raise TileCacheError("The tile cache declares no spatial reference.")

    def select_lod(self, pixel_size: float) -> LevelOfDetail:
        """Return the level whose resolution is closest to a pixel size."""
        if not self.lods:
            raise TileCacheError("The tile cache lists no levels of detail.")
        finest = max(self.lods, key=lambda lod: lod.level)
        if pixel_size <= 0:
            return finest
        usable = [lod for lod in self.lods if lod.resolution > 0]
        if not usable:
            return finest
        return min(
            usable,
            key=lambda lod: (abs(math.log(lod.resolution / pixel_size)), -lod.level),
        )


@dataclass(frozen=True)
class CacheTileRef:
    """One cache tile and where it lands inside an output tile."""

    level: int
    row: int
    col: int
    dst_row: int
    dst_col: int

    def url(self, service_url: str) -> str:
        """Build the cache URL for this tile."""
        return f"{service_url.rstrip('/')}/tile/{self.level}/{self.row}/{self.col}"


@dataclass(frozen=True)
class CacheFetchResult:
    """Outcome of reading one output tile from the cache."""

    bytes_downloaded: int
    empty: bool


def export_image_supported(capabilities: Iterable[str]) -> bool:
    """Report whether a service accepts exportImage requests."""
    return not any(str(item).strip().lower() == TILES_ONLY_CAPABILITY for item in capabilities)


def parse_tile_info(data: dict[str, Any]) -> TileCacheInfo | None:
    """Read the tile cache grid from an ImageServer JSON response."""
    tile_info = data.get("tileInfo")
    if not isinstance(tile_info, dict):
        return None

    origin = tile_info.get("origin")
    if not isinstance(origin, dict) or "x" not in origin or "y" not in origin:
        return None

    lods = _parse_lods(tile_info.get("lods"))
    if not lods:
        return None

    spatial_reference = tile_info.get("spatialReference") or data.get("spatialReference") or {}
    return TileCacheInfo(
        tile_width=int(tile_info.get("cols") or 256),
        tile_height=int(tile_info.get("rows") or 256),
        tile_format=str(tile_info.get("format") or "").upper(),
        origin_x=float(origin["x"]),
        origin_y=float(origin["y"]),
        lods=lods,
        spatial_reference=spatial_reference,
    )


def _parse_lods(raw: Any) -> tuple[LevelOfDetail, ...]:
    """Read the levels of detail from a tileInfo block."""
    if not isinstance(raw, list):
        return ()
    lods = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        resolution = item.get("resolution")
        if item.get("level") is None or not resolution:
            continue
        lods.append(LevelOfDetail(level=int(item["level"]), resolution=float(resolution)))
    return tuple(lods)


def _snap_to_pixel(value: float) -> float:
    """Round a pixel bound that sits within a tolerance of a whole pixel."""
    nearest = round(value)
    if abs(value - nearest) < PIXEL_SNAP_TOLERANCE:
        return float(nearest)
    return value


def compute_cache_tile_grid(
    extent: dict[str, float],
    cache: TileCacheInfo,
    lod: LevelOfDetail,
    tile_size: int = 4096,
) -> Iterator[TileSpec]:
    """Generate output tiles that align with the cache grid."""
    resolution = lod.resolution
    block_w = max(cache.tile_width, (tile_size // cache.tile_width) * cache.tile_width)
    block_h = max(cache.tile_height, (tile_size // cache.tile_height) * cache.tile_height)

    left = math.floor(_snap_to_pixel((extent["xmin"] - cache.origin_x) / resolution))
    right = math.ceil(_snap_to_pixel((extent["xmax"] - cache.origin_x) / resolution))
    top = math.floor(_snap_to_pixel((cache.origin_y - extent["ymax"]) / resolution))
    bottom = math.ceil(_snap_to_pixel((cache.origin_y - extent["ymin"]) / resolution))

    start_col = (left // block_w) * block_w
    start_row = (top // block_h) * block_h

    for y, block_row in enumerate(range(start_row, bottom, block_h)):
        row0 = max(block_row, top)
        row1 = min(block_row + block_h, bottom)
        for x, block_col in enumerate(range(start_col, right, block_w)):
            col0 = max(block_col, left)
            col1 = min(block_col + block_w, right)
            yield TileSpec(
                x=x,
                y=y,
                bbox=(
                    cache.origin_x + col0 * resolution,
                    cache.origin_y - row1 * resolution,
                    cache.origin_x + col1 * resolution,
                    cache.origin_y - row0 * resolution,
                ),
                width_px=col1 - col0,
                height_px=row1 - row0,
            )


def cache_tile_refs(
    tile: TileSpec,
    cache: TileCacheInfo,
    lod: LevelOfDetail,
) -> list[CacheTileRef]:
    """List the cache tiles that cover an output tile."""
    left = round((tile.bbox[0] - cache.origin_x) / lod.resolution)
    top = round((cache.origin_y - tile.bbox[3]) / lod.resolution)
    right = max(round((tile.bbox[2] - cache.origin_x) / lod.resolution), left + 1)
    bottom = max(round((cache.origin_y - tile.bbox[1]) / lod.resolution), top + 1)

    refs = []
    for row in range(top // cache.tile_height, (bottom - 1) // cache.tile_height + 1):
        if row < 0:
            continue
        for col in range(left // cache.tile_width, (right - 1) // cache.tile_width + 1):
            if col < 0:
                continue
            refs.append(
                CacheTileRef(
                    level=lod.level,
                    row=row,
                    col=col,
                    dst_row=row * cache.tile_height - top,
                    dst_col=col * cache.tile_width - left,
                )
            )
    return refs


def ensure_cache_readable(cache: TileCacheInfo) -> None:
    """Check the cache format and its decoder before the first request."""
    if cache.is_lerc:
        _load_lerc()
        return
    if cache.tile_format in IMAGE_FORMATS:
        return
    raise TileCacheError(
        f"Portolan cannot read the tile cache format '{cache.tile_format}'. "
        f"It reads {', '.join(sorted(LERC_FORMATS | IMAGE_FORMATS))}."
    )


def decode_tile_bytes(content: bytes, tile_format: str) -> tuple[NDArray[Any], NDArray[np.bool_]]:
    """Decode one cache tile."""
    normalized = tile_format.upper()
    if normalized in LERC_FORMATS:
        return _decode_lerc(content, normalized)
    if normalized in IMAGE_FORMATS:
        return _decode_image(content, normalized)
    raise TileCacheError(
        f"Portolan cannot read the tile cache format '{tile_format}'. "
        f"It reads {', '.join(sorted(LERC_FORMATS | IMAGE_FORMATS))}."
    )


def _load_lerc() -> Any:
    """Import the lerc package, with an explanation when it does not load."""
    reason = (
        "This service stores LERC tiles. GDAL cannot decode them, because its "
        "MRF driver reads an older LERC2 version, so Portolan needs the 'lerc' "
        "package."
    )
    try:
        import lerc
    except ImportError as exc:
        raise TileCacheError(f"{reason} Install it with 'pip install lerc'. {exc}") from exc
    except OSError as exc:
        raise TileCacheError(
            f"{reason} The package is installed, and its binary did not load on "
            f"this platform: {exc}. The lerc wheel carries binaries for Windows "
            f"x64, macOS, and Linux x86-64 only. Run the extraction on one of "
            f"those. A cache that stores PNG or JPEG needs no LERC decoder, and "
            f"reads on every platform."
        ) from exc
    return lerc


def _decode_lerc(content: bytes, tile_format: str) -> tuple[NDArray[Any], NDArray[np.bool_]]:
    """Decode a LERC tile with the lerc package."""
    lerc = _load_lerc()
    try:
        decoded = lerc.decode_4D(content)
    except Exception as exc:
        raise TileCacheError(f"Failed to decode a {tile_format} tile: {exc}") from exc

    if not isinstance(decoded, tuple) or decoded[0] != 0:
        code = decoded[0] if isinstance(decoded, tuple) else decoded
        raise TileCacheError(f"Failed to decode a {tile_format} tile: lerc error code {code}.")

    data = np.asarray(decoded[1])
    if data.ndim == _GRAY_NDIM:
        data = data[np.newaxis, :, :]
    if data.ndim != _MULTIBAND_NDIM:
        raise TileCacheError(
            f"A {tile_format} tile holds {data.ndim} dimensions. Portolan reads 2 or 3."
        )

    mask = _lerc_mask(decoded[2], data.shape[1:])
    mask = _apply_lerc_nodata(
        data, mask, decoded[_LERC_NODATA_INDEX] if len(decoded) > _LERC_NODATA_INDEX else None
    )
    return data, mask


def _lerc_mask(raw: Any, shape: tuple[int, ...]) -> NDArray[np.bool_]:
    """Normalize the lerc validity mask to one 2D array."""
    if raw is None:
        return np.ones(shape, dtype=np.bool_)
    mask: NDArray[np.bool_] = np.asarray(raw, dtype=np.bool_)
    if mask.ndim == _MULTIBAND_NDIM:
        mask = np.asarray(mask.any(axis=0), dtype=np.bool_)
    return mask


def _apply_lerc_nodata(
    data: NDArray[Any],
    mask: NDArray[np.bool_],
    no_data: Any,
) -> NDArray[np.bool_]:
    """Fold per-band noData values into the validity mask."""
    if no_data is None:
        return mask
    combined: NDArray[np.bool_] = mask.copy()
    for band in range(min(data.shape[0], int(np.size(no_data)))):
        if np.ma.is_masked(no_data[band]):
            continue
        combined &= np.asarray(data[band] != no_data[band], dtype=np.bool_)
    return combined


def _decode_image(content: bytes, tile_format: str) -> tuple[NDArray[Any], NDArray[np.bool_]]:
    """Decode a PNG or JPEG cache tile with rasterio."""
    from rasterio.io import MemoryFile

    try:
        with MemoryFile(content) as memfile, memfile.open() as src:
            data = src.read()
            mask = src.dataset_mask() > 0
    except Exception as exc:
        raise TileCacheError(f"Failed to decode a {tile_format} tile: {exc}") from exc
    valid: NDArray[np.bool_] = np.asarray(mask, dtype=np.bool_)
    return data, valid


def select_probe_lod(
    cache: TileCacheInfo,
    read_lod: LevelOfDetail,
    block_px: int,
) -> LevelOfDetail | None:
    """Return a coarse level where one cache tile covers a whole output block."""
    if block_px <= cache.tile_width:
        return None  # The block is one cache tile, so a probe saves no request.
    span = block_px * read_lod.resolution
    needed = span / cache.tile_width
    candidates = [
        lod for lod in cache.lods if lod.level < read_lod.level and lod.resolution >= needed
    ]
    if not candidates:
        return None
    return min(candidates, key=lambda lod: lod.resolution)


async def probe_block_empty(
    service_url: str,
    tile: TileSpec,
    client: httpx.AsyncClient,
    cache: TileCacheInfo,
    probe_lod: LevelOfDetail,
) -> bool:
    """Report whether a coarse level holds no data for one output block."""
    refs = cache_tile_refs(tile, cache, probe_lod)
    if not refs:
        return False

    try:
        bodies = await _gather_cache_tiles(refs, service_url, client)
    except TileCacheError:
        return False

    for body in bodies:
        if body is None:
            continue  # HTTP 404: the cache holds no tile there.
        try:
            _, mask = decode_tile_bytes(body, cache.tile_format)
        except TileCacheError:
            return False
        if mask.any():
            return False
    return True


async def fetch_cache_tile(
    service_url: str,
    tile: TileSpec,
    output_path: Path,
    client: httpx.AsyncClient,
    cache: TileCacheInfo,
    lod: LevelOfDetail,
    crs: str,
    *,
    max_concurrent: int = DEFAULT_CACHE_CONCURRENCY,
) -> CacheFetchResult:
    """Read one output tile from the cache and write it as a GeoTIFF."""
    refs = cache_tile_refs(tile, cache, lod)
    if not refs:
        return CacheFetchResult(bytes_downloaded=0, empty=True)

    bodies = await _gather_cache_tiles(refs, service_url, client, asyncio.Semaphore(max_concurrent))
    present = [(ref, body) for ref, body in zip(refs, bodies, strict=True) if body is not None]
    if not present:
        return CacheFetchResult(bytes_downloaded=0, empty=True)

    total_bytes = sum(len(body) for _, body in present)
    loop = asyncio.get_event_loop()
    written = await loop.run_in_executor(
        None, _write_mosaic, present, tile, cache, lod, crs, output_path
    )
    return CacheFetchResult(bytes_downloaded=total_bytes, empty=not written)


async def _gather_cache_tiles(
    refs: Sequence[CacheTileRef],
    service_url: str,
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore | None = None,
) -> list[bytes | None]:
    """Download several cache tiles, and wait for all of them."""

    async def _one(ref: CacheTileRef) -> bytes | None:
        if semaphore is None:
            return await _download_cache_tile(ref, service_url, client)
        async with semaphore:
            return await _download_cache_tile(ref, service_url, client)

    results = await asyncio.gather(*(_one(ref) for ref in refs), return_exceptions=True)
    for result in results:
        if isinstance(result, BaseException):
            raise result
    return cast("list[bytes | None]", results)


async def _download_cache_tile(
    ref: CacheTileRef,
    service_url: str,
    client: httpx.AsyncClient,
) -> bytes | None:
    """Download one cache tile."""
    url = ref.url(service_url)
    shown = redact_credentials(url)  # A message names the tile, not the credential.
    try:
        response = await client.get(url)
    except httpx.TimeoutException as exc:
        raise TileCacheError(f"Timeout while reading {shown}.") from exc
    except httpx.RequestError as exc:
        raise TileCacheError(redact_credentials(f"Failed to read {url}: {exc}")) from exc

    if response.status_code == HTTPStatus.NOT_FOUND:
        return None
    if response.status_code == HTTPStatus.TOO_MANY_REQUESTS:
        raise TileCacheRateLimitError(shown, _retry_after_seconds(response))
    if response.status_code != HTTPStatus.OK:
        raise TileCacheError(
            f"Cache read failed: HTTP {response.status_code} for {shown}.",
            status_code=response.status_code,
        )
    return response.content


def _retry_after_seconds(response: httpx.Response) -> float | None:
    """Read the Retry-After header as a number of seconds."""
    raw = response.headers.get("Retry-After")
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _masked_fill_value(dtype: np.dtype[Any]) -> float:
    """Return the value that marks a masked pixel for a pixel type."""
    return float("nan") if np.issubdtype(dtype, np.floating) else 0.0


def _write_mosaic(
    tiles: Sequence[tuple[CacheTileRef, bytes]],
    tile: TileSpec,
    cache: TileCacheInfo,
    lod: LevelOfDetail,
    crs: str,
    output_path: Path,
) -> bool:
    """Decode cache tiles into one array and write a GeoTIFF."""
    import rasterio
    from rasterio.transform import from_origin

    height, width = tile.height_px, tile.width_px
    mask = np.zeros((height, width), dtype=bool)
    data: NDArray[Any] | None = None
    fill: float = 0.0

    for ref, content in tiles:
        patch, patch_mask = decode_tile_bytes(content, cache.tile_format)
        if data is None:
            fill = _masked_fill_value(patch.dtype)
            data = np.full((patch.shape[0], height, width), fill, dtype=patch.dtype)
        elif patch.shape[0] != data.shape[0]:
            raise TileCacheError(
                f"The '{cache.tile_format}' cache mixes {data.shape[0]}-band and "
                f"{patch.shape[0]}-band tiles. Portolan cannot merge them."
            )
        _place_patch(data, mask, patch, patch_mask, ref, fill)

    if data is None or not mask.any():
        return False

    try:
        with rasterio.open(
            output_path,
            "w",
            driver="GTiff",
            width=width,
            height=height,
            count=data.shape[0],
            dtype=data.dtype,
            crs=crs,
            nodata=fill if np.isnan(fill) else None,
            transform=from_origin(tile.bbox[0], tile.bbox[3], lod.resolution, lod.resolution),
        ) as dst:
            dst.write(data)
            if not np.isnan(fill):
                # A float raster marks validity with nan nodata alone. GDAL ignores a mask beside
                # nodata.
                dst.write_mask(mask)
    except Exception as exc:
        raise TileCacheError(f"Failed to write {output_path}: {exc}") from exc
    return True


def _place_patch(
    data: NDArray[Any],
    mask: NDArray[np.bool_],
    patch: NDArray[Any],
    patch_mask: NDArray[np.bool_],
    ref: CacheTileRef,
    fill: float,
) -> None:
    """Copy one decoded cache tile into the output arrays."""
    src_row = max(0, -ref.dst_row)
    src_col = max(0, -ref.dst_col)
    dst_row = max(0, ref.dst_row)
    dst_col = max(0, ref.dst_col)
    rows = min(patch.shape[1] - src_row, data.shape[1] - dst_row)
    cols = min(patch.shape[2] - src_col, data.shape[2] - dst_col)
    if rows <= 0 or cols <= 0:
        return

    values = patch[:, src_row : src_row + rows, src_col : src_col + cols]
    valid = patch_mask[src_row : src_row + rows, src_col : src_col + cols]
    # The fill keeps the patch dtype. A bare Python float would promote an
    # integer patch to float64, and the write would cast it back.
    blank = np.asarray(fill, dtype=values.dtype)
    data[:, dst_row : dst_row + rows, dst_col : dst_col + cols] = np.where(valid, values, blank)
    mask[dst_row : dst_row + rows, dst_col : dst_col + cols] = valid
