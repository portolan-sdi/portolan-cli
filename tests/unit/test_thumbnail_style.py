"""Unit tests for thumbnail_style module.

Tests parsing of Mapbox GL styles for thumbnail rendering, including:
- Simple color extraction
- Match expression parsing (categorical styles)
- Case-insensitive field matching
- Edge cases (missing fields, no fill layer, etc.)
"""

from pathlib import Path

import pytest

from portolan_cli.thumbnail_style import (
    ThumbnailStyle,
    load_thumbnail_style,
    parse_match_expression,
    resolve_colors_for_gdf,
)

# =============================================================================
# parse_match_expression tests
# =============================================================================


@pytest.mark.unit
def test_parse_match_expression_simple():
    """Parse a simple match expression with one category."""
    expr = ["match", ["get", "land_use"], "residential", "#ff6b6b", "#95afc0"]
    field, color_map, default = parse_match_expression(expr)

    assert field == "land_use"
    assert color_map == {"residential": "#ff6b6b"}
    assert default == "#95afc0"


@pytest.mark.unit
def test_parse_match_expression_multiple_categories():
    """Parse a match expression with multiple categories."""
    expr = [
        "match",
        ["get", "land_use"],
        "residential",
        "#ff6b6b",
        "commercial",
        "#4ecdc4",
        "industrial",
        "#45b7d1",
        "#95afc0",
    ]
    field, color_map, default = parse_match_expression(expr)

    assert field == "land_use"
    assert color_map == {
        "residential": "#ff6b6b",
        "commercial": "#4ecdc4",
        "industrial": "#45b7d1",
    }
    assert default == "#95afc0"


@pytest.mark.unit
def test_parse_match_expression_numeric_values():
    """Parse a match expression with numeric category values."""
    expr = ["match", ["get", "zone_code"], 1, "#ff0000", 2, "#00ff00", "#0000ff"]
    field, color_map, default = parse_match_expression(expr)

    assert field == "zone_code"
    assert color_map == {1: "#ff0000", 2: "#00ff00"}
    assert default == "#0000ff"


@pytest.mark.unit
def test_parse_match_expression_invalid_format():
    """Return None for invalid match expression format."""
    # Not a list
    assert parse_match_expression("invalid") is None

    # Too short
    assert parse_match_expression(["match"]) is None

    # Not a match expression
    assert parse_match_expression(["interpolate", ["linear"], ["get", "x"]]) is None

    # Invalid get expression
    assert parse_match_expression(["match", "not_a_get", "val", "#000", "#fff"]) is None


# =============================================================================
# load_thumbnail_style tests
# =============================================================================


@pytest.mark.unit
def test_load_thumbnail_style_simple_color(tmp_path: Path):
    """Load a style with simple fill color."""
    style_file = tmp_path / "style.json"
    style_file.write_text(
        """{
        "version": 8,
        "layers": [{
            "type": "fill",
            "paint": {
                "fill-color": "#ff0000",
                "fill-opacity": 0.8
            }
        }]
    }"""
    )

    style = load_thumbnail_style(style_file)

    assert style is not None
    assert style.fill_color == "#ff0000"
    assert style.fill_opacity == 0.8
    assert style.color_field is None
    assert style.color_map is None


@pytest.mark.unit
def test_load_thumbnail_style_categorical(tmp_path: Path):
    """Load a style with categorical match expression."""
    style_file = tmp_path / "style.json"
    style_file.write_text(
        """{
        "version": 8,
        "layers": [{
            "type": "fill",
            "paint": {
                "fill-color": ["match", ["get", "land_use"],
                    "residential", "#ff6b6b",
                    "commercial", "#4ecdc4",
                    "#95afc0"
                ],
                "fill-opacity": 0.7
            }
        }]
    }"""
    )

    style = load_thumbnail_style(style_file)

    assert style is not None
    assert style.fill_color == "#95afc0"  # default from match
    assert style.fill_opacity == 0.7
    assert style.color_field == "land_use"
    assert style.color_map == {"residential": "#ff6b6b", "commercial": "#4ecdc4"}


@pytest.mark.unit
def test_load_thumbnail_style_with_outline(tmp_path: Path):
    """Load a style with fill-outline-color."""
    style_file = tmp_path / "style.json"
    style_file.write_text(
        """{
        "version": 8,
        "layers": [{
            "type": "fill",
            "paint": {
                "fill-color": "#3388ff",
                "fill-opacity": 0.6,
                "fill-outline-color": "#2266cc"
            }
        }]
    }"""
    )

    style = load_thumbnail_style(style_file)

    assert style is not None
    assert style.fill_color == "#3388ff"
    assert style.edge_color == "#2266cc"


@pytest.mark.unit
def test_load_thumbnail_style_no_fill_layer(tmp_path: Path):
    """Return None when style has no fill layer."""
    style_file = tmp_path / "style.json"
    style_file.write_text(
        """{
        "version": 8,
        "layers": [{
            "type": "line",
            "paint": {"line-color": "#ff0000"}
        }]
    }"""
    )

    style = load_thumbnail_style(style_file)
    assert style is None


@pytest.mark.unit
def test_load_thumbnail_style_missing_file():
    """Return None for missing style file."""
    style = load_thumbnail_style(Path("/nonexistent/style.json"))
    assert style is None


@pytest.mark.unit
def test_load_thumbnail_style_invalid_json(tmp_path: Path):
    """Return None for invalid JSON."""
    style_file = tmp_path / "style.json"
    style_file.write_text("not valid json")

    style = load_thumbnail_style(style_file)
    assert style is None


@pytest.mark.unit
def test_load_thumbnail_style_default_opacity(tmp_path: Path):
    """Use default opacity when not specified."""
    style_file = tmp_path / "style.json"
    style_file.write_text(
        """{
        "version": 8,
        "layers": [{"type": "fill", "paint": {"fill-color": "#ff0000"}}]
    }"""
    )

    style = load_thumbnail_style(style_file)

    assert style is not None
    assert style.fill_opacity == 1.0  # Mapbox GL default


@pytest.mark.unit
def test_load_thumbnail_style_uses_first_fill_layer(tmp_path: Path):
    """Use the first fill layer when multiple exist."""
    style_file = tmp_path / "style.json"
    style_file.write_text(
        """{
        "version": 8,
        "layers": [
            {"type": "line", "paint": {"line-color": "#000000"}},
            {"type": "fill", "paint": {"fill-color": "#ff0000"}},
            {"type": "fill", "paint": {"fill-color": "#00ff00"}}
        ]
    }"""
    )

    style = load_thumbnail_style(style_file)

    assert style is not None
    assert style.fill_color == "#ff0000"  # First fill layer


@pytest.mark.unit
def test_load_thumbnail_style_from_fixture():
    """Load from existing test fixture."""
    fixture_path = (
        Path(__file__).parent.parent
        / "fixtures"
        / "metadata"
        / "style"
        / "valid"
        / "style_categorical.json"
    )

    if not fixture_path.exists():
        pytest.skip("Fixture not found")

    style = load_thumbnail_style(fixture_path)

    assert style is not None
    assert style.color_field == "land_use"
    assert "residential" in style.color_map


# =============================================================================
# resolve_colors_for_gdf tests
# =============================================================================


@pytest.mark.unit
def test_resolve_colors_uniform():
    """Return uniform color when no color_map."""
    pytest.importorskip("geopandas")
    import geopandas as gpd
    from shapely.geometry import Point

    gdf = gpd.GeoDataFrame({"a": [1, 2, 3]}, geometry=[Point(0, 0)] * 3)
    style = ThumbnailStyle(
        fill_color="#ff0000",
        fill_opacity=0.5,
        edge_color=None,
        color_field=None,
        color_map=None,
    )

    colors = resolve_colors_for_gdf(gdf, style)

    assert colors == "#ff0000"


@pytest.mark.unit
def test_resolve_colors_categorical():
    """Resolve categorical colors from field values."""
    pytest.importorskip("geopandas")
    import geopandas as gpd
    from shapely.geometry import Point

    gdf = gpd.GeoDataFrame(
        {"land_use": ["residential", "commercial", "residential"]},
        geometry=[Point(0, 0)] * 3,
    )
    style = ThumbnailStyle(
        fill_color="#000000",
        fill_opacity=0.7,
        edge_color=None,
        color_field="land_use",
        color_map={"residential": "#ff6b6b", "commercial": "#4ecdc4"},
    )

    colors = resolve_colors_for_gdf(gdf, style)

    assert list(colors) == ["#ff6b6b", "#4ecdc4", "#ff6b6b"]


@pytest.mark.unit
def test_resolve_colors_case_insensitive():
    """Match field names case-insensitively."""
    pytest.importorskip("geopandas")
    import geopandas as gpd
    from shapely.geometry import Point

    # GDF has uppercase column, style expects lowercase
    gdf = gpd.GeoDataFrame(
        {"LAND_USE": ["residential", "commercial"]},
        geometry=[Point(0, 0)] * 2,
    )
    style = ThumbnailStyle(
        fill_color="#000000",
        fill_opacity=0.7,
        edge_color=None,
        color_field="land_use",  # lowercase
        color_map={"residential": "#ff6b6b", "commercial": "#4ecdc4"},
    )

    colors = resolve_colors_for_gdf(gdf, style)

    assert list(colors) == ["#ff6b6b", "#4ecdc4"]


@pytest.mark.unit
def test_resolve_colors_missing_field():
    """Use default color when field not in GDF."""
    pytest.importorskip("geopandas")
    import geopandas as gpd
    from shapely.geometry import Point

    gdf = gpd.GeoDataFrame(
        {"other_field": ["a", "b"]},
        geometry=[Point(0, 0)] * 2,
    )
    style = ThumbnailStyle(
        fill_color="#default",
        fill_opacity=0.7,
        edge_color=None,
        color_field="land_use",  # not in GDF
        color_map={"residential": "#ff6b6b"},
    )

    colors = resolve_colors_for_gdf(gdf, style)

    # Should fall back to default fill_color
    assert colors == "#default"


@pytest.mark.unit
def test_resolve_colors_unmapped_values():
    """Use default color for values not in color_map."""
    pytest.importorskip("geopandas")
    import geopandas as gpd
    from shapely.geometry import Point

    gdf = gpd.GeoDataFrame(
        {"land_use": ["residential", "unknown_type"]},
        geometry=[Point(0, 0)] * 2,
    )
    style = ThumbnailStyle(
        fill_color="#default",
        fill_opacity=0.7,
        edge_color=None,
        color_field="land_use",
        color_map={"residential": "#ff6b6b"},  # no mapping for unknown_type
    )

    colors = resolve_colors_for_gdf(gdf, style)

    assert list(colors) == ["#ff6b6b", "#default"]
