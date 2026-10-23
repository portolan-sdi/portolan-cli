"""Tests for the query-merge helper and the published-string sanitizer (#981, #939)."""

from __future__ import annotations

import pytest

from portolan_cli.extract.common.urls import (
    WFS_REQUEST_PARAMS,
    merge_query,
    sanitize_published_string,
)

pytestmark = pytest.mark.unit


class TestMergeQuery:
    def test_override_replaces_a_parameter_of_any_case(self) -> None:
        """#981: SERVICE and service are one parameter, so the URL holds one."""
        url = "https://h.example/geoserver/wfs?SERVICE=WFS&request=GetCapabilities&apikey=SECRET"

        merged = merge_query(url, {"service": "WFS", "request": "GetFeature", "typename": "ws:lyr"})

        assert merged == (
            "https://h.example/geoserver/wfs?apikey=SECRET&service=WFS"
            "&request=GetFeature&typename=ws%3Alyr"
        )

    def test_drop_removes_wfs_request_parameters_of_any_case(self) -> None:
        """#981 rule 3: a WMS URL carries no WFS request parameter."""
        url = "https://h.example/geoserver/wfs?apikey=k%2B1&typeNames=x&OUTPUTFORMAT=json"

        merged = merge_query(url, {"service": "WMS"}, drop=WFS_REQUEST_PARAMS)

        assert merged == "https://h.example/geoserver/wfs?apikey=k%2B1&service=WMS"

    def test_path_is_kept(self) -> None:
        assert merge_query("https://h.example/a/b", {"f": "json"}) == "https://h.example/a/b?f=json"


class TestSanitizePublishedString:
    @pytest.mark.parametrize(
        "name",
        [
            "token",
            "api_key",
            "apikey",
            "KEY",
            "access_token",
            "signature",
            "sig",
            "authkey",
            "X-Goog-Signature",
            "subscription-key",
        ],
    )
    def test_credential_parameter_is_removed(self, name: str) -> None:
        url = f"https://h.example/wfs?service=WFS&{name}=SECRET&typename=a"

        assert sanitize_published_string(url) == "https://h.example/wfs?service=WFS&typename=a"

    def test_userinfo_is_removed(self) -> None:
        assert (
            sanitize_published_string("https://user:pw@h.example:8080/wfs?service=WFS")
            == "https://h.example:8080/wfs?service=WFS"
        )

    def test_url_without_credentials_is_unchanged(self) -> None:
        url = "https://h.example/arcgis/rest/services/X/FeatureServer/0"

        assert sanitize_published_string(url) == url

    @pytest.mark.parametrize(
        "value",
        [
            "file:///tmp/catalog/data.geojson",
            "sqlite:////tmp/tmp.4EuYh4nkoq/c/.portolan/iceberg.db",
            "/home/user/catalog/.portolan/iceberg.db",
            "C:\\Users\\me\\catalog",
            "C:/Users/me/catalog",
        ],
    )
    def test_local_location_is_refused(self, value: str) -> None:
        """#939: a local path names the machine that ran the command."""
        assert sanitize_published_string(value) is None

    def test_plain_text_is_unchanged(self) -> None:
        assert sanitize_published_string("City of Philadelphia") == "City of Philadelphia"

    def test_url_inside_text_loses_its_credential(self) -> None:
        """#981: an error message that quotes a request URL drops the credential of the URL."""
        text = "Error page from https://u:pw@h.example/wfs?authkey=SECRET&n=1: blocked. See /tmp/x"

        assert sanitize_published_string(text) == (
            "Error page from https://h.example/wfs?n=1: blocked. See /tmp/x"
        )

    def test_url_inside_text_without_credential_is_unchanged(self) -> None:
        text = "Layer roads at https://h.example/MapServer/0?f=json&a=b%2Fc: HTTP 500"

        assert sanitize_published_string(text) == text
