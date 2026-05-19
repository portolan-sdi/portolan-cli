"""Tests for CSW metadata client."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.unit

FIXTURES_DIR = Path(__file__).parent.parent.parent.parent / "fixtures" / "csw"


class TestFetchMetadataRecord:
    """Tests for fetch_metadata_record function."""

    @pytest.fixture
    def belgium_buildings_xml(self) -> str:
        """Load Belgium buildings ISO 19139 fixture."""
        fixture_path = FIXTURES_DIR / "belgium_buildings_iso19139.xml"
        return fixture_path.read_text(encoding="utf-8")

    def test_fetches_and_parses_csw_url(self, belgium_buildings_xml: str) -> None:
        """Fetches CSW URL and returns parsed ISOMetadata."""
        from portolan_cli.extract.csw.client import fetch_metadata_record

        mock_response = MagicMock()
        mock_response.text = belgium_buildings_xml
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()

        with patch("requests.get", return_value=mock_response) as mock_get:
            metadata = fetch_metadata_record("https://example.com/csw?request=GetRecordById&id=abc")

            mock_get.assert_called_once()
            assert metadata is not None
            assert metadata.file_identifier == "9a8322bd-f53a-4f99-ad9e-753b45bdee85"
            assert metadata.title == "INSPIRE - Bâtiments en Wallonie (BE)"

    def test_returns_none_on_http_error(self) -> None:
        """Returns None when HTTP request fails."""
        import requests

        from portolan_cli.extract.csw.client import fetch_metadata_record

        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found")

        with patch("requests.get", return_value=mock_response):
            metadata = fetch_metadata_record("https://example.com/csw?id=missing")

            assert metadata is None

    def test_returns_none_on_timeout(self) -> None:
        """Returns None when request times out."""
        import requests

        from portolan_cli.extract.csw.client import fetch_metadata_record

        with patch("requests.get", side_effect=requests.exceptions.Timeout):
            metadata = fetch_metadata_record("https://example.com/csw?id=slow")

            assert metadata is None

    def test_returns_none_on_connection_error(self) -> None:
        """Returns None when connection fails."""
        import requests

        from portolan_cli.extract.csw.client import fetch_metadata_record

        with patch("requests.get", side_effect=requests.exceptions.ConnectionError):
            metadata = fetch_metadata_record("https://example.com/csw?id=offline")

            assert metadata is None

    def test_returns_none_on_parse_error(self) -> None:
        """Returns None when XML parsing fails."""
        from portolan_cli.extract.csw.client import fetch_metadata_record

        mock_response = MagicMock()
        mock_response.text = "<html><body>Not XML metadata</body></html>"
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()

        with patch("requests.get", return_value=mock_response):
            metadata = fetch_metadata_record("https://example.com/not-csw")

            assert metadata is None

    def test_uses_custom_timeout(self, belgium_buildings_xml: str) -> None:
        """Respects custom timeout parameter."""
        from portolan_cli.extract.csw.client import fetch_metadata_record

        mock_response = MagicMock()
        mock_response.text = belgium_buildings_xml
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()

        with patch("requests.get", return_value=mock_response) as mock_get:
            fetch_metadata_record("https://example.com/csw", timeout=120.0)

            mock_get.assert_called_once()
            call_kwargs = mock_get.call_args.kwargs
            assert call_kwargs.get("timeout") == 120.0

    def test_default_timeout(self, belgium_buildings_xml: str) -> None:
        """Uses default timeout of 30 seconds."""
        from portolan_cli.extract.csw.client import fetch_metadata_record

        mock_response = MagicMock()
        mock_response.text = belgium_buildings_xml
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()

        with patch("requests.get", return_value=mock_response) as mock_get:
            fetch_metadata_record("https://example.com/csw")

            call_kwargs = mock_get.call_args.kwargs
            assert call_kwargs.get("timeout") == 30.0


class TestDetectMetadataUrlType:
    """Tests for detect_metadata_url_type function."""

    def test_detects_csw_getrecordbyid(self) -> None:
        """Detects CSW GetRecordById URLs."""
        from portolan_cli.extract.csw.client import detect_metadata_url_type

        url = "https://metawal.wallonie.be/geonetwork/inspire/fre/csw?REQUEST=GetRecordById&SERVICE=CSW&id=abc"
        assert detect_metadata_url_type(url) == "csw"

    def test_detects_csw_lowercase(self) -> None:
        """Detects CSW URLs with lowercase parameters."""
        from portolan_cli.extract.csw.client import detect_metadata_url_type

        url = "https://example.com/csw?request=getrecordbyid&service=csw&id=abc"
        assert detect_metadata_url_type(url) == "csw"

    def test_detects_static_xml(self) -> None:
        """Detects static XML file URLs."""
        from portolan_cli.extract.csw.client import detect_metadata_url_type

        assert detect_metadata_url_type("https://example.com/metadata.xml") == "xml"
        assert detect_metadata_url_type("https://example.com/path/record.XML") == "xml"

    def test_detects_html(self) -> None:
        """Detects HTML page URLs."""
        from portolan_cli.extract.csw.client import detect_metadata_url_type

        assert detect_metadata_url_type("https://example.com/metadata.html") == "html"
        assert detect_metadata_url_type("https://example.com/record.htm") == "html"

    def test_detects_geonetwork_api(self) -> None:
        """Detects GeoNetwork API URLs."""
        from portolan_cli.extract.csw.client import detect_metadata_url_type

        url = "https://example.com/geonetwork/srv/api/records/abc-123"
        assert detect_metadata_url_type(url) == "geonetwork_api"

    def test_returns_unknown_for_unrecognized(self) -> None:
        """Returns 'unknown' for unrecognized URL patterns."""
        from portolan_cli.extract.csw.client import detect_metadata_url_type

        assert detect_metadata_url_type("https://example.com/some/path") == "unknown"
        assert detect_metadata_url_type("https://example.com/data.json") == "unknown"


class TestIsMetadataUrlSupported:
    """Tests for is_metadata_url_supported function."""

    def test_csw_is_supported(self) -> None:
        """CSW URLs are supported."""
        from portolan_cli.extract.csw.client import is_metadata_url_supported

        url = "https://example.com/csw?request=GetRecordById&id=abc"
        assert is_metadata_url_supported(url) is True

    def test_static_xml_is_supported(self) -> None:
        """Static XML URLs are supported."""
        from portolan_cli.extract.csw.client import is_metadata_url_supported

        assert is_metadata_url_supported("https://example.com/metadata.xml") is True

    def test_html_is_not_supported(self) -> None:
        """HTML URLs are not supported (can't parse)."""
        from portolan_cli.extract.csw.client import is_metadata_url_supported

        assert is_metadata_url_supported("https://example.com/metadata.html") is False

    def test_unknown_is_not_supported(self) -> None:
        """Unknown URL types are not supported."""
        from portolan_cli.extract.csw.client import is_metadata_url_supported

        assert is_metadata_url_supported("https://example.com/random") is False


class TestFetchMetadataForLayer:
    """Tests for fetch_metadata_for_layer convenience function."""

    @pytest.fixture
    def belgium_buildings_xml(self) -> str:
        """Load Belgium buildings ISO 19139 fixture."""
        fixture_path = FIXTURES_DIR / "belgium_buildings_iso19139.xml"
        return fixture_path.read_text(encoding="utf-8")

    def test_fetches_first_supported_url(self, belgium_buildings_xml: str) -> None:
        """Fetches from first supported URL in list."""
        from portolan_cli.extract.csw.client import fetch_metadata_for_layer

        mock_response = MagicMock()
        mock_response.text = belgium_buildings_xml
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()

        metadata_urls = [
            {"url": "https://example.com/page.html"},  # Not supported
            {"url": "https://example.com/csw?request=GetRecordById&id=abc"},  # Supported
        ]

        with patch("requests.get", return_value=mock_response):
            metadata = fetch_metadata_for_layer(metadata_urls)

            assert metadata is not None
            assert metadata.file_identifier == "9a8322bd-f53a-4f99-ad9e-753b45bdee85"

    def test_returns_none_for_empty_list(self) -> None:
        """Returns None when no metadata URLs provided."""
        from portolan_cli.extract.csw.client import fetch_metadata_for_layer

        assert fetch_metadata_for_layer([]) is None
        assert fetch_metadata_for_layer(None) is None

    def test_returns_none_when_all_unsupported(self) -> None:
        """Returns None when all URLs are unsupported types."""
        from portolan_cli.extract.csw.client import fetch_metadata_for_layer

        metadata_urls = [
            {"url": "https://example.com/page.html"},
            {"url": "https://example.com/other.htm"},
        ]

        assert fetch_metadata_for_layer(metadata_urls) is None

    def test_tries_next_url_on_failure(self, belgium_buildings_xml: str) -> None:
        """Tries next URL when first one fails."""
        import requests

        from portolan_cli.extract.csw.client import fetch_metadata_for_layer

        call_count = 0

        def mock_get(url: str, **kwargs: object) -> MagicMock:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise requests.exceptions.ConnectionError("First URL failed")
            mock_response = MagicMock()
            mock_response.text = belgium_buildings_xml
            mock_response.status_code = 200
            mock_response.raise_for_status = MagicMock()
            return mock_response

        metadata_urls = [
            {"url": "https://example.com/csw?request=GetRecordById&id=first"},
            {"url": "https://example.com/csw?request=GetRecordById&id=second"},
        ]

        with patch("requests.get", side_effect=mock_get):
            metadata = fetch_metadata_for_layer(metadata_urls)

            assert metadata is not None
            assert call_count == 2
