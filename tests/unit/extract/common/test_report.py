import pytest

from portolan_cli.extract.common.report import FolderCoverage


@pytest.mark.unit
def test_folder_coverage_roundtrip() -> None:
    cov = FolderCoverage(
        folders_visited=["A", "B"],
        folders_skipped=[("Locked", "499 Token Required")],
        services_found=5,
    )
    d = cov.to_dict()
    assert d["folders_visited"] == ["A", "B"]
    assert d["folders_skipped"] == [{"folder": "Locked", "reason": "499 Token Required"}]
    assert d["services_found"] == 5
    back = FolderCoverage.from_dict(d)
    assert back == cov
