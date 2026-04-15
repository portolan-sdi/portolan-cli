"""Basic import tests to verify iceberg backend package structure."""

import pytest


@pytest.mark.unit
def test_import_iceberg_backend():
    """Verify the iceberg backend can be imported and IcebergBackend is accessible."""
    from portolan_cli.backends.iceberg import IcebergBackend

    assert IcebergBackend is not None
    assert IcebergBackend.__name__ == "IcebergBackend"


@pytest.mark.unit
def test_import_iceberg_version():
    """Verify __version__ is a string."""
    from portolan_cli.backends.iceberg import __version__

    assert isinstance(__version__, str)


@pytest.mark.unit
def test_import_dependencies():
    """Verify core dependencies are importable."""
    import pyarrow
    import pyiceberg

    assert pyarrow is not None
    assert pyiceberg is not None
