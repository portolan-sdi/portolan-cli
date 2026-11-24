"""The lane switch of the e2e tests: the compose stack, or a hosted catalog from the environment."""

from __future__ import annotations

import pytest

from portolan_cli.backends.iceberg.auth import NoAuth, OAuth2ClientCredentials
from tests import iceberg_stack
from tests.iceberg_stack import RestAdmin, new_namespace, select_lane, stack_environment

pytestmark = pytest.mark.unit

HOSTED = {
    "PORTOLAN_TEST_ICEBERG_LANE": "hosted",
    "PORTOLAN_ICEBERG_URI": "https://catalog.example.org",
    "PORTOLAN_ICEBERG_WAREHOUSE": "my-warehouse",
    "PORTOLAN_ICEBERG_CLIENT_ID": "client",
    "PORTOLAN_ICEBERG_CLIENT_SECRET": "a-secret",
    "PORTOLAN_S3_ENDPOINT": "s3.example.org",
    "AWS_ACCESS_KEY_ID": "key",
    "AWS_SECRET_ACCESS_KEY": "a-secret",
    "AWS_REGION": "eu-west-1",
}


def test_no_lane_variable_selects_the_compose_stack() -> None:
    lane = select_lane({})

    assert lane.name == "docker"
    assert isinstance(lane.token_provider(), NoAuth)
    assert stack_environment(lane, "t1")["PORTOLAN_ICEBERG_AUTH"] == "none"


def test_the_hosted_lane_reads_the_backend_variables() -> None:
    lane = select_lane(HOSTED)

    env = stack_environment(lane, "portolan_ci_1_ab")
    assert (lane.name, lane.uri, lane.bucket, lane.s3_use_ssl) == (
        "hosted",
        "https://catalog.example.org",
        "my-warehouse",
        True,
    )
    assert isinstance(lane.token_provider(), OAuth2ClientCredentials)
    assert env["PORTOLAN_ICEBERG_AUTH"] == "oauth2"
    assert env["PORTOLAN_ICEBERG_CLIENT_ID"] == "client"
    assert env["PORTOLAN_ICEBERG_WAREHOUSE"] == "my-warehouse"
    assert env["AWS_REGION"] == "eu-west-1"
    assert "a-secret" not in repr(lane)


def test_the_bucket_variable_names_a_bucket_other_than_the_warehouse() -> None:
    lane = select_lane({**HOSTED, "PORTOLAN_TEST_ICEBERG_BUCKET": "other-bucket"})

    assert lane.bucket == "other-bucket"


def test_the_hosted_lane_skips_without_its_variables() -> None:
    with pytest.raises(pytest.skip.Exception) as skipped:
        select_lane({"PORTOLAN_TEST_ICEBERG_LANE": "hosted", "PORTOLAN_ICEBERG_URI": "https://x"})

    assert str(skipped.value) == (
        "the hosted lane needs PORTOLAN_ICEBERG_WAREHOUSE, PORTOLAN_S3_ENDPOINT, "
        "AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY"
    )


def test_an_unknown_lane_is_a_usage_error() -> None:
    with pytest.raises(pytest.UsageError, match="must be docker or hosted, not 'cloud'"):
        select_lane({"PORTOLAN_TEST_ICEBERG_LANE": "cloud"})


def test_a_hosted_namespace_carries_its_creation_time(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("tests.iceberg_stack.time.time", lambda: 1700000000.5)

    hosted = new_namespace(select_lane(HOSTED))
    local = new_namespace(select_lane({}))

    assert hosted.startswith("portolan_ci_1700000000_")
    assert len(hosted) == len("portolan_ci_1700000000_") + 10
    assert local.startswith("t")
    assert len(local) == 11


def test_the_sweep_drops_only_old_test_namespaces(monkeypatch: pytest.MonkeyPatch) -> None:
    admin = RestAdmin(select_lane(HOSTED))
    now = 1_700_000_000
    old = f"portolan_ci_{now - iceberg_stack.SWEEP_AFTER_SECONDS - 1}_0a1b2c3d4e"
    recent = f"portolan_ci_{now - 60}_0a1b2c3d4e"
    dropped: list[str] = []
    monkeypatch.setattr(
        admin, "list_namespaces", lambda: [old, recent, "portolake", "portolan_ci_x"]
    )
    monkeypatch.setattr(admin, "drop_namespace", dropped.append)

    assert admin.sweep(now=now) == [old]
    assert dropped == [old]
