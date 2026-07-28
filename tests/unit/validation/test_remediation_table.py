"""Completeness gate for the remediation table.

The table maps a rashid rule id onto how `portolan check` remediates it: `--fix`
does it (AUTO), the agent has to act on a requirement sentence (INSTRUCT), or it
is a hosting-server setting outside the catalog (EXTERNAL). Two properties
matter and neither can be spot-checked: every id rashid can emit must resolve to
*some* remediation without raising, and every AUTO entry must name the fixer key
Phase 3 will register.
"""

from __future__ import annotations

from types import ModuleType

import pytest
from rashid import data, live, runner, schema, structural
from rashid.rules import DEFAULT_RULES

from portolan_cli.validation.remediation import (
    DEFAULT_REMEDIATION,
    RULE_REMEDIATION,
    Bucket,
    Remediation,
    remediation_for,
)

pytestmark = pytest.mark.unit


def _declared_ids(module: ModuleType) -> set[str]:
    """Every ``PTL-*`` id a rashid pass module declares as a constant."""
    return {
        value
        for name, value in vars(module).items()
        if name.isupper() and isinstance(value, str) and value.startswith("PTL-")
    }


ALL_RULE_IDS = {rule.id for rule in DEFAULT_RULES}.union(
    *(_declared_ids(m) for m in (data, live, runner, schema, structural))
)


class TestCompleteness:
    def test_every_rashid_rule_id_resolves(self) -> None:
        """No id rashid can emit may crash the lookup."""
        for rule_id in ALL_RULE_IDS:
            assert isinstance(remediation_for(rule_id), Remediation)

    def test_every_rashid_rule_id_is_mapped(self) -> None:
        """The table covers rashid's whole surface; nothing silently defaults."""
        assert ALL_RULE_IDS - set(RULE_REMEDIATION) == set()

    def test_table_has_no_ids_rashid_cannot_emit(self) -> None:
        """A rule renamed upstream must not leave a dead row behind."""
        assert set(RULE_REMEDIATION) - ALL_RULE_IDS == set()

    def test_unmapped_id_falls_back_without_crashing(self) -> None:
        """A rule id added upstream before we map it degrades, it does not raise."""
        assert remediation_for("PTL-XXX-999") is DEFAULT_REMEDIATION
        assert DEFAULT_REMEDIATION.bucket is Bucket.INSTRUCT
        assert DEFAULT_REMEDIATION.fixer is None


class TestBucketInvariants:
    def test_auto_entries_name_a_fixer(self) -> None:
        auto_without_fixer = [
            rule_id
            for rule_id, rem in RULE_REMEDIATION.items()
            if rem.bucket is Bucket.AUTO and not rem.fixer
        ]
        assert auto_without_fixer == []

    def test_non_auto_entries_name_no_fixer(self) -> None:
        """A fixer key on an INSTRUCT/EXTERNAL row would never be dispatched."""
        stray = [
            rule_id
            for rule_id, rem in RULE_REMEDIATION.items()
            if rem.bucket is not Bucket.AUTO and rem.fixer is not None
        ]
        assert stray == []

    def test_every_entry_states_a_requirement(self) -> None:
        blank = [
            rule_id for rule_id, rem in RULE_REMEDIATION.items() if not rem.requirement.strip()
        ]
        assert blank == []

    def test_live_defects_are_external(self) -> None:
        """Range/CORS defects are server configuration, never a catalog edit.

        ``PTL-LIV-000`` is excluded: it is the pass's own degradation warning
        (the prober could not reach the host), not a defect in the hosting.
        """
        live_buckets = {
            RULE_REMEDIATION[rule_id].bucket
            for rule_id in RULE_REMEDIATION
            if rule_id.startswith("PTL-LIV-") and rule_id != "PTL-LIV-000"
        }
        assert live_buckets == {Bucket.EXTERNAL}

    def test_known_auto_and_instruct_assignments(self) -> None:
        """Spot-check the boundary the bucket table draws inside a family."""
        # Scaffolding and links are mechanical; prose quality is judgment.
        assert RULE_REMEDIATION["PTL-FIL-003"].bucket is Bucket.AUTO
        assert RULE_REMEDIATION["PTL-FIL-005"].bucket is Bucket.INSTRUCT
        # A missing title can be humanized; whether a title reads well cannot.
        assert RULE_REMEDIATION["PTL-TTL-001"].bucket is Bucket.AUTO
        assert RULE_REMEDIATION["PTL-TTL-002"].bucket is Bucket.INSTRUCT
        # Re-converting an asset is mechanical; where it is hosted is not.
        assert RULE_REMEDIATION["PTL-DAT-004"].fixer == "convert"
        assert RULE_REMEDIATION["PTL-AST-002"].bucket is Bucket.INSTRUCT


class TestBucketSerialization:
    def test_bucket_values_are_stable_strings(self) -> None:
        """The JSON payload emits `bucket.value`; agents key off these."""
        assert [b.value for b in Bucket] == ["auto", "instruct", "external"]
