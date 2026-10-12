# Issue: a failed push leaves new bytes in an overwritten key

## Symptom

A push fails after the asset upload. The `versions.json` write is one such
failure. The remote `versions.json` still names the old current version.
An asset key that the failed push overwrote holds the new bytes. The sha256
that the remote current version records for that key does not match the
stored object.

The remote catalog is readable after the rollback. Each key that the remote
current version uses still exists. Only the bytes of an overwritten asset can differ.

## Root cause

Asset hrefs contain no version segment. Each version writes to the same object
key, such as `{prefix}/{collection}/data.parquet`. A push overwrites the key
in place, and the store keeps no copy of the old bytes. So a rollback cannot
restore them.

Before October 2026 the rollback deleted every key that the push wrote
(Issue #965). That removed the previous good data, `collection.json`, and the
root `catalog.json`. Now `_existing_keys_before_push` in
`portolan_cli/sync/push.py` records the keys that exist before the upload.
One paginated listing reads the collection prefix. One HEAD checks each key
outside that prefix. `_cleanup_uploaded_assets` then deletes only the keys
that the failed push created.

When the listing or a HEAD fails, the rollback deletes nothing. A key that
the push created then stays in the store as an orphan. An orphan is harmless,
because no `versions.json` references it.

## Workaround

Run the push again. The next push compares the local current snapshot with
the remote current snapshot. It uploads the asset again when the two sha256
values differ, which is the case after the failed push of a new version.

If the local current version goes back to the bytes that the remote current
version records, the next push skips the key, also with `--force`. Then copy
the local file to the key by hand.

A full fix needs version segments in asset hrefs, or a staged upload that
copies into place only after the `versions.json` write succeeds.

## References

- Issue #965, the rollback that deleted live objects.
- Issue #962, the comparison with the remote current snapshot.

## Regression test

- `tests/integration/test_s3_moto.py::TestPushS3Integration::test_failed_push_keeps_the_remote_current_version_on_s3`
- `tests/unit/test_push.py::TestRollbackKeepsExistingKeys`
