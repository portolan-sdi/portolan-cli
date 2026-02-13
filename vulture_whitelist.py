# Vulture whitelist for false positives
# https://github.com/jendrikseipp/vulture#whitelisting
# ruff: noqa: F841

# Abstract method parameters are intentionally unused in base class
# These are used by concrete implementations
catalog_path = None  # Used by ValidationRule.check() implementations

# VersioningBackend protocol parameters are intentionally unused in stubs
# These will be used when methods are wired to versions.py
target_version = None  # Used by rollback() implementations
keep = None  # Used by prune() implementations
