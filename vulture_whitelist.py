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

# CLI options that are defined for future interactive prompting
# The --auto flag is accepted by CLI but prompting not yet implemented
auto_mode = None  # Will control interactive prompting when implemented
