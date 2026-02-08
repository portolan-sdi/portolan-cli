# Vulture whitelist for false positives
# https://github.com/jendrikseipp/vulture#whitelisting
# ruff: noqa: F841

# Abstract method parameters are intentionally unused in base class
# These are used by concrete implementations
catalog_path = None  # Used by ValidationRule.check() implementations

# Note: Test fixture parameters are now marked as used in-line with
# assertions or assignment to _ to satisfy vulture. The blanket exclusion
# of test_upload.py has been removed from pyproject.toml.
