# Vulture whitelist for false positives
# https://github.com/jendrikseipp/vulture#whitelisting
# ruff: noqa: F841

# Abstract method parameters are intentionally unused in base class
# These are used by concrete implementations
catalog_path = None  # Used by ValidationRule.check() implementations

# Test fixtures - pytest injects these, vulture doesn't understand
mock_aws_credentials = None  # Fixture for AWS credentials tests
capsys = None  # pytest built-in fixture
args = None  # Mock function parameter in tests
