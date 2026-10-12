"""The built-in format handlers, one package per format (#947).

Each package imports only ``portolan_cli.api``, ``portolan_cli.hookspecs``,
its own package, and third-party libraries, the same as an outside plugin.
``tests/unit/plugins/test_boundary.py`` checks this. The hook module
``portolan_cli.plugins.builtin.formats`` registers them.
"""
