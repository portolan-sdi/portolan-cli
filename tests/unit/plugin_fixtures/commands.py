"""Click commands that the command group tests load through entry points."""

from __future__ import annotations

import click


@click.group()
def group() -> None:
    """Example plugin commands."""


@group.command()
def hello() -> None:
    """Say hello."""
    click.echo("hello from the plugin")


@click.command()
def rollback() -> None:
    """Roll back a version."""
    click.echo("plugin rollback")


@click.command()
def add() -> None:
    """A command name that collides with a built-in."""
    click.echo("plugin add")


not_a_command = object()
