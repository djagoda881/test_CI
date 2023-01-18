#!/usr/bin/env python

import typer

import integra.source as source
import integra.base_model as base_model
import integra.model as model
import integra.seed as seed

app = typer.Typer()
app.add_typer(source.app, name="source")
app.add_typer(base_model.app, name="base_model")
app.add_typer(model.app, name="model")
app.add_typer(seed.app, name="seed")


def cli():
    """For python script installation purposes (flit)"""
    app()


if __name__ == "__main__":
    app()
