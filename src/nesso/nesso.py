#!/usr/bin/env python
# changes 0.0.1
import typer

import nesso.source as source
import nesso.base_model as base_model
import nesso.model as model
import nesso.seed as seed

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
