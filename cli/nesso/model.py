import pathlib

import typer
from .common import call_shell, run_in_dbt_project
from rich import print
from rich.prompt import Prompt

app = typer.Typer()

MODELS_DIR = pathlib.Path(__file__).resolve().parent.parent.parent.joinpath("models")


@app.command()
@run_in_dbt_project
def bootstrap(
    model: str = typer.Argument(..., help="The name of the model."),
    mart: str = typer.Argument(
        ..., help="The data mart in which the models will be located."
    ),
    project: str = typer.Argument(..., help="The name of the project inside the mart."),
):
    """
    Bootstrap a new project.

    Args:
        model (str): The name of the model.
        mart (str): The data mart in which the models will be located.
        project (str): The name of the project inside the mart.

    Generates the following files:\n
        - models/marts/<MART>/<PROJECT_NAME>/<MODEL_NAME>/<MODEL_NAME>.sql (empty file)

    The empty model SQL then needs to be developed by user. Once it's ready, user
    can then call `dbt run` to materialize the model then later `model bootstrap-yaml <MODEL_NAME> <MART> <PROJECT_NAME>` to bootstrap relevant YML file(s).

    Example:
        `nesso model bootstrap c4c_example sales cloud_for_customer`
    """
    PROJECT_DIR = MODELS_DIR.joinpath("marts", mart, project)
    MODEL_DIR = PROJECT_DIR.joinpath(model)

    if not MODEL_DIR.exists():
        MODEL_DIR.mkdir(parents=True, exist_ok=True)

    sql_path = MODEL_DIR.joinpath(model + ".sql")
    try:
        sql_path.touch(exist_ok=False)
        sql_path_short = pathlib.Path(
            "models", "marts", mart, project, model, model + ".sql"
        )
        print(
            f"File [bright_black]{sql_path_short}[/bright_black] has been created [green]successfully[/green]."
        )
    except FileExistsError:
        pass

    print("Model bootstrapping is [green]complete[/green].")

    print(
        """\n[white]Once you create your model(s), you can run [/white][bright_black]nesso model bootstrap-yaml <YOUR_MODEL_NAME>[/bright_black]
[white]to bootstrap relevant YML file(s).[/white]"""
    )


@app.command()
@run_in_dbt_project
def bootstrap_yaml(
    model: str = typer.Argument(..., help="The name of the model."),
    mart: str = typer.Argument(
        ..., help="The mart (schema) in which the model is located."
    ),
    project: str = typer.Argument(
        ..., help="The name of the project under which the model falls."
    ),
    technical_owner: str = typer.Option(
        None, "--technical-owner", help="The technical owner of the table."
    ),
    business_owner: str = typer.Option(
        None, "--business-owner", help="The business owner of the table."
    ),
    target: str = typer.Option(
        "qa",
        "--target",
        "-t",
        help="The environment to use. This should only be altered in production.",
    ),
    case_sensitive_cols: bool = typer.Option(
        True, "--case_sensitive_cols", help="Whether the database is case-sensitive."
    ),
):
    """
    Bootstrap the YAML file for a particular model. The model must already be materialized.

    Args:
        model (str): The name of the model.
        mart (str): The data mart in which the models will be located.
        project (str): The name of the project inside the mart.
        technical_owner (str, Optional): The technical owner of the table.
        business_owner (str, Optional): The business owner of the table.
        target (str, Optional): The environment to use. This should only be altered in production.
        case_sensitive_cols (bool, optional): Determine if a given database type is case-sensitive. Defaults to True.

    The model can be materialized by using a command `dbt run -m <MODEL_NAME>`

    Example:
        `nesso model bootstrap-yaml c4c_example sales cloud_for_customer`

    """

    PROJECT_DIR = MODELS_DIR.joinpath("marts", mart, project)
    MODEL_DIR = PROJECT_DIR.joinpath(model)

    if not MODEL_DIR.exists():
        MODEL_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Creating YAML for model [blue]{model}[/blue]...")
    yml_path = MODEL_DIR.joinpath(model + ".yml")

    if technical_owner is None:
        technical_owner = Prompt.ask(
            f"Please provide a [white]technical owner[/white] for model [blue]{model}[/blue] and then press [green]ENTER[/green]"
        )
    if business_owner is None:
        business_owner = Prompt.ask(
            f"Please provide a [white]business owner[/white] for model [blue]{model}[/blue] and then press [green]ENTER[/green]"
        )

    generate_model_yaml_text_command = f"""dbt -q run-operation generate_model_yaml --target {target} --args '{{"model_name": "{model}", "technical_owner":{technical_owner}, "business_owner":{business_owner}, "upstream_metadata": true, "case_sensitive_cols": {case_sensitive_cols}}}'"""
    model_yaml_text = call_shell(generate_model_yaml_text_command)
    with open(yml_path, "w") as file:
        file.write(model_yaml_text)
    print(f"YAML template for [blue]{model}[/blue] has been created successfully.")


if __name__ == "__main__":
    app()
