import pathlib

import typer
from integra.common import call_shell, run_in_dbt_project
from rich import print
from rich.prompt import Prompt

app = typer.Typer()

MODELS_DIR = pathlib.Path(__file__).resolve().parent.parent.parent.joinpath("models")


@app.command()
@run_in_dbt_project
def bootstrap(
    model_name: str = typer.Argument(..., help="The name of the model."),
    mart: str = typer.Argument(
        ..., help="The data mart in which the models will be located."
    ),
    project_name: str = typer.Argument(
        ..., help="The name of the project inside the mart."
    ),
):
    """
    Bootstrap a new project.

    Args:
        model_name (str): The name of the model.
        mart (str): The data mart in which the models will be located.
        project_name (str): The name of the project inside the mart.

    Generates the following files:\n
        - models/marts/<MART>/<PROJECT_NAME>/<MODEL_NAME>/<MODEL_NAME>.sql (empty file)

    The empty model SQL then needs to be developed by user. Once it's ready, user
    can then call `dbt run` to materialize the model then later `model bootstrap-yaml <MODEL_NAME> <MART> <PROJECT_NAME>` to bootstrap relevant YML file(s).

    Example:
        integra model bootstrap c4c_example sales cloud_for_customer
    """
    PROJECT_DIR = MODELS_DIR.joinpath("marts", mart, project_name)
    MODEL_DIR = PROJECT_DIR.joinpath(model_name)

    if not MODEL_DIR.exists():
        MODEL_DIR.mkdir(parents=True, exist_ok=True)

    sql_path = MODEL_DIR.joinpath(model_name + ".sql")
    try:
        sql_path.touch(exist_ok=False)
        sql_path_short = pathlib.Path(
            "models", "marts", mart, project_name, model_name, model_name + ".sql"
        )
        print(
            f"File [bright_black]{sql_path_short}[/bright_black] has been created [green]successfully[/green]."
        )
    except FileExistsError:
        pass

    print("Model bootstrapping is [green]complete[/green].")

    print(
        """\n[white]Once you create your model(s), you can run [/white][bright_black]integra model bootstrap-yaml <YOUR_MODEL_NAME>[/bright_black]
[white]to bootstrap relevant YML file(s).[/white]"""
    )


@app.command()
@run_in_dbt_project
def bootstrap_yaml(
    model_name: str = typer.Argument(..., help="The name of the model."),
    mart: str = typer.Argument(
        ..., help="The mart (schema) in which the model is located."
    ),
    project_name: str = typer.Argument(
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
):
    """
    Bootstrap the YAML file for a particular model. The model must already be materialized.

    Args:
        model_name (str): The name of the model.
        mart (str): The data mart in which the models will be located.
        project_name (str): The name of the project inside the mart.
        technical_owner (str, Optional): The technical owner of the table.
        business_owner (str, Optional): The business owner of the table.
        target (str, Optional): The environment to use. This should only be altered in production.

    The model can be materialized by using a command `dbt run -m <MODEL_NAME>`

    Example:
        integra model bootstrap-yaml c4c_example sales cloud_for_customer

    """

    PROJECT_DIR = MODELS_DIR.joinpath("marts", mart, project_name)
    MODEL_DIR = PROJECT_DIR.joinpath(model_name)

    if not MODEL_DIR.exists():
        MODEL_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Creating YAML for model [blue]{model_name}[/blue]...")
    yml_path = MODEL_DIR.joinpath(model_name + ".yml")

    if technical_owner is None:
        technical_owner = Prompt.ask(
            f"Please provide a [white]technical owner[/white] for model [blue]{model_name}[/blue] and then press [green]ENTER[/green]"
        )
    if business_owner is None:
        business_owner = Prompt.ask(
            f"Please provide a [white]business owner[/white] for model [blue]{model_name}[/blue] and then press [green]ENTER[/green]"
        )

    generate_model_yaml_text_command = f"""dbt -q run-operation generate_model_yaml --target {target} --args '{{"model_name": "{model_name}", "technical_owner":{technical_owner}, "business_owner":{business_owner}, "upstream_metadata": true}}'"""
    model_yaml_text = call_shell(generate_model_yaml_text_command)
    with open(yml_path, "w") as file:
        file.write(model_yaml_text)
    print(f"YAML template for [blue]{model_name}[/blue] has been created successfully.")


if __name__ == "__main__":
    app()
