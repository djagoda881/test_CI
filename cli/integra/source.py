import typer
import yaml
from rich import print
from rich.panel import Panel
from rich.prompt import Prompt
from yaml.loader import SafeLoader

from integra.base_model import create as create_base_model

from .common import (
    BASE_MODELS_SCHEMA,
    DBT_PROJECT_DIR,
    call_shell,
    run_in_dbt_project,
)

app = typer.Typer()


class SourceTableExistsError(Exception):
    pass


def check_if_source_exists(source: str) -> bool:
    source_path = DBT_PROJECT_DIR.joinpath("models", "sources", source, source + ".yml")
    return source_path.exists()


def check_if_source_table_exists(source: str, table_name: str) -> bool:
    if check_if_source_exists(source):
        yml_path = DBT_PROJECT_DIR.joinpath(
            "models", "sources", source, source + ".yml"
        )
        with open(yml_path) as f:
            source_definitions = yaml.load(f, Loader=SafeLoader)["sources"]
            source_definition = [
                sd for sd in source_definitions if sd["name"] == source
            ][0]
            source_tables = source_definition["tables"]
            if not source_tables:
                return False
            else:
                return any([table["name"] == table_name for table in source_tables])
    return False


@app.command()
@run_in_dbt_project
def create(
    source: str,
    force: bool = typer.Option(False, "--force", "-f"),
    no_profile: bool = typer.Option(False, "--no-profile", "-np"),
):
    """
    Adds a new source with all existing tables in it.

    Args:
        source (str): The name of the source schema.
        force (bool, optional): Overwrites the existing source. Defaults to False.
        no_profile (bool, optional): Whether to perform table profiling. Defaults to False.

    Returns:
        bool: Whether the operation was successful.
    """

    base_dir = DBT_PROJECT_DIR.joinpath("models", "sources", source)
    source_path = base_dir.joinpath(source + ".yml")

    if not base_dir.exists():
        base_dir.mkdir(parents=True, exist_ok=True)

    source_exists = check_if_source_exists(source)
    if source_exists:
        if force:
            operation = "overwriting"
        else:
            print(f"Source [blue]{source}[/blue] [b]already exists[/b]. Skipping...")
            return False
    else:
        operation = "creating"

    print(f"[white]{operation.title()} source[/white] [blue]{source}[/blue]...")

    download_tables = (
        f"""dbt -q run-operation get_tables --args '{{"schema_name": "{source}"}}'"""
    )
    source_tables = call_shell(download_tables)
    source_tables = source_tables.strip().split(",")

    for table in source_tables:

        docs_path = base_dir.joinpath(f"{table}.md")
        docs_path_fmt = f"[bright_black]{docs_path}[/bright_black]"
        fqn_fmt = f"[white]{source}.{table}[/white]"

        if no_profile:
            print(f"Creating description template for model {fqn_fmt}...")
            content = call_shell(
                f"""dbt -q run-operation create_description_markdown --args '{{"schema": "{source}", "relation_name": {table}}}'"""
            )
            success_msg = (
                f"Description template successfully written to {docs_path_fmt}."
            )
        else:
            print(f"Profiling source table {fqn_fmt}...")
            content = call_shell(
                f"""dbt -q run-operation print_profile_docs --args '{{"schema": "{source}", "relation_name": {table}}}'"""
            )
            success_msg = f"Profile successfully written to {docs_path_fmt}."

        with open(docs_path, "w") as file:
            file.write(content)

        print(success_msg)

        print(
            Panel(
                f"""Please open {docs_path_fmt}
                and add your description in the [blue]📝 Details[/blue] section before continuing.""",
                title="ATTENTION",
                width=90,
            )
        )
        Prompt.ask("Press [green]ENTER[/green] to continue")

    generate_yaml_text_command = f"""dbt -q run-operation generate_source --args '{{"schema_name": "{source}"}}'"""
    yaml_text = call_shell(generate_yaml_text_command)

    with open(source_path, "w") as file:
        file.write(yaml_text)

    operation_past_tenses = {"overwriting": "overwritten", "creating": "created"}
    operation_past_tense = operation_past_tenses[operation]
    print(f"Source [blue]{source}[/blue] has been {operation_past_tense} successfully.")

    return True


@app.command()
@run_in_dbt_project
def add(
    source: str = typer.Argument(..., help="The name of the source schema."),
    table_name: str = typer.Argument(..., help="The name of the table to add."),
    project: str = typer.Argument(..., help="Name of the current dbt_project."),
    technical_owner: str = typer.Option(
        None, "--technical-owner", help="The technical owner of the table."
    ),
    business_owner: str = typer.Option(
        None, "--business-owner", help="The business owner of the table."
    ),
    no_profile: bool = typer.Option(False, "--no-profile", "-np"),
    case_sensitive_cols: bool = typer.Option(
        True, "--case_sensitive_cols", help="Whether the database is case-sensitive."
    ),
) -> bool:
    """
    Add a new table to a source schema and materializes it as a base model.

    Args:
        source (str): The name of the source schema.
        table_name (str): The name of the table to add.
        project (str): The name of current dbt project.
        technical_owner (str): The technical owner of the table.
        business_owner (str): The business owner of the table.
        no_profile (bool, optional): Whether to perform table profiling.
        The generated profile will be added to each table's documentation.
        Defaults to False.
        case_sensitive_cols (bool, optional): Determine if a given database type is case-sensitive. Defaults to True.

    Raises:
        SourceTableExistsError: If the table already exists in the source YAML.

    Returns:
        bool: Whether the operation was successful.
    """
    base_dir = DBT_PROJECT_DIR.joinpath("models", "sources", source)
    source_path = base_dir.joinpath(source + ".yml")
    fqn = f"{source}.{table_name}"
    fqn_fmt = f"[white]{source}.{table_name}[/white]"
    fqn_fmt_visible = f"[blue]{source}.{table_name}[/blue]"

    if check_if_source_table_exists(source=source, table_name=table_name):
        # TODO: change to --force (check if dbt can actually overwrite a single table)
        raise SourceTableExistsError(
            f"Source table '{fqn}' already exists. Skipping..."
        )

    docs_path = base_dir.joinpath(f"{table_name}.md")
    docs_path_fmt = f"[bright_black]{docs_path}[/bright_black]"

    if no_profile:
        print(f"Creating description template for model {fqn_fmt}...")
        content = call_shell(
            f"""dbt -q run-operation create_description_markdown --args '{{"schema": "{source}", "relation_name": {table_name}}}'"""
        )
        success_msg = f"Description template successfully written to {docs_path_fmt}."
    else:
        print(f"Profiling source table {fqn_fmt}...")
        content = call_shell(
            f"""dbt -q run-operation print_profile_docs --args '{{"schema": "{source}", "relation_name": {table_name}}}'"""
        )
        success_msg = f"Profile successfully written to {docs_path_fmt}."

    with open(docs_path, "w") as file:
        file.write(content)

    print(success_msg)

    print(
        Panel(
            f"""Please open {docs_path_fmt}
            and add your description in the [blue]📝 Details[/blue] section before continuing.""",
            title="ATTENTION",
            width=90,
        )
    )
    Prompt.ask("Press [green]ENTER[/green] to continue")

    if technical_owner is None:
        technical_owner = Prompt.ask(
            f"\nPlease type the [white]technical owner[/white] for the source table [blue]{fqn_fmt}[/blue] and then press [green]ENTER[/green].\n"
        )

    if business_owner is None:
        business_owner = Prompt.ask(
            f"\nPlease type the [white]business owner[/white] for the source table [blue]{fqn_fmt}[/blue] and then press [green]ENTER[/green].\n"
        )

    generate_source_text_command = f"""dbt -q run-operation generate_source --args '{{"schema_name": "{source}", "table_names": ["{table_name}",], "technical_owner": "{technical_owner}", "business_owner": "{business_owner}"}}'"""
    source_text = call_shell(generate_source_text_command)

    with open(source_path, "a") as file:
        file.write(source_text)

    print(f"Source table {fqn_fmt} has been added successfully.")

    print(
        Panel(
            f"""Please edit the source table's YAML definition in
            [bright_black]{source_path}[/bright_black] by providing relevant metadata, tests, etc. before continuing.""",
            title="ATTENTION",
            width=90,
        )
    )
    Prompt.ask("Press [green]ENTER[/green] to continue")

    base_model = table_name
    base_model_fqn = f"{BASE_MODELS_SCHEMA}.{base_model}"

    create_base_model(source, base_model, project, case_sensitive_cols)
    call_shell(f"dbt -q run --select {base_model}")

    print(f"Base model {base_model_fqn} has been materialized successfully.")

    print(f"\nSource {fqn_fmt_visible} has been added [green]successfully[/green].")

    return True


if __name__ == "__main__":
    app()
