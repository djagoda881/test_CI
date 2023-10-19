import oyaml as yaml
import typer
from nesso.base_model import create as create_base_model
from rich import print
from rich.panel import Panel
from rich.prompt import Prompt

from .common import DBT_PROJECT_DIR, call_shell, run_in_dbt_project

app = typer.Typer()

# fdfsfsdfdsw
# dsfsfsda f

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
            source_definitions = yaml.safe_load(f)["sources"]
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
    project: str = typer.Option(DBT_PROJECT_DIR.name, "--project", "-p"),
    case_sensitive_cols: bool = typer.Option(
        True,
        "--case-sensitive-cols",
        "-c",
        help="Whether the column names of the source case-sensitive.",
    ),
    force: bool = typer.Option(False, "--force", "-f"),
    no_profile: bool = typer.Option(False, "--no-profile", "-np"),
    create_base_models: bool = typer.Option(
        False,
        "--create-base-models",
        "-b",
        help="Whether to also create base models for all models in `source`.",
    ),
):
    """
    Adds a new source schema with all existing tables in it.

    Args:
        source (str): The name of the source schema.
        case_sensitive_cols (bool, optional): Determine if a given database type is case-sensitive. Defaults to True.
        force (bool, optional): Overwrites the existing source. Defaults to False.
        no_profile (bool, optional): Whether to perform table profiling. Defaults to False.
        project (str): The name of current dbt project. Defaults to DBT_PROJECT_DIR.name variable.

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

    args = {"schema_name": source, "print_result": "True"}
    get_existing_tables_cmd = (
        f"dbt -q run-operation get_tables_in_schema --args '{args}'"
    )
    existing_tables = call_shell(get_existing_tables_cmd).strip().split(",")

    for table in existing_tables:

        docs_path = base_dir.joinpath(f"{table}.md")
        docs_path_fmt = f"[bright_black]{docs_path}[/bright_black]"
        fqn_fmt = f"[white]{source}.{table}[/white]"

        if no_profile:
            print(f"Creating description template for model [blue]{fqn_fmt}[/blue]...")
            args = {"schema": source, "relation_name": table}
            content = call_shell(
                f"dbt -q run-operation create_description_markdown --args '{args}'"
            )
            success_msg = (
                f"Description template successfully written to {docs_path_fmt}."
            )
        else:
            print(f"Profiling source table {fqn_fmt}...")
            args = {"schema": source, "relation_name": table}
            content = call_shell(
                f"dbt -q run-operation print_profile_docs --args '{args}'"
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

    # Generate the YAML file.
    generate_yaml_text_command = f"""dbt -q run-operation generate_source --args '{{"schema_name": "{source}", "case_sensitive_cols": {case_sensitive_cols}}}'"""
    source_yaml: dict = yaml.safe_load(call_shell(generate_yaml_text_command))
    with open(source_path, "w") as file:
        yaml.safe_dump(source_yaml, file)

    # Print success message.
    operation_past_tenses = {"overwriting": "overwritten", "creating": "created"}
    operation_past_tense = operation_past_tenses[operation]
    print(f"Source [blue]{source}[/blue] has been {operation_past_tense} successfully.")

    # Create base models for all added sources.
    if create_base_models:
        for table in existing_tables:
            create_base_model(
                source, table, project=project, case_sensitive_cols=case_sensitive_cols
            )

    return True


@app.command()
@run_in_dbt_project
def add(
    source: str = typer.Argument(..., help="The name of the source schema."),
    project: str = typer.Option(DBT_PROJECT_DIR.name, "--project", "-p"),
    table_name: str = typer.Argument(..., help="The name of the table to add."),
    technical_owner: str = typer.Option(
        None, "--technical-owner", help="The technical owner of the table."
    ),
    business_owner: str = typer.Option(
        None, "--business-owner", help="The business owner of the table."
    ),
    no_profile: bool = typer.Option(False, "--no-profile", "-np"),
    case_sensitive_cols: bool = typer.Option(
        True,
        "--case-sensitive-cols",
        "-c",
        help="Whether the column names of the source case-sensitive.",
    ),
) -> bool:
    """
    Add a new table to a source schema and materializes it as a base model.

    Args:
        source (str): The name of the source schema.
        table_name (str): The name of the table to add.
        technical_owner (str): The technical owner of the table.
        business_owner (str): The business owner of the table.
        no_profile (bool, optional): Whether to perform table profiling.
        The generated profile will be added to each table's documentation.
        Defaults to False.
        case_sensitive_cols (bool, optional): Determine if a given database type is case-sensitive. Defaults to True.
        project (str): The name of current dbt project. Defaults to DBT_PROJECT_DIR.name variable.

    Raises:
        SourceTableExistsError: If the table already exists in the source YAML.

    Returns:
        bool: Whether the operation was successful.
    """
    base_dir = DBT_PROJECT_DIR.joinpath("models", "sources", source)
    source_path = base_dir.joinpath(source + ".yml")
    fqn = f"{source}.{table_name}"
    fqn_fmt = f"[white]{source}.{table_name}[/white]"

    if check_if_source_table_exists(source=source, table_name=table_name):
        # TODO: change to --force (check if dbt can actually overwrite a single table)
        raise SourceTableExistsError(
            f"Source table '{fqn}' already exists. Skipping..."
        )

    # Generate docs.
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
            width=120,
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

    # Generate source YAML and append it to the sources schema.
    args = {
        "schema_name": source,
        "table_names": [
            table_name,
        ],
        "technical_owner": technical_owner,
        "business_owner": business_owner,
        "case_sensitive_cols": case_sensitive_cols,
    }
    generate_source_text_command = (
        f"""dbt -q run-operation generate_source --args '{args}'"""
    )
    source_str = call_shell(generate_source_text_command)
    with open(source_path, "a") as file:
        file.write(source_str)

    print(f"Source table {fqn_fmt} has been added successfully.")

    print(
        Panel(
            f"""Please edit the source table's YAML definition in
            [bright_black]{source_path}[/bright_black] by providing relevant metadata, tests, etc. before continuing.""",
            title="ATTENTION",
            width=120,
        )
    )
    Prompt.ask("Press [green]ENTER[/green] to continue")

    create_base_model(source, table_name, project, case_sensitive_cols)

    return True


if __name__ == "__main__":
    app()
