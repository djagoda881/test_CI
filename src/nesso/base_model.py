from .common import (
    call_shell,
    DBT_PROJECT_DIR,
    BASE_MODELS_SCHEMA,
    run_in_dbt_project,
)
from rich import print

import typer

app = typer.Typer()


def check_if_base_model_exists(base_model_name: str) -> bool:
    sql_path = DBT_PROJECT_DIR.joinpath(
        "models", BASE_MODELS_SCHEMA, base_model_name, (f"{base_model_name}.sql")
    )
    yml_path = DBT_PROJECT_DIR.joinpath(
        "models", BASE_MODELS_SCHEMA, base_model_name, (f"{base_model_name}.yml")
    )

    both_files_exist = sql_path.exists() and yml_path.exists()
    none_files_exist = not sql_path.exists() and not yml_path.exists()

    fqn = f"[blue]{BASE_MODELS_SCHEMA}.{base_model_name}[/blue]"
    msg = f"SQL or YML file for the base model {fqn} is missing.\nPlease remove the remaining file."
    assert both_files_exist or none_files_exist, msg

    return sql_path.exists()


@app.command()
@run_in_dbt_project
def create(
    source: str,
    source_table_name: str,
    project: str = DBT_PROJECT_DIR.name,
    case_sensitive_cols: bool = True,
    force: bool = typer.Option(False, "--force", "-f"),
):
    """
    Creates a base model for the selected table from the source.

    Args:
        source (str): The name of the source schema.
        table_name (str): The name of the table to add.
        project (str, optional): The name of current dbt project. Defaults to `DBT_PROJECT_DIR.name`.
        case_sensitive_cols (bool, optional): Determine if a given database type is case-sensitive. Defaults to True.
        force (bool, optional): Specifies whether the model is to be overwritten. Defaults to False.
    """

    base_model_name = f"stg_{source_table_name}"

    base_dir = DBT_PROJECT_DIR.joinpath("models", BASE_MODELS_SCHEMA, base_model_name)
    yml_path = base_dir.joinpath(f"{base_model_name}.yml")
    sql_path = base_dir.joinpath(f"{base_model_name}.sql")

    if not base_dir.exists():
        base_dir.mkdir(parents=True, exist_ok=True)

    fqn = f"{BASE_MODELS_SCHEMA}.{base_model_name}"
    fqn_fmt = f"[white]{fqn}[/white]"
    source_fqn = f"{source}.{source_table_name}"
    source_fqn_fmt = f"[white]{source_fqn}[/white]"

    base_model_exists = check_if_base_model_exists(base_model_name)
    if base_model_exists:
        if force:
            operation = "overwriting"
        else:
            print(f"Base model {fqn_fmt} already exists. Skipping...")
            return
    else:
        operation = "creating"

    operation_past_tenses = {"overwriting": "overwritten", "creating": "created"}
    operation_past_tense = operation_past_tenses[operation]

    # Generate SQL
    print(f"{operation.title()} base model {fqn_fmt} from {source_fqn_fmt}...")
    base_model_content = call_shell(
        f"""dbt -q run-operation generate_base_model --args '{{"source_name": "{source}", "table_name": "{source_table_name}", "project": "{project}", "case_sensitive_cols" : {case_sensitive_cols}}}'"""
    )
    with open(sql_path, "w") as file:
        file.write(base_model_content)
    print(f"Base model {fqn_fmt} has been {operation_past_tense} successfully.")

    # Generate YAML
    print(f"{operation.title()} YAML template for base model {fqn}...")
    base_model_yaml_content = call_shell(
        f"""dbt -q run-operation generate_model_yaml --args '{{"model_name": "{source_table_name}", "base_model": True }}'"""
    )
    with open(yml_path, "w") as file:
        file.write(base_model_yaml_content)

    call_shell(f"dbt run --select {base_model_name}")

    print(
        f"YAML template for base model {fqn_fmt} has been {operation_past_tense} successfully."
    )


if __name__ == "__main__":
    app()
