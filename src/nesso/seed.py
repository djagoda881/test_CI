"""
In order to avoid confusion, it's useful distinguish between these four entities:

a) seed file
A CSV file that is used by dbt to create a source table. We extend that functionality
to also support Excel files.

b) seed
A dbt concept which allows to upload CSVs as tables. In this file, we use this word
to refer to an entry about a particular seed file in the seed schema.

c) seed schema
The YAML file holding metadata about seed tables.

d) seed table
The materialized seed, ie. the actual database table created based on the seed file.

Therefore, to check if a seed exists, we'll be looking for a relevant entry in the seed schema.
To check if a seed table exists, we will be checking in the database. And to check if
the seed file or seed schema exists, we'll be examining or searching for YAML files.
"""


from pathlib import Path

import oyaml as yaml
import pandas as pd
import typer
from loguru import logger
from rich import print

from .common import DBT_PROJECT_DIR, call_shell, run_in_dbt_project

app = typer.Typer()

DEFAULT_SEED_SCHEMA_PATH = DBT_PROJECT_DIR.joinpath(
    "seeds", "master_data", "schema.yml"
)


def _excel_to_csv(infile: str, outfile: str) -> None:
    """
    Converts an Excel file to a CSV.

    Args:
        infile (str): The path to the Excel file to be converted.
        outfile (str): The path where the CSV file should be saved.
    """

    df = pd.read_excel(infile)

    # Sanitize column names.
    df.columns = [f"{column.strip().replace(' ', '_')}" for column in df.columns]
    df.columns = df.columns.str.replace("[,;{}()\n\t=]", "", regex=True)

    df.to_csv(
        outfile,
        index=True,
    )

    logger.debug(f"{outfile.name} has been created successfully.")


def check_if_schema_exists(schema_path: str) -> bool:
    # Enforce pathlib.Path type.
    schema_path = Path(schema_path)

    # File doesn't exist.
    if not schema_path.exists():
        return False

    # File is empty.
    with open(schema_path) as f:
        schema_yaml = yaml.safe_load(f)
    if schema_yaml is None:
        return False

    return "version" in schema_yaml and "seeds" in schema_yaml


def check_if_seed_exists(
    seed: str, schema_path: str = DEFAULT_SEED_SCHEMA_PATH
) -> bool:
    """
    Checks if a seed is present in seed schema file.

    Args:
        seed (str) The name of the seed.
        schema_path (str, Optional) The path to the seed schema. Defaults to
            `DEFAULT_SEED_SCHEMA_PATH`.

    Returns:
        True if the seed is present in the schema, otherwise False.
    """
    # Enforce pathlib.Path type.
    schema_path = Path(schema_path)

    if not schema_path.exists():
        return False

    with open(schema_path) as f:
        seeds = yaml.safe_load(f)["seeds"]

    if not seeds:
        return False

    return any([s.get("name").lower() == seed.lower() for s in seeds])


def create_schema(schema_path: str = DEFAULT_SEED_SCHEMA_PATH) -> None:
    # Enforce pathlib.Path type.
    schema_path = Path(schema_path)
    if schema_path.exists():
        raise ValueError(f"Schema '{schema_path}' already exists.")

    schema_yaml = call_shell(f"dbt -q run-operation generate_seed_schema_yaml").strip()
    with open(schema_path, "w") as f:
        f.write(schema_yaml)


def add_to_schema(
    seed: str,
    technical_owner: str,
    business_owner: str,
    schema_path: str = DEFAULT_SEED_SCHEMA_PATH,
    target: str = "qa",
    case_sensitive_cols: bool = True,
    overwrite: bool = False,
) -> None:

    exists = check_if_seed_exists(seed)
    if exists and not overwrite:
        raise ValueError(
            f"Seed {seed} already exists and 'overwrite' is set to 'False'."
        )

    schema_exists = check_if_schema_exists(schema_path=schema_path)
    if not schema_exists:
        create_schema(schema_path=schema_path)

    args = {
        "seed": seed,
        "technical_owner": technical_owner,
        "business_owner": business_owner,
        "case_sensitive_cols": case_sensitive_cols,
    }
    command = (
        f"""dbt -q run-operation --target {target} generate_seed_yaml --args '{args}'"""
    )
    current_seed_schema = yaml.safe_load(call_shell(command))[0]
    with open(schema_path) as f:
        seed_schema = yaml.safe_load(f)
    seed_schema["seeds"].append(current_seed_schema)
    with open(schema_path, "w") as f:
        yaml.safe_dump(seed_schema, f)

    successful_comment = f"[blue]{seed}[/blue] has been successfully added to [white]{schema_path}[/white]."
    print(successful_comment)


@app.command()
@run_in_dbt_project
def register(
    seed: str = typer.Argument(..., help="The name of the seed to register."),
    technical_owner: str = typer.Option(
        ..., "--technical-owner", help="The technical owner of the table."
    ),
    business_owner: str = typer.Option(
        ..., "--business-owner", help="The business owner of the table."
    ),
    overwrite: bool = typer.Option(
        False,
        "--overwrite",
        "-o",
        help="Whether to overwrite the schema YAML.",
    ),
    schema_path: str = typer.Option(
        DEFAULT_SEED_SCHEMA_PATH,
        "--yaml-path",
        help="""The absolute path of the schema file to which to append seed schema, 
default path is DBT_PROJECT_DIR/seeds/master_data/schema.yml""",
    ),
    target: str = typer.Option(
        "qa",
        "--target",
        "-t",
        help="the target to work with, options are ('qa', 'prod') default is qa",
    ),
) -> None:
    """
    Add an entry for the seed in seed schema and, if needed, materialize it.

    Args:
        seed (str): The name of the seed to register.
        technical_owner (str): The technical owner of the table.
        business_owner (str): The business owner of the table.
        overwrite (bool, Optional): Whether to overwrite schema YAML file with seed information
            if seed is already present in schema YAML file.
        schema_path (str, Optional): The absolute path of the YAML file to append seed information.
            Default path is DBT_PROJECT_DIR/seeds/aster_data/schema.yml.
        target (str, Optional): The target to work with, options are ('qa', 'prod').
            Defaults to qa.
    """

    # Enforce types to fix Typer converting strings to typer.Option objects.
    schema_path = Path(schema_path)
    technical_owner = str(technical_owner)
    business_owner = str(business_owner)

    print(f"Registering seed [blue]{seed}[/blue]...")

    seed_exists = check_if_seed_exists(seed, schema_path=schema_path)
    if seed_exists:
        # We need to handle the case when the seed is present in the schema,
        # but not materialized (eg. the table was dropped after the seed was registered).
        args = {"schema_pattern": "", "table_pattern": seed}
        seed_table_exists_query = f"""dbt -q run-operation get_relations_by_pattern --target {target} --args '{args}'"""
        seed_table_exists = call_shell(seed_table_exists_query)

        if not seed_table_exists:
            call_shell(f"dbt -q seed --target {target} --select {seed}")

    add_to_schema(
        seed,
        technical_owner=technical_owner,
        business_owner=business_owner,
        target=target,
    )

    print(f"Seed [blue]{seed}[/blue] has been registered [green]Successfully[/green].")


if __name__ == "__main__":
    app()
