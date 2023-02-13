from pathlib import Path

import pandas as pd
import typer
import yaml
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


def get_all_seeds(target: str = "qa") -> list[str]:
    """
    Retrieve all seeds in project. This is done by using `dbt ls` to list
    all the seed (CSV) files in the seed directory (as specified in `dbt_project.yml`).

    Note that this does not mean that the seeds are materialized.

    Args:
        target (str, optional) The target to work with, options are ('qa', 'prod').
            Defaults to qa.

    Returns:
        list[str]: All seed names in your project.
    """
    seed_paths = (
        call_shell(f"""dbt -q ls --resource-type seed --target {target}""")
        .strip()
        .split("\n")
    )
    seed_names = [seed_path.split(".")[-1] for seed_path in seed_paths]
    return seed_names


def check_if_seed_exists(seed: str) -> bool:
    """
    Check if a seed file is present in the seeds directory.

    Args:
        seed (str): The name of the seed to check.

    Returns:
        bool: Whether the seed is registered in dbt.
    """
    all_seeds = get_all_seeds()
    return seed in all_seeds


def check_seed_in_yaml(seed: str, yaml_path: str = DEFAULT_SEED_SCHEMA_PATH) -> bool:
    """
    Checks if a seed name is inside the schema YAML file.

    Args:
        seed (str) The name of the seed.
        yaml_path (str, Optional) The path to the file with seed YAML schema.
            Defaults to `DEFAULT_SEED_SCHEMA_PATH` variable.

    Returns:
        If the seed name provided to the function is in schema YAML file,
            it returns True, otherwise it returns False
    """
    if not yaml_path.is_file():
        return False

    with open(yaml_path) as f:
        seeds_in_yaml = yaml.safe_load(f)["seeds"]
        return any([seed.get("name").lower() == seed.lower() for seed in seeds_in_yaml])


def add_to_schema(
    seed: str,
    technical_owner: str,
    business_owner: str,
    schema_path: str = DEFAULT_SEED_SCHEMA_PATH,
    target: str = "qa",
    case_sensitive_cols: bool = True,
) -> None:
    command = f"""
dbt -q run-operation --target {target} create_seed_yaml_text --args '{
    {
        "seeds": [seed],
        "technical_owner": {technical_owner},
        "business_owner": {business_owner},
        "case_sensitive_cols":{case_sensitive_cols}
    }
}'
"""
    yaml_text = call_shell(command)

    with open(schema_path, "a") as file:
        file.write(yaml_text)

    successful_comment = f"[blue]{seed}[/blue] has been successfully added to [white]{schema_path}[/white]."
    print(successful_comment)


def create_seed_yaml_text(
    seeds: list[str],
    technical_owner: str,
    business_owner: str,
    new: str = False,
    target: str = "qa",
    case_sensitive_cols: bool = True,
) -> bool:

    """
    Returns text of YAML file with selected seeds information in it.

    Args:
        seeds (list[str]): The name(s) of the seed(s).
        technical_owner (str, Optional): The technical owner of the table.
        business_owner (str, Optional): The business owner of the table.
        new (bool, Optional): Whether to include headers that are needed when creating
            a new yaml. Defaults to False.
        target (str, Optional): The target to work with, options are ('qa', 'prod').
            Defaults to qa.
        case_sensitive_cols (bool, optional): Determine if a given database type is
            case-sensitive. Defaults to True.

    Returns:
        yaml_text (str): String with yaml formatting containing seeds information.
    """

    if len(seeds) == 0:
        print("No seeds to append to YAML file")
        return False

    generate_yaml_text_command = f"""
dbt -q run-operation --target {target} create_seed_yaml_text --args '{
    {
        "technical_owner": {technical_owner},
        "business_owner": {business_owner},
        "new": {new},
        "case_sensitive_cols":{case_sensitive_cols}
    }
}'
"""
    yaml_text = call_shell(generate_yaml_text_command)
    return yaml_text


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
) -> bool:
    """
    Generate YAML and, if needed, create a database table for provided seed file.

    Args:
        seed (str): The name of the seed to register.
        technical_owner (str): The technical owner of the table.
        business_owner (str): The business owner of the table.
        overwrite (bool, Optional): Whether to overwrite schema YAML file with seed information
            if seed is already present in schema YAML file.
        yaml_path (str, Optional): The absolute path of the YAML file to append seed information.
            Default path is DBT_PROJECT_DIR/seeds/aster_data/schema.yml.
        target (str, Optional): The target to work with, options are ('qa', 'prod').
            Defaults to qa.
    """

    # Enforce pathlib.Path type
    yaml_path = Path(yaml_path)

    # excel_files, all_files = list_excel_files(DEFAULT_SEED_SCHEMA_PATH.parent)

    # for excel_file in excel_files:
    #     filename_without_file_extension = excel_file.split(".")[0]
    #     filename_with_csv_file_extension = (
    #         filename_without_file_extension.replace(" ", "_") + ".csv"
    #     )

    #     if filename_with_csv_file_extension not in all_files:
    #         _excel_to_csv(
    #             DEFAULT_SEED_SCHEMA_PATH.parent.joinpath(excel_file),
    #             DEFAULT_SEED_SCHEMA_PATH.parent.joinpath(
    #                 filename_with_csv_file_extension
    #             ),
    #         ),

    # # If the seed isn't present in the seed schema YAML, materialize it
    # # in the database.
    # if not check_seed_exists(seed):
    #     print(f"Creating table {seed}...")
    #     print("[white]This may take some time[/white], [green]do not worry[/green]...")
    #     call_shell(f"dbt seed --target {target} -s {seed}")

    is_seed_in_schema = check_seed_in_yaml(seed, yaml_path=yaml_path)
    if is_seed_in_schema:
        print(
            f"Seed [blue]{seed}[/blue] [b]already exists[/b] in the schema file. Skipping..."
        )

    mode = "w" if not is_seed_in_schema or overwrite else "a"
    operation = "created" if mode == "w" else "appended"

    print(f"Registering seed [blue]{seed}[/blue]...")
    yaml_text = create_yaml(
        seeds=[seed],
        technical_owner=technical_owner,
        business_owner=business_owner,
        new=new,
        target=target,
    )

    if yaml_text:
        with open(yaml_path, mode) as file:
            file.write(yaml_text)

        successful_comment = f"[white]{yaml_path}[/white] file has been {operation} [green]successfully[/green] with [blue]{seeds_to_register}[/blue] information ."
        print(successful_comment)
        return True


if __name__ == "__main__":
    app()
