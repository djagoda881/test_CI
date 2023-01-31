import os
import typer
from typing import List
import yaml
import pandas as pd
from rich import print
from rich.prompt import Prompt
from rich.prompt import Prompt
from pathlib import Path
from yaml.loader import SafeLoader
from .common import DBT_PROJECT_DIR, call_shell, run_in_dbt_project


app = typer.Typer()

DEFAULT_SEED_SCHEMA_PATH = DBT_PROJECT_DIR.joinpath(
    "seeds", "master_data", "schema.yml"
)


def get_excel_files(
    filename: str = None, seed_files_path: str = DEFAULT_SEED_SCHEMA_PATH.parent
) -> None:

    """
    Downloads all files not saved in .csv format from master_data for conversion to csv.

    Args:
        filename (str, optional): The name of the file to be converted, if nothing is specified, all files will be included. Defaults to None.
        seed_files_path (str, optional): Path to master data folder containing all seeds. Defaults to `DEFAULT_SEED_SCHEMA_PATH.parent`.

    """

    all_files_under_seeds_dir = os.listdir(seed_files_path)

    supported_extensions = (".xls", ".xlsx", ".xlsm", ".xlsb", ".odf", ".ods", ".odt")

    excel_files = [
        file
        for file in all_files_under_seeds_dir
        if file.endswith(supported_extensions)
    ]

    if filename is not None:
        if filename not in excel_files:
            return
        else:
            excel_files = [filename]

    for excel_file in excel_files:
        filename_without_file_extension = excel_file.split(".")[0]
        filename_with_csv_file_extension = (
            filename_without_file_extension.replace(" ", "_") + ".csv"
        )

        if filename_with_csv_file_extension not in all_files_under_seeds_dir:
            _excel_to_csv(seed_files_path, excel_file, filename_with_csv_file_extension)


def _excel_to_csv(seeds_dir, excel_file, filename_with_csv_file_extension) -> None:

    """
    Creates a csv file from the specified excel file

    Args:
        seeds_dir (str): Path to master data folder containing all seeds.
        excel_file (str): Name of the file to be converted.
        filename_with_csv_file_extension (str): The name to have for the csv file.

    """

    # Read excel, create df, transform df to csv and save
    df = pd.read_excel(os.path.join(seeds_dir, excel_file))

    df.columns = [f"{column.strip().replace(' ', '_')}" for column in df.columns]
    # Replacing invalid character(s) among " ,;{}()\n\t=" as these are forbidden in column names by Databricks
    df.columns = df.columns.str.replace("[,;{}()\n\t=]", "", regex=True)

    df.to_csv(os.path.join(seeds_dir, filename_with_csv_file_extension), index=False)
    print(
        f"Created [white]{excel_file}[/white] as a copy of [white]{filename_with_csv_file_extension}[/white] [green]successfully[/green]."
    )


def get_all_seeds(target: str = "qa") -> List[str]:
    """
    Runs 'dbt --resource-type seeds' to retrieve all seeds in project.

    Args:
        target (str, optional) The target to work with, options are ('qa', 'prod'). Defaults to qa.

    Returns:
        list: All seed names in your project.
    """
    seed_paths = (
        call_shell(f"""dbt ls --resource-type seed --target {target}""")
        .strip()
        .split("\n")
    )
    seed_paths = [
        seed_name
        for seed_name in seed_paths
        if " " not in seed_name and len(seed_name) > 0
    ]
    seed_names = [seed_path.split(".")[-1] for seed_path in seed_paths]
    return seed_names


def check_seed_exists(seed_name: str) -> bool:

    all_seeds = get_all_seeds()
    return seed_name in all_seeds


def check_seed_in_yaml(
    seed_name: str, yaml_path: str = DEFAULT_SEED_SCHEMA_PATH
) -> bool:
    """
    Checks if a seed name is inside the schema YAML file.

    Args:
        seed_name (str) The name of the seed.
        yaml_path (str, Optional) The path to the file with seed YAML schema.  Defaults to `DEFAULT_SEED_SCHEMA_PATH` variable.

    Returns:
        If the seed name provided to the function is in schema YAML file it returns True, otherwise it returns False
    """
    if not yaml_path.is_file():
        return False

    with open(yaml_path) as f:
        seeds_in_yaml = yaml.load(f, Loader=SafeLoader)["seeds"]
        return any(
            [seed.get("name").lower() == seed_name.lower() for seed in seeds_in_yaml]
        )


def get_seeds_to_register(yaml_path) -> List[str]:
    "Returns list of seeds to be registered"
    seeds_to_register = []

    all_seeds = get_all_seeds()
    for seed in all_seeds:
        if not seed:
            continue
        if not check_seed_in_yaml(seed, yaml_path=yaml_path):
            seeds_to_register.append(seed)

    return seeds_to_register


def create_yaml(
    seeds: List[str],
    technical_owner: str,
    business_owner: str,
    new: str = False,
    target: str = "qa",
    case_sensitive_cols: bool = True,
) -> bool:

    """
    Returns text of YAML file with selected seeds information in it.

    Args:
        seeds (List[str]): The name of the seed.
        technical_owner (str, Optional): The technical owner of the table.
        business_owner (str, Optional): The business owner of the table.
        new (bool, Optional): Whether to include headers that are needed when creating a new yaml. Defaults to False.
        target (str, Optional): The target to work with, options are ('qa', 'prod'). Defaults to qa.
        case_sensitive_cols (bool, optional): Determine if a given database type is case-sensitive. Defaults to True.

    Returns:
        yaml_text (str): String with yaml formatting containing seeds information
    """

    if not seeds:
        print("No seeds to append to YAML file")
        return False

    generate_yaml_text_command = f"""dbt -q run-operation --target {target} create_seed_yaml_text --args '{{"seed_names": [{', '.join(seeds)}], "technical_owner":{technical_owner}, "business_owner":{business_owner}, "new": {new}, "case_sensitive_cols":{case_sensitive_cols}}}'"""
    yaml_text = call_shell(generate_yaml_text_command)
    return yaml_text


@app.command()
@run_in_dbt_project
def register(
    seed: str = typer.Argument(None, help="The name of the seed to register."),
    technical_owner: str = typer.Option(
        None, "--technical-owner", help="The technical owner of the table."
    ),
    business_owner: str = typer.Option(
        None, "--business-owner", help="The business owner of the table."
    ),
    overwrite: bool = typer.Option(
        False,
        "--overwrite",
        "-o",
        help="Whether to overwrite the schema YAML.",
    ),
    yaml_path: str = typer.Option(
        DEFAULT_SEED_SCHEMA_PATH,
        "--yaml-path",
        help="The absolute path of the YAML file to which to append seed information, default path is DBT_PROJECT_DIR/seeds/aster_data/schema.yml",
    ),
    target: str = typer.Option(
        "qa",
        "--target",
        "-t",
        help="the target to work with, options are ('qa', 'prod') default is qa",
    ),
) -> bool:
    """
    Registers seeds in dbt and seeds information in schema YAML file under yaml_path

    Args:
        seed (str, Optional): The name of the seed to register, if not specified all seeds will be registered.
        technical_owner (str): The technical owner of the table.
        business_owner (str): The business owner of the table.
        overwrite (bool, Optional): Whether to overwrite schema YAML file with seed information if seed is already present in schema YAML file.
        yaml_path (str, Optional): The absolute path of the YAML file to append seed information. Default path is DBT_PROJECT_DIR/seeds/aster_data/schema.yml.
        target (str, Optional): The target to work with, options are ('qa', 'prod'). Defaults to qa.
    """

    # if yaml_path is a string (user input), convert it to a Path object
    if isinstance(yaml_path, str):
        yaml_path = Path(yaml_path)

    get_excel_files(filename=seed)

    # If no seed was passed, register all seeds
    if not seed:
        print("Uploading all available seeds in this project to dbt.")
        print("[white]This may take some time[/white], [green]do not worry[/green]")
        call_shell(f"dbt seed --target {target}")
    else:
        seed_exists = check_seed_exists(seed)
        if not seed_exists:
            print(f"Atempting to upload seed {seed} to dbt.")
            print("[white]This may take some time[/white], [green]do not worry[/green]")
            call_shell(f"dbt seed --target {target} -s {seed}")
        seed_exists = check_seed_exists(seed)
        if not seed_exists:
            print(f"Seed [b]{seed}[/b] does not exist. Skipping...")
            return

    yaml_exists = yaml_path.is_file()

    if yaml_exists and seed:
        seed_in_yaml = check_seed_in_yaml(seed, yaml_path=yaml_path)
        if seed_in_yaml:
            print(
                f"Seed information of [blue]{seed}[/blue] [b]already exists[/b] on the YAML file. Skipping..."
            )

    new = bool(not yaml_exists or overwrite)
    mode = "w" if new else "a"
    operation = "created" if new else "appended"
    if not seed:
        seeds_to_register: List[str] = get_seeds_to_register(yaml_path=yaml_path)

    else:
        # We do this because create_yaml() expects a list of seeds
        seeds_to_register = [seed]

    if technical_owner is None:
        technical_owner = Prompt.ask(
            f"Please provide a [white]technical owner[/white] for seed(s) [blue]{seed}[/blue] and then press [green]ENTER[/green]"
        )
    if business_owner is None:
        business_owner = Prompt.ask(
            f"Please provide a [white]business owner[/white] for seed(s) [blue]{seed}[/blue] and then press [green]ENTER[/green]"
        )

    print(f"Seeds to register: {seeds_to_register}")
    yaml_text: str = create_yaml(
        seeds=seeds_to_register,
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
