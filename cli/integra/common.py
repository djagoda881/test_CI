import os
import functools
import subprocess
from pathlib import Path
from rich import print
import typing

DBT_PROJECT_DIR = Path(__file__).resolve().parent.parent.parent
BASE_MODELS_SCHEMA = "conformed"


def call_shell(command):
    result: str = subprocess.check_output(command, shell=True).decode("utf-8")
    return result


def find_dbt_project():
    """
    Finds dbt projects available in current, parent, and child directories.

    returns:
        dbt_proyect_path (str): Absolute path to a dbt proyect.
    """
    # Recursive search for "dbt_project.yml" under current directory and parent directories
    cwd: str = os.getcwd()
    while cwd != os.path.dirname(cwd):
        is_dbt_project_under_cwd: bool = "dbt_project.yml" in os.listdir(cwd)
        if is_dbt_project_under_cwd:
            dbt_project_path = cwd
            return dbt_project_path
        cwd: str = os.path.dirname(cwd)

    # Recursive search for "dbt/*/dbt_project.yml" structure under current directory and parent directories
    cwd: str = os.getcwd()
    while cwd != os.path.dirname(cwd):
        dbt_projects_under_cwd: list = [
            path for path in Path(cwd).rglob("dbt/*/dbt_project.yml")
        ]
        if dbt_projects_under_cwd:
            break
        cwd: str = os.path.dirname(cwd)

    # If more than one "dbt/*/dbt_project.yml" choose between available projects
    if dbt_projects_under_cwd:
        if len(dbt_projects_under_cwd) > 1:
            for i, path in enumerate(dbt_projects_under_cwd):
                print(i + 1, path)
            dbt_project_selected: int = int(
                input("Type number of desired dbt project and press enter: ")
            )

            dbt_project_path: str = dbt_projects_under_cwd[
                dbt_project_selected - 1
            ].parent
            return dbt_project_path

        else:
            dbt_project_path: str = dbt_projects_under_cwd[0].parent
            return dbt_project_path

    else:
        print("No dbt projects available.")
        dbt_project_path: str = None
        return dbt_project_path


def run_in_dbt_project(func: callable) -> callable:
    """
    Decorates functions to change directory to a dbt project before running underlying function.
    """
    dbt_project_path: str = find_dbt_project()

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if not dbt_project_path:
            return False

        original_directory: str = os.getcwd()
        os.chdir(dbt_project_path)
        value = func(*args, **kwargs)
        os.chdir(original_directory)
        return value

    return wrapper


if __name__ == "__main__":
    pass
