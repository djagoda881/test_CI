import os
import functools
import subprocess
from pathlib import Path
from rich import print

DBT_PROJECT_DIR = Path(__file__).resolve().parent.parent.parent
BASE_MODELS_SCHEMA = "conformed"


def call_shell(command):
    return subprocess.check_output(command, shell=True).decode("utf-8")


def find_dbt_project():

    cwd = os.getcwd()
    while cwd != os.path.dirname(cwd):
        dbt_project_in_cwd = "dbt_project.yml" in os.listdir(cwd)
        if dbt_project_in_cwd:
            dbt_project_path = cwd
            return dbt_project_path
        cwd = os.path.dirname(cwd)

    cwd = os.getcwd()
    while cwd != os.path.dirname(cwd):
        dbt_projects_under_cwd = [
            path for path in Path(cwd).rglob("dbt/lakehouse/dbt_project.yml")
        ]
        if dbt_projects_under_cwd:
            break
        cwd = os.path.dirname(cwd)

    if dbt_projects_under_cwd:
        if len(dbt_projects_under_cwd) > 1:
            for i, path in enumerate(dbt_projects_under_cwd):
                print(i + 1, path)
            dbt_project_selected = int(
                float(input("Type number of desired dbt project and press enter: "))
            )
            dbt_project_path = dbt_projects_under_cwd[dbt_project_selected + -1].parent
            return dbt_project_path

        else:
            dbt_project_path = dbt_projects_under_cwd[0].parent
            return dbt_project_path

    else:
        print("No dbt project available")
        dbt_project_path = False
        return dbt_project_path


def run_in_dbt_project(func):
    dbt_project_path = find_dbt_project()

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if not dbt_project_path:
            return False

        original_directory = os.getcwd()
        os.chdir(dbt_project_path)
        value = func(*args, **kwargs)
        os.chdir(original_directory)
        return value

    return wrapper


if __name__ == "__main__":
    pass
