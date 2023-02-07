import functools
import os
import subprocess
from pathlib import Path

BASE_MODELS_SCHEMA = "conformed"


def call_shell(command: str) -> str:
    result = subprocess.check_output(command, shell=True).decode("utf-8")
    return result


def get_current_dbt_project_path():
    cwd = os.getcwd()
    while cwd != os.path.dirname(cwd):
        dbt_project_paths = [
            path for path in Path(cwd).rglob("*dbt_project.yml")
        ]
        if dbt_project_paths:
            break
        cwd = os.path.dirname(cwd)

    # The first path on the list is the dbt project closest to our current
    # working directory.
    dbt_project_path = dbt_project_paths[0]
    
    return Path(dbt_project_path).parent


DBT_PROJECT_DIR = get_current_dbt_project_path()


def run_in_dbt_project(func: callable, dbt_project_dir: Path = DBT_PROJECT_DIR) -> callable:
    """
    Decorates functions to change directory to a dbt project before running underlying function.
    """
    dbt_project_path = dbt_project_dir

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if not dbt_project_path:
            return False

        original_directory = os.getcwd()
        os.chdir(dbt_project_path)
        # Ensure we go back to original dir even if the function throws an exception.
        try:
            original_func_return_value = func(*args, **kwargs)
        except Exception as e:
            raise e
        finally:
            os.chdir(original_directory)
        return original_func_return_value

    return wrapper
