import functools
import os
import subprocess
from pathlib import Path

import oyaml as yaml

BASE_MODELS_SCHEMA = "conformed"

# fds gdghsgshdg
# zdfsdfsd fsdfsdfsdfsdf
def call_shell(command: str) -> str:
    result = subprocess.check_output(command, shell=True).decode("utf-8")
    return result
#zd dsffsdfsdfsdfsdfsd

def get_current_dbt_project_path():
    cwd = os.getcwd()
    while cwd != os.path.dirname(cwd):
        dbt_project_paths = [path for path in Path(cwd).rglob("*dbt_project.yml")]
        if dbt_project_paths:
            break
        cwd = os.path.dirname(cwd)

    # The first path on the list is the dbt project closest to our current
    # working directory.
    dbt_project_path = dbt_project_paths[0]

    return Path(dbt_project_path).parent


DBT_PROJECT_DIR = get_current_dbt_project_path()


def get_current_dbt_project() -> str:
    with open(DBT_PROJECT_DIR.joinpath("dbt_project.yml")) as f:
        config = yaml.safe_load(f)
    return config.get("name")


CURRENT_PROJECT_NAME = get_current_dbt_project()


def run_in_dbt_project(
    func: callable, dbt_project_dir: Path = DBT_PROJECT_DIR
) -> callable:
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


def get_current_dbt_target(profiles_dir: str = None) -> str:
    profiles_option = f"--profiles-dir {profiles_dir}" if profiles_dir else ""
    command = f"""dbt debug {profiles_option} | grep "profiles" | grep -o '/[^"]*'"""
    dbt_profiles_path = call_shell(command).strip()

    with open(dbt_profiles_path) as f:
        dbt_profiles = yaml.safe_load(f)

    current_project_profile = dbt_profiles.get(CURRENT_PROJECT_NAME, {})
    target = current_project_profile.get("target")

    if target is None:
        raise ValueError(
            f"Target not present in dbt profiles file at '{dbt_profiles_path}'"
        )

    return target


def get_current_dbt_schema(profiles_dir: str = None) -> str:
    profiles_option = f"--profiles-dir {profiles_dir}" if profiles_dir else ""
    command = f"""dbt debug {profiles_option} | grep "profiles" | grep -o '/[^"]*'"""
    dbt_profiles_path = call_shell(command).strip()

    with open(dbt_profiles_path) as f:
        dbt_profiles = yaml.safe_load(f)

    current_project_profile = dbt_profiles.get(CURRENT_PROJECT_NAME, {})
    target = current_project_profile.get("target")

    if target is None:
        raise ValueError(
            f"Target not present in dbt profiles file at '{dbt_profiles_path}'"
        )

    schema = current_project_profile["outputs"][target].get("schema")

    return schema
