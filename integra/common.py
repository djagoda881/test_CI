import os
import functools
import subprocess
from pathlib import Path
from rich import print

DBT_PROJECT_DIR = Path(__file__).resolve().parent.parent.parent
BASE_MODELS_SCHEMA = "conformed"


def call_shell(command):
    return subprocess.check_output(command, shell=True).decode("utf-8")


def run_in_dbt_project(func):
    original_directory = os.getcwd()

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        os.chdir(DBT_PROJECT_DIR)
        value = func(*args, **kwargs)
        return value

    os.chdir(original_directory)
    return wrapper
