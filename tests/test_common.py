import os
import shutil
from pathlib import Path

import oyaml as yaml

from nesso.common import (
    CURRENT_PROJECT_NAME,
    DBT_PROJECT_DIR,
    get_current_dbt_project,
    get_current_dbt_target,
    run_in_dbt_project,
)

FAKE_DBT_PROFILES_DIR = Path("/tmp/path")
FAKE_DBT_PROFILES_PATH = FAKE_DBT_PROFILES_DIR.joinpath("profiles.yml")


def test_run_in_dbt_project():
    @run_in_dbt_project
    def check_if_in_dbt_project():
        cwd = os.getcwd()
        return str(cwd) == str(DBT_PROJECT_DIR)

    decorator_works = check_if_in_dbt_project()
    assert decorator_works


def test_get_current_dbt_project():
    working_dir = os.getcwd()
    os.chdir("/home/nesso/tests/dbt_projects/postgres")

    project = get_current_dbt_project()

    assert project == "postgres"

    # Cleanup.
    os.chdir(working_dir)


def test_get_current_dbt_target():
    # Assumptions.
    assert not FAKE_DBT_PROFILES_PATH.exists()

    FAKE_DBT_PROFILES_DIR.mkdir(parents=True, exist_ok=True)

    test_target = "test_target"
    fake_profiles = {CURRENT_PROJECT_NAME: {"target": test_target, "outputs": {}}}
    with open(FAKE_DBT_PROFILES_PATH, "w") as f:
        yaml.safe_dump(fake_profiles, f)

    target = get_current_dbt_target(profiles_dir=FAKE_DBT_PROFILES_DIR)
    assert target == test_target

    # Cleanup.
    shutil.rmtree(FAKE_DBT_PROFILES_DIR, ignore_errors=True)
