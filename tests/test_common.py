import os
import shutil
from pathlib import Path

import oyaml as yaml

from nesso.common import (
    CURRENT_PROJECT_NAME,
    DBT_PROJECT_DIR,
    get_current_dbt_project,
    get_current_dbt_schema,
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

    postgres_project_dir = Path(__file__).parent.joinpath("dbt_projects", "postgres")
    os.chdir(postgres_project_dir)

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


def test_get_current_dbt_schema():
    # Assumptions.
    assert not FAKE_DBT_PROFILES_PATH.exists()

    FAKE_DBT_PROFILES_DIR.mkdir(parents=True, exist_ok=True)

    test_target = "test_target"
    test_schema = "test_schema"
    fake_profiles = {
        CURRENT_PROJECT_NAME: {
            "target": test_target,
            "outputs": {test_target: {"schema": test_schema}},
        }
    }
    with open(FAKE_DBT_PROFILES_PATH, "w") as f:
        yaml.safe_dump(fake_profiles, f)

    schema = get_current_dbt_schema(profiles_dir=FAKE_DBT_PROFILES_DIR)
    assert schema == test_schema

    # Cleanup.
    shutil.rmtree(FAKE_DBT_PROFILES_DIR, ignore_errors=True)
