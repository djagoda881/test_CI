import builtins
import os
import shutil

import mock
from getkey import key
from nesso.base_model import check_if_base_model_exists
from nesso.common import DBT_PROJECT_DIR
from nesso.model import bootstrap, bootstrap_yaml
from nesso.source import check_if_source_exists
from nesso.source import create as create_source


TEST_SOURCE = "public"
TEST_TABLE_CONTACT_BASE_MODEL = "stg_test_table_contact"
TEST_TABLE_ACCOUNT_BASE_MODEL = "stg_test_table_account"

MART = "unit_test"
PROJECT = "unit_test"
MODEL = "test_model"

MODEL_PATH = DBT_PROJECT_DIR.joinpath(
    "models", "marts", MART, PROJECT, MODEL, MODEL + ".sql"
)
MODEL_YAML_PATH = DBT_PROJECT_DIR.joinpath(
    "models", "marts", MART, PROJECT, MODEL, MODEL + ".yml"
)


def test_model_bootstrap():

    assert not MODEL_PATH.exists()

    bootstrap(MODEL, MART, PROJECT)

    assert MODEL_PATH.exists()

    # Cleaning up after the test
    MODEL_PATH.unlink()


def test_model_bootstrap_yaml(postgres_connection, create_data_contacts_table):

    assert not check_if_source_exists(TEST_SOURCE)
    assert not check_if_base_model_exists(TEST_TABLE_CONTACT_BASE_MODEL)
    assert not MODEL_YAML_PATH.exists()

    # Creating Source
    with mock.patch.object(
        builtins,
        "input",
        lambda: key.ENTER,
    ):

        create_source(TEST_SOURCE)

    # Creating model
    bootstrap(MODEL, MART, PROJECT)

    with open(MODEL_PATH, "a") as f:
        f.write("select * from {{ " + "ref( 'stg_" + TEST_TABLE_CONTACT + "' )" + " }}")

    os.system(f"dbt run -m {MODEL}")

    # Creating model.yml file
    bootstrap_yaml(
        MODEL, MART, PROJECT, "test_technical_owner", "test_business_owner", "qa"
    )

    assert MODEL_YAML_PATH.exists()

    # Cleaning up after the test
    engine.execute(f"DROP VIEW IF EXISTS {MODEL};")
    engine.execute(f"DROP VIEW IF EXISTS {TEST_TABLE_CONTACT_BASE_MODEL};")
    engine.execute(f"DROP TABLE IF EXISTS {TEST_TABLE_CONTACT};")
    shutil.rmtree(
        DBT_PROJECT_DIR.joinpath("models", "sources", TEST_SOURCE),
        ignore_errors=True,
    )
    shutil.rmtree(
        DBT_PROJECT_DIR.joinpath("models", "conformed"),
        ignore_errors=True,
    )
    shutil.rmtree(
        DBT_PROJECT_DIR.joinpath("models", "marts", MART),
        ignore_errors=True,
    )