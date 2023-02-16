import builtins
import os
import shutil

import mock
from getkey import key

from nesso.base_model import check_if_base_model_exists
from nesso.common import BASE_MODELS_SCHEMA, DBT_PROJECT_DIR
from nesso.model import bootstrap, bootstrap_yaml
from nesso.source import check_if_source_exists
from nesso.source import create as create_source


def test_model_bootstrap(MODEL, MODEL_PATH, MART, PROJECT):

    assert not MODEL_PATH.exists()

    bootstrap(MODEL, mart=MART, project=PROJECT)

    assert MODEL_PATH.exists()

    # Cleaning up after the test
    MODEL_PATH.unlink()


def test_model_bootstrap_yaml(
    postgres_connection,
    MODEL,
    MODEL_PATH,
    MODEL_YAML_PATH,
    MART,
    PROJECT,
    TEST_SOURCE,
    TEST_TABLE_CONTACT,
    TEST_TABLE_CONTACT_BASE_MODEL,
):

    assert not check_if_source_exists(TEST_SOURCE)
    assert not check_if_base_model_exists(TEST_TABLE_CONTACT_BASE_MODEL)
    assert not MODEL_YAML_PATH.exists()

    # Create the source and register sources & base models.
    with mock.patch.object(
        builtins,
        "input",
        lambda: key.ENTER,
    ):

        create_source(TEST_SOURCE, project=DBT_PROJECT_DIR.name)

    # Materialize the model.
    bootstrap(MODEL, mart=MART, project=PROJECT)

    with open(MODEL_PATH, "a") as f:
        f.write("select * from {{ " + "ref( 'stg_" + TEST_TABLE_CONTACT + "' )" + " }}")

    os.system(f"dbt run -m {MODEL}")

    # Bootstrap YAML for the model.
    bootstrap_yaml(
        model=MODEL,
        mart=MART,
        project=PROJECT,
        technical_owner="test_technical_owner",
        business_owner="test_business_owner",
        target="qa",
    )

    assert MODEL_YAML_PATH.exists()

    # Cleanup.
    postgres_connection.execute(f"DROP VIEW IF EXISTS {MODEL};")
    postgres_connection.execute(f"DROP VIEW IF EXISTS {TEST_TABLE_CONTACT_BASE_MODEL};")

    shutil.rmtree(
        DBT_PROJECT_DIR.joinpath("models", "sources", TEST_SOURCE),
        ignore_errors=True,
    )
    shutil.rmtree(
        DBT_PROJECT_DIR.joinpath("models", BASE_MODELS_SCHEMA),
        ignore_errors=True,
    )
    shutil.rmtree(
        DBT_PROJECT_DIR.joinpath("models", "marts"),
        ignore_errors=True,
    )
