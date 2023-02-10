import builtins
import shutil

import mock
from getkey import key
from nesso.base_model import check_if_base_model_exists
from nesso.base_model import create as create_base_model
from nesso.common import DBT_PROJECT_DIR
from nesso.source import check_if_source_exists, check_if_source_table_exists
from nesso.source import create as create_source


def test_base_model_create(
    postgres_connection,
    TEST_SOURCE,
    TEST_TABLE_CONTACT,
    TEST_TABLE_CONTACT_BASE_MODEL,
):

    # Assumptions.
    assert not check_if_source_exists(TEST_SOURCE)
    assert not check_if_source_table_exists(TEST_SOURCE, TEST_TABLE_CONTACT)
    assert not check_if_base_model_exists(TEST_TABLE_CONTACT_BASE_MODEL)

    # Create a source with the test table in it.
    with mock.patch.object(
        builtins,
        "input",
        lambda: key.ENTER,
    ):

        create_source(TEST_SOURCE, create_base_models=False)

    assert check_if_source_exists(TEST_SOURCE)
    assert check_if_source_table_exists(TEST_SOURCE, TEST_TABLE_CONTACT)

    create_base_model(source=TEST_SOURCE, source_table_name=TEST_TABLE_CONTACT)

    assert check_if_base_model_exists(TEST_TABLE_CONTACT_BASE_MODEL)

    # Cleanup.
    shutil.rmtree(
        DBT_PROJECT_DIR.joinpath("models", "sources", TEST_SOURCE),
        ignore_errors=True,
    )
    shutil.rmtree(
        DBT_PROJECT_DIR.joinpath("models", "conformed"),
        ignore_errors=True,
    )
    postgres_connection.execute(f"DROP VIEW IF EXISTS {TEST_TABLE_CONTACT_BASE_MODEL};")
