import builtins
import shutil

import mock
import oyaml as yaml
import pytest
from getkey import key
from nesso.base_model import check_if_base_model_exists
from nesso.common import DBT_PROJECT_DIR
from nesso.source import add as add_source
from nesso.source import check_if_source_exists, check_if_source_table_exists
from nesso.source import create as create_source


@pytest.fixture(scope="session")
def SOURCE_PATH(TEST_SOURCE):
    yield DBT_PROJECT_DIR.joinpath("models", "sources", TEST_SOURCE, TEST_SOURCE + ".yml")

TEST_TABLE_CONTACT_BASE_MODEL = "stg_test_table_contact"
TEST_TABLE_ACCOUNT_BASE_MODEL = "stg_test_table_account"


def test_check_if_source_exists(SOURCE_PATH, TEST_SOURCE):

    assert not check_if_source_exists(TEST_SOURCE)
    
    SOURCE_PATH.parent.mkdir(parents=True, exist_ok=True)
    SOURCE_PATH.touch()

    assert check_if_source_exists(TEST_SOURCE)

    shutil.rmtree(
        DBT_PROJECT_DIR.joinpath("models", "sources", TEST_SOURCE), ignore_errors=True
    )


def test_check_if_source_table_exists(SOURCE_PATH, TEST_SOURCE, TEST_TABLE_CONTACT):

    assert not check_if_source_table_exists(TEST_SOURCE, TEST_TABLE_CONTACT)

    SOURCE_PATH.parent.mkdir(parents=True, exist_ok=True)

    source_yaml = {
        "version": 2,
        "sources": [{"name": TEST_SOURCE, "tables": [{"name": TEST_TABLE_CONTACT}]}],
    }
    with open(SOURCE_PATH, "w") as f:
        yaml.safe_dump(source_yaml, f)

    assert check_if_source_table_exists(TEST_SOURCE, TEST_TABLE_CONTACT)
    
    # Cleanup.
    shutil.rmtree(
        DBT_PROJECT_DIR.joinpath("models", "sources", TEST_SOURCE),
        ignore_errors=True,
    )


def test_source_create(postgres_connection, TEST_SOURCE, TEST_TABLE_CONTACT, TEST_TABLE_ACCOUNT):

    assert not check_if_source_exists(TEST_SOURCE)

    # Create a new source.
    with mock.patch.object(
        builtins,
        "input",
        lambda: key.ENTER,
    ):

        create_source(TEST_SOURCE)

    assert check_if_source_exists(TEST_SOURCE)

    # Cleanup
    shutil.rmtree(
        DBT_PROJECT_DIR.joinpath("models", "sources", TEST_SOURCE),
        ignore_errors=False,
    )
    shutil.rmtree(
        DBT_PROJECT_DIR.joinpath("models", "conformed"),
        ignore_errors=False,
    )

    postgres_connection.execute(f"DROP VIEW IF EXISTS {TEST_TABLE_CONTACT_BASE_MODEL};")
    postgres_connection.execute(f"DROP VIEW IF EXISTS {TEST_TABLE_ACCOUNT_BASE_MODEL};")


def test_source_add(postgres_connection, SOURCE_PATH, TEST_SOURCE, TEST_TABLE_ACCOUNT, TEST_TABLE_CONTACT):

    assert not check_if_source_exists(TEST_SOURCE)
    assert not check_if_source_table_exists(TEST_SOURCE, TEST_TABLE_ACCOUNT)
    assert not check_if_base_model_exists(TEST_TABLE_ACCOUNT_BASE_MODEL)

    # Create a source with only one of the two tables that exist in the database.
    SOURCE_PATH.parent.mkdir(parents=True, exist_ok=True)
    source_yaml =  {
        "version": 2,
        "sources": [
            {
                "name": TEST_SOURCE,
                "tables": [{"name": TEST_TABLE_CONTACT}]
            }
        ],
    }

    # This is just to have properly indented YAML
    # https://github.com/yaml/pyyaml/issues/234#issuecomment-765894586
    class CorrectIndentationDumper(yaml.Dumper):
        def increase_indent(self, flow=False, *args, **kwargs):
            return super().increase_indent(flow=flow, indentless=False)

    with open(SOURCE_PATH, "w") as f:
        yaml.dump(source_yaml, f, Dumper=CorrectIndentationDumper)

    # Add another table to the source.
    with mock.patch.object(
        builtins,
        "input",
        lambda: key.ENTER,
    ):

        add_source(
            TEST_SOURCE,
            TEST_TABLE_ACCOUNT,
            case_sensitive_cols=True,
        )

    assert check_if_source_exists(TEST_SOURCE)
    assert check_if_source_table_exists(TEST_SOURCE, TEST_TABLE_ACCOUNT)
    assert check_if_base_model_exists(TEST_TABLE_ACCOUNT_BASE_MODEL)

    # Cleanup.
    shutil.rmtree(
        DBT_PROJECT_DIR.joinpath("models", "sources", TEST_SOURCE),
        ignore_errors=True,
    )
    shutil.rmtree(
        DBT_PROJECT_DIR.joinpath("models", "conformed"),
        ignore_errors=True,
    )
    postgres_connection.execute(f"DROP VIEW IF EXISTS {TEST_TABLE_ACCOUNT_BASE_MODEL};")
    postgres_connection.execute(f"DROP TABLE IF EXISTS {TEST_TABLE_ACCOUNT};")
