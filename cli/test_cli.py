import pandas as pd
import pytest
import yaml
import random
import pdb
import os
import shutil
import builtins
import mock
from getkey import key

from faker import Faker
from datetime import datetime
from pydantic import BaseModel, Field

from integra.base_model import check_if_base_model_exists, create
from integra.model import bootstrap, bootstrap_yaml
from integra.common import DBT_PROJECT_DIR, run_in_dbt_project
from integra.source import check_if_source_exists, check_if_source_table_exists
from integra.source import create as create_source, add as add_source
from integra.seed import register
from integra.seed import (
    check_seed_in_yaml,
    get_all_seeds,
)
from sqlalchemy import create_engine

fake = Faker()

TEST_SOURCE = "public"
TEST_TABLE_CONTACT = "test_table_contact"
TEST_TABLE_CONTACT_BASE_MODEL = "stg_test_table_contact"
TEST_TABLE_ACCOUNT = "test_table_account"
TEST_TABLE_ACCOUNT_BASE_MODEL = "stg_test_table_account"
TEST_SEED_AVG_NAME = "average_salary_test"
TEST_SEED_COUNTRIES_NAME = "countries_example"
MART = "unit_test"
PROJECT = "unit_test"
MODEL = "test_model"

NROWS = 100

SOURCE_PATH = DBT_PROJECT_DIR.joinpath(
    "models", "sources", TEST_SOURCE, TEST_SOURCE + ".yml"
)
MODEL_PATH = DBT_PROJECT_DIR.joinpath(
    "models", "marts", MART, PROJECT, MODEL, MODEL + ".sql"
)
MODEL_YAML_PATH = DBT_PROJECT_DIR.joinpath(
    "models", "marts", MART, PROJECT, MODEL, MODEL + ".yml"
)
DEFAULT_SEED_SCHEMA_PATH = DBT_PROJECT_DIR.joinpath(
    "seeds", "master_data", "schema.yml"
)
TEST_SEED_AVG_PATH = DEFAULT_SEED_SCHEMA_PATH.parent.joinpath(
    f"{TEST_SEED_AVG_NAME}.csv"
)
TEST_SEED_COUNTRIES_PATH = DEFAULT_SEED_SCHEMA_PATH.parent.joinpath(
    f"{TEST_SEED_COUNTRIES_NAME}.csv"
)

engine = create_engine("postgresql://user:password@postgres:5432/db")


@pytest.fixture()
def create_data_contacts_table():
    class Contact(BaseModel):
        Id: str = Field(default_factory=lambda: i)
        AccountId: str = Field(default_factory=lambda: random.randint(1, NROWS))
        FirstName: str = Field(default_factory=fake.first_name)
        LastName: str = Field(default_factory=fake.last_name)
        ContactEMail: str = Field(default_factory=fake.email)
        MailingCity: str = Field(default_factory=fake.city)
        Country: str = Field(default_factory=fake.country)
        # we need to use alias as pydantic doesn't support fields starting with an underscore
        viadot_downloaded_at_utc: datetime = Field(
            default_factory=datetime.utcnow, alias="_viadot_downloaded_at_utc"
        )

    contacts = []

    for i in range(1, NROWS + 1):
        contacts.append(Contact(Id=i).dict(by_alias=True))
    contacts_df_pandas = pd.DataFrame(contacts)

    contacts_df_pandas.to_sql(
        TEST_TABLE_CONTACT, engine, if_exists="replace", index=False
    )


def add_data_accouts_table():
    class Account(BaseModel):
        id: str = Field(default_factory=lambda: i)
        name: str = Field(default_factory=fake.company)
        email: str = Field(default_factory=fake.email)
        mobile: str = Field(default_factory=fake.phone_number)
        country: str = Field(default_factory=fake.country)
        city: str = Field(default_factory=fake.city)
        # we need to use alias as pydantic doesn't support fields starting with an underscore
        viadot_downloaded_at_utc: datetime = Field(
            default_factory=datetime.utcnow, alias="_viadot_downloaded_at_utc"
        )

    accounts = []

    for i in range(1, NROWS + 1):
        accounts.append(Account(id=i).dict(by_alias=True))
    accounts_df_pandas = pd.DataFrame(accounts)

    accounts_df_pandas.to_sql(
        TEST_TABLE_ACCOUNT, engine, if_exists="replace", index=False
    )


@pytest.fixture(scope="session", autouse=True)
def clean_up_project(request):
    def cleanup():
        engine.execute(f"DROP TABLE IF EXISTS {TEST_SEED_COUNTRIES_NAME};")
        engine.execute(f"DROP TABLE IF EXISTS {TEST_SEED_AVG_NAME};")
        engine.execute(f"DROP VIEW IF EXISTS {MODEL};")
        engine.execute(f"DROP VIEW IF EXISTS {TEST_TABLE_ACCOUNT_BASE_MODEL};")
        engine.execute(f"DROP VIEW IF EXISTS {TEST_TABLE_CONTACT_BASE_MODEL};")
        engine.execute(f"DROP TABLE IF EXISTS {TEST_TABLE_CONTACT};")
        engine.execute(f"DROP TABLE IF EXISTS {TEST_TABLE_ACCOUNT};")

        TEST_SEED_AVG_PATH.unlink(missing_ok=True)
        TEST_SEED_COUNTRIES_PATH.unlink(missing_ok=True)
        DEFAULT_SEED_SCHEMA_PATH.unlink(missing_ok=True)

        shutil.rmtree(
            DBT_PROJECT_DIR.joinpath("models", "marts", MART),
            ignore_errors=True,
        )

        shutil.rmtree(
            DBT_PROJECT_DIR.joinpath("models", "sources", TEST_SOURCE),
            ignore_errors=True,
        )

        shutil.rmtree(
            DBT_PROJECT_DIR.joinpath("models", "conformed"),
            ignore_errors=True,
        )
        engine.dispose()

    yield
    cleanup()


# --------------------------- START INTEGRA COMMON COMMAND TESTING --------------------------- #


def test_run_in_dbt_project():
    @run_in_dbt_project
    def check_if_in_dbt_project():
        cwd = os.getcwd()
        return str(cwd) == str(DBT_PROJECT_DIR)

    decorator_works = check_if_in_dbt_project()
    assert decorator_works


# --------------------------- END INTEGRA COMMON COMMAND TESTING --------------------------- #

# ------------------------------- START INTEGRA SOURCE TESTING ----------------------------- #


def test_check_if_source_exists():

    assert not check_if_source_exists(TEST_SOURCE)

    SOURCE_PATH.parent.mkdir(parents=True, exist_ok=True)
    SOURCE_PATH.touch()

    assert check_if_source_exists(TEST_SOURCE)

    shutil.rmtree(
        DBT_PROJECT_DIR.joinpath("models", "sources", TEST_SOURCE), ignore_errors=True
    )


def test_check_if_source_table_exists():

    assert not check_if_source_table_exists(TEST_SOURCE, TEST_TABLE_CONTACT)

    SOURCE_PATH.parent.mkdir(parents=True, exist_ok=True)

    source_yaml = {
        "version": 2,
        "sources": [{"name": TEST_SOURCE, "tables": [{"name": TEST_TABLE_CONTACT}]}],
    }
    with open(SOURCE_PATH, "w") as f:
        yaml.dump(source_yaml, f)

    assert check_if_source_table_exists(TEST_SOURCE, TEST_TABLE_CONTACT)
    shutil.rmtree(
        DBT_PROJECT_DIR.joinpath("models", "sources", TEST_SOURCE),
        ignore_errors=True,
    )


def test_source_create(create_data_contacts_table):

    assert not check_if_source_exists(TEST_SOURCE)

    # Creating Source
    with mock.patch.object(
        builtins,
        "input",
        lambda: key.ENTER,
    ):

        create_source(TEST_SOURCE)

    assert check_if_source_exists(TEST_SOURCE)

    # Cleaning up after the test
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


def test_base_model_create(create_data_contacts_table):

    assert not check_if_source_exists(TEST_SOURCE)
    assert not check_if_base_model_exists(TEST_TABLE_ACCOUNT_BASE_MODEL)

    # Creating Source
    with mock.patch.object(
        builtins,
        "input",
        lambda: key.ENTER,
    ):

        create_source(TEST_SOURCE)

    assert check_if_base_model_exists(TEST_TABLE_CONTACT_BASE_MODEL)
    assert check_if_source_exists(TEST_SOURCE)

    # Cleaning up after the test
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


def test_source_add(create_data_contacts_table):

    assert not check_if_source_exists(TEST_SOURCE)
    assert not check_if_source_table_exists(TEST_SOURCE, TEST_TABLE_ACCOUNT)
    assert not check_if_base_model_exists(TEST_TABLE_ACCOUNT_BASE_MODEL)

    # Creating source
    with mock.patch.object(
        builtins,
        "input",
        lambda: key.ENTER,
    ):

        create_source(TEST_SOURCE)

    # Adding table to database
    add_data_accouts_table()

    # Adding table to source
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

    assert check_if_source_table_exists(TEST_SOURCE, TEST_TABLE_ACCOUNT)
    assert check_if_base_model_exists(TEST_TABLE_ACCOUNT_BASE_MODEL)
    assert check_if_source_exists(TEST_SOURCE)

    # Cleaning up after the test
    engine.execute(f"DROP VIEW IF EXISTS {TEST_TABLE_ACCOUNT_BASE_MODEL};")
    engine.execute(f"DROP VIEW IF EXISTS {TEST_TABLE_CONTACT_BASE_MODEL};")
    engine.execute(f"DROP TABLE IF EXISTS {TEST_TABLE_ACCOUNT};")
    engine.execute(f"DROP TABLE IF EXISTS {TEST_TABLE_CONTACT};")
    shutil.rmtree(
        DBT_PROJECT_DIR.joinpath("models", "sources", TEST_SOURCE),
        ignore_errors=True,
    )
    shutil.rmtree(
        DBT_PROJECT_DIR.joinpath("models", "conformed"),
        ignore_errors=True,
    )


# ----------------------------- START INTEGRA MODEL TESTING ---------------------------- #


def test_model_bootstrap():

    assert not MODEL_PATH.exists()

    bootstrap(MODEL, MART, PROJECT)

    assert MODEL_PATH.exists()

    # Cleaning up after the test
    MODEL_PATH.unlink()


def test_model_bootstrap_yaml(create_data_contacts_table):

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


# --------------------------- END INTEGRA MODEL COMMAND TESTING --------------------------- #

# ----------------------------- START INTEGRA SEED TESTING ---------------------------- #


def test_create_all(yaml_path: str = DEFAULT_SEED_SCHEMA_PATH):

    # check yaml does not exist
    assert not yaml_path.is_file()

    # create yaml with all seeds information
    registered = register(
        seed=None,
        overwrite=False,
        yaml_path=yaml_path,
        target="qa",
        technical_owner="test_technical_owner",
        business_owner="test_business_owner",
    )
    assert registered is True

    # check if yaml file exists
    assert yaml_path.is_file()

    # check if one by one if every seed info is inside yaml file
    for seed in get_all_seeds(target="qa"):
        assert check_seed_in_yaml(seed, yaml_path=yaml_path)

    # Cleaning up after the test
    yaml_path.unlink()
    TEST_SEED_AVG_PATH.unlink(missing_ok=True)

    engine.execute(f"DROP TABLE IF EXISTS {TEST_SEED_AVG_NAME};")


def test_update_all(yaml_path: str = DEFAULT_SEED_SCHEMA_PATH):

    # check yaml does not exist
    assert not yaml_path.is_file()

    # create yaml with all seeds information
    registered = register(
        seed=None,
        overwrite=False,
        yaml_path=yaml_path,
        target="qa",
        technical_owner="test_technical_owner",
        business_owner="test_business_owner",
    )
    assert registered is True

    # check if yaml file exists
    assert yaml_path.is_file()

    try:
        shutil.copyfile(
            DBT_PROJECT_DIR.joinpath(f"{TEST_SEED_COUNTRIES_NAME}.csv"),
            DEFAULT_SEED_SCHEMA_PATH.parent.joinpath(f"{TEST_SEED_COUNTRIES_NAME}.csv"),
        )
    except:
        assert False, "The file was not copied"

    # Update yaml with all new seeds information
    registered = register(
        seed=None,
        overwrite=False,
        yaml_path=yaml_path,
        target="qa",
        technical_owner="test_technical_owner",
        business_owner="test_business_owner",
    )
    assert registered is True

    # check one by one if every seed info is inside yaml file
    for seed in get_all_seeds(target="qa"):
        assert check_seed_in_yaml(seed, yaml_path=yaml_path)

    # Cleaning up after the test
    TEST_SEED_AVG_PATH.unlink(missing_ok=True)
    TEST_SEED_COUNTRIES_PATH.unlink(missing_ok=True)
    DEFAULT_SEED_SCHEMA_PATH.unlink(missing_ok=True)

    engine.execute(f"DROP TABLE IF EXISTS {TEST_SEED_COUNTRIES_NAME};")
    engine.execute(f"DROP TABLE IF EXISTS {TEST_SEED_AVG_NAME};")


# --------------------------- END INTEGRA SEED COMMAND TESTING --------------------------- #
