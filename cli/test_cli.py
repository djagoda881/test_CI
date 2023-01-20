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
from pathlib import Path

from integra.base_model import check_if_base_model_exists, create
from integra.model import bootstrap, bootstrap_yaml
from integra.common import DBT_PROJECT_DIR, BASE_MODELS_SCHEMA, run_in_dbt_project
from integra.source import check_if_source_exists, check_if_source_table_exists
from integra.source import create as create_source, add as add_source
from integra.seed import create_yaml, register
from integra.seed import (
    check_seed_in_yaml,
    get_all_seeds,
)
from sqlalchemy import create_engine, text

fake = Faker()

TEST_SOURCE = "public"
TEST_TABLE_CONTACTS = "test_table_contacts"
TEST_TABLE_ACCOUNTS = "test_table_accounts"
MART = "unit_test"
PROJECT = "unit_test"
MODEL = "test_model"

nrows = 100

sql_path = DBT_PROJECT_DIR.joinpath(
    "models", BASE_MODELS_SCHEMA, TEST_SOURCE, TEST_TABLE_CONTACTS + ".sql"
)
yml_path = DBT_PROJECT_DIR.joinpath(
    "models", BASE_MODELS_SCHEMA, TEST_SOURCE, TEST_TABLE_CONTACTS + ".yml"
)
source_path = DBT_PROJECT_DIR.joinpath(
    "models", "sources", TEST_SOURCE, TEST_SOURCE + ".yml"
)
model_path = DBT_PROJECT_DIR.joinpath(
    "models", "marts", MART, PROJECT, MODEL, MODEL + ".sql"
)
model_yaml_path = DBT_PROJECT_DIR.joinpath(
    "models", "marts", MART, PROJECT, MODEL, MODEL + ".yml"
)

engine = create_engine("postgresql://user:password@postgres:5432/db")


@pytest.fixture()
def create_data_contacts_table():
    class Contact(BaseModel):
        Id: str = Field(default_factory=lambda: i)
        AccountId: str = Field(default_factory=lambda: random.randint(1, nrows))
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

    for i in range(1, nrows + 1):
        contacts.append(Contact(Id=i).dict(by_alias=True))
    contacts_df_pandas = pd.DataFrame(contacts)

    contacts_df_pandas.to_sql(
        TEST_TABLE_CONTACTS, engine, if_exists="replace", index=False
    )


@pytest.fixture()
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

    for i in range(1, nrows + 1):
        accounts.append(Account(id=i).dict(by_alias=True))
    accounts_df_pandas = pd.DataFrame(accounts)

    accounts_df_pandas.to_sql(
        TEST_TABLE_ACCOUNTS, engine, if_exists="replace", index=False
    )

    yield

    engine.execute(f"DROP TABLE IF EXISTS {TEST_TABLE_ACCOUNTS};")


@pytest.fixture()
def clean_up_project():

    engine.execute("DROP TABLE IF EXISTS countries;")
    engine.execute(f"DROP VIEW IF EXISTS {MODEL};")
    engine.execute(f"DROP VIEW IF EXISTS stg_{TEST_TABLE_CONTACTS};")
    engine.execute(f"DROP TABLE IF EXISTS {TEST_TABLE_CONTACTS};")

    shutil.rmtree(
        DBT_PROJECT_DIR.joinpath("models", "marts", MART),
        ignore_errors=True,
    )

    shutil.rmtree(
        DBT_PROJECT_DIR.joinpath("models", "sources", TEST_SOURCE),
        ignore_errors=True,
    )

    shutil.rmtree(
        DBT_PROJECT_DIR.joinpath("models", "conformed", TEST_SOURCE),
        ignore_errors=True,
    )


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


def test_check_if_source_exists(clean_up_project):

    assert not check_if_source_exists(TEST_SOURCE)

    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.touch()

    assert check_if_source_exists(TEST_SOURCE)

    shutil.rmtree(
        DBT_PROJECT_DIR.joinpath("models", "sources", TEST_SOURCE), ignore_errors=True
    )


def test_check_if_source_table_exists():

    assert not check_if_source_table_exists(TEST_SOURCE, TEST_TABLE_CONTACTS)

    source_path.parent.mkdir(parents=True, exist_ok=True)

    source_yaml = {
        "version": 2,
        "sources": [{"name": TEST_SOURCE, "tables": [{"name": TEST_TABLE_CONTACTS}]}],
    }
    with open(source_path, "w") as f:
        yaml.dump(source_yaml, f)

    assert check_if_source_table_exists(TEST_SOURCE, TEST_TABLE_CONTACTS)
    shutil.rmtree(
        DBT_PROJECT_DIR.joinpath("models", "sources", TEST_SOURCE),
        ignore_errors=True,
    )


def test_source_create(create_data_contacts_table):

    assert not check_if_source_exists(TEST_SOURCE)

    create_source(TEST_SOURCE)

    assert check_if_source_exists(TEST_SOURCE)


def test_base_model_create():

    create(TEST_SOURCE, TEST_TABLE_CONTACTS)
    os.system(f"dbt run -m stg_{TEST_TABLE_CONTACTS}")

    assert check_if_base_model_exists(TEST_SOURCE, TEST_TABLE_CONTACTS)


def test_source_add(add_data_accouts_table):

    assert not check_if_source_table_exists(TEST_SOURCE, TEST_TABLE_ACCOUNTS)
    assert not check_if_base_model_exists(TEST_SOURCE, TEST_TABLE_ACCOUNTS)

    with mock.patch.object(
        builtins,
        "input",
        lambda: key.ENTER,
    ):

        add_source(
            TEST_SOURCE,
            TEST_TABLE_ACCOUNTS,
            project="postgres",
            case_sensitive_cols=True,
        )

    assert check_if_source_table_exists(TEST_SOURCE, TEST_TABLE_ACCOUNTS)
    assert check_if_base_model_exists(TEST_SOURCE, TEST_TABLE_ACCOUNTS)


# ----------------------------- START INTEGRA MODEL TESTING ---------------------------- #


def test_model_bootstrap():

    assert not model_path.exists()

    bootstrap(MODEL, MART, PROJECT)

    assert model_path.exists()


def test_model_bootstrap_yaml():

    assert not model_yaml_path.exists()

    with open(model_path, "a") as f:
        f.write(
            "select * from {{ " + "ref( 'stg_" + TEST_TABLE_CONTACTS + "' )" + " }}"
        )

    os.system(f"dbt run -m {MODEL}")
    bootstrap_yaml(MODEL, MART, PROJECT, "test", "test", "qa")

    assert model_path.exists()


# --------------------------- END INTEGRA MODEL COMMAND TESTING --------------------------- #

# ----------------------------- START INTEGRA SEED TESTING ---------------------------- #

DEFAULT_SEED_SCHEMA_PATH = Path(__file__).resolve().parent / "test.yml"


def test_create_all(yaml_path: str = DEFAULT_SEED_SCHEMA_PATH):

    # Delete yaml_path if exists to ensure reproducible behaviour of test
    try:
        yaml_path.unlink()
    except FileNotFoundError:
        pass

    # check yaml does not exist
    assert not yaml_path.is_file()

    # create yaml with all seeds information
    registered = register(
        seed=None,
        overwrite=False,
        yaml_path=yaml_path,
        target="qa",
        technical_owner="test",
        business_owner="test",
    )
    assert registered is True

    # check if yaml file exists
    assert yaml_path.is_file()

    # check if one by one if every seed info is inside yaml file
    for seed in get_all_seeds(target="qa"):
        assert check_seed_in_yaml(seed, yaml_path=yaml_path)
    # Delete yaml_path if exists to ensure reproducible behaviour of test
    try:
        yaml_path.unlink()
    except FileNotFoundError:
        pass


def test_update_all(yaml_path: str = DEFAULT_SEED_SCHEMA_PATH):

    # Delete yaml_path if exists to ensure reproducible behaviour of test
    try:
        yaml_path.unlink()
    except FileNotFoundError:
        pass
    # create yaml with countries information
    create_yaml(
        "countries",
        technical_owner="test",
        business_owner="test",
    )

    # create yaml with all seeds information
    registered = register(
        seed=None,
        overwrite=False,
        yaml_path=yaml_path,
        target="qa",
        technical_owner="test",
        business_owner="test",
    )
    assert registered is True

    # check one by one if every seed info is inside yaml file
    for seed in get_all_seeds(target="qa"):
        assert check_seed_in_yaml(seed, yaml_path=yaml_path)

    # Delete yaml_path if exists to ensure reproducible behaviour of test
    try:
        yaml_path.unlink()
    except FileNotFoundError:
        pass


# --------------------------- END INTEGRA SEED COMMAND TESTING --------------------------- #


def test_clean_up(clean_up_project):
    assert True
