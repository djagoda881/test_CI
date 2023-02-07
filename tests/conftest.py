import random
import shutil
from datetime import datetime

import pandas as pd
import pytest
from faker import Faker
from nesso.common import DBT_PROJECT_DIR
from pydantic import BaseModel, Field
from sqlalchemy import create_engine

fake = Faker()


MART = "unit_test"
PROJECT = "unit_test"
MODEL = "test_model"

NROWS = 100
MODEL_PATH = DBT_PROJECT_DIR.joinpath(
    "models", "marts", MART, PROJECT, MODEL, MODEL + ".sql"
)
MODEL_YAML_PATH = DBT_PROJECT_DIR.joinpath(
    "models", "marts", MART, PROJECT, MODEL, MODEL + ".yml"
)

@pytest.fixture(scope="session", autouse=True)
def setup_and_teardown():
    shutil.rmtree(
        DBT_PROJECT_DIR.joinpath("models", "sources"), ignore_errors=True
    )
    shutil.rmtree(
        DBT_PROJECT_DIR.joinpath("models", "conformed"), ignore_errors=True
    )
    
    yield

    shutil.rmtree(
        DBT_PROJECT_DIR.joinpath("models", "sources"), ignore_errors=True
    )
    shutil.rmtree(
        DBT_PROJECT_DIR.joinpath("models", "conformed"), ignore_errors=True
    )


@pytest.fixture(scope="session")
def postgres_connection():
    connection = create_engine("postgresql://user:password@nesso_postgres:5432/db")
    yield connection
    connection.dispose()


@pytest.fixture(scope="session")
def TEST_TABLE_CONTACT():
    yield "test_table_contact"


@pytest.fixture(scope="session")
def TEST_TABLE_ACCOUNT():
    yield "test_table_account"


@pytest.fixture(scope="session")
def TEST_SOURCE():
    yield "public"


@pytest.fixture(scope="session", autouse=True)
def create_contacts_table(postgres_connection, TEST_SOURCE, TEST_TABLE_CONTACT):

    postgres_connection.execute(f"DROP TABLE IF EXISTS {TEST_TABLE_CONTACT} CASCADE;")

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
        TEST_TABLE_CONTACT, postgres_connection, schema=TEST_SOURCE, if_exists="replace", index=False
    )

    yield

    postgres_connection.execute(f"DROP TABLE IF EXISTS {TEST_TABLE_CONTACT} CASCADE;")


@pytest.fixture(scope="session", autouse=True)
def create_accouts_table(postgres_connection, TEST_SOURCE, TEST_TABLE_ACCOUNT):

    postgres_connection.execute(f"DROP TABLE IF EXISTS {TEST_TABLE_ACCOUNT} CASCADE;")

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
        TEST_TABLE_ACCOUNT, postgres_connection, schema=TEST_SOURCE, if_exists="replace", index=False
    )

    yield

    postgres_connection.execute(f"DROP TABLE IF EXISTS {TEST_TABLE_ACCOUNT} CASCADE;")

