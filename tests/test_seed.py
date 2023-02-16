from pathlib import Path

import oyaml as yaml

from nesso.common import DBT_PROJECT_DIR
from nesso.seed import (
    add_to_schema,
    check_if_schema_exists,
    check_if_seed_exists,
    create_schema,
    register,
)

TEST_SEED_AVG_NAME = "average_salary_test"
TEST_SEED_COUNTRIES_NAME = "countries_example"
TEST_SEED_PEOPLE_NAME = "people_example"

SEED_SCHEMA_PATH = DBT_PROJECT_DIR.joinpath("seeds", "master_data", "schema.yml")
TEST_SEED_AVG_PATH = SEED_SCHEMA_PATH.parent.joinpath(f"{TEST_SEED_AVG_NAME}.csv")
TEST_SEED_COUNTRIES_PATH = SEED_SCHEMA_PATH.parent.joinpath(
    f"{TEST_SEED_COUNTRIES_NAME}.csv"
)
TEST_SEED_PEOPLE_PATH = SEED_SCHEMA_PATH.parent.joinpath(f"{TEST_SEED_PEOPLE_NAME}.csv")


def test_check_if_schema_exists():
    # Assumptions.
    assert not SEED_SCHEMA_PATH.exists()
    assert not check_if_schema_exists(SEED_SCHEMA_PATH)

    # Create test schema
    seed_schema_yaml = {
        "version": 2,
        "seeds": [{"name": TEST_SEED_COUNTRIES_NAME}],
    }
    with open(SEED_SCHEMA_PATH, "w") as f:
        yaml.safe_dump(seed_schema_yaml, f)

    assert check_if_schema_exists(SEED_SCHEMA_PATH)

    # Cleanup.
    SEED_SCHEMA_PATH.unlink()


def test_check_if_seed_exists():

    # Assumptions.
    assert TEST_SEED_COUNTRIES_PATH.exists()
    assert not SEED_SCHEMA_PATH.exists()

    # Only one seed should be present.
    exists = check_if_seed_exists(TEST_SEED_COUNTRIES_NAME)

    assert exists is False

    # Create test seed.
    seed_schema_yaml = {
        "version": 2,
        "seeds": [{"name": TEST_SEED_COUNTRIES_NAME}],
    }
    with open(SEED_SCHEMA_PATH, "w") as f:
        yaml.safe_dump(seed_schema_yaml, f)

    # Validate.
    assert check_if_seed_exists(TEST_SEED_COUNTRIES_NAME)

    # Cleanup.
    SEED_SCHEMA_PATH.unlink()


def test_check_if_seed_exists_returns_false_when_untrue():

    # Assumptions.
    assert TEST_SEED_COUNTRIES_PATH.exists()
    assert not SEED_SCHEMA_PATH.exists()

    # Only one seed should be present.
    exists = check_if_seed_exists(TEST_SEED_COUNTRIES_NAME)

    assert exists is False

    # Create test seed.
    seed_schema_yaml = {
        "version": 2,
        "seeds": [{"name": "some_other_seed"}],
    }
    with open(SEED_SCHEMA_PATH, "w") as f:
        yaml.safe_dump(seed_schema_yaml, f)

    # Validate.
    assert not check_if_seed_exists(TEST_SEED_COUNTRIES_NAME)

    # Cleanup.
    SEED_SCHEMA_PATH.unlink()


def test_check_if_seed_exists_handles_empty_schema():

    # Assumptions.
    assert TEST_SEED_COUNTRIES_PATH.exists()
    assert not SEED_SCHEMA_PATH.exists()

    # Create test seed.
    seed_schema_yaml = {
        "version": 2,
        "seeds": [],
    }
    with open(SEED_SCHEMA_PATH, "w") as f:
        yaml.safe_dump(seed_schema_yaml, f)

    exists = check_if_seed_exists(TEST_SEED_COUNTRIES_NAME)

    # Validate.
    assert exists is False

    # Cleanup.
    SEED_SCHEMA_PATH.unlink()


def test_check_if_seed_exists_reads_seed_schema_path():

    fake_schema_path = Path("fake_schema.yml")

    # Assumptions.
    assert TEST_SEED_COUNTRIES_PATH.exists()
    assert not fake_schema_path.exists()

    # Only one seed should be present.
    exists = check_if_seed_exists(
        TEST_SEED_COUNTRIES_NAME, schema_path=fake_schema_path
    )

    assert exists is False

    # Create test seed.
    seed_schema_yaml = {
        "version": 2,
        "seeds": [{"name": TEST_SEED_COUNTRIES_NAME}],
    }
    with open(fake_schema_path, "w") as f:
        yaml.safe_dump(seed_schema_yaml, f)

    # Validate.
    assert check_if_seed_exists(TEST_SEED_COUNTRIES_NAME, schema_path=fake_schema_path)

    # Cleanup.
    fake_schema_path.unlink()


def test_create_schema():
    # Assumptions.
    assert not SEED_SCHEMA_PATH.exists()

    create_schema(SEED_SCHEMA_PATH)

    # Validate.
    assert SEED_SCHEMA_PATH.exists()

    with open(SEED_SCHEMA_PATH) as f:
        schema = yaml.safe_load(f)
    assert "version" in schema
    assert "seeds" in schema

    # Cleanup.
    SEED_SCHEMA_PATH.unlink()


def test_add_to_schema():
    # Assumptions.
    assert not SEED_SCHEMA_PATH.exists()

    # Preconditions.
    create_schema()
    assert SEED_SCHEMA_PATH.exists()

    # Validate
    add_to_schema(
        TEST_SEED_COUNTRIES_NAME,
        schema_path=SEED_SCHEMA_PATH,
        technical_owner="test_technical_owner",
        business_owner="test_business_owner",
        target="qa",
    )
    assert check_if_seed_exists(TEST_SEED_COUNTRIES_NAME) is True

    # Cleanup.
    SEED_SCHEMA_PATH.unlink()


def test_register(postgres_connection):

    # Assumptions.
    assert TEST_SEED_COUNTRIES_PATH.exists()
    assert not SEED_SCHEMA_PATH.exists()

    # Register the seed, ie. materialize it and create an entry for it
    # in the seed schema file.
    register(
        seed=TEST_SEED_COUNTRIES_NAME,
        schema_path=SEED_SCHEMA_PATH,
        technical_owner="test_technical_owner",
        business_owner="test_business_owner",
        target="qa",
    )

    # Preliminary checks.
    assert SEED_SCHEMA_PATH.exists()

    # Check if the seed was materialized.
    seed_table_query = f"SELECT FROM information_schema.tables WHERE table_name='{TEST_SEED_COUNTRIES_NAME}'"
    exists_query = f"SELECT EXISTS ({seed_table_query});"
    is_materialized = postgres_connection.execute(exists_query).fetchone()[0]
    assert is_materialized

    # Check if it was added to the schema file.
    assert check_if_seed_exists(TEST_SEED_COUNTRIES_NAME, schema_path=SEED_SCHEMA_PATH)

    # Cleanup.
    SEED_SCHEMA_PATH.unlink()
    postgres_connection.execute(f"DROP TABLE IF EXISTS {TEST_SEED_COUNTRIES_NAME};")
