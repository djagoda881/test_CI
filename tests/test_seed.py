
import shutil

from nesso.common import DBT_PROJECT_DIR
from nesso.seed import check_seed_in_yaml, get_all_seeds, register


TEST_SEED_AVG_NAME = "average_salary_test"
TEST_SEED_COUNTRIES_NAME = "countries_example"
DEFAULT_SEED_SCHEMA_PATH = DBT_PROJECT_DIR.joinpath(
    "seeds", "master_data", "schema.yml"
)
TEST_SEED_AVG_PATH = DEFAULT_SEED_SCHEMA_PATH.parent.joinpath(
    f"{TEST_SEED_AVG_NAME}.csv"
)
TEST_SEED_COUNTRIES_PATH = DEFAULT_SEED_SCHEMA_PATH.parent.joinpath(
    f"{TEST_SEED_COUNTRIES_NAME}.csv"
)

def test_create_all(postgres_connection, yaml_path: str = DEFAULT_SEED_SCHEMA_PATH):

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

    postgres_connection.execute(f"DROP TABLE IF EXISTS {TEST_SEED_AVG_NAME};")


def test_update_all(postgres_connection, yaml_path: str = DEFAULT_SEED_SCHEMA_PATH):

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

    postgres_connection.execute(f"DROP TABLE IF EXISTS {TEST_SEED_COUNTRIES_NAME};")
    postgres_connection.execute(f"DROP TABLE IF EXISTS {TEST_SEED_AVG_NAME};")
