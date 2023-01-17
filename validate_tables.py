import logging
from pathlib import Path
from typing import List
import yaml
import os

logger = logging.getLogger(__name__)

PROJECT_DIR = Path(__file__).resolve().parent.joinpath("dbt", "lakehouse")

VALID_SCHEMA_VERSION = 2


def get_dbt_object_type(data):
    if "sources" in data:
        schema_type = "sources"
    elif "models" in data:
        schema_type = "models"
    elif "seeds" in data:
        schema_type = "seeds"
    return schema_type


def get_metadata_information(file_path):
    with open(file_path) as file:
        data = yaml.safe_load(file)

    schema_type = get_dbt_object_type(data)
    metadata_information = data[schema_type]

    if schema_type == "sources":
        metadata_information = metadata_information[0].get("tables")

    return metadata_information


def validate_description_in_file(file_path):

    information = get_metadata_information(file_path=file_path)

    descriptions = []
    for table in information:
        table_description = table.get("description")
        descriptions.append(table_description)
        columns = table.get("columns")
        for column in columns:
            column_description = column.get("description")
            descriptions.append(column_description)

    all_descriptions_filled = all(descriptions)
    if not all_descriptions_filled:
        raise ValueError(f"Please fill all descriptions in {file_path} file.")

    return True


def validate_technical_owner_in_file(file_path, email_domain):
    information = get_metadata_information(file_path=file_path)

    technical_owners = []
    for table in information:
        technical_owner = table.get("meta").get("technical_owner")
        technical_owners.append(technical_owner)

    all_technical_owners_are_filled = all(technical_owners)
    if not all_technical_owners_are_filled:
        raise ValueError(f"Please fill in the technical owner in the {file_path} file.")

    email_termination = f"@{email_domain}" if email_domain else ""

    technical_owners_validity = []
    for technical_owner in technical_owners:
        technical_owner_is_valid_email = technical_owner.endswith(email_termination)
        technical_owner_is_valid_group = technical_owner.startswith("@")

        technical_owner_is_valid = bool(
            technical_owner_is_valid_email or technical_owner_is_valid_group
        )
        technical_owners_validity.append(technical_owner_is_valid)

    all_technical_owners_are_valid = all(technical_owners_validity)
    if not all_technical_owners_are_valid:
        raise ValueError(
            f"Please insert valid technical owner in {file_path} file. technical_owner should be an email {'ending with ' + email_termination if email_termination else ''} or a group starting with '@'."
        )

    return True


def validate_business_owner_in_file(file_path, email_domain):
    information = get_metadata_information(file_path=file_path)

    business_owners = []
    for table in information:
        business_owner = table.get("meta").get("business_owner")
        business_owners.append(business_owner)

    all_business_owners_are_filled = all(business_owners)
    if not all_business_owners_are_filled:
        raise ValueError(f"Please fill in the business owner in the {file_path} file.")

    email_termination = f"@{email_domain}" if email_domain else ""

    business_owners_validity = []
    for business_owner in business_owners:
        business_owner_is_valid_email = business_owner.endswith(email_termination)
        business_owner_is_valid_group = business_owner.startswith("@")

        business_owner_is_valid = bool(
            business_owner_is_valid_email or business_owner_is_valid_group
        )
        business_owners_validity.append(business_owner_is_valid)

    all_business_owners_are_valid = all(business_owners_validity)
    if not all_business_owners_are_valid:
        raise ValueError(
            f"Please insert valid business owner in {file_path} file. business_owner should be an email {'ending with ' + email_termination if email_termination else ''} or a group starting with '@'."
        )

    return True


def validate_version_in_file(file_path, schema_version=VALID_SCHEMA_VERSION):
    with open(file_path) as file:
        data = yaml.safe_load(file)
    version = data.get("version")
    if version != schema_version:
        raise ValueError(f"Please use version {schema_version} in {file_path} file.")


def validate_file(
    file_path: str,
    email_domain: str,
    schema_version: str,
):
    """
    Checks if the fields retrieved from the metadata files in the dbt project are valid.

    The following fields are verified:
    1) The `description` field is filled in
    2) The `technical_owner` field is using the correct email domain or correct group structure
    3) The `business_owner` field is using the correct email domain or correct group structure
    4) The `version` field is using the correct version number

    Args:
        description(str): The `description` field retrieved from a specific table.
        technical_owner(str): The `technical_owner` field retrieved from a specific table.
        business_owner(str): The `business_owner` field retrieved from a specific table.
        version(int): The `version` field retrieved from a specific table.
        email_domain(str, optional): The `email_domain` field retrieved from a specific table.
        dir_path(str): The path under which the file being validated exists.

    Returns:
        bool: `True` if all the fields are valid, `Exception` otherwise.

    """

    # Description validation
    validate_description_in_file(file_path=file_path)

    # Technical Owner validation
    validate_technical_owner_in_file(file_path=file_path, email_domain=email_domain)

    # Business Owner validation
    validate_business_owner_in_file(file_path=file_path, email_domain=email_domain)

    # Version validation
    validate_version_in_file(file_path=file_path, schema_version=schema_version)

    return True


def get_yaml_paths_under_directory(directory: str) -> bool:
    """
    returns paths of yaml files under 'directory' argument, and return path of yamls inside.

    Args:
        directory (str): A directory that contains metadata files.

    Returns:
        paths_to_metadata_yamls (List[str]): List containing absolute paths to yaml files under dir_list
    """
    paths_to_metadata_yamls = []
    for path in Path(directory).rglob("*.yml"):
        absoute_path = str(path.absolute())
        paths_to_metadata_yamls.append(absoute_path)

    return paths_to_metadata_yamls


def get_models_and_seeds_paths(project_dir) -> List[str]:
    project_yml_path = f"{project_dir}/dbt_project.yml"

    with open(project_yml_path) as file:
        data = yaml.safe_load(file)
        models_paths = data["model-paths"]
        seeds_paths = data["seed-paths"]

    models_and_seeds_paths = models_paths + seeds_paths

    models_and_seeds_full_paths = [
        os.path.join(project_dir, model_or_seed_path)
        for model_or_seed_path in models_and_seeds_paths
    ]

    return models_and_seeds_full_paths


def run_table_level_validation(project_dir, email_domain, schema_version) -> bool:
    models_and_seeds_paths = get_models_and_seeds_paths(project_dir)

    paths_of_files_to_validate = []
    # Get path of yamls under models and seeds dirs
    for directory_path in models_and_seeds_paths:
        yamls_paths = get_yaml_paths_under_directory(directory_path)

        # using .extend() because get_yaml_paths_under_directory returns a list
        paths_of_files_to_validate.extend(yamls_paths)

    for path in paths_of_files_to_validate:
        validate_file(
            file_path=path, email_domain=email_domain, schema_version=schema_version
        )

    return True


if __name__ == "__main__":
    run_table_level_validation(
        project_dir=PROJECT_DIR,
        email_domain="",
        schema_version=VALID_SCHEMA_VERSION,
    )
