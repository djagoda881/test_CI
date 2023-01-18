import logging
from pathlib import Path
from typing import List
import yaml
import os
import sys

# solution to import files across the repo until we make this repo a python package
current_directory = os.path.dirname(__file__)
parent_directory = os.path.dirname(current_directory)
integra_module = os.path.join(parent_directory, "cli", "integra")
sys.path.insert(1, integra_module)


from common import find_dbt_project

logger = logging.getLogger(__name__)

PROJECT_DIR = find_dbt_project()

VALID_SCHEMA_VERSION: int = 2


def get_dbt_object_type(data: str) -> str:
    """
    Gets dbt object type ("sources", "models", or "seeds") depending on yaml data.

    Args:
        data (str): The content of a "metadata yaml file" of a dbt source, seed or model.

    Returns:
        schema_type (str): the schema type of the dbt object. ("sources", "models", or "seeds").
    """
    if "sources" in data:
        schema_type: str = "sources"
    elif "models" in data:
        schema_type: str = "models"
    elif "seeds" in data:
        schema_type: str = "seeds"
    return schema_type


def get_metadata_information(file_path: str) -> list:
    """
    Gets metadata information of a dbt object.

    Args:
        file_path (str): Path to the metadata file of a dbt object.

    Returns:
        metadata_information (str): The metadata information of the dbt object.
    """
    with open(file_path) as file:
        data: dict = yaml.safe_load(file)

    schema_type: str = get_dbt_object_type(data)
    metadata_information: list = data[schema_type]

    if schema_type == "sources":
        metadata_information: list = metadata_information[0].get("tables")

    return metadata_information


def validate_description_in_file(file_path: str) -> bool:
    """
    Validates descriptions in a metadata file of a dbt object.

    Args:
        file_path (str): Path to the metadata file of a dbt object.

    Returns:
        bool: `True` if all the field is valid, `Exception` otherwise.
    """
    information: list = get_metadata_information(file_path=file_path)

    descriptions: list = []
    for table in information:
        table_description: str = table.get("description")
        descriptions.append(table_description)
        columns: list = table.get("columns")
        for column in columns:
            column_description: str = column.get("description")
            descriptions.append(column_description)

    all_descriptions_filled: bool = all(descriptions)
    if not all_descriptions_filled:
        raise ValueError(f"Please fill all descriptions in {file_path} file.")

    return True


def validate_technical_owner_in_file(file_path: str, email_domain: str) -> bool:
    """
    Validates technical owner in a metadata file of a dbt object.

    Args:
        file_path (str): Path to the metadata file of a dbt object.

    Returns:
        bool: `True` if all the field is valid, `Exception` otherwise.
    """
    information: list = get_metadata_information(file_path=file_path)

    technical_owners: list = []
    for table in information:
        technical_owner: str = table.get("meta").get("technical_owner")
        technical_owners.append(technical_owner)

    all_technical_owners_are_filled: bool = all(technical_owners)
    if not all_technical_owners_are_filled:
        raise ValueError(f"Please fill in the technical owner in the {file_path} file.")

    email_termination: str = f"@{email_domain}" if email_domain else ""

    technical_owners_validity: list = []
    for technical_owner in technical_owners:
        technical_owner_is_valid_email: bool = technical_owner.endswith(
            email_termination
        )
        technical_owner_is_valid_group: bool = technical_owner.startswith("@")

        technical_owner_is_valid: bool = bool(
            technical_owner_is_valid_email or technical_owner_is_valid_group
        )
        technical_owners_validity.append(technical_owner_is_valid)

    all_technical_owners_are_valid: bool = all(technical_owners_validity)
    if not all_technical_owners_are_valid:
        raise ValueError(
            f"Please insert valid technical owner in {file_path} file. technical_owner should be an email {'ending with ' + email_termination if email_termination else ''} or a group starting with '@'."
        )

    return True


def validate_business_owner_in_file(file_path: str, email_domain: str) -> bool:
    """
    Validates business owner in a metadata file of a dbt object.

    Args:
        file_path (str): Path to the metadata file of a dbt object.

    Returns:
        bool: `True` if all the field is valid, `Exception` otherwise.
    """
    information: list = get_metadata_information(file_path=file_path)

    business_owners: list = []
    for table in information:
        business_owner: str = table.get("meta").get("business_owner")
        business_owners.append(business_owner)

    all_business_owners_are_filled: bool = all(business_owners)
    if not all_business_owners_are_filled:
        raise ValueError(f"Please fill in the business owner in the {file_path} file.")

    email_termination: str = f"@{email_domain}" if email_domain else ""

    business_owners_validity: list = []
    for business_owner in business_owners:
        business_owner_is_valid_email: bool = business_owner.endswith(email_termination)
        business_owner_is_valid_group: bool = business_owner.startswith("@")

        business_owner_is_valid = bool(
            business_owner_is_valid_email or business_owner_is_valid_group
        )
        business_owners_validity.append(business_owner_is_valid)

    all_business_owners_are_valid: bool = all(business_owners_validity)
    if not all_business_owners_are_valid:
        raise ValueError(
            f"Please insert valid business owner in {file_path} file. business_owner should be an email {'ending with ' + email_termination if email_termination else ''} or a group starting with '@'."
        )

    return True


def validate_version_in_file(
    file_path: str, schema_version: int = VALID_SCHEMA_VERSION
) -> bool:
    """
    Validates schema version in a metadata file of a dbt object.

    Args:
        file_path (str): Path to the metadata file of a dbt object.

    Returns:
        bool: `True` if all the field is valid, `Exception` otherwise.
    """
    with open(file_path) as file:
        data: dict = yaml.safe_load(file)
    version: int = data.get("version")
    if version != schema_version:
        raise ValueError(f"Please use version {schema_version} in {file_path} file.")
    return True


def validate_file(
    file_path: str,
    email_domain: str,
    schema_version: str,
) -> bool:
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


def get_yaml_paths_under_directory(directory_path: str) -> List[str]:
    """
    Gets paths of yaml files under 'directory_path' argument, and return path of yamls inside.

    Args:
        directory_path (str): Path to a directory that contains metadata files.

    Returns:
        paths_to_metadata_yamls (List[str]): List containing absolute paths to yaml files under dir_list
    """
    paths_to_metadata_yamls: list = []
    for path in Path(directory_path).rglob("*.yml"):
        absoute_path: str = str(path.absolute())
        paths_to_metadata_yamls.append(absoute_path)

    return paths_to_metadata_yamls


def get_models_and_seeds_paths(project_dir: str) -> List[str]:
    """
    Gets all models and seeds paths under a dbt project.

    Args:
        project_dir (str): Path to the main dbt project.

    Returns:
        models_and_seeds_full_paths: The full path of every.
    """
    project_yml_path: str = f"{project_dir}/dbt_project.yml"

    with open(project_yml_path) as file:
        data: dict = yaml.safe_load(file)
        models_paths: list = data["model-paths"]
        seeds_paths: list = data["seed-paths"]

    models_and_seeds_paths: list = models_paths + seeds_paths

    models_and_seeds_full_paths: list = [
        os.path.join(project_dir, model_or_seed_path)
        for model_or_seed_path in models_and_seeds_paths
    ]

    return models_and_seeds_full_paths


def run_table_level_validation(
    project_dir: str, email_domain: str, schema_version: int
) -> bool:
    """
    Runs the validation of all tables inside a dbt project

    Args:
        project_dir (str): Path to the main dbt project.
        email_domain (str): valid email domain for your organization.
        schema_version (int): valid schema version for your organization.

    Returns:
        bool: `True` if all the fields are valid, `Exception` otherwise.
    """

    models_and_seeds_paths: list = get_models_and_seeds_paths(project_dir)

    paths_of_files_to_validate: list = []
    # Get path of yamls under models and seeds dirs
    for directory_path in models_and_seeds_paths:
        yamls_paths: list = get_yaml_paths_under_directory(directory_path)

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
