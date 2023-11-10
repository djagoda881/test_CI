from pathlib import Path
from typing import Union

import tomlkit
import argparse

PYPROJECT_PATH = Path(__file__).parent.parent.parent.joinpath("pyproject.toml")


def update_nesso_cli_pyproject_version(path: Union[str, Path]) -> None:
    """
    Update project version in pyproject.toml file.

    Args:
        path (Union[str, Path]): Path to the YAML file.
    """
    parser = argparse.ArgumentParser(description="Update pyproject.toml version.")
    parser.add_argument("--tag", required=True, help="Specify the version tag")

    args = vars(parser.parse_args())
    tag = args["tag"]
    if tag.startswith("v"):
        tag = tag[1:]

    with open(path, mode="rt", encoding="utf-8") as fp:
        data = tomlkit.load(fp)

    data["project"]["version"] = tag

    with open(path, mode="wt", encoding="utf-8") as file:
        tomlkit.dump(data, file)


if __name__ == "__main__":
    update_nesso_cli_pyproject_version(PYPROJECT_PATH)
