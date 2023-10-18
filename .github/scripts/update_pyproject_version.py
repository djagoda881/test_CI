import subprocess
from pathlib import Path
from typing import Union

import tomlkit

PYPROJECT_PATH = Path(__file__).parent.parent.parent.joinpath("pyproject.toml")


def update_nesso_cli_pyproject_version(path: Union[str, Path]) -> None:
    """
    Update project version in pyproject.toml file.

    Args:
        path (Union[str, Path]): Path to the YAML file.
    """
    get_tag_command = subprocess.check_output(
        ["git", "describe", "--tags", "--abbrev=0"], universal_newlines=True
    )
    tag = get_tag_command.strip()
    breakpoint()
    if tag.startswith("v"):
        tag = tag[1:]

    with open(path, mode="rt", encoding="utf-8") as fp:
        data = tomlkit.load(fp)

    data["project"]["version"] = tag

    with open(path, mode="wt", encoding="utf-8") as file:
        tomlkit.dump(data, file)


if __name__ == "__main__":
    update_nesso_cli_pyproject_version(PYPROJECT_PATH)
