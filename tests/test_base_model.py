
import builtins
import os
import random
import shutil
from datetime import datetime

import mock
import pandas as pd
import pytest
import yaml

from getkey import key
from nesso.base_model import check_if_base_model_exists
from nesso.model import bootstrap, bootstrap_yaml
from nesso.source import add as add_source
from nesso.source import check_if_source_exists, check_if_source_table_exists
from nesso.source import create as create_source



def test_base_model_create(TEST_SOURCE, postgres_connection):

    assert not check_if_source_exists(TEST_SOURCE)
    assert not check_if_base_model_exists(TEST_TABLE_ACCOUNT_BASE_MODEL)

    # Create the source
    with mock.patch.object(
        builtins,
        "input",
        lambda: key.ENTER,
    ):

        create_source(TEST_SOURCE)

    assert check_if_base_model_exists(TEST_TABLE_CONTACT_BASE_MODEL)
    assert check_if_source_exists(TEST_SOURCE)

    # Cleanup.
    shutil.rmtree(
        DBT_PROJECT_DIR.joinpath("models", "sources", TEST_SOURCE),
        ignore_errors=True,
    )
    shutil.rmtree(
        DBT_PROJECT_DIR.joinpath("models", "conformed"),
        ignore_errors=True,
    )
    postgres_connection.execute(f"DROP VIEW IF EXISTS {TEST_TABLE_CONTACT_BASE_MODEL};")
    postgres_connection.execute(f"DROP TABLE IF EXISTS {TEST_TABLE_CONTACT};")