import pytest


@pytest.fixture(scope="session")
def pickle_file(tmp_path_factory):
    return tmp_path_factory.mktemp("temp_dir")
