import os
import pickle as stdlib_pickle
import subprocess
import sys

from cloud_utils.cache.stores import pickle


def test_pickle_store_flushes_sub_threshold_writes_on_exit(tmp_path):
    # A handful of writes never reaches the periodic-sync threshold, so without the
    # exit flush the on-disk cache is never written. A clean process exit must still
    # persist all of them.
    keys = 3
    subprocess.run(
        [
            sys.executable,
            "-c",
            "from cloud_utils.cache.stores import pickle;"
            f"_, set_item = pickle.make_store({str(tmp_path)!r}, 'exit-store');"
            f"[set_item(str(i), i) for i in range({keys})]",
        ],
        check=True,
        env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)},
    )
    with open(tmp_path / "exit-store.pickle", "rb") as cache_file:
        assert len(stdlib_pickle.load(cache_file)) == keys


def test_pickle_store(pickle_file):
    get_item, set_item = pickle.make_store(pickle_file, "test-store")

    set_item("1", 1)
    set_item("2", 2)
    set_item("3", 3)
    assert get_item("1") == 1
    assert get_item("2") == 2
    assert get_item("3") == 3


def test_pickle_store_more_than_10_changed_keys(pickle_file):
    get_item, set_item = pickle.make_store(pickle_file, "test-store")
    for x in range(12):
        set_item(f"{x}", x)
    for x in range(12):
        assert get_item(f"{x}") == x


async def test_pickle_store_async(pickle_file):
    get_item, set_item = pickle.make_async_store(pickle_file, "test-store")

    await set_item("1", 1)
    await set_item("2", 2)
    await set_item("3", 3)
    assert await get_item("1") == 1
    assert await get_item("2") == 2
    assert await get_item("3") == 3
