from cloud_utils.cache.stores import pickle


def test_pickle_store(pickle_file):
    get_item, set_item = pickle.make_store(pickle_file, "test-store", 50)

    set_item("1", 1)
    set_item("2", 2)
    set_item("3", 3)
    assert get_item("1") == 1
    assert get_item("2") == 2
    assert get_item("3") == 3


def test_pickle_store_more_than_10_changed_keys(pickle_file):
    get_item, set_item = pickle.make_store(pickle_file, "test-store", 10)
    for x in range(12):
        set_item(f"{x}", x)
    for x in range(12):
        assert get_item(f"{x}") == x


async def test_pickle_store_async(pickle_file):
    get_item, set_item = pickle.make_async_store(pickle_file, "test-store", 50)

    await set_item("1", 1)
    await set_item("2", 2)
    await set_item("3", 3)
    assert await get_item("1") == 1
    assert await get_item("2") == 2
    assert await get_item("3") == 3


def test_pickle_store_sync_threshold_one_persists_every_write(pickle_file):
    # sync_threshold=1 flushes on every write, so a fresh store reads all keys from disk.
    _, set_item = pickle.make_store(pickle_file, "test-store", 1)
    for x in range(3):
        set_item(f"{x}", x)
    get_item, _ = pickle.make_store(pickle_file, "test-store", 1)
    for x in range(3):
        assert get_item(f"{x}") == x
