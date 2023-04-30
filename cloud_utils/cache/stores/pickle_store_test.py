from cloud_utils.cache.stores import pickle_store


def test_pickle_store(pickle_file):
    get_item, set_item = pickle_store.make_store("test-store", pickle_file)

    set_item("1", 1)
    set_item("2", 2)
    set_item("3", 3)
    assert get_item("1") == 1
    assert get_item("2") == 2
    assert get_item("3") == 3


def test_pickle_store_sync(pickle_file):
    get_item, set_item = pickle_store.make_store("test-store", pickle_file)
    for x in range(12):
        set_item(f"{x}", x)
    for x in range(12):
        assert get_item(f"{x}") == x
