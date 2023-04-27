from cloud_utils.cache.store import pickle_store


def test_pickle_store():
    get_item, set_item = pickle_store.make_store("test-store", 1)

    set_item("1", 1)
    set_item("2", 2)
    set_item("3", 3)

    assert get_item("1") == 1
    assert get_item("2") == 2
    assert get_item("3") == 3
