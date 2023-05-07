import pytest

from cloud_utils.cache.stores import lru_memory


def test_store_bounded_lru():
    get_item, set_item = lru_memory.make_store(1, "my_store")

    set_item("1", 1)
    set_item("2", 2)
    set_item("3", 3)

    with pytest.raises(KeyError):
        get_item("1")

    with pytest.raises(KeyError):
        get_item("2")

    assert get_item("3") == 3


def test_store_unbounded():
    get_item, set_item = lru_memory.make_store(0, "unbound_store")

    set_item("1", 1)
    set_item("2", 2)
    set_item("3", 3)

    assert get_item("1") == 1
    assert get_item("2") == 2
    assert get_item("3") == 3
