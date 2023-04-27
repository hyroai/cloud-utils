import pytest

from cloud_utils.cache import lru_memory_store


def test_store_bounded_lru():
    get_item, set_item = lru_memory_store.make_store(1)

    set_item("1", 1)
    set_item("2", 2)
    set_item("3", 3)

    with pytest.raises(KeyError):
        get_item("1")

    with pytest.raises(KeyError):
        get_item("2")

    assert get_item("3") == 3


def test_store_unbounded():
    get_item, set_item = lru_memory_store.make_store(0)

    set_item("1", 1)
    set_item("2", 2)
    set_item("3", 3)

    assert get_item("1") == 1
    assert get_item("2") == 2
    assert get_item("3") == 3
