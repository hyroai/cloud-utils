import collections
import logging

import gamla


def make_store(max_size: int):

    store = collections.OrderedDict()

    def set_item(key: str, value):
        store[key] = value

        if max_size and len(store) > max_size:
            store.popitem(last=False)  # Pop least recently used key.

        logging.info(
            f"Setting {key} with {value} in the lru memory store.",
        )

    def get_item(key: str):
        item = store[key]
        store[key] = item  # Touch key, for LRU.
        return item

    return get_item, set_item


make_async_store = gamla.compose_left(
    make_store,
    gamla.map(gamla.wrap_awaitable),
    tuple,
)
