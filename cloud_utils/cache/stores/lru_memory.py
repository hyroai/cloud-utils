import collections
import logging

import gamla

from cloud_utils.cache import utils


def make_store(name: str, max_size: int):
    utils.log_initialized_cache(name)
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
