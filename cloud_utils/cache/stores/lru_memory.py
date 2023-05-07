import collections
import logging

import gamla

from cloud_utils.cache import utils


def make_store(max_size: int, name: str):
    utils.log_initialized_cache("lru", name)
    store = collections.OrderedDict()

    def set_item(key: str, value):
        cache_key = utils.cache_key_name(name, key)
        store[cache_key] = value

        if max_size and len(store) > max_size and max_size != 0:
            store.popitem(last=False)  # Pop least recently used key.

        logging.info(
            f"Setting {cache_key} with {value} in the lru memory store.",
        )

    def get_item(key: str):
        cache_key = utils.cache_key_name(name, key)
        item = store[cache_key]
        store[cache_key] = item  # Touch key, for LRU.
        return item

    return get_item, set_item


make_async_store = gamla.compose_left(
    make_store,
    gamla.map(gamla.wrap_awaitable),
    tuple,
)
