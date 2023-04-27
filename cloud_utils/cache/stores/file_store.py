_MAX_CHAGES_TO_TRIGGER_FLUSH = 10


def make_store(name: str) -> Tuple[Callable, Callable]:
    change_count = 0
    sync_running = False
    sync_start = 0.0

    # Initialize cache.
    try:
        cache = _load_cache_from_local(name)
        logging.info(f"Loaded {len(cache):,} cache items from local file for {name}.")
    except (OSError, IOError, EOFError, pickle.UnpicklingError) as err:
        logging.info(
            f"Cache {name} does not exist or is invalid. Initializing an empty cache. Error: {err}.",
        )
        cache = {}

    def get_item(key: Tuple):
        return cache[key]

    def set_item(key: Tuple, value):
        nonlocal change_count, sync_running, sync_start

        change_count += 1
        cache[key] = value

        if change_count >= num_misses_to_trigger_sync and not sync_running:
            logging.info(
                f"More than {num_misses_to_trigger_sync:,} keys changed in cache {name}. Syncing with local file.",
            )

            sync_running = True
            sync_start = timeit.default_timer()

            try:
                _save_cache_locally(name, cache)
                sync_running = False
                logging.info(
                    f"Synced cache {name} to local file in {timeit.default_timer() - sync_start}.",
                )
                change_count -= num_misses_to_trigger_sync
            except (OSError, IOError, EOFError) as exception:
                logging.error(
                    f"Could not sync {name} with local file. Error: {exception}.",
                )

    return get_item, set_item


make_async_store = gamla.compose_left(
    make_store, gamla.map(gamla.wrap_awaitable), tuple
)
