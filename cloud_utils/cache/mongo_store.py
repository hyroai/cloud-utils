import concurrent.futures
import functools
import logging
from typing import TYPE_CHECKING, Any, Callable, FrozenSet, Optional, Text, Tuple

import gamla
import pymongo
import toolz
from cloud_utils import config
from toolz import curried

if TYPE_CHECKING:  # Prevent cyclic imports
    from nlu.intents import utils


@functools.lru_cache()
def _get_mongo_client():
    return pymongo.MongoClient(config.MONGODB_URI)


@functools.lru_cache()
def _get_executor_pool():
    return concurrent.futures.ThreadPoolExecutor()


@functools.lru_cache(maxsize=None)
def get_disambiguation_recordings(
    function_name: Text = "store_embedding",
) -> Tuple[Any, ...]:
    """currently using data from all bots"""
    db = _get_mongo_client().get_database("function_recordings")

    return toolz.pipe(
        db.get_collection("recordings").find({"function_name": function_name}),
        curried.map(
            lambda doc: (
                {
                    "option_text": doc["args"]["option_text"],
                    "original_text": doc["args"]["original_text"],
                    "result": doc["result"],
                }
            )
        ),
        tuple,
    )


def sync_recordings_to_mongo(function_name, recordings):
    db = _get_mongo_client().get_database("function_recordings")
    collection = db.get_collection("recordings")

    logging.info(
        f"Saving {len(recordings)} recordings from function {function_name} to mongo..."
    )

    for args_result_tuple in recordings:
        document = {
            "function_name": function_name,
            "args": {arg: val for arg, val in args_result_tuple[0]},
            "convesastion_id": args_result_tuple[2],
            "bot_id": args_result_tuple[3],
        }
        collection.update_one(
            document, {"$set": {"result": args_result_tuple[1]}}, upsert=True
        )

    logging.info(f"Done saving recordings from {function_name} to mongo.")


def get_formatted_recording(
    args: Tuple[Any],
    result: Any,
    intent_dependencies: "utils.IntentDependencies",
    result_handler: Callable = toolz.identity,
):
    return (
        args,
        result_handler(result),
        intent_dependencies.conversation_id if intent_dependencies else None,
        intent_dependencies.bot_id if intent_dependencies else None,
    )


def record_to_mongo(
    args_to_save: Tuple[Text, ...],
    sync_interval: int = 20,
    result_handler: Callable = toolz.identity,
    intent_dependencies: Optional["utils.IntentDependencies"] = None,
    override_name: Optional[Text] = None,
) -> Callable:
    def decorator(func):
        buffer = set()

        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            result = await func(*args, **kwargs)

            if result is not None and config.FUNCTION_RECORDINGS_ENABLE:
                named_arg_values = toolz.pipe(
                    [
                        zip(func.__code__.co_varnames[: len(args)], args),
                        tuple(kwargs.items()),
                    ],
                    toolz.concat,
                    curried.filter(
                        toolz.compose_left(toolz.first, args_to_save.__contains__)
                    ),
                    tuple,
                )
                buffer.add(
                    get_formatted_recording(
                        named_arg_values,
                        result,
                        intent_dependencies,
                        result_handler=result_handler,
                    )
                )

            if len(buffer) == sync_interval:
                _get_executor_pool().submit(
                    sync_recordings_to_mongo,
                    override_name if override_name else func.__name__,
                    buffer.copy(),
                )
                buffer.clear()

            return result

        return wrapper

    return decorator


@gamla.curry
def get_synonym_from_data(
    text: Text, external_data: Tuple[Any, ...], min_threshold=2
) -> FrozenSet[Optional[Text]]:
    return toolz.pipe(
        external_data,
        curried.filter(lambda x: x["option_text"] == text),
        curried.map(lambda x: {x["original_text"]: x["result"]}),
        curried.merge_with(tuple),
        curried.valfilter(all),
        curried.valfilter(lambda option: len(option) >= min_threshold),
        dict.keys,
        frozenset,
    )
