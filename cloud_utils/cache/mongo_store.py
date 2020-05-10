import functools
import logging
from typing import Any, Text, Tuple

import pymongo
import toolz
from toolz import curried

from cloud_utils import config


@functools.lru_cache()
def _get_mongo_client():
    return pymongo.MongoClient(config.MONGODB_URI)


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
            "conversation_id": args_result_tuple[2],
            "bot_id": args_result_tuple[3],
        }
        collection.update_one(
            document, {"$set": {"result": args_result_tuple[1]}}, upsert=True
        )

    logging.info(f"Done saving recordings from {function_name} to mongo.")
