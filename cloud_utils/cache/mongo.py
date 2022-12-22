import functools
import ssl
from typing import Any, Callable, Dict, Iterable, Text, Tuple

import gamla
import pymongo

ASCENDING = pymongo.ASCENDING
DESCENDING = pymongo.DESCENDING


def client(mongodb_uri: str, **kwargs) -> pymongo.MongoClient:
    return pymongo.MongoClient(mongodb_uri, **kwargs)


def collection_from_db(
    connection_string: str,
    database_name: str,
) -> Callable[[str], pymongo.collection.Collection]:
    @functools.lru_cache
    def collection_from_db(collection_name: str) -> pymongo.collection.Collection:
        return (
            client(
                connection_string,
                connect=False,
                ssl_cert_reqs=ssl.CERT_NONE,
                connectTimeoutMS=120_000,
                maxPoolSize=200,
            )
            .get_database(database_name)
            .get_collection(collection_name)
        )

    return collection_from_db


@gamla.curry
def aggregate(
    collection: pymongo.collection.Collection,
    aggregation: Iterable[Dict[Text, Any]],
) -> pymongo.command_cursor.CommandCursor:
    return collection.aggregate(list(aggregation), allowDiskUse=True)


@gamla.curry
def find(
    query: Dict[Text, Any],
    collection: pymongo.collection.Collection,
) -> pymongo.cursor.Cursor:
    return collection.find(query)


find_all = find({})


@gamla.curry
def sort(
    collection: pymongo.collection.Collection,
    key: str,
    direction: int,
):
    return collection.sort(key, direction)


@gamla.curry
def count(collection: pymongo.collection.Collection, query: Dict[Text, Any]) -> int:
    return collection.count_documents(query)


def add_match_filter(f: Callable) -> Tuple[Dict, ...]:
    return gamla.compose_left(
        gamla.head,
        gamla.itemgetter("$match"),
        f,
        query_to_match_aggregation_stage,
        gamla.wrap_tuple,
    )


query_to_count_aggregation_stage = gamla.just({"$count": "count"})


query_to_match_aggregation_stage = gamla.value_to_dict("$match")


query_to_sample_aggregation_stage = gamla.compose_left(
    gamla.value_to_dict("size"),
    gamla.value_to_dict("$sample"),
)


query_to_sort_aggregation_stage = gamla.value_to_dict("$sort")
