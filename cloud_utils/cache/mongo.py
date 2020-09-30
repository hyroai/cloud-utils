from typing import Any, Callable, Dict, Iterable, Text, Tuple

import gamla
import pymongo
import toolz
from toolz import curried

ASCENDING = pymongo.ASCENDING
DESCENDING = pymongo.DESCENDING


def get_client(mongodb_uri: Text, **kwargs) -> pymongo.MongoClient:
    return pymongo.MongoClient(mongodb_uri, **kwargs)


@gamla.curry
def aggregate(
    collection: pymongo.collection.Collection, aggregation: Iterable[Dict[Text, Any]],
) -> pymongo.command_cursor.CommandCursor:
    return collection.aggregate(list(aggregation), allowDiskUse=True)


@gamla.curry
def find(
    query: Dict[Text, Any], collection: pymongo.collection.Collection,
) -> pymongo.cursor.Cursor:
    return collection.find(query)


find_all = find({})


@gamla.curry
def sort(
    collection: pymongo.collection.Collection, key: Text, direction: int,
):
    return collection.sort(key, direction)


@gamla.curry
def count(collection: pymongo.collection.Collection, query: Dict[Text, Any]) -> int:
    return collection.count_documents(query)


def add_match_filter(f: Callable) -> Tuple[Dict, ...]:
    return toolz.compose_left(
        toolz.first,
        curried.get("$match"),
        f,
        query_to_match_aggregation_stage,
        gamla.wrap_tuple,
    )


def query_to_count_aggregation_stage() -> Dict[Text, Text]:
    return {"$count": "count"}


def query_to_match_aggregation_stage(
    query: Dict[Text, Any],
) -> Dict[Text, Dict[Text, Any]]:
    return {"$match": query}


def query_to_sample_aggregation_stage(n: int) -> Dict[Text, Dict[Text, int]]:
    return {"$sample": {"size": n}}


def query_to_sort_aggregation_stage(
    query: Dict[Text, int],
) -> Dict[Text, Dict[Text, int]]:
    return {"$sort": query}
