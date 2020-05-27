from typing import Any, Callable, Dict, Iterable, Text, Tuple

import gamla
import pymongo
import toolz
from toolz import curried

ASCENDING = pymongo.ASCENDING
DESCENDING = pymongo.DESCENDING


def get_client(mongodb_uri: Text, **kwargs):
    return pymongo.MongoClient(mongodb_uri, **kwargs)


@gamla.curry
def aggregate(collection, aggregation: Iterable[Dict[Text, Any]]):
    return collection.aggregate(list(aggregation), allowDiskUse=True)


@gamla.curry
def find(query, collection):
    return collection.find(query)


find_all = find({})


@gamla.curry
def sort(key, direction, collection):
    return collection.sort(key, direction)


@gamla.curry
def count(query, collection):
    return collection.count_documents(query)


def add_match_filter(f: Callable) -> Tuple[Dict, ...]:
    return toolz.compose_left(
        toolz.first,
        curried.get("$match"),
        f,
        query_to_match_aggregation_stage,
        gamla.wrap_tuple,
    )


def query_to_match_aggregation_stage(query):
    return {"$match": query}
