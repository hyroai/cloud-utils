from typing import Any, Callable, Dict, Iterable, Text, Tuple

import gamla
import pymongo

ASCENDING = pymongo.ASCENDING
DESCENDING = pymongo.DESCENDING


def client(mongodb_uri: Text, **kwargs):
    return pymongo.MongoClient(mongodb_uri, **kwargs)


@gamla.curry
def aggregate(
    collection: pymongo.collection.Collection,
    aggregation: Iterable[Dict[Text, Any]],
) -> Tuple[Dict, ...]:
    return collection.aggregate(list(aggregation), allowDiskUse=True)


@gamla.curry
def find(
    query: Dict[Text, Any],
    collection: pymongo.collection.Collection,
) -> Tuple[Dict, ...]:
    return collection.find(query)


find_all = find({})


@gamla.curry
def sort(
    collection: pymongo.collection.Collection,
    key: Text,
    direction: int,
) -> Tuple[Dict, ...]:
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
