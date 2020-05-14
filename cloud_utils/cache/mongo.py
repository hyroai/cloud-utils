from typing import Any, Dict, Iterable, Text

import gamla
import pymongo

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
