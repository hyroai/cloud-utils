from typing import Any, Dict, Iterable, Text

import pymongo
import toolz

from cloud_utils import config


def get_db(db_name: Text):
    return pymongo.MongoClient(f"{config.MONGODB_URI}/{db_name}")


@toolz.curry
def aggregate(collection, aggregation: Iterable[Dict[Text, Any]]):
    return collection.aggregate(list(aggregation), allowDiskUse=True)


@toolz.curry
def find(query, collection):
    return collection.find(query)


find_all = find({})


@toolz.curry
def sort(key, direction, collection):
    return collection.sort(key, direction)


@toolz.curry
def count(query, collection):
    return collection.count_documents(query)
