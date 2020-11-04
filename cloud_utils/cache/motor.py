import datetime
from typing import Text

import pymongo
from cloud_utils.cache import mongo
from motor import motor_asyncio

from analysis import fields
import gamla
from toolz import curried


def get_database_collection(
    mongodb_uri: Text,
    collection_name: Text,
    database: Text,
) -> motor_asyncio.AsyncIOMotorCollection:
    return gamla.pipe(
        mongodb_uri,
        motor_asyncio.AsyncIOMotorClient,
        curried.get_in([database, collection_name]),
    )


@gamla.curry
async def cursor_to_list(length: int, cursor: motor_asyncio.AsyncIOMotorCursor):
    try:
        return await cursor.to_list(length=length)
    except pymongo.errors.ExecutionTimeout:
        return ()


@gamla.curry
async def query_events(
    event_type: Text,
    bot_id: Text,
    collection: motor_asyncio.AsyncIOMotorCollection,
    timestamp: datetime.datetime,
):
    cursor = collection.find(
        {
            fields.EVENT_TYPE: event_type,
            fields.CREATED_AT: {"$lte": timestamp},
            fields.BOT_ID: bot_id,
        },
        max_time_ms=6000,
    ).sort(fields.CREATED_AT, mongo.DESCENDING)
    try:
        return await cursor.to_list(length=100)
    except pymongo.errors.ExecutionTimeout:
        return ()


