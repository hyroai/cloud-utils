from typing import Text

import gamla
import pymongo
from motor import motor_asyncio
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
