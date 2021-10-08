import gamla
import pymongo
from motor import motor_asyncio


def database_collection(
    mongodb_uri: str,
    collection_name: str,
    database: str,
) -> motor_asyncio.AsyncIOMotorCollection:
    return gamla.pipe(
        mongodb_uri,
        motor_asyncio.AsyncIOMotorClient,
        gamla.get_in([database, collection_name]),
    )


@gamla.curry
async def cursor_to_list(length: int, cursor: motor_asyncio.AsyncIOMotorCursor):
    try:
        return await cursor.to_list(length=length)
    except pymongo.errors.ExecutionTimeout:
        return ()
