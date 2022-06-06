# crud API
from typing import List, Union
from models.tortoise import Events, Users
from tortoise.query_utils import Prefetch


async def post(payload) -> int:
    users = Users(**payload)
    await users.save()
    return users.id