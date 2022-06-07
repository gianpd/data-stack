# crud API
from typing import List, Union
from models.tortoise import Events, Users

async def post(payload, users=True) -> int:
    element = await Users.create(**payload.dict(exclude_unset=True)) if users \
        else await Events.create(**payload.dict(exclude_unset=True))
    return element.user_id

