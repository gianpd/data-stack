from typing import List, Union
from models.tortoise import Events, Users

#TODO: Do other methods

async def post(payload, users=True) -> int:
    """
    Create Users or Events raw.
    """
    element = await Users.create(**payload.dict(exclude_unset=True)) if users \
        else await Events.create(**payload.dict(exclude_unset=True))
    return element.user_id

