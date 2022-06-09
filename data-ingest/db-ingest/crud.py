from typing import List, Union
from models.tortoise import Events, Users

#TODO: Do other methods

async def post(payload, users=True) -> int:
    """
    Create Users or Events raw.
    """
    # element = await Users.create(**payload.dict(exclude_unset=True)) if users \
    #     else await Events.create(**payload.dict(exclude_unset=True))
    element = await Users(**payload) if users else await Events(**payload)
    await element.save()
    return element.user_id

async def get_event(id: int) -> Union[dict, None]:
    event = await Events.filter(id=id).first().values()
    if event:
        return event
    return None
