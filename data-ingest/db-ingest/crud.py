# crud API
from typing import List, Union
from models.tortoise import Events, Users
from tortoise.query_utils import Prefetch


def post(payload, users=True) -> int:
    element = Users(**payload) if users else Events(**payload)
    element.save()
    return element.id 

