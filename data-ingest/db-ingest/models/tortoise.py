from tortoise import fields, models
from tortoise.contrib.pydantic import pydantic_model_creator

class Users(models.Model):
    user_id = fields.CharField(max_length=255, pk=True)
    storage = fields.CharField(max_length=255)
    created = fields.DatetimeField()
    timestamp = fields.DatetimeField(auto_new_add=True)

    def __str__(self):
        return self.user_id

class Events(models.Model):
    user_id = fields.CharField(max_length=255, pk=True)
    direction = fields.CharField(max_length=255)
    size  = fields.IntField()
    status = fields.DecimalField(max_digits=5, decimal_places=3)
    transfer_time = fields.FloatField()
    transfer_speed = fields.FloatField()
    timestamp = fields.IntField()
    #timestamp = fields.DatetimeField(auto_new_add=True)

    def __str__(self):
        return self.user_id


UsersSchema = pydantic_model_creator(Users)
EventsSchema = pydantic_model_creator(Events)
