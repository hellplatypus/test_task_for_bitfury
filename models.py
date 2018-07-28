from peewee import *

from postgres import BaseModel


class RawData(BaseModel):
    user_id = IntegerField()
    event_id = IntegerField()
    amount = IntegerField()


class AggData(BaseModel):
    user_id = IntegerField(primary_key=True)
    balance = IntegerField()
    event_number = IntegerField()
    best_event = IntegerField()
    worst_event = IntegerField()


class LastProcessedId(BaseModel):
    last_id = IntegerField()
