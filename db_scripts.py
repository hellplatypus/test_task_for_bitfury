from random import randint, shuffle

from peewee import DoesNotExist, fn

from models import RawData, AggData, LastProcessedId
from postgres import db


def populate_raw_data():

    if RawData.select().count() > 0:
        return

    records_number = randint(90000, 100000)

    with db.atomic():
        for i in range(records_number):
            RawData.create(user_id=randint(1, 100), event_id=randint(1, 100), amount=randint(-100000, 100000))


def process_raw_data(limit_per_run, offset):

    # producing temporary results from limited amount of records
    temp_data = {}
    source_data = RawData.select().limit(limit_per_run).offset(offset)

    last_id = None

    for item in source_data:
        if item.user_id not in temp_data.keys():
            temp_data[item.user_id] = {'user_id': item.user_id, 'balance': item.amount, 'event_number': 1,
                                       'best_event': item.amount, 'worst_event': item.amount}
        else:
            temp_data[item.user_id]['balance'] += item.amount
            temp_data[item.user_id]['event_number'] += 1

            if temp_data[item.user_id]['best_event'] < item.amount:
                temp_data[item.user_id]['best_event'] = item.amount
            if temp_data[item.user_id]['worst_event'] > item.amount:
                temp_data[item.user_id]['worst_event'] = item.amount
        last_id = item.id

    last_processed_id = LastProcessedId.get()
    last_processed_id.last_id = last_id
    last_processed_id.save()

    # merging temporary results with previous results in DB
    for item in temp_data.values():

        try:
            record = AggData.get(user_id=item['user_id'])
            record.balance += item['balance']
            record.event_number += item['event_number']
            if record.best_event < item['best_event']:
                record.best_event = item['best_event']
            if record.worst_event > item['worst_event']:
                record.worst_event = item['worst_event']
            record.save()
        except DoesNotExist:
            AggData.get_or_create(**item)


def aggregate_data():

    LastProcessedId.drop_table()
    LastProcessedId.create_table()
    LastProcessedId.create(last_id=0)

    AggData.drop_table()
    AggData.create_table()

    limit_per_run = 10000
    offset = 0

    while RawData.select().limit(limit_per_run).offset(offset).count() > 0:
        process_raw_data(limit_per_run, offset)
        offset += limit_per_run


def check():

    user_ids_list = list(AggData.select(AggData.user_id).distinct())
    shuffle(user_ids_list)
    chosens = user_ids_list[:10]

    for user_id in chosens:
        agg_data = AggData.get(user_id=user_id)

        balance = RawData.select(fn.Sum(RawData.amount)).where(RawData.user_id == user_id).scalar()
        assert (balance == agg_data.balance)

        event_number = RawData.select(RawData.event_id).where(RawData.user_id == user_id).count()
        assert (event_number == agg_data.event_number)

        best_event = RawData.select(fn.Max(RawData.amount)).where(RawData.user_id == user_id).scalar()
        assert (best_event == agg_data.best_event)

        worst_event = RawData.select(fn.Min(RawData.amount)).where(RawData.user_id == user_id).scalar()
        assert (worst_event == agg_data.worst_event)


if __name__ == '__main__':
    from time import time
    t = time()
    populate_raw_data()
    # aggregate_data()
    # check()
    print(time() - t)
