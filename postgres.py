from peewee import *


USERNAME = 'useruser'
PASSWORD = 'pass'
DB = 'db'
HOST = 'localhost'


db = PostgresqlDatabase(user=USERNAME, password=PASSWORD, database=DB, host=HOST)


class BaseModel(Model):
    class Meta:
        database = db
