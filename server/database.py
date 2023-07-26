from datetime import datetime
import os

import peewee

# Set the path to the SSL CA certificate to use
cacert = os.path.join(os.path.dirname(__file__), "DigiCertGlobalRootG2.crt")

# db = peewee.SqliteDatabase("feed_database.db")
db = peewee.MySQLDatabase(
    os.environ["DATABASE_DB"],
    user=os.environ["DATABASE_USER"],
    password=os.environ["DATABASE_PASSWORD"],
    host=os.environ["DATABASE_HOST"],
    ssl_disabled=False,
    ssl_ca=cacert,
)


class BaseModel(peewee.Model):
    class Meta:
        database = db


class Post(BaseModel):
    uri = peewee.CharField(index=True)
    cid = peewee.CharField()
    reply_parent = peewee.CharField(null=True, default=None)
    reply_root = peewee.CharField(null=True, default=None)
    indexed_at = peewee.DateTimeField(default=datetime.now)


class SubscriptionState(BaseModel):
    service = peewee.CharField(unique=True)
    cursor = peewee.IntegerField()


if db.is_closed():
    db.connect()
    db.create_tables([Post, SubscriptionState])
