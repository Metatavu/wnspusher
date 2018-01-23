from flask import Flask, request
from pony import orm
from datetime import datetime
import threading
import atexit
import requests

db = orm.Database()
app = Flask(__name__)
worker_thread = threading.Thread()
atexit.register(worker_thread.cancel)

class Subscriber(db.Entity):
    id = orm.PrimaryKey(int, auto=True)
    channel_url = orm.Required(str, unique=True)
    subscribed = orm.Required(datetime)
    pending = orm.Set(lambda: PendingNotification)


class Message(db.Entity):
    id = orm.PrimaryKey(int, auto=True)
    content = orm.Required(str)
    launch_args = orm.Required(str)
    pending = orm.Set(lambda: PendingNotification)


class PendingNotification(db.Entity):
    id = orm.PrimaryKey(int, auto=True)
    subscriber = orm.Required(lambda: Subscriber)
    message = orm.Required(lambda: Message)
    issued = orm.Required(datetime)
    

def process_notification():
    next_pending = PendingNotification.select().order_by(PendingNotification.issued).first()
    if next_pending is not None:
        message = next_pending.message
        subscriber = next_pending.subscriber
        requests.post(subscriber.channel_url, headers = {
            'Content-Type': 'text/xml',
            'X-WNS-Type': 'wns/toast'
        })
        

@orm.db_session
def add_subscription(channel_url):
    sub = Subscriber(channel_url=channel_url, subscribed=datetime.utcnow())
    

@orm.db_session
def broadcast_notification(content, launch_args):
    message = Message(content=content, launch_args=launch_args)
    for sub in orm.select(s for s in Subscriber):
        pending = PendingNotification(subscriber=sub, message=message)




@app.route("/subscribers", methods=['POST'])
def add_subscriber_endpoint():
    body = request.get_json()
    add_subscription(body.channel_url)


@app.route("/notifications", methods=['POST'])
def broadcast_notification_endpoint():
    body = request.get_json()
    broadcast_notification(body.content, body.launchArgs)