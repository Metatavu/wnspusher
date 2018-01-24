from flask import Flask, request
from pony import orm
from datetime import datetime
import threading
import requests
import toml
import os
import time

class ConfigurationException(Exception):
    pass

if not 'WNS_PUSHER_CONFIG' in os.environ:
    raise ConfigurationException("Environment variable WNS_PUSHER_CONFIG not set")

_config = toml.load(os.environ['WNS_PUSHER_CONFIG'])

_db = orm.Database()

class Subscriber(_db.Entity):
    id = orm.PrimaryKey(int, auto=True)
    app = orm.Required(lambda: ClientApplication)
    channel_url = orm.Required(str, unique=True)
    subscribed = orm.Required(datetime)
    pending = orm.Set(lambda: PendingNotification)


class Message(_db.Entity):
    id = orm.PrimaryKey(int, auto=True)
    app = orm.Required(lambda: ClientApplication)
    content = orm.Required(str)
    launch_args = orm.Required(str)
    content_type = orm.Required(str)
    wns_type = orm.Required(str)
    pending = orm.Set(lambda: PendingNotification)


class PendingNotification(_db.Entity):
    id = orm.PrimaryKey(int, auto=True)
    subscriber = orm.Required(lambda: Subscriber)
    message = orm.Required(lambda: Message)
    issued = orm.Required(datetime)
    
    
class ClientApplication(_db.Entity):
    id = orm.PrimaryKey(int, auto=True)
    app_name = orm.Required(str)
    client_id = orm.Required(str)
    client_secret = orm.Required(str)
    tokens = orm.Set(lambda: WnsAccessToken)
    subscribers = orm.Set(lambda: Subscriber)
    messages = orm.Set(lambda: Message)
    api_keys = orm.Set(lambda: ApiKey)

    
class WnsAccessToken(_db.Entity):
    id = orm.PrimaryKey(int, auto=True)
    app = orm.Required(lambda: ClientApplication)
    issued = orm.Required(datetime)
    content = orm.Required(str)

    
class ApiKey(_db.Entity):
    id = orm.PrimaryKey(int, auto=True)
    app = orm.Required(lambda: ClientApplication)
    issued = orm.Required(datetime)
    key = orm.Required(str)


class NoSuchAppException(Exception):
    pass


def _do_post_request(url, data, headers=None):
    return requests.post(url, data=data, headers=headers)


def _ensure_access_token():
    for app in ClientApplication.select():
        if not WnsAccessToken.exists(app=app):
            data = {
                "grant_type": "client_credentials",
                "client_id": app.client_id,
                "client_secret": app.client_secret,
                "scope": _config["token_scope"],
            }
            result = _do_post_request(_config["token_url"], data=data)
            result_json = result.json()
            WnsAccessToken(app=app,
                           content=result_json.access_token,
                           issued=datetime.utcnow())


@orm.db_session
def add_subscription(channel_url):
    Subscriber(channel_url=channel_url, subscribed=datetime.utcnow())
    

@orm.db_session
def broadcast_notification(app_name, content, launch_args, content_type, wns_type):
    app = ClientApplication.get(app_name=app_name)
    if app is None:
        raise NoSuchAppException("No app named {}".format(app_name))
    message = Message(app=app,
                      content=content,
                      launch_args=launch_args,
                      content_type=content_type,
                      wns_type=wns_type)
    for sub in orm.select(s for s in Subscriber if s.app == app):
        PendingNotification(subscriber=sub, message=message)


def process_notification():
    with orm.db_session:
        _ensure_access_token()
    with orm.db_session:
        next_pending = (PendingNotification
                            .select()
                            .order_by(lambda p: p.issued)
                            .first())
        if next_pending is not None:
            message = next_pending.message
            subscriber = next_pending.subscriber
            app = message.app
            token = (WnsAccessToken
                        .select(lambda t: t.app==app)
                        .order_by(orm.desc(lambda t: t.issued))
                        .first())
            headers = {
                'Content-Type': message.content_type,
                'X-WNS-Type': message.wns_type,
                'Authorization': 'Bearer {}'.format(token.content),
            }
            data = message.content
            _do_post_request(subscriber.channel_url, data=data, headers=headers)
            next_pending.delete()


def worker_func():
    while True:
        time.sleep(_config["process_interval"])
        process_notification()


app = Flask(__name__)


@app.route("/subscribers", methods=['POST'])
def add_subscriber_endpoint():
    body = request.get_json()
    add_subscription(body.channel_url)


@app.route("/notifications", methods=['POST'])
def broadcast_notification_endpoint():
    body = request.get_json()
    broadcast_notification(app_name=body.app_name,
                           content=body.content,
                           launch_args=body.launchArgs,
                           content_type=body.contentType,
                           wns_type=body.wnsType)


@app.before_first_request
def init_app():
    if _config["db_provider"] == 'sqlite':
        _db.bind(provider='sqlite',
                filename=_config["db_filename"],
                create_db=_config["db_create"])
    elif _config["db_provider"] == 'postgres':
        _db.bind(provider='postgres',
                user=_config["db_user"],
                password=_config["db_password"],
                host=_config["db_host"],
                database=_config["db_database"])
    else:
        raise ConfigurationException("Database configured improperly")

    if _config["db_debug"]:
        orm.set_sql_debug(True)

    if _config["db_startup"]=='make_tables':
        _db.generate_mapping(create_tables=True)
    # elif _config["db_startup"]=='migrate': -- TODO
    #    _db.generate_mapping(create_tables=False)
    elif _config["db_startup"]=='none':
        _db.generate_mapping(create_tables=False)
    else:
        raise ConfigurationException("Database startup configured improperly")
        
    worker_thread = threading.Thread(target=worker_func)
    worker_thread.daemon = True
    worker_thread.start()