from flask import Flask, request, jsonify
from pony import orm
from datetime import datetime
from enum import IntEnum
import threading
import requests
import toml
import os
import time
import logging
import yoyo


PERMISSION_LEVEL_SUBSCRIBE = 1
PERMISSION_LEVEL_PUSH = 2


class ConfigurationException(Exception):
    pass


class UnauthorizedException(Exception):
    pass


class ForbiddenException(Exception):
    pass


if 'WNS_PUSHER_CONFIG' not in os.environ:
    raise ConfigurationException(
        "Environment variable WNS_PUSHER_CONFIG not set")


_config = toml.load(os.environ['WNS_PUSHER_CONFIG'])


_db = orm.Database()


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logging.basicConfig(level=logging.DEBUG)


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
    permission_level = orm.Required(int)
    issued = orm.Required(datetime)
    key = orm.Required(str, unique=True)


class NoSuchAppException(Exception):
    pass


def _do_post_request(url, data, headers=None):
    return requests.post(url, data=data, headers=headers)


def _ensure_access_token():
    for app in ClientApplication.select():
        if not WnsAccessToken.exists(app=app):
            logger.info("no access token for %s, fetching new...",
                        app.app_name)
            data = {
                "grant_type": "client_credentials",
                "client_id": app.client_id,
                "client_secret": app.client_secret,
                "scope": _config["token_scope"],
            }
            result = _do_post_request(_config["token_url"], data=data)
            result_json = result.json()
            logger.info("got access token: %s", result.text)
            WnsAccessToken(app=app,
                           content=result_json["access_token"],
                           issued=datetime.utcnow())


@orm.db_session
def add_subscription(app_name, channel_url):
    app = ClientApplication.get(app_name=app_name)
    if app is None:
        raise NoSuchAppException("No app named {}".format(app_name))
    return Subscriber(app=app,
                      channel_url=channel_url,
                      subscribed=datetime.utcnow())


@orm.db_session
def broadcast_notification(app_name,
                           content,
                           launch_args,
                           content_type,
                           wns_type):
    app = ClientApplication.get(app_name=app_name)
    if app is None:
        raise NoSuchAppException("No app named {}".format(app_name))
    message = Message(app=app,
                      content=content,
                      launch_args=launch_args,
                      content_type=content_type,
                      wns_type=wns_type)
    num_pending = 0
    for sub in orm.select(s for s in Subscriber if s.app == app):
        PendingNotification(subscriber=sub,
                            message=message,
                            issued=datetime.utcnow())
        num_pending += 1
    return {"num_pending": num_pending}


@orm.db_session
def key_permission_level(auth_header):
    parts = auth_header.split(" ")
    if len(parts) != 2:
        raise ValueError("Incorrect auth_header")
    (auth_type, key) = parts
    if auth_type != "APIKEY":
        raise ValueError("Incorrect auth type")
    api_key_object = ApiKey.get(key=key)
    if api_key_object is None:
        return None
    else:
        return api_key_object.permission_level


def process_notification():
    with orm.db_session:
        _ensure_access_token()
    with orm.db_session:
        next_pending = (PendingNotification
                        .select()
                        .order_by(lambda p: p.issued)
                        .first())
        if next_pending is not None:
            logger.info("processing pending notification %s...", next_pending)
            message = next_pending.message
            subscriber = next_pending.subscriber
            app = message.app
            token = (WnsAccessToken
                     .select(lambda t: t.app == app)
                     .order_by(orm.desc(lambda t: t.issued))
                     .first())
            headers = {
                'Content-Type': message.content_type,
                'X-WNS-Type': message.wns_type,
                'Authorization': 'Bearer {}'.format(token.content),
            }
            data = message.content
            result = _do_post_request(subscriber.channel_url,
                                      data=data,
                                      headers=headers)
            logger.info("got response for posting notification: %s",
                        (result.status_code, result.text, result.headers))
            next_pending.delete()


def worker_func():
    while True:
        time.sleep(_config["process_interval"])
        process_notification()


app = Flask(__name__)


def check_permissions(target_level):
    if 'Authorization' not in request.headers:
        raise ValueError("Authentication header not set")
    auth_header = request.headers['Authorization']
    permission_level = key_permission_level(auth_header)
    if permission_level is None:
        raise UnauthorizedException("Invalid API key")
    if permission_level < target_level:
        raise UnauthorizedException("API key doesn't have enough permissions")


def jsonify_error(error_msg, details):
    return jsonify({
        "error": error_msg,
        "details": str(details)
    })


@app.errorhandler(UnauthorizedException)
def handle_unauthorized(e):
    return jsonify_error("Unauthorized request", e), 401


@app.errorhandler(ForbiddenException)
def handle_forbidden(e):
    return jsonify_error("Forbidden request", e), 403


@app.errorhandler(ValueError)
@app.errorhandler(KeyError)
def handle_invalid(e):
    return jsonify_error("Invalid input value", e), 400


@app.errorhandler(Exception)
def handle_error(e):
    return jsonify_error("Internal server error", e), 500


@app.route("/subscribers", methods=['POST'])
def add_subscriber_endpoint():
    check_permissions(PERMISSION_LEVEL_SUBSCRIBE)
    body = request.get_json()
    logger.info("Adding subscriber %s to %s",
                body["app"],
                body["channelUrl"])
    new_sub = add_subscription(body["app"], body["channelUrl"])
    return jsonify({
        "app": new_sub.app,
        "channelUrl": new_sub.channel_url
    })


@app.route("/notifications", methods=['POST'])
def broadcast_notification_endpoint():
    check_permissions(PERMISSION_LEVEL_PUSH)
    body = request.get_json()
    logger.info("Sending notification %s to %s",
                body["content"],
                body["appName"])
    info = broadcast_notification(app_name=body["appName"],
                                  content=body["content"],
                                  launch_args=body["launchArgs"],
                                  content_type=body["contentType"],
                                  wns_type=body["wnsType"])
    return jsonify({
        "num_pending": info["num_pending"]
    })


@app.before_first_request
def init_app():
    if _config["db_provider"] == 'sqlite':
        _db.bind(provider='sqlite',
                 filename=_config["db_filename"],
                 create_db=_config["db_create"])
        migration_url = 'sqlite:///{}'.format(_config['db_filename'])
    elif _config["db_provider"] == 'postgres':
        _db.bind(provider='postgres',
                 user=_config["db_user"],
                 password=_config["db_password"],
                 host=_config["db_host"],
                 database=_config["db_database"])
        migration_url = 'postgresql://{}:{}@{}/{}'.format(
            _config["db_user"],
            _config["db_password"],
            _config["db_host"],
            _config["db_database"])
    else:
        raise ConfigurationException("Database configured improperly")

    if _config["db_debug"]:
        orm.set_sql_debug(True)

    if _config["db_startup"] == 'make_tables':
        _db.generate_mapping(create_tables=True)
    elif _config["db_startup"] == 'migrate':
        backend = yoyo.get_backend(migration_url)
        migrations = yoyo.read_migrations(_config['db_migrations_dir'])
        backend.apply_migrations(backend.to_apply(migrations))
        _db.generate_mapping(create_tables=False)
    elif _config["db_startup"] == 'none':
        _db.generate_mapping(create_tables=False)
    else:
        raise ConfigurationException("Database startup configured improperly")

    worker_thread = threading.Thread(target=worker_func)
    worker_thread.daemon = True
    worker_thread.start()
