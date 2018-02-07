#     Push broker for Windows Notification Services
#     Copyright (C) 2018 Metatavu Oy
#
#     This program is free software: you can redistribute it and/or modify
#     it under the terms of the GNU Affero General Public License as
#     published by the Free Software Foundation, either version 3 of the
#     License, or (at your option) any later version.
#
#     This program is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU Affero General Public License for more details.
#
#     You should have received a copy of the GNU Affero General Public License
#     along with this program.  If not, see <https://www.gnu.org/licenses/>.

from flask import Flask, request, jsonify
from werkzeug.exceptions import BadRequest
from pony import orm
from datetime import datetime
import requests
import toml
import os
import time
import logging
import yoyo
import sys
import urllib.parse
import signal


PERMISSION_LEVEL_SUBSCRIBE = 1
PERMISSION_LEVEL_PUSH = 2


class ConfigurationException(Exception):
    pass


class UnauthorizedException(Exception):
    pass


class ForbiddenException(Exception):
    pass


class NoSuchAppException(Exception):
    pass


if 'WNS_PUSHER_CONFIG' in os.environ:
    _config = toml.load(os.environ['WNS_PUSHER_CONFIG'])
elif len(sys.argv) >= 2:
    _config = toml.load(sys.argv[1])
else:
    raise ConfigurationException("Configuration file location not set." +
                                 "Pass it as argv[1] or WNS_PUSHER_CONFIG" +
                                 "environment variable.")


_db = orm.Database()


logging.basicConfig(level=getattr(logging, _config['logging_level']),
                    stream=sys.stderr,
                    format="%(asctime)s - %(module)s:%(message)s")


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
    content_type = orm.Required(str)
    wns_type = orm.Required(str)
    pending = orm.Set(lambda: PendingNotification)


class PendingNotification(_db.Entity):
    id = orm.PrimaryKey(int, auto=True)
    subscriber = orm.Required(lambda: Subscriber)
    message = orm.Required(lambda: Message)
    issued = orm.Required(datetime, index=True)


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


def _do_post_request(url, data, headers=None):
    return requests.post(url, data=data, headers=headers)


def _refresh_access_token(app):
    orm.delete(t for t in WnsAccessToken if t.app == app)
    logging.info("invalid/missing access token for %s, fetching new",
                 app.app_name)
    data = {
        "grant_type": "client_credentials",
        "client_id": app.client_id,
        "client_secret": app.client_secret,
        "scope": _config["token_scope"],
    }
    result = _do_post_request(_config["token_url"], data=data)
    result_json = result.json()
    logging.debug("got access token: %s", result.text)
    if result.status_code == 200:
        WnsAccessToken(app=app,
                       content=result_json["access_token"],
                       issued=datetime.utcnow())


@orm.db_session
def add_subscriber(app_name, channel_url):
    app = ClientApplication.get(app_name=app_name)
    if app is None:
        raise NoSuchAppException("No app named {}".format(app_name))
    channel_url_parsed = urllib.parse.urlparse(channel_url)
    if not channel_url_parsed.netloc.endswith(_config["channel_url_domain"]):
        raise ValueError("Channel url doesn't point to " +
                         _config["channel_url_domain"])
    # Subject to a benign race condition; worst case is harmless error message
    subscriber = Subscriber.get(channel_url=channel_url)
    if subscriber is None:
        subscriber = Subscriber(app=app,
                                channel_url=channel_url,
                                subscribed=datetime.utcnow())
    return subscriber


@orm.db_session
def broadcast_notification(app_name,
                           content,
                           content_type,
                           wns_type):
    app = ClientApplication.get(app_name=app_name)
    if app is None:
        raise NoSuchAppException("No app named {}".format(app_name))
    message = Message(app=app,
                      content=content,
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
def key_permission_level(auth_header, app_name):
    parts = auth_header.split(" ")
    if len(parts) != 2:
        raise ValueError("Incorrect auth header")
    (auth_type, key) = parts
    if auth_type != "APIKEY":
        raise ValueError("Incorrect auth type")
    api_key_object = ApiKey.get(key=key)
    app = ClientApplication.get(app_name=app_name)
    if api_key_object is None:
        raise UnauthorizedException("API key doesn't exist")
    if api_key_object.app.id != app.id:
        raise UnauthorizedException("API key associated with wrong app")
    else:
        return api_key_object.permission_level


def process_notification():
    with orm.db_session:
        next_pending = (PendingNotification
                        .select()
                        .order_by(lambda p: p.issued)
                        .first())
        if next_pending is not None:
            logging.debug("Processing pending notification %s", next_pending)
            message = next_pending.message
            subscriber = next_pending.subscriber
            app = message.app
            token = (WnsAccessToken
                     .select(lambda t: t.app == app)
                     .order_by(orm.desc(lambda t: t.issued))
                     .first())
            if token is None:
                _refresh_access_token(app)
                return False
            headers = {
                'Content-Type': message.content_type,
                'X-WNS-Type': message.wns_type,
                'Authorization': 'Bearer {}'.format(token.content),
            }
            data = message.content
            result = _do_post_request(subscriber.channel_url,
                                      data=data,
                                      headers=headers)
            logging.debug("Got response for posting notification: %s",
                          (result.status_code, result.text, result.headers))
            is_ok = False
            purge = False
            if result.status_code == 401:
                _refresh_access_token(app)
            elif result.status_code == 410:
                # Channel expired
                purge = True
            elif result.status_code == 403:
                # Channel associated with wrong app
                logging.info("Channel associated with wrong app: {}"
                             .format(subscriber.channel_url))
                purge = True
            elif result.status_code == 404:
                # Invalid channel
                logging.info("Invalid channel URL: {}"
                             .format(subscriber.channel_url))
                purge = True
            elif result.status_code == 200:
                next_pending.delete()
                is_ok = True
            elif 500 <= result.status_code < 600:
                # Error in Microsoft Store, try again later
                pass
            else:
                logging.error(
                    "Unrecognized result from WNS: %s",
                    (result.status_code, result.text, result.headers))
                next_pending.delete()
            if purge:
                orm.delete(p for p in PendingNotification
                           if p.subscriber == subscriber)
                subscriber.delete()
            return is_ok
        else:
            return True


app = Flask(__name__)


def _check_permissions(target_level, app_name):
    if 'Authorization' not in request.headers:
        raise ValueError("Authorization header not set")
    auth_header = request.headers['Authorization']
    permission_level = key_permission_level(auth_header, app_name)
    if permission_level < target_level:
        raise UnauthorizedException("API key doesn't have enough permissions")


def _jsonify_error(error_msg, details):
    return jsonify({
        "error": error_msg,
        "details": str(details)
    })


@app.errorhandler(UnauthorizedException)
def handle_unauthorized(e):
    return _jsonify_error("Unauthorized request", e), 401


@app.errorhandler(ForbiddenException)
def handle_forbidden(e):
    return _jsonify_error("Forbidden request", e), 403


@app.errorhandler(ValueError)
@app.errorhandler(KeyError)
@app.errorhandler(BadRequest)
def handle_invalid(e):
    return _jsonify_error("Invalid input value", e), 400


@app.errorhandler(Exception)
def handle_error(e):
    logging.error("Internal server error: %s", e)
    return _jsonify_error("Internal server error", e), 500


@app.route("/subscribers", methods=['POST'])
def add_subscriber_endpoint():
    body = request.get_json()
    _check_permissions(PERMISSION_LEVEL_SUBSCRIBE, body["app"])
    logging.info("Adding subscriber %s to %s",
                 body["app"],
                 body["channelUrl"])
    add_subscriber(body["app"], body["channelUrl"])
    return jsonify({
        "app": body["app"],
        "channelUrl": body["channelUrl"]
    })


@app.route("/notifications", methods=['POST'])
def broadcast_notification_endpoint():
    body = request.get_json()
    _check_permissions(PERMISSION_LEVEL_PUSH, body["app"])
    logging.info("Sending notification %s to %s",
                 body["content"],
                 body["app"])
    info = broadcast_notification(app_name=body["app"],
                                  content=body["content"],
                                  content_type=body["contentType"],
                                  wns_type=body["wnsType"])
    return jsonify({
        "num_pending": info["num_pending"]
    })


@app.route("/ping", methods=['GET', 'POST'])
def ping_endpoint():
    return "pong"


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


def process_queue():
    init_app()
    finished = False
    if hasattr(signal, 'SIGTERM'):
        def abort(signum, frame):
            nonlocal finished
            finished = True # noqa
        signal.signal(signal.SIGTERM, abort)
    while not finished:
        time.sleep(_config["process_interval"])
        process_notification()


if __name__ == "__main__":
    # run in queue processing mode
    process_queue()
