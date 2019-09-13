from flask import Flask, g
from flask_moment import Moment

from config.get_config import Config

def main(env):
    app = Flask(__name__)

    moment = Moment()
    app.config.from_object(Config(env))
    Config(env).init_app(app)
    moment.init_app(app)

    from spiders import spider
    app.register_blueprint(spider)

    return app
