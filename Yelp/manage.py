import os

from flask_script import (Manager, Shell, Server)

from config.get_config import Config
from app import main

ENV = os.getenv('ENV') or 'development'
app = main(ENV)
manager = Manager(app)

# Run flask server
manager.add_command("runserver", Server(Config(ENV).API_HOST, port=Config(ENV).API_PORT))

def make_shell_context():
    return dict(app=app)

manager.add_command("shell", Shell(make_context=make_shell_context))

if __name__ == '__main__':
    manager.run()
