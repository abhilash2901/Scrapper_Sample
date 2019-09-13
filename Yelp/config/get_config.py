import os, errno, sys
import logging
from logging import StreamHandler, FileHandler
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))


class Config:

    def __init__(self, env):
        self.env = env

        self.API_HOST = '0.0.0.0'
        self.API_PORT = '3500'
        self.Proxy = {}

        basedir = os.path.abspath(os.path.dirname(__file__))
        self.logger = logging.getLogger('new-market-crawlers-flask')
        pardir = os.path.abspath(os.path.join(basedir, os.pardir))

        try:
            devlogfile = \
                '{root}/logs/new-market-crawlers-flask-{env}.log'.format(
                        root=pardir, env=self.env)
            if not os.path.exists(devlogfile):
                os.makedirs(os.path.dirname(devlogfile))
            logfile = devlogfile
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                print("Can't create log files!")
                pass

        self.logfile = logfile
        logFormatStr = '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s: %(module)s: %(funcName)s: %(lineno)d]'
        if self.env != "development":
            logging.basicConfig(format=logFormatStr, filename=self.logfile, level=logging.ERROR)
        else:
            logging.basicConfig(format=logFormatStr, filename=self.logfile, level=logging.DEBUG)
        self.formatter = logging.Formatter(logFormatStr, '%m-%d %H:%M:%S')
        file_handler = FileHandler(self.logfile)
        file_handler.setFormatter(self.formatter)
        self.logger.addHandler(file_handler)
        if self.env != "development":
            self.logger.setLevel(logging.ERROR)
        else:
            self.logger.setLevel(logging.DEBUG)

        if not os.path.exists(os.path.dirname(self.logfile)):
            try:
                os.makedirs(os.path.dirname(self.logfile))
                print("created log file : %s" % self.logfile)
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    self.logger.error("log file can't created")
                    pass

    def init_app(self, app):
        if self.env != "development":
            fh = logging.FileHandler(self.logfile)
            fh.setFormatter(self.formatter)
            app.logger.addHandler(fh)
        else:
            app.DEBUG = True
            fh = StreamHandler()
            fh.setLevel(logging.INFO)
            app.logger.setLevel(logging.INFO)
            app.logger.addHandler(fh)
