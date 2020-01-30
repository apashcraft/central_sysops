import logging
import os

from flask import Flask
from flask_bootstrap import Bootstrap
from flask_mail import Mail
from flask_mongoengine import MongoEngine
from logging.handlers import SMTPHandler, RotatingFileHandler

from config import Config


app = Flask(__name__)
app.config.from_object(Config)
app.config['MONGODB_SETTINGS'] = {
    'host': ''
}

db = MongoEngine(app)
bootstrap = Bootstrap(app)
mail = Mail(app)


# register errors blueprint
from app.errors import bp as errors_bp
app.register_blueprint(errors_bp)

# register main blueprint
from app.main import bp as main_bp
app.register_blueprint(main_bp)


if not app.debug:
    if app.config['MAIL_SERVER']:
        auth = None
        if app.config['MAIL_USERNAME'] or app.config['MAIL_PASSWORD']:
            auth = (app.config['MAIL_USERNAME'], app.config['MAIL_PASSWORD'])
        secure = None
        if app.config['MAIL_USE_TLS']:
            secure = ()
        mail_handler = SMTPHandler(
            mailhost=(app.config['MAIL_SERVER']),
            fromaddr='no-reply@' + app.config['MAIL_SERVER'],
            toaddrs=app.config['ADMINS'], subject='SysOps Failure',
            credentials=auth, secure=secure)
        mail_handler.setLevel(logging.ERROR)
        app.logger.addHandler(mail_handler)
    if not os.path.exists('logs'):
        os.mkdir('logs')
    file_handler = RotatingFileHandler('logs/sysops.log', maxBytes=10240,
                                       backupCount=10)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d'))
    file_handler.setLevel(logging.INFO)
    app.logger.addHandler(file_handler)

    app.logger.setLevel(logging.INFO)
    app.logger.info('SysOps startup')


from app import models
from app.modules import *
