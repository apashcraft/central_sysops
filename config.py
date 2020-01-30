import os
basedir = os.path.abspath(os.path.dirname(__file__))


class Config(object):
    SECRET_KEY = os.environ.get('SECRET_KEY')
    MAIL_SERVER = ''
    MAIL_USE_TLS = os.environ.get('MAIL_USE_TLS') is not None
    #MAIL_USERNAME = os.environ.get('MAIL_USERNAME')
    #MAIL_PASSWORD = os.environ.get('MAIL_PASSWORD')
    ADMINS = ['adam.ashcraft@delaware.gov']
