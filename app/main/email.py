import os

from datetime import datetime
from flask import render_template
from flask_mail import Message
from threading import Thread

from app import app, mail


def async_send_mail(app, msg):
    with app.app_context():
        mail.send(msg)


def add_attachments(msg, attachments):
    root = attachments.pop(0)
    root = str(root.absolute())
    for attach in attachments:
        path = root + '/' + attach
        with app.open_resource(path) as fp:
            msg.attach(attach, "text/plain", fp.read())
    return msg


def send_email(subject, sender, recipients, text_body, html_body, attach=None):
    msg = Message(subject, sender=sender, recipients=recipients)
    msg.body = text_body
    msg.html = html_body
    if attach:
        msg = add_attachments(msg, attach)

    send = Thread(target=async_send_mail, args=[app, msg])
    send.start()
    return send


def load_automation_template(automation):
    now = datetime.now()
    today_noon = now.replace(hour=12, minute=0, second=0, microsecond=0)
    today_evening = now.replace(hour=17, minute=0, second=0, microsecond=0)

    if now < today_noon:
        when = "morning"
    elif now < today_evening:
        when = "afternoon"
    else:
        when = "evening"

    date = now.strftime("%m/%d/%Y")
    time = now.strftime("%H:%M:%S")

    text_template = render_template('email/automation_email.txt',
                                    when=when, automation=automation,
                                    date=date, time=time)
    html_template = render_template('email/automation_email.html',
                                    when=when, automation=automation,
                                    date=date, time=time)

    return text_template, html_template
