from datetime import datetime
from flask import flash

from app.main.email import send_email, load_automation_template
from app.modules import reload_db, load_custom_properties, vsphere_powerstates
from app.modules import vsphere_old_new_os, run_splitter


def reload_db_task():
    flash("Reloading database...")
    reload_db.reload()
    flash("Completed reload.")

    return None


def custom_property_loader_task():
    flash("Copying SolarWinds custom properties based on Chef run lists...")
    load_custom_properties.load_all()
    flash("Complete.")

    return None


def vsphere_powerstates_task():
    flash("Pulling current powerstates from vSphere...")
    attachment = vsphere_powerstates.get_powerstates()
    flash("Complete. ")
    return attachment


def vsphere_old_new_task():
    flash("Pulling servers containing -old or -new")
    attachment = vsphere_old_new_os.find_old_new()
    return attachment


def run_splitter_task():
    run_splitter.run()
    flash("Complete.")

    return None


def test_task():
    time = datetime.now()
    flash(f"The time is {time}")

    return None


def run(selection):
    task = selection['task']
    result = 1
    attach = None
    if task == 'reload_db':
        attach = reload_db_task()
    elif task == 'custom_property_loader':
        attach = custom_property_loader_task()
    elif task == 'vsphere_powerstates':
        attach = vsphere_powerstates_task()
        automation = "vSphere Powerstates"
    elif task == 'vsphere_old_new':
        attach = vsphere_old_new_task()
    elif task == 'run_splitter':
        attach = run_splitter_task()
    elif task == 'test':
        attach = test_task()

    if selection['email']:
        text, html = load_automation_template(task)
        subject = f"Requested automation: {automation} Complete"
        sender = "central@sysops.gov"
        recipients = selection['email']
        if attach:
            send_email(subject, sender, recipients, text, html, attach=attach)
        else:
            send_email(subject, sender, recipients, text, html)

    return result
