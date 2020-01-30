from dateutil.parser import parse
from flask import render_template, request

from werkzeug.exceptions import BadRequestKeyError

from app.main import bp
from app.models import Server
from app.modules import manage_automations


@bp.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        try:
            search_str = request.form['search']
            return search(search_str)
        except IndexError:
            search_str = ""
            return search(search_str)
    return render_template('index.html', title='Home')


@bp.route('/search')
def search(search_str):
    if search_str == "":
        servers = []
    else:
        query = {}
        terms = search_str.split(',')
        for items in terms:
            parsed = items.split('=')
            key = parsed[0].strip()
            value = parsed[1].strip()
            query[key] = value
        servers = Server.objects(__raw__=query)
    return render_template('search.html', servers=servers)


@bp.route('/automations', methods=['GET', 'POST'])
def automations():
    tasks = [
            ['reload_db', 'Refresh database'],
            ['custom_property_loader', 'Chef to SolarWinds custom properties'],
            ['vsphere_powerstates', 'vSphere Powerstates'],
            ['vsphere_old_new', 'vSphere servers with old/new postscript'],
            ['run_splitter', 'Attempt test run in split collections'],
            ['test', 'Test']
        ]
    if request.method == 'POST':
        try:
            search_str = request.form['search']
            return search(search_str)
        except BadRequestKeyError:
            pass
        try:
            selection = {
                'task': request.form['automations']
            }
            email_address = request.form['email-address']
            if email_address:
                selection['email'] = [email_address]
            else:
                selection['email'] = None

            if request.form['schedule'] == 'schedule':
                date = request.form['date-schedule']
                time = request.form['time-schedule']
                selection['schedule'] = parse(f"{date} {time}")
            result = manage_automations.run(selection)

        except BadRequestKeyError:
            pass
    return render_template('automations.html', title='Automations', tasks=tasks)
