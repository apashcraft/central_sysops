import atexit
import datetime
import ijson
import json
import keyring
import pandas as pd
import requests
import six.moves.urllib.parse

from pathlib import Path
from requests.exceptions import HTTPError

from requests.packages.urllib3.exceptions import InsecureRequestWarning
from requests.packages.urllib3 import disable_warnings
disable_warnings(InsecureRequestWarning)


class Interface:

    def __init__(self, interface):
        self.data_path = Path('app/data/')
        self.access_path = self.data_path / 'api_data.json'
        self.access_data = self.load_cache(self.access_path, interface)

    def load_cache(self, path, interface):
        with open(path, 'r') as f:
            data = json.loads(f.read())
        data = data[interface]

        return data


class DatabaseInterface(Interface):
    from pymongo import MongoClient

    def __init__(self, database):
        interface = 'mongodb'
        super().__init__(interface)
        self.mongo_client = self.create_mongo_client

    def create_mongo_client(self):
        server = self.access_data['server']
        port = self.access_data['port']
        client = self.MongoClient(server, port)

        return client


class ChefInterface(Interface):

    from chef import api

    def __init__(self):
        interface = 'chef'
        super().__init__(interface)
        self.auth()

    def auth(self):
        user = self.access_data['username']
        pem = f"{self.data_path}/{self.access_data['pem']}"
        server = self.access_data['server']
        self.chef_server = self.api.ChefAPI(
            server,
            pem,
            user,
            ssl_verify=False)

    def chef_search(self, index=None, query=None, header={}):
        if query is None and index is None:
            self.search = '/search'
        elif query is None:
            self.search = '/search/' + index
        elif index is None:
            print("Please provide an index with your query.")
            self.search = '/search'
        else:
            self.query = dict(q=query, rows=2000, start=0)
            self.search = '/search/' + index + '?' + six.moves.urllib.parse.urlencode(self.query)
        response = self.chef_server.api_request('GET', self.search, header)
        return json.dumps(response, indent=4)

    def roles(self):
        end_point = '/roles'
        response = self.chef_server.api_request('GET', end_point)
        return response

    def role_run_list(self, role):
        end_point = f'/roles/{role}'
        response = self.chef_server.api_request('GET', end_point)
        run_list = response['run_list']
        return run_list


class SolarWindsInterface(Interface):
    from orionsdk import SwisClient

    def __init__(self):
        interface = 'solarwinds'
        super().__init__(interface)
        self.swis = self.auth()

    def auth(self):
        username = self.access_data['username']
        password = keyring.get_password('solarwinds', username)
        server = self.access_data['server']

        return self.SwisClient(server, username, password)

    def query(self, query_str, node=None):
        if node is None:
            self.results = self.swis.query(query_str)
        else:
            self.results = self.swis.query(query_str % str(tuple(node)))

        return self.results

    def change_custom_properties(self, uri, updated_props):
        self.swis.update(uri + '/CustomProperties', **updated_props)

        return 1


class ServiceNowInterface(Interface):
    import pysnow

    def __init__(self):
        interface = 'servicenow'
        super().__init__(interface)
        self.sn = self.create_client()
        if Path(self.data_path / 'uuids_to_names.json').is_file():
            with open(self.data_path / 'uuids_to_names.json', 'r') as f:
                self.uuid_names = json.loads(f.read())
        else:
            self.uuid_names = {}

    def get_token(self):
        token_url = f"https://{self.access['instance']}.service-now.com/oauth_token.do"
        response = requests.post(token_url, data=self.access, proxies=self.proxies)
        token_data = response.json()

        print(json.dumps(token_data, indent=4))
        return token_data['refresh_token'], token_data['access_token'], token_data['token_type']

    def update_tokens(self):
        refresh_token, access_token, token_type = self.get_token()

        tokens = {
            "refresh_token": refresh_token,
            "access_token": access_token,
            "timestamp": str(datetime.datetime.now()),
            "token_type": token_type
        }

        return tokens

    def create_client(self):
        self.access = self.access_data['access']
        client_secret = keyring.get_password("servicenow",
                                             self.access['client_id'])
        password = keyring.get_password("servicenow", self.access['username'])
        self.access['client_secret'] = client_secret
        self.access['password'] = password
        self.proxies = self.access_data['proxies']
        session = requests.Session()
        session.proxies.update(self.proxies)
        self.tokens = self.update_tokens()
        headers = {
            "Accept": "application/json",
            "Authorization": f"{self.tokens['token_type']} {self.tokens['access_token']}"
        }
        session.headers.update(headers)
        session.verify = False
        sn = self.pysnow.Client(instance=self.access['instance'], session=session)

        return sn

    def uuid_name_match(self, uuid):
        api_address = f"https://{self.access['instance']}.service-now.com/api/now"

        # Pull uuid from full record address
        if api_address in uuid:
            uuid = uuid.replace(api_address, '')

        if uuid in self.uuid_names.keys():  # Cache already contains UUID name
            return self.uuid_names[uuid]
        else:  # Cache does not contain UUID name, call API for record.
            resource = self.sn.resource(api_path=uuid)
            response = resource.get()
            record = response.all()
            self.uuid_names[uuid] = record[0]['name']
            return self.uuid_names[uuid]

    def record_cleaning(self, fields, record):
        for field in fields:
            if not record[field] == '':
                record[field] = self.uuid_name_match(record[field]['link'])

        return record

    def save_uuid_record(self):
        with open(Path(self.data_path / 'uuids_to_names.json'), 'w') as f:
            json.dump(self.uuid_names, f, indent=4)

        return 0

    def server_audit(self, servers=None, offset=None):
        resource = self.sn.resource(chunk_size=8192, api_path='/table/cmdb_ci_server')
        if servers is not None:
            servers = map(lambda server: server.lower(), servers)
        try:
            if offset is None:
                response = resource.get(query={}, stream=True)
            else:
                response = resource.get(query={}, stream=True, offset=offset)

            records = {}
            fields_clean = ['support_group', 'u_administrator_group',
                            'department', 'u_data_custodian_group']
            count = 0
            for record in response.all():
                record = self.record_cleaning(fields_clean, record)
                record['name'] = record['name'].lower()
                records[record['name']] = record
                count += 1
        except ijson.backends.python.UnexpectedSymbol:
            pass

        drop_values = ['asset', 'sys_domain', 'assigned_to',
                       'u_subnet']
        frame = pd.DataFrame(records)
        frame = frame.T
        try:
            frame = frame.drop(drop_values, axis=1)
        except KeyError:
            pass
        self.save_uuid_record()
        if servers is None:
            return frame
        else:
            frame = frame[frame['name'].isin(servers)]
            return frame

    def business_system_audit(self, offset=None):
        resource = self.sn.resource(chunk_size=8192, api_path='/table/cmdb_rel_ci')
        if offset is None:
            response = resource.get(query={}, stream=True)
        else:
            response = resource.get(query={}, stream=True, offset=offset)

        records = {}
        fields_clean = ['parent', 'child', 'type']
        try:
            for record in response.all():
                try:
                    record = self.record_cleaning(fields_clean, record)
                    records[record['parent']] = record
                except HTTPError:
                    pass
        except ijson.backends.python.UnexpectedSymbol:
            pass

        frame = pd.DataFrame(records)
        frame = frame.T
        self.save_uuid_record()
        return frame


class vCenterInterface(Interface):

    from pyVim import connect
    from pyVmomi import vim, VmomiSupport
    from vmware.vapi.vsphere.client import create_vsphere_client

    def __init__(self):
        interface = 'vcenter'
        super().__init__(interface)
        self.username = self.access_data['username']
        self.password = keyring.get_password('vcenter', self.username)

    def get_vs_servers(self):
        return self.access_data['servers']

    def create_vsphere_client(self, server):
        vs = self.connect.SmartConnect(host=server,
                                       user=self.username,
                                       pwd=self.password)
        atexit.register(self.connect.Disconnect, vs)

        return vs

    def create_vcloud_client(self, server):
        session = requests.session()
        session.verify = False
        vc = self.create_vsphere_client(
            server=server,
            username=self.username,
            password=self.password,
            session=session
        )

        return vc

    def get_all_nodes(self, vs):
        vm = None
        entity_stack = vs.content.rootFolder.childEntity
        servers = []
        while entity_stack:
            vm = entity_stack.pop()
            try:
                vm_data = json.dumps(vm.summary,
                                     cls=self.VmomiSupport.VmomiJSONEncoder,
                                     sort_keys=True, indent=4)
                vm_data = json.loads(vm_data)
                servers.append(vm_data)
            except AttributeError:
                pass
            if hasattr(vm, "childEntity"):
                entity_stack.extend(vm.childEntity)
            elif isinstance(vm, self.vim.Datacenter):
                entity_stack.append(vm.vmFolder)
        return servers

    def get_vc_tags(self, vc):
        """
        Runs through each tag category, pulls down tag and vm data, then
        returns processed tag and category data for tags that contain
        at least one server association
        """
        tag_ids = []

        categories = vc.tagging.Category.list()
        category_per_tag = {}
        for category in categories:
            cat_info = vc.tagging.Category.get(category)
            cat_name = cat_info.name
            tag_list = vc.tagging.Tag.list_tags_for_category(category)
            for tag in tag_list:
                tag_name = vc.tagging.Tag.get(tag).description
                category_per_tag[tag_name] = cat_name
            tag_ids.append(vc.tagging.Tag.list_tags_for_category(category))
        tags = {}
        vm_tag = "vm-"
        for tag_id in tag_ids:
            for tag in tag_id:
                tag_node_ids = [node.id for node in vc.tagging.TagAssociation.list_attached_objects(tag)]
                tag_name = vc.tagging.Tag.get(tag).description
                if any(vm_tag in s for s in tag_node_ids):
                    tags[tag_name] = tag_node_ids

        return tags, category_per_tag

    def process_vc_tags(self, tags):
        vm_tags = {}
        for tag in tags.keys():
            vms = tags[tag]
            for vm in vms:
                if vm not in vm_tags.keys():
                    vm_tags[vm] = [tag]
                else:
                    vm_tags[vm].append(tag)

        return vm_tags
