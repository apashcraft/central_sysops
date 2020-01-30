import json

from bson import errors
from pathlib import Path
from pymongo import MongoClient, UpdateOne
from pyVmomi import vim, VmomiSupport


from app.modules.classes.interface import ChefInterface
from app.modules.classes.interface import ServiceNowInterface
from app.modules.classes.interface import SolarWindsInterface
from app.modules.classes.interface import vCenterInterface


class DataLoader:
    """
    Pulls raw data from each of the monitored APIs and loads them
    into the database
    """

    def __init__(self):
        self.data_path = Path('app/data')
        access_path = self.data_path / 'api_data.json'
        self.api_access = self.load_cache(access_path)
        self.mongo_client = self.create_mongo_client()

    def load_cache(self, path):
        """
        Loads the API access data into memory
        """
        with open(path, 'r') as f:
            data = json.loads(f.read())

        return data

    def create_mongo_client(self):
        """
        Creates the Mongo client required to interact with Mongo database
        """
        server = self.api_access['mongodb']['server']
        port = self.api_access['mongodb']['port']
        client = MongoClient(server, port)

        return client

    def select_collection(self, collection):
        db = self.api_access['mongodb']['database']
        server_db = self.mongo_client[db]
        return server_db[collection]


class SolarWindsLoader(DataLoader):

    def __init__(self):
        super().__init__()

    def create_solarwinds_client(self):
        """
        Creates the SolarWinds client to interact with SolarWinds API
        """
        sw = SolarWindsInterface()

        return sw

    def solarwinds_pull(self):
        """
        Pulls select raw per-server data from SolarWinds and loads in Mongo
        database
        """
        print("Updating SolarWinds data...")
        collection = super().select_collection('master')

        solarwinds_client = self.create_solarwinds_client()
        query_str = """
                    SELECT n.Node.NodeName, n.Node.Uri, n.ApplicationTier,
                    n.Business_Application, n.Business_Application_DL,
                    n.City, n.Comments, n.Core_App_1, n.Core_App_2,
                    n.Core_App_3, n.CPU_count,
                    n.Custom_Application_Management_DL,
                    n.Custom_Application_Monitor, n.Database_Management_Group,
                    n.Department, n.Device_Location, n.Duplicate_SUSID,
                    n.Environment, n.Hardware_Management_Group_DL,
                    n.IS_Virtual,
                    n.LifeCycle, n.ManagedBy, n.Manufacturer,
                    n.OS_Management_Group_DL, n.Patching_Schedule_Name,
                    n.Query,
                    n.Server_Role, n.Sub_Department, n.Support_Contact,
                    n.Support_Contact_Number, n.Support_Facility_Group,
                    n.TLS_SNI_Values, n.Vendor_Information, n.Verified_By,
                    n.Verified_Date
                    FROM Orion.NodesCustomProperties n
                    WHERE n.Node.NodeName !=''
                    """
        servers = (solarwinds_client.query(query_str))['results']
        bulk = []
        for server in servers:
            query = {"name": server['NodeName']}
            payload = {"$set": {"solarwinds": server}}
            bulk.append(UpdateOne(query, payload, upsert=True))

        collection.bulk_write(bulk)
        print("SolarWinds data updated.")
        return bulk


class ChefLoader(DataLoader):

    def __init__(self):
        super().__init__()

    def create_chef_client(self):
        """
        Creates the Chef Server client for interacting with Chef Server API
        """
        chef = ChefInterface()

        return chef

    def chef_pull(self):
        """
        Pulls raw per-server data from Chef Server and loads in Mongo database
        """
        print("Updating Chef data...")

        chef_client = self.create_chef_client()
        response = chef_client.chef_search(
                index='node',
                query="name:*")
        response = json.loads(response)
        servers = response['rows']

        collection = super().select_collection('master')

        bulk = []
        for server in servers:
            server['name'] = server['name'].lower().split('.', maxsplit=1)[0]
            server['name'] = server['name'].strip()
            if collection.count_documents({'name': server['name']}) >= 1:
                server = self.chef_ulong_max_cleaner(server)
                query = {'name': server['name']}
                payload = {"$set": {"chef": server}}
                try:
                    collection.update_one(query, payload)
                except errors.InvalidDocument:
                    server = self.package_null_char_cleaner(server)
                    collection.update_one(query, payload)

        print("Chef data updated.")

        return bulk

    def package_null_char_cleaner(self, node):
        """
        Removes null string characters from package names in Chef data,
        necessary for converting Chef data to Mongo format
        """
        packages = node['automatic']['packages']
        clean_packages = {}
        for package in packages.keys():
            clean_package = package.replace("\u0000", "")
            clean_packages[clean_package] = packages[package]
        node['automatic']['packages'] = clean_packages
        return node

    def chef_ulong_max_cleaner(self, node):
        """
        Converts the ULONG_MAX variable in the Chef node data to a string,
        necessary because ULONG_MAX is usually large enough that it breaks
        the Mongo 64-bit integer limit
        """
        try:
            node['automatic']['sysconf']['ULONG_MAX'] = str(node['automatic']['sysconf']['ULONG_MAX'])
            return node
        except KeyError:
            return node


class ServiceNowLoader(DataLoader):

    def __init__(self):
        super().__init__()

    def create_servicenow_client(self):
        """
        Creates the ServiceNow client for interacting with ServiceNow API
        """
        sn = ServiceNowInterface()

        return sn

    def servicenow_pull(self):
        """
        Pulls per-server data from ServiceNow API and calls servicenow_load
        function to load in Mongo database

        """
        print("Updating ServiceNow data...")

        collection = super().select_collection('master')

        servicenow_client = self.create_servicenow_client()
        server_frame = servicenow_client.server_audit()
        self.servicenow_load(collection, server_frame)
        server_frame = servicenow_client.server_audit(offset=4500)
        self.servicenow_load(collection, server_frame)

        print("ServiceNow data updated.")
        return True

    def servicenow_load(self, collection, server_frame):
        """
        Takes server data returned by ServiceNow and loads in Mongo database
        """

        bulk = []
        for server in server_frame.index:
            node_data = server_frame.loc[server].to_json()
            node_data = json.loads(node_data)
            name = node_data['name'].lower().split('.', maxsplit=1)[0]
            name = name.strip()
            if collection.count_documents({'name': name}) >= 1:
                query = {'name': name}
                payload = {"$set": {"servicenow": node_data}}
                bulk.append(UpdateOne(query, payload))

        collection.bulk_write(bulk)
        return bulk

    def bs_servicenow(self):
        """
        Calls a business system audit from the ServiceNow interface to
        retrieve every business system relationship in ServiceNow, checks
        the child in the relationship for a recognized server name, then sends
        business system to business_system.servicenow array if there's a
        match
        """

        servicenow_client = self.create_servicenow_client()
        bulk = []
        print("Updating business systems: ServiceNow...")
        ci_frame = servicenow_client.business_system_audit()

        collection = super().select_collection('master')

        for ci in ci_frame.index:
            system_data = ci_frame.loc[ci].to_json()
            system_data = json.loads(system_data)
            server = system_data['child'].lower().split('.', maxsplit=1)[0]
            server = server.strip()
            if collection.count_documents({'name': server}) >= 1:
                query = {'name': server}
                bs = system_data['parent'].lower()
                payload = {'$addToSet': {'business_system.servicenow': bs}}
                bulk.append(UpdateOne(query, payload))

        collection.bulk_write(bulk)
        print("ServiceNow business systems updated.")
        return ci_frame


class vCenterLoader(DataLoader):

    def __init__(self):
        super().__init__()
        self.vcenter_access = self.api_access['vcenter']
        self.vcenter = vCenterInterface()

    def vcenter_pull(self):
        """
        Calls the vsphere_pull function per vCenter server
        """

        collection = super().select_collection('master')

        for server in self.vcenter_access['servers']:
            vs = self.vcenter.create_vsphere_client(server)
            self.vsphere_pull(collection, vs, server)

        return True

    def vsphere_pull(self, collection, client, server):
        """
        Pulls per-server raw vSphere data and loads in Mongo database
        """

        bulk = []
        print(f"Updating vCenter data from {server}...")
        vm = None
        entity_stack = client.content.rootFolder.childEntity
        while entity_stack:
            vm = entity_stack.pop()
            try:
                vm_data = json.dumps(vm.summary,
                                     cls=VmomiSupport.VmomiJSONEncoder,
                                     sort_keys=True, indent=4)
                vm_data = json.loads(vm_data)
                name = vm_data['config']['name'].lower().split('.', maxsplit=1)[0]
                name = name.strip()
                if collection.count_documents({'name': name}) >= 1:
                    query = {'name': name}
                    payload = {"$set": {"vcenter": vm_data}}
                    bulk.append(UpdateOne(query, payload))
            except AttributeError:
                pass
            if hasattr(vm, "childEntity"):
                entity_stack.extend(vm.childEntity)
            elif isinstance(vm, vim.Datacenter):
                entity_stack.append(vm.vmFolder)

        collection.bulk_write(bulk)
        print(f"vCenter data from {server} updated.")
        return bulk

    def vcenter_tags(self):
        """
        Calls load_vc_tags for each vCenter server
        """

        collection = super().select_collection('master')

        for server in self.vcenter_access['servers']:
            vc = self.vcenter.create_vcloud_client(server)
            self.load_vc_tags(vc, collection, server)

        return True

    def load_vc_tags(self, vcloud, collection, server):
        """
        Calls get_vc_tags, then process tag, category, and node UUIDs
        and loads each tag per-server in Mongo database under vcenter_tags
        dictionary
        """

        bulk = []
        print(f"Updating vCenter tags from {server}...")
        tags, categories = self.vcenter.get_vc_tags(vcloud)
        processed_tags = self.vcenter.process_vc_tags(tags)

        for vm in processed_tags.keys():
            query = {"vcenter_morefid": vm}
            for tag in processed_tags[vm]:
                category = categories[tag]
                payload = {"$set": {f"vcenter_tags.{category.lower()}": tag.lower()}}
                bulk.append(UpdateOne(query, payload))

        collection.bulk_write(bulk)
        print(f"vCenter tags from {server} updated.")
        return bulk
