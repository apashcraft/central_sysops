import json

from datetime import datetime
from pathlib import Path
from pymongo import MongoClient, UpdateOne


class TopProcessor:
    """
    Processes top-level data based on prior loaded raw data
    """

    def __init__(self):
        self.data_path = Path('app/data')
        access_path = self.data_path / 'api_data.json'
        self.api_access = self.load_cache(access_path)
        # Create Mongo client
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

    def load_chef_tags(self, collection):
        """
        Pulls Chef tags from raw Chef data and loads them in top level
        data under chef_tags as an array.
        """

        bulk = []
        print("Processing Chef tags...")
        query = {'chef': {'$ne': None}}
        projection = {'_id': 1, 'chef.normal.tags': 1}

        servers = collection.find(query, projection)
        for server in servers:
            tags = server['chef']['normal']['tags']
            try:
                tags = [tag.lower() for tag in tags]
            except TypeError:
                pass
            query = {'_id': server['_id']}
            payload = {'$set': {'chef_tags': tags}}
            bulk.append(UpdateOne(query, payload))

        collection.bulk_write(bulk)
        return bulk

    def os_platform(self, collection):
        """
        Pulls operating system and platform from raw Chef data and loads
        them in top level data under platform and operating_system as strings
        """

        bulk = []
        print("Processing OS and platform...")
        query = {'chef': {'$ne': None}}
        projection = {
                        '_id': 1,
                        'chef.automatic': 1,
                     }

        servers = collection.find(query, projection)

        for server in servers:
            operating_system = server['chef']['automatic']['kernel']['name'].lower()
            if operating_system[0:9] == "microsoft":
                platform = "windows"
                operating_system = operating_system[10:].lower()
            else:
                platform = "linux"
                os_name = server['chef']['automatic']['platform'].lower()
                os_version = server['chef']['automatic']['platform_version'].lower()
                operating_system = f"{os_name} {os_version}"
            query = {'_id': server['_id']}
            payload = {'$set':
                       {'operating_system': operating_system,
                        'platform': platform}}
            bulk.append(UpdateOne(query, payload))

        collection.bulk_write(bulk)
        return bulk

    def network(self, collection):
        """
        Pulls all IPs from Chef raw data and loads them in top level data
        under ip_addresses as an array
        """

        bulk = []
        print("Processing network information...")
        query = {'chef': {'$ne': None}}
        projection = {
                        '_id': 1,
                        'chef.automatic.network.interfaces': 1,
                        'platform': 1
                     }
        servers = collection.find(query, projection)

        for server in servers:
            ips = []
            platform = server['platform'].lower()
            interfaces = server['chef']['automatic']['network']['interfaces']
            for interface in interfaces:
                try:
                    ip_address = interfaces[interface]['addresses'].keys()
                    if '127.0.0.1' not in ip_address:
                        for ip in ip_address:
                            family = interfaces[interface]['addresses'][ip]['family'].lower()
                            if family == "inet":
                                ips.append(ip.lower())
                except KeyError:
                    pass
            query = {'_id': server['_id']}
            payload = {'$set': {'ip': ips}}
            bulk.append(UpdateOne(query, payload))

        collection.bulk_write(bulk)
        return bulk

    def power_state(self, collection):
        """
        Pulls all power states from vCenter raw data and loads them in top
        level data under power_states as a string
        """

        bulk = []
        print("Processing power state...")
        query = {'vcenter': {'$ne': None}}
        projection = {
                        '_id': 1,
                        'vcenter.runtime.powerState': 1
                     }
        servers = collection.find(query, projection)

        for server in servers:
            power_state = server['vcenter']['runtime']['powerState']
            if power_state == 'poweredOn':
                power_state = 'on'
            else:
                power_state = 'off'
            query = {'_id': server['_id']}
            payload = {'$set': {'power_state': power_state}}
            bulk.append(UpdateOne(query, payload))

        collection.bulk_write(bulk)
        return bulk

    def ids(self, collection):
        """
        Pulls all UUIDs for each service from the respective raw data and
        loads them in top level data under a UUID dictionary
        """

        bulk = []
        print("Processing UUIDs...")
        query = {}
        projection = {
                        '_id': 1,
                        'chef.automatic.chef_guid': 1,
                        'solarwinds.Uri': 1,
                        'vcenter.vm': 1,
                        'vcenter.config': 1,
                        'servicenow.sys_id': 1
                     }
        servers = collection.find(query, projection)

        for server in servers:
            query = {'_id': server['_id']}
            try:
                chef_guid = server['chef']['automatic']['chef_guid'].lower()
            except KeyError:
                chef_guid = ""
            try:
                node_id = server['solarwinds']['Uri'].lower()
            except KeyError:
                node_id = ""
            try:
                moref_id = server['vcenter']['vm']
                moref_id = moref_id.replace('vim.VirtualMachine:', '').lower()
                instance_uuid = server['vcenter']['config']['instanceUuid'].lower()
                smbios_uuid = server['vcenter']['config']['uuid'].lower()
            except KeyError:
                moref_id = ""
                instance_uuid = ""
                smbios_uuid = ""
            vcenter = {
                       'MoRefID': moref_id,
                       'InstanceUuid': instance_uuid,
                       'SMBiosUuid': smbios_uuid
                    }
            try:
                sys_id = server['servicenow']['sys_id'].lower()
            except KeyError:
                sys_id = ""
            uuids = {
                     'chef_guuid': chef_guid,
                     'solarwinds_uri': node_id,
                     'vcenter': vcenter,
                     'servicenow_uuid': sys_id
                     }

            payload = {'$set': {'uuids': uuids}}

            bulk.append(UpdateOne(query, payload))

        collection.bulk_write(bulk)
        return bulk

    def bs_vcenter(self, collection):
        """
        Pulls the DTI_System tag value from vcenter_tags and loads them
        under business_system.vcenter as string
        """

        bulk = []
        print("Processing business systems: vCenter...")
        query = {'vcenter_tags': {'$ne': {}}}
        projection = {'_id': 1, 'vcenter_tags': 1}
        servers = collection.find(query, projection)

        for server in servers:
            query = {'_id': server['_id']}
            try:
                bs = server['vcenter_tags']['DTI_Systems'].lower()
            except KeyError:
                bs = ""
            payload = {'$set': {'business_system.vcenter': bs}}
            bulk.append(UpdateOne(query, payload))

        collection.bulk_write(bulk)
        return bulk

    def bs_chef(self, collection):
        """
        Pulls all values in chef_tags that begin with bs_ and loads them
        in an Array under business_system.chef
        """

        bulk = []
        print("Processing business systems: Chef...")
        query = {'chef_tags': {'$ne': None}}
        projection = {'_id': 1, 'name': 1, 'chef_tags': 1}

        servers = collection.find(query, projection)

        for server in servers:
            bs = []
            tags = server['chef_tags']
            try:
                for tag in tags:
                    if tag.startswith('bs_'):
                        bs.append(tag[3:].lower())
            except TypeError:
                pass
            query = {'_id': server['_id']}
            if len(bs) != 0:
                payload = {'$set': {'business_system.chef': bs}}
            else:
                payload = {'$set': {'business_system.chef': ""}}
            bulk.append(UpdateOne(query, payload))

        collection.bulk_write(bulk)
        return bulk

    def bs_solarwinds(self, collection):
        """
        Pulls the SolarWinds Business_Application value and loads it under
        business_system.solarwinds
        """

        bulk = []
        print("Processing business systems: Solarwinds...")
        query = {'solarwinds': {'$ne': None}}
        projection = {'_id':1, 'name': 1, 'solarwinds.Business_Application': 1}

        servers = collection.find(query, projection)
        for server in servers:
            bs = server['solarwinds']['Business_Application']
            try:
                bs.lower()
            except (AttributeError, TypeError):
                pass
            query = {'_id': server['_id']}
            payload = {'$set': {'business_system.solarwinds': bs}}
            bulk.append(UpdateOne(query, payload))

        collection.bulk_write(bulk)
        return bulk

    def chef_run_list(self, collection):
        """
        Pulls the Chef run_list data and updates it under chef_run_list in top
        """

        bulk = []
        query = {'chef': {'$ne': None}}
        projection = {'_id': 1, 'chef.run_list': 1}
        servers = collection.find(query, projection)

        for server in servers:
            run_list = server['chef']['run_list']
            run_list = [x.replace('role[', '').replace(']', '').lower() for x in run_list]
            query = {'_id': server['_id']}
            payload = {'$set': {'chef_run_list': run_list}}
            bulk.append(UpdateOne(query, payload))

        collection.bulk_write(bulk)
        return bulk

    def timestamp(self, collection):
        """
        Creates a now timestamp and updates the last_updated field
        """

        bulk = []
        query = {}
        projection = {'_id': 1}
        servers = collection.find(query, projection)

        print("Processing last updated timestamp")
        timestamp = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
        for server in servers:
            query = {'_id': server['_id']}
            payload = {"$set": {"last_updated": timestamp}}
            bulk.append(UpdateOne(query, payload))
        print("Done.")

        collection.bulk_write(bulk)
        return bulk

    def administration(self, collection):
        bulk = []
        query = {}
        projection = {'_id': 1, 'solarwinds': 1}
        servers = collection.find(query, projection)

        print("Processing administrative data...")
        for server in servers:
            try:
                tier = server['solarwinds']['ApplicationTier'].lower()
            except (AttributeError, TypeError):
                tier = ""
            try:
                department = server['solarwinds']['Department'].lower()
            except (AttributeError, TypeError):
                department = ""
            try:
                environment = server['solarwinds']['Environment'].lower()
            except (AttributeError, TypeError):
                environment = ""
            try:
                application = server['solarwinds']['Business_Application'].lower()
            except (AttributeError, TypeError):
                application = ""
            query = {'_id': server['_id']}
            payload = {'$set': {'application': application,
                                'department': department,
                                'environment': environment,
                                'tier': tier}}
            bulk.append(UpdateOne(query, payload))
        print("Done.")
        collection.bulk_write(bulk)
        return bulk

    def get_master(self):
        client = self.create_mongo_client
        servers_db = client.dti_servers
        master = servers_db.master
        return master

    def mass_update_master(self):
        """
        Runs all top level update functions on the master collection
        """
        dti_servers = self.mongo_client.dti_servers
        master_collection = dti_servers.master
        self.load_chef_tags(master_collection)
        self.os_platform(master_collection)
        self.network(master_collection)
        self.ids(master_collection)
        self.bs_vcenter(master_collection)
        self.bs_chef(master_collection)
        self.bs_solarwinds(master_collection)
        self.chef_run_list(master_collection)
        self.administration(master_collection)
        self.timestamp(master_collection)

        return True


def main():
    processor = TopProcessor()
    processor.mass_update_master()


if __name__ == "__main__":
    main()
