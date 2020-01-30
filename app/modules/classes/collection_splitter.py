import json

from pathlib import Path
from pymongo import UpdateOne

from app.modules.classes.interface import DatabaseInterface


class SplitCollections:

    def __init__(self, database):
        self.data_path = Path('app/data')
        access_path = self.data_path / 'api_data.json'
        self.api_access = self.load_cache(access_path)
        # Create Mongo client
        mongo = DatabaseInterface()
        self.client = mongo.create_mongo_client()
        self.database = self.client[database]

    def split(self, split, projection):
        print(f"Splitting {split} collection from master...")

        master = self.database.master
        split = self.database[f'{split}']

        query = {}
        servers = master.find(query, projection)
        bulk = []

        for server in servers:
            query = {'_id': server['_id']}
            payload = {'$set': server}
            old_data = split.find(query)
            #save_history(old_data)
            bulk.append(UpdateOne(query, payload, upsert=True))
            break

        split.bulk_write(bulk)
        return bulk

    def top_split(self):
        projection = {
                      'chef': 0,
                      'servicenow': 0,
                      'solarwinds': 0,
                      'vcenter': 0
                      }

        bulk = self.split("top", projection)

        return bulk

    def raw_split(self):
        projection = {
                      'name': 1,
                      'chef': 1,
                      'servicenow': 1,
                      'solarwinds': 1,
                      'vcenter': 1
                    }

        bulk = self.split("raw", projection)

        return bulk

    def save_history(self, old_data):
        pass

    def mass_update(self):
        self.top_split()
        self.raw_split()


def main():
    splitter = SplitCollections()
    splitter.mass_update()
    print("Done")


if __name__ == "__main__":
    main()
