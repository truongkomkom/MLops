import json
from pymongo.mongo_client import MongoClient
import pandas as pd


class MongodbOperation:

    def __init__(self, client_url: str, database_name: str, collection_name: str = None):
        self.client_url = client_url
        self.database_name = database_name
        self.collection_name = collection_name

    def create_client(self):
        return MongoClient(self.client_url)

    def create_database(self):
        client = self.create_client()
        return client[self.database_name]

    def create_collection(self, collection_name: str = None):
        database = self.create_database()
        collection_name = collection_name or self.collection_name
        if not collection_name:
            raise ValueError("Collection name must be provided")
        return database[collection_name]

    def insert_record(self, record: dict, collection_name: str = None):
        collection = self.create_collection(collection_name)
        if isinstance(record, list):
            if not all(isinstance(data, dict) for data in record):
                raise TypeError("All records in the list must be dictionaries")
            collection.insert_many(record)
        elif isinstance(record, dict):
            collection.insert_one(record)
        else:
            raise TypeError("Record must be a dictionary or a list of dictionaries")

    def bulk_insert(self, datafile: str, collection_name: str = None):
        if datafile.endswith('.csv'):
            data = pd.read_csv(datafile, encoding='utf-8')
        elif datafile.endswith('.xlsx'):
            data = pd.read_excel(datafile, encoding='utf-8')
        else:
            raise ValueError("File format not supported")

        datajson = json.loads(data.to_json(orient='records'))
        collection = self.create_collection(collection_name)
        collection.insert_many(datajson)



