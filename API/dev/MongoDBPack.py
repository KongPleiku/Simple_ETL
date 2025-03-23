import os
import gradio as gr
import pandas as pd
from pymongo import MongoClient

class MongdbConnection:
    def __init__(self, name: str, uri: str):
        self.name = name
        self.uri = uri
        self.connected = False

    def connect(self):
        try:
            self.client = MongoClient(self.uri)
            self.connected = True
            return True
        except Exception as e:
            print(e)
            self.connected = False
            return False
    
    def ListDataBases(self):
        return self.client.list_database_names()
    
    def ListCollections(self, database_name: str):
        return self.client[database_name].list_collection_names()
    
    def fetch_data(self, database_name: str, collection_name: str):
        if database_name and collection_name:
            client = self.client
            db = client[database_name]
            collection = db[collection_name]
            data = list(collection.find({})) 

            df = pd.DataFrame(data)
            if "_id" in df.columns:
                df.drop("_id", axis=1, inplace=True) 

            return df
        
        return pd.DataFrame()
    
    def fetch_data_batch(self,database_name, collection_name, i, batch_size):
        if database_name and collection_name:
            client = self.client
            db = client[database_name]
            collection = db[collection_name]
            data = list(collection.find({}).skip(i).limit(batch_size))  
            df = pd.DataFrame(data)
            if "_id" in df.columns:
                df.drop("_id", axis=1, inplace=True) 
            return df
        return pd.DataFrame()
    
    def fetch_data_preview(self, database_name, collection_name, limit):
        if database_name and collection_name:
            client = self.client
            db = client[database_name]
            collection = db[collection_name]
            data = list(collection.find({}).limit(limit)) 
            df = pd.DataFrame(data)
            if "_id" in df.columns:
                df.drop("_id", axis=1, inplace=True)  
            return df
        return pd.DataFrame()
    
    def count_documents(self,database_name, document_name):
        if database_name and document_name:
            client = self.client
            db = client[database_name]
            collection = db[document_name]
            count = collection.count_documents({})
            return count
        return 0
    
    def __str__(self):
        return "MongoDB"
    
class MongodbManager:
    def __init__(self):
        self.connection_Dict = {}
    
    def createConnection(self, ConnectionName: str, ConectionUri: str):
        temp = MongdbConnection(ConnectionName, ConectionUri)
        if temp.connect():
            self.connection_Dict[ConnectionName] = temp
            gr.Info("Created " + ConnectionName + " Connection and Successfull Connected")
            return True
        else:
            gr.Info("Failed to Create" + ConnectionName + " Connection")
            return False
    
    def getConnection(self, ConnectionName: str) -> MongdbConnection:
        return self.connection_Dict[ConnectionName]
    
    def showConnections(self):
        connection_list = []
        for name, connection in self.connection_Dict.items():
            status = "Connected" if connection.connected else "Disconnected"
            connection_list.append([name, connection.uri, status])
        return connection_list
    
    def listConnections(self):
        return list(self.connection_Dict.keys())
    
    def loadConnection(self):
        with open('D:\VS_prj\SparkETLRepo\Logic_Loom\API\dev\connection.txt', 'r') as file:
            for line in file:
                line = line.strip()
                if line:
                    name, uri = line.split(',', 1)
                    self.createConnection(name.strip(), uri.strip())