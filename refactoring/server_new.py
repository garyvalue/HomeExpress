import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import mysql.connector
from requests import get
import datetime
import time
import pandas as pd
from tqdm import tqdm

connection = mysql.connector.connect(
    host="localhost",
    user='admin',
    password='Me.53007680',
    database='BUS_BD'
)

db_agent = connection.cursor()

class bus_database:
    
    def restore(self, table_name: str = None):
        # it is a public class for impletement the restore action in sql
        pass
    
    class bus_dict:
        class kwb:
            def route(self, selection: str):
                pass
            def route_stop(self, selection: str):
                pass
            def stop(self, selection: str):
                pass
            
        class nfb:
            def route(self, selection: str):
                pass
            def route_stop(self, selection: str):
                pass
            def stop(self, selection: str):
                pass
        
        class eta:
            def table(self, selection: str):
                pass
            
        class mixed_route:
            def table(self, selection: str):
                pass
        
        
class user_database:
    def __init__(self, user_id: str):
        self.user_id = user_id
        self.exists = self._query() 
        self.locat_data = self.location(user_id) if self.exists else None
        pass
    
    def register(self, locat_data: tuples) -> None:
        pass
    
    def _query(self) -> Bool:
        pass
    
    class location:
        def __init__(self, user_id):
            pass
        
        def get(self) -> tuples:
            pass
        
        def change(self) -> None:
            pass
    
    def restore(self) -> None:
        pass
    
class table:
    def __init__(self, table_name, sql:List = [], data:List = [], columns:List = []):
        self.table_name = table_name
        self.sql = sql
        self.data = data
        self.columns = columns
        self.exists = self._find()      
            
    def create(self):
        pass
    
    def clean(self):
        pass
    
    def _find(self) -> Bool:
        pass
    
    def query(self):
        pass
    
    def update(self):
        pass
    
    def insert(self):
        pass
    
    def columns_praser(self):
        pass
    
    
class web_spider:
    def __init__(self, task_list:List = []):
        self.set_url = task_list
        
    def _scrap(self):
        pass
    
    def _result(self):
        pass
    
    def get(self):
        pass
    
    def multithread_scrap(self):
        pass
