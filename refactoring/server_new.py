import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import mysql.connector
from requests import get
import datetime
import time
import pandas as pd
from tqdm import tqdm

database = mysql.connector.connect(
    host="localhost",
    user='admin',
    password='Me.53007680',
    database='BUS_BD'
)

agent = database.cursor()

class bus_database:
    
    def restore(self, table_name: str = None):
        # this function will delete the table in sql then recreate such table for update
        if table_name: to_do = self.bus_dict.get(table_name)
        else: to_do = self.bus_dict.get()
            
        for table_name, parameters in to_do.items()
            table = sql_table(table_name)
            if table.exists: table.delete()
            table.create(parameters['column'])
            """
            [1.] take the value from bus_dict
            [2.] create a table
            3. scrap data
            4. insert data
            """
        pass
    
    class bus_dict:
          
        class kwb:
            def route(self, selection: str):
                url = 'https://data.etabus.gov.hk/v1/transport/kmb/route/'
                column_list = [
                    'co CHAR(4)',
                    'route CHAR(4)',
                    'bound CHAR(1)',
                    'service_type CHAR(1)',
                    'orig_en TEXT(255)',
                    'orig_tc TEXT(255)',
                    'orig_sc TEXT(255)',
                    'dest_en TEXT(255)',
                    'dest_tc TEXT(255)',
                    'dest_sc TEXT(255)',
                    'data_timestamp TEXT(30)'
                ]
                table_dict = {'url': list(url), 'column': column_list}
                return table_dict if selection == 'all' else table_dict.get(selection) 
                pass
            
            def route_stop(self, selection: str):
                url = 'https://data.etabus.gov.hk/v1/transport/kmb/route-stop'
                column_list = [
                    'co CHAR(4)',
                    'route CHAR(4)',
                    'orig_en TEXT(255)',
                    'orig_tc TEXT(255)',
                    'orig_sc TEXT(255)',
                    'dest_en TEXT(255)',
                    'dest_tc TEXT(255)',
                    'dest_sc TEXT(255)',
                    'data_timestamp TEXT(30)'
                ]
                table_dict = {'url': list(url), 'column': column_list}
                return table_dict if selection == 'all' else table_dict.get(selection) 
                pass
            
            def stop(self, selection: str):
                url = 'https://data.etabus.gov.hk/v1/transport/kmb/stop'
                column_list = [
                    'stop CHAR(16)',
                    'name_en TEXT(255)',
                    'name_tc TEXT(255)',
                    'name_sc TEXT(255)',
                    'lat TEXT(255)',
                    'lng TEXT(255)',
                    'data_timestamp TEXT(30)'
                ]
                table_dict = {'url': list(url), 'column': column_list}
                return table_dict if selection == 'all' else table_dict.get(selection) 
                pass
            
        class nfb:
            def route(self, selection: str):
                url = [f'https://rt.data.gov.hk/v1/transport/citybus-nwfb/route/{co}' for co in [
                     'ctb', 'nwfb']]
                column_list = [
                    'co CHAR(4)',
                    'route CHAR(4)',
                    'orig_en TEXT(255)',
                    'orig_tc TEXT(255)',
                    'orig_sc TEXT(255)',
                    'dest_en TEXT(255)',
                    'dest_tc TEXT(255)',
                    'dest_sc TEXT(255)',
                    'data_timestamp TEXT(30)'
                ]
                table_dict = {'url': url, 'column': column_list}
                return table_dict if selection == 'all' else table_dict.get(selection) 
                pass
            
            def route_stop(self, selection: str):
                column_list = [
                    'co CHAR(4)',
                    'route CHAR(4)',
                    'dir CHAR(1)',
                    'seq SMALLINT',
                    'stop CHAR(16)',
                    'data_timestamp TEXT(30)'
                ]
                url = self._generator(
                    'https://rt.data.gov.hk/v1/transport/citybus-nwfb/route-stop/', 
                    parameters=['co', 'route'],
                    table_name='nfb_route'
                )
                table_dict = {'url': url, 'column': column_list}
                return table_dict if selection == 'all' else table_dict.get(selection) 
                pass
            
            def stop(self, selection: str):
                column_list = [
                    'stop CHAR(16)',
                    'name_en TEXT(255)',
                    'name_tc TEXT(255)',
                    'name_sc TEXT(255)',
                    'lat TEXT(255)',
                    'lng TEXT(255)',
                    'data_timestamp TEXT(30)'
                ]
                url = self.generator(
                    'https://rt.data.gov.hk/v1/transport/citybus-nwfb/stop/',
                    parameters=['stop'],
                    table_name='nfb_route_stop'
                )
                table_dict = {'url': url, 'column': column_list}
                return table_dict if selection == 'all' else table_dict.get(selection) 
                pass
        
            def _generator(self, url, parameters, table_name):
                return [url+'/'.join(data) 
                        for data in table(
                    table_name=table_name, 
                    columns=parameter
                ).query()]
            
        class eta:
            def table(self, selection: str):
                pass
            
        class mixed_route:
            def table(self, selection: str):
                pass
            
        def get(self, table_name: str = '', selection: str = 'all'):
            indexes = {
                'kwb_route' : self.kwb.route(selection),
                'kwb_route_stop':self.kwb.route_stop(selection),
                'kwb_stop':self.kwb.stop(selection),
                'nfb_route': self.nfb.route(selection),
                'nfb_route_stop': self.nfb.route_stop(selection),
                'nfb_stop': self.nfb.stop(selection)
            } 
            return {talbe_name: indexes[table_name]} if table_name else indexes 
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
    
class sql_table:
    def __init__(self, table_name, sql:List = [], data:List = [], columns:List = [], condition:str = ''):
        self.table_name = table_name
        self.sql = sql
        self.data = data
        self.columns = columns
        self.condition = condition
        self.exists = self._find()      
            
    def create(self):
        sql = f"""
        CREATE TABLE {self.talbe_name}
        ({','.join(columns)})
        """
        agent.execute(sql)
        database.commit()
        pass
    
    def delete(self):
        sql = f"""
        DROP TABLE {self.table_name}
        """
        agent.execute(sql)
        database.commit()
        pass
    
    def _find(self) -> Bool:
        sql = f"""
        SHOW TABLES
        """
        agent.execute(sql)
        return self.table_name in agent.fetchall()
        pass
    
    def query(self):
        sql = f"""
        SELECT {self.columns}
        FROM {self.table_name}
        """
        if condition: sql+=f"WHERE {condition}"
        agent.execute(sql)
        return agent.fetchall()
        pass
    
    def update(self):
        sql = f"""
        UPDATE {self.table_name}
        SET {self.column} = {self.data}
        WHERE {self.condition}
        """
        
        pass
    
    def insert(self):
        sql = f"""
        INSERT INTO {self.table_name}
        ({self.columns})
        VALUE
        ({self.data})
        """
        agent.execute(sql)
        database.commit()
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

class control_panel:
    pass
