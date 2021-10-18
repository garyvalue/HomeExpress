from typing import List
import mysql.connector
from requests import get
from tqdm import tqdm
import concurrent.futures
import time 
import pandas as pd
import csv

class syncDataBase:

    def refresh_data(self):
        self.build_BD()
        self.scrap_kwb_data()
        self.scrap_nfb_route()
        self.scrap_nfb_route_stop()
        self.scrap_nfb_stop()
        pass

    def build_BD(self):
        kwb_route = """
        CREATE TABLE kwb_route (
            co CHAR(4),
            route CHAR(4),
            bound CHAR(1),
            service_type CHAR(1),
            orig_en TEXT(255),
            orig_tc TEXT(255),
            orig_sc TEXT(255),
            dest_en TEXT(255),
            dest_tc TEXT(255),
            dest_sc TEXT(255),
            data_timestamp TEXT(30)
        )
        """

        kwb_stop = """
        CREATE TABLE kwb_stop (
            stop CHAR(16),
            name_en TEXT(255),
            name_tc TEXT(255),
            name_sc TEXT(255),
            lat TEXT(255),
            lng TEXT(255),
            data_timestamp TEXT(30)
        )
        """

        kwb_route_stop = """
        CREATE TABLE kwb_route_stop (
            co CHAR(4),
            route CHAR(4),
            bound CHAR(1),
            service_type CHAR(1),
            seq SMALLINT(3),
            stop CHAR(16),
            data_timestamp TEXT(30)
        )
        """

        ctb_route = """
        CREATE TABLE ctb_route(
            co CHAR(4),
            route CHAR(4),
            orig_en TEXT(255),
            orig_tc TEXT(255),
            orig_sc TEXT(255),
            dest_en TEXT(255),
            dest_tc TEXT(255),
            dest_sc TEXT(255),
            data_timestamp TEXT(30)
        )
        """

        ctb_route_stop = """
        CREATE TABLE ctb_route_stop(
            co CHAR(4),
            route CHAR(4),
            dir CHAR(1),
            seq SMALLINT,
            stop CHAR(16),
            data_timestamp TEXT(30)
        )
        """

        ctb_stop = """
        CREATE TABLE ctb_stop(
            stop CHAR(16),
            name_en TEXT(255),
            name_tc TEXT(255),
            name_sc TEXT(255),
            lat TEXT(255),
            lng TEXT(255),
            data_timestamp TEXT(30)
        )
        """
        
        nwfb_route = """
        CREATE TABLE nwfb_route(
            co CHAR(4),
            route CHAR(4),
            orig_en TEXT(255),
            orig_tc TEXT(255),
            orig_sc TEXT(255),
            dest_en TEXT(255),
            dest_tc TEXT(255),
            dest_sc TEXT(255),
            data_timestamp TEXT(30)
        )
        """

        nwfb_route_stop = """
        CREATE TABLE nwfb_route_stop(
            co CHAR(4),
            route CHAR(4),
            dir CHAR(1),
            seq SMALLINT,
            stop CHAR(16),
            data_timestamp TEXT(30)
        )
        """

        nwfb_stop = """
        CREATE TABLE nwfb_stop(
            stop CHAR(16),
            name_en TEXT(255),
            name_tc TEXT(255),
            name_sc TEXT(255),
            lat TEXT(255),
            lng TEXT(255),
            data_timestamp TEXT(30)
        )
        """
        nfb_route = """
        CREATE TABLE nfb_route(
            co CHAR(4),
            route CHAR(4),
            orig_en TEXT(255),
            orig_tc TEXT(255),
            orig_sc TEXT(255),
            dest_en TEXT(255),
            dest_tc TEXT(255),
            dest_sc TEXT(255),
            data_timestamp TEXT(30)
        )
        """

        nfb_route_stop = """
        CREATE TABLE nfb_route_stop(
            co CHAR(4),
            route CHAR(4),
            dir CHAR(1),
            seq SMALLINT,
            stop CHAR(16),
            data_timestamp TEXT(30)
        )
        """

        nfb_stop = """
        CREATE TABLE nfb_stop(
            stop CHAR(16),
            name_en TEXT(255),
            name_tc TEXT(255),
            name_sc TEXT(255),
            lat TEXT(255),
            lng TEXT(255),
            data_timestamp TEXT(30)
        )
        """
        
        mycursor.execute("SHOW TABLES")
        lst = [x[0] for x in mycursor]
        print(f'clean database')
        for i in tqdm(lst):
            if i != 'eta_data':
                mycursor.execute(f"DROP TABLE {i}")

        print('initate data table')
        for c in tqdm([kwb_route, kwb_route_stop, kwb_stop, ctb_route, ctb_route_stop, ctb_stop, nwfb_route, nwfb_route_stop, nwfb_stop, nfb_route, nfb_route_stop, nfb_stop]):
            mycursor.execute(c)

    def insert_data(self, url):
        try: 
            while True:
                try:
                    data = get(url).json()['data']
                    break
                except:
                    pass
            assert data
            if type(data) is not dict: 
                val = [tuple(d.values()) for d in tqdm(data, leave=False)]
                lst = [x for x in data[0].keys()]
            else: 
                val = [tuple(data.values())]
                lst = list(data.keys())
                
            col = "("

            s = "("
            for i in lst:
                if i == 'long': col += 'lng'
                else: col += i
                s += '%s'
                if lst.index(i) + 1 == len(lst): 
                    col += ")"
                    s += ")"
                else: 
                    col+= ", "
                    s+=", "
            sql = f"INSERT INTO {self.table} {col} VALUES {s}"
            mycursor.executemany(sql, val)
            mybd.commit()
            
        except AssertionError:
            pass
        
    def scrap_kwb_data(self):
        kwb_route = """
        CREATE TABLE kwb_route (
            co CHAR(4),
            route CHAR(4),
            bound CHAR(1),
            service_type CHAR(1),
            orig_en TEXT(255),
            orig_tc TEXT(255),
            orig_sc TEXT(255),
            dest_en TEXT(255),
            dest_tc TEXT(255),
            dest_sc TEXT(255),
            data_timestamp TEXT(30)
        )
        """

        kwb_stop = """
        CREATE TABLE kwb_stop (
            stop CHAR(16),
            name_en TEXT(255),
            name_tc TEXT(255),
            name_sc TEXT(255),
            lat TEXT(255),
            lng TEXT(255),
            data_timestamp TEXT(30)
        )
        """

        kwb_route_stop = """
        CREATE TABLE kwb_route_stop (
            co CHAR(4),
            route CHAR(4),
            bound CHAR(1),
            service_type CHAR(1),
            seq SMALLINT(3),
            stop CHAR(16),
            data_timestamp TEXT(30)
        )
        """
        
        dit = {
            'kwb_route' : 'https://data.etabus.gov.hk/v1/transport/kmb/route/', 
            'kwb_route_stop' : 'https://data.etabus.gov.hk/v1/transport/kmb/route-stop', 
            'kwb_stop' : 'https://data.etabus.gov.hk/v1/transport/kmb/stop', 
        }

        for sql, table in zip([kwb_route, kwb_stop, kwb_route_stop], 
                              ['kwb_route', 'kwb_stop', 'kwb_route_stop']
                            ):
            self.table = table
            if self.check_table(): self.clear_table()
            else: mycursor.execute(sql)
            self.insert_data(dit[table])

    def scrap_nfb_route(self):
        sql = """
        CREATE TABLE nfb_route(
            co CHAR(4),
            route CHAR(4),
            orig_en TEXT(255),
            orig_tc TEXT(255),
            orig_sc TEXT(255),
            dest_en TEXT(255),
            dest_tc TEXT(255),
            dest_sc TEXT(255),
            data_timestamp TEXT(30)
        )
        """
        self.table = 'nfb_route'
        if self.check_table(): self.clear_table()
        else: mycursor.execute(sql)
        
        for co in ['ctb', 'nwfb']:
            url = f'https://rt.data.gov.hk/v1/transport/citybus-nwfb/route/{co}'
            self.insert_data(url)
            
    def scrap_nfb_route_stop(self):
        sql = """
        CREATE TABLE nfb_route_stop(
            co CHAR(4),
            route CHAR(4),
            dir CHAR(1),
            seq SMALLINT,
            stop CHAR(16),
            data_timestamp TEXT(30)
        )
        """
        self.table = 'nfb_route_stop'
        if self.check_table(): self.clear_table()
        else: mycursor.execute(sql)
        
        mycursor.execute(f"""
            SELECT DISTINCT route, co 
            FROM nfb_route
        """
        )
        result = mycursor.fetchall()    
        for route, co in tqdm(result):
            for direction in ['inbound', 'outbound']:
                url = f'https://rt.data.gov.hk/v1/transport/citybus-nwfb/route-stop/{co}/{route}/{direction}'
                self.insert_data(url)
    
    def scrap_nfb_stop(self):
        sql = """
        CREATE TABLE nfb_stop(
            stop CHAR(16),
            name_en TEXT(255),
            name_tc TEXT(255),
            name_sc TEXT(255),
            lat TEXT(255),
            lng TEXT(255),
            data_timestamp TEXT(30)
        )
        """
        self.table = f'nfb_stop'
        if self.check_table(): self.clear_table()
        else: mycursor.execute(sql)
        mycursor.execute(f"""
            SELECT DISTINCT stop
            FROM nfb_route_stop
        """)
        result = mycursor.fetchall()

        for stop_id in tqdm(result):
            url = f'https://rt.data.gov.hk/v1/transport/citybus-nwfb/stop/{stop_id[0]}'
            self.insert_data(url)
            
    def get_mixed_route(self):
        sql = f"""
            CREATE TABLE mixed_route
            SELECT DISTINCT LEFT_T.route
            FROM kwb_route AS LEFT_T
            INNER JOIN(
                SELECT route
                FROM nfb_route
            ) AS RIGHT_T
            ON RIGHT_T.route = LEFT_T.route
            WHERE 
                LENGTH(LEFT_T.route)
            -	LENGTH(		
                    replace(
                        replace(
                            replace(
                                replace(
                                    replace(
                                        replace(
                                            replace(
                                                replace(
                                                    replace(
                                                        replace(
                                                            LEFT_T.route,'0',''
                                                            ),'1',''
                                                        ),'2',''
                                                    ),'3',''
                                                ),'4',''
                                            ),'5',''
                                        ),'6',''
                                    ),'7',''
                                ),'8',''
                            ),'9',''
                        )
                    ) >=3;
        """
        mycursor.execute(sql)
        mybd.commit()

    def check_table(self):
        mycursor.execute("SHOW TABLES")
        return self.table in [x[0] for x in mycursor]

    def clear_table(self):
        mycursor.execute(f"TRUNCATE {self.table}")
        
class eta_scrap:
    def __init__(self):
        self.table = 'eta_data'
        self.access_kwb()
        self.access_nfb()
        pass
    
    def build_bd(self):
        sql = """
        CREATE TABLE eta_data(
            co CHAR(4),
            route CHAR(4),
            dir CHAR(1),
            service_type CHAR(1),
            seq SMALLINT,
            stop CHAR(16),
            dest_tc TEXT(255),
            dest_sc TEXT(255),
            dest_en TEXT(255),
            eta TEXT(30),
            eta_seq TEXT(1),
            rmk_tc TEXT(30),
            rmk_sc TEXT(30),
            rmk_en TEXT(30),
            data_timestamp TEXT(30)
        )
        """
        mycursor.execute(sql)
        pass
        
    
    def record(self):
        while True:
            self.sql_kwb()
            for i in tqdm(range(50), leave=False):
                self.sql_nfb()
        pass
    
    def sql_kwb(self):
        series = self.kwb_df.sample(1).squeeze()
        url = f"https://data.etabus.gov.hk/v1/transport/kmb/route-eta/{series.route}/{series.service_type}"
        
        try: 
            while True:
                try:
                    data = get(url).json()['data']
                    break
                except:
                    pass
            assert data
            if type(data) is not dict: 
                val = [tuple(d.values()) for d in tqdm(data, leave=False)]
                lst = [x for x in data[0].keys()]
            else: 
                val = [tuple(data.values())]
                lst = list(data.keys())
                
            col = "("

            s = "("
            for i in lst:
                if i == 'long': col += 'lng'
                else: col += i
                s += '%s'
                if lst.index(i) + 1 == len(lst): 
                    col += ")"
                    s += ")"
                else: 
                    col+= ", "
                    s+=", "
            sql = f"INSERT INTO {self.table} {col} VALUES {s}"
            mycursor.executemany(sql, val)
            mybd.commit()
            
        except AssertionError:
            pass
        pass
    
    def sql_nfb(self):
        series = self.nfb_df.sample(1).squeeze()
        url = f'https://rt.data.gov.hk/v1/transport/citybus-nwfb/eta/{series.co}/{series.stop}/{series.route}'
        try: 
            while True:
                try:
                    data = get(url).json()['data']
                    break
                except:
                    pass
            assert data
            if type(data) is not dict: 
                val = [tuple(d.values()) for d in tqdm(data, leave=False)]
                lst = [x for x in data[0].keys()]
            else: 
                val = [tuple(data.values())]
                lst = list(data.keys())
                
            col = "("

            s = "("
            for i in lst:
                if i == 'long': col += 'lng'
                else: col += i
                s += '%s'
                if lst.index(i) + 1 == len(lst): 
                    col += ")"
                    s += ")"
                else: 
                    col+= ", "
                    s+=", "
            sql = f"INSERT INTO {self.table} {col} VALUES {s}"
            mycursor.executemany(sql, val)
            mybd.commit()
            
        except AssertionError:
            pass
        pass
    
    def access_kwb(self):
        sql = """
            SELECT service_type, route
            FROM kwb_route
        """
        mycursor.execute(sql)
        self.output = mycursor.fetchall()
        df = pd.DataFrame(self.output)
        df.columns = ['service_type', 'route']
        self.kwb_df = df.copy()
        pass
    
    def access_nfb(self):
        sql = """
            SELECT route, stop, co
            FROM nfb_route_stop
        """
        mycursor.execute(sql)
        self.output = mycursor.fetchall()
        df = pd.DataFrame(self.output)
        df.columns = ['route', 'stop', 'co']
        self.nfb_df = df.copy()
        pass
        

mybd = mysql.connector.connect(
    host="localhost",
    user='admin',
    password='Me.53007680',
    database='BUS_BD'
)

mycursor = mybd.cursor()
print('connected')
a = eta_scrap()
a.record()
# b.scrap_nfb_stop()
