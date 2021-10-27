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


class database:
    # this class is for creating the data table for data_base
    def __init__(self) -> None:
        self.bus_data = ['route', 'route_stop', 'stop']
        self.bus_company = ['kwb', 'nfb']
        self.database_dict = self.get_initiate_dict()
        self.nfb_df = []
        self.kwb_df = []
        self.access_database()
        pass

    def initiate_table(self, table_name, sql_command, url) -> None:
        # this function will input the table name and sql command to access the sql server and implement web-scrapping action
        db_agent.execute(sql_command)

        # the following code is going to insert the data to database
        if type(url) is not list:
            data = self.request_data(url)
            self.insert_data(data, table_name) if data else None
        else:
            for link in tqdm(url):
                if table_name == 'nfb_route_stop':
                    for bound in ['/inbound', '/outbound']:
                        data = self.request_data(link+bound)
                        self.insert_data(data, table_name) if data else None
                else:
                    data = self.request_data(link)
                    self.insert_data(data, table_name) if data else None
        pass

    def get_initiate_dict(self) -> dict:
        return {'kwb_route':
                ["""
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
                    )""",
                 'https://data.etabus.gov.hk/v1/transport/kmb/route/'],
                'kwb_stop':
                    ["""
                    CREATE TABLE kwb_stop (
                        stop CHAR(16),
                        name_en TEXT(255),
                        name_tc TEXT(255),
                        name_sc TEXT(255),
                        lat TEXT(255),
                        lng TEXT(255),
                        data_timestamp TEXT(30)
                    )""",
                     'https://data.etabus.gov.hk/v1/transport/kmb/stop'
                     ],

                'kwb_route_stop':
                ["""
                CREATE TABLE kwb_route_stop (
                    co CHAR(4),
                    route CHAR(4),
                    bound CHAR(1),
                    service_type CHAR(1),
                    seq SMALLINT(3),
                    stop CHAR(16),
                    data_timestamp TEXT(30)
                )
                """,
                 'https://data.etabus.gov.hk/v1/transport/kmb/route-stop'
                 ],
                'nfb_route':
                ["""
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
                )""",
                 [f'https://rt.data.gov.hk/v1/transport/citybus-nwfb/route/{co}' for co in [
                     'ctb', 'nwfb']]
                 ],
                'nfb_route_stop':
                [["""
                    CREATE TABLE nfb_route_stop(
                    co CHAR(4),
                    route CHAR(4),
                    dir CHAR(1),
                    seq SMALLINT,
                    stop CHAR(16),
                    data_timestamp TEXT(30)
                )""",
                  """
                SELECT DISTINCT co, route 
                FROM nfb_route
                """],
                 'https://rt.data.gov.hk/v1/transport/citybus-nwfb/route-stop/'
                 ],
                'nfb_stop':
                [["""
                CREATE TABLE nfb_stop(
                    stop CHAR(16),
                    name_en TEXT(255),
                    name_tc TEXT(255),
                    name_sc TEXT(255),
                    lat TEXT(255),
                    lng TEXT(255),
                    data_timestamp TEXT(30)
                )""",
                  """
                SELECT DISTINCT stop
                FROM nfb_route_stop
                """],
                 'https://rt.data.gov.hk/v1/transport/citybus-nwfb/stop/'
                 ]
                }

    def initiate(self, key, value) -> None:
        # this function is going to go throught the whole process of initiate_database
        table_name = key
        sql_command, url = value
        if 'kwb' in key or key == 'nfb_route':
            self.initiate_table(table_name, sql_command, url)
        elif 'nfb' in key:
            db_agent.execute(sql_command[1])
            self.initiate_table(
                table_name,
                sql_command[0],
                [url + '/'.join(parameter)
                 for parameter in db_agent.fetchall()]
            )

    def table_refresh(self, table_name=None, restart=False) -> None:
        # this function is going to select the specific table to update
        if restart:
            for key, value in self.database_dict.items():
                self.clean_table(key)
                self.initiate(key, value)
        else:
            if (value := self.database_dict.get(table_name)) and table_name:
                self.initiate(table_name, value)
            pass
        self.refresh_mixed_list()
        pass

    def refresh_mixed_list(self):
        if self.check_table('mixed_route'):
            db_agent.execute(f"DROP TABLE mixed_route")
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
        db_agent.execute(sql)
        connection.commit()
        pass

    def clean_table(self, table_name):
        if self.check_table(table_name):
            db_agent.execute(f"DROP TABLE {table_name}")

    def insert_data(self, data, table_name):
        if type(data) is list:
            list_value = [list(d.values()) for d in tqdm(data, leave=False)]
            list_key = [key for key in data[0].keys()]
        else:
            list_value = list(data.values())
            list_key = list(data.keys())
        for index, value in enumerate(list_key):
            if value == 'long':
                list_key[index] = 'lng'
        table_column = ", ".join(list_key)
        column_format = ", ".join(['%s' for _ in enumerate(list_key)])
        if type(list_value[0]) is not list and type(list_value[0]) is not dict:
            list_value = [list_value]
        db_agent.executemany(f'''
            INSERT INTO {table_name} ({table_column}) 
            VALUES ({column_format})
            ''', list_value)
        connection.commit()

    def check_table(self, table_name):
        db_agent.execute("SHOW TABLES")
        return table_name in [table[0] for table in db_agent.fetchall()]

    def rebuild_database(self):
        db_agent.execute("SHOW TABLES")
        for i in db_agent.fetchall():
            db_agent.execute(f"DROP TABLE {i[0]}")

    def request_data(self, url):
        while True:
            try:
                data = get(url).json()['data']
                break
            except:
                pass
        return data

    def create_eta_table(self):
        # this function is going to create the eta table to store the eta data
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
        db_agent.execute(sql)
        pass

    def access_database(self):
        # I expect this function will execute the sql command to get the existing table of kwb and nfb for get eta process
        sql = """
            SELECT service_type, route
            FROM kwb_route
        """
        db_agent.execute(sql)
        self.output = db_agent.fetchall()
        df = pd.DataFrame(self.output)
        df.columns = ['service_type', 'route']
        self.kwb_df = df.copy()

        sql = """
            SELECT route, stop, co
            FROM nfb_route_stop
        """
        db_agent.execute(sql)
        self.output = db_agent.fetchall()
        df = pd.DataFrame(self.output)
        df.columns = ['route', 'stop', 'co']
        self.nfb_df = df.copy()
        pass

    def get_eta(self):
        # this function will receive the input of the company, and the route information for posting parameters to api

        for key, value in self.sample_route().items():
            if key == 'kwb':
                url = f"https://data.etabus.gov.hk/v1/transport/kmb/route-eta/{value.route}/{value.service_type}"
                if data := self.request_data(url):
                    self.insert_data(data, 'eta_data')
                pass
            elif key == 'nfb':
                for row in tqdm(value, leave=False):
                    url = f'https://rt.data.gov.hk/v1/transport/citybus-nwfb/eta/{row[0]}/{row[1]}/{row[2]}'
                    if data := self.request_data(url):
                        self.insert_data(data, 'eta_data')
                pass
        pass

    def sample_route(self):
        # this function will sample the row from the sql table to post the parameters to get_eta
        kwb_sample = self.kwb_df.sample(1).squeeze()
        sql = f"""
        SELECT co, stop, route
        FROM nfb_route_stop
        WHERE route = '{self.nfb_df.sample(1).squeeze().route}'
        """
        db_agent.execute(sql)
        nfb_sample = db_agent.fetchall()
        return {'kwb': kwb_sample, 'nfb': nfb_sample}
        pass


class multithread:

    def __init__(self, url_list) -> None:
        self.url_list = url_list
        self.data = []
        pass

    def act_requets(self, url) -> None:
        try:
            data = requests.get(url).json()['data']
            self.stored_data(data) if data else None
            time.sleep(2)

        except requests.exceptions.RequestException as e:
            print(e)
        pass

    def executor(self):
        thread = []
        with ThreadPoolExecutor(max_workers=2) as executor:
            for url in tqdm(self.url_list):
                thread.append(executor.submit(self.act_requets, url))
        pass

    def stored_data(self, data):
        self.data += data

    def return_data(self):
        return self.data


class user_database:
    def __init__(self, user_id) -> None:
        self.user_id = user_id
        pass

    def query(self):
        sql = f"""
        SELECT *
        FROM user_data
        WHERE user_id = {self.user_id}
        """
        db_agent.execute(sql)
        return db_agent.fetchall()

    def login(self):
        if data := self.query():
            return data
        else:
            return None
        pass

    def create_userbase(self):
        sql = """
        CREATE TABLE user_data
        (
        user_id
        user_name
        home_location
        location_A
        location_B
        )
        """
        db_agent.execute(sql)
        pass

    def registration(self, location, column='home_location'):
        sql = f"""
        INSERT INTO user_data ({self.user_id}, {column})
        VALUES ({location})
        """
        db_agent.execute(sql)
        connection.commit()

        pass

    def change_location(self, column, location):
        sql = f"""
        UPDATE user_data
        SET {column} = {location}
        WHERE user_id = {self.user_id}
        """
        pass


class control_panel:
    def __init__(self) -> None:
        print('control center')
        self.database = database()
        self.run = True
        pass

    def keyboard_input(self):
        self.keyboard = input('what action would you take ?\n')

    def control_flow(self) -> None:
        if self.keyboard == 'rebuild':
            if input('confirm? (y/n)') == 'y':
                self.database.rebuild_database()
                self.database.table_refresh()
        elif self.keyboard == 'create':
            if (table_name := input('which table that you want to create ?\n')) in self.database.database_dict:
                db_agent.execute(f"DROP TABLE {table_name}")
                self.database.table_refresh(table_name)
        elif self.keyboard == 'scrap':
            self.scrapping()
        elif self.keyboard == 'exit':
            self.run = False

    def scrapping(self):
        while True:
            self.database.get_eta()
        pass

    def control_main(self):
        while self.run:
            self.keyboard_input()
            self.control_flow()


class route_match:
    def __init__(self, home, user) -> None:
        self.user_lat = user[0]
        self.user_lng = user[1]
        self.home_lat = home[0]
        self.home_lng = home[1]
        self.distance = 0.003
        self.mixed_route = self.query_mixed_route()
        self.connector = db_agent.cursor()
        self.column = self.column_dict()
        pass

    def nfb_query(self):
        sql = f"""
            SELECT 
                LEFT_T.*, RIGHT_T.stop, RIGHT_T.name_tc, RIGHT_T.seq, RIGHT_T.distance
            FROM
                (SELECT 
                    T1.stop,
                        name_tc,
                        seq,
                        route,
                        dir,
                        co,
                        SQRT(POWER(lat - {self.user_lat}, 2) + POWER(lng - {self.user_lng}, 2)) AS distance
                FROM
                    nfb_stop AS T1
                RIGHT JOIN (SELECT 
                    seq, stop, route, dir, co
                FROM
                    nfb_route_stop) AS T2 ON T2.stop = T1.stop
                WHERE
                    SQRT(POWER(lat - {self.user_lat}, 2) + POWER(lng - {self.user_lng}, 2)) < {self.distance}
                GROUP BY T2.route , T2.dir
                ORDER BY distance) AS LEFT_T
                    LEFT JOIN
                (SELECT 
                    T1.stop,
                        name_tc,
                        seq,
                        route,
                        dir,
                        SQRT(POWER(lat - {self.home_lat}, 2) + POWER(lng - {self.home_lng}, 2)) AS distance
                FROM
                    nfb_stop AS T1
                LEFT JOIN (SELECT 
                    seq, stop, route, dir
                FROM
                    nfb_route_stop) AS T2 ON T2.stop = T1.stop
                WHERE
                    SQRT(POWER(lat - {self.home_lat}, 2) + POWER(lng - {self.home_lng}, 2)) < {self.distance}
                GROUP BY T2.route , T2.dir
                ORDER BY distance) AS RIGHT_T ON LEFT_T.route = RIGHT_T.route
            WHERE
                RIGHT_T.dir = LEFT_T.dir
                    AND RIGHT_T.route IS NOT NULL
                    AND LEFT_T.seq < RIGHT_T.seq;
        """

        db_agent.execute(sql)
        return db_agent.fetchall()

    def kwb_query(self):
        sql = f"""
            SELECT 
                LEFT_T.*, RIGHT_T.stop, RIGHT_T.name_tc, RIGHT_T.seq, RIGHT_T.distance
            FROM
                (SELECT 
                    T1.stop,
                        name_tc,
                        seq,
                        route,
                        bound,
                        service_type,
                        SQRT(POWER(lat - {self.user_lat}, 2) + POWER(lng - {self.user_lng}, 2)) AS distance
                FROM
                    kwb_stop AS T1
                RIGHT JOIN (SELECT 
                    seq, stop, route, bound, service_type
                FROM
                    kwb_route_stop) AS T2 ON T2.stop = T1.stop
                WHERE
                    SQRT(POWER(lat - {self.user_lat}, 2) + POWER(lng - {self.user_lng}, 2)) < {self.distance}
                GROUP BY T2.route , T2.bound
                ORDER BY distance) AS LEFT_T
                    LEFT JOIN
                (SELECT 
                    T1.stop,
                        name_tc,
                        seq,
                        route,
                        bound,
                        SQRT(POWER(lat - {self.home_lat}, 2) + POWER(lng - {self.home_lng}, 2)) AS distance
                FROM
                    kwb_stop AS T1
                LEFT JOIN (SELECT 
                    seq, stop, route, bound
                FROM
                    kwb_route_stop) AS T2 ON T2.stop = T1.stop
                WHERE
                    SQRT(POWER(lat - {self.home_lat}, 2) + POWER(lng - {self.home_lng}, 2)) < {self.distance}
                GROUP BY T2.route , T2.bound
                ORDER BY distance) AS RIGHT_T ON LEFT_T.route = RIGHT_T.route
            WHERE
                RIGHT_T.bound = LEFT_T.bound
                    AND RIGHT_T.route IS NOT NULL
                    AND LEFT_T.seq < RIGHT_T.seq;
        """

        db_agent.execute(sql)
        return db_agent.fetchall()

    def mixed_query(self):
        sql = """
        SELECT route
        FROM mixed_route
        """
        db_agent.execute(sql)
        return db_agent.fetchall()

    def output_data(self, query, co):
        val = {'1': [],
               '2': [],
               '3': [],
               'min': [],
               'rmk_tc_1': [],
               'rmk_tc_2': [],
               'rmk_tc_3': [],
               'locat': []}
        for row in query:
            for key, value in zip(self.column[query], row):
                val[key].append(value)
                if key == 'route': route = value
                elif key == 'co': co = value
                elif key == 'orig_stop': stop = value
                elif key == 'service_type': service_type = value
                
                if co == "KWB": 
                    val['co'].append(co)
                    url =  f'https://data.etabus.gov.hk/v1/transport/kmb/eta/{stop}/{route}/{service_type}'
                elif co == "NFB":
                    val['service_type'].append("_")
                    url = f'https://rt.data.gov.hk/v1/transport/citybus-nwfb/eta/{co}/{stop}/{route}'
                
                if d := self.request_eta(url):
                    for key, value in d.items(): val[key].append(value)
        return val
        pass
    
    def request_eta(self, url):
        try:
            while True:
                try:
                    data = get(url).json()['data']
                    break
                except:
                    pass
            eta_dict = {
                '1': "N.A.",
                '2': "N.A.",
                '3': "N.A.",
                'min': " ",
                'rmk_tc_1': " ",
                'rmk_tc_2': " ",
                'rmk_tc_3': " ",
                'locat': " "
            }
            assert data

            for row in data:
                if row['eta']:
                    eta_dict[str(row['eta_seq'])] = row['eta'][11:-9]
                    eta_dict[f'rmk_tc_{row["eta_seq"]}'] = row['rmk_tc']
                    if row['eta_seq'] == 1:
                        time = datetime.datetime.strptime(
                            row['eta'][:-9], "%Y-%m-%dT%H:%M") - datetime.datetime.now()
                        time = str(abs(time)).split(':')
                        time = int(time[0]) * 60 + int(time[1])
                        eta_dict['min'] = time
                        eta_dict['locat'] = row['dest_tc']

            return eta_dict
        except AssertionError:
            return eta_dict

    def column_dict(self):
        return {
            "KWB":
                ['orig_stop', 'orig_name_tc', 'orig_seq', 'route', 'bound', 'service_type',
                    'orig_distance', 'dest_stop', 'dest_name_tc', 'dest_seq', 'dest_distance', 'co'],
            "NFB":
                ['orig_stop', 'orig_name_tc', 'orig_seq', 'route', 'bound', 'co',
                 'orig_distance', 'dest_stop', 'dest_name_tc', 'dest_seq', 'dest_distance', 'service_type']
        }


if __name__ == '__main__':
    main = control_panel()
    main.control_main()
