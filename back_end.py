import mysql.connector
from requests import get
import datetime
import time
import pandas as pd


bd = mysql.connector.connect(
    host="localhost",
    user='admin',
    password='Me.53007680',
    database='BUS_BD'
)

BD = bd.cursor()

class match:
    
    def __init__(self, user, home = (22.2797084,114.1816558)) -> None:
        self.user_lat = user[0]
        self.user_lng = user[1]
        self.home_lat = home[0]
        self.home_lng = home[1]
        self.distance = 0.003
        self.mixed_route = self.query_mixed_route()
        self.connector = bd.cursor()
        pass
    
    def get_nfb_match(self):
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
        
        BD.execute(sql)
        self.output = BD.fetchall()
        self.co = "NFB"
        if self.output: self.convert_output()
        return self.output
    
    def get_kwb_match(self):
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
                
        BD.execute(sql)
        self.output = BD.fetchall()
        self.co = "KWB"
        if self.output: self.convert_output()
        return self.output
    
    def get_match_result(self):
        kwb=self.get_kwb_match()
        nfb=self.get_nfb_match()
        if all([kwb, nfb]):
            for key, value in kwb.items():
                nfb[key] += value
            # here we need to complete the clean up process
            # if the route in sql list, then we execute the replace process
            return nfb
        elif any([nfb, kwb]):
            if kwb: return kwb
            else: return nfb
        else:
            return None
    
    def convert_output(self):
        key_dict = {
            "KWB" : 
                ['orig_stop', 'orig_name_tc', 'orig_seq', 'route', 'bound', 'service_type',
                'orig_distance', 'dest_stop', 'dest_name_tc', 'dest_seq', 'dest_distance', 'co']
            ,
            "NFB" : 
                ['orig_stop', 'orig_name_tc', 'orig_seq', 'route', 'bound', 'co',
                'orig_distance', 'dest_stop', 'dest_name_tc', 'dest_seq', 'dest_distance', 'service_type']
            
        }
        val = {}
        val['1'] = []
        val['2'] = []
        val['3'] = []
        val['min'] = []
        val['rmk_tc_1'] = []
        val['rmk_tc_2'] = []
        val['rmk_tc_3'] = []
        val['locat'] = []
        for key in key_dict[self.co]:
            val[key] = []
        for row in self.output:
            for key, value in zip(key_dict[self.co], row):
                val[key].append(value)
                if key == 'route': route = value
                elif key == 'co': co = value
                elif key == 'orig_stop': stop = value
                elif key == 'service_type': service_type = value
            if self.co == "KWB": 
                val['co'].append(self.co)
                self.url = f'https://data.etabus.gov.hk/v1/transport/kmb/eta/{stop}/{route}/{service_type}'
            elif self.co == "NFB": 
                val['service_type'].append("_")
                self.url = f'https://rt.data.gov.hk/v1/transport/citybus-nwfb/eta/{co}/{stop}/{route}'
            if d:= self.get_eta():
                for key, value in d.items():
                    val[key].append(value)
        self.output = val

    def get_eta(self):
        try:
            while True:
                try:
                    data = get(self.url).json()['data']
                    break
                except:
                    pass
            eta_dict = {
                '1':"N.A.",
                '2':"N.A.",
                '3':"N.A.",
                'min':" ",
                'rmk_tc_1':" ",
                'rmk_tc_2':" ",
                'rmk_tc_3':" ",
                'locat': " "
            }
            assert data

            for row in data:
                if row['eta']:
                    eta_dict[str(row['eta_seq'])] = row['eta'][11:-9]
                    eta_dict[f'rmk_tc_{row["eta_seq"]}'] = row['rmk_tc']
                    if row['eta_seq'] == 1: 
                        time =  datetime.datetime.strptime(row['eta'][:-9], "%Y-%m-%dT%H:%M") - datetime.datetime.now()
                        time = str(abs(time)).split(':')
                        time = int(time[0]) * 60 + int(time[1])
                        eta_dict['min'] = time
                        eta_dict['locat'] = row['dest_tc']
                
            return eta_dict
        except AssertionError:
            return eta_dict
            pass

    def query_mixed_route(self):
        sql = """
        SELECT route
        FROM mixed_route
        """
        BD.execute(sql)
        output = BD.fetchall()
        

class login:
    def __init__(self, user_id):
        self.id = user_id
        
    def query(self):
        sql = f"""
        SELECT place_name, lat, lng
        FROM user_db
        WHERE user = {self.id}
        """
        BD.execute(sql)
        output = BD.fetchall()
        if output: return output
        else: return None

if __name__ == "__main__":
    start = time.time()
    user = (22.3270946,114.166605)
    home = (22.294048, 114.169296)
    m = match(user, home)
    
    
    print('processing time:')
    print(time.time() - start)
