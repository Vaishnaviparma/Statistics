#!/home/xiao.gu/local/bin/python2.7
import MySQLdb
import pandas as pd 
import numpy as np
import sqlalchemy
import time 

from datetime import timedelta
from datetime import datetime
from sqlalchemy import create_engine
from functools import partial

from constant import * # Self-defined constant values
from DBconnection import * # self-define DataBase connection functions
from common import * # self-defined function

import multiprocessing as mp

##############################################################

def getDict(group):
    myTuple = zip(group['bnd'], group['cnt'])
    myDict = dict(myTuple)
    return myDict


# def singleQueryRun(stmt, MySQL_config, engine, subuhgid):
# http://stackoverflow.com/questions/14219038/python-multiprocessing-typeerror-expected-string-or-unicode-object-nonetype-f
# engine object can not be pickled!
def singleQueryRun(stmt, configs, MySQL_config, subuhgid):
    flag = True
    t0 = datetime.now()
    while flag:
        try:
            print "################## Processing sub bucket {0} ...".format(subuhgid)
            configs["subuhgid"] = subuhgid
            # to avoid memory issue tuning waiting seconds (11min: 6core x 5s, 8min20s, 16cores * mod 8 * 5s)
            # (8mins 16cores * mod 6 * 5 )
            time.sleep(int(subuhgid, 16) % 6 * 5) 
            query = gen_query(stmt, configs)
            data1 = MySQLConnectionWithOutput(query, MySQL_config) 
            print "################## Grouping by ..."   
            grp = data1.groupby(['id','os','uhgid'])
            print "################## Applying getDict ..."   
            grps = grp.apply(lambda x: getDict(x))
            print "################## Resetting index ..."   
            results = grps.reset_index(name = 'pbag')
            results['pbag'] = [dict([key, int(value)] for key, value in a.iteritems()) for a in results['pbag']]
            print "################## Pushing data back to MemSQL ..."
            # Must have the exact same name with table in data base
            results.to_sql(con=engine, 
            name='xiao_traffic_agg', 
            if_exists='append', 
            index=False, 
            chunksize=100000, 
            dtype={'id':sqlalchemy.types.String(),
                'os':sqlalchemy.types.String(),
                'uhgid':sqlalchemy.types.INTEGER(), 
                'pbag':sqlalchemy.types.JSON()}
                )
            flag = False
        except Exception as e:
            print e
            flag = True
    t1 = datetime.now()
    print "################## Finished {uhgid} - {subuhgid} ##################".format(**configs)
    print "# Running time of a single Run"+ str(t1 - t0)


def mergeUserTraffic(stmt, configs, MySQL_config, engine):
    start_date = datetime.strptime(configs['date'],"%Y-%m-%d") + timedelta(days=0)
    end_date = datetime.strptime(configs['date'],"%Y-%m-%d") + timedelta(days=configs['traffic_days'])
    configs['start_date'] = str(start_date)[:10]
    configs['end_date'] = str(end_date)[:10]
    query = gen_query(stmt, configs)
    print "################## Merging Traffic for bucket {uhgid} ...".format(**configs)
    deleteStmt = """
    delete from xiao_traffic_agg where os = '{os}' and uhgid = {uhgid}
    """
    deleteQuery = gen_query(deleteStmt, configs)
    MySQLConnectionNoOutput(deleteQuery, MySQL_config)
    pool = mp.Pool(processes=16) # Tuning This parameter
    func = partial(singleQueryRun, query, configs, MySQL_config)
    results = pool.map(func, configs["subuhgid_list"])
    pool.close()
    pool.join()
    
    print "################## Finished bucket {uhgid} ##################".format(**configs)

def RunQuery(stmt, configs, engine, MySQL_config = MySQLConfig.copy()):
    t = datetime.now()
    for i in range(0, 128, 1):
        t0 = datetime.now()
        configs["uhgid"] = i
        # query = gen_query(stmt, configs)
        mergeUserTraffic(stmt, configs, MySQL_config, engine)
        t1 = datetime.now()
        print "# Running time for bucket {0} :".format(i) + str(t1 - t0)
    print "# Total Runtime of all bucket " + str(datetime.now() - t)

stmt = """
select uh as id,
      os,
      bnd,
      sum(cnt) as cnt,
      uhgid
from uh_pi_m
where os='{os}' and bnd!=''
and date>='{start_date}' and date <'{end_date}' and uhgid={uhgid} and substr(uh, -1, 1) = '{{subuhgid}}'
group by id, os, bnd, uhgid;
"""


engine = create_engine("mysql://{user}:@{host}/{db}".format(**MySQLConfig))


configs = {'date':'2017-04-01', 'os':'Android', 'uhgid':127, 'traffic_days':123, 'subuhgid_list':list("0123456789abcdef")}        
RunQuery(stmt, configs, engine)
configs = {'date':'2017-04-01', 'os':'iOS', 'uhgid':127, 'traffic_days':123, 'subuhgid_list':list("0123456789abcdef")}        
RunQuery(stmt, configs, engine)
