# -*- coding: utf-8 -*-
"""
Created on Fri Sep  9 14:21:04 2022

@author: DSethura
"""


import dbcreds
import pyodbc
import logging

username= dbcreds.credentials['username']
password= dbcreds.credentials['password']
server= dbcreds.credentials['server']
database= dbcreds.credentials['database']


def getconobj():
    class dbconn:
        def __init__(self,username=username,password=password,server=server,database=database):
          self.database=database
          self.username=username
          self.password=password
          self.server=server
    return dbconn

def create_conn():
    #connstr=f'''SERVER={server};DATABASE={database};UID={username};PWD={password}'''
    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      f'Server={server};'
                      f'Database={database};'
                      f'UID={username};'
                      f'PWD={password};''Trusted_Connection=no;')
    #conn=pyodbc.connect(connstr)
    cursor=conn.cursor()
    
    return conn,cursor
    
def close_conn(conn,cursor):
    conn.close()
    cursor.close()
    
    
def setlogger(logfile,log=logging.getLogger(__name__),level=logging.DEBUG,logformat="%(asctime)s %(filename)s:%(lineno)s (%(funcName)s) %(levelname)s :: %(message)s"):
        
        log_f = logging.FileHandler(logfile)
        #log_f = logging.FileHandler(f'{data_dir}/{__file__}.log')
        log_f.setLevel(level)
        log_f.setFormatter(logformat)
        log.addHandler(log_f)
        return log
    
def nullhandler(df,junk=['null','?']):
    colslist=df.columns
    
    for col in colslist:
       for i in junk:
            df[col].replace(i,None)
        
       if isinstance(df[col].dtype,str):
           df[col].fillna('NA')
      
       if isinstance(df[col].dtype,int):
            df[col].fillna(101)
            
    return df
           
           
      
            