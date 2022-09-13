# -*- coding: utf-8 -*-
"""
Created on Mon Sep 12 11:40:58 2022

@author: HAnduri
"""


import utils
import pandas as pd
from datetime import datetime
import logging

logger=utils.setlogger(logfile='DIM_CHARGE_CATEG_PY.log')

def main():
    conn,cursor= utils.create_conn()
    logger.info('connect created')
    
    src_q='''select * from [BCMPWMT].[CUST_ADDR_ZONE]'''

    df=pd.read_sql(src_q,conn)
    logger.info('Query executed and src data extracted')
    df['ADDR_ZONE_ID'] = df['ADDR_ZONE_ID'].str.strip().astype('int').replace('NULL',101).fillna(101)
    df['TENANT_ORG_ID'] = df['TENANT_ORG_ID'].str.strip().astype('int').replace('NULL',101).fillna(101)
    df['DATA_SRC_ID'] = df['DATA_SRC_ID'].str.strip().astype('int').replace('NULL',101).fillna(101)
    df['CITY'] = df['CITY'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['POSTAL_CD'] = df['POSTAL_CD'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['STATE'] = df['STATE'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['DELTD_YN'] = df['DELTD_YN'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['CRE_USER'] = df['CRE_USER'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['CRE_DT'] = pd.to_datetime(df['CRE_DT'].replace('NULL','01-01-1900').fillna('01-01-1900')).dt.date
    df['UPD_USER'] = df['UPD_USER'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['UPD_TS'] = pd.to_datetime(df['UPD_TS'].replace('NULL','01-01-1900 00:00:00').fillna('01-01-1900 00:00:00'))
    
     
    
    cleaned_df=utils.nullhandler(df)
    logger.info('Null values handled')
    

    cursor.fast_executemany = True
    
    insert_to_tmp_tbl_stmt='''insert into  stg_dim_cust_addr_zone_python_IN1542
    values (?,?,?,?,?,?,?,?,?,?,?)'''
    collist=[
'ADDR_ZONE_ID',
'TENANT_ORG_ID',
'DATA_SRC_ID',
'CITY',
'POSTAL_CD',
'STATE',
'DELTD_YN',
'CRE_USER',
'CRE_DT',
'UPD_USER',
'UPD_TS'


         ]
    cursor.executemany(insert_to_tmp_tbl_stmt, cleaned_df[collist].values.tolist())
 
    conn.commit()

   
    target_insert='''insert into  dim_cust_addr_zone_python_IN1542
    select  
ADDR_ZONE_ID,
TENANT_ORG_ID,
DATA_SRC_ID,
CITY,
POSTAL_CD,
STATE,
DELTD_YN,
CRE_USER,
CRE_DT,
UPD_USER,
UPD_TS


    from stg_dim_cust_addr_zone_python_IN1542'''
    
    cursor.execute(target_insert)
    conn.commit()







    
    
    
    
if __name__=='__main__':
    main()

