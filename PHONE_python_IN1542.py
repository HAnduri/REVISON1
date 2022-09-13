# -*- coding: utf-8 -*-
"""
Created on Mon Sep 12 15:12:58 2022

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
    
    src_q='''select * from [BCMPWMT].[CUST_PHONE]'''
# =============================================================================
# CREATE TABLE stg_DIM_CUST_PHONE_python_IN1542(
# Cust_phone_key  int identity(1,1) primary key	NOT NULL,		
# PHONE_ID  bigint  NOT NULL			                ,
# TENANT_ORG_ID  int  NOT NULL						,
# CNTCT_TYPE_ID  bigint  NOT NULL						,
# SRC_PHONE_ID  varchar(50)  NOT NULL					,
# DATA_SRC_ID  int  NOT NULL							,
# AREA_CD  varchar(50)  NOT NULL						,
# CNTRY_CD  varchar(50)  NOT NULL						,
# EXTN  varchar(50)  NOT NULL							,
# CRE_DT  Date  NOT NULL								,
# DELTD_YN  varchar(50)  NOT NULL						,
# UPD_TS  DateTime  NOT NULL			
# =============================================================================
    df=pd.read_sql(src_q,conn)
    logger.info('Query executed and src data extracted')
    
    df['PHONE_ID'] = df['PHONE_ID'].replace('NULL',101).astype('float64').fillna(101)
    df['TENANT_ORG_ID'] = df['TENANT_ORG_ID'].astype('int').replace('NULL',101).fillna(101)
    df['CNTCT_TYPE_ID'] = df['CNTCT_TYPE_ID'].replace('NULL',101).astype('float64').fillna(101)
    df['SRC_PHONE_ID'] = df['SRC_PHONE_ID'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['DATA_SRC_ID'] = df['DATA_SRC_ID'].replace('NULL',101).astype('int')
    df['AREA_CD'] = df['AREA_CD'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['CNTRY_CD'] = df['CNTRY_CD'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['EXTN'] = df['EXTN'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['CRE_DT'] = pd.to_datetime(df['CRE_DT'].replace('NULL','01-01-1900').fillna('01-01-1900'))
    df['DELTD_YN'] = df['DELTD_YN'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['UPD_TS'] = pd.to_datetime(df['UPD_TS'].replace('NULL','01-01-1900 00:00:00')).fillna('01-01-1900 00:00:00')
    
    
    
    
    
# =============================================================================
#     df['ADDR_ID'] = df['ADDR_ID'].replace('NULL',101).astype('float64').fillna(101)
#     df['TENANT_ORG_ID'] = df['TENANT_ORG_ID'].astype('int').replace('NULL',101).fillna(101)
#     df['DATA_SRC_ID'] = df['DATA_SRC_ID'].astype('int').replace(['NULL','?'],[101,101]).fillna(101)
#     # (df['VALID_TS'].replace(['NULL','?'],['01/01/1900','01/01/1900'].fillna('01/01/1900'),infer_datetime_format=True)).
#     df['VALID_TS'] = pd.to_datetime(df['VALID_TS'].replace(('?','NULL' ),('01/01/1900')).fillna('01-01-1900')).dt.strftime('%d/%b/%Y')
#     df['VALID_STS'] = df['VALID_STS'].replace(['NULL','?'],[101,101]).astype(int).fillna(101)
#     df['CITY'] = df['CITY'].str.strip().replace(['NULL','?'],['N/A','N/A']).astype('str').fillna('N/A')
#     df['MUNICIPALITY'] = df['MUNICIPALITY'].str.strip().replace(['NULL','?'],['N/A','N/A']).astype('str').fillna('N/A').apply(lambda x:x if len(x)>2 and len(x)<=8 else 'N/A')
#     df['TOWN'] = df['TOWN'].str.strip().replace(['NULL','?'],['N/A','N/A']).astype('str').fillna('N/A')
#     df['VILLAGE'] = df['VILLAGE'].str.strip().replace(['NULL','?'],['N/A','N/A']).astype('str').fillna('N/A')
#     df['COUNTY'] = df['COUNTY'].str.strip().replace(['NULL','?'],['N/A','N/A']).astype('str').fillna('N/A')
#     df['DISTRICT'] = df['DISTRICT'].str.strip().replace(['NULL','?'],['N/A','N/A']).astype('str').fillna('N/A')
#     df['ZIP_CD'] = df['ZIP_CD'].astype('int').replace('NULL',101).fillna(101)
#     df['POSTAL_CD'] = df['POSTAL_CD'].str.strip().replace(['NULL','?'],['N/A','N/A']).astype('str').fillna('N/A')
#     df['ZIP_EXTN'] = df['ZIP_EXTN'].str.strip().replace(['NULL','?'],['N/A','N/A']).astype('str').fillna('N/A')
#     df['ADDR_TYPE'] = df['ADDR_TYPE'].str.strip().replace(['NULL','?'],['N/A','N/A']).astype('str').fillna('N/A')
#     df['AREA'] = df['AREA'].str.strip().replace(['NULL','?'],['N/A','N/A']).astype('str').fillna('N/A')
#     df['CNTRY_CD'] = df['CNTRY_CD'].str.strip().replace(['NULL','?'],['N/A','N/A']).astype('str').fillna('N/A')
#     df['STATE_PRVNCE_TYPE'] = df['STATE_PRVNCE_TYPE'].str.strip().replace(['NULL','?'],['N/A','N/A']).astype('str').fillna('N/A')
#     df['OWNER_ID'] = df['OWNER_ID'].replace(['NULL','?'],[101,101]).astype('int').fillna(101)
#     df['PARENT_ID'] = df['PARENT_ID'].replace(['NULL','?'],[101,101]).astype('int').fillna(101)
#     df['DELTD_YN'] = df['DELTD_YN'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
#     df['CRE_DT'] = pd.to_datetime(df['CRE_DT'].replace('NULL','01-01-1900').fillna('01-01-1900'))
#     df['CRE_USER'] = df['CRE_USER'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
# =============================================================================


    
    cleaned_df=utils.nullhandler(df)
    logger.info('Null values handled')
    

    cursor.fast_executemany = True
    
    insert_to_tmp_tbl_stmt='''insert into   stg_DIM_CUST_PHONE_python_IN1542
    values (?,?,?,?,?,?,?,?,?,?,?)'''
    collist=[
'PHONE_ID',
'TENANT_ORG_ID',
'CNTCT_TYPE_ID',
'SRC_PHONE_ID',
'DATA_SRC_ID',
'AREA_CD',
'CNTRY_CD',
'EXTN',
'CRE_DT',
'DELTD_YN',
'UPD_TS',




         ]
    cursor.executemany(insert_to_tmp_tbl_stmt, cleaned_df[collist].values.tolist())
 
    conn.commit()

   
    target_insert='''insert into   DIM_CUST_PHONE_python_IN1542
    select  
PHONE_ID,
TENANT_ORG_ID,
CNTCT_TYPE_ID,
SRC_PHONE_ID,
DATA_SRC_ID,
AREA_CD,
CNTRY_CD,
EXTN,
CRE_DT,
DELTD_YN,
UPD_TS



    from stg_DIM_CUST_PHONE_python_IN1542'''
    
    cursor.execute(target_insert)
    conn.commit()

