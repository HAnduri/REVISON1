# -*- coding: utf-8 -*-
"""
Created on Mon Sep 12 17:26:20 2022

@author: HAnduri
"""


import utils
import pandas as pd
from datetime import datetime
import logging
import re

logger=utils.setlogger(logfile='DIM_CHARGE_CATEG_PY.log')

def main():
    conn,cursor= utils.create_conn()
    logger.info('connect created')
    
    
# =============================================================================
#     CREATE TABLE
# dim_prod_SQL_IN1542(
# prod_key   int identity(1,1) primary key	NOT NULL	,		
# CATLG_ITEM_ID  integer  NOT NULL						,
# PRMRY_DSTRBTR_NM  varchar(200)  NOT NULL						,
# PRMRY_VEND_NUM  integer  NOT NULL						,
# SRC_IMS_CRE_TS  varchar(50)  NOT NULL						,
# SRC_IMS_MODFD_TS  varchar(50)  NOT NULL						,
# VEND_PACK_QTY  integer  NOT NULL						,
# WHSE_PACK_QTY  integer  NOT NULL						,
# CURR_PRICE_MODFD_TS  datetime  NOT NULL					,
# AMT_ITEM_COST  decimal(19,6)  NOT NULL						,
# AMT_BASE_ITEM_PRICE  decimal(19,6)  NOT NULL					,
# AMT_BASE_SUGG_PRICE  decimal(19,6)  NOT NULL					,
# AMT_SUGG_PRICE  decimal(19,6)  NOT NULL						,
# MIN_ITEM_COST  decimal(19,6)  NOT NULL						,
# ORIG_PRICE  decimal(19,6)  NOT NULL							,
# ORIG_ITEM_PRICE  decimal(19,6)  NOT NULL						,
# PROD_NM  varchar (200) NOT NULL								,
# PROD_HT  decimal(19,6)  NOT NULL								,
# PROD_WT  decimal(19,6)  NOT NULL								,
# PROD_LEN  decimal(19,6)  NOT NULL								,
# PROD_WDTH  decimal(19,6)  NOT NULL							,
# CRE_DT  date  NOT NULL									,
# CRE_USER  varchar(50)  NOT NULL				,
# UPD_TS  datetime  NOT NULL				,
# UPD_USER  varchar(50)  NOT NULL				
# )
# =============================================================================
    src_q='''select * from [BCMPWMT].[PROD]'''

    df=pd.read_sql(src_q,conn)
    logger.info('Query executed and src data extracted')
    df['CATLG_ITEM_ID'] = df['CATLG_ITEM_ID'] .replace('NULL',101).astype(int).fillna(101)
    df['PRMRY_DSTRBTR_NM'] = df['PRMRY_DSTRBTR_NM'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['PRMRY_VEND_NUM'] = df['PRMRY_VEND_NUM'].replace('NULL',101).astype(int).fillna(101)
    df['SRC_IMS_CRE_TS'] = df['SRC_IMS_CRE_TS'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['SRC_IMS_MODFD_TS'] = df['SRC_IMS_MODFD_TS'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['VEND_PACK_QTY'] = df['VEND_PACK_QTY'].replace('NULL',0).astype(int).fillna(0) 
    df['WHSE_PACK_QTY'] = df['WHSE_PACK_QTY'].replace('NULL',0).astype(int).fillna(0)  
    df['CURR_PRICE_MODFD_TS'] = pd.to_datetime(df['CURR_PRICE_MODFD_TS'].replace('NULL','01-01-1900 00:00:00')).fillna('01-01-1900 00:00:00')
    df['AMT_ITEM_COST'] = df['AMT_ITEM_COST'].replace('NULL',0).astype(float).fillna(0)  
    df['AMT_BASE_ITEM_PRICE'] = df['AMT_BASE_ITEM_PRICE'].replace('NULL',0).astype(float).fillna(0)  
    df['AMT_BASE_SUGG_PRICE'] = df['AMT_BASE_SUGG_PRICE'].replace('NULL',0).astype(float).fillna(0)   
    df['AMT_SUGG_PRICE'] = df['AMT_SUGG_PRICE'].replace('NULL',0).astype(float).fillna(0)   
    df['MIN_ITEM_COST'] = df['MIN_ITEM_COST'].replace('NULL',0).astype(float).fillna(0)   
    df['ORIG_PRICE'] = df['ORIG_PRICE'].replace('NULL',0).astype(float).fillna(0)   
    df['ORIG_ITEM_PRICE'] = df['ORIG_ITEM_PRICE'].replace('NULL',0).astype(float).fillna(0)   
    df['PROD_NM'] = df['PROD_NM'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['PROD_HT'] = df['PROD_HT'].apply(pd.to_numeric,errors='coerce').replace('NULL',0).astype(float).fillna(0)  
    df['PROD_WT'] = df['PROD_WT'].replace('NULL',0).astype(float).fillna(0)   
    df['PROD_LEN'] = df['PROD_LEN'].replace('NULL',0).astype(float).fillna(0)    
    df['PROD_WDTH'] = df['PROD_WDTH'].replace('NULL',0).astype(float).fillna(0)    
    df['CRE_DT'] = pd.to_datetime(df['CRE_DT'].replace('NULL','01-01-1900')).fillna('01-01-1900')
    df['CRE_USER'] = df['CRE_USER'].replace('NULL','N/A').astype('str').fillna('N/A')
    df['UPD_TS'] = pd.to_datetime(df['UPD_TS'].apply(pd.to_datetime,errors='coerce')).replace('NULL','01-01-1900 00:00:00').fillna('01-01-1900 00:00:00')
    df['UPD_USER'] = df['UPD_USER'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')

    
    cleaned_df=utils.nullhandler(df)
    logger.info('Null values handled')
    

    cursor.fast_executemany = True
    
    insert_to_tmp_tbl_stmt='''insert into  stg_dim_prod_python_IN1542
    values (?,?,?,?,?,?,?,?,?,?,
            ?,?,?,?,?,?,?,?,?,?,
            ?,?,?,?)'''
    collist=[
'CATLG_ITEM_ID',
'PRMRY_DSTRBTR_NM',
'PRMRY_VEND_NUM',
'SRC_IMS_CRE_TS',
'SRC_IMS_MODFD_TS',
'VEND_PACK_QTY',
'WHSE_PACK_QTY',
'CURR_PRICE_MODFD_TS',
'AMT_ITEM_COST',
'AMT_BASE_ITEM_PRICE',
'AMT_BASE_SUGG_PRICE',
'AMT_SUGG_PRICE',
'MIN_ITEM_COST',
'ORIG_PRICE',
'ORIG_ITEM_PRICE',
'PROD_NM',
'PROD_HT',
'PROD_WT',
'PROD_LEN',
'PROD_WDTH',
'CRE_DT',
'CRE_USER',
'UPD_TS',
'UPD_USER'



         ]
    cursor.executemany(insert_to_tmp_tbl_stmt, cleaned_df[collist].values.tolist())
 
    conn.commit()

   
    target_insert='''insert into  dim_prod_python_IN1542
    select  
CATLG_ITEM_ID,
PRMRY_DSTRBTR_NM,
PRMRY_VEND_NUM,
SRC_IMS_CRE_TS,
SRC_IMS_MODFD_TS,
VEND_PACK_QTY,
WHSE_PACK_QTY,
CURR_PRICE_MODFD_TS,
AMT_ITEM_COST,
AMT_BASE_ITEM_PRICE,
AMT_BASE_SUGG_PRICE,
AMT_SUGG_PRICE,
MIN_ITEM_COST,
ORIG_PRICE,
ORIG_ITEM_PRICE,
PROD_NM,
PROD_HT,
PROD_WT,
PROD_LEN,
PROD_WDTH,
CRE_DT,
CRE_USER,
UPD_TS,
UPD_USER




    from stg_dim_prod_python_IN1542'''
    
    cursor.execute(target_insert)
    conn.commit()







    
    
    
    
if __name__=='__main__':
    main()

