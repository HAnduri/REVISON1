# -*- coding: utf-8 -*-
"""
Created on Tue Sep 13 08:08:59 2022

@author: HAnduri
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Sep 12 17:26:20 2022

@author: HAnduri
"""

import numpy as np
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
# =============================================================================
	   ,
# OFFR_START_DT   DATETIME  NOT NULL		   ,
# OFFR_END_TS   DATETIME  NOT NULL		   ,
# OFFR_TYPE_ID   VARCHAR(50) NOT NULL		   ,
# COMM_PCT   DECIMAL(25,18)  NOT NULL			   ,
# SLR_OFFR_ID   VARCHAR(50) NOT NULL			   ,
# PRTNR_ID   VARCHAR(50)  NOT NULL			   ,
# START_PRICE   DECIMAL(25,18)  NOT NULL			   ,
# LAST_PRICE_UPD_TS   DATETIME  NOT NULL	   ,
# CURR_PRICE   DECIMAL(25,18)  NOT NULL			   ,
# CURR_SUGG_PRICE   DECIMAL(25,18)  NOT NULL		   ,
# BASE_ITEM_PRICE   DECIMAL(25,18)  NOT NULL		   ,
# BASE_SUGG_PRICE   DECIMAL (25,18) NOT NULL		   ,
# UOM   VARCHAR(50)  NOT NULL					   ,
# TAXABLE_IND   int  NOT NULL				   ,
# GIFT_WRAP_IND   int  NOT NULL			   ,
# SHIP_ALONE_IND   int  NOT NULL			   ,
# FREE_RETURNS_IND   int  NOT NULL		   ,
# SLR_UPC   VARCHAR(50)  NOT NULL				   ,
# SHIPTOSTORE_IND   int  NOT NULL			   ,
# PIP_IND   int  NOT NULL					   ,
# PRE_ORDER_IND   int  NOT NULL			   ,
# CRE_DT   DATE  NOT NULL					   ,
# UPD_TS   DATETIME  NOT NULL				   ,
# 		
# OFFR_KEY   	 int identity(1,1) NOT NULL	   )
# =============================================================================
# 
# =============================================================================

# 
# =============================================================================
# =============================================================================
# df['DATA_SRC_ID'] = df['DATA_SRC_ID'].astype('int').replace('NULL',101).fillna(101)
# df['VALID_TS'] = pd.to_datetime(df['VALID_TS'].replace('?','01-01-1900 00:00:00').replace('NULL','01-01-1900 00:00:00').fillna('01-01-1900 00:00:00'))
# df['VALID_STS'] = df['VALID_STS'].str.strip().replace(['NULL','?'],['N/A','N/A']).astype('str').fillna('N/A')
# =============================================================================
src_q='''select * from [BCMPWMT].[OFFR]'''
df=pd.read_sql(src_q,conn)
logger.info('Query executed and src data extracted')
df['OFFER_PK'] = df['OFFER_PK'].str.strip().replace(('NULL','?'),'N/A').astype('str').fillna('N/A')
df['CATLG_ITEM_ID'] = df['CATLG_ITEM_ID'].replace('NULL',101).fillna(101).astype('int64')
df['SRC_ORG_CD'] = df['SRC_ORG_CD'].replace(['?', 'NULL'],[np.nan, np.nan]).fillna(101).astype('int64')
df['TENANT_ORG_ID'] = df['TENANT_ORG_ID'].replace(['?', 'NULL'],[np.nan, np.nan]).fillna(101).astype('int64')
df['SRC_ITEM_KEY'] = df['SRC_ITEM_KEY'].replace(['?', 'NULL'],[np.nan, np.nan]).fillna(101).astype('int64')
df['UPC'] = df['UPC'].replace(['?', 'NULL'],[np.nan, np.nan]).fillna('N/A')
df['WM_ITEM_NUM'] = df['WM_ITEM_NUM'].replace(['?', 'NULL'],[np.nan, np.nan]).fillna(101).astype('int64')
df['WM_UPC'] = df['WM_UPC'].replace(['?', 'NULL'],[np.nan, np.nan]).fillna('N/A')
df['OFFR_NM'] = df['OFFR_NM'].replace(['?', 'NULL'],[np.nan, np.nan]).fillna('N/A')
df['OFFR_START_TS'] = pd.to_datetime(df['OFFR_START_TS'].apply(pd.to_datetime, errors='coerce').replace('NULL','01-01-1900 00:00:00')).fillna('01-01-1900 00:00:00') 
df['OFFR_START_DT'] = pd.to_datetime(df['OFFR_START_DT'].replace(['?', 'NULL'],[np.nan, np.nan]).fillna('01-01-1900'), infer_datetime_format=True).dt.date
df['OFFR_END_TS'] = pd.to_datetime(df['OFFR_END_TS'].replace(['?', 'NULL'],[np.nan, np.nan]), infer_datetime_format=True, errors = 'coerce' ).fillna('01-01-1900')
df['OFFR_TYPE_ID'] = df['OFFR_TYPE_ID'].replace(['?', 'NULL'],[np.nan, np.nan]).fillna('N/A')
df['COMM_PCT'] = df['COMM_PCT'].apply(pd.to_numeric, errors='coerce').fillna(0).astype('float')
df['SLR_OFFR_ID'] = df['SLR_OFFR_ID'].replace(['?', 'NULL'],[np.nan, np.nan]).fillna('N/A')
df['PRTNR_ID'] = df['PRTNR_ID'].replace(['?', 'NULL'],[np.nan, np.nan]).fillna('N/A')
df['START_PRICE'] = df['START_PRICE'].apply(pd.to_numeric, errors='coerce').fillna(0).astype('float')
df['LAST_PRICE_UPD_TS'] = pd.to_datetime(df['LAST_PRICE_UPD_TS'].replace(['?', 'NULL'],[np.nan, np.nan]).fillna('01-01-1900'), infer_datetime_format=True)
df['CURR_PRICE'] = df['CURR_PRICE'].apply(pd.to_numeric, errors='coerce').fillna(0).astype('float')
df['CURR_SUGG_PRICE'] = df['CURR_SUGG_PRICE'].apply(pd.to_numeric, errors='coerce').fillna(0).astype('float')
df['BASE_ITEM_PRICE'] = df['BASE_ITEM_PRICE'].apply(pd.to_numeric, errors='coerce').fillna(0).astype('float')
df['BASE_SUGG_PRICE'] = df['BASE_SUGG_PRICE'].apply(pd.to_numeric, errors='coerce').fillna(0).astype('float')
df['UOM'] = df['UOM'].replace(['?', 'NULL'],[np.nan, np.nan]).fillna('N/A')
df['TAXABLE_IND'] = df['TAXABLE_IND'].replace(['?', 'NULL'],[np.nan, np.nan]).fillna(101).astype('float').astype(int)
df['GIFT_WRAP_IND'] = df['GIFT_WRAP_IND'].replace(['?', 'NULL'],[np.nan, np.nan]).fillna(101).astype('int')
df['SHIP_ALONE_IND'] = df['SHIP_ALONE_IND'].replace(['?', 'NULL'],[np.nan, np.nan]).fillna(101).astype('int')
df['FREE_RETURNS_IND'] = df['FREE_RETURNS_IND'].replace(['?', 'NULL'],[np.nan, np.nan]).fillna(101).astype('int')
df['SLR_UPC'] = df['SLR_UPC'].replace(['?', 'NULL'],[np.nan, np.nan]).fillna('N/A')
df['SHIPTOSTORE_IND'] = df['SHIPTOSTORE_IND'].apply(pd.to_numeric, errors='coerce').fillna(101).astype('int64')
df['PIP_IND'] = df['PIP_IND'].replace(['?', 'NULL'],[np.nan, np.nan]).fillna(101).astype('int')
df['PRE_ORDER_IND'] = df['PRE_ORDER_IND'].replace(['?', 'NULL'],[np.nan, np.nan]).fillna(101).astype('int')
df['CRE_DT'] = pd.to_datetime(df['CRE_DT'].replace(['?', 'NULL' ,'0'],[np.nan, np.nan, np.nan]).fillna('01-01-1900'), infer_datetime_format=True).dt.date
df['UPD_TS'] = pd.to_datetime(df['UPD_TS'].apply(lambda x: x if not re.match('[a-zA-z]',str(x)) else '01-01-1900').fillna('01-01-1900'), infer_datetime_format=True)
  
          
    

    
cleaned_df=utils.nullhandler(df)
logger.info('Null values handled')
    

cursor.fast_executemany = True
    
insert_to_tmp_tbl_stmt='''insert into   stg_Dim_OFFR_python_IN1542
values (?,?,?,?,?,?,?,?,?,?,
            ?,?,?,?,?,?,?,?,?,?,
            ?,?,?,?,?,?,?,?,?,?,
            ?,?,?)'''
collist=['OFFER_PK',

'CATLG_ITEM_ID',
'SRC_ORG_CD',
'TENANT_ORG_ID',
'SRC_ITEM_KEY',
'UPC',
'WM_ITEM_NUM',
'WM_UPC',
'OFFR_NM',
'OFFR_START_TS',
'OFFR_START_DT',
'OFFR_END_TS',
'OFFR_TYPE_ID',
'COMM_PCT',
'SLR_OFFR_ID',
'PRTNR_ID',
'START_PRICE',
'LAST_PRICE_UPD_TS',
'CURR_PRICE',
'CURR_SUGG_PRICE',
'BASE_ITEM_PRICE',
'BASE_SUGG_PRICE',
'UOM',
'TAXABLE_IND',
'GIFT_WRAP_IND',
'SHIP_ALONE_IND',
'FREE_RETURNS_IND',
'SLR_UPC',
'SHIPTOSTORE_IND',
'PIP_IND',
'PRE_ORDER_IND',
'CRE_DT',
'UPD_TS'           ] 
cursor.executemany(insert_to_tmp_tbl_stmt, df[collist].values.tolist())
 
conn.commit()

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