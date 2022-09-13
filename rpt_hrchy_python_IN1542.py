# -*- coding: utf-8 -*-
"""
Created on Mon Sep 12 18:15:48 2022

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
# ,DIV_NM  varchar(200)  NOT NULL							
# ,SUPER_DEPT_ID  FLOAT  NOT NULL							
# ,SUPER_DEPT_NM  varchar(200)  NOT NULL					
# ,DEPT_ID  FLOAT  NOT NULL								
# ,DEPT_NM  varchar(250)  NOT NULL							
# ,CATEG_NM  varchar(200)  NOT NULL						
# ,SUB_CATEG_ID  FLOAT  NOT NULL							
# ,SUB_CATEG_NM  varchar(200)  NOT NULL							
# ,ITEM_CATEG_GROUPING_ID  varchar(200)  NOT NULL				
# ,SRC_CRE_TS  nvarchar(255)  NOT NULL						
# ,SRC_MODFD_TS  nvarchar(255)  NOT NULL					
# ,SRC_HRCHY_MODFD_TS  datetime  NOT NULL					
# ,CATEG_MGR_NM  varchar(200)  NOT NULL					
# ,BUYER_NM  varchar(200)  NOT NULL						
# ,EFF_BEGIN_DT  date  NOT NULL							
# ,EFF_END_DT  date  NOT NULL								
# ,RPT_HRCHY_ID_PATH  varchar(200)  NOT NULL				
# ,CATEG_ID  FLOAT  NOT NULL								
# ,CONSUMABLE_IND  nvarchar(255)  NOT NULL					
# ,CURR_IND  FLOAT  NOT NULL								
# ,CRE_DT  date  NOT NULL									
# ,CRE_USER  nvarchar(255)  NOT NULL						
# ,UPD_TS  datetime  NOT NULL								
# ,UPD_USER  nvarchar(255)  NOT NULL		)
# =============================================================================

    src_q='''select * from [BCMPWMT].[RPT_HRCHY]'''

    df=pd.read_sql(src_q,conn)
    logger.info('Query executed and src data extracted')
    df['RPT_HRCHY_ID'] = df['RPT_HRCHY_ID'].replace('NULL',101).astype('float').fillna(101)
    df['SRC_RPT_HRCHY_ID'] = df['SRC_RPT_HRCHY_ID'].replace('NULL',101).astype('float').fillna(101)
    df['TENANT_ORG_ID'] = df['TENANT_ORG_ID'].replace('NULL','N/A').astype('str').fillna('N/A')
    df['RPT_HRCHY_PATH'] = df['RPT_HRCHY_PATH'].str.strip().replace(('NULL','?'),'N/A').astype('str').fillna('N/A')
    df['DIV_ID'] = df['DIV_ID'].replace('NULL',101).astype('float').fillna(101)
    df['DIV_NM'] = df['DIV_NM'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A') 
    df['SUPER_DEPT_ID'] = df['SUPER_DEPT_ID'].replace('NULL',101).astype('float').fillna(101)
    df['SUPER_DEPT_NM'] = df['SUPER_DEPT_NM'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['DEPT_ID'] = df['DEPT_ID'].replace('NULL',101).astype('float').fillna(101)
    df['DEPT_NM'] = df['DEPT_NM'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['CATEG_NM'] = df['CATEG_NM'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['SUB_CATEG_ID'] = df['SUB_CATEG_ID'].replace('NULL',101).astype('float').fillna(101)
    df['SUB_CATEG_NM'] = df['SUB_CATEG_NM'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['ITEM_CATEG_GROUPING_ID'] = df['ITEM_CATEG_GROUPING_ID'].str.strip().replace(('NULL','?'),'N/A').astype('str').fillna('N/A')
    df['SRC_CRE_TS'] =df['SRC_CRE_TS'].replace('NULL','N/A').astype('str').fillna('N/A') 
    
    df['SRC_MODFD_TS'] = df['SRC_MODFD_TS'].replace('NULL','N/A').astype('str').fillna('N/A') 
    df['SRC_HRCHY_MODFD_TS'] = pd.to_datetime(df['SRC_HRCHY_MODFD_TS'].replace('NULL','01-01-1900 00:00:00')).fillna('01-01-1900 00:00:00')
    df['CATEG_MGR_NM'] = df['CATEG_MGR_NM'].str.strip().replace(('NULL','?'),'N/A').astype('str').fillna('N/A')
    df['BUYER_NM'] = df['BUYER_NM'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['EFF_BEGIN_DT'] = pd.to_datetime(df['EFF_BEGIN_DT'].apply(pd.to_datetime,errors='coerce')).replace('NULL','01-01-1900').fillna('01-01-1900').dt.date
    df['EFF_END_DT'] = pd.to_datetime(df['EFF_END_DT'].replace(('NULL','?'),'01-01-1900')).fillna('01-01-1900').dt.date
    df['RPT_HRCHY_ID_PATH'] = df['RPT_HRCHY_ID_PATH'].str.strip().replace(('NULL','?'),'N/A').astype('str').fillna('N/A')
    df['CATEG_ID'] = df['CATEG_ID'].replace(('NULL','?'),101).astype('float').fillna(101)
    df['CONSUMABLE_IND'] = df['CONSUMABLE_IND'].replace(('NULL','?'),101).astype('float').fillna(101)
    df['CURR_IND'] = df['CURR_IND'].replace(('NULL','?'),101).astype('float').fillna(101)
    df['CRE_DT'] = pd.to_datetime(df['CRE_DT'].apply(pd.to_datetime,errors='coerce').replace('NULL','01-01-1900')).fillna('01-01-1900').dt.date
    df['CRE_USER'] = df['CRE_USER'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A') 
    df['UPD_TS'] = pd.to_datetime(df['UPD_TS'].replace('NULL','01-01-1900 00:00:00')).fillna('01-01-1900 00:00:00')
    df['UPD_USER'] = df['UPD_USER'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')  


    
    cleaned_df=utils.nullhandler(df)
    logger.info('Null values handled')
    

    cursor.fast_executemany = True
    
    insert_to_tmp_tbl_stmt='''insert into  str_dim_rpt_hrchy_python_IN1542
    values (?,?,?,?,?,?,?,?,?,?,
            ?,?,?,?,?,?,?,?,?,?,
            ?,?,?,?,?,?,?,?,?)'''
    collist=[

'RPT_HRCHY_ID',
'SRC_RPT_HRCHY_ID',
'TENANT_ORG_ID',
'RPT_HRCHY_PATH',
'DIV_ID',
'DIV_NM',
'SUPER_DEPT_ID',
'SUPER_DEPT_NM',
'DEPT_ID',
'DEPT_NM',
'CATEG_NM',
'SUB_CATEG_ID',
'SUB_CATEG_NM',
'ITEM_CATEG_GROUPING_ID',
'SRC_CRE_TS',
'SRC_MODFD_TS',
'SRC_HRCHY_MODFD_TS',
'CATEG_MGR_NM',
'BUYER_NM',
'EFF_BEGIN_DT',
'EFF_END_DT',
'RPT_HRCHY_ID_PATH',
'CATEG_ID',
'CONSUMABLE_IND',
'CURR_IND',
'CRE_DT',
'CRE_USER',
'UPD_TS',
'UPD_USER'




         ]
    cursor.executemany(insert_to_tmp_tbl_stmt, df[collist].values.tolist())
 
    conn.commit()

   
    target_insert='''insert into  dim_rpt_hrchy_python_IN1542
    select  
RPT_HRCHY_ID,
SRC_RPT_HRCHY_ID,
TENANT_ORG_ID,
RPT_HRCHY_PATH,
DIV_ID,
DIV_NM,
SUPER_DEPT_ID,
SUPER_DEPT_NM,
DEPT_ID,
DEPT_NM,
CATEG_NM,
SUB_CATEG_ID,
SUB_CATEG_NM,
ITEM_CATEG_GROUPING_ID,
SRC_CRE_TS,
SRC_MODFD_TS,
SRC_HRCHY_MODFD_TS,
CATEG_MGR_NM,
BUYER_NM,
EFF_BEGIN_DT,
EFF_END_DT,
RPT_HRCHY_ID_PATH,
CATEG_ID,
CONSUMABLE_IND,
CURR_IND,
CRE_DT,
CRE_USER,
UPD_TS,
UPD_USER





    from str_dim_rpt_hrchy_python_IN1542'''
    
    cursor.execute(target_insert)
    conn.commit()