# -*- coding: utf-8 -*-
"""
Created on Tue Sep 13 14:21:58 2022

@author: HAnduri
"""

# -*- coding: utf-8 -*-
"""
Created on Tue Sep 13 13:08:51 2022

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
    
    src_q='''select * from [BCMPWMT].[SALES_ORDER_ADJ]'''

    df=pd.read_sql(src_q,conn)
    
    src_q1='''select * from DIM_DAY_HOUR_sql_IN1542'''
    df1=pd.read_sql(src_q1,conn)

    ADJ_RPT_TS
    hour_id
    Date_id
    
    df2['CHARGE_CATEG_ID']=df2['CHARGE_CATEG_ID'].astype(int)
    df['CHARGE_CATEG_ID']=df2['CHARGE_CATEG_ID'].astype(int)
    
    df1['Date_id']=pd.to_datetime(df1['Date_id']).dt.date
    df['CHRG_CRE_DT'] =pd.to_datetime(df['CHRG_CRE_DT']).dt.date
    
    
    merge_1=pd.merge(df,df1 ,left_on='CHRG_CRE_DT' ,right_on='Date_id',how='left')
    merge_1
    merge_2=pd.merge(merge_1,df2, left_on= 'CHARGE_CATEG_ID',right_on = 'CHARGE_CATEG_ID', how='left')
    merge_2
         
    logger.info('Query executed and src data extracted')
    df['ADJ_ID'] = df['ADJ_ID'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['SALES_ORDER_NUM'] = df['SALES_ORDER_NUM'].str.strip().replace('NULL',101).astype('float64').fillna(101)
    df['SALES_ORDER_LINE_NUM'] = df['SALES_ORDER_LINE_NUM'].str.strip().astype('int').replace('NULL',101).fillna(101)
    df['TENANT_ORG_ID'] = df['TENANT_ORG_ID'].str.strip().astype('int').replace('NULL',101).fillna(101)
    df['CHARGE_CATEG_ID'] = df['CHARGE_CATEG_ID'].str.strip().astype('int').replace('NULL',101).fillna(101)
    df['CHRG_CATEG_MAP_ID'] = df['CHRG_CATEG_MAP_ID'].str.strip().astype('int').replace('NULL',101).fillna(101)
    df['CHRG_NM'] = df['CHRG_NM'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['RSN_CD'] = df['RSN_CD'].str.strip().astype('int').replace('NULL',101).fillna(101)
    df['QTY'] = df['QTY'].str.strip().astype('int').replace('NULL',101).fillna(101)
    df['ADJUSMENT_AMT'] = df['ADJUSMENT_AMT'].str.strip().replace('NULL',101).astype('float').fillna(101)
    df['ADJ_RPT_TS'] = df['ADJ_RPT_TS'].str.strip().replace('NULL','01-01-1900 00:00:00').fillna('01-01-1900 00:00:00')
    df['RTN_IND'] = df['RTN_IND'].str.strip().astype('int').replace('NULL',101).fillna(101)
    df['RTN_TS'] = df['RTN_TS'].str.strip().replace('NULL','01-01-1900 00:00:00').fillna('01-01-1900 00:00:00')
    df['XCHNG_IND'] = df['XCHNG_IND'].str.strip().astype('int').replace('NULL',101).fillna(101)
    df['ADJ_RPT_TS_KEY'] = df['ADJ_RPT_TS_KEY'].str.strip().astype('int').replace('NULL',101).fillna(101)
    df['RTN_TS_KEY'] = df['RTN_TS_KEY'].str.strip().astype('int').replace('NULL',101).fillna(101)
    df['RFND_TS_KEY'] = df['RFND_TS_KEY'].str.strip().astype('int').replace('NULL',101).fillna(101)
    df['RFND_TS'] = df['RFND_TS'].str.strip().replace('NULL','01-01-1900 00:00:00').fillna('01-01-1900 00:00:00')
    df['charge_categ_key'] = df['charge_categ_key'].str.strip().astype('int').replace('NULL',101).fillna(101)
    df['Rsn_cd_KEY'] = df['Rsn_cd_KEY'].str.strip().astype('int').replace('NULL',101).fillna(101)
    
            

    



 
    
    cleaned_df=utils.nullhandler(df)
    logger.info('Null values handled')
    

    cursor.fast_executemany = True
    
    insert_to_tmp_tbl_stmt='''insert into  FACT_ORDER_LINE_CHRG_PYTHON_IN1542
    values (?,?,?,?,?,?,?,?,?,?,
            ?,?,?,?,?,?,?,?,?,?)'''
    collist=[
'ADJ_ID',
'SALES_ORDER_NUM',
'SALES_ORDER_LINE_NUM',
'TENANT_ORG_ID',
'CHARGE_CATEG_ID',
'CHRG_CATEG_MAP_ID',
'CHRG_NM',
'RSN_CD',
'QTY',
'ADJUSMENT_AMT',
'ADJ_RPT_TS',
'RTN_IND',
'RTN_TS',
'XCHNG_IND',
'ADJ_RPT_TS_KEY',
'RTN_TS_KEY',
'RFND_TS_KEY',
'RFND_TS',
'charge_categ_key',
'Rsn_cd_KEY',



         ]
    cursor.executemany(insert_to_tmp_tbl_stmt,df[collist].values.tolist())
 
    conn.commit()