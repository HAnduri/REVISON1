# -*- coding: utf-8 -*-
"""
Created on Sun Sep 11 16:23:11 2022

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
    
    src_query='''
    select * from stg_Dim_ORDER_STS_MASTER_LKP_python1_IN1542

    '''

    df=pd.read_sql(src_query,conn)
    logger.info('Query executed and src data extracted')
    
    

    logger.info('Applying transformations')
    df['ORDER_STS_MASTER_ID'] = df['ORDER_STS_MASTER_ID'].replace('NULL',101).astype('int64').fillna(101)
    df['ORDER_STS_MASTER_CD'] = df['ORDER_STS_MASTER_CD'].replace('NULL','N/A').fillna('N/A')
    df['ORDER_STS_SHORT_DESC'] = df['ORDER_STS_SHORT_DESC'].str.strip().replace('NULL','N/A').fillna('N/A')
    df['ORDER_STS_LONG_DESC'] = df['ORDER_STS_LONG_DESC'].str.strip().replace('NULL','N/A').fillna('N/A')

    df['CRE_TS'] = pd.to_datetime(df['CRE_TS'].replace('NULL','01-01-1900').fillna('01-01-1900'),infer_datetime_format=True)

    df['UPD_TS'] =pd.to_datetime(df['UPD_TS'].replace('NULL','01-01-1900').fillna('01-01-1900'))


    
    cleaned_df=utils.nullhandler(df)
    logger.info('Null values handled')

    insert_to_tmp_tbl_stmt=""
    cursor.fast_executemany = True
    for index,row in cleaned_df.iterrows():
          insert_to_tmp_tbl_stmt='''insert into Dim_ORDER_STS_MASTER_LKP_python1_IN1542
     values (?,?,?,?,?,?)'''
    collist=['ORDER_STS_MASTER_ID', 'ORDER_STS_MASTER_CD', 'ORDER_STS_SHORT_DESC',
           'ORDER_STS_LONG_DESC', 'CRE_TS', 
            'UPD_TS']
    cursor.executemany(insert_to_tmp_tbl_stmt, cleaned_df[collist].values.tolist())
  
    conn.commit()

    
if __name__=='__main__':
    main()
