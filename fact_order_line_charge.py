import utils
import pandas as pd
from datetime import datetime
import logging

logger=utils.setlogger(logfile='DIM_CHARGE_CATEG_PY.log')

def main():
    conn,cursor= utils.create_conn()
    logger.info('connect created')
    
    src_q='''select * from [BCMPWMT].[ORDER_LINE_CHRG]'''

    df=pd.read_sql(src_q,conn)
    
    src_q1='''select * from dim_day_sql_in1542'''
    df1=pd.read_sql(src_q1,conn)
    
    src_q2='''select * from STG_DIM_CHARGE_CATEG_PYTHON_IN1542'''
    df2=pd.read_sql(src_q2,conn)
    
    df2['CHARGE_CATEG_ID']=pd.to_datetime(df2['CHARGE_CATEG_ID']).dt.date
    df['CHARGE_CATEG_ID']=pd.to_datetime(df2['CHARGE_CATEG_ID']).dt.date
    
    logger.info('Query executed and src data extracted')
    df['SALES_ORDER_NUM'] = df['SALES_ORDER_NUM'].replace('NULL',101).astype('float64').fillna(101)
    df['SALES_ORDER_LINE_NUM'] = df['SALES_ORDER_LINE_NUM'].astype('int').replace('NULL',101).fillna(101)
    df['TENANT_ORG_ID'] = df['TENANT_ORG_ID'].replace('NULL',101).fillna(101).astype('int')
    df['CHARGE_CATEG_ID'] = df['CHARGE_CATEG_ID'].replace('NULL',101).fillna(101).astype('int')
    df['CHRG_CATEG_MAP_ID'] = df['CHRG_CATEG_MAP_ID'].replace('NULL',101).fillna(101).astype('int')
    df['CHARGE_NM'] = df['CHARGE_NM'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['CHARGE_AMT'] = df['CHARGE_AMT'].replace('NULL',0).fillna(0).astype(float).astype('int') 
    df['CHRG_CRE_DT'] =pd.to_datetime( df['CHRG_CRE_DT'].replace('NULL','01-01-1900')).dt.date.fillna('01-01-1900')
    df['CHRG_QTY'] = df['CHRG_QTY'].replace('NULL',0).astype('int').fillna(0)
    df['CHRG_CRE_DT_KEY'] = df['CHRG_CRE_DT_KEY']
    df['charge_categ_key'] = df['charge_categ_key']
    

    



 
    
    cleaned_df=utils.nullhandler(df)
    logger.info('Null values handled')
    

    cursor.fast_executemany = True
    
    insert_to_tmp_tbl_stmt='''insert into  stg_dim_cust_address_python_IN1542
    values (?,?,?,?,?,?,?,?,?,?,?,?)'''
    collist=[
'SALES_ORDER_NUM',
'SALES_ORDER_LINE_NUM',
'TENANT_ORG_ID',
'CHARGE_CATEG_ID',
'CHRG_CATEG_MAP_ID',
'CHARGE_NM',
'CHARGE_AMT',
'CHRG_CRE_DT',
'CHRG_QTY',
'CHRG_CRE_DT_KEY',
'charge_categ_key',


         ]
    cursor.executemany(insert_to_tmp_tbl_stmt, cleaned_df[collist].values.tolist())
 
    conn.commit()


