# -*- coding: utf-8 -*-
"""
Created on Mon Sep 12 12:05:33 2022

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
    
    src_q='''select * from [BCMPWMT].[CUST_ADDR1]'''
    src_q1='''  select * from stg_dim_cust_addr1_python_IN1542 '''
    df1=pd.read_sql(src_q1,conn)
    df1
    df=pd.read_sql(src_q,conn)
    logger.info('Query executed and src data extracted')
    df['ADDR_ID'] = df['ADDR_ID'].replace('NULL',101).astype('float64').fillna(101)
    df['TENANT_ORG_ID'] = df['TENANT_ORG_ID'].astype('int').replace('NULL',101).fillna(101)
    df['DATA_SRC_ID'] = df['DATA_SRC_ID'].astype('int').replace(['NULL','?'],[101,101]).fillna(101)
    # (df['VALID_TS'].replace(['NULL','?'],['01/01/1900','01/01/1900'].fillna('01/01/1900'),infer_datetime_format=True)).
    df['VALID_TS'] = pd.to_datetime(df['VALID_TS'].replace(('?','NULL' ),('01/01/1900')).fillna('01-01-1900')).dt.strftime('%d/%b/%Y')
    df['VALID_STS'] = df['VALID_STS'].replace(['NULL','?'],[101,101]).astype(int).fillna(101)
    df['CITY'] = df['CITY'].str.strip().replace(['NULL','?'],['N/A','N/A']).astype('str').fillna('N/A')
    df['MUNICIPALITY'] = df['MUNICIPALITY'].str.strip().replace(['NULL','?'],['N/A','N/A']).astype('str').fillna('N/A').apply(lambda x:x if len(x)>2 and len(x)<=8 else 'N/A')
    df['TOWN'] = df['TOWN'].str.strip().replace(['NULL','?'],['N/A','N/A']).astype('str').fillna('N/A')
    df['VILLAGE'] = df['VILLAGE'].str.strip().replace(['NULL','?'],['N/A','N/A']).astype('str').fillna('N/A')
    df['COUNTY'] = df['COUNTY'].str.strip().replace(['NULL','?'],['N/A','N/A']).astype('str').fillna('N/A')
    df['DISTRICT'] = df['DISTRICT'].str.strip().replace(['NULL','?'],['N/A','N/A']).astype('str').fillna('N/A')
    df['ZIP_CD'] = df['ZIP_CD'].astype('int').replace('NULL',101).fillna(101)
    df['POSTAL_CD'] = df['POSTAL_CD'].str.strip().replace(['NULL','?'],['N/A','N/A']).astype('str').fillna('N/A')
    df['ZIP_EXTN'] = df['ZIP_EXTN'].str.strip().replace(['NULL','?'],['N/A','N/A']).astype('str').fillna('N/A')
    df['ADDR_TYPE'] = df['ADDR_TYPE'].str.strip().replace(['NULL','?'],['N/A','N/A']).astype('str').fillna('N/A')
    df['AREA'] = df['AREA'].str.strip().replace(['NULL','?'],['N/A','N/A']).astype('str').fillna('N/A')
    df['CNTRY_CD'] = df['CNTRY_CD'].str.strip().replace(['NULL','?'],['N/A','N/A']).astype('str').fillna('N/A')
    df['STATE_PRVNCE_TYPE'] = df['STATE_PRVNCE_TYPE'].str.strip().replace(['NULL','?'],['N/A','N/A']).astype('str').fillna('N/A')
    df['OWNER_ID'] = df['OWNER_ID'].replace(['NULL','?'],[101,101]).astype('int').fillna(101)
    df['PARENT_ID'] = df['PARENT_ID'].replace(['NULL','?'],[101,101]).astype('int').fillna(101)
    df['DELTD_YN'] = df['DELTD_YN'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['CRE_DT'] = pd.to_datetime(df['CRE_DT'].replace('NULL','01-01-1900').fillna('01-01-1900'))
    df['CRE_USER'] = df['CRE_USER'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')


    
    cleaned_df=utils.nullhandler(df)
    logger.info('Null values handled')
    

    cursor.fast_executemany = True
    
    insert_to_tmp_tbl_stmt='''insert into  stg_dim_cust_addr1_python_IN1542
    values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
    collist=[
'ADDR_ID',
'TENANT_ORG_ID',
'DATA_SRC_ID',
'VALID_TS',
'VALID_STS',
'CITY',
'MUNICIPALITY',
'TOWN',
'VILLAGE',
'COUNTY',
'DISTRICT',
'ZIP_CD',
'POSTAL_CD',
'ZIP_EXTN',
'ADDR_TYPE',
'AREA',
'CNTRY_CD',
'STATE_PRVNCE_TYPE',
'OWNER_ID',
'PARENT_ID',
'DELTD_YN',
'CRE_DT',
'CRE_USER'



         ]
    cursor.executemany(insert_to_tmp_tbl_stmt, cleaned_df[collist].values.tolist())
 
    conn.commit()

   
    target_insert='''insert into  dim_cust_addr1_python_IN1542
    select  
ADDR_ID,
TENANT_ORG_ID,
DATA_SRC_ID,
VALID_TS,
VALID_STS,
CITY,
MUNICIPALITY,
TOWN,
VILLAGE,
COUNTY,
DISTRICT,
ZIP_CD,
POSTAL_CD,
ZIP_EXTN,
ADDR_TYPE,
AREA,
CNTRY_CD,
STATE_PRVNCE_TYPE,
OWNER_ID,
PARENT_ID,
DELTD_YN,
CRE_DT,
CRE_USER



    from stg_dim_cust_addr1_python_IN1542'''
    
    cursor.execute(target_insert)
    conn.commit()

    scd1_city='''
update dim_cust_addr1_python_IN1542
set

[CITY]=a.[CITY],
[MUNICIPALITY]=a.[MUNICIPALITY],
[TOWN]=a.[TOWN],
[VILLAGE]=a.[VILLAGE],
[COUNTY]=a.[COUNTY],
[DISTRICT]=a.[DISTRICT]

from  
stg_dim_cust_addr1_python_IN1542 a left join dim_cust_addr1_python_IN1542 t

on a.[ADDR_ID]=t.[ADDR_ID]
where t.[ADDR_ID] is not null  and (a.CITY<>t.CITY or a.[MUNICIPALITY]<>t.[MUNICIPALITY]
                                    or a.TOWN<>t.TOWN  or a.VILLAGE<>t.VILLAGE or 
                                    a.[COUNTY]<>t.[COUNTY] or a.[DISTRICT]<>t.[DISTRICT] )'''
    cursor.execute(scd1_city)
    conn.commit()


    
    
    
    
if __name__=='__main__':
    main()




















