# -*- coding: utf-8 -*-
"""
Created on Mon Sep 12 07:59:08 2022

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
    
    src_q='''select * from [BCMPWMT].[CUST_ACCT]'''

    df=pd.read_sql(src_q,conn)
    logger.info('Query executed and src data extracted')

    
# =============================================================================
# If Email id has @ then ltrim(rtrim(email)) else N/A
# =============================================================================
# If Email id has @ then ltrim(rtrim(email)) else N/A
# 
# CREATE TABLE stg_dim_CUST_ACCT_python_IN1542
# (
# cust_acct_key  int identity(1,1) primary key	NOT NULL	,	
# ACCT_ID  bigint  NOT NULL			,
# CUST_ID  int  NOT NULL			    ,
# TENANT_ORG_ID  int  NOT NULL		,
# ACCT_STS_ID  int  NOT NULL			,
# ACCT_TYPE_ID  int  NOT NULL			,
# EMAIL  varchar(250)  NOT NULL		,
# VALID_CUST_IND  INT  NOT NULL		,
# CRE_DT  date  NOT NULL			    ,
# CRE_USER  varchar(250)  NOT NULL	,	
# UPD_TS  datetime  NOT NULL			,
# UPD_USER  varchar(250)  NOT NULL	,	
# Start_Date  datetime  NOT NULL		,
# End_Date  datetime NULL		,
# DELTD_YN  char(1)  NOT NULL			
# )
# =============================================================================
# =============================================================================
# a='harith@'
# k=a.find('@')
# print(k)
# =============================================================================
# =============================================================================


    df['ACCT_ID'] = df['ACCT_ID'].str.strip().replace('NULL',101).astype('float64').fillna(101)
    df['CUST_ID'] = df['CUST_ID'].str.strip().astype('int').replace('NULL',101).fillna(101)
    df['TENANT_ORG_ID'] = df['TENANT_ORG_ID'].str.strip().astype('int').replace('NULL',101).fillna(101)
    df['ACCT_STS_ID'] = df['ACCT_STS_ID'].str.strip().astype('int').replace('NULL',101).fillna(101)
    df['ACCT_TYPE_ID'] = df['ACCT_TYPE_ID'].str.strip().astype('int').replace('NULL',101).fillna(101)
    df['EMAIL'] = df['EMAIL'].apply(lambda x:x if x.find('@')>1 else 'N/A')
    df['VALID_CUST_IND'] = df['VALID_CUST_IND'].replace('NULL',101).astype('int').fillna(101)
    df['CRE_DT'] = pd.to_datetime(df['CRE_DT'].str.strip().replace('NULL','01-01-1900').fillna('01-01-1900')).dt.date
    df['CRE_USER'] = df['CRE_USER'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['UPD_TS'] = pd.to_datetime(df['UPD_TS'].str.strip().replace('NULL','01-01-1900 00:00:00').fillna('01-01-1900 00:00:00'))
    df['UPD_USER'] = df['UPD_USER'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')

    df['DELTD_YN'] = df['DELTD_YN'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')





 
    
    cleaned_df=utils.nullhandler(df)
    logger.info('Null values handled')
    

    cursor.fast_executemany = True
    
    insert_to_tmp_tbl_stmt='''insert into stg_dim_CUST_ACCT_python_IN1542
    values (?,?,?,?,?,?,?,?,?,?,?,getdate(),NULL,?)'''
    collist=[
'ACCT_ID',
'CUST_ID',
'TENANT_ORG_ID',
'ACCT_STS_ID',
'ACCT_TYPE_ID',
'EMAIL',
'VALID_CUST_IND',
'CRE_DT',
'CRE_USER',
'UPD_TS',
'UPD_USER',

'DELTD_YN']

    cursor.executemany(insert_to_tmp_tbl_stmt, cleaned_df[collist].values.tolist())
 
    conn.commit()
    
   
    target_insert='''insert into dim_CUST_ACCT_python_IN1542
    select  
ACCT_ID,
CUST_ID,
TENANT_ORG_ID,
ACCT_STS_ID,
ACCT_TYPE_ID,
EMAIL,
VALID_CUST_IND,
CRE_DT,
CRE_USER,
UPD_TS,
UPD_USER,
Start_Date,
End_Date,
DELTD_YN

          
          
          
          
    from stg_dim_CUST_ACCT_python_IN1542'''
    
    cursor.execute(target_insert)
    conn.commit()
    
    
    
    scd_2_cust=  ''' insert into dim_CUST_ACCT_python_IN1542
 select 

a.[ACCT_ID],
a.[CUST_ID],
a.[TENANT_ORG_ID],
a.[ACCT_STS_ID],
a.[ACCT_TYPE_ID],
a.[EMAIL],
a.[VALID_CUST_IND],
a.[CRE_DT],
a.[CRE_USER],
a.[UPD_TS],
a.[UPD_USER],
a.[Start_Date],
null ,
a.[DELTD_YN]




 from stg_dim_CUST_ACCT_python_IN1542 a left join dim_CUST_ACCT_python_IN1542 b
on a.[ACCT_ID]=b.[ACCT_ID]
where (b.[ACCT_ID] IS NULL)or(b.[END_DATE] is null and a.[EMAIL]<>b.[EMAIL]  )

update dim_CUST_ACCT_python_IN1542
SET end_date=getdate()
 from stg_dim_CUST_ACCT_python_IN1542 a left join dim_CUST_ACCT_python_IN1542 b
on a.[ACCT_ID]=b.[ACCT_ID]
where (b.[END_DATE] is null and a.[EMAIL]<>b.[EMAIL]  )








'''



    cursor.execute(scd_2_cust)
    conn.commit()
    

    
    
    
    
if __name__=='__main__':
    main()