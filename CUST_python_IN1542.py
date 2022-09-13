

import utils
import pandas as pd
from datetime import datetime
import logging

logger=utils.setlogger(logfile='DIM_CHARGE_CATEG_PY.log')

def main():
    conn,cursor= utils.create_conn()
    logger.info('connect created')
    
    src_q='''select * from [BCMPWMT].[CUST]'''

    df=pd.read_sql(src_q,conn)
    logger.info('Query executed and src data extracted')

    
# =============================================================================

# =============================================================================
# CREATE TABLE DIM_CUST_SQL_IN1542(
# 
# cust_key	int    identity(1,1) primary key            ,
# CUST_ID	int               NOT NULL     ,
# TENANT_ORG_ID	int       NOT NULL     ,
# CUST_TYPE_ID	int       NOT NULL     ,
# NICKNAME	varchar(50)   NOT NULL     ,
# SALUTE	varchar(50)       NOT NULL     ,
# MIDDLE_NM	varchar(50)   NOT NULL     ,
# CUST_TITLE	varchar(50)   NOT NULL     ,
# SUFFIX	varchar(50)       NOT NULL     ,
# WM_EMPLOYEE_ID	int       NOT NULL     ,
# CRE_DT	date              NOT NULL     ,
# CRE_USER	varchar(50)   NOT NULL     ,
# UPD_TS	datetime          NOT NULL     ,
# UPD_USER	varchar(50)   NOT NULL     ,
# START_DATE  DATE,
# END_DATE DATE,
# SIGNUP_TS	datetime      NOT NULL     ,
# REALM_ID	varchar(50)   NOT NULL     ,
# 				{row['STS_ID']},
# =============================================================================
# =============================================================================



# 	   
# # =============================================================================
# =============================================================================
# VALID_CUST_IND	varchar(50) NOT NULL    ,
# DELTD_YN	varchar(50)   NOT NULL)    ; 
# =============================================================================
# =============================================================================
    logger.info('Applying transformations')
    df['CUST_ID'] = df['CUST_ID'].str.strip().astype('int').replace('NULL',101).fillna(101)
    df['TENANT_ORG_ID'] = df['TENANT_ORG_ID'].str.strip().astype('int').replace('NULL',101).fillna(101)
    df['CUST_TYPE_ID'] = df['CUST_TYPE_ID'].str.strip().astype('int').replace('NULL',101).fillna(101)
    df['NICKNAME'] = df['NICKNAME'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A').str.title()
    df['SALUTE'] = df['SALUTE'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A').apply(lambda x:x[:4])
    df['MIDDLE_NM'] = df['MIDDLE_NM'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['CUST_TITLE'] = df['CUST_TITLE'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A').replace('#',' ')
    df['SUFFIX'] = df['SUFFIX'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A').apply(lambda x:x[0].lower()+x[1].upper()+x[2:].lower())
    df['WM_EMPLOYEE_ID'] = df['WM_EMPLOYEE_ID'].str.strip().astype('int').replace('NULL',101).fillna(101)
    df['CRE_DT'] = pd.to_datetime(df['CRE_DT'].str.strip().replace('NULL','01-01-1900').fillna('01-01-1900')).dt.date
    df['CRE_USER'] = df['CRE_USER'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['UPD_TS'] = pd.to_datetime(df['UPD_TS'].str.strip().replace('NULL','01-01-1900 00:00:00').fillna('01-01-1900 00:00:00')).dt.date
    df['UPD_USER'] = df['UPD_USER'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')

    df['SIGNUP_TS'] = pd.to_datetime(df['SIGNUP_TS'].str.strip().replace('NULL','01-01-1900 00:00:00').fillna('01-01-1900 00:00:00'))
    df['REALM_ID'] = df['REALM_ID'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')

    df['VALID_CUST_IND'] = df['VALID_CUST_IND'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['DELTD_YN'] = df['DELTD_YN'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')

 
    
    cleaned_df=utils.nullhandler(df)
    logger.info('Null values handled')
    

    cursor.fast_executemany = True
    
    insert_to_tmp_tbl_stmt='''insert into stg_DIM_CUST_python_IN1542
    values (?,?,?,?,?,?,?,?,?,?,?,?,?,getdate(),NULL,?,?,?,?)'''
    collist=['CUST_ID', 'TENANT_ORG_ID','CUST_TYPE_ID','NICKNAME','SALUTE','MIDDLE_NM','CUST_TITLE','SUFFIX',
          'WM_EMPLOYEE_ID','CRE_DT','CRE_USER','UPD_TS','UPD_USER','SIGNUP_TS','REALM_ID','VALID_CUST_IND','DELTD_YN'
         ]
    cursor.executemany(insert_to_tmp_tbl_stmt, cleaned_df[collist].values.tolist())
 
    conn.commit()
    
   
    target_insert='''insert into DIM_CUST_python_IN1542
    select  CUST_ID, TENANT_ORG_ID,CUST_TYPE_ID,NICKNAME,SALUTE,MIDDLE_NM,CUST_TITLE,SUFFIX,
          WM_EMPLOYEE_ID,CRE_DT,CRE_USER,UPD_TS,UPD_USER,START_DATE,END_DATE,SIGNUP_TS,REALM_ID,VALID_CUST_IND,DELTD_YN
    from stg_DIM_CUST_python_IN1542'''
    
    cursor.execute(target_insert)
    conn.commit()
# =============================================================================
#     SALUTE

# CUST_TITLE
# SUFFIX
# 
# =============================================================================
    
    
    scd_2_cust=  ''' insert into DIM_CUST_python_IN1542 
 select 
a.[CUST_ID],a.[TENANT_ORG_ID], a.[CUST_TYPE_ID], a.[NICKNAME],a.[SALUTE],a.[MIDDLE_NM],
a.[CUST_TITLE],a.[SUFFIX], a.[WM_EMPLOYEE_ID], a.[CRE_DT], a.[CRE_USER],a.[UPD_TS],a.[UPD_USER],a.[START_DATE],NULL,a.[SIGNUP_TS],a.[REALM_ID]
,a.[VALID_CUST_IND],a.[DELTD_YN]



 from stg_DIM_CUST_python_IN1542 a left join DIM_CUST_python_IN1542 b
on a.[CUST_ID]=b.[CUST_ID]
where (b.[CUST_ID] IS NULL)or(b.[END_DATE] is null and (b.[SALUTE]<>a.[SALUTE] OR b.[SUFFIX]<>a.[SUFFIX] OR b.[CUST_TITLE]<>a.[CUST_TITLE] ))
                              
                              
              update DIM_CUST_python_IN1542 
              SET end_date=getdate()
               from stg_DIM_CUST_python_IN1542  a left join DIM_CUST_python_IN1542  b
              on  a.[CUST_ID]=b.[CUST_ID]
              where b.[END_DATE] is null and (b.[SALUTE]<>a.[SALUTE] OR b.[SUFFIX]<>a.[SUFFIX] OR b.[CUST_TITLE]<>a.[CUST_TITLE] 

                
                              
                              
                              )'''

    cursor.execute(scd_2_cust)
    conn.commit()
    
    






    
    
    
    
if __name__=='__main__':
    main()