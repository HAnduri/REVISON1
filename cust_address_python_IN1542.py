# -*- coding: utf-8 -*-
"""
Created on Mon Sep 12 09:32:48 2022

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
    
    src_q='''select * from [BCMPWMT].[CUST_ADDR]'''

    df=pd.read_sql(src_q,conn)
    logger.info('Query executed and src data extracted')
# =============================================================================
# cust_addr_key  int identity(1,1) primary key	NOT NULL			    ,	
# ADDR_ID  float NOT NULL				    ,
# TENANT_ORG_ID  int  NOT NULL				,
# DATA_SRC_ID  int  NOT NULL				    ,
# VALID_TS  datetime  NOT NULL				,
# VALID_STS  varchar(200)  NOT NULL				,
# CITY  nvarchar(255)  NOT NULL				,
# MUNICIPALITY  nvarchar(255)  NOT NULL		,
# TOWN  nvarchar(255)  NOT NULL				,
# VILLAGE  nvarchar(255)  NOT NULL			,
# COUNTY  nvarchar(255)  NOT NULL				,
# DISTRICT  nvarchar(255)  NOT NULL			,
# ZIP_CD  int  NOT NULL				        ,
# POSTAL_CD  int  NOT NULL				    ,
# ZIP_EXTN  int  NOT NULL				        ,
# ADDR_TYPE  nvarchar(255)  NOT NULL			,
# AREA  nvarchar(255)  NOT NULL				,
# CNTRY_CD  nvarchar(255)  NOT NULL			,
# STATE_PRVNCE_TYPE  nvarchar(255)  NOT NULL	,
# OWNER_ID  int  NOT NULL				        ,
# PARENT_ID  int  NOT NULL				    ,
# DELTD_YN  char(1)  NOT NULL				    ,
# Start_Date  datetime  NOT NULL				,
# End_Date  datetime   NULL				,
# CRE_DT  date  NOT NULL				        ,
# CRE_USER  nvarchar(255)  NOT NULL			,
# UPD_TS  datetime  NOT NULL				    ,
# UPD_USER  nvarchar(255)  NOT NULL
# =============================================================================
# =============================================================================
#     
# k='habbc'.replace(('c','b'),('i','i'))
# print(k)
# =============================================================================


    logger.info('Applying transformations')
    df['ADDR_ID'] = df['ADDR_ID'].replace('NULL',101).astype('float').fillna(101)
    df['TENANT_ORG_ID'] = df['TENANT_ORG_ID'].astype('int').replace('NULL',101).fillna(101)
    df['DATA_SRC_ID'] = df['DATA_SRC_ID'].astype('int').replace('NULL',101).fillna(101)
    df['VALID_TS'] = pd.to_datetime(df['VALID_TS'].replace('?','01-01-1900 00:00:00').replace('NULL','01-01-1900 00:00:00').fillna('01-01-1900 00:00:00'))
    df['VALID_STS'] = df['VALID_STS'].str.strip().replace(['NULL','?'],['N/A','N/A']).astype('str').fillna('N/A')
    df['CITY'] = df['CITY'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['MUNICIPALITY'] = df['MUNICIPALITY'].str.strip().replace(['NULL','?'],['N/A','N/A']).astype('str').fillna('N/A')
    df['TOWN'] = df['TOWN'].str.strip().replace(['NULL','?'],['N/A','N/A']).astype('str').fillna('N/A')
    df['VILLAGE'] = df['VILLAGE'].str.strip().replace(['NULL','?'],['N/A','N/A']).astype('str').fillna('N/A')
    df['COUNTY'] = df['COUNTY'].str.strip().replace(['NULL','?'],['N/A','N/A']).astype('str').fillna('N/A')
    df['DISTRICT'] = df['DISTRICT'].str.strip().replace(['NULL','?'],['N/A','N/A']).astype('str').fillna('N/A')
    df['ZIP_CD'] = df['ZIP_CD'].replace('NULL',101).fillna(101).astype('int')
    df['POSTAL_CD'] = df['POSTAL_CD'].replace('NULL',101).replace('?',101).fillna(101).astype('int')
    df['ZIP_EXTN'] = df['ZIP_EXTN'].replace('NULL',101).replace('?',101).fillna(101).astype('int')
    df['ADDR_TYPE'] = df['ADDR_TYPE'].str.strip().replace(['NULL','?'],['N/A','N/A']).astype('str').fillna('N/A')
    df['AREA'] = df['AREA'].str.strip().replace(['NULL','?'],['N/A','N/A']).astype('str').fillna('N/A')
    df['CNTRY_CD'] = df['CNTRY_CD'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['STATE_PRVNCE_TYPE'] = df['STATE_PRVNCE_TYPE'].str.strip().replace(['NULL','?'],['N/A','N/A']).astype('str').fillna('N/A')
    df['OWNER_ID'] = df['OWNER_ID'].replace('NULL',101).replace('?',101).fillna(101).astype('int')
    df['PARENT_ID'] = df['PARENT_ID'].replace('NULL',101).replace('?',101).fillna(101).astype('int')
    df['DELTD_YN'] = df['DELTD_YN'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A') 

    df['CRE_DT'] = pd.to_datetime(df['CRE_DT'].replace('NULL','01-01-1900').fillna('01-01-1900')).dt.date
    df['CRE_USER'] = df['CRE_USER'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    #df['UPD_TS'] = pd.to_datetime(df['UPD_TS'], infer_datetime_format=True).replace(['?', 'NULL'],['01-01-1900', '01-01-1900']).fillna('01-01-1900')
    df['UPD_TS'] = pd.to_datetime(df['UPD_TS'].astype(str).replace('?','01-01-1900 00:00:00').fillna('01-01-1900 00:00:00'), infer_datetime_format= True)

    # df['UPD_TS'] = pd.to_datetime(df['UPD_TS']).dt.strftime(df['UPD_TS'],'%m/%d/%y %H:%M:%S').replace('NULL','01-01-1900').replace('?','01-01-1900').fillna('01-01-1900')
    df['UPD_USER'] = df['UPD_USER'].str.strip().replace('NULL','N/A').replace('?','N/A').fillna('N/A')

 
    
    cleaned_df=utils.nullhandler(df)
    logger.info('Null values handled')
    

    cursor.fast_executemany = True
    
    insert_to_tmp_tbl_stmt='''insert into  stg_dim_cust_address_python_IN1542
    values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, getdate(),NULL,?,?,?,?)'''
    collist=['ADDR_ID',
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
'CRE_USER',
'UPD_TS',
'UPD_USER'

         ]
    cursor.executemany(insert_to_tmp_tbl_stmt, cleaned_df[collist].values.tolist())
 
    conn.commit()

   
    target_insert='''insert into  dim_cust_address_python_IN1542
    select  ADDR_ID,
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
Start_Date,
End_Date,
CRE_DT,
CRE_USER,
UPD_TS,
UPD_USER

    from stg_dim_cust_address_python_IN1542'''
    
    cursor.execute(target_insert)
    conn.commit()
# =============================================================================
#     SALUTE

# CUST_TITLE
# SUFFIX
# 
# =============================================================================
    
    
    scd_2_cust=  ''' insert into  dim_cust_address_python_IN1542
 select 
a.[ADDR_ID],
a.[TENANT_ORG_ID],
a.[DATA_SRC_ID],
a.[VALID_TS],
a.[VALID_STS],
a.[CITY],
a.[MUNICIPALITY],
a.[TOWN],
a.[VILLAGE],
a.[COUNTY],
a.[DISTRICT],
a.[ZIP_CD],
a.[POSTAL_CD],
a.[ZIP_EXTN],
a.[ADDR_TYPE],
a.[AREA],
a.[CNTRY_CD],
a.[STATE_PRVNCE_TYPE],
a.[OWNER_ID],
a.[PARENT_ID],
a.[DELTD_YN],
a.[Start_Date],
a.[End_Date],
a.[CRE_DT],
a.[CRE_USER],
a.[UPD_TS],
a.[UPD_USER]




 from stg_dim_cust_address_python_IN1542 a left join  dim_cust_address_python_IN1542 b
on a.[ADDR_ID]=b.[ADDR_ID]
where b.[ADDR_ID] IS NULL or(b.[END_DATE] is null and (b.[CITY]<>a.[CITY] OR b.[MUNICIPALITY]<>a.[MUNICIPALITY] 
                                                        OR b.[TOWN]<>a.[TOWN]
                                                        or b.[VILLAGE]<>a.[VILLAGE] OR b.[COUNTY]<>a.[COUNTY]
                                                        OR b.[DISTRICT]<>a.[DISTRICT]
                              OR b.[ZIP_CD]<>a.[ZIP_CD]
                              or b.[POSTAL_CD]<>a.[POSTAL_CD] OR b.[ZIP_EXTN]<>a.[ZIP_EXTN]
                              OR b.[ADDR_TYPE]<>a.[ADDR_TYPE]
                                     OR b.[AREA]<>a.[AREA]
                                     or b.[CNTRY_CD]<>a.[CNTRY_CD] OR b.[STATE_PRVNCE_TYPE]<>a.[STATE_PRVNCE_TYPE]
                                     OR b.[OWNER_ID]<>a.[OWNER_ID] or b.[PARENT_ID]<>a.[PARENT_ID]    ) ) 
                                     
                              
                              
  
                                 
                              
                              
                              
                              
                              
              update  dim_cust_address_python_IN1542
              SET end_date=getdate()
               from stg_dim_cust_address_python_IN1542  a left join  dim_cust_address_python_IN1542 b
              on  a.[ADDR_ID]=b.[ADDR_ID]
              where b.[END_DATE] is null and (b.[CITY]<>a.[CITY] OR b.[MUNICIPALITY]<>a.[MUNICIPALITY] 
                                                                      OR b.[TOWN]<>a.[TOWN]
                                                                      or b.[VILLAGE]<>a.[VILLAGE] OR b.[COUNTY]<>a.[COUNTY]
                                                                      OR b.[DISTRICT]<>a.[DISTRICT]
                                            OR b.[ZIP_CD]<>a.[ZIP_CD]
                                            or b.[POSTAL_CD]<>a.[POSTAL_CD] OR b.[ZIP_EXTN]<>a.[ZIP_EXTN]
                                            OR b.[ADDR_TYPE]<>a.[ADDR_TYPE]
                                                   OR b.[AREA]<>a.[AREA]
                                                   or b.[CNTRY_CD]<>a.[CNTRY_CD] OR b.[STATE_PRVNCE_TYPE]<>a.[STATE_PRVNCE_TYPE]
                                                   OR b.[OWNER_ID]<>a.[OWNER_ID] or b.[PARENT_ID]<>a.[PARENT_ID]   )  

                
                              
                              
                              '''

    cursor.execute(scd_2_cust)
    conn.commit()
    
    






    
    
    
    
if __name__=='__main__':
    main()

