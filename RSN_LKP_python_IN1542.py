import utils
import pandas as pd
from datetime import datetime
import logging

logger=utils.setlogger(logfile='DIM_CHARGE_CATEG_PY.log')

def main():
    conn,cursor= utils.create_conn()
    logger.info('connect created')
    
    src_query='''
    select * from [BCMPWMT].[RSN_LKP]

    '''

    df=pd.read_sql(src_query,conn)
    logger.info('Query executed and src data extracted')
    
    
# =============================================================================
# =============================================================================
# create table stg_dim_RSN_LKP_python_IN1542(
# rsn_key   int identity(1,1) primary key	NOT NULL  ,
# RSN_ID  integer  NOT NULL					  ,
# TENANT_ORG_ID  integer  NOT NULL				,
# DATA_SRC_ID  integer  NOT NULL				  ,
# RSN_TYPE_ID  integer  NOT NULL				  ,
# RSN_CD  integer  NOT NULL					  ,
# SRC_RSN_ID  integer  NOT NULL				  ,
# RSN_DESC  varchar(200)  NOT NULL				,
# RSN_LONG_DESC  varchar(200)  NOT NULL			,	
# CRE_TS  datetime  NOT NULL					  ,
# CRE_USER  varchar(50)  NOT NULL				  ,
# UPD_TS  datetime  NOT NULL					  ,
# UPD_USER  varchar(50)  NOT NULL				  )	
# =============================================================================

# =============================================================================
    logger.info('Applying transformations')
    df['RSN_ID'] = df['RSN_ID'].str.strip().astype('int').replace('NULL',101).fillna(101)
    df['TENANT_ORG_ID'] = df['TENANT_ORG_ID'].str.strip().astype('int').replace('NULL',101).fillna(101)
    df['DATA_SRC_ID'] = df['DATA_SRC_ID'].str.strip().astype('int').replace('NULL',101).fillna(101)
    df['RSN_TYPE_ID'] = df['RSN_TYPE_ID'].str.strip().astype('int').replace('NULL',101).fillna(101)
    df['RSN_CD'] = df['RSN_CD'].apply(lambda x:x if x.isdigit() else 101)
    df['SRC_RSN_ID'] = df['SRC_RSN_ID'].str.strip().astype('int').replace('NULL',101).fillna(101)
    df['RSN_DESC'] = df['RSN_DESC'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['RSN_LONG_DESC'] = df['RSN_LONG_DESC'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['CRE_TS'] = pd.to_datetime(df['CRE_TS'].str.strip().replace('NULL','01-01-1900 00:00:00').fillna('01-01-1900 00:00:00'))
    df['CRE_USER'] = df['CRE_USER'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['UPD_TS'] = pd.to_datetime(df['UPD_TS'].str.strip().replace('NULL','01-01-1900 00:00:00').fillna('01-01-1900 00:00:00'))
    df['UPD_USER'] = df['UPD_USER'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
        
         
    
    cleaned_df=utils.nullhandler(df)
    logger.info('Null values handled')
    
    cursor.fast_executemany = True
    
    insert_to_tmp_tbl_stmt='''insert into stg_dim_RSN_LKP_python_IN1542
    values (?,?,?,?,?,?,?,?,?,?,?,?)'''
    collist=['RSN_ID', 'TENANT_ORG_ID','DATA_SRC_ID','RSN_TYPE_ID','RSN_CD','SRC_RSN_ID','RSN_DESC','RSN_LONG_DESC',
          'CRE_TS','CRE_USER','UPD_TS','UPD_USER']
    cursor.executemany(insert_to_tmp_tbl_stmt, cleaned_df[collist].values.tolist())
 
    conn.commit()
    
    target_insert='''insert into dim_RSN_LKP_python_IN1542
    select  RSN_ID, TENANT_ORG_ID,DATA_SRC_ID,RSN_TYPE_ID,RSN_CD,SRC_RSN_ID,RSN_DESC,RSN_LONG_DESC,
          CRE_TS,CRE_USER,UPD_TS,UPD_USER
    from stg_dim_RSN_LKP_python_IN1542'''
    
    cursor.execute(target_insert)
    conn.commit()
    
    
    
if __name__=='__main__':
    main()