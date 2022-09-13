# -*- coding: utf-8 -*-
"""
Created on Sat Sep 10 15:24:52 2022

@author: HAnduri
"""

# -*- coding: utf-8 -*-
"""
Created on Sat Sep 10 14:29:51 2022

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
    select * from stg_DIM_CUST_EMAIL_python_IN1542

    '''
# =============================================================================
#   Cust_email_key  int identity(1,1) primary key	NOT NULL	,	
# EMAIL_ID  bigint  NOT NULL									,
# TENANT_ORG_ID  int  NOT NULL								,
# CNTCT_TYPE_ID  int  NOT NULL								,
# DATA_SRC_ID  int  NOT NULL									,
# DELTD_YN  varchar  NOT NULL									,
# CRE_DT  Date  NOT NULL										,
# UPD_TS  Date  NOT NULL	'''	
# =============================================================================
    
    src_cust_emaildf=pd.read_sql(src_query,conn)
    logger.info('Query executed and src data extracted')
    
    
    logger.info('Applying transformations')
    src_cust_emaildf['EMAIL_ID']=src_cust_emaildf['EMAIL_ID'].replace('NULL',101).astype('int').fillna(101)
    src_cust_emaildf['TENANT_ORG_ID']=src_cust_emaildf['TENANT_ORG_ID'].replace('NULL',101).astype('int').fillna(101)
    src_cust_emaildf['CNTCT_TYPE_ID']=src_cust_emaildf['CNTCT_TYPE_ID'].replace('NULL',101).astype('int').fillna(101)
  
    #src_cust_emaildf['SRC_EMAIL_ID']=src_cust_emaildf['SRC_EMAIL_ID'].apply(lambda x: x.strip() if len(x)>5 else str.upper(x.strip()))
            
    src_cust_emaildf['DATA_SRC_ID']=src_cust_emaildf['DATA_SRC_ID'].replace('NULL',101).astype('int').fillna(101)
    src_cust_emaildf['DELTD_YN']=src_cust_emaildf['DELTD_YN'].replace('NULL','N/A').fillna('N/A')
    src_cust_emaildf['CRE_DT']=pd.to_datetime(src_cust_emaildf['CRE_DT'],infer_datetime_format=True).dt.date
    src_cust_emaildf['UPD_TS']=pd.to_datetime(src_cust_emaildf['UPD_TS'].replace('NULL','01-01-1990').fillna('01-01-1990'),infer_datetime_format=True).dt.date
            
    
    cleaned_df=utils.nullhandler(src_cust_emaildf)
    logger.info('Null values handled')


    insert_to_tmp_tbl_stmt=""
    cursor.fast_executemany = True
    for index,row in cleaned_df.iterrows():
          insert_to_tmp_tbl_stmt='''insert into DIM_CUST_EMAIL_python_IN1542
     values (?,?,?,?,?,?,?)'''
    collist=['EMAIL_ID', 'TENANT_ORG_ID', 'CNTCT_TYPE_ID',
           'DATA_SRC_ID', 'DELTD_YN', 
            'CRE_DT','UPD_TS']
    cursor.executemany(insert_to_tmp_tbl_stmt, cleaned_df[collist].values.tolist())
  
    conn.commit()
    

       
    
    
if __name__=='__main__':
    main()