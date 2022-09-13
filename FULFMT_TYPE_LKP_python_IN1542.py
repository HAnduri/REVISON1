# -*- coding: utf-8 -*-
"""
Created on Sun Sep 11 08:34:45 2022

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
    select * from stg_Dim_FULFMT_TYPE_LKP_python_IN1542

    '''

    src_cust_emaildf=pd.read_sql(src_query,conn)
    logger.info('Query executed and src data extracted')
    
    # =============================================================================
    #     src_cust_emaildf['TENANT_ORG_ID']=src_cust_emaildf['TENANT_ORG_ID'].replace('NULL',101).astype('int').fillna(101)
    #     src_cust_emaildf['DATA_SRC_ID']=src_cust_emaildf['DATA_SRC_ID'].replace('NULL',101).astype('int').fillna(101)
    #     src_cust_emaildf['STS_CD']=src_cust_emaildf['STS_CD'].replace('NULL','N/A').fillna('N/A')
    #     src_cust_emaildf['SRC_STS_ID']=src_cust_emaildf['SRC_STS_ID'].replace('NULL',101).astype('int').fillna(101)
    #     src_cust_emaildf['STS_DESC']=src_cust_emaildf['STS_DESC'].replace('NULL','N/A').fillna('N/A')
    # =============================================================================
# =============================================================================
# # =============================================================================
# CREATE TABLE  stg_Dim_FULFMT_TYPE_LKP_python_IN1542(
# FULFMT_TYPE_KEY  int identity(1,1) 	NOT NULL,				
# FULFMT_TYPE_ID  INT PRIMARY	KEY	,		
# FULFMT_TYPE_CD  VARCHAR(50)  NOT NULL		,			
# FULFMT_TYPE_DESC  VARCHAR (50) NOT NULL		,			
# CRE_DT  DATE  NOT NULL				,	
# UPD_TS  Nvarchar(255)  NOT NULL	)
# # =============================================================================
# =============================================================================
    logger.info('Applying transformations')
    src_cust_emaildf['FULFMT_TYPE_ID']=src_cust_emaildf['FULFMT_TYPE_ID'].replace('NULL',101).astype('int').fillna(101)
    src_cust_emaildf['FULFMT_TYPE_CD']=src_cust_emaildf['FULFMT_TYPE_CD'].replace('NULL','N/A').fillna('N/A')

    src_cust_emaildf['FULFMT_TYPE_DESC']=src_cust_emaildf['FULFMT_TYPE_DESC'].replace('NULL','N/A').fillna('N/A')    
    src_cust_emaildf['CRE_DT']=pd.to_datetime(src_cust_emaildf['CRE_DT'].replace('NULL','01-01-1900').fillna('01-01-1900'),infer_datetime_format=True)
    src_cust_emaildf['UPD_TS']=src_cust_emaildf['UPD_TS'].replace('NULL','N/A').fillna('N/A')
 
    
    cleaned_df=utils.nullhandler(src_cust_emaildf)
    logger.info('Null values handled')
    
    insertstmt=''
    for index,row in cleaned_df.iterrows():
       
        insertstmt+=f'''insert into  Dim_FULFMT_TYPE_LKP_python_IN1542
        values ({row['FULFMT_TYPE_ID']},'{row['FULFMT_TYPE_CD']}','{row['FULFMT_TYPE_DESC']}','{row['CRE_DT']}','{row['UPD_TS']}')
        '''
        print(insertstmt)
  
    cursor.execute(insertstmt)
    conn.commit()
    
if __name__=='__main__':
    main()