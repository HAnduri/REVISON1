# -*- coding: utf-8 -*-
"""
Created on Sun Sep 11 07:31:38 2022

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
    select * from stg_DIM_STS_LKP_python_IN1542

    '''

    src_cust_emaildf=pd.read_sql(src_query,conn)
    logger.info('Query executed and src data extracted')
    
    

    logger.info('Applying transformations')
    src_cust_emaildf['STS_ID']=src_cust_emaildf['STS_ID'].replace('NULL',101).astype('int').fillna(101)
    src_cust_emaildf['STS_MASTER_ID']=src_cust_emaildf['STS_MASTER_ID'].replace('NULL',101).astype('int').fillna(101)
    src_cust_emaildf['TENANT_ORG_ID']=src_cust_emaildf['TENANT_ORG_ID'].replace('NULL',101).astype('int').fillna(101)
    src_cust_emaildf['DATA_SRC_ID']=src_cust_emaildf['DATA_SRC_ID'].replace('NULL',101).astype('int').fillna(101)
    src_cust_emaildf['STS_CD']=src_cust_emaildf['STS_CD'].replace('NULL','N/A').fillna('N/A')
    src_cust_emaildf['SRC_STS_ID']=src_cust_emaildf['SRC_STS_ID'].replace('NULL',101).astype('int').fillna(101)
    src_cust_emaildf['STS_DESC']=src_cust_emaildf['STS_DESC'].replace('NULL','N/A').fillna('N/A').astype(str)
    src_cust_emaildf['STS_LONG_DESC']=src_cust_emaildf['STS_LONG_DESC'].replace('NULL','N/A').fillna('N/A')    
    src_cust_emaildf['CRE_TS']='Q'+pd.to_datetime(src_cust_emaildf['CRE_TS'].replace('NULL','N/A').fillna('N/A')).dt.quarter.astype('str')+'-'+pd.to_datetime(src_cust_emaildf['CRE_TS'].replace('NULL','N/A').fillna('N/A')).dt.year.astype('str')
    
    src_cust_emaildf['UPD_TS_Q']='Q'+ pd.to_datetime(src_cust_emaildf['UPD_TS'].replace('NULL','01/01/1900').fillna('01/01/1900')).dt.quarter.astype('str')
    src_cust_emaildf['UPD_TS_Y']=src_cust_emaildf['UPD_TS_Q']+'-'+pd.to_datetime(src_cust_emaildf['UPD_TS'].replace('NULL','01/01/1900').fillna('01/01/1900')).dt.year.astype('str')
    src_cust_emaildf['UPD_TS']= src_cust_emaildf['UPD_TS_Y'].replace('Q1-1990','N/A')                                                                        
 
    
    cleaned_df=utils.nullhandler(src_cust_emaildf)
    logger.info('Null values handled')
    
    insertstmt=''
    for index,row in cleaned_df.iterrows():
       
        insertstmt+=f'''insert into DIM_STS_LKP_python_IN1542
        values ({row['STS_ID']},{row['STS_MASTER_ID']},{row['TENANT_ORG_ID']},{row['DATA_SRC_ID']},'{row['STS_CD']}',{row['SRC_STS_ID']},'{row['STS_DESC']}','{row['STS_LONG_DESC']}','{row['CRE_TS']}','{row['UPD_TS']}')
        '''
        print(insertstmt)
  
    cursor.execute(insertstmt)
    conn.commit()
    

    
    target_insert='''insert into DIM_STS_LKP_python_IN1542 
    select   STS_ID, STS_MASTER_ID, TENANT_ORG_ID, DATA_SRC_ID,STS_CD,SRC_STS_ID,STS_DESC,STS_LONG_DESC,CRE_TS,UPD_TS   
    from stg_DIM_STS_LKP_python_IN1542'''
    
    cursor.execute(target_insert)
    conn.commit()
    
    
    
    
    
if __name__=='__main__':
    main()