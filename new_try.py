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
    select * from [BCMPWMT].CHARGE_CATEG_LKP

    '''
    
    
    src_charge_categdf=pd.read_sql(src_query,conn)
    logger.info('Query executed and src data extracted')
    
    
    logger.info('Applying transformations')
    src_charge_categdf['CHARGE_CATEG_ID']=src_charge_categdf['CHARGE_CATEG_ID'].str.strip().astype('int')
    src_charge_categdf['TENANT_ORG_ID']=src_charge_categdf['TENANT_ORG_ID'].str.strip().astype('int')
    src_charge_categdf['CHARGE_CATEG']=src_charge_categdf['CHARGE_CATEG'].str.lower()
    src_charge_categdf['CHARGE_CATEG']=src_charge_categdf['CHARGE_CATEG'].apply(lambda x: x.strip() if len(x)>5 else str.upper(x.strip()))
    
    src_charge_categdf['CHARGE_CATEG_DESC']=src_charge_categdf['CHARGE_CATEG_DESC'].str.strip()
    src_charge_categdf['TAX_IND']=src_charge_categdf['TAX_IND'].str.strip().astype('int')
    
    
    cleaned_df=utils.nullhandler(src_charge_categdf)
    logger.info('Null values handled')
    
    insertstmt=''
    for index,row in cleaned_df.iterrows():
       
        insertstmt+=f'''insert into IN1542.STG_DIM_CHARGE_CATEG_PYTHON_IN1542
        values ({row['CHARGE_CATEG_ID']},{row['TENANT_ORG_ID']},'{row['CHARGE_CATEG']}','{row['CHARGE_CATEG_DESC']}', {row['TAX_IND']},1)
        '''
        print(insertstmt)
  
    cursor.execute(insertstmt)
    conn.commit()
    
    
if __name__=='__main__':
    main()
    

