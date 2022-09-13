# -*- coding: utf-8 -*-
"""
Created on Mon Sep 12 15:35:08 2022

@author: HAnduri
"""

# =============================================================================
# df['ORG_ID'] = df['ORG_ID'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
# df['SRC_ORG_CD'] = df['SRC_ORG_CD'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
# df['ORG_TYPE_ID'] = df['ORG_TYPE_ID'].str.strip().astype('int').replace('NULL',101).fillna(101)
# df['ORG_NM'] = df['ORG_NM'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
# df['PARENT_ORG_ID'] = df['PARENT_ORG_ID'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
# df['PARENT_ORG_NM'] = df['PARENT_ORG_NM'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
# df['WM_RDC_NUM'] = df['WM_RDC_NUM'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
# df['WM_STORE_NUM'] = df['WM_STORE_NUM'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
# df['WM_DSTRBTR_NO'] = df['WM_DSTRBTR_NO'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
# df['WH_IND'] = df['WH_IND'].str.strip().replace('NULL',101).astype('float64').fillna(101)
# df['DSV_IND'] = df['DSV_IND'].str.strip().replace('NULL',101).astype('float64').fillna(101)
# df['ACTV_IND'] = df['ACTV_IND'].str.strip().replace('NULL',101).astype('float64').fillna(101)
# df['EFF_BEGIN_DT'] = df['EFF_BEGIN_DT'].str.strip().replace('NULL','01-01-1900').fillna('01-01-1900')
# df['EFF_END_DT'] = df['EFF_END_DT'].str.strip().replace('NULL','01-01-1900').fillna('01-01-1900')
# df['CRE_DT'] = df['CRE_DT'].str.strip().replace('NULL','01-01-1900').fillna('01-01-1900')
# df['Is_Valid Flag'] = df['Is_Valid Flag'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
# df['UPD_TS'] = df['UPD_TS'].str.strip().replace('NULL','01-01-1900 00:00:00').fillna('01-01-1900 00:00:00')
# 
# =============================================================================
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
    
    src_q='''select * from [BCMPWMT].[ORG_BUSINESS_UNIT]'''

    df=pd.read_sql(src_q,conn)
    logger.info('Query executed and src data extracted')

    
    
    df['ORG_ID'] = df['ORG_ID'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['SRC_ORG_CD'] = df['SRC_ORG_CD'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['ORG_TYPE_ID'] = df['ORG_TYPE_ID'].astype('int').replace('NULL',101).fillna(101)
    df['ORG_NM'] = df['ORG_NM'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['PARENT_ORG_ID'] = df['PARENT_ORG_ID'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['PARENT_ORG_NM'] = df['PARENT_ORG_NM'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['WM_RDC_NUM'] = df['WM_RDC_NUM'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['WM_STORE_NUM'] = df['WM_STORE_NUM'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['WM_DSTRBTR_NO'] = df['WM_DSTRBTR_NO'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['WH_IND'] = df['WH_IND'].replace('NULL',101).astype('float64').fillna(101)
    df['DSV_IND'] = df['DSV_IND'].replace('NULL',101).astype('float64').fillna(101)
    df['ACTV_IND'] = df['ACTV_IND'].replace('NULL',101).astype('float64').fillna(101)
    df['EFF_BEGIN_DT'] = pd.to_datetime(df['EFF_BEGIN_DT'].replace('NULL','01-01-1900')).fillna('01-01-1900')
    df['EFF_END_DT'] = pd.to_datetime(df['EFF_END_DT'].replace('NULL','01-01-1900')).fillna('01-01-1900')
    df['CRE_DT'] = pd.to_datetime(df['CRE_DT'].replace('NULL','01-01-1900').fillna('01-01-1900'))

    df['UPD_TS'] =pd.to_datetime(df['UPD_TS'].replace('NULL','01-01-1900 00:00:00')).fillna('01-01-1900 00:00:00')


    





 
    
    cleaned_df=utils.nullhandler(df)
    logger.info('Null values handled')
    

    cursor.fast_executemany = True
    
    insert_to_tmp_tbl_stmt='''insert into stg_DIM_ORG_BUSINESS_UNIT_python_IN1542
    values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?)'''
    collist=[
'ORG_ID',
'SRC_ORG_CD',
'ORG_TYPE_ID',
'ORG_NM',
'PARENT_ORG_ID',
'PARENT_ORG_NM',
'WM_RDC_NUM',
'WM_STORE_NUM',
'WM_DSTRBTR_NO',
'WH_IND',
'DSV_IND',
'ACTV_IND',
'EFF_BEGIN_DT',
'EFF_END_DT',
'CRE_DT',



'UPD_TS']



    cursor.executemany(insert_to_tmp_tbl_stmt, cleaned_df[collist].values.tolist())
 
    conn.commit()
    
   
    target_insert='''insert into DIM_ORG_BUSINESS_UNIT_python_IN1542
    select  
ORG_ID,
SRC_ORG_CD,
ORG_TYPE_ID,
ORG_NM,
PARENT_ORG_ID,
PARENT_ORG_NM,
WM_RDC_NUM,
WM_STORE_NUM,
WM_DSTRBTR_NO,
WH_IND,
DSV_IND,
ACTV_IND,
EFF_BEGIN_DT,
EFF_END_DT,
CRE_DT,
Is_Valid_Flag,
UPD_TS


          
          
          
          
    from stg_DIM_ORG_BUSINESS_UNIT_python_IN1542'''
    
    cursor.execute(target_insert)
    conn.commit()
# =============================================================================
#     
#   ORG_NM
# PARENT_ORG_ID
# PARENT_ORG_NM
# WM_RDC_NUM
# WM_STORE_NUM
# WM_DSTRBTR_NO
# =============================================================================
  




    sc2_flag='''
    
    insert into DIM_ORG_BUSINESS_UNIT_python_IN1542
    SELECT
    a.[ORG_ID],
    a.[SRC_ORG_CD],
    a.[ORG_TYPE_ID],
    a.[ORG_NM],
    a.[PARENT_ORG_ID],
    a.[PARENT_ORG_NM],
    a.[WM_RDC_NUM],
    a.[WM_STORE_NUM],
    a.[WM_DSTRBTR_NO],
    a.[WH_IND],
    a.[DSV_IND],
    a.[ACTV_IND],
    a.[EFF_BEGIN_DT],
    a.[EFF_END_DT],
    a.[CRE_DT],
    1 as Is_Valid_Flag,
    a.[UPD_TS]
    
    
    FROM stg_DIM_ORG_BUSINESS_UNIT_python_IN1542 a LEFT join DIM_ORG_BUSINESS_UNIT_python_IN1542 t
     ON a.[ORG_ID]=t.[ORG_ID]
    WHERE t.[ORG_ID] IS NULL or ((  
        
        t.ORG_NM!=a.ORG_NM or
        t.PARENT_ORG_ID!=a.PARENT_ORG_ID or
        t. PARENT_ORG_NM!=a. PARENT_ORG_NM or
        t.WM_RDC_NUM!=a.WM_RDC_NUM or
        t.WM_STORE_NUM!=a.WM_STORE_NUM  or
        t.WM_DSTRBTR_NO!=a.WM_DSTRBTR_NO
        
        
        
        
        )
        
        
        
        and t.[Is_Valid_Flag]= 1)
    
    UPDATE DIM_ORG_BUSINESS_UNIT_python_IN1542
    SET Is_Valid_Flag= 0
    FROM stg_DIM_ORG_BUSINESS_UNIT_python_IN1542 a left JOIN DIM_ORG_BUSINESS_UNIT_python_IN1542 t
    ON a.[ORG_ID]=t.[ORG_ID]
    WHERE (  
        
        t.ORG_NM!=a.ORG_NM or
        t.PARENT_ORG_ID!=a.PARENT_ORG_ID or
        t.PARENT_ORG_NM!=a. PARENT_ORG_NM or
        t.WM_RDC_NUM!=a.WM_RDC_NUM or
        t.WM_STORE_NUM!=a.WM_STORE_NUM  or
        t.WM_DSTRBTR_NO!=a.WM_DSTRBTR_NO    ) and  t.[Is_Valid_Flag]= 1'''
    cursor.execute(sc2_flag)
    conn.commit()
    




    

    
    
    
    
if __name__=='__main__':
    main()