

import utils
import pandas as pd
from datetime import datetime
import logging

logger=utils.setlogger(logfile='DIM_CHARGE_CATEG_PY.log')

def main():
    conn,cursor= utils.create_conn()
    logger.info('connect created')
    
    src_query='''
    select * from [BCMPWMT].[ORG_TYPE_LKP]

    '''

    df=pd.read_sql(src_query,conn)
    logger.info('Query executed and src data extracted')

    

    df['ORG_TYPE_ID'] = df['ORG_TYPE_ID'].astype('int').replace('NULL',101).fillna(101)
    df['ORG_TYPE_CD'] = df['ORG_TYPE_CD'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['ORG_TYPE_DESC'] = df['ORG_TYPE_DESC'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['ORG_TYPE_NM'] = df['ORG_TYPE_NM'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['PARENT_ORG_TYPE_NM'] = df['PARENT_ORG_TYPE_NM'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['PARENT_ORG_TYPE_CD'] = df['PARENT_ORG_TYPE_CD'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
    df['CRE_DT'] = pd.to_datetime(df['CRE_DT'].replace('NULL','01-01-1900')).fillna('01-01-1900')
    df['CRE_USER'] = df['CRE_USER'].str.strip().replace('NULL','N/A').astype('str').fillna('N/A')
 


  

    cursor.fast_executemany = True
            
    insert_to_tmp_tbl_stmt='''insert into  stg_Dim_ORG_TYPE_LKP_python_IN1542
            values (?,?,?,?,?,?,?,?,null,null
                    )'''
    collist=[
        'ORG_TYPE_ID',
        'ORG_TYPE_CD',
        'ORG_TYPE_DESC',
        'ORG_TYPE_NM',
        'PARENT_ORG_TYPE_NM',
        'PARENT_ORG_TYPE_CD',
        'CRE_DT',
        'CRE_USER'
      ]
    cursor.executemany(insert_to_tmp_tbl_stmt, df[collist].values.tolist())
    conn.commit()
    
           
    target_insert='''insert into  Dim_ORG_TYPE_LKP_python_IN1542
            select  
       ORG_TYPE_ID,
    ORG_TYPE_CD,
    ORG_TYPE_DESC,
    ORG_TYPE_NM,
    PARENT_ORG_TYPE_NM,
    PARENT_ORG_TYPE_CD,
    CRE_DT,
    CRE_USER,
    p1,
    p2
    
    
        
        
        
        
            from  stg_Dim_ORG_TYPE_LKP_python_IN1542'''
            
    cursor.execute(target_insert)
    conn.commit()
    
    


    scd3='''
    INSERT INTO Dim_ORG_TYPE_LKP_python_IN1542
    
    SELECT 
    a.[ORG_TYPE_ID],
    a.[ORG_TYPE_CD],
    a.[ORG_TYPE_DESC],
    a.[ORG_TYPE_NM],
    a.[PARENT_ORG_TYPE_NM],
    a.[PARENT_ORG_TYPE_CD],
    a.[CRE_DT],
    a.[CRE_USER],
    NULL as p1,NULL AS p2
    FROM stg_Dim_ORG_TYPE_LKP_python_IN1542 a  LEFT JOIN  Dim_ORG_TYPE_LKP_python_IN1542 t ON a.[ORG_TYPE_ID] = t.[ORG_TYPE_ID]
    WHERE  t.[ORG_TYPE_ID] IS NULL
    
    UPDATE Dim_ORG_TYPE_LKP_python_IN1542
    SET [PARENT_ORG_TYPE_NM]=a.[PARENT_ORG_TYPE_NM] ,p1=t.[PARENT_ORG_TYPE_NM],p2=t.p1
    FROM stg_Dim_ORG_TYPE_LKP_python_IN1542 a left JOIN Dim_ORG_TYPE_LKP_python_IN1542 t ON a.[ORG_TYPE_ID] = t.[ORG_TYPE_ID]
    WHERE t.[PARENT_ORG_TYPE_NM] <> a.[PARENT_ORG_TYPE_NM] '''
    
    
    cursor.execute(scd3)
    conn.commit()
        
    
    
    
if __name__=='__main__':
    main()
    
if __name__=='__main__':
    main()