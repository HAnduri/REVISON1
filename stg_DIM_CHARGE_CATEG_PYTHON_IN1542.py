
import utils
import pandas as pd
from datetime import datetime as dt
import numpy as np
import importlib
importlib.reload(utils)
import logging


def main():
     conn,cursor= utils.create_conn()
     logger.info('connect created')
     charge_quary = ''' select * from [BCMPWMT].CHARGE_CATEG_LKP'''
     dim_charge_quary = pd.read_sql(charge_quary,conn)

    
        
    
dim_charge_quary['CHARGE_CATEG_ID']=dim_charge_quary['CHARGE_CATEG_ID'].str.strip().astype(int)
dim_charge_quary['TENANT_ORG_ID'] = dim_charge_quary['TENANT_ORG_ID'].str.strip().astype(int)
dim_charge_quary['CHARGE_CATEG'] = dim_charge_quary['CHARGE_CATEG'].apply(lambda x: x.strip() if len(x)>5  else str.upper(x.strip()))
dim_charge_quary['CHARGE_CATEG_DESC'] = dim_charge_quary['CHARGE_CATEG_DESC'].str.strip()
dim_charge_quary['TAX_IND']=dim_charge_quary['TAX_IND'].str.strip().astype(int)
             
         
         
cleaned_df=utils.null_handling(dim_charge_quary)
truncate_table ='''truncate table STG_DIM_CHARGE_CATEG_PYTHON_IN1542'''
conn.execute(truncate_table)
conn.commit()
insertstmt=''
for index,row in cleaned_df.iterrows():
   
 insertstmt+=f'''insert into STG_DIM_CHARGE_CATEG_PYTHON_IN1542
values ({row['CHARGE_CATEG_ID']},{row['TENANT_ORG_ID']},'{row['CHARGE_CATEG']}','{row['CHARGE_CATEG_DESC']}', {row['TAX_IND']},1)
'''
 print(insertstmt)
  
 cursor.execute(insertstmt)
 conn.commit()
 scd1_quary = '''INSERT INTO DIM_CHARGE_CATEG_PYTHON_IN1542
            SELECT s.CHARGE_CATEG_ID
            , s.TENANT_ORG_ID
            , s.CHARGE_CATEG
            , s.CHARGE_CATEG_DESC
            , s.TAX_IND,
            CASE
            WHEN t.CHARGE_CATEG_ID IS NULL THEN 1
            ELSE 1+(SELECT MAX(t.Version) FROM DIM_CHARGE_CATEG_PYTHON_IN1548 t JOIN STG_DIM_CHARGE_CATEG_PYTHON_IN1542 s ON s.CHARGE_CATEG_ID = t.CHARGE_CATEG_ID WHERE t.CHARGE_CATEG <> s.CHARGE_CATEG) END as Version
            FROM STG_DIM_CHARGE_CATEG_PYTHON_IN1548 s
            LEFT JOIN DIM_CHARGE_CATEG_PYTHON_IN1548 t
            ON t.CHARGE_CATEG_ID = s.CHARGE_CATEG_ID
            LEFT JOIN
            (SELECT CHARGE_CATEG_ID, MAX(Version) as Max_Version from DIM_CHARGE_CATEG_PYTHON_IN1548 GROUP BY CHARGE_CATEG_ID) a
            on t.CHARGE_CATEG_ID=a.CHARGE_CATEG_ID
            WHERE t.CHARGE_CATEG_ID IS NULL OR ((t.CHARGE_CATEG_ID IS NOT NULL) AND (t.CHARGE_CATEG <> s.CHARGE_CATEG ) AND 
            t.Version = a.Max_Version)
            '''
            
 cursor.execute(scd1_quary)
 conn.commit()
 
if __name__='__main__':
main()