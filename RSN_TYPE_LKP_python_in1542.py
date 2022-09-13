import utils
import pandas as pd
from datetime import datetime
import logging

logger=utils.setlogger(logfile='DIM_CHARGE_CATEG_PY.log')

def main():
    conn,cursor= utils.create_conn()
    logger.info('connect created')
    
    src_query='''
    select * from stg_Dim_RSN_TYPE_LKP_python_in1542

    '''
# =============================================================================
# =============================================================================
# create table stg_Dim_RSN_TYPE_LKP_python_in1542(
# RSN_TYPE_ID  INT  NOT NULL			,
# RSN_TYPE_CD  varchar(50)  NOT NULL		,	
# RSN_TYPE_DESC  varchar(200)  NOT NULL	,		
# CRE_TS  datetime  NOT NULL			,
# CRE_USER  varchar(50)  NOT NULL			,
# RSN_TYPE_LKP_KEY  int identity(1,1) )
# =============================================================================

# =============================================================================
    
    src_cust_emaildf=pd.read_sql(src_query,conn)
    logger.info('Query executed and src data extracted')
    
    
    logger.info('Applying transformations')
    src_cust_emaildf['RSN_TYPE_ID']=src_cust_emaildf['RSN_TYPE_ID'].replace('NULL',101).astype('int').fillna(101)
    src_cust_emaildf['RSN_TYPE_CD']=src_cust_emaildf['RSN_TYPE_CD'].astype(str)
    src_cust_emaildf['RSN_TYPE_DESC ']=src_cust_emaildf['RSN_TYPE_DESC'].astype(str)
  
    #src_cust_emaildf['SRC_EMAIL_ID']=src_cust_emaildf['SRC_EMAIL_ID'].apply(lambda x: x.strip() if len(x)>5 else str.upper(x.strip()))
            
    src_cust_emaildf['CRE_TS']=pd.to_datetime(src_cust_emaildf['CRE_TS'],infer_datetime_format=True)
    src_cust_emaildf['CRE_USER']=src_cust_emaildf['CRE_USER'].replace('NULL','N/A').fillna('N/A')
 
 
    
    cleaned_df=utils.nullhandler(src_cust_emaildf)
    logger.info('Null values handled')
    
    insertstmt=''
    for index,row in cleaned_df.iterrows():
       
        insertstmt+=f'''insert into Dim_RSN_TYPE_LKP_python_in1542
        values ({row['RSN_TYPE_ID']},'{row['RSN_TYPE_CD']}','{row['RSN_TYPE_DESC']}','{row['CRE_TS']}','{row['CRE_USER']}')
        '''
        print(insertstmt)
  
    cursor.execute(insertstmt)
    conn.commit()
    
if __name__=='__main__':
    main()
