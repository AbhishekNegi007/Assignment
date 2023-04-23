import contextlib
import numpy as np
import pandas as pd
import json
from sqlalchemy import create_engine
from datetime import datetime,timedelta
from airflow import DAG,XComArg
from airflow.operators.python_operator import PythonOperator


def read_csv_file(path = "csv/sales.csv"):
    return (pd.read_csv(path).to_json(orient='records'))    

def tranform_df(df):
    df = pd.DataFrame(json.loads(df))
    for col in df.columns:
        #Check for the object type column
        
        #Will try to convert each object column to date if it is not ditected by the pandas library
        if df[col].dtype == 'object':
            with contextlib.suppress(Exception):
                #try to convert into datetime column 
                #if failed, then suppress the exception as it cannot be datetime object
                df[col] = pd.to_datetime(df[col])
    
    #Replace the dataframe data having space
    df = df.replace(r'^\s*$', np.NaN, regex=True) 
    
    #Drop the Nan values
    df.dropna(inplace=True)
    return df.to_json(orient='records')

def store_into_database(df):
    df = pd.DataFrame(json.loads(df))
    try:
        engine = create_engine('postgresql://airflow:airflow@postgresql:5432')
        df.to_sql('sales_tbl', engine, if_exists='replace', index=False)
    except Exception as exc:
        return (f" Not able to the Store the Dataframe into database\nException : {str(exc)}")



default_args = {
    'owner': 'user',
    'start_date': datetime(2023, 4, 22),
}


dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='Read csv file and dump into the database',
    schedule_interval=timedelta(days=1),
)

read_data = PythonOperator(
    task_id='read_data',
    python_callable=read_csv_file,
    dag=dag,
)

trasform_data = PythonOperator(
    task_id='trasform_data',
    python_callable=tranform_df,
    op_kwargs={'df':'{{ti.xcom_pull(task_ids="read_data") }}'},
    dag=dag,
)

store_data = PythonOperator(
    task_id='store_data',
    python_callable=store_into_database,
    op_kwargs={'df':'{{ti.xcom_pull(task_ids="trasform_data") }}'},
    dag=dag,
)

read_data >> trasform_data >> store_data