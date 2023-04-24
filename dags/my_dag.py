from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
import pandas as pd
import numpy as np
from datetime import datetime
def pipeline_initialized():
    print("Pipeline initialized Successfully")

def replace_cat_feature(dataset,features_nan):
    data=dataset.copy()
    data[features_nan]=data[features_nan].fillna('Missing')
    return data

def missing_values_category():
    dataset = pd.read_csv('dags/files/train.csv')
    features_nan = [feature for feature in dataset.columns if
                    dataset[feature].isnull().sum() > 1 and dataset[feature].dtypes == 'O']
    dataset = replace_cat_feature(dataset, features_nan)

def missing_values_numerical():
    dataset = pd.read_csv('dags/files/train.csv')
    numerical_with_nan = [feature for feature in dataset.columns if
                          dataset[feature].isnull().sum() > 1 and dataset[feature].dtypes != 'O']
    for feature in numerical_with_nan:
        median_value = dataset[feature].median()
        dataset[feature + 'nan'] = np.where(dataset[feature].isnull(), 1, 0)
        dataset[feature].fillna(median_value, inplace=True)

def pipeline_completed():
    print("Pipeline completed successfully")



default_args={
    "owner":"airflow",
    "email_on_failure":False,
    "email_on_retry":False,
    "retries":1,
}
with DAG("my_dag",start_date=datetime(2021,1,1),
         schedule_interval="@daily",default_args=default_args) as dag:

    task_1= PythonOperator(
        task_id="Pipeline_initialized",
        python_callable=pipeline_initialized
    )

    task_2 = FileSensor(
        task_id="Checking_if_file_is_present",
        fs_conn_id="file_path",
        filepath="train.csv",
    )

    task_3 = PythonOperator(
        task_id="Handling_missing_values_for_Category",
        python_callable= missing_values_category
    )

    task_4 = PythonOperator(
        task_id="Handling_missing_values_for_Numerical",
        python_callable=missing_values_numerical
    )

    task_5 = PythonOperator(
        task_id="Pipeline_completed",
        python_callable=pipeline_completed
    )

    task_1 >> task_2 >> task_3 >> task_4 >> task_5