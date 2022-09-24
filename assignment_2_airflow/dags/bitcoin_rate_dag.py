from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

import requests
import json

url = "https://api.coindesk.com/v1/bpi/currentprice.json"

def get_testing_increase():
    """
    Gets totalTestResultsIncrease field from Covid API for given state and returns value
    """
    res = requests.get(url)
    testing_increase = json.loads(res.text)
    return testing_increase
    #ti.xcom_push(key='testing_increase', value=testing_increase)

def get_time_update(ti):
    data = get_testing_increase()
    time = data['time']['updated']
    ti.xcom_push(key='time_updated', value=time)

def get_usd_rate(ti):
    data = get_testing_increase()
    usd = data['bpi']['USD']['rate_float']
    ti.xcom_push(key='usd_rate', value=usd)

def get_gbp_rate(ti):
    data = get_testing_increase()
    gbp = data['bpi']['GBP']['rate_float']
    ti.xcom_push(key='gbp_rate', value=gbp)

def get_eur_rate(ti):
    data = get_testing_increase()
    eur = data['bpi']['EUR']['rate_float']
    ti.xcom_push(key='gbp_rate', value=eur)


# Default settings applied to all tasks
default_args = {
    'start_date': datetime(2022, 1, 1)
}

with DAG('xcom_dag',
         schedule_interval=timedelta(minutes=1),
         default_args=default_args,
         catchup=False
         ) as dag:

    get_data = PythonOperator(
        task_id = 'get_bitcoin_rate',
        python_callable=get_testing_increase,
    )

    get_time_update = PythonOperator(
        task_id = 'time_update', 
        python_callable=get_time_update,
    )

    get_usd_rate = PythonOperator(
        task_id = 'usd_rate', 
        python_callable=get_usd_rate,
    )

    get_gbp_rate = PythonOperator(
        task_id = 'gbp_rate', 
        python_callable=get_gbp_rate,
    )

    get_eur_rate = PythonOperator(
        task_id = 'eur_rate', 
        python_callable=get_eur_rate,
    )

    get_data >> [get_time_update, get_usd_rate, get_gbp_rate, get_eur_rate]