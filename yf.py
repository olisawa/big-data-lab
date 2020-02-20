from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, date

from yahoofinancials import YahooFinancials


def upload_to_s3(hook, data, key):

    hook.load_string(string_data=data, key=key, bucket_name='l.klimovich', replace=True)


bucket = 'l.klimovich'
s3_hook = S3Hook(aws_conn_id='S3_connect')

stock_aapl = YahooFinancials('AAPL')
hist_data_aapl = stock_aapl.get_historical_price_data(str(date.today()-timedelta(days=365)), str(date.today()), 'daily')

stock_wfc = YahooFinancials('WFC')
hist_data_wfc = stock_wfc.get_historical_price_data(str(date.today()-timedelta(days=365)), str(date.today()), 'daily')

stock_dao = YahooFinancials('DAO')
hist_data_dao = stock_dao.get_historical_price_data(str(date.today()-timedelta(days=365)), str(date.today()), 'daily')

stock_uber = YahooFinancials('UBER')
hist_data_uber = stock_uber.get_historical_price_data(str(date.today()-timedelta(days=365)), str(date.today()), 'daily')

stock_vir = YahooFinancials('VIR')
hist_data_vir = stock_vir.get_historical_price_data(str(date.today()-timedelta(days=365)), str(date.today()), 'daily')

stock_vir = YahooFinancials('VIR')
hist_data_vir = stock_vir.get_historical_price_data(str(date.today()-timedelta(days=365)), str(date.today()), 'daily')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': '2020-02-18', #datetime.today(),
    'email': ['olisawa@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('load_10_years_data_to_s3', default_args=default_args, schedule_interval='@once')

t1 = PythonOperator(
    task_id='load_AAPL',
    python_callable=upload_to_s3,
    op_kwargs={'hook': s3_hook, 'data': str(hist_data_aapl), 'key': 'yahoo-fin/hist_data/hist_data_aapl.json'},
    dag=dag
)

t2 = PythonOperator(
    task_id='load_WFC',
    python_callable=upload_to_s3,
    op_kwargs={'hook': s3_hook, 'data': str(hist_data_wfc), 'key': 'yahoo-fin/hist_data/hist_data_wfc.json'},
    dag=dag
)

t3 = PythonOperator(
    task_id='load_DAO',
    python_callable=upload_to_s3,
    op_kwargs={'hook': s3_hook, 'data': str(hist_data_dao), 'key': 'yahoo-fin/hist_data/hist_data_dao.json'},
    dag=dag
)

t4 = PythonOperator(
    task_id='load_UBER',
    python_callable=upload_to_s3,
    op_kwargs={'hook': s3_hook, 'data': str(hist_data_uber), 'key': 'yahoo-fin/hist_data/hist_data_uber.json'},
    dag=dag
)

t5 = PythonOperator(
    task_id='load_VIR',
    python_callable=upload_to_s3,
    op_kwargs={'hook': s3_hook, 'data': str(hist_data_vir), 'key': 'yahoo-fin/hist_data/hist_data_vir.json'},
    dag=dag
)

t1 >> t2 >> t3 >> t4 >> t5
