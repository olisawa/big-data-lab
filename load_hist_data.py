from airflow import DAG
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

stock_spce = YahooFinancials('SPCE')
hist_data_spce = stock_spce.get_historical_price_data(str(date.today()-timedelta(days=365)), str(date.today()), 'daily')

stock_nio = YahooFinancials('NIO')
hist_data_nio = stock_nio.get_historical_price_data(str(date.today()-timedelta(days=365)), str(date.today()), 'daily')

stock_amd = YahooFinancials('AMD')
hist_data_amd = stock_amd.get_historical_price_data(str(date.today()-timedelta(days=365)), str(date.today()), 'daily')

stock_ge = YahooFinancials('GE')
hist_data_ge = stock_ge.get_historical_price_data(str(date.today()-timedelta(days=365)), str(date.today()), 'daily')

stock_tsla = YahooFinancials('TSLA')
hist_data_tsla = stock_tsla.get_historical_price_data(str(date.today()-timedelta(days=365)), str(date.today()), 'daily')


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

dag = DAG('load_hist_data_to_s3', default_args=default_args, schedule_interval='@once')

task_load_aapl = PythonOperator(
    task_id='load_AAPL',
    python_callable=upload_to_s3,
    op_kwargs={'hook': s3_hook, 'data': str(hist_data_aapl), 'key': 'yahoo-fin/hist_data/hist_data_aapl.json'},
    dag=dag
)

task_load_wfc = PythonOperator(
    task_id='load_WFC',
    python_callable=upload_to_s3,
    op_kwargs={'hook': s3_hook, 'data': str(hist_data_wfc), 'key': 'yahoo-fin/hist_data/hist_data_wfc.json'},
    dag=dag
)

task_load_dao = PythonOperator(
    task_id='load_DAO',
    python_callable=upload_to_s3,
    op_kwargs={'hook': s3_hook, 'data': str(hist_data_dao), 'key': 'yahoo-fin/hist_data/hist_data_dao.json'},
    dag=dag
)

task_load_uber = PythonOperator(
    task_id='load_UBER',
    python_callable=upload_to_s3,
    op_kwargs={'hook': s3_hook, 'data': str(hist_data_uber), 'key': 'yahoo-fin/hist_data/hist_data_uber.json'},
    dag=dag
)

task_load_vir = PythonOperator(
    task_id='load_VIR',
    python_callable=upload_to_s3,
    op_kwargs={'hook': s3_hook, 'data': str(hist_data_vir), 'key': 'yahoo-fin/hist_data/hist_data_vir.json'},
    dag=dag
)

task_load_spce = PythonOperator(
    task_id='load_SPCE',
    python_callable=upload_to_s3,
    op_kwargs={'hook': s3_hook, 'data': str(hist_data_spce), 'key': 'yahoo-fin/hist_data/hist_data_spce.json'},
    dag=dag
)

task_load_nio = PythonOperator(
    task_id='load_NIO',
    python_callable=upload_to_s3,
    op_kwargs={'hook': s3_hook, 'data': str(hist_data_nio), 'key': 'yahoo-fin/hist_data/hist_data_nio.json'},
    dag=dag
)

task_load_amd = PythonOperator(
    task_id='load_AMD',
    python_callable=upload_to_s3,
    op_kwargs={'hook': s3_hook, 'data': str(hist_data_amd), 'key': 'yahoo-fin/hist_data/hist_data_amd.json'},
    dag=dag
)

task_load_ge = PythonOperator(
    task_id='load_GE',
    python_callable=upload_to_s3,
    op_kwargs={'hook': s3_hook, 'data': str(hist_data_ge), 'key': 'yahoo-fin/hist_data/hist_data_ge.json'},
    dag=dag
)

task_load_tsla = PythonOperator(
    task_id='load_TSLA',
    python_callable=upload_to_s3,
    op_kwargs={'hook': s3_hook, 'data': str(hist_data_tsla), 'key': 'yahoo-fin/hist_data/hist_data_tsla.json'},
    dag=dag
)

task_load_aapl >> task_load_wfc >> task_load_dao >> task_load_uber >> task_load_vir >> task_load_spce >> task_load_nio >> task_load_amd >> task_load_ge >> task_load_tsla
