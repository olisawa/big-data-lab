from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, date

from yahoofinancials import YahooFinancials


def upload_to_s3(hook, data, key):

    hook.load_string(string_data=data, key=key, bucket_name='l.klimovich', replace=True)


s3_hook = S3Hook(aws_conn_id='S3_connect')
start_date = str(date.today()-timedelta(days=1))
end_date = str(date.today())

stock_aapl = YahooFinancials('AAPL')
daily_data_aapl = stock_aapl.get_historical_price_data(start_date, end_date, 'daily')

stock_wfc = YahooFinancials('WFC')
daily_data_wfc = stock_wfc.get_historical_price_data(start_date, end_date, 'daily')

stock_dao = YahooFinancials('DAO')
daily_data_dao = stock_dao.get_historical_price_data(start_date, end_date, 'daily')

stock_uber = YahooFinancials('UBER')
daily_data_uber = stock_uber.get_historical_price_data(start_date, end_date, 'daily')

stock_vir = YahooFinancials('VIR')
daily_data_vir = stock_vir.get_historical_price_data(start_date, end_date, 'daily')

stock_spce = YahooFinancials('SPCE')
daily_data_spce = stock_spce.get_historical_price_data(start_date, end_date, 'daily')

stock_nio = YahooFinancials('NIO')
daily_data_nio = stock_nio.get_historical_price_data(start_date, end_date, 'daily')

stock_amd = YahooFinancials('AMD')
daily_data_amd = stock_amd.get_historical_price_data(start_date, end_date, 'daily')

stock_ge = YahooFinancials('GE')
daily_data_ge = stock_ge.get_historical_price_data(start_date, end_date, 'daily')

stock_tsla = YahooFinancials('TSLA')
daily_data_tsla = stock_tsla.get_historical_price_data(start_date, end_date, 'daily')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': '2020-02-19',
    'email': ['olisawa@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('load_daily_stocks_data_to_s3', default_args=default_args, schedule_interval='@daily')

task_load_aapl = PythonOperator(
    task_id='load_AAPL',
    python_callable=upload_to_s3,
    op_kwargs={'hook': s3_hook, 'data': str(daily_data_aapl), 'key': f'yahoo-fin/AAPL/data_{start_date}.json'},
    dag=dag
)

task_load_wfc = PythonOperator(
    task_id='load_WFC',
    python_callable=upload_to_s3,
    op_kwargs={'hook': s3_hook, 'data': str(daily_data_wfc), 'key': f'yahoo-fin/WFC/data_{start_date}.json'},
    dag=dag
)

task_load_dao = PythonOperator(
    task_id='load_DAO',
    python_callable=upload_to_s3,
    op_kwargs={'hook': s3_hook, 'data': str(daily_data_dao), 'key': f'yahoo-fin/DAO/data_{start_date}.json'},
    dag=dag
)

task_load_uber = PythonOperator(
    task_id='load_UBER',
    python_callable=upload_to_s3,
    op_kwargs={'hook': s3_hook, 'data': str(daily_data_uber), 'key': f'yahoo-fin/UBER/data_{start_date}.json'},
    dag=dag
)

task_load_vir = PythonOperator(
    task_id='load_VIR',
    python_callable=upload_to_s3,
    op_kwargs={'hook': s3_hook, 'data': str(daily_data_vir), 'key': f'yahoo-fin/VIR/data_{start_date}.json'},
    dag=dag
)

task_load_spce = PythonOperator(
    task_id='load_SPCE',
    python_callable=upload_to_s3,
    op_kwargs={'hook': s3_hook, 'data': str(daily_data_spce), 'key': f'yahoo-fin/SPCE/data_{start_date}.json'},
    dag=dag
)

task_load_nio = PythonOperator(
    task_id='load_NIO',
    python_callable=upload_to_s3,
    op_kwargs={'hook': s3_hook, 'data': str(daily_data_nio), 'key': f'yahoo-fin/NIO/data_{start_date}.json'},
    dag=dag
)

task_load_amd = PythonOperator(
    task_id='load_AMD',
    python_callable=upload_to_s3,
    op_kwargs={'hook': s3_hook, 'data': str(daily_data_amd), 'key': f'yahoo-fin/AMD/data_{start_date}.json'},
    dag=dag
)

task_load_ge = PythonOperator(
    task_id='load_GE',
    python_callable=upload_to_s3,
    op_kwargs={'hook': s3_hook, 'data': str(daily_data_ge), 'key': f'yahoo-fin/GE/data_{start_date}.json'},
    dag=dag
)

task_load_tsla = PythonOperator(
    task_id='load_TSLA',
    python_callable=upload_to_s3,
    op_kwargs={'hook': s3_hook, 'data': str(daily_data_tsla), 'key': f'yahoo-fin/TSLA/data_{start_date}.json'},
    dag=dag
)

task_load_aapl >> task_load_wfc >> task_load_dao >> task_load_uber >> task_load_vir >> task_load_spce >> task_load_nio >> task_load_amd >> task_load_ge >> task_load_tsla