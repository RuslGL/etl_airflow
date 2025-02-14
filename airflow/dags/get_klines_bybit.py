import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta, timezone
import requests
from clickhouse_driver import Client

from dotenv import load_dotenv

load_dotenv(load_dotenv('/root/airflow/.env'))

user = str(os.getenv('user'))
password = str(os.getenv('password'))



###### UTILS FUNCTIONS ######
def round_down_to_5_minutes(dt):
    return dt - timedelta(minutes=dt.minute % 5, seconds=dt.second, microseconds=dt.microsecond)


def format_dtms_to_time(ts_ms):
    timestamp_s = int(ts_ms) // 1000
    return datetime.fromtimestamp(timestamp_s, timezone.utc)


###### EXTRACT FUNCTIONS ######
def get_kline(symbol, **context):
    execution_time = context['execution_date']
    time_str = execution_time.isoformat()

    kline_time = datetime.fromisoformat(time_str) - timedelta(minutes=5)
    rounded_dt = round_down_to_5_minutes(kline_time)
    kline_time = int(rounded_dt.timestamp() * 1000)  # start time

    params = {
        'category': 'spot',
        'symbol': symbol,
        'interval': '5',
        'start': kline_time,
        'limit': 1
    }
    end_point = '/v5/market/kline'
    url = 'https://api.bybit.com'

    res = requests.get(url + end_point, params).json()
    return res  # XCom автоматически передаст это значение


###### TRANSFORM FUNCTIONS ######
def transform_data(**context):
    ti = context['ti']
    extract_tasks = ['extract_data_one', 'extract_data_two', 'extract_data_three',
                     'extract_data_four', 'extract_data_five', 'extract_data_six',
                     'extract_data_seven', 'extract_data_eight', 'extract_data_nine',
                     'extract_data_ten']

    all_data = []
    for task in extract_tasks:
        data = ti.xcom_pull(task_ids=task)
        if data and data.get('retMsg') == 'OK' and data.get('result').get('list'):
            result = data.get('result').get('list')[0]
            values = (
                data.get('result').get('symbol')[:-4],
                format_dtms_to_time(result[0]).isoformat(),  # Сохраняем как строку
                float(result[1]),
                float(result[2]),
                float(result[3]),
                float(result[4]),
                float(result[5]),
                float(result[6]),
            )
            all_data.append(values)

    return all_data


###### LOAD FUNCTIONS ######
def upload_to_clickhouse(**context):
    ti = context['ti']
    values = ti.xcom_pull(task_ids='transform_data')
    print('user', user, password)

    client = Client(
        host='87.236.22.62',
        user=user,
        password=password,
        database='bybit_history'
    )

    if values:
        fixed_values = [(v[0], datetime.fromisoformat(v[1]), *v[2:]) for v in values]
        client.execute(f'INSERT INTO five_min_klines VALUES', fixed_values)
        print("Данные успешно загружены")
    else:
        print("Нет данных для загрузки")

# 'BTCUSDT' 'XRPUSDT' 'ETHUSDT' 'SOLUSDT' 'LTCUSDT' 'SUIUSDT' 'BNBUSDT' 'TRUMPUSDT' 'DOGEUSDT' 'BCNUSDT'

###### DAG ######
with DAG(
        'bybit_demo',
        schedule_interval='5 * * * *',
        start_date=datetime(2025, 1, 1),
        # end_date=datetime(2024,1,2),
        max_active_runs=1
) as dag:



    task_extract_one = PythonOperator(
        task_id='extract_data_one',
        python_callable=get_kline,
        provide_context=True,
        op_kwargs={'symbol': 'BTCUSDT'}
    )



    task_extract_two = PythonOperator(
        task_id='extract_data_two',
        python_callable=get_kline,
        provide_context=True,
        op_kwargs={'symbol': 'XRPUSDT'}
    )

    task_extract_three = PythonOperator(
        task_id='extract_data_three',
        python_callable=get_kline,
        provide_context=True,
        op_kwargs={'symbol': 'ETHUSDT'}
    )

    task_extract_four = PythonOperator(
        task_id='extract_data_four',
        python_callable=get_kline,
        provide_context=True,
        op_kwargs={'symbol': 'SOLUSDT'}
    )

    task_extract_five = PythonOperator(
        task_id='extract_data_five',
        python_callable=get_kline,
        provide_context=True,
        op_kwargs={'symbol': 'LTCUSDT'}
    )

    task_extract_six = PythonOperator(
        task_id='extract_data_six',
        python_callable=get_kline,
        provide_context=True,
        op_kwargs={'symbol': 'SUIUSDT'}
    )

    task_extract_seven = PythonOperator(
        task_id='extract_data_seven',
        python_callable=get_kline,
        provide_context=True,
        op_kwargs={'symbol': 'BNBUSDT'}
    )

    task_extract_eight = PythonOperator(
        task_id='extract_data_eight',
        python_callable=get_kline,
        provide_context=True,
        op_kwargs={'symbol': 'TRUMPUSDT'}
    )

    task_extract_nine = PythonOperator(
        task_id='extract_data_nine',
        python_callable=get_kline,
        provide_context=True,
        op_kwargs={'symbol': 'DOGEUSDT'}
    )

    task_extract_ten = PythonOperator(
        task_id='extract_data_ten',
        python_callable=get_kline,
        provide_context=True,
        op_kwargs={'symbol': 'BCNUSDT'}
    )


    task_transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
    )

    task_upload = PythonOperator(
        task_id='upload_to_clickhouse',
        python_callable=upload_to_clickhouse,
        provide_context=True,
    )

    [task_extract_one, task_extract_two, task_extract_three, task_extract_four, task_extract_five, task_extract_six, task_extract_seven, task_extract_eight, task_extract_nine, task_extract_ten] >> task_transform >> task_upload
