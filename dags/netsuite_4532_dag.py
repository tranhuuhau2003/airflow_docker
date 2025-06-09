from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import requests
import os
import sys
import pandas as pd
from io import StringIO

# Import LoginInformation và TaraFunction
current_dir = os.path.dirname(__file__)
project_path = os.path.abspath(os.path.join(current_dir, '../plugins/Project'))
sys.path.append(project_path)

from LoginInformation import Netsuite_getLoginCookies, Netsuite_getLoginAccount
from TaraFunction import upsert_data2datalake

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='netsuite_saved_search_4532_memory',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['netsuite', 'datalake', 'memory']
) as dag:

    start = EmptyOperator(task_id='start')

    def extract_from_netsuite(**context):
        searchid = '4532'
        login_cookies = Netsuite_getLoginCookies()
        login_data = Netsuite_getLoginAccount()

        # Debug login data và cookies
        print("🔐 login_data:", login_data)
        print("🍪 login_cookies:", login_cookies)

        auth_url = 'https://7258775.app.netsuite.com/app/login/secure/enterpriselogin.nl'
        download_url = (
            f"https://7258775.app.netsuite.com/app/common/search/searchresults.csv?"
            f"searchid={searchid}&frame=be&csv=Export&report=&grid=T"
            f"&Transaction_TRANDATEfrom=01%2F01%2F2024"
        )

        session = requests.Session()
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Referer": "https://7258775.app.netsuite.com",
            "Origin": "https://7258775.app.netsuite.com"
        }

        login_response = session.post(auth_url, data=login_data, cookies=login_cookies, headers=headers)

        # Nếu trả về HTML (login page), raise lỗi
        if login_response.status_code != 200 or "<html" in login_response.text.lower():
            print("🔍 Login status code:", login_response.status_code)
            print("🔍 Content-Type:", login_response.headers.get("Content-Type", "N/A"))
            print("🔍 Response sample:", login_response.text[:300])
            raise Exception("❌ Đăng nhập NetSuite thất bại hoặc trả về HTML")

        response = session.get(download_url, verify=True, timeout=60)
        csv_text = response.text.strip()

        if response.status_code != 200 or not csv_text or "<html" in csv_text.lower():
            print("⚠️ CSV response bị lỗi hoặc là HTML:")
            print("🔍 Status:", response.status_code)
            print("🔍 CSV preview:", csv_text[:300])
            raise Exception("❌ Dữ liệu NetSuite trả về không hợp lệ")

        # Parse CSV trong memory
        df = pd.read_csv(StringIO(csv_text))
        df['bi_updatetime'] = datetime.today().replace(microsecond=0)
        df['source'] = 'netsuite_4532'

        # Đẩy JSON hóa df vào XCom
        context['ti'].xcom_push(key='df_csv', value=df.to_json(orient='records'))
        print(f"✅ Lấy {len(df)} dòng dữ liệu từ NetSuite")

    def load_to_datalake(**context):
        df_json = context['ti'].xcom_pull(key='df_csv')
        df = pd.read_json(df_json, orient='records')

        upsert_data2datalake(
            df=df,
            tablename='"NetSuite"."F99_CustomSearch4532"',
            primary_key='source'
        )
        print("✅ Đẩy dữ liệu vào Datalake thành công")

    extract = PythonOperator(
        task_id='extract_from_netsuite',
        python_callable=extract_from_netsuite,
        provide_context=True
    )

    load = PythonOperator(
        task_id='load_to_datalake',
        python_callable=load_to_datalake,
        provide_context=True
    )

    end = EmptyOperator(task_id='end')

    start >> extract >> load >> end
