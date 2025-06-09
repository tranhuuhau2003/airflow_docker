from datetime import datetime, date
import pandas as pd
import requests
import os
from io import StringIO
import sys
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import LoginInformation và TaraFunction
current_dir = os.path.dirname(__file__)
project_path = os.path.abspath(os.path.join(current_dir, '../plugins/Project'))
sys.path.append(project_path)

from LoginInformation import *
from TaraFunction import *

def download_and_upsert_F99():
    session = requests.Session()

    # Login NetSuite
    my_cookies = Netsuite_getLoginCookies()
    login_data = Netsuite_getLoginAccount()
    auth_url = 'https://7258775.app.netsuite.com/app/login/secure/enterpriselogin.nl'
    session.post(auth_url, data=login_data, cookies=my_cookies)

    # Tải dữ liệu
    search_url = "https://7258775.app.netsuite.com/app/common/search/searchresults.csv?searchid=4532&frame=be&csv=Export&report="
    response = session.get(search_url, verify=True, allow_redirects=True, timeout=60, stream=True)

    if response.status_code != 200:
        raise Exception(f"[F99] Không thể tải file. Mã lỗi HTTP: {response.status_code}")

    if "<html" in response.text[:300].lower():
        raise Exception(f"[F99] Nội dung trả về là HTML. Có thể là lỗi quyền hoặc login NetSuite.")

    raw_text = response.content.decode('utf-8-sig')
    print("[F99] Preview nội dung:", raw_text[:500])

    try:
        df = pd.read_csv(StringIO(raw_text), skiprows=0, dtype=str)
        print(f"[F99] Dòng sau khi đọc vào pandas: {len(df)}")

        if df.empty or df.shape[1] < 2:
            raise ValueError("[F99] DataFrame trống hoặc không đúng format (có thể do sai skiprows hoặc encoding)")

    except Exception as e:
        print("[F99] Lỗi đọc CSV:", str(e))
        raise Exception("Không thể tải hoặc dữ liệu không hợp lệ")

    df.columns = df.columns.str.strip()
    df['bi_updatetime'] = datetime.today().replace(microsecond=0)
    df['filename'] = date.today().strftime("%Y%m")

    try:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce', format='%d/%m/%Y')
        start_date = pd.to_datetime("2022-01-01")
        end_date = pd.to_datetime("2023-12-31")
        df = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)]
    except Exception as e:
        raise Exception(f"[F99] Không thể xử lý lọc ngày từ cột 'Date': {str(e)}")

    print(f"[F99] Số dòng sau khi lọc từ 01/01/2022 đến 31/12/2023: {len(df)}")

    chunk_size = 50000
    total_rows = len(df)
    for i in range(0, total_rows, chunk_size):
        chunk = df.iloc[i:i+chunk_size]
        print(f"[F99] Ghi chunk từ dòng {i} đến {min(i+chunk_size, total_rows)}")
        upsert_data2datalake(
            df=chunk,
            tablename='"NetSuite"."F98_CustomSearch_4532_22to24"',
            primary_key='filename'
        )

def upsert_F99_to_warehouse():
    import pandas as pd

    query = """
        SELECT * FROM "NetSuite"."F98_CustomSearch_4532_22to24"
        WHERE "Date" BETWEEN '2022-01-01' AND '2023-12-31'
    """
    try:
        df = DatalakeQuery(query)
        print(f"[F99 → DW] Đã lấy {len(df)} dòng từ Datalake.")
    except Exception as e:
        raise Exception(f"[F99 → DW] Lỗi khi truy vấn Datalake: {e}")

    try:
        if df.empty:
            print("[F99 → DW] Không có dữ liệu để ghi vào Data Warehouse.")
            return

        chunk_size = 50000
        total_rows = len(df)
        for i in range(0, total_rows, chunk_size):
            chunk = df.iloc[i:i+chunk_size]
            print(f"[F99 → DW] Ghi chunk từ dòng {i} đến {min(i+chunk_size, total_rows)}")
            upsert_data2datawarehouse(
                df=chunk,
                tablename='"NetSuite"."F98_CustomSearch_4532_22to24"',
                primary_key='filename'
            )

        print(f"[F99 → DW] Ghi thành công {len(df)} dòng vào Data Warehouse.")
    except Exception as e:
        raise Exception(f"[F99 → DW] Ghi dữ liệu vào Data Warehouse thất bại: {e}")

with DAG(
    dag_id='NetSuite_22to24_Daily_ToWarehouse_F98',
    start_date=pendulum.datetime(2024, 11, 27, tz="Asia/Bangkok"),
    schedule='0 5 * * *',
    catchup=False,
    max_active_runs=1
) as dag:

    task_download_and_upsert = PythonOperator(
        task_id='Download_And_Upsert_F98',
        python_callable=download_and_upsert_F99
    )

    task_upsert_to_warehouse = PythonOperator(
        task_id='Upsert_F98_To_DataWarehouse',
        python_callable=upsert_F99_to_warehouse
    )

    task_download_and_upsert >> task_upsert_to_warehouse
