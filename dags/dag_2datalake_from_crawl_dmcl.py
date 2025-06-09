from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

def upload_to_datalake():
    # Thiết lập logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Hàm đọc dữ liệu từ tệp Excel gốc
    def get_raw_data():
        file_path = "./db/raw/follow_web_price.xlsx"
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File {file_path} không tồn tại")
        sheet_name = "follow_price"
        sheet_names = pd.ExcelFile(file_path).sheet_names
        if sheet_name not in sheet_names:
            raise ValueError(f"Sheet '{sheet_name}' không tồn tại. Các sheet có sẵn: {sheet_names}")
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=2)
        df = df.drop(df.index[0]).reset_index(drop=True)
        return df[['MODEL', 'CP', 'NAV CODE', 'NHÓM HÀNG']]

    # Hàm đọc dữ liệu từ tệp Excel kết quả
    def get_result_data():
        file_path = "./db/results/tara/dmcl_tara.xlsx"
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File {file_path} không tồn tại")
        df = pd.read_excel(file_path, sheet_name="follow_price_dmcl")
        return df[['MODEL', 'CP', 'Unnamed: 9', 'Giá', 'Giá Cuối DMCL', 'Phương Pháp']]

    # Hàm tạo bảng trong DataLake
    def create_table(hook):
        create_table_sql = """
        CREATE SCHEMA IF NOT EXISTS test_airflow;
        CREATE TABLE IF NOT EXISTS test_airflow.product_price (
            model VARCHAR(255),
            cp VARCHAR(100),
            url TEXT,
            price VARCHAR(100),
            final_price VARCHAR(100),
            method VARCHAR(100),
            nav_code VARCHAR(100),
            category VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        try:
            with hook.get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(create_table_sql)
                conn.commit()
                logger.info("Table product_price created or already exists in schema test_airflow.")
        except Exception as e:
            logger.error(f"Error creating table: {str(e)}")
            raise

    # Hàm chèn dữ liệu vào DataLake
    def insert_data(hook, df):
        try:
            with hook.get_conn() as conn:
                cursor = conn.cursor()
                for _, row in df.iterrows():
                    insert_sql = """
                    INSERT INTO test_airflow.product_price
                    (model, cp, url, price, final_price, method, nav_code, category, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    """
                    cursor.execute(insert_sql, (
                        row['MODEL'],
                        row['CP'],
                        row['Unnamed: 9'],
                        row['Giá'],
                        row['Giá Cuối DMCL'],
                        row['Phương Pháp'],
                        row['NAV CODE'],
                        row['NHÓM HÀNG']
                    ))
                conn.commit()
                logger.info(f"Inserted {len(df)} rows into test_airflow.product_price.")
        except Exception as e:
            logger.error(f"Error inserting data: {str(e)}")
            raise

    try:
        # Đọc dữ liệu từ hai tệp Excel
        df_raw = get_raw_data()
        df_result = get_result_data()

        # Kết hợp dữ liệu dựa trên MODEL và CP
        df_merged = df_result.merge(
            df_raw[['MODEL', 'CP', 'NAV CODE', 'NHÓM HÀNG']],
            on=['MODEL', 'CP'],
            how='left'
        )

        # Kết nối với DataLake bằng PostgresHook
        hook = PostgresHook(postgres_conn_id="con_datalake")

        # Tạo bảng nếu chưa tồn tại
        create_table(hook)

        # Chèn dữ liệu vào bảng
        insert_data(hook, df_merged)

    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")
        raise

with DAG(
    dag_id="upload_to_datalake_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["datalake", "dmcl", "tara"],
) as dag:

    upload_task = PythonOperator(
        task_id="upload_to_product_price",
        python_callable=upload_to_datalake
    )

    upload_task