from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
from psycopg2.extras import execute_values
import psycopg2

EXCEL_FILE_PATH = "/opt/airflow/excel/all_tara_backup.xlsx"  # Cập nhật đường dẫn thật

def create_schema_and_table():
    hook = PostgresHook(postgres_conn_id="con_datalake")
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("CREATE SCHEMA IF NOT EXISTS test_airflow;")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS test_airflow.test_data_tara (
            id SERIAL PRIMARY KEY,
            product_group TEXT,
            model TEXT,
            nav_code TEXT,
            dien_may_cho_lon TEXT,
            dien_may_xanh TEXT,
            nguyen_kim TEXT,
            price_dmcl NUMERIC,
            final_price_dmcl NUMERIC,
            price_dmx NUMERIC,
            final_price_dmx NUMERIC,
            price_nk NUMERIC,
            final_price_nk NUMERIC,
            link_dmcl TEXT,
            link_dmx TEXT,
            link_nk TEXT
        );
    """)
    conn.commit()
    print("Schema và bảng đã được tạo hoặc đã tồn tại.")

# def load_excel_to_postgres():
#     df = pd.read_excel(EXCEL_FILE_PATH)

#     if df.empty:
#         raise ValueError("Excel file is empty!")

#     df = df.rename(columns={
#         "NHÓM HÀNG": "product_group",
#         "MODEL": "model",
#         "NAV CODE": "nav_code",
#         "Điện Máy Chợ Lớn": "dien_may_cho_lon",
#         "Điện Máy Xanh": "dien_may_xanh",
#         "Nguyễn Kim": "nguyen_kim",
#         "Giá DMCL - 20/05/2025": "price_dmcl",
#         "Giá Cuối DMCL - 20/05/2025": "final_price_dmcl",
#         "Giá DMX - 20/05/2025": "price_dmx",
#         "Giá Cuối DMX - 20/05/2025": "final_price_dmx",
#         "Giá NK - 20/05/2025": "price_nk",
#         "Giá Cuối NK - 20/05/2025": "final_price_nk",
#         "LINK DMCL": "link_dmcl",
#         "LINK DMX": "link_dmx",
#         "LINK NK": "link_nk"
#     })

#     hook = PostgresHook(postgres_conn_id="datalake_airflow")
#     engine = hook.get_sqlalchemy_engine()

#     df.to_sql(
#         name="test_data_tara",
#         con=engine,
#         schema="test_airflow",
#         if_exists="append",
#         index=False
#     )

#     print(f"Đã chèn {len(df)} dòng dữ liệu vào test_airflow.test_data_tara.")


def load_excel_to_postgres():
    df = pd.read_excel(EXCEL_FILE_PATH)

    if df.empty:
        raise ValueError("Excel file is empty!")

    df = df.rename(columns={
        "NHÓM HÀNG": "product_group",
        "MODEL": "model",
        "NAV CODE": "nav_code",
        "Điện Máy Chợ Lớn": "dien_may_cho_lon",
        "Điện Máy Xanh": "dien_may_xanh",
        "Nguyễn Kim": "nguyen_kim",
        "Giá DMCL - 20/05/2025": "price_dmcl",
        "Giá Cuối DMCL - 20/05/2025": "final_price_dmcl",
        "Giá DMX - 20/05/2025": "price_dmx",
        "Giá Cuối DMX - 20/05/2025": "final_price_dmx",
        "Giá NK - 20/05/2025": "price_nk",
        "Giá Cuối NK - 20/05/2025": "final_price_nk",
        "LINK DMCL": "link_dmcl",
        "LINK DMX": "link_dmx",
        "LINK NK": "link_nk"
    })

    hook = PostgresHook(postgres_conn_id="con_datalake")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Lấy tên cột và dữ liệu tuple
    cols = list(df.columns)
    values = [tuple(x) for x in df.to_numpy()]

    # Tạo câu lệnh insert dùng execute_values
    insert_query = f"""
        INSERT INTO test_airflow.test_data_tara ({', '.join(cols)}) VALUES %s
    """

    execute_values(cursor, insert_query, values)
    conn.commit()
    cursor.close()
    conn.close()

    print(f"Đã chèn {len(df)} dòng dữ liệu vào test_airflow.test_data_tara.")


    
with DAG(
    dag_id="excel_to_postgres_test_data_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["excel", "postgres", "load"],
) as dag:

    create_table_task = PythonOperator(
        task_id="create_schema_and_table",
        python_callable=create_schema_and_table
    )

    load_data_task = PythonOperator(
        task_id="load_excel_data_to_postgres",
        python_callable=load_excel_to_postgres
    )

    create_table_task >> load_data_task
