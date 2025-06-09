from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

def test_postgres_conn_and_create_table():
    try:
        hook = PostgresHook(postgres_conn_id="con_datalake")
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("CREATE SCHEMA IF NOT EXISTS my_schema;")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS my_schema.test_airflow_table (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
        print("✅ Tạo schema và bảng thành công hoặc đã tồn tại.")
    except Exception as e:
        print("❌ Lỗi khi kết nối hoặc tạo bảng:", str(e))
        raise

def read_data_from_postgres():
    try:
        hook = PostgresHook(postgres_conn_id="datalake_airflow")
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM my_schema.test_airflow_table;")
        rows = cursor.fetchall()

        print(f"✅ Đọc được {len(rows)} dòng dữ liệu:")
        for row in rows:
            print(row)
    except Exception as e:
        print("❌ Lỗi khi đọc dữ liệu:", str(e))
        raise

with DAG(
    dag_id="test_postgres_create_table_and_read_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test", "postgres"],
) as dag:

    create_table_task = PythonOperator(
        task_id="create_postgres_table",
        python_callable=test_postgres_conn_and_create_table
    )

    read_data_task = PythonOperator(
        task_id="read_postgres_data",
        python_callable=read_data_from_postgres
    )

    create_table_task >> read_data_task
