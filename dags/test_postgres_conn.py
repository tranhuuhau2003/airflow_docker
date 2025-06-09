from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

def test_postgres_conn():
    try:
        hook = PostgresHook(postgres_conn_id="con_datalake")  # Đảm bảo trùng ID trong Airflow
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print("✅ Kết nối thành công. PostgreSQL version:", version)
    except Exception as e:
        print("❌ Kết nối thất bại:", str(e))
        raise

with DAG(
    dag_id="test_postgres_connection_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # chỉ chạy thủ công
    catchup=False,
    tags=["test", "postgres"],
) as dag:

    test_connection = PythonOperator(
        task_id="test_postgres_connection",
        python_callable=test_postgres_conn
    )
