from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

def drop_postgres_schema_and_table():
    try:
        hook = PostgresHook(postgres_conn_id="con_datalake")
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Drop schema with CASCADE to also drop all tables within it
        cursor.execute("DROP SCHEMA IF EXISTS my_schema CASCADE;")
        conn.commit()
        print("✅ Schema 'my_schema' và tất cả bảng trong đó đã được xóa thành công.")
    except Exception as e:
        print("❌ Lỗi khi xóa schema:", str(e))
        raise

with DAG(
    dag_id="drop_postgres_schema_and_table_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test", "postgres", "cleanup"],
) as dag:

    drop_schema_task = PythonOperator(
        task_id="drop_postgres_schema",
        python_callable=drop_postgres_schema_and_table
    )