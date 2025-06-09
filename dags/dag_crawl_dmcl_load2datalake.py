from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import pandas as pd
import os
import csv
import time
import tempfile
import shutil
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

# ---------------------------
# TASK 1: Crawl data
# ---------------------------
def crawl_dmcl():
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
        return df[['MODEL', 'CP', 'Unnamed: 9']]

    def initialize_error_file():
        error_file = './db/error/error_product_dmcl.csv'
        os.makedirs(os.path.dirname(error_file), exist_ok=True)
        with open(error_file, 'w', newline='', encoding='utf-8') as f:
            csv.writer(f).writerow(['MODEL', 'CP KD', 'LINK', 'ERROR'])
        return error_file

    def log_error_product(error_file, model, cp_kd, url, error_msg):
        with open(error_file, 'a', newline='', encoding='utf-8') as f:
            csv.writer(f).writerow([model, cp_kd, url, error_msg])

    def get_price_discount(driver, url, model):
        price, discount_value, method_used = None, None, "None"
        try:
            driver.get(url)
            time.sleep(2)
            xpaths = [
                "//*[@id='product_detail']/section/div[1]/div[1]/div/div[2]/div[2]/section/div[1]/div[2]/strong",
                "//*[@id='product_detail']/section/div[1]/div[1]/div/div[2]/div[2]/section/div[1]/div[2]/div/p/strong",
                "//*[@id='product_detail']/section/div[1]/div[1]/div/div[2]/div[2]/section/div[1]/div[2]/div[1]/span",
                "//*[@id='product_detail']/section/div[1]/div[1]/div/div[2]/div[2]/section/div[1]/div/strong",
            ]
            for i, xp in enumerate(xpaths):
                try:
                    price = driver.find_element(By.XPATH, xp).text.strip()
                    method_used = f"Method {i+1}"
                    break
                except NoSuchElementException:
                    continue
            try:
                discount_text = driver.find_element(By.XPATH, "//*[@id='product_detail']/section/div[1]/div[1]/div/div[2]/div[2]/section/div[2]/div/ul/li[1]/p").text.strip()
                if "Trị Giá" in discount_text:
                    discount_value = ''.join(filter(str.isdigit, discount_text))
                    method_used += " + Discount"
            except NoSuchElementException:
                pass
        except Exception:
            pass
        return price, discount_value, method_used

    error_file = initialize_error_file()
    try:
        df = get_raw_data()
    except Exception as e:
        print(f"Error reading input: {e}")
        return

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    user_data_dir = tempfile.mkdtemp()
    options.add_argument(f"--user-data-dir={user_data_dir}")
    driver = webdriver.Chrome(options=options)

    df['Giá'] = pd.Series(dtype='object')
    df['Giá Cuối DMCL'] = pd.Series(dtype='object')
    df['Phương Pháp'] = pd.Series(dtype='object')

    for index, row in df.iterrows():
        model, cp_kd, url = row['MODEL'], row['CP'], row['Unnamed: 9']
        if pd.notna(url) and url:
            try:
                price, discount, method = get_price_discount(driver, url, model)
                if price:
                    df.at[index, 'Giá'] = price
                    df.at[index, 'Phương Pháp'] = method
                    df.at[index, 'Giá Cuối DMCL'] = discount if discount else price
                else:
                    log_error_product(error_file, model, cp_kd, url, "No price found")
            except Exception as e:
                log_error_product(error_file, model, cp_kd, url, f"Exception: {e}")
        else:
            log_error_product(error_file, model, cp_kd, "N/A", "No URL")

    driver.quit()
    shutil.rmtree(user_data_dir, ignore_errors=True)

    os.makedirs("./db/results/tara", exist_ok=True)
    df.to_excel("./db/results/tara/dmcl_tara.xlsx", index=False, engine="openpyxl", sheet_name="follow_price_dmcl")

# ---------------------------
# TASK 2: Upload to DataLake
# ---------------------------
def from_crawl_dmcl_2datalake():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    def get_raw_data():
        file_path = "./db/raw/follow_web_price.xlsx"
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File {file_path} không tồn tại")
        df = pd.read_excel(file_path, sheet_name="follow_price", header=2)
        df = df.drop(df.index[0]).reset_index(drop=True)
        return df[['MODEL', 'CP', 'NAV CODE', 'NHÓM HÀNG']]

    def get_result_data():
        file_path = "./db/results/tara/dmcl_tara.xlsx"
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File {file_path} không tồn tại")
        df = pd.read_excel(file_path, sheet_name="follow_price_dmcl")
        return df[['MODEL', 'CP', 'Unnamed: 9', 'Giá', 'Giá Cuối DMCL', 'Phương Pháp']]

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
        with hook.get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(create_table_sql)
            conn.commit()

    def insert_data(hook, df):
        with hook.get_conn() as conn:
            cursor = conn.cursor()
            for _, row in df.iterrows():
                cursor.execute("""
                INSERT INTO test_airflow.product_price
                (model, cp, url, price, final_price, method, nav_code, category, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                """, (
                    row['MODEL'], row['CP'], row['Unnamed: 9'], row['Giá'],
                    row['Giá Cuối DMCL'], row['Phương Pháp'],
                    row['NAV CODE'], row['NHÓM HÀNG']
                ))
            conn.commit()

    try:
        df_raw = get_raw_data()
        df_result = get_result_data()
        df_merged = df_result.merge(df_raw, on=['MODEL', 'CP'], how='left')

        hook = PostgresHook(postgres_conn_id="con_datalake")
        create_table(hook)
        insert_data(hook, df_merged)
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")
        raise

# ---------------------------
# DAG Definition
# ---------------------------
with DAG(
    dag_id="crawl_dcml_and_upload_2datalake_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["dmcl", "tara", "datalake", "web_crawl"],
) as dag:

    task_crawl = PythonOperator(
        task_id="crawl_dmcl",
        python_callable=crawl_dmcl,
    )

    task_upload = PythonOperator(
        task_id="from_crawl_dmcl_2datalake",
        python_callable=from_crawl_dmcl_2datalake,
    )

    task_crawl >> task_upload  # Thiết lập thứ tự
