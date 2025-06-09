# filepath: dags/dag_crawl_dmcl.py

from airflow import DAG
from airflow.operators.python import PythonOperator
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

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

def crawl_dmcl_tara():
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
            writer = csv.writer(f)
            writer.writerow(['MODEL', 'CP KD', 'LINK', 'ERROR'])
        return error_file

    def log_error_product(error_file, model, cp_kd, url, error_msg):
        with open(error_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([model, cp_kd, url, error_msg])

    def get_price_discount(driver, url, model):
        price = None
        discount_value = None
        method_used = "None"
        try:
            driver.get(url)
            time.sleep(2)
            try:
                price = driver.find_element(By.XPATH, "//*[@id='product_detail']/section/div[1]/div[1]/div/div[2]/div[2]/section/div[1]/div[2]/strong").text.strip()
                method_used = "Method 1"
            except NoSuchElementException:
                try:
                    price = driver.find_element(By.XPATH, "//*[@id='product_detail']/section/div[1]/div[1]/div/div[2]/div[2]/section/div[1]/div[2]/div/p/strong").text.strip()
                    method_used = "Method 2"
                except NoSuchElementException:
                    try:
                        price = driver.find_element(By.XPATH, "//*[@id='product_detail']/section/div[1]/div[1]/div/div[2]/div[2]/section/div[1]/div[2]/div[1]/span").text.strip()
                        method_used = "Method 3"
                    except NoSuchElementException:
                        try:
                            price = driver.find_element(By.XPATH, "//*[@id='product_detail']/section/div[1]/div[1]/div/div[2]/div[2]/section/div[1]/div/strong").text.strip()
                            method_used = "Method 4"
                        except NoSuchElementException:
                            pass
            try:
                discount_text = driver.find_element(By.XPATH, "//*[@id='product_detail']/section/div[1]/div[1]/div/div[2]/div[2]/section/div[2]/div/ul/li[1]/p").text.strip()
                if "Trị Giá" in discount_text:
                    discount_value = ''.join(filter(str.isdigit, discount_text))
                    method_used += " + Discount"
            except NoSuchElementException:
                pass
        except TimeoutException:
            pass
        except Exception:
            pass
        return price, discount_value, method_used

    error_file = initialize_error_file()
    try:
        df_dmcl = get_raw_data()
    except Exception as e:
        print(f"Error reading input: {e}")
        return

    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--start-maximized")

    user_data_dir = tempfile.mkdtemp()
    chrome_options.add_argument(f"--user-data-dir={user_data_dir}")

    driver = webdriver.Chrome(options=chrome_options)

    df_dmcl['Giá'] = pd.Series(dtype='object')
    df_dmcl['Giá Cuối DMCL'] = pd.Series(dtype='object')
    df_dmcl['Phương Pháp'] = pd.Series(dtype='object')

    for index, row in df_dmcl.iterrows():
        model = row['MODEL']
        cp_kd = row['CP']
        url = row['Unnamed: 9']
        if pd.notna(url) and url:
            try:
                price, discount_value, method_used = get_price_discount(driver, url, model)
                if price:
                    df_dmcl.at[index, 'Giá'] = price
                    df_dmcl.at[index, 'Phương Pháp'] = method_used
                    if discount_value:
                        df_dmcl.at[index, 'Giá Cuối DMCL'] = discount_value
                    else:
                        df_dmcl.at[index, 'Giá Cuối DMCL'] = price
                else:
                    log_error_product(error_file, model, cp_kd, url, "No price found")
            except Exception as e:
                log_error_product(error_file, model, cp_kd, url, f"Exception: {e}")
        else:
            log_error_product(error_file, model, cp_kd, "N/A", "No URL")

    driver.quit()
    shutil.rmtree(user_data_dir, ignore_errors=True)


    os.makedirs("./db/results/tara", exist_ok=True)
    df_dmcl.to_excel("./db/results/tara/dmcl_tara.xlsx", index=False, engine="openpyxl", sheet_name="follow_price_dmcl")

with DAG(
    dag_id="crawl_dmcl_tara_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["web_crawl", "dmcl", "tara"],
) as dag:

    crawl_task = PythonOperator(
        task_id="crawl_dmcl_prices",
        python_callable=crawl_dmcl_tara
    )

    crawl_task
