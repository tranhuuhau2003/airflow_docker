FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
ENV AIRFLOW_HOME=/opt/airflow

# Cài đặt các thư viện hệ thống cần thiết cho Airflow và Chromium
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    wget \
    git \
    libpq-dev \
    chromium \
    chromium-driver \
    fonts-liberation \
    libappindicator3-1 \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdbus-1-3 \
    libgdk-pixbuf2.0-0 \
    libnspr4 \
    libnss3 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    xdg-utils \
  && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver

# Cài đặt Airflow 2.9.1 và các thư viện Python
RUN pip install --upgrade pip setuptools wheel \
 && pip install 'pendulum<3.0.0' \
 && pip install 'apache-airflow[postgres,celery]==2.9.1' \
 && pip install --no-cache-dir \
    pandas \
    openpyxl \
    beautifulsoup4 \
    lxml \
    selenium \
    webdriver-manager \
    psycopg2-binary \
    python-dotenv \
    joblib \
    tensorflow \
    scikit-learn \
    pyarrow 

# Tạo user airflow và gán quyền thư mục
RUN useradd -ms /bin/bash airflow \
 && mkdir -p ${AIRFLOW_HOME} && chown -R airflow:airflow ${AIRFLOW_HOME}

USER airflow
WORKDIR ${AIRFLOW_HOME}