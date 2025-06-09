#!/usr/bin/env python
# coding: utf-8

def BackUpFile (path_current):
    import os,glob,shutil
    import pandas as pd
    from datetime import datetime
    
    # Create backup directory:
    path_backup = path_current.replace('Library_External','Library_External_backup')
    try:
        os.makedirs(path_backup)
    except:
        pass
    # Check and backup
    today_text = datetime.today().strftime('%Y%m%d')
    list_file_backup=glob.glob(path_backup+'/*.xlsx')
    list_file_current=glob.glob(path_current+'/*.xlsx')
    list_file_backup.sort()
    list_file_current.sort()
    for file in list_file_current:
        file_name=os.path.split(file)[1].split(".")[0]
        if len(glob.glob(path_backup+'/'+file_name+'*.xlsx'))==0:
            # Rename the backup file
            new_file_name = file_name.split('.')[0]+'-ver'+today_text+'.xlsx'
            # Copy to backup directory
            shutil.copyfile(file, path_backup+'/'+new_file_name)
        else:
            dict_file = pd.read_excel(file,sheet_name=None)
            dict_file_backup_latest_ver = pd.read_excel(sorted(glob.glob(path_backup+'/'+file_name+'*.xlsx'))[-1],sheet_name=None)
            for sheet_name, df_file in dict_file.items():
                if sheet_name in dict_file_backup_latest_ver:
                    if not df_file.equals(dict_file_backup_latest_ver[sheet_name]):
                        # Rename the backup file
                        new_file_name = file_name.split('.')[0]+'-ver'+today_text+'.xlsx'
                        # Copy to backup directory
                        shutil.copyfile(file, path_backup+'/'+new_file_name)
                else:
                    # Rename the backup file
                    new_file_name = file_name.split('.')[0]+'-ver'+today_text+'.xlsx'
                    # Copy to backup directory
                    shutil.copyfile(file, path_backup+'/'+new_file_name)
    file_list = glob.glob(path_backup+'/*app-srv*.xlsx',recursive=True)
    for file in file_list:
        os.remove(file)

def my_account():
    return {'hostname':'203.29.16.145','username':'adminde','password':'PleaseTara20241231','port':'5432'}

def con_datalake():
    return 'postgresql+psycopg2://postgres:123456@172.16.0.55:5432/datalake'
def con_datawarehouse():
    return 'postgresql+psycopg2://postgres:123456@172.16.0.55:5432/datawarehouse'

def write_log(new_log):
    file = open("Project/logs_running.txt", "a")
    file.write("\n"+new_log)
    file.close()

def convert_epoch2datetime(epoch,unit='s'):
    '''
    Function to convert from epochtime to datetime with:
    epoch: value in epochtime
    unit: ['s','ms'] with 's' if epochtime in second, 'ms' if epochtime in milisecond
    '''
    import pandas as pd
    return pd.to_datetime(epoch,unit=unit) + pd.Timedelta('07:00:00')

def convert_tf2yn(value):
    if value == '1' or value == 1 or value == 'True':
        return "Yes"
    elif value == '0' or value == 0 or value == 'False':
        return "No"
    else:
        return ""

def convert_float(x):
    import pandas as pd
    temp = str(x).strip(" -")
    if pd.isnull(x)==True or temp=="":
        return 0
    else:
        return float(x)

def email_sending (log):
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    log.split()
    sender_address = 'abc' #'ac@tarajsc.onmicrosoft.com'
    sender_pass = 'qcnmdqgfqmxpzdpq'
    receiver_address = 'nguyennhc@tara.com.vn'
    message = MIMEMultipart()
    message['From'] = sender_address
    message['To'] = receiver_address
    message['Subject'] = 'Automated reporting - '+log.split(",")[2]+' - '+log.split(",")[1]
    mail_content=   'Running Datetime: '+log.split(",")[0]+'\n'+ \
                    'Duration: '+log.split(",")[3]
    #The body and the attachments for the mail
    message.attach(MIMEText(mail_content, 'plain'))
    #Create SMTP session for sending the mail
    session = smtplib.SMTP('smtp.office365.com', 587) #use gmail with port
    session.starttls() #enable security
    session.login(sender_address, sender_pass) #login with mail_id and password
    text = message.as_string()
    session.sendmail(sender_address, receiver_address, text)
    session.quit()

def Upload2Datalake (file_name,upload_type='replace'):
    import os,glob,time,psycopg2,sqlalchemy,sys,re,csv
    sys.path.append('Project')
    import pandas as pd
    from TaraFunction import my_account,email_sending,con_datalake
    from datetime import datetime
    from sqlalchemy import create_engine
    
    start_time = time.time()
    path=r'Downloads'
    list_file=glob.glob(path+'/*.csv')
    skiprow=0
    link_file=[file for file in list_file if file_name in file][0]
    with open(link_file, newline='',encoding='utf8') as f:
        csv_reader = csv.reader(f)
        pattern = re.compile(r'\?*\,\?*')
        for i, row in enumerate(csv_reader):
            if pattern.search(str(row)):
                skiprow=i-1
                break
    df = pd.read_csv(link_file,dtype=str,skiprows=skiprow)
    file_name=file_name.replace("BI","_")
    # Upload to Datalake
    try:
        alchemyEngine = create_engine(con_datalake())
        conn = alchemyEngine.connect();
        df.to_sql(file_name, con=conn, if_exists=upload_type, index=False, method='multi', schema = 'NetSuite')
        conn.close()
        new_log = str(datetime.now())+',NetSuite.'+file_name+' to DataLake.'+file_name+',Succeed,'+str(time.time()-start_time)
    except:
        new_log = str(datetime.now())+',NetSuite.'+file_name+' to DataLake.'+file_name+',Failed,'+str(time.time()-start_time)
    
    # Write log
    file = open("Project/logs_running.txt", "a")
    file.write("\n"+new_log)
    file.close()

    # Sending email
    if 'Failed' in new_log:
        email_sending(new_log)

def DatawarehouseExecuteQuery(query):
    from sqlalchemy import create_engine
    import psycopg2,sqlalchemy
    alchemyEngine = create_engine(con_datawarehouse())
    conn = alchemyEngine.raw_connection()
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()
    
def DatalakeExecuteQuery(query):
    from sqlalchemy import create_engine
    import psycopg2,sqlalchemy
    alchemyEngine = create_engine(con_datalake())
    conn = alchemyEngine.raw_connection()
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()

def DatalakeQuery(query: str):
    '''
    Truy vấn PostgreSQL Datalake và trả về kết quả dạng DataFrame.
    '''
    import psycopg2
    import pandas as pd

    try:
        conn = psycopg2.connect(
            dbname='datalake',
            user='postgres',
            password='123456',
            host='172.16.0.55',
            port='5432'
        )
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df

    except Exception as e:
        raise Exception(f"[DatalakeQuery] Truy vấn thất bại: {e}")


def convertTV(text):
    """
    Convert Tieng Viet
    """
    import re
    import sys
    patterns = {
    # unicode composition
    '[àáảãạăắằẵặẳâầấậẫẩ]': 'a',
    '[đ]': 'd',
    '[èéẻẽẹêềếểễệ]': 'e',
    '[ìíỉĩị]': 'i',
    '[òóỏõọôồốổỗộơờớởỡợ]': 'o',
    '[ùúủũụưừứửữự]': 'u',
    '[ỳýỷỹỵ]': 'y',
    # unicode decomposition
    'á':'a','à':'a','ả':'a','ã':'a','ạ':'a','ầ':'a','ấ':'a','ẩ':'a','ẫ':'a','ậ':'a','ắ':'a','ằ':'a','ẳ':'a','ẵ':'a','ặ':'a',
    'è':'e','é':'e','ẻ':'e','ẽ':'e','ẹ':'e','ế':'e','ề':'e','ể':'e','ễ':'e','ệ':'e',
    'í':'i','ì':'i','ỉ':'i','ĩ':'i','ị':'i',
    'ó':'o','ò':'o','ỏ':'o','õ':'o','ọ':'o','ố':'o','ồ':'o','ổ':'o','ỗ':'o','ộ':'o','ớ':'o','ờ':'o','ở':'o','ỡ':'o','ợ':'o',
    'ý':'y','ỳ':'y','ỷ':'y','ỹ':'y','ỵ':'y',
    'ú':'u','ù':'u','ủ':'u','ũ':'u','ụ':'u','ứ':'u','ừ':'u','ử':'u','ữ':'u','ự':'u'
    }
    output = text
    for regex, replace in patterns.items():
        output = re.sub(regex, replace, output)
        # deal with upper case
        output = re.sub(regex.upper(), replace.upper(), output)
    return output

# def upsert_data2datalake(df,tablename,primary_key):
#     '''
#     function to update and insert data to postgresql with:
#     df: dataframe which is needed to insert, update to postgresql
#     tablename: table name of destiny sql table. For example: '"BaseWorkflow"."Jobs"'
#     primary_key: primary key name from table
#     *Column name from both data must be the same
#     '''
#     from sqlalchemy import create_engine
#     import psycopg2,sqlalchemy
#     import pandas as pd
#     # Delete row
#     try:
#         primary_text = ','.join(f"'{str(i)}'" for i in df[primary_key].unique().tolist())
#         sql_query = f'DELETE FROM {tablename} WHERE "{primary_key}" in ({primary_text});'
#         DatalakeExecuteQuery(query=sql_query)
#     except:
#         pass
#     # Append data
#     alchemyEngine = create_engine(con_datalake())
#     conn = alchemyEngine.connect()
#     df.to_sql(tablename.replace('"','').split('.')[1],
#                 con=conn,
#                 if_exists='append',
#                 index=False,
#                 schema=tablename.replace('"','').split('.')[0])
#     conn.close()

def upsert_data2datalake(df, tablename, primary_key):
    '''
    Ghi dữ liệu bằng psycopg2 thuần (không dùng SQLAlchemy)
    '''
    import psycopg2
    import pandas as pd

    schema = tablename.replace('"', '').split('.')[0]
    table = tablename.replace('"', '').split('.')[1]
    full_table = f'"{schema}"."{table}"'

    # Xóa dữ liệu cũ
    try:
        primary_vals = ','.join(f"'{str(i)}'" for i in df[primary_key].unique())
        delete_sql = f'DELETE FROM {full_table} WHERE "{primary_key}" IN ({primary_vals});'
        DatalakeExecuteQuery(delete_sql)
    except Exception as e:
        print(f"[Datalake] Lỗi xóa dữ liệu cũ: {e}")

    # Ghi dữ liệu mới
    try:
        conn = psycopg2.connect(
            dbname='datalake',
            user='postgres',
            password='123456',
            host='172.16.0.55',
            port='5432'
        )
        cur = conn.cursor()

        # Tạo câu lệnh INSERT
        cols = ','.join(f'"{col}"' for col in df.columns)
        values = [tuple(x) for x in df.to_numpy()]
        args_str = ','.join(cur.mogrify(f"({','.join(['%s'] * len(df.columns))})", row).decode("utf-8") for row in values)
        insert_sql = f"INSERT INTO {full_table} ({cols}) VALUES {args_str};"

        cur.execute(insert_sql)
        conn.commit()
        cur.close()
        conn.close()

        print(f"[Datalake] Ghi thành công {len(df)} dòng vào {tablename}")
    except Exception as e:
        raise Exception(f"[Datalake] Ghi dữ liệu thất bại: {e}")

def upsert_data2datawarehouse(df, tablename, primary_key):
    '''
    Ghi dữ liệu vào PostgreSQL (Datawarehouse) bằng psycopg2 thuần.
    df: DataFrame cần ghi.
    tablename: Tên bảng định dạng '"schema"."table"'.
    primary_key: Tên cột khóa chính.
    '''
    import psycopg2
    import pandas as pd

    schema = tablename.replace('"', '').split('.')[0]
    table = tablename.replace('"', '').split('.')[1]
    full_table = f'"{schema}"."{table}"'

    # Xóa dữ liệu cũ
    try:
        primary_vals = ','.join(f"'{str(i)}'" for i in df[primary_key].unique())
        delete_sql = f'DELETE FROM {full_table} WHERE "{primary_key}" IN ({primary_vals});'
        DatawarehouseExecuteQuery(delete_sql)
    except Exception as e:
        print(f"[Datawarehouse] Lỗi xóa dữ liệu cũ: {e}")

    # Ghi dữ liệu mới
    try:
        conn = psycopg2.connect(
            dbname='datawarehouse',  # hoặc thay bằng tên DB chính xác
            user='postgres',
            password='123456',       # thay bằng mật khẩu thật nếu cần
            host='172.16.0.55',      # hoặc thay host nếu khác
            port='5432'
        )
        cur = conn.cursor()

        # Tạo câu lệnh INSERT
        cols = ','.join(f'"{col}"' for col in df.columns)
        values = [tuple(x) for x in df.to_numpy()]
        args_str = ','.join(
            cur.mogrify(f"({','.join(['%s'] * len(df.columns))})", row).decode("utf-8")
            for row in values
        )
        insert_sql = f"INSERT INTO {full_table} ({cols}) VALUES {args_str};"

        cur.execute(insert_sql)
        conn.commit()
        cur.close()
        conn.close()

        print(f"[Datawarehouse] Ghi thành công {len(df)} dòng vào {tablename}")
    except Exception as e:
        raise Exception(f"[Datawarehouse] Ghi dữ liệu thất bại: {e}")



# def upsert_data2datawarehouse(df,tablename,primary_key):
#     '''
#     function to update and insert data to postgresql with:
#     df: dataframe which is needed to insert, update to postgresql
#     tablename: table name of destiny sql table. For example: '"BaseWorkflow"."Jobs"'
#     primary_key: primary key name from table
#     *Column name from both data must be the same
#     '''
#     from sqlalchemy import create_engine
#     import psycopg2,sqlalchemy
#     import pandas as pd
#     # Delete row
#     try:
#         primary_text = ','.join(f"'{str(i)}'" for i in df[primary_key].unique().tolist())
#         sql_query = f'DELETE FROM {tablename} WHERE "{primary_key}" in ({primary_text});'
#         DatawarehouseExecuteQuery(query=sql_query)
#     except:
#         pass
#     # Append data
#     alchemyEngine = create_engine(con_datawarehouse())
#     conn = alchemyEngine.connect()
#     df.to_sql(tablename.replace('"','').split('.')[1],
#                 con=conn,
#                 if_exists='append',
#                 index=False,
#                 schema=tablename.replace('"','').split('.')[0])
#     conn.close()

def day_to_second(datetime_value):
    '''
    Get total second from time in datetime
    datetime_value: value in datetime
    '''
    return datetime_value.hour*3600+datetime_value.minute*60+datetime_value.second
def duration_on_busy_day(start_time,end_time):
    '''
    Get duration between 2 datetime, not including weekend, holiday
    start_time: column name of start time of dataframe
    end_time: column name of end time of dataframe
    '''
    import numpy as np
    import pandas as pd
    from datetime import date, datetime
    list_holidays = ['2023-01-01','2023-01-02','2023-01-15','2023-01-16','2023-01-17','2023-01-18',
                     '2023-01-19','2023-01-20','2023-01-21','2023-01-22','2023-01-23','2023-01-24',
                     '2023-01-25','2023-01-26','2023-01-27','2023-01-28','2023-01-29','2023-04-29',
                     '2023-04-30','2023-05-01','2023-05-02','2023-05-03','2023-09-01','2023-09-02',
                     '2023-09-03','2023-09-04']
    if pd.isna(start_time) or pd.isna(end_time):
        return np.nan
    else:
        number_workday=np.busday_count(start_time.date(),end_time.date(),holidays=list_holidays)
        if start_time.weekday()==5 or start_time.weekday()==6 or start_time.strftime("%Y-%m-%d") in list_holidays:
            if end_time.weekday()==5 or end_time.weekday()==6 or end_time.strftime("%Y-%m-%d") in list_holidays:
                return number_workday*86400
            else:
                return number_workday*86400+day_to_second(end_time)
        else:
            if end_time.weekday()==5 or end_time.weekday()==6 or end_time.strftime("%Y-%m-%d") in list_holidays:
                return number_workday*86400-day_to_second(start_time)
            else:
                if number_workday>=1:
                    return number_workday*86400+day_to_second(end_time)-day_to_second(start_time)
                else:
                    return (end_time-start_time).total_seconds()

def age_generation(value):
    '''
    Get the Generation of person from their date of birth value in format date/datetime
    '''
    import pandas as pd
    import numpy as np
    if pd.isnull(value) or value.year<1883:
        return np.nan
    elif value.year<=1900:
        return "Lost Generation"
    elif value.year<=1927:
        return "G.I. Generation"
    elif value.year<=1945:
        return "Silent Generation"
    elif value.year<=1964:
        return "Baby Boomers"
    elif value.year<=1980:
        return "Generation X"
    elif value.year<=1996:
        return "Generation Y"
    elif value.year<=2012:
        return "Generation Z"
    else:
        return "Generation Alpha"

def intersection(list1, list2):
    '''
    Get intersection value of 2 list
    '''
    list3 = [value for value in list1 if value in list2]
    return list3

def difference(list1, list2):
    '''
    Get difference value of 2 list
    '''
    list3 = [value for value in list1 + list2 if value not in list1 or value not in list2]
    return list3

def write_log_dmx(new_log):
    file = open("D:/Tara/Python/PY files/dmx.txt", "a")
    file.write("\n"+new_log)
    file.close()

def add_new_column_sql(sql_table_name,df):
    '''
    Check if the dataframe "df" have new columns compare to sql_table_name.
    Ex:
    sql_table_name = '"BaseRequest"."Requests"'
    '''
    sql_query = f'''
    SELECT * FROM {sql_table_name} LIMIT 0;
    '''
    df_sql = pd.read_sql(sql_query,con=con_datalake())
    list_new_column = [col for col in df.columns.to_list() if col not in df_sql.columns.to_list()]
    if len(list_new_column)>0:
        for col in list_new_column:
            sql_query = f'''
            ALTER TABLE {sql_table_name} ADD COLUMN {col} TEXT;
            '''
            DatalakeExecuteQuery(query=sql_query)


def check_data_format(table_name,df_check,except_col=['']):
    '''
    Function to check if all data in "df_check" could match format in "table_name"
    table_name: Table name in Data Warehouse. Example: "ExternalHRD"."Training_Data"
    df_check: dataframe need to be checked
    except_col: list of column don't need to check
    '''
    import pandas as pd
    # Load Dataframe from Data Warehouse
    sql_query=f'SELECT * FROM {table_name} LIMIT 1'
    df_wh = pd.read_sql(sql_query,con=con_datawarehouse())
    series_wh = df_wh.dtypes[df_wh.dtypes != 'object']
    
    # Check format:
    list_error = []
    for i in range(len(series_wh)):
        if series_wh.index[i] not in except_col:
            try:
                df_check[series_wh.index[i]] =  df_check[series_wh.index[i]].astype(series_wh[i])
            except:
                list_error.append(series_wh.index[i])
    return ', '.join(list_error)

def check_column_name(table_name,df_check):
    '''
    Function to check if all column name in "df_check" could match all column name in "table_name"
    table_name: Table name in Data Warehouse
    df_check: dataframe need to be checked
    '''
    import pandas as pd
    
    # Load Dataframe from Data Warehouse
    sql_query=f'SELECT * FROM {table_name} LIMIT 1'
    df_wh = pd.read_sql(sql_query,con=con_datawarehouse())
    list_error = intersection(df_wh.columns.to_list(),df_check.columns.to_list())
    list_error = difference(list_error,df_check.columns.to_list())
    return ', '.join(list_error)

def email_sending_external_file (file_name,
                                 sheet_name,
                                 list_receiver_address=['nguyennhc@tara.com.vn'],
                                 result=True,
                                 error_format='',
                                 error_col_name=''):
    '''
    file_name: file name of external file
    list_receiver_address: list of receiver address
    '''
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    sender_address = 'abc' #'ac@tarajsc.onmicrosoft.com'
    sender_pass = 'qcnmdqgfqmxpzdpq'
    receiver_address = ', '.join(list_receiver_address)
    message = MIMEMultipart()
    message['From'] = sender_address
    message['To'] = receiver_address
    if result==True:
        message['Subject'] = f'''Automated mail - Successed - file {file_name} - sheet {sheet_name}'''
        mail_content = f'''
        Dear,

        We are pleased to inform you that your file, "{file_name}",
        specifically the sheet "{sheet_name}", has been successfully uploaded to our Data Warehouse.

        This is an automated message. Please do not reply to this email.
        
        Best regards,
        BIA
        '''
    else:
        message['Subject'] = f'''Automated mail - Failed - file {file_name} - sheet {sheet_name}'''
        mail_content = f'''
        Dear,

        We regret to inform you that your file, "{file_name}",
        specifically the sheet "{sheet_name}", can't be uploaded to our Data Warehouse.
        
        Please review your file for the following issues:
        - Columns containing errors in format: {error_format}
        - Columns added without prior consent: {error_col_name}
        
        This is an automated message. Please do not reply to this email.
        
        Best regards,
        BIA
        '''
    #The body and the attachments for the mail
    message.attach(MIMEText(mail_content, 'plain'))
    #Create SMTP session for sending the mail
    session = smtplib.SMTP('smtp.office365.com', 587) #use gmail with port
    session.starttls() #enable security
    session.login(sender_address, sender_pass) #login with mail_id and password
    text = message.as_string()
    session.sendmail(sender_address, receiver_address, text)
    session.quit()




