import requests
import sys
import os
import pandas as pd
from io import StringIO

# Thêm đường dẫn tới thư mục chứa LoginInformation.py
current_dir = os.path.dirname(__file__)
project_path = os.path.abspath(os.path.join(current_dir, '../plugins/Project'))
sys.path.append(project_path)

from LoginInformation import Netsuite_getLoginCookies, Netsuite_getLoginAccount

def download_saved_search_4532():
    # Đăng nhập NetSuite
    login_cookies = Netsuite_getLoginCookies()
    login_data = Netsuite_getLoginAccount()
    auth_url = 'https://7258775.app.netsuite.com/app/login/secure/enterpriselogin.nl'

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0"
    })

    login_response = session.post(auth_url, data=login_data, cookies=login_cookies)
    
    # Kiểm tra login thành công hay không (nếu trả về HTML thì fail)
    if login_response.status_code != 200 or "<html" in login_response.text.lower():
        print("❌ Đăng nhập NetSuite thất bại hoặc trả về HTML.")
        print("↪️ Preview:", login_response.text[:500])
        raise Exception("Login failed")

    # Tải dữ liệu từ Saved Search
    searchid = '4532'
    download_url = (
        f"https://7258775.app.netsuite.com/app/common/search/searchresults.csv?"
        f"searchid={searchid}&frame=be&csv=Export&report=&grid=T"
        f"&Transaction_TRANDATEfrom=01%2F01%2F2024"
    )

    response = session.get(download_url, verify=True, timeout=60)
    csv_text = response.text.strip()

    # Kiểm tra phản hồi có hợp lệ không
    if response.status_code != 200 or not csv_text or "<html" in csv_text.lower():
        print("❌ Tải file thất bại hoặc không phải CSV.")
        print("↪️ Preview:", csv_text[:500])
        raise Exception("Download failed or returned HTML")

    # Parse CSV bằng pandas
    try:
        df = pd.read_csv(StringIO(csv_text))
    except Exception as e:
        print("❌ Lỗi khi parse CSV")
        print("↪️ Preview:", csv_text[:500])
        raise e

    print(f"✅ Tải và parse thành công {len(df)} dòng.")
    print(df.head())  # In thử vài dòng

    return df

# Gọi thử nếu chạy trực tiếp
if __name__ == "__main__":
    download_saved_search_4532()
