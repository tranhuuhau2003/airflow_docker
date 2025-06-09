import requests
import sys
import os

# Thêm đường dẫn tuyệt đối tới thư mục chứa LoginInformation.py
current_dir = os.path.dirname(__file__)  # airflow-docker/dags
project_path = os.path.abspath(os.path.join(current_dir, '../plugins/Project'))
sys.path.append(project_path)

from LoginInformation import Netsuite_getLoginCookies, Netsuite_getLoginAccount

def download_saved_search_4532():
    # Thư mục lưu file
    save_to_location = 'D:\\TranHuuHau\\airflow-docker\\db\\results\\tara'
    os.makedirs(save_to_location, exist_ok=True)

    # Tên file lưu
    file_name = 'CustomSavedSearch_4532'

    # Đăng nhập NetSuite
    login_cookies = Netsuite_getLoginCookies()
    login_data = Netsuite_getLoginAccount()
    auth_url = 'https://7258775.app.netsuite.com/app/login/secure/enterpriselogin.nl'

    session = requests.Session()
    login_response = session.post(auth_url, data=login_data, cookies=login_cookies)
    if login_response.status_code != 200:
        raise Exception("Đăng nhập NetSuite thất bại!")

    # Tạo URL từ searchid
    searchid = '4532'
    download_url = f"https://7258775.app.netsuite.com/app/common/search/searchresults.csv?searchid={searchid}&frame=be&csv=Export&report="

    # Tải file CSV và lưu
    response = session.get(download_url, verify=True, allow_redirects=True, timeout=60, stream=True)
    if response.status_code == 200 and response.text.strip():
        file_path = os.path.join(save_to_location, f"{file_name}.csv")
        with open(file_path, 'w', encoding="utf-8") as f:
            f.write(response.text)
        print(f"✅ Tải file thành công: {file_path}")
    else:
        print("❌ Tải file thất bại hoặc file rỗng.")

# Chạy hàm
if __name__ == "__main__":
    download_saved_search_4532()
