import requests
import pandas as pd

url = "https://www.chinamoney.com.cn/ags/ms/cm-u-bond-md/BondDetailInfo"
payload = {"bondDefinedCode": "gcajg04m71"}
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Referer": "https://www.chinamoney.com.cn/chinese/zqjc/?bondDefinedCode=gcajg04m71"
}

print(f"Requesting {url} with {payload}...")
r = requests.post(url, data=payload, headers=headers)
print(f"Status Code: {r.status_code}")
print(f"Response snippet: {r.text[:500]}")

try:
    data_json = r.json()
    print("JSON success!")
    print(data_json['data']['bondBaseInfo'])
except Exception as e:
    print(f"JSON Error: {e}")
