import requests

url = "https://www.chinamoney.com.cn/ags/ms/cm-u-bond-md/BondMarketInfoList2"
payload = {
    "pageNo": "1",
    "pageSize": "15",
    "bondName": "25国开15",
}
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
    "Referer": "https://www.chinamoney.com.cn/chinese/zqjc/"
}
r = requests.post(url, data=payload, headers=headers)
print(f"Status: {r.status_code}")
print(f"Headers: {r.headers}")
print(f"Content: {r.text[:500]}")
