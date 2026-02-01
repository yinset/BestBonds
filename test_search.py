import requests

def search_bond(bond_name):
    url = "https://www.chinamoney.com.cn/ags/ms/cm-u-bond-md/BondBasicInfo"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
        "Referer": "https://www.chinamoney.com.cn/chinese/zqjc/"
    }
    # 根据中国货币网接口，搜索通常是 POST 或者带参数的 GET
    # 这里我们尝试搜索接口
    payload = {
        "bondName": bond_name,
        "pageIndex": 1,
        "pageSize": 15
    }
    r = requests.post(url, data=payload, headers=headers)
    print(f"Status: {r.status_code}")
    try:
        print(r.json())
    except:
        print(r.text[:200])

if __name__ == "__main__":
    search_bond("25国开15")
