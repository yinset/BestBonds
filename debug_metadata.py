from batch_bond_analysis import get_bond_metadata_raw
import json
import requests

if __name__ == "__main__":
    symbol = "25国开15"
    print(f"Testing {symbol} with session...")
    
    session = requests.Session()
    # 先访问首页获取 cookie
    session.get("https://www.chinamoney.com.cn/chinese/zqjc/", timeout=10)
    
    res = get_bond_metadata_raw(symbol, session=session)
    print(f"Result: {json.dumps(res, indent=4, ensure_ascii=False)}")
