import akshare as ak
import pandas as pd

try:
    # 获取一个具体债券的详情
    detail_df = ak.bond_info_detail_cm(symbol="25国开15")
    print("Detail for 25国开15:")
    print(detail_df)
except Exception as e:
    print(f"Error: {e}")
