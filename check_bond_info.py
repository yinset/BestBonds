import akshare as ak
import pandas as pd

try:
    # 尝试获取一些债券信息
    bond_info_df = ak.bond_info_cm(bond_type="国债")
    print("Columns in bond_info_cm:")
    print(bond_info_df.columns.tolist())
    print(bond_info_df.head())
except Exception as e:
    print(f"Error: {e}")
