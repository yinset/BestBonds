import akshare as ak
import pandas as pd

print("--- Testing bond_china_close_return ---")
try:
    df = ak.bond_china_close_return(symbol="中债国债收益率曲线", period="1")
    print(df.head())
except Exception as e:
    print(f"Error bond_china_close_return: {e}")

print("\n--- Testing bond_info_cm with minimal filters ---")
try:
    # Try to get a broader list
    df_info = ak.bond_info_cm(bond_type="国债")
    print(f"Columns: {df_info.columns.tolist()}")
    print(df_info.head())
except Exception as e:
    print(f"Error bond_info_cm: {e}")
