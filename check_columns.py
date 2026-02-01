import akshare as ak
import pandas as pd

try:
    bond_spot_deal_df = ak.bond_spot_deal()
    print("Columns available in bond_spot_deal:")
    print(bond_spot_deal_df.columns.tolist())
    print("\nFirst 5 rows of data:")
    print(bond_spot_deal_df.head())
except Exception as e:
    print(f"Error: {e}")
