import akshare as ak
import pandas as pd

def try_detail(symbol):
    print(f"\nTrying symbol: {symbol}")
    try:
        df = ak.bond_info_detail_cm(symbol=symbol)
        print(df)
    except Exception as e:
        print(f"Error for {symbol}: {e}")

# Get some symbols from bond_info_cm
try:
    info_df = ak.bond_info_cm(bond_type="国债")
    if not info_df.empty:
        # Try with 债券简称
        name = info_df.iloc[0]['债券简称']
        try_detail(name)
        
        # Try with 查询代码
        query_code = info_df.iloc[0]['查询代码']
        try_detail(query_code)
        
        # Try with 债券代码
        bond_code = info_df.iloc[0]['债券代码']
        try_detail(bond_code)
except Exception as e:
    print(f"Error getting symbols: {e}")
