import akshare as ak
import pandas as pd
import os

def fetch_and_save_bond_data():
    """
    获取中国债券市场行情数据（现券市场成交行情）并保存到 Excel。
    """
    print("正在从 akshare 获取现券市场成交行情数据...")
    try:
        # 获取现券市场成交行情数据
        bond_spot_deal_df = ak.bond_spot_deal()
        
        if bond_spot_deal_df is None or bond_spot_deal_df.empty:
            print("未能获取到数据，请检查网络连接或接口状态。")
            return

        print(f"成功获取到 {len(bond_spot_deal_df)} 条数据。")
        
        # 定义保存路径
        file_name = "bond_spot_deal.xlsx"
        file_path = os.path.join(os.getcwd(), file_name)
        
        # 保存到 Excel
        print(f"正在保存数据到 {file_name}...")
        bond_spot_deal_df.to_excel(file_path, index=False, engine='openpyxl')
        
        print(f"数据已成功保存至: {file_path}")
        
    except Exception as e:
        print(f"发生错误: {e}")

if __name__ == "__main__":
    fetch_and_save_bond_data()
