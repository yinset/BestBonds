import akshare as ak
import pandas as pd
import requests
from datetime import datetime
import numpy as np

def get_bond_metadata(symbol):
    """
    获取债券元数据（到期日、票息、付息频率）
    """
    try:
        # 1. 获取查询代码
        info_df = ak.bond_info_cm(bond_name=symbol)
        if info_df.empty:
            return None
        
        query_code = info_df.iloc[0]['查询代码']
        
        # 2. 调用详情接口
        url = "https://www.chinamoney.com.cn/ags/ms/cm-u-bond-md/BondDetailInfo"
        payload = {"bondDefinedCode": query_code}
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
            "Referer": f"https://www.chinamoney.com.cn/chinese/zqjc/?bondDefinedCode={query_code}"
        }
        
        r = requests.post(url, data=payload, headers=headers)
        if r.status_code != 200:
            return None
        
        data = r.json()['data']['bondBaseInfo']
        
        # 3. 解析关键字段
        metadata = {
            'symbol': symbol,
            'maturity_date': data.get('mrtyDate'),
            'coupon_rate': float(data.get('parCouponRate', 0)) / 100, # 转换为小数
            'frequency': data.get('couponFrqncy', '年'),
            'issue_date': data.get('issueDate'),
            'bond_type': data.get('bondType')
        }
        return metadata
    except Exception as e:
        print(f"Error fetching metadata for {symbol}: {e}")
        return None

def calculate_duration(yield_val, coupon_rate, maturity_date, frequency_str='年', settlement_date=None):
    """
    计算债券久期
    :param yield_val: 到期收益率 (例如 0.025 表示 2.5%)
    :param coupon_rate: 票面利率 (例如 0.03 表示 3%)
    :param maturity_date: 到期日 (YYYY-MM-DD)
    :param frequency_str: 付息频率 ('年', '半年', '季')
    :param settlement_date: 结算日/当前日期 (默认今天)
    """
    if settlement_date is None:
        settlement_date = datetime.now()
    else:
        settlement_date = datetime.strptime(settlement_date, '%Y-%m-%d')
        
    maturity_date = datetime.strptime(maturity_date, '%Y-%m-%d')
    
    # 剩余年限
    years_to_maturity = (maturity_date - settlement_date).days / 365.25
    if years_to_maturity <= 0:
        return 0, 0, 0 # 已到期
        
    # 付息频率
    freq_map = {'年': 1, '半年': 2, '季': 4}
    m = freq_map.get(frequency_str, 1)
    
    # 现金流计算
    # 简化模型：假设下次付息就在 1/m 年后，或者根据剩余期限计算
    # 这里使用简化模型计算离散现金流
    num_payments = int(np.ceil(years_to_maturity * m))
    times = np.array([(i + (years_to_maturity * m % 1 or 1) - (years_to_maturity * m % 1 == 0)) / m for i in range(num_payments)])
    # 修正 times，确保最后一个现金流在到期日
    times = np.linspace(years_to_maturity % (1/m) or (1/m), years_to_maturity, num_payments)
    
    cash_flows = np.array([coupon_rate / m * 100] * num_payments)
    cash_flows[-1] += 100 # 加上本金
    
    # 折现因子
    discount_factors = 1 / (1 + yield_val / m) ** (times * m)
    
    # 现值
    pv_cfs = cash_flows * discount_factors
    price = np.sum(pv_cfs)
    
    # 麦考利久期
    macaulay_duration = np.sum(times * pv_cfs) / price
    
    # 修正久期
    modified_duration = macaulay_duration / (1 + yield_val / m)
    
    return years_to_maturity, macaulay_duration, modified_duration

if __name__ == "__main__":
    # 示例：计算 "25国开15" 的久期
    bond_name = "25国开15"
    print(f"正在获取 {bond_name} 的数据...")
    meta = get_bond_metadata(bond_name)
    
    if meta:
        print(f"元数据获取成功: {meta}")
        
        # 假设当前市场收益率为 1.8% (从 bond_spot_deal 获取)
        current_yield = 0.018 
        
        years, mac_dur, mod_dur = calculate_duration(
            yield_val=current_yield,
            coupon_rate=meta['coupon_rate'],
            maturity_date=meta['maturity_date'],
            frequency_str=meta['frequency']
        )
        
        print(f"\n计算结果:")
        print(f"剩余期限: {years:.2f} 年")
        print(f"麦考利久期: {mac_dur:.4f}")
        print(f"修正久期: {mod_dur:.4f}")
    else:
        print("未能获取到债券元数据。")
