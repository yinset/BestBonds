import akshare as ak
import pandas as pd
import requests
import os
import numpy as np
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import time

import threading

import random

# 配置
CACHE_FILE = "bond_metadata_cache.csv"
OUTPUT_FILE = "bond_analysis_results.xlsx"
CONCURRENT_THREADS = 10
SAVE_INTERVAL = 50
RETRY_COUNT = 3
DELAY_BETWEEN_REQUESTS = 2.0 

USER_AGENTS = [
    # Chrome on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    # Chrome on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    # Firefox on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    # Firefox on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
    # Edge on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
    # Safari on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    # Chrome on Linux
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
]

# 全局锁用于安全写文件和更新字典
cache_lock = threading.Lock()

def save_cache_to_file(cache_dict):
    """安全保存缓存到文件"""
    if not cache_dict:
        return
    with cache_lock:
        try:
            df = pd.DataFrame(list(cache_dict.values()))
            # 确保 symbol 列存在
            if not df.empty and 'symbol' in df.columns:
                df.to_csv(CACHE_FILE, index=False)
            elif not df.empty:
                print("警告: 缓存数据中缺失 'symbol' 列，跳过保存。")
        except Exception as e:
            print(f"保存缓存失败: {e}")

def get_bond_metadata_raw(symbol, session=None):
    """
    底层获取债券元数据的逻辑，手动处理请求以绕过 AKShare 的 JSON 解析错误
    """
    for attempt in range(RETRY_COUNT):
        try:
            # 0. 预处理符号
            search_symbol = symbol.replace(" ", "")
            
            # 1. 获取查询代码 (BondMarketInfoList2)
            search_url = "https://www.chinamoney.com.cn/ags/ms/cm-u-bond-md/BondMarketInfoList2"
            search_payload = {
                "pageNo": "1",
                "pageSize": "15",
                "bondName": search_symbol,
                "bondCode": "",
                "issueEnty": "",
                "bondType": "",
                "bondSpclPrjctVrty": "",
                "couponType": "",
                "issueYear": "",
                "entyDefinedCode": "",
                "rtngShrt": "",
            }
            headers = {
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "X-Requested-With": "XMLHttpRequest",
                "Origin": "https://www.chinamoney.com.cn",
                "Referer": "https://www.chinamoney.com.cn/chinese/zqjc/"
            }
            
            # 增加随机抖动
            time.sleep(DELAY_BETWEEN_REQUESTS + random.random() * 2) 
            
            if session:
                r_search = session.post(search_url, data=search_payload, headers=headers, timeout=15)
            else:
                r_search = requests.post(search_url, data=search_payload, headers=headers, timeout=15)
                
            if r_search.status_code == 421:
                tqdm.write(f"警告: {symbol} 触发 421 频率限制 (连接过多)，正在重试...")
                time.sleep(10) # 触发 421 时强制等待更久
                continue
                
            if r_search.status_code != 200:
                tqdm.write(f"失败: {symbol} 搜索接口返回状态码 {r_search.status_code}")
                return None
                
            search_json = r_search.json()
            result_list = search_json.get('data', {}).get('resultList', [])
            if not result_list:
                return None
                
            # 匹配最接近的简称
            query_code = None
            for res in result_list:
                if res.get('bondName').replace(" ", "") == search_symbol:
                    query_code = res.get('bondDefinedCode')
                    break
            
            if not query_code:
                query_code = result_list[0].get('bondDefinedCode')

            # 2. 调用详情接口
            detail_url = "https://www.chinamoney.com.cn/ags/ms/cm-u-bond-md/BondDetailInfo"
            detail_payload = {"bondDefinedCode": query_code}
            detail_headers = headers.copy()
            detail_headers["Referer"] = f"https://www.chinamoney.com.cn/chinese/zqjc/?bondDefinedCode={query_code}"
            
            time.sleep(1.0 + random.random())
            
            if session:
                r_detail = session.post(detail_url, data=detail_payload, headers=detail_headers, timeout=15)
            else:
                r_detail = requests.post(detail_url, data=detail_payload, headers=detail_headers, timeout=15)
                
            if r_detail.status_code != 200:
                tqdm.write(f"失败: {symbol} 详情接口返回状态码 {r_detail.status_code}")
                continue
            
            res_json = r_detail.json()
            if 'data' not in res_json or 'bondBaseInfo' not in res_json['data']:
                continue
                
            data = res_json['data']['bondBaseInfo']
            
            metadata = {
                'symbol': symbol,
                'maturity_date': data.get('mrtyDate'),
                'coupon_rate': float(data.get('parCouponRate', 0)) / 100 if data.get('parCouponRate') and data.get('parCouponRate') != '---' else 0,
                'frequency': data.get('couponFrqncy', '年'),
                'bond_type': data.get('bondType')
            }
            return metadata
        except Exception as e:
            tqdm.write(f"异常: {symbol} 抓取过程中出现错误: {e}")
            time.sleep(2)
            continue
    return None

def calculate_duration(yield_val, coupon_rate, maturity_date, frequency_str='年', settlement_date=None):
    """
    计算债券久期、剩余期限（年）和剩余天数
    """
    try:
        if settlement_date is None:
            settlement_date = datetime.now()
        else:
            settlement_date = datetime.strptime(settlement_date, '%Y-%m-%d')
            
        if not maturity_date or maturity_date == '---':
            return None, None, None, None
            
        maturity_date_dt = datetime.strptime(maturity_date, '%Y-%m-%d')
        
        # 剩余天数
        days_to_maturity = (maturity_date_dt - settlement_date).days
        if days_to_maturity <= 0:
            return 0, 0, 0, 0
            
        # 剩余年限格式化: 够一年则进位一年 (这里理解为天数/365向上取整，或者按照用户描述的逻辑)
        # 用户描述: "够一年则进位一年"。通常指 1.1年 -> 2年？
        # 或者是指显示为 "X年" 或 "X天"。
        # 重新理解: "添加一个剩余期限列。单位为天。够一年则进位一年。"
        # 这里的进位一年可能是指在年限显示上，如果天数超过365则算作1年。
        # 但既然单位是天，那就直接显示天数。
        # 另外可能需要一个中文可读的列。
        
        years_to_maturity = days_to_maturity / 365.25
        
        # 到期收益率处理
        if pd.isna(yield_val) or yield_val == 0:
            return years_to_maturity, None, None, days_to_maturity
        
        y = yield_val / 100 
        
        freq_map = {'年': 1, '半年': 2, '季': 4, '按年付息': 1, '半年付息': 2, '按季付息': 4}
        m = freq_map.get(frequency_str, 1)
        
        num_payments = int(np.ceil(years_to_maturity * m))
        times = np.linspace(years_to_maturity % (1/m) or (1/m), years_to_maturity, num_payments)
        
        cash_flows = np.array([coupon_rate / m * 100] * num_payments)
        cash_flows[-1] += 100 
        
        discount_factors = 1 / (1 + y / m) ** (times * m)
        pv_cfs = cash_flows * discount_factors
        price = np.sum(pv_cfs)
        
        if price == 0:
            return years_to_maturity, None, None, days_to_maturity
            
        macaulay_duration = np.sum(times * pv_cfs) / price
        modified_duration = macaulay_duration / (1 + y / m)
        
        return years_to_maturity, macaulay_duration, modified_duration, days_to_maturity
    except:
        return None, None, None, None

def main():
    print("1. 正在获取最新成交行情数据...")
    try:
        deal_df = ak.bond_spot_deal()
        print(f"成功获取 {len(deal_df)} 条成交记录。")
    except Exception as e:
        print(f"获取行情失败: {e}")
        return

    # 2. 加载缓存
    cache = {}
    if os.path.exists(CACHE_FILE):
        print(f"2. 正在加载本地缓存 {CACHE_FILE}...")
        try:
            cache_df = pd.read_csv(CACHE_FILE)
            if not cache_df.empty and 'symbol' in cache_df.columns:
                # 核心修复：去重后再转字典，防止 set_index 报错
                cache = cache_df.drop_duplicates(subset=['symbol']).set_index('symbol').to_dict('index')
                print(f"已加载 {len(cache)} 条债券元数据缓存。")
            else:
                print("2. 缓存文件格式异常，将开始全新抓取。")
        except Exception as e:
            print(f"2. 加载缓存失败 ({e})，将开始全新抓取。")
    else:
        print("2. 未发现本地缓存，将开始全新抓取。")

    # 3. 筛选需要更新元数据的债券
    symbols_to_fetch = [s for s in deal_df['债券简称'].unique() if s not in cache]
    
    if symbols_to_fetch:
        print(f"3. 发现 {len(symbols_to_fetch)} 个新债券，正在抓取元数据（并发数: {CONCURRENT_THREADS}）...")
        processed_count = 0
        success_count = 0
        session = requests.Session()
        # 预访问首页
        try:
            session.get("https://www.chinamoney.com.cn/chinese/zqjc/", timeout=15)
        except:
            pass
            
        with ThreadPoolExecutor(max_workers=CONCURRENT_THREADS) as executor:
            future_to_symbol = {executor.submit(get_bond_metadata_raw, s, session): s for s in symbols_to_fetch}
            for future in tqdm(as_completed(future_to_symbol), total=len(symbols_to_fetch)):
                symbol = future_to_symbol[future]
                processed_count += 1
                try:
                    data = future.result()
                    if data:
                        cache[symbol] = data
                        success_count += 1
                    
                    # 每处理 SAVE_INTERVAL 个，执行一次保存
                    if processed_count % SAVE_INTERVAL == 0:
                        save_cache_to_file(cache)
                        tqdm.write(f"已处理 {processed_count} 个，当前缓存成功 {len(cache)} 个债券数据。")
                except Exception as e:
                    pass
        
        # 最终保存一次
        save_cache_to_file(cache)
        print(f"缓存已更新，当前共计 {len(cache)} 条记录。")
    else:
        print("3. 所有债券元数据均已在缓存中。")

    # 4. 计算指标
    print("4. 正在计算剩余期限及久期...")
    results = []
    today_str = datetime.now().strftime('%Y-%m-%d')
    
    for _, row in deal_df.iterrows():
        symbol = row['债券简称']
        meta = cache.get(symbol)
        
        res_row = row.to_dict()
        if meta:
            y_val = row['加权收益率'] if not pd.isna(row['加权收益率']) else row['最新收益率']
            
            years, mac_dur, mod_dur, days = calculate_duration(
                yield_val=y_val,
                coupon_rate=meta['coupon_rate'],
                maturity_date=meta['maturity_date'],
                frequency_str=meta['frequency']
            )
            
            # 处理 "够一年则进位一年" 的逻辑
            # 这里理解为：如果天数 >= 365，则按年计算（向下取整），剩下的天数另计？
            # 或者是：剩余年限列显示为整数年（向上取整）？
            # 用户的原话是 "添加一个剩余期限列。单位为天。够一年则进位一年。"
            # 最合理的解释是：添加一个列，如果不足365天显示天数，如果超过365天，则显示为年（进位）。
            # 或者更简单的：列名是“剩余期限(天)”，但逻辑是“够一年进位一年”。
            # 重新读： "添加一个剩余期限列。单位为天。够一年则进位一年。"
            # 可能是指： 366天 -> 2年？ 还是 366天 -> 366天，但在另一个列体现进位？
            # 决定增加两个列： "剩余期限(天)" 和 "剩余期限(年)"。
            # "够一年则进位一年" 应用在“年”的逻辑上： math.ceil(days / 365)
            
            res_row['到期日'] = meta['maturity_date']
            res_row['票面利率'] = meta['coupon_rate']
            res_row['付息频率'] = meta['frequency']
            res_row['剩余年限'] = np.ceil(days / 365) if days is not None else None
            res_row['剩余天数'] = days
            res_row['麦考利久期'] = mac_dur
            res_row['修正久期'] = mod_dur
        else:
            res_row['到期日'] = None
            res_row['票面利率'] = None
            res_row['付息频率'] = None
            res_row['剩余年限'] = None
            res_row['剩余天数'] = None
            res_row['麦考利久期'] = None
            res_row['修正久期'] = None
            
        results.append(res_row)

    # 5. 保存结果
    final_df = pd.DataFrame(results)
    
    # 映射表头为中文
    header_mapping = {
        '债券简称': '债券简称',
        '剩余天数': '剩余期限(天)',
        '剩余年限': '剩余期限(年)',
        '修正久期': '修正久期',
        '麦考利久期': '麦考利久期',
        '到期日': '到期日',
        '票面利率': '票面利率',
        '付息频率': '付息频率',
        '加权收益率': '加权收益率',
        '最新收益率': '最新收益率',
        '成交净价': '成交净价',
        '交易量': '成交量(万)',
        '成交时间': '成交时间'
    }
    
    # 调整列顺序并重命名
    cols_order = [
        '债券简称', '剩余天数', '剩余年限', '修正久期', '麦考利久期', 
        '到期日', '加权收益率', '最新收益率', '成交净价', '交易量', '成交时间'
    ]
    
    # 确保列存在
    existing_cols = [c for c in cols_order if c in final_df.columns]
    final_df = final_df[existing_cols]
    final_df.rename(columns=header_mapping, inplace=True)
    
    final_df.to_excel(OUTPUT_FILE, index=False, engine='openpyxl')
    print(f"5. 分析完成！结果已保存至: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
