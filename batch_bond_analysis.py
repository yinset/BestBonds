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
CONCURRENT_THREADS = 1
SAVE_INTERVAL = 10
RETRY_COUNT = 5
DELAY_BETWEEN_REQUESTS = 5.0 

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
    底层获取债券元数据的逻辑，增加反爬措施
    """
    for attempt in range(RETRY_COUNT):
        try:
            search_symbol = symbol.replace(" ", "")
            
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
            
            ua = random.choice(USER_AGENTS)
            headers = {
                "User-Agent": ua,
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "X-Requested-With": "XMLHttpRequest",
                "Origin": "https://www.chinamoney.com.cn",
                "Referer": "https://www.chinamoney.com.cn/chinese/zqjc/",
                "Connection": "keep-alive",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
            }
            
            # 增加更长的随机延迟
            sleep_time = DELAY_BETWEEN_REQUESTS + random.uniform(2.0, 5.0) * (attempt + 1)
            time.sleep(sleep_time) 
            
            caller = session if session else requests
            r_search = caller.post(search_url, data=search_payload, headers=headers, timeout=20)
                
            if r_search.status_code in [403, 421]:
                wait_time = 30 * (attempt + 1)
                tqdm.write(f"警告: {symbol} 触发访问限制 ({r_search.status_code})，第 {attempt+1} 次尝试，等待 {wait_time} 秒...")
                time.sleep(wait_time)
                continue
                
            if r_search.status_code != 200:
                tqdm.write(f"失败: {symbol} 搜索接口返回状态码 {r_search.status_code}")
                continue
                
            search_json = r_search.json()
            result_list = search_json.get('data', {}).get('resultList', [])
            if not result_list:
                return None
                
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
            
            # 模拟人的操作间隔
            time.sleep(random.uniform(1.5, 3.0))
            
            r_detail = caller.post(detail_url, data=detail_payload, headers=detail_headers, timeout=20)
                
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
            tqdm.write(f"异常: {symbol} 抓取错误: {e}")
            time.sleep(5)
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
            
            # 处理 "够一年则进位一年" 的逻辑：366天 -> 1年1天
            if days is not None:
                years_part = days // 365
                days_part = days % 365
                if years_part > 0:
                    tenor_display = f"{years_part}年{days_part}天" if days_part > 0 else f"{years_part}年"
                else:
                    tenor_display = f"{days_part}天"
            else:
                tenor_display = None
            
            res_row['到期日'] = meta['maturity_date']
            res_row['票面利率'] = meta['coupon_rate']
            res_row['付息频率'] = meta['frequency']
            res_row['剩余期限_格式化'] = tenor_display
            res_row['剩余天数'] = days
            res_row['麦考利久期'] = mac_dur
            res_row['修正久期'] = mod_dur

            # 计算税后收益率
            # 国债和地方政府债免除20%的利息所得税。其他的债券需要上缴。
            # 地方债判断逻辑：bond_type包含"地方政府债" 或者 债券简称(symbol)中包含省份/城市地名关键词
            bond_type = meta.get('bond_type', '')
            
            # 定义地名关键词，用于识别地方债（包含省级行政区、简称、计划单列市及部分核心城市）
            loc_keywords = [
                 '北京', '京', '天津', '津', '河北', '冀', '山西', '晋', '内蒙古', '蒙', 
                 '辽宁', '辽', '吉林', '吉', '黑龙江', '黑', '上海', '沪', '江苏', '苏', 
                 '浙江', '浙', '安徽', '皖', '福建', '闽', '江西', '赣', '山东', '鲁', 
                 '河南', '豫', '湖北', '鄂', '湖南', '湘', '广东', '粤', '广西', '桂', 
                 '海南', '琼', '重庆', '渝', '四川', '川', '蜀', '贵州', '黔', '贵', 
                 '云南', '滇', '云', '西藏', '藏', '陕西', '陕', '秦', '甘肃', '甘', '陇', 
                 '青海', '青', '宁夏', '宁', '新疆', '新', '兵团', '深圳', '深', '大连', 
                 '青岛', '宁波', '厦门', '苏州', '无锡', '常州', '南通', '扬州', '镇江', 
                 '泰州', '徐州', '连云港', '淮安', '盐城', '宿迁', '杭州', '嘉兴', '湖州', 
                 '绍兴', '金华', '衢州', '舟山', '台州', '丽水', '合肥', '芜湖', '蚌埠', 
                 '淮南', '马鞍山', '淮北', '铜陵', '安庆', '黄山', '滁州', '阜阳', '宿州', 
                 '六安', '亳州', '池州', '宣城', '福州', '泉州', '漳州', '莆田', '三明', 
                 '南平', '龙岩', '宁德', '济南', '淄博', '枣庄', '东营', '烟台', '潍坊', 
                 '济宁', '泰安', '威海', '日照', '临沂', '德州', '聊城', '滨州', '菏泽', 
                 '广州', '韶关', '珠海', '汕头', '佛山', '江门', '湛江', '茂名', '肇庆', 
                 '惠州', '梅州', '汕尾', '河源', '阳江', '清远', '东莞', '中山', '潮州', 
                 '揭阳', '云浮', '南宁', '柳州', '桂林', '梧州', '北海', '防城港', '钦州', 
                 '贵港', '玉林', '百色', '贺州', '河池', '来宾', '崇左', '成都', '绵阳', 
                 '自贡', '攀枝花', '泸州', '德阳', '广元', '遂宁', '内江', '乐山', '南充', 
                 '宜宾', '广安', '达州', '眉山', '雅安', '巴中', '资阳', '阿坝', '甘孜', '凉山'
             ]
            is_local_gov_bond = '地方政府债' in bond_type or any(kw in symbol for kw in loc_keywords)
            
            if bond_type == '国债' or is_local_gov_bond:
                after_tax_yield = y_val
            else:
                after_tax_yield = y_val * 0.8 if not pd.isna(y_val) else None
            res_row['税后收益率'] = after_tax_yield
        else:
            res_row['到期日'] = None
            res_row['票面利率'] = None
            res_row['付息频率'] = None
            res_row['剩余期限_格式化'] = None
            res_row['剩余天数'] = None
            res_row['麦考利久期'] = None
            res_row['修正久期'] = None
            res_row['税后收益率'] = None
            
        results.append(res_row)

    # 5. 保存结果
    final_df = pd.DataFrame(results)
    
    # 映射表头为中文
    header_mapping = {
         '债券简称': '债券简称',
         '剩余天数': '剩余天数',
         '剩余期限_格式化': '剩余期限',
         '修正久期': '修正久期',
         '麦考利久期': '麦考利久期',
         '税后收益率': '税后收益率',
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
         '债券简称', '剩余天数', '剩余期限_格式化', '税后收益率', '修正久期', '麦考利久期', 
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
