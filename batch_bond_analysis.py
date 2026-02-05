import akshare as ak
import pandas as pd
import requests
import os
import numpy as np
from datetime import datetime, time as dt_time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import time
import re
import threading
import random

# 配置
CACHE_DIR = "cache"
CACHE_FILE = os.path.join(CACHE_DIR, "bond_metadata_cache.csv")
OUTPUT_FILE_BASE = "bond_analysis_results"  # 基础文件名，会自动加上日期
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

def is_within_cache_window():
    """
    判断当前时间是否处于交易日的 8:00-20:00 之间。
    如果在该窗口内，返回 True，表示应优先使用缓存。
    """
    now = datetime.now()
    
    # 1. 检查是否为周末
    if now.weekday() >= 5: # 5是周六，6是周日
        return False
        
    # 2. 检查时间是否在 8:00 - 20:00 之间
    start_time = dt_time(8, 0)
    end_time = dt_time(20, 0)
    current_time = now.time()
    
    if start_time <= current_time <= end_time:
        return True
        
    return False

def get_latest_cache_date():
    """
    从 cache 目录中寻找日期最近的成交数据缓存文件。
    返回日期字符串 (YYYY-MM-DD) 和文件路径。
    """
    if not os.path.exists(CACHE_DIR):
        return None, None
        
    pattern = re.compile(r"bond_deal_cache_(\d{4}-\d{2}-\d{2})\.csv")
    cache_files = []
    
    for f in os.listdir(CACHE_DIR):
        match = pattern.match(f)
        if match:
            date_str = match.group(1)
            cache_files.append((date_str, os.path.join(CACHE_DIR, f)))
            
    if not cache_files:
        return None, None
        
    # 按日期排序，取最近的一个
    cache_files.sort(key=lambda x: x[0], reverse=True)
    return cache_files[0]

def save_cache_to_file(cache_dict):
    """安全保存缓存到文件"""
    if not cache_dict:
        return
    with cache_lock:
        try:
            # 确保每个 metadata 字典里都有 symbol 字段
            # 如果是从 cache_df.set_index('symbol', drop=False).to_dict('index') 出来的，
            # 里面已经包含了 symbol。这里做个双重保险。
            data_list = []
            for symbol, meta in cache_dict.items():
                if 'symbol' not in meta:
                    meta['symbol'] = symbol
                data_list.append(meta)
                
            df = pd.DataFrame(data_list)
            if not df.empty:
                # 使用 utf-8-sig 以便 Excel 打开不乱码，且能正确处理中文
                df.to_csv(CACHE_FILE, index=False, encoding='utf-8-sig')
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
                'bond_type': data.get('bondType'),
                'coupon_type': data.get('intrstPayMeth')
            }
            return metadata
        except Exception as e:
            tqdm.write(f"异常: {symbol} 抓取错误: {e}")
            time.sleep(5)
            continue
    return None

import warnings
import traceback
from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

def day_count_fraction(date1, date2, convention='Act/365'):
    """
    根据天数计算惯例计算两个日期之间的时间份额（年）
    """
    days = (date2 - date1).days
    if convention == 'Act/Act':
        # 简化版 Act/Act: 如果跨越闰年，按实际天数计算
        # 严格版需要判断每个时间段所在年份的总天数
        # 这里采用 365.25 作为近似，或者更精确地根据年份判断
        year1 = date1.year
        year2 = date2.year
        if year1 == year2:
            days_in_year = 366 if (year1 % 4 == 0 and (year1 % 100 != 0 or year1 % 400 == 0)) else 365
            return days / days_in_year
        else:
            # 跨年情况，按 365.25 平均
            return days / 365.25
    elif convention == '30/360':
        d1 = min(30, date1.day)
        d2 = date2.day
        if d1 == 30 and d2 == 31:
            d2 = 30
        return (360 * (date2.year - date1.year) + 30 * (date2.month - date1.month) + (d2 - d1)) / 360
    else:  # Act/365
        return days / 365

def get_coupon_dates(settlement_date, maturity_date, frequency_str):
    """
    生成实际付息日列表，从到期日向回推算
    """
    freq_map = {'年': 1, '半年': 2, '季': 4, '按年付息': 1, '半年付息': 2, '按季付息': 4}
    m = freq_map.get(frequency_str, 1)
    months_step = 12 // m
    
    coupon_dates = []
    current_date = maturity_date
    
    # 向回推算直到 settlement_date 之前
    while current_date > settlement_date:
        coupon_dates.append(current_date)
        # 使用 pandas 的 DateOffset 进行精确的月份减法
        current_date = current_date - pd.DateOffset(months=months_step)
    
    # 最后一项推出来的 current_date 即为上一个付息日（或起息日）
    last_coupon_date = current_date
    coupon_dates.sort()
    return coupon_dates, last_coupon_date

def calculate_duration(yield_val, coupon_rate, maturity_date, frequency_str='年', settlement_date=None, bond_type='', coupon_type=''):
    """
    计算债券久期、剩余期限（年）和剩余天数（优化版）
    """
    try:
        # 1. 初始化日期
        if settlement_date is None:
            settlement_dt = datetime.now()
        elif isinstance(settlement_date, str):
            settlement_dt = datetime.strptime(settlement_date, '%Y-%m-%d')
        else:
            settlement_dt = settlement_date
            
        # 统一处理为日期，避免时间差导致的计算偏差
        if hasattr(settlement_dt, 'replace'):
            settlement_dt = settlement_dt.replace(hour=0, minute=0, second=0, microsecond=0)
            
        if not maturity_date or maturity_date == '---':
            return None, None, None, None
            
        if isinstance(maturity_date, str):
            maturity_dt = datetime.strptime(maturity_date, '%Y-%m-%d')
        else:
            maturity_dt = maturity_date
            
        # 统一去除时间部分
        if hasattr(maturity_dt, 'replace'):
            maturity_dt = maturity_dt.replace(hour=0, minute=0, second=0, microsecond=0)

        # 2. 剩余天数 (到期日 - 结算日)
        delta = maturity_dt - settlement_dt
        days_to_maturity = delta.days
        
        # 剩余年限 (用于久期计算中的时间份额)
        years_to_maturity = days_to_maturity / 365.25
        
        if days_to_maturity <= 0:
            return 0, 0, 0, 0

        # 3. 收益率合理性检查
        if not isinstance(yield_val, (int, float, np.float64)) or pd.isna(yield_val) or yield_val == 0:
             # 如果收益率无效，仅返回剩余天数和粗略期限
             return years_to_maturity, None, None, days_to_maturity

        if yield_val < -10 or yield_val > 100:  # 收益率范围检查
            warnings.warn(f"到期收益率 {yield_val}% 可能不合理")

        # 4. 确定计算惯例
        convention = 'Act/Act' if any(bt in bond_type for bt in ['国债', '地方政府债']) else 'Act/365'
        
        # 5. 生成现金流时间点
        # 判断是否为贴现或零息债券
        is_discount_or_zero = False
        if coupon_type and any(kw in coupon_type for kw in ['贴现', '零息']):
            is_discount_or_zero = True

        freq_map = {'年': 1, '半年': 2, '季': 4, '按年付息': 1, '半年付息': 2, '按季付息': 4}
        m = freq_map.get(frequency_str, 1)

        if is_discount_or_zero:
            coupon_dates = [maturity_dt]
            last_coupon_date = settlement_dt # 简化处理，应计利息为0
            coupon_per_period = 0
            cash_flows = np.array([100.0])
        else:
            coupon_dates, last_coupon_date = get_coupon_dates(settlement_dt, maturity_dt, frequency_str)
            if not coupon_dates: # 异常情况
                return years_to_maturity, None, None, days_to_maturity
            
            # 每期利息
            coupon_per_period = (coupon_rate * 100) / m
            cash_flows = np.array([coupon_per_period] * len(coupon_dates))
            cash_flows[-1] += 100 # 加上本金

        # 6. 计算各现金流的时间份额 (years from settlement)
        times = np.array([day_count_fraction(settlement_dt, d, convention) for d in coupon_dates])
        
        # 7. 计算应计利息 (Accrued Interest)
        next_coupon_date = coupon_dates[0]
        days_since_last = (settlement_dt - last_coupon_date).days
        days_in_period = (next_coupon_date - last_coupon_date).days
        
        accrued_interest = coupon_per_period * (days_since_last / days_in_period) if days_in_period > 0 else 0
        
        # 8. 计算价格和久期
        y = yield_val / 100 
        
        # 中国市场通常采用复利计算（剩余期限 > 1年）
        # 如果剩余期限 <= 1年，通常采用单利 (price = (principal + interest) / (1 + y * t))
        years_to_mat = times[-1]
        
        if years_to_mat > 1:
            # 复利折现
            discount_factors = 1 / (1 + y / m) ** (times * m)
        else:
            # 单利折现 (针对最后一次现金流)
            discount_factors = 1 / (1 + y * times)
            
        pv_cfs = cash_flows * discount_factors
        full_price = np.sum(pv_cfs)
        
        if full_price <= 0:
            return years_to_mat, None, None, days_to_maturity
            
        macaulay_duration = np.sum(times * pv_cfs) / full_price
        
        if years_to_mat > 1:
            modified_duration = macaulay_duration / (1 + y / m)
        else:
            # 短期债修正久期
            modified_duration = macaulay_duration / (1 + y) 
            
        return years_to_mat, macaulay_duration, modified_duration, days_to_maturity
    except Exception as e:
        # traceback.print_exc() # 调试用
        return None, None, None, None

def create_homepage_sheet(writer, short_bonds_raw, mid_bonds_raw, long_bonds_raw, header_mapping, cols_order):
    """
    创建主页sheet，展示推荐购买的债券
    分为四个区域：
    - 左上角：无风险短期债券选择
    - 右上角：中风险中期债券选择
    - 左下角：高风险长期债券选择
    - 右下角：备注区
    """
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
    
    # 创建工作表
    ws = writer.book.create_sheet("主页", 0)  # 插入到第一个位置
    
    # 处理DataFrame，应用与process_sheet_df相同的逻辑
    def prepare_df(df):
        if df.empty:
            return df
        existing_cols = [c for c in cols_order if c in df.columns]
        df_prepared = df[existing_cols].copy()
        df_prepared.sort_values('税后年收益率', ascending=False, inplace=True)
        df_prepared.rename(columns=header_mapping, inplace=True)
        return df_prepared
    
    short_bonds = prepare_df(short_bonds_raw)
    mid_bonds = prepare_df(mid_bonds_raw)
    long_bonds = prepare_df(long_bonds_raw)
    
    # 定义样式
    header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
    header_font = Font(bold=True, color="FFFFFF", size=11)
    title_font = Font(bold=True, size=12)
    border = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )
    center_alignment = Alignment(horizontal='center', vertical='center')
    left_alignment = Alignment(horizontal='left', vertical='center')
    
    # 收益率和久期的颜色填充
    yield_fill = PatternFill(start_color="FFE699", end_color="FFE699", fill_type="solid")  # 浅黄色
    duration_fill = PatternFill(start_color="C5E0B4", end_color="C5E0B4", fill_type="solid")  # 浅绿色
    
    def filter_top_yield_bonds(df, col_name='税后年收益率'):
        """筛选税后年收益率从最高到最高*0.9的债券"""
        if df.empty:
            return pd.DataFrame()
        
        # 检查列名是否存在（可能是原始列名或重命名后的列名）
        actual_col = None
        if col_name in df.columns:
            actual_col = col_name
        elif '税后年收益率' in df.columns:
            actual_col = '税后年收益率'
        else:
            return pd.DataFrame()
        
        # 过滤掉税后年收益率为空或无效的数据
        df_filtered = df[df[actual_col].notna() & (df[actual_col] > 0)].copy()
        if df_filtered.empty:
            return pd.DataFrame()
        
        # 按税后年收益率降序排序
        df_sorted = df_filtered.sort_values(actual_col, ascending=False)
        
        # 获取最高收益率
        max_yield = df_sorted[actual_col].iloc[0]
        min_yield = max_yield * 0.9
        
        # 筛选范围
        result = df_sorted[df_sorted[actual_col] >= min_yield].copy()
        return result
    
    def write_bond_table(ws, start_row, start_col, title, bonds_df, max_rows=15):
        """在指定位置写入债券表格"""
        if bonds_df.empty:
            # 写入标题和空提示
            ws.merge_cells(start_row=start_row, start_column=start_col, end_row=start_row, end_column=start_col+6)
            cell = ws.cell(row=start_row, column=start_col)
            cell.value = title
            cell.font = title_font
            cell.alignment = center_alignment
            cell.fill = header_fill
            cell.font = header_font
            
            ws.merge_cells(start_row=start_row+1, start_column=start_col, end_row=start_row+1, end_column=start_col+6)
            cell = ws.cell(row=start_row+1, column=start_col)
            cell.value = "暂无符合条件的债券"
            cell.alignment = center_alignment
            return start_row + 2
        
        # 准备显示的列（使用重命名后的列名，因为DataFrame已经通过prepare_df重命名过）
        display_cols = ['债券简称', '税后年收益率', '麦考利久期', '剩余期限', '到期日', '票面利率', '成交额(亿元)']
        
        # 写入标题
        ws.merge_cells(start_row=start_row, start_column=start_col, end_row=start_row, end_column=start_col+len(display_cols)-1)
        cell = ws.cell(row=start_row, column=start_col)
        cell.value = title
        cell.font = title_font
        cell.alignment = center_alignment
        cell.fill = header_fill
        cell.font = header_font
        
        # 写入表头
        header_row = start_row + 1
        for idx, col_name in enumerate(display_cols):
            cell = ws.cell(row=header_row, column=start_col+idx)
            cell.value = col_name
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = center_alignment
            cell.border = border
        
        # 写入数据（限制最大行数）
        data_rows = min(len(bonds_df), max_rows)
        for row_idx in range(data_rows):
            excel_row = header_row + 1 + row_idx
            for col_idx, col_name in enumerate(display_cols):
                cell = ws.cell(row=excel_row, column=start_col+col_idx)
                # DataFrame已经重命名过，直接使用display_cols中的列名
                df_col = col_name
                
                if df_col in bonds_df.columns:
                    value = bonds_df.iloc[row_idx][df_col]
                    # 处理NaN值
                    if pd.isna(value):
                        cell.value = '---'
                    elif isinstance(value, (int, float)):
                        if col_name == '税后年收益率':
                            cell.value = round(value, 4)
                        elif col_name == '麦考利久期':
                            cell.value = round(value, 4)
                        elif col_name == '票面利率':
                            cell.value = round(value, 4)
                        elif col_name == '成交额(亿元)':
                            cell.value = int(value) if isinstance(value, float) and value.is_integer() else value
                        else:
                            cell.value = value
                    else:
                        cell.value = str(value)
                else:
                    cell.value = '---'
                
                # 设置样式
                cell.border = border
                cell.alignment = center_alignment if col_idx < 2 else left_alignment
                
                # 给税后年收益率和麦考利久期加颜色
                if col_name == '税后年收益率':
                    cell.fill = yield_fill
                elif col_name == '麦考利久期':
                    cell.fill = duration_fill
        
        # 设置列宽
        col_widths = [20, 12, 12, 15, 12, 10, 12]
        for idx, width in enumerate(col_widths):
            ws.column_dimensions[get_column_letter(start_col+idx)].width = width
        
        return excel_row + 2
    
    # 筛选各类型债券
    top_short = filter_top_yield_bonds(short_bonds)
    top_mid = filter_top_yield_bonds(mid_bonds)
    top_long = filter_top_yield_bonds(long_bonds)
    
    # 左侧：三个债券表格垂直排列 (从A1开始)
    # 限制每个风险组最多显示10个债券
    end_row_1 = write_bond_table(ws, 1, 1, "无风险短期债券选择", top_short, max_rows=10)
    end_row_2 = write_bond_table(ws, end_row_1, 1, "中风险中期债券选择", top_mid, max_rows=10)
    end_row_3 = write_bond_table(ws, end_row_2, 1, "高风险长期债券选择", top_long, max_rows=10)
    
    # 右侧：备注区 (从H1开始，即第8列)
    notes_start_row = 1
    # 备注区标题
    ws.merge_cells(start_row=notes_start_row, start_column=8, end_row=notes_start_row, end_column=14)
    cell = ws.cell(row=notes_start_row, column=8)
    cell.value = "备注区"
    cell.font = title_font
    cell.alignment = center_alignment
    cell.fill = header_fill
    cell.font = header_font
    cell.border = border
    
    # 备注内容区域
    # 内容区单元格合并大小为24行
    notes_content_rows = 24 
    # 先合并内容区域
    ws.merge_cells(start_row=notes_start_row+1, start_column=8, end_row=notes_start_row+notes_content_rows, end_column=14)
    # 然后设置合并单元格的样式
    cell = ws.cell(row=notes_start_row+1, column=8)
    
    remarks_text = """***************************如果不清楚存款利率变化会给债券带来怎样的风险，请直接选择无风险短期债券区***************************

1.推荐在四大行和招商银行购买。更推荐在招商银行购买，菜单更人性化。
2.目前接口只能抓取到机构间的数据和收益率，实际利率会有一点点点点降低，银行柜台债小概率会搜不到某个国债（未面向个人投资者），详情见银行。
3.如何度量债券风险：一笔钱距离你越久远，利率变化对你即将获得的所有钱产生的蝴蝶效应越大。时间是决定债券波动率的决定因素。
4.久期是什么？为什么久期能够估算利率的杠杆率？
假设你购买了30年期限的，持有到期的年收益率为2%的国债，购买额度100万。每一年会给2万元利息，最后一年连本带息付102万。

让我们想象一块无重力木板，将一整条木板等分成30份相同长度后划分刻度，第0年不付息，0这个刻度不放钱，1-29年在每一个刻度上放n年后的2万元，最后一个刻度上放30年后的102万
然后，找到木板两边平衡的支点（重心），重心所在的刻度就是这段现金流的久期（久期单位为年）。这就是麦考利久期的物理意义——现金流时间的加权平均（权重是现金流的现值）。
证明这段钱（受到利率的影响程度）等效于一个剩余期限为久期年的一笔钱。

在本例中，想要获得久期，我们首先把未来的钱计算成现值。
n年后的2万现值为20000/(1+0.02)^n
第 1 年：≈ 1.9608 万元
第 2 年：≈ 1.9223 万元
…
第 29 年：≈ 1.1395 万元
第 30 年：102 万的现值 ≈ 56.3112 万元

计算重心：
分子 = (Σ(29,n=1) n * n年后现金流现值) + 30 * 56.3112 万元= 2285.429 万元
分母：所有现值重量的总和 = 债券价格 = 100 万元（总现值）

（重心）久期 = 分子/分母 = 22.85年
如果利率上升 1%，债券价值变化大约 -22.85%，实际计算为-19.99672%,误差符合预计。即本例债券的现值会由原本的100万元变为大约80万元。
如果利率下降 0.5%，债券价值变化大约 +11.43%，实际计算为+12.001026%,误差符合预计。即本例债券的现值会由原本的100万元变为大约112万元。

故久期基本上可以度量债券因未来利率变化的粗略波动幅度，即债券的风险度。
参照自己的波动需求选择！"""

    cell.value = remarks_text
    cell.border = border
    cell.alignment = Alignment(horizontal='left', vertical='top', wrap_text=True)
    # 设置列宽
    ws.column_dimensions[get_column_letter(8)].width = 50
    ws.column_dimensions['N'].width = 28
    
    # 设置行高
    ws.row_dimensions[1].height = 25
    for row in range(2, ws.max_row + 1):
        ws.row_dimensions[row].height = 20


def main():
    # 1. 确定日期和缓存策略
    now = datetime.now()
    use_cache = is_within_cache_window()
    
    latest_date_str, latest_cache_path = get_latest_cache_date()
    
    # 获取结算日字符串
    if use_cache and latest_date_str:
        # 在 8:00-20:00 窗口内，且存在历史缓存
        settlement_dt_str = latest_date_str
        deal_cache_file = latest_cache_path
        print(f"当前处于交易期 (交易日 8:00-20:00)，将使用历史最近交易日的缓存数据: {latest_date_str}")
    else:
        # 窗口外，或者没有任何缓存，尝试抓取今天的数据
        settlement_dt_str = now.strftime("%Y-%m-%d")
        deal_cache_file = os.path.join(CACHE_DIR, f"bond_deal_cache_{settlement_dt_str}.csv")
        if not use_cache:
            print(f"当前处于抓取窗口 (非交易日或非 8:00-20:00)，将尝试获取最新数据...")
        else:
            print(f"未发现任何历史缓存，将尝试获取最新数据...")

    output_file = f"{OUTPUT_FILE_BASE}_{settlement_dt_str}.xlsx"
    
    # 2. 获取并清洗成交数据
    deal_df = None
    if os.path.exists(deal_cache_file):
        print(f"1. 正在从本地缓存 {deal_cache_file} 加载成交行情数据...")
        try:
            deal_df = pd.read_csv(deal_cache_file, encoding='utf-8-sig')
            print(f"成功从缓存加载 {len(deal_df)} 条成交记录。")
        except Exception as e:
            print(f"加载成交行情缓存失败: {e}，将重新抓取...")

    if deal_df is None:
        print("1. 正在从接口获取最新成交行情数据...")
        try:
            deal_df = ak.bond_spot_deal()
            print(f"成功获取 {len(deal_df)} 条成交记录。")
            
            # 保存到缓存
            deal_df.to_csv(deal_cache_file, index=False, encoding='utf-8-sig')
            print(f"已保存成交行情到缓存: {deal_cache_file}")
        except Exception as e:
            print(f"获取行情失败: {e}")
            return

    if deal_df is not None:
        # 统一筛选逻辑：在加载完成后立即执行所有过滤
        initial_count = len(deal_df)
        
        # 1. 筛选国债且非贴现
        mask = deal_df['债券简称'].str.contains('国债') & ~deal_df['债券简称'].str.contains('贴现')
        
        # 2. 筛选成交量（>= 10亿元）
        if '交易量' in deal_df.columns:
            mask = mask & (deal_df['交易量'] >= 10)
            
        deal_df = deal_df[mask].copy()
        print(f"1. 统一筛选完成：从 {initial_count} 条过滤至 {len(deal_df)} 条（条件：国债、非贴现、成交量>=10亿）。")

    # 3. 加载元数据缓存
    cache = {}
    if os.path.exists(CACHE_FILE):
        print(f"2. 正在加载本地缓存 {CACHE_FILE}...")
        try:
            # 使用 utf-8-sig 处理可能存在的 BOM 头
            cache_df = pd.read_csv(CACHE_FILE, encoding='utf-8-sig')
            if not cache_df.empty and 'symbol' in cache_df.columns:
                cache_df = cache_df.dropna(subset=['symbol'])
                cache_df = cache_df.drop_duplicates(subset=['symbol'], keep='last')
                cache = cache_df.set_index('symbol', drop=False).to_dict('index')
                print(f"已加载 {len(cache)} 条债券元数据缓存。")
            else:
                print("2. 缓存文件格式异常。")
        except Exception as e:
            print(f"2. 加载缓存失败 ({e})。")
    else:
        print("2. 未发现本地债券元数据缓存。")

    # 4. 检查是否需要抓取新债券的元数据
    # 建立一个规范化名称的索引，提高查找效率
    normalized_cache = {k.replace(" ", ""): v for k, v in cache.items()}
    symbols_to_fetch = [s for s in deal_df['债券简称'].unique() if s.replace(" ", "") not in normalized_cache]
    
    if symbols_to_fetch:
        print(f"3. 发现 {len(symbols_to_fetch)} 个新债券缺失元数据，正在抓取（并发数: {CONCURRENT_THREADS}）...")
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
            for future in tqdm(as_completed(future_to_symbol), total=len(symbols_to_fetch), desc="抓取进度"):
                symbol = future_to_symbol[future]
                processed_count += 1
                try:
                    data = future.result()
                    if data:
                        cache[symbol] = data
                        normalized_cache[symbol.replace(" ", "")] = data
                        success_count += 1
                    
                    # 每处理 SAVE_INTERVAL 个，执行一次保存
                    if processed_count % SAVE_INTERVAL == 0:
                        save_cache_to_file(cache)
                except Exception as e:
                    pass
        
        # 最终保存一次
        save_cache_to_file(cache)
        print(f"3. 抓取完成。本次成功: {success_count}, 失败: {len(symbols_to_fetch) - success_count}。当前总缓存: {len(cache)} 条。")
    else:
        print("3. 所有成交债券的元数据已在缓存中，跳过抓取。")

    # 5. 计算指标
    print("4. 正在计算剩余期限及久期...")
    results = []
    
    session = requests.Session()
    # 预访问首页
    try:
        session.get("https://www.chinamoney.com.cn/chinese/zqjc/", timeout=15)
    except:
        pass

    for _, row in tqdm(deal_df.iterrows(), total=len(deal_df), desc="计算进度"):
        symbol = row['债券简称']
        search_key = symbol.replace(" ", "")
        meta = normalized_cache.get(search_key)
        
        # 如果缓存没有，尝试实时抓取
        if not meta:
            tqdm.write(f"缓存未命中: {symbol}，尝试实时抓取...")
            meta = get_bond_metadata_raw(symbol, session=session)
            if meta:
                cache[symbol] = meta
                normalized_cache[search_key] = meta
                # 抓取成功后顺便存一下文件，防止中途退出
                if len(cache) % SAVE_INTERVAL == 0:
                    save_cache_to_file(cache)
        
        res_row = row.to_dict()
        
        # 格式化交易量
        if '交易量' in res_row and not pd.isna(res_row['交易量']):
             vol = res_row['交易量']
             # 如果是整数则去掉小数点，保持数值类型以方便筛选
             if isinstance(vol, (float, np.float64)) and vol.is_integer():
                 vol = int(vol)
             res_row['交易量'] = vol

        if meta:
            y_val = row['加权收益率'] if not pd.isna(row['加权收益率']) else row['最新收益率']
            
            years, mac_dur, mod_dur, days = calculate_duration(
                yield_val=y_val,
                coupon_rate=meta['coupon_rate'],
                maturity_date=meta['maturity_date'],
                frequency_str=meta['frequency'],
                settlement_date=settlement_dt_str,
                bond_type=meta.get('bond_type', ''),
                coupon_type=meta.get('coupon_type', '')
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
            res_row['付息方式'] = meta.get('coupon_type', '---')
            res_row['剩余期限_格式化'] = tenor_display
            res_row['剩余天数'] = days
            res_row['麦考利久期'] = mac_dur
            res_row['修正久期'] = mod_dur
            res_row['债券类型'] = meta.get('bond_type', '未知')

            # 计算税后年收益率
            # 国债和地方政府债免除20%的利息所得税。其他的债券需要上缴。
            # 地方债判断逻辑：bond_type包含"地方政府债"
            bond_type = meta.get('bond_type', '')
            is_local_gov_bond = '地方政府债' in bond_type
            
            if bond_type == '国债' or is_local_gov_bond:
                after_tax_yield = y_val
            else:
                after_tax_yield = y_val * 0.8 if not pd.isna(y_val) else None
            res_row['税后年收益率'] = after_tax_yield
        else:
            res_row['到期日'] = None
            res_row['票面利率'] = None
            res_row['付息频率'] = None
            res_row['付息方式'] = None
            res_row['剩余期限_格式化'] = None
            res_row['剩余天数'] = None
            res_row['麦考利久期'] = None
            res_row['修正久期'] = None
            res_row['税后年收益率'] = None
            res_row['债券类型'] = None
            
        results.append(res_row)

    # 最终保存一次缓存
    save_cache_to_file(cache)

    # 5. 分类并导出
    print("5. 正在对债券进行分类并排序...")
    final_df = pd.DataFrame(results)
    if final_df.empty:
        print("未发现符合条件的债券数据。")
        return

    # 定义列映射和顺序
    header_mapping = {
        '债券简称': '债券简称',
        '债券类型': '债券类型',
        '剩余天数': '剩余天数',
        '剩余期限_格式化': '剩余期限',
        '修正久期': '修正久期',
        '麦考利久期': '麦考利久期',
        '税后年收益率': '税后年收益率',
        '到期日': '到期日',
        '票面利率': '票面利率',
        '付息频率': '付息频率',
        '付息方式': '付息方式',
        '加权收益率': '加权收益率',
        '最新收益率': '最新收益率',
        '成交净价': '成交净价',
        '交易量': '成交额(亿元)',
        '成交时间': '成交时间'
    }
    
    cols_order = [
        '债券简称', '债券类型', '剩余天数', '剩余期限_格式化', '税后年收益率', '修正久期', '麦考利久期', 
        '到期日', '票面利率', '付息频率', '付息方式', '加权收益率', '最新收益率', '成交净价', '交易量', '成交时间'
    ]

    def process_sheet_df(df, sort_by='税后年收益率'):
        # 确保列存在并按序排列
        existing_cols = [c for c in cols_order if c in df.columns]
        df_sorted = df[existing_cols].copy()
        # 排序逻辑
        if sort_by in df_sorted.columns:
            df_sorted.sort_values(sort_by, ascending=False, inplace=True)
        # 重命名表头
        df_sorted.rename(columns=header_mapping, inplace=True)
        return df_sorted

    # 分类逻辑
    # 久期：短 (<= 0.5), 长 (>= 5), 中 (其他)
    short_bonds = final_df[final_df['修正久期'] <= 0.5]
    mid_bonds = final_df[(final_df['修正久期'] > 0.5) & (final_df['修正久期'] < 5)]
    long_bonds = final_df[final_df['修正久期'] >= 5]

    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # 先创建主页sheet
        create_homepage_sheet(writer, short_bonds, mid_bonds, long_bonds, header_mapping, cols_order)
        
        # 按照最新要求，不再区分免税和其他，仅按期限分 Sheet
        process_sheet_df(short_bonds).to_excel(writer, sheet_name='短期债券', index=False)
        process_sheet_df(mid_bonds).to_excel(writer, sheet_name='中期债券', index=False)
        process_sheet_df(long_bonds).to_excel(writer, sheet_name='长期债券', index=False)
        
        # 全部汇总：按交易量（成交额）降序排列
        process_sheet_df(final_df, sort_by='交易量').to_excel(writer, sheet_name='全部债券', index=False)

    print(f"6. 分析完成！结果已保存至: {output_file}")

if __name__ == "__main__":
    main()
