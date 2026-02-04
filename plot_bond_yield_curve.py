
import akshare as ak
import pandas as pd
import matplotlib.pyplot as plt
import os
import time
import random
from datetime import datetime, timedelta

# 配置
CACHE_FILE = "china_bond_yield_cache.csv"
CURVE_NAME = "中债国债收益率曲线"
# 抓取的时间跨度限制（单次请求小于一年，建议300天）
FETCH_STEP_DAYS = 300
# 默认起始时间
DEFAULT_START_DATE = "2001-01-01"
# 默认截止时间（None 表示持续更新至今天）
DEFAULT_END_DATE = None

# 设置中文字体（Windows常用字体）
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False

def fetch_yield_data(start_date_str, end_date_str):
    """
    分段抓取收益率数据，带拟人化延迟
    """
    start_dt = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    all_data = []
    current_start = start_dt
    
    while current_start < end_dt:
        current_end = min(current_start + timedelta(days=FETCH_STEP_DAYS), end_dt)
        
        s_str = current_start.strftime("%Y%m%d")
        e_str = current_end.strftime("%Y%m%d")
        
        print(f"正在抓取 {s_str} 到 {e_str} 的数据...")
        
        try:
            # 拟人操作：随机延迟
            time.sleep(random.uniform(3.0, 6.0))
            
            df = ak.bond_china_yield(start_date=s_str, end_date=e_str)
            if not df.empty:
                # 只保留国债曲线
                df_cgb = df[df['曲线名称'] == CURVE_NAME]
                all_data.append(df_cgb)
                print(f"  成功获取 {len(df_cgb)} 条国债记录")
            
        except Exception as e:
            print(f"  抓取失败: {e}")
            time.sleep(10) # 失败后多等一会
            
        current_start = current_end + timedelta(days=1)
        
    if not all_data:
        return pd.DataFrame()
        
    return pd.concat(all_data, ignore_index=True)

def load_and_update_cache():
    """
    加载并更新缓存数据，支持双向更新
    """
    today = datetime.now().strftime("%Y-%m-%d")
    
    if os.path.exists(CACHE_FILE):
        print(f"加载现有缓存: {CACHE_FILE}")
        cache_df = pd.read_csv(CACHE_FILE)
        cache_df['日期'] = pd.to_datetime(cache_df['日期']).dt.strftime("%Y-%m-%d")
        
        # 获取缓存的时间范围
        min_date_str = cache_df['日期'].min()
        max_date_str = cache_df['日期'].max()
        
        new_data_list = [cache_df]
        needs_save = False

        # 1. 检查是否需要向后更新（补齐历史数据）
        if min_date_str > DEFAULT_START_DATE:
            history_end = (datetime.strptime(min_date_str, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
            print(f"缓存最早日期为 {min_date_str}，晚于设定起始日期 {DEFAULT_START_DATE}。")
            print(f"准备补全历史数据: {DEFAULT_START_DATE} 到 {history_end}...")
            hist_data = fetch_yield_data(DEFAULT_START_DATE, history_end)
            if not hist_data.empty:
                new_data_list.append(hist_data)
                needs_save = True

        # 2. 检查是否需要向前更新（同步最新数据）
        if max_date_str < today:
            update_start = (datetime.strptime(max_date_str, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            print(f"缓存最后日期为 {max_date_str}，早于今天 {today}。")
            print(f"准备同步最新数据: {update_start} 到 {today}...")
            latest_data = fetch_yield_data(update_start, today)
            if not latest_data.empty:
                new_data_list.append(latest_data)
                needs_save = True

        if needs_save:
            updated_df = pd.concat(new_data_list, ignore_index=True)
            # 统一日期格式为字符串，确保排序和去重一致
            updated_df['日期'] = pd.to_datetime(updated_df['日期']).dt.strftime("%Y-%m-%d")
            updated_df.drop_duplicates(subset=['日期', '曲线名称'], inplace=True)
            updated_df.sort_values('日期', inplace=True)
            updated_df.to_csv(CACHE_FILE, index=False)
            print(f"缓存已更新，当前共有 {len(updated_df)} 条记录。")
            return updated_df
        else:
            print("当前缓存已覆盖设定时间范围，无需更新。")
            return cache_df
    else:
        print(f"未发现缓存，开始从 {DEFAULT_START_DATE} 到 {today} 完整抓取...")
        full_data = fetch_yield_data(DEFAULT_START_DATE, today)
        if not full_data.empty:
            full_data['日期'] = pd.to_datetime(full_data['日期']).dt.strftime("%Y-%m-%d")
            full_data.sort_values('日期', inplace=True)
            full_data.to_csv(CACHE_FILE, index=False)
            print(f"全量抓取完成，共 {len(full_data)} 条记录。")
        return full_data

def plot_yield_curves(df):
    """
    绘制收益率曲线走势
    """
    if df.empty:
        print("无数据可供绘图")
        return
        
    # 转换日期格式
    df['日期'] = pd.to_datetime(df['日期'])
    df.set_index('日期', inplace=True)
    df.sort_index(inplace=True)
    
    # 选择关键期限进行绘制
    # 用户要求：仅绘制 1年 和 30年 两条曲线
    tenors = ['1年', '30年']
    available_tenors = [t for t in tenors if t in df.columns]
    
    plt.figure(figsize=(15, 8))
    
    for tenor in available_tenors:
        # 确保数据是数值型
        plot_data = pd.to_numeric(df[tenor], errors='coerce')
        plt.plot(plot_data, label=f'{tenor}国债收益率')
        
    plt.title(f'{CURVE_NAME} 历史走势图', fontsize=16)
    plt.xlabel('日期', fontsize=12)
    plt.ylabel('收益率 (%)', fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend(loc='best')
    
    # 保存图片
    plt.savefig('china_bond_yield_curve.png', dpi=300)
    print("收益率走势图已保存至: china_bond_yield_curve.png")
    
    # 也可以展示最新的收益率曲线（截面）
    latest_date = df.index.max()
    latest_row = df.loc[latest_date]
    
    # 提取所有包含“年”或“月”的列作为期限
    period_cols = [c for c in df.columns if '年' in c or '月' in c]
    # 排序期限：简单映射为月数
    def tenor_to_months(t):
        if '月' in t: return int(t.replace('月', ''))
        if '年' in t: return int(t.replace('年', '')) * 12
        return 999
    
    period_cols.sort(key=tenor_to_months)
    
    plt.figure(figsize=(12, 6))
    curve_data = [latest_row[c] for c in period_cols]
    plt.plot(period_cols, curve_data, marker='o', linestyle='-', linewidth=2)
    plt.title(f'最新国债收益率曲线 (日期: {latest_date.strftime("%Y-%m-%d")})', fontsize=14)
    plt.xlabel('期限', fontsize=12)
    plt.ylabel('收益率 (%)', fontsize=12)
    plt.grid(True, alpha=0.3)
    
    plt.savefig('latest_yield_curve.png', dpi=300)
    print("最新收益率曲线图已保存至: latest_yield_curve.png")
    
    plt.show()

def main():
    print("开始处理中债收益率曲线数据...")
    
    # 强制执行：先完整拉取并更新缓存
    df = load_and_update_cache()
    
    # 再次从缓存读取，确保数据是最完整的
    if os.path.exists(CACHE_FILE):
        df = pd.read_csv(CACHE_FILE)
        print(f"数据全部拉取并缓存完成，当前共有 {len(df)} 条记录。开始进行分析绘图...")
        plot_yield_curves(df)
    else:
        print("未能获取到数据，请检查网络或接口限制。")

if __name__ == "__main__":
    main()
