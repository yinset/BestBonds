# 存款去哪儿？ 🏦💰
本项目致力于解决银行存款不知道放哪的问题。通过直接参与债券市场，您可以跳过银行定期存款这个不灵活、提前支取就无效且单利利息的收费中介，拥抱复利、提前支取按天计息的世界，参照自己的需求获取更高收益！
> **PS：在抓取分析前查询 https://www.chinamoney.com.cn/chinese/mkdatabond 看是否已经刷新交易量数据（一般在晚上八点左右刷新），不然会自动调用历史最近交易日数据。**
>
> **久期约等于债券受利率变化的杠杆倍数。**
> 
> **久期与时间、利率的关系参考** 作者：https://github.com/ClamJom
<img width="640" height="516" alt="duration_risk_heatmap" src="https://github.com/user-attachments/assets/f572a1a6-cc92-4059-a1fb-57fce6053965" />


---

## **结果示例** 
2026-02-04的运行结果
<img width="1169" height="1352" alt="image" src="https://github.com/user-attachments/assets/d3114ab7-2ab7-4672-a2d6-4e9be44f3c63" />


---

## **项目核心功能** 🚀

1.  **自动化行情抓取**：利用 `akshare` 获取最新的债券成交行情，实时掌握市场脉搏。
2.  **深度元数据分析**：自动从中国货币网（ChinaMoney）抓取债券的详细信息（起息日、到期日、票面利率、付息频率等）。
3.  **专业指标计算**：
    *   **剩余期限**：精确计算到天，并转换为易读的“X年Y天”格式。
    *   **收益率分析**：基于加权收益率和最新收益率进行评估。
4.  **收益率曲线可视化**：
    *   绘制 1 年期及 30 年期国债的历史收益率走势图。
    *   生成最新的全期限国债收益率曲线图，辅助决策。
5.  **智能缓存机制**：内置完善的 CSV 缓存系统，减少重复抓取，规避反爬风险。

---

## **快速开始** 🛠️

### **1. 环境准备**
确保您的电脑已安装 Python 3.x，并安装必要的依赖库：

```bash
pip install akshare pandas requests tqdm matplotlib openpyxl
```

### **2. 运行债券批量分析**
执行以下脚本，程序将自动拉取成交量较大的债券并计算各项指标：

```bash
python batch_bond_analysis.py
```
*   **输出结果**：将生成 `bond_analysis_results_YYYY-MM-DD.xlsx` 供查看。

### **3. 查看收益率走势**
tools文件夹运行绘图脚本，直观查看国债收益率变化：

```bash
python plot_bond_yield_curve.py
```
*   **输出结果**：
    *   `china_bond_yield_curve.png`：1年/30年国债历史走势。
    *   `latest_yield_curve.png`：当前时点的收益率曲线形状。

---

## **投资提醒** ⚠️

*   **利率风险**：债券价格与利率成反比，久期越长，受利率波动影响越大。

---

**使用它！参照自己需求获取更高收益吧！**
