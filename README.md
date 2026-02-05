# 存款去哪儿？ 🏦💰

本项目致力于解决银行存款不知道放哪的问题。通过直接参与债券市场，您可以跳过银行定期存款这不方便且收费的中介，参照自己的波动需求获取更高的收益！

> **PS：久期约等于无风险利率的杠杆率。久期长的债券投资有风险，仓位设置需谨慎。在抓取分析前查询https://www.chinamoney.com.cn/chinese/mkdatabond
> 看是否已经刷新交易量数据（一般在晚上八点左右刷新），否则会抓不到数据。**

---

## **项目核心功能** 🚀

1.  **自动化行情抓取**：利用 `akshare` 获取最新的债券成交行情，实时掌握市场脉搏。
2.  **深度元数据分析**：自动从中国货币网（ChinaMoney）抓取债券的详细信息（起息日、到期日、票面利率、付息频率等）。
3.  **专业指标计算**：
    *   **剩余期限**：精确计算到天，并转换为易读的“X年Y天”格式。
    *   **久期计算**：自动计算麦考利久期（Macaulay Duration）和修正久期（Modified Duration），帮助评估利率风险。
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
*   **配置项**：可在脚本顶部修改 `SETTLEMENT_DATE`（结算日）或 `CONCURRENT_THREADS`（并发数）。

### **3. 查看收益率走势**
运行绘图脚本，直观查看国债收益率变化：

```bash
python plot_bond_yield_curve.py
```
*   **输出结果**：
    *   `china_bond_yield_curve.png`：1年/30年国债历史走势。
    *   `latest_yield_curve.png`：当前时点的收益率曲线形状。

---

## **项目结构** 📂

*   [batch_bond_analysis.py](file:///c:/Users/11825/OneDrive/BestBonds/batch_bond_analysis.py)：核心分析逻辑，负责抓取数据、计算久期并导出 Excel。
*   [plot_bond_yield_curve.py](file:///c:/Users/11825/OneDrive/BestBonds/plot_bond_yield_curve.py)：绘图工具，展示国债收益率的历史趋势与现状。
*   `bond_metadata_cache.csv`：债券元数据缓存。
*   `china_bond_yield_cache.csv`：中债收益率历史数据缓存。

---

## **投资提醒** ⚠️

*   **利率风险**：债券价格与利率成反比，久期越长，受利率波动影响越大。
*   **信用风险**：本项目侧重于国债及高流动性债券，请在投资前仔细甄别发行人信用。
*   **流动性风险**：虽然脚本筛选了成交量大的债券，但实际交易中仍需注意市场深度。

---

**使用它！参照自己需求获取更高收益吧！**
