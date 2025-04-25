# barslast_util.py

import pandas as pd
import numpy as np
from datetime import date, datetime
from tqsdk import TqApi, TqAuth, TqBacktest, TqSim, BacktestFinished # 导入 TQSDK 相关组件
from tqsdk.tafunc import ma # 导入 ma 函数
import os
from dotenv import load_dotenv # 用于加载 .env 文件
from typing import Optional

# ==================================================
# BARSLAST 函数定义 (保持不变，这是核心工具函数)
# ==================================================
def BARSLAST(condition_series: pd.Series) -> int:
    """
    计算距离上一次条件为 True 的 K 线周期数。
    Args:
        condition_series: 布尔型的 Pandas Series。
    Returns:
        int: 距离上一次 True 的周期数 (>=1)，或 -1 (未找到或序列太短)。
    """
    if not isinstance(condition_series, pd.Series) or condition_series.dtype != bool:
        raise TypeError("BARSLAST 输入必须是布尔型的 Pandas Series。")
    n = len(condition_series)
    if n < 2:
        return -1
    current_iloc = n - 1
    series_before_now = condition_series.iloc[:current_iloc]
    true_indices = np.where(series_before_now)[0]
    if len(true_indices) == 0:
        return -1
    else:
        last_true_iloc = true_indices[-1]
        return current_iloc - last_true_iloc

# ==================================================
# 当 barslast_util.py 被直接运行时，执行以下测试代码
# ==================================================
if __name__ == "__main__":
    print("--- 开始独立测试 BARSLAST 函数 (使用 TQPY 回测数据流) ---")
    print("    本测试将使用与提供的 backtest.py 相同的参数配置回测环境。")

    # --- 参数设置 (直接从 backtest.py 复制并确认) ---
    SHORT = 12  # 短周期均线
    LONG = 26  # 长周期均线
    SYMBOL = "DCE.m2401"  # 合约代码 (与 backtest.py 一致, 请确保有效)
    KLINE_DURATION_SECONDS = 5 * 60 # K线周期：5分钟 (与 backtest.py 一致)
    START_DATE = date(2023, 1, 1)   # 回测开始日期 (与 backtest.py 一致)
    END_DATE = date(2023, 12, 31)  # 回测结束日期 (与 backtest.py 一致)

    # --- 加载认证信息 (从 .env 文件) ---
    if not load_dotenv():
        print("警告：未能找到或加载 .env 文件。")
    kq_account = os.getenv("KQ_ACCOUNT")
    kq_password = os.getenv("KQ_PASSWORD")
    if not kq_account or not kq_password:
        print("错误：请在 .env 文件中设置 KQ_ACCOUNT 和 KQ_PASSWORD 以进行 TqAuth 认证。")
        exit() # 缺少认证信息，无法继续

    # --- TQSDK 初始化 (使用与 backtest.py 一致的参数) ---
    api = None # 初始化 api 变量
    print(f"\n测试参数: 合约={SYMBOL}, 周期={KLINE_DURATION_SECONDS}秒, 时间={START_DATE} to {END_DATE}")
    print(f"认证方式: TqAuth (账户: {kq_account})")

    try:
        # 注意：这里我们不关心初始资金，所以可以不使用 TqSim
        api = TqApi(backtest=TqBacktest(start_dt=START_DATE, end_dt=END_DATE),
                    auth=TqAuth(kq_account, kq_password))
        print("TqApi 初始化完成，开始模拟数据加载和 BARSLAST 测试...")

        # --- 获取 K 线数据流 ---
        # data_length 需要足够长以计算指标和处理 shift
        data_length = LONG + 5
        klines = api.get_kline_serial(SYMBOL, duration_seconds=KLINE_DURATION_SECONDS, data_length=data_length)

        # --- 主循环 (仅用于数据更新和 BARSLAST 测试) ---
        k_count = 0 # K 线计数器
        print_freq = 50 # 每 50 根 K 线打印一次 BARSLAST 结果 (5分钟线，减少打印频率)

        while True:
            api.wait_update()

            # 检查是否有新 K 线，并确保数据足够
            if not klines.empty and api.is_changing(klines.iloc[-1], "datetime"):
                k_count += 1
                if len(klines) < LONG + 2: # 保证能计算长均线和 .shift(1)
                    continue

                # --- 计算必要指标 (仅用于生成测试条件) ---
                short_avg = ma(klines["close"], SHORT)
                long_avg = ma(klines["close"], LONG)

                # 确保指标有效
                if pd.isna(short_avg.iloc[-1]) or pd.isna(long_avg.iloc[-1]) or \
                   pd.isna(short_avg.iloc[-2]) or pd.isna(long_avg.iloc[-2]):
                    continue

                # --- 生成示例条件序列 ---
                # 需要处理 shift(1) 可能在开头产生的 NaN
                condition_gc = (short_avg > long_avg) & (short_avg.shift(1) <= long_avg.shift(1))
                condition_dc = (short_avg < long_avg) & (short_avg.shift(1) >= long_avg.shift(1))
                condition_close_above_long = klines["close"] > long_avg
                condition_short_above_long = short_avg > long_avg # 短均线是否在长均线上方

                # --- 调用 BARSLAST 函数 (核心测试点) ---
                # 使用 .fillna(False) 处理因 shift 产生的 NaN
                bars_gc = BARSLAST(condition_gc.fillna(False))
                bars_dc = BARSLAST(condition_dc.fillna(False))
                bars_cal = BARSLAST(condition_close_above_long.fillna(False))
                bars_sal = BARSLAST(condition_short_above_long.fillna(False)) # 测试短上长条件

                # --- 按频率打印结果 ---
                if k_count % print_freq == 0:
                    current_dt_nano = klines.iloc[-1]["datetime"]
                    current_dt = pd.to_datetime(current_dt_nano, unit='ns', utc=True).tz_convert('Asia/Shanghai')
                    print(f"\n{current_dt} (K线计数: {k_count}, 序列长度: {len(klines)}) BARSLAST 测试:")
                    print(f"  收盘价: {klines.close.iloc[-1]:.2f}, MA{SHORT}: {short_avg.iloc[-1]:.2f}, MA{LONG}: {long_avg.iloc[-1]:.2f}")
                    print(f"  BARSLAST(金叉): {bars_gc}")
                    print(f"  BARSLAST(死叉): {bars_dc}")
                    print(f"  BARSLAST(收盘价 > MA{LONG}): {bars_cal}")
                    print(f"  BARSLAST(MA{SHORT} > MA{LONG}): {bars_sal}")

            # --- 这里不执行任何 TargetPosTask 或交易逻辑 ---

    except BacktestFinished:
        print("\n--- 回测数据流结束，BARSLAST 测试完成 ---")

    except Exception as e:
        print(f"\n测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()

    finally:
        # --- 清理 ---
        if api: # 检查 api 是否已成功初始化
            api.close()
            print("TqApi 连接已关闭。")
        print("--- BARSLAST 函数独立测试结束 ---")