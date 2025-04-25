# data_fetcher.py

import pandas as pd
from tqsdk import TqApi, TqBacktest, BacktestFinished
from typing import Optional
import warnings
from datetime import datetime # <<---- 1. 导入 datetime 模块

# 忽略 TQSDK 内部操作可能引发的特定 pandas 警告
warnings.filterwarnings("ignore", category=FutureWarning, module="tqsdk.sim")

# 定义常用的 K 线周期常量 (单位：秒)
DURATION_DAILY = 24 * 60 * 60
DURATION_HOUR = 60 * 60
DURATION_MINUTE = 60

def fetch_and_prepare_futures_data(
    symbol: str,
    start_dt_str: str, # 重命名参数以示区分
    end_dt_str: str,   # 重命名参数以示区分
    kline_duration: int = DURATION_DAILY
) -> Optional[pd.DataFrame]:
    """
    使用 TQPY 从 TQSDK 获取指定期货合约和日期范围的历史 K 线数据，并进行预处理。

    Args:
        symbol (str): 期货合约代码。
        start_dt_str (str): 开始日期字符串 (格式 'YYYY-MM-DD')。
        end_dt_str (str): 结束日期字符串 (格式 'YYYY-MM-DD')。
        kline_duration (int): K 线周期，单位为秒。

    Returns:
        Optional[pd.DataFrame]: 处理后的 DataFrame 或 None。
    """
    required_columns = ['datetime', 'open', 'close']
    api = None

    print(f"正在使用 TQPY 获取 {symbol} 从 {start_dt_str} 到 {end_dt_str} 的 K 线数据...")
    print(f"K 线周期: {kline_duration} 秒")

    try:
        # --- >>>> 2. 在初始化 TqApi 之前，转换日期字符串 <<<< ---
        try:
            # 尝试按 'YYYY-MM-DD' 格式解析日期字符串
            start_dt_obj = datetime.strptime(start_dt_str, '%Y-%m-%d').date() # 转为 date 对象
            end_dt_obj = datetime.strptime(end_dt_str, '%Y-%m-%d').date()     # 转为 date 对象
            # TqBacktest 接受 date 或 datetime 对象均可
            print(f"日期字符串成功转换为 date 对象: 开始={start_dt_obj}, 结束={end_dt_obj}")
        except ValueError:
            print(f"错误：无法解析日期字符串 '{start_dt_str}' 或 '{end_dt_str}'。")
            print("请确保格式为 'YYYY-MM-DD'。")
            return None
        # --- >>>> 日期转换结束 <<<< ---

        # 1. 初始化 TqApi 和 TqBacktest (使用转换后的 date 对象)
        api = TqApi(backtest=TqBacktest(start_dt=start_dt_obj, end_dt=end_dt_obj), auth="免费用户")
        print("TqApi 初始化成功。")

        # 2. 获取 K 线序列对象
        klines = api.get_kline_serial(symbol, duration=kline_duration, data_length=200)
        print(f"已请求 K 线序列对象: {symbol}, 周期={kline_duration}秒")

        # 3. 等待数据加载完成
        print("正在等待 TQSDK 加载历史数据...")
        while True:
            api.wait_update()
            if api.is_backtest_finished():
                print("数据加载过程完成 (收到 BacktestFinished 信号)。")
                break

        # --- 数据已加载完毕 ---
        if klines is None or klines.empty:
            print(f"错误：未能获取到 {symbol} 的 K 线数据。")
            return None

        print(f"成功获取 {len(klines)} 条 K 线原始数据。")

        # 4. 筛选和处理数据
        df_processed = pd.DataFrame({
            'datetime': klines['datetime'],
            'open': klines['open'],
            'close': klines['close']
        })

        # 5. 转换 datetime 列并处理时区
        df_processed['datetime'] = pd.to_datetime(df_processed['datetime'], unit='ns', utc=True)
        try:
            df_processed['datetime'] = df_processed['datetime'].dt.tz_convert('Asia/Shanghai')
            print("时间已转换为上海时区 (Asia/Shanghai)。")
        except Exception as tz_err:
            print(f"警告：时区转换为上海时间失败 ({tz_err})，将使用 UTC 时间。")

        # 6. 设置索引并重命名
        df_processed = df_processed.set_index('datetime')
        df_processed.index.name = 'trade_date'

        # 7. 排序
        df_processed = df_processed.sort_index(ascending=True)

        print("数据处理完成，准备返回 DataFrame。")
        return df_processed

    # ... [后面的 except 和 finally 块保持不变] ...
    except BacktestFinished:
        print("警告：在数据完全处理前收到 BacktestFinished 异常。")
        if 'klines' in locals() and klines is not None and not klines.empty:
             print("尝试处理已获取的部分数据...")
             df_processed = pd.DataFrame({
                 'datetime': klines['datetime'], 'open': klines['open'], 'close': klines['close']
             })
             df_processed['datetime'] = pd.to_datetime(df_processed['datetime'], unit='ns', utc=True)
             try: df_processed['datetime'] = df_processed['datetime'].dt.tz_convert('Asia/Shanghai')
             except Exception: pass
             df_processed = df_processed.set_index('datetime')
             df_processed.index.name = 'trade_date'
             df_processed = df_processed.sort_index(ascending=True)
             return df_processed
        else:
            print("错误：回测提前结束且未获取到有效数据。")
            return None
    except Exception as e:
        import traceback
        print(f"获取或处理 TQPY 数据时发生严重错误: {e}")
        print("详细错误追踪信息:")
        print(traceback.format_exc())
        return None
    finally:
        if api:
            api.close()
            print("TqApi 连接已关闭。")


# --- 模块测试代码 (也需要修改传入的参数名) ---
if __name__ == "__main__":
    print("--- 开始测试 data_fetcher 模块 (使用 TQPY 获取期货数据) ---")
    TEST_SYMBOL = 'SHFE.rb2410'
    TEST_START_STR = '2023-06-01' # 使用字符串
    TEST_END_STR = '2023-12-31'   # 使用字符串
    TEST_DURATION = DURATION_DAILY
    print("\n提示：请确保 TQSDK 环境配置正确...")
    print(f"测试参数：合约={TEST_SYMBOL}, 开始={TEST_START_STR}, 结束={TEST_END_STR}, 周期=日线")

    # 调用核心函数进行测试 (传入字符串)
    futures_data = fetch_and_prepare_futures_data(
        symbol=TEST_SYMBOL,
        start_dt_str=TEST_START_STR, # 使用修改后的参数名
        end_dt_str=TEST_END_STR,   # 使用修改后的参数名
        kline_duration=TEST_DURATION
    )

    # ... [后面的检查和打印逻辑不变] ...
    if futures_data is not None:
        print("\n测试成功：数据获取并处理完成。")
        print("数据信息 (Info):")
        futures_data.info()
        print("\n数据前 5 行 (Head):")
        print(futures_data.head())
        print("\n数据后 5 行 (Tail):")
        print(futures_data.tail())
    else:
        print("\n测试失败：未能获取或处理数据。请检查之前的错误信息。")
    print("--- 结束测试 data_fetcher 模块 ---")