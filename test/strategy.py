# strategy.py

import pandas as pd
from tqsdk import TqApi, TargetPosTask, BacktestFinished
from tqsdk.tafunc import ma # 导入移动平均线函数
from datetime import datetime

def run_dual_ma_strategy(api: TqApi, symbol: str, short_period: int, long_period: int, volume: int, kline_duration: int):
    """
    执行双均线策略的核心逻辑。

    Args:
        api (TqApi): TqApi 实例。
        symbol (str): 交易的合约代码。
        short_period (int): 短周期均线窗口。
        long_period (int): 长周期均线窗口。
        volume (int): 每次交易的目标手数 (正数表示做多，负数表示做空)。
        kline_duration (int): K 线周期，单位为秒。
    """
    print(f"启动双均线策略: 合约={symbol}, 短周期={short_period}, 长周期={long_period}, 手数={volume}, K线周期={kline_duration}秒")

    # 计算需要获取的 K 线数据长度，至少需要长周期+2才能计算当前和上一根均线值
    # 稍微加一点 buffer 更安全
    data_length = long_period + 5

    # 获取 K 线序列对象
    # 注意：get_kline_serial 的参数是 duration, 不是 duration_seconds
    # klines = api.get_kline_serial(symbol, duration=kline_duration, data_length=data_length)
    # 修正后的代码
    klines = api.get_kline_serial(symbol, duration_seconds=kline_duration, data_length=data_length)
    # 创建目标持仓管理任务实例
    target_pos = TargetPosTask(api, symbol)

    try:
        while True:
            # 等待数据更新或回测结束信号
            api.wait_update()

            # 检查是否有新的 K 线生成（检查最后一根 K 线的 datetime 是否变化）
            # 确保 klines 不为空再访问 iloc[-1]
            if not klines.empty and api.is_changing(klines.iloc[-1], "datetime"):
                # 确保 K 线数据长度足够计算最长周期的均线
                if len(klines) < long_period + 1: # 需要 long_period+1 根才能计算出长周期均线
                    continue

                # 计算短周期和长周期移动平均线 (使用收盘价)
                short_avg = ma(klines["close"], short_period)
                long_avg = ma(klines["close"], long_period)

                # 确保均线值已有效计算出来 (非 NaN)
                # 至少需要比较当前和上一根K线的均线值
                if pd.isna(short_avg.iloc[-1]) or pd.isna(long_avg.iloc[-1]) or \
                   pd.isna(short_avg.iloc[-2]) or pd.isna(long_avg.iloc[-2]):
                    continue # 如果均线还未计算出来，则跳过

                current_dt_nano = klines.iloc[-1]["datetime"]
                current_dt = pd.to_datetime(current_dt_nano, unit='ns', utc=True).tz_convert('Asia/Shanghai')
                print(f"\n{current_dt} 新 K 线:")
                print(f"  Close: {klines.iloc[-1]['close']:.2f}")
                print(f"  MA{short_period}: {short_avg.iloc[-1]:.2f} (上一周期: {short_avg.iloc[-2]:.2f})")
                print(f"  MA{long_period}: {long_avg.iloc[-1]:.2f} (上一周期: {long_avg.iloc[-2]:.2f})")

                # 金叉判断：短均线上穿长均线
                # 条件：当前短均线 > 当前长均线  并且  上一周期短均线 <= 上一周期长均线
                if short_avg.iloc[-1] > long_avg.iloc[-1] and short_avg.iloc[-2] <= long_avg.iloc[-2]:
                    print(f"*** {current_dt} 金叉信号 ***")
                    print(f"  设置目标持仓为: {volume} 手")
                    target_pos.set_target_volume(volume) # 设置目标持仓为 volume 手 (做多)

                # 死叉判断：短均线下穿长均线
                # 条件：当前短均线 < 当前长均线  并且  上一周期短均线 >= 上一周期长均线
                elif short_avg.iloc[-1] < long_avg.iloc[-1] and short_avg.iloc[-2] >= long_avg.iloc[-2]:
                    print(f"--- {current_dt} 死叉信号 ---")
                    # 如果 volume 是正数（表示做多），死叉时应平仓，目标设为 0
                    # 如果 volume 是负数（表示做空），可以考虑死叉时开空仓（目标设为 -volume）
                    # 这里我们假设 volume 代表多头手数，死叉时平多仓
                    if volume > 0:
                        print(f"  设置目标持仓为: 0 手")
                        target_pos.set_target_volume(0)
                    # 如果需要根据死叉做空，可以取消下面注释
                    # else:
                    #     print(f"  设置目标持仓为: {volume} 手") # volume 本身为负
                    #     target_pos.set_target_volume(volume)

            # 可以在这里添加其他逻辑，比如定时打印账户信息等
            # if api.is_changing(api.get_account()):
            #     account = api.get_account()
            #     position = api.get_position(symbol)
            #     print(f"账户更新: Balance={account.balance:.2f}, Position={position.pos}")

    except BacktestFinished:
        print("策略模块收到回测结束信号。")
        # 回测结束时，可以在主程序中获取最终结果

    except Exception as e:
        print(f"策略模块运行时发生错误: {e}")
        import traceback
        traceback.print_exc()

    print("策略函数执行完毕。")