from datetime import date
import pandas as pd
from tqsdk import TqApi, TqAuth, TqBacktest, TargetPosTask, TqSim
from tqsdk.tafunc import ma

# 需要传入合约代码和bool条件，默认日期是2023-1-01到2023-12-31
def BARSLAST(symbol, condition, start_dt=date(2023, 1, 1), end_dt=date(2023, 12, 31)):
    """
    回测双均线策略并计算BARSLAST结果，使用日K线，仅考虑交易日周期。

    参数：
        symbol (str): 期货代码，例如 "DCE.m2401"。
        condition (callable or pd.Series): 布尔条件函数（接受klines返回布尔Series）或布尔Series。
        start_dt (date): 回测开始日期，默认为2023-01-01。
        end_dt (date): 回测结束日期，默认为2023-12-31。

    返回：
        pd.DataFrame: 包含所有回测记录的DataFrame，列包括：
            datetime: 时间
            price: 收盘价
            balance: 账户余额
            position: 手数
            barslast: BARSLAST值
            trade: 交易信息
    """
    # 策略参数
    SHORT = 12  # 短周期均线
    LONG = 26  # 长周期均线
    INITIAL_CAPITAL = 1_000_000  # 初始本金
    INVEST_RATIO = 0.2  # 每次投入本金的比例（20%）

    # 创建结果列表，用于存储每个时间点的数据
    results = []

    try:
        # 创建API实例，启用回测模式
        api = TqApi(account=TqSim(init_balance=1_000_000), backtest=TqBacktest(start_dt=start_dt, end_dt=end_dt),
                    auth=TqAuth("zyf_01", "@J8wrFVd5sHBcwF"))
        print(f"开始回测：{symbol}")

        # 动态获取合约乘数
        quote = api.get_quote(symbol)
        VOLUME_MULTIPLE = quote.volume_multiple
        if not VOLUME_MULTIPLE:
            raise ValueError(f"无法获取 {symbol} 的合约乘数")

        # 获取日K线数据
        data_length = LONG + 2
        klines = api.get_kline_serial(symbol, duration_seconds=24 * 60 * 60, data_length=data_length)

        # 检查索引重复
        if klines.index.duplicated().any():
            print("警告：K线索引包含重复值，尝试去重")
            klines = klines.loc[~klines.index.duplicated(keep='last')]

        # 创建目标持仓任务
        target_pos = TargetPosTask(api, symbol)

        # 获取账户信息
        account = api.get_account()

        # 创建历史条件记录字典
        condition_history = {}
        dt_list = []  # 存储时间顺序

        def calculate_barslast(history, dt_list, current_dt):
            """
            计算BARSLAST值：返回上一次True距离当前交易日的周期数

            参数:
                history (dict): 历史条件记录，键为datetime，值为布尔值
                dt_list (list): 按时间顺序排列的datetime列表
                current_dt: 当前datetime

            返回:
                int: BARSLAST值
            """
            # 如果当前条件为True，返回0
            if history[current_dt]:
                return 0

            # 找到当前日期在时间列表中的位置
            current_idx = dt_list.index(current_dt)

            # 从当前位置向前查找最近的True
            for i in range(current_idx - 1, -1, -1):
                if history[dt_list[i]]:
                    return current_idx - i

            # 如果没有找到True，返回-1
            return -1

        while True:
            if not api.wait_update():
                print("回测结束")
                break

            # 当K线更新时执行逻辑
            if api.is_changing(klines.iloc[-1], "datetime"):
                # 获取当前K线的时间和价格
                current_dt = pd.Timestamp(klines.datetime.iloc[-1], unit='ns')
                current_price = klines.close.iloc[-1]

                # 添加当前时间到时间列表
                if current_dt not in dt_list:
                    dt_list.append(current_dt)

                # 计算均线和仓位信息
                short_avg = ma(klines["close"], SHORT)
                long_avg = ma(klines["close"], LONG)
                current_capital = account.balance
                invest_amount = current_capital * INVEST_RATIO
                position = int(invest_amount / (current_price * VOLUME_MULTIPLE))

                # 更新条件历史记录
                if callable(condition):
                    condition_history[current_dt] = condition(klines).iloc[-1]
                else:
                    condition_history[current_dt] = condition.iloc[-1] if current_dt in condition.index else False

                # 计算BARSLAST
                bars_last = calculate_barslast(condition_history, dt_list, current_dt)

                # 双均线策略
                trade_action = None
                if len(short_avg) >= 2 and len(long_avg) >= 2:
                    if short_avg.iloc[-2] < long_avg.iloc[-2] and short_avg.iloc[-1] > long_avg.iloc[-1]:
                        trade_action = f"开多 {position} 手"
                        target_pos.set_target_volume(position)
                    elif short_avg.iloc[-2] > long_avg.iloc[-2] and short_avg.iloc[-1] < long_avg.iloc[-1]:
                        trade_action = f"开空 {position} 手"
                        target_pos.set_target_volume(-position)

                # 存储当前记录
                record = {
                    'datetime': current_dt,
                    'price': current_price,
                    'balance': current_capital,
                    'position': position,
                    'barslast': bars_last,
                    'trade': trade_action
                }
                results.append(record)

                # 打印日志
                print(f"时间: {current_dt}, "
                      f"价格: {current_price:.2f}, 净值: {current_capital:.2f}, "
                      f"手数: {position}, BARSLAST: {bars_last}, 交易: {trade_action or '无'}")

    except Exception as e:
        print(f"回测失败: {str(e)}")
        import traceback
        traceback.print_exc()

    finally:
        # 确保API关闭
        if 'api' in locals():
            api.close()

    # 将结果转换为DataFrame并返回
    if results:
        return pd.DataFrame(results)
    else:
        return pd.DataFrame(columns=['datetime', 'price', 'balance', 'position', 'barslast', 'trade'])


if __name__ == "__main__":
    # 示例布尔条件：收盘价等于3748.00
    condition_func = lambda klines: klines["close"] == 3748.00

    # 示例布尔条件：检测金叉（短期均线从下方穿过长期均线）
    def golden_cross_condition(klines):
        # 计算短期和长期均线
        short_avg = ma(klines["close"], 12)  # 短周期均线
        long_avg = ma(klines["close"], 26)  # 长周期均线

        # 创建一个与klines长度相同的布尔Series，初始值全为False
        result = pd.Series(False, index=klines.index)

        # 检查是否有足够的数据来判断金叉
        if len(short_avg) >= 2 and len(long_avg) >= 2:
            # 对于最新的数据点，检查是否出现金叉
            result.iloc[-1] = (short_avg.iloc[-2] < long_avg.iloc[-2] and
                               short_avg.iloc[-1] > long_avg.iloc[-1])

        return result

    # 示例布尔条件：检测死叉
    def death_cross_condition(klines):
        # 计算短期和长期均线
        short_avg = ma(klines["close"], 12)  # 短周期均线
        long_avg = ma(klines["close"], 26)  # 长周期均线

        # 创建一个与klines长度相同的布尔Series，初始值全为False
        result = pd.Series(False, index=klines.index)

        # 检查是否有足够的数据来判断死叉
        if len(short_avg) >= 2 and len(long_avg) >= 2:
            # 对于最新的数据点，检查是否出现死叉
            result.iloc[-1] = (short_avg.iloc[-2] > long_avg.iloc[-2] and
                               short_avg.iloc[-1] < long_avg.iloc[-1])

        return result


    # 运行模块并获取结果
    result_df = BARSLAST(symbol="DCE.m2401", condition=condition_func)

    # 显示结果
    print("\n回测结果DataFrame:")
    print(result_df.head())

    # 保存结果到CSV文件
    result_df.to_csv("barslast_results.csv", index=False)
    print("结果已保存到 barslast_results.csv")
