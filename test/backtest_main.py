# backtest_main.py

from tqsdk import TqApi, TqBacktest, BacktestFinished, TqAuth
from datetime import date, datetime
from strategy import run_dual_ma_strategy
import os                 # 导入 os 模块
from dotenv import load_dotenv # 导入 load_dotenv

# --- 加载环境变量 ---
# load_dotenv 会查找当前目录或父目录中的 .env 文件并加载
if not load_dotenv():
    print("警告：未能找到或加载 .env 文件。请确保 .env 文件存在。")

# --- 回测参数配置 ---
FUTURES_SYMBOL = 'SHFE.cu1902'
START_DT_STR = '2023-01-01'
END_DT_STR = '2023-12-31' # 或 '2024-01-01'
SHORT_PERIOD = 12
LONG_PERIOD = 26
TRADE_VOLUME = 1
KLINE_DURATION = 24 * 60 * 60

# --- 从环境变量获取账户密码 ---
# 使用 os.getenv 读取 .env 文件加载的环境变量
kq_account = os.getenv("KQ_ACCOUNT")
kq_password = os.getenv("KQ_PASSWORD")

# 检查账户密码是否已设置
if not kq_account or not kq_password:
    print("错误：未能从 .env 文件中获取 KQ_ACCOUNT 或 KQ_PASSWORD。")
    print("请确保 .env 文件存在，并包含正确的快期账户和密码。")
    exit() # 如果缺少账户信息则退出

# --- 初始化 TQSDK ---
api = None

try:
    # 转换日期字符串为 date 对象
    try:
        start_dt_obj = datetime.strptime(START_DT_STR, '%Y-%m-%d').date()
        end_dt_obj = datetime.strptime(END_DT_STR, '%Y-%m-%d').date()
    except ValueError:
        print("错误：无法解析日期字符串。请确保格式为 'YYYY-MM-DD'。")
        exit()

    print(f"初始化 TqApi 进行回测...")
    print(f"合约: {FUTURES_SYMBOL}")
    print(f"周期: {start_dt_obj} 到 {end_dt_obj}")
    print(f"策略: 双均线 ({SHORT_PERIOD}/{LONG_PERIOD}), 手数={TRADE_VOLUME}, 周期={KLINE_DURATION}秒")
    print(f"认证方式: TqAuth (使用 .env 文件中的账户: {kq_account})") # 确认使用了 TqAuth

    # --- >>>> 修改 TqApi 初始化，使用 TqAuth <<<< ---
    api = TqApi(
        backtest=TqBacktest(start_dt=start_dt_obj, end_dt=end_dt_obj),
        # 使用从 .env 文件加载的账户和密码进行认证
        auth=TqAuth(kq_account, kq_password)
    )
    # --- >>>> 修改结束 <<<< ---

    # --- 运行策略 ---
    run_dual_ma_strategy(
        api=api,
        symbol=FUTURES_SYMBOL,
        short_period=SHORT_PERIOD,
        long_period=LONG_PERIOD,
        volume=TRADE_VOLUME,
        kline_duration=KLINE_DURATION
    )

except BacktestFinished:
    print("\n==================== 回测结束 ====================")
    if api:
        account = api.get_account()
        trades = api.get_trade_records()
        print(f"最终账户权益: {account.balance:.2f}") # 注意：回测模式下 balance 可能需要额外计算
        # TQSDK 回测的账户对象可能不直接反映基于历史数据的精确资金曲线
        # 可能需要根据 trades 自行计算详细的回测指标
        print(f"总交易次数: {len(trades)}")
        # ... [打印交易记录等] ...
    else:
        print("API 未成功初始化，无法获取回测结果。")

except Exception as e:
    print(f"\n回测主程序运行时发生未处理错误: {e}")
    import traceback
    traceback.print_exc()

finally:
    # --- 清理工作 ---
    if api:
        api.close()
        print("TqApi 连接已关闭。")
    print("回测程序结束。")