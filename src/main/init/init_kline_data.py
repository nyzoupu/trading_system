import os
import pandas as pd
from src.main.utils.sql_util import  MySQLUtil

# 设置工作目录为脚本所在路径
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

from complete_trading_system_v2_4_4h  import CompleteTradingSystem

if __name__ == '__main__':
    try:
        # 创建交易系统实例
        trading_system = CompleteTradingSystem()

        # 配置参数
        symbol = 'SUIUSDT'  # 交易对
        interval = '1m'  # 时间间隔
        # start_date = '2025-04-07 16:00:00'  # 开始日期
        start_date = '2025-08-04 00:00:00'  # 开始日期
        end_date = '2025-08-05 23:45:00'  # 结束日期（None表示到现在）

        MySQLUtil.init_pool()

        # 1. 获取历史数据
        df = trading_system.get_historical_data(symbol, interval, start_date, end_date)
        df["symbol"] = symbol  # 固定交易对
        df["interval"] = interval  # 固定周期
        #初始化才可以这样
        df['id'] = range(1, len(df) + 1)
        # 添加当前时间列
        df["create_datetime"] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        MySQLUtil.insert_dataframe('kline_data', df)

        if df is not None:
            print(f"📊 数据集包含 {len(df)} 条记录")

        else:
            print("❌ 处理失败")

    except Exception as e:
        print(f"❌ 处理过程中出错: {e}")
        import traceback

        traceback.print_exc()