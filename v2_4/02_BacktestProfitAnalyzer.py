import pandas as pd

def calculate_daily_pnl(csv_path, initial_btc=1.0, start_date=None, price_threshold=0.02):
    """
    根据CSV文件中的行情和label数据，模拟买卖，计算每日资产和收益等指标

    参数：
    - csv_path: CSV文件路径，需包含open_time, close, label字段
    - initial_btc: 初始持有的BTC数量（默认为1）
    - start_date: 筛选起始日期，格式'YYYY-MM-DD'，默认为None（从头开始）
    - price_threshold: 价格波动阈值，当持有BTC且当前价格较买入价下跌超过此阈值时补仓（默认为0.02，即2%）

    返回：
    - 处理后含每日资产、收益等字段的DataFrame
    """
    # 读取CSV，解析open_time为datetime
    df = pd.read_csv(csv_path, parse_dates=['open_time'])

    # 过滤起始日期
    if start_date:
        df = df[df['open_time'] >= pd.to_datetime(start_date)].copy()

    # 重置索引，确保连续整数索引
    df.reset_index(drop=True, inplace=True)

    if df.empty:
        raise ValueError("数据为空，请检查CSV文件路径或起始日期。")

    # 按时间排序
    df.sort_values('open_time', inplace=True)
    df.reset_index(drop=True, inplace=True)

    # 初始化持仓与资金状态
    btc_qty = initial_btc
    usdt_qty = 0.0
    buy_price = None  # 记录买入价格

    asset_list = []
    daily_return_list = []
    cumulative_return_list = []
    cumulative_return_rate_list = []

    # 初始资产价值（用首条收盘价计算）
    initial_asset = initial_btc * df.loc[0, 'close']
    prev_asset = initial_asset
    cumulative_return = 0.0

    for idx, row in df.iterrows():
        price = row['close']
        label = row['label']

        # label规则：
        # 0 - 持有，不操作
        # 1 - 买入（用所有USDT买BTC）
        # 2 - 卖出（卖出所有BTC换USDT）

        if label == 1:
            # 买入：全部USDT换BTC（前提有USDT）
            if usdt_qty > 0:
                if btc_qty == 0:
                    # 没有持仓，直接买入
                    btc_bought = usdt_qty / price
                    btc_qty += btc_bought
                    usdt_qty = 0.0
                    buy_price = price
                else:
                    # 已有持仓，检查价格差异
                    if buy_price is not None:
                        price_diff = (price - buy_price) / buy_price
                        # 如果当前价格低于买入价超过阈值，补仓
                        if price_diff < -price_threshold:
                            btc_bought = usdt_qty / price
                            btc_qty += btc_bought
                            usdt_qty = 0.0
                            buy_price = price  # 更新买入价为当前补仓价格
        elif label == 2:
            # 卖出：全部BTC换USDT
            if btc_qty > 0:
                usdt_qty += btc_qty * price
                btc_qty = 0.0
                buy_price = None  # 卖出后重置买入价

        # 当前总资产 = BTC市值 + USDT现金
        current_asset = btc_qty * price + usdt_qty

        # 计算日收益（资产增减）
        daily_return = current_asset - prev_asset
        cumulative_return += daily_return
        cumulative_return_rate = cumulative_return / initial_asset

        # 记录
        asset_list.append(current_asset)
        daily_return_list.append(daily_return)
        cumulative_return_list.append(cumulative_return)
        cumulative_return_rate_list.append(cumulative_return_rate)

        prev_asset = current_asset

    # 添加结果列
    df['asset'] = asset_list
    df['daily_return'] = daily_return_list
    df['cumulative_return'] = cumulative_return_list
    df['cumulative_return_rate'] = cumulative_return_rate_list

    return df


if __name__ == "__main__":
    input_csv = "complete_dataset_SUIUSDT_4h_squeeze_luxalgo_advanced1.csv"
    start_date = "2023-01-01"  # 根据你的示例数据起始时间设定
    initial_btc = 1.0

    result_df = calculate_daily_pnl(input_csv, initial_btc, start_date)

    # 保存结果到新CSV
    result_df.to_csv(input_csv+"_pnl_result.csv", index=False)
    print("计算完成，结果已保存至  "+input_csv)