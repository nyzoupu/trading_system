import os
import requests
import pandas as pd
import numpy as np
import io
from datetime import datetime, timedelta, timezone
from DrissionPage import SessionPage
import json
import time
from scipy import stats
import warnings
from sklearn.preprocessing import MinMaxScaler

warnings.filterwarnings('ignore')

# 设置工作目录为脚本所在路径
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)


class CompleteTradingSystem:
    """完整的数字货币量化交易系统"""

    def __init__(self):
        self.base_url = 'https://api.binance.com/api/v3/klines'

        # 原油API优化配置 (Yahoo Finance)
        self.oil_batch_config = {
            'max_records_per_request': 5000,  # 每次请求最大记录数
            'min_request_interval': 0.5,  # 最小请求间隔(秒) - Yahoo Finance相对宽松
            'max_retries': 3,  # 最大重试次数
            'max_batches': 100,  # 最大批次数限制
            'timeout': 60  # 请求超时时间(秒)
        }

    def update_oil_batch_config(self, **kwargs):
        """更新原油API批量获取配置

        参数:
            max_records_per_request: 每次请求最大记录数 (默认5000)
            min_request_interval: 最小请求间隔秒数 (默认0.5)
            max_retries: 最大重试次数 (默认3)
            max_batches: 最大批次数限制 (默认100)
            timeout: 请求超时时间秒数 (默认60)
        """
        for key, value in kwargs.items():
            if key in self.oil_batch_config:
                old_value = self.oil_batch_config[key]
                self.oil_batch_config[key] = value
                print(f"📝 配置更新: {key} = {old_value} → {value}")
            else:
                print(f"⚠️ 未知配置项: {key}")

        print("🔧 当前原油API配置:")
        for key, value in self.oil_batch_config.items():
            print(f"   - {key}: {value}")

    def get_historical_data(self, symbol, interval, start_str, end_str=None, limit=1000):
        """获取历史K线数据"""
        start_time = int(pd.Timestamp(start_str).timestamp() * 1000)
        end_time = int(pd.Timestamp(end_str).timestamp() * 1000) if end_str else None
        all_klines = []

        print(f"正在获取 {symbol} {interval} 历史数据...")

        while True:
            params = {
                'symbol': symbol.upper(),
                'interval': interval,
                'startTime': start_time,
                'limit': limit
            }
            if end_time:
                params['endTime'] = end_time

            try:
                proxies = {
                    'http': 'socks5h://127.0.0.1:7890',
                    'https': 'socks5h://127.0.0.1:7890'
                }
                headers = {
                    'User-Agent': 'PostmanRuntime/7.44.1',
                    'Accept': '*/*',
                    'Accept-Language': 'zh-CN,zh;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Connection': 'keep-alive',
                    'Referer': 'https://finance.yahoo.com/quote/CL=F/history?p=CL%3DF',
                    'Cookie': 'B=1f3q7g2b0g8c5&b=3&s=4k; GUC=0; A1=20231030.01; A3=20231030.01; A2=20231030.01; A4=20231030.01; A5=20231030.01; A6=20231030.01; A7=20231030.01; A8=20231030.01; A9=20231030.01; A10=20231030.01; A11=20231030.01; A12=20231030.01'
                }

                response = requests.get(self.base_url, params=params, headers=headers, timeout=60, proxies=proxies)
                response.raise_for_status()
                klines = response.json()

                if not klines:
                    break

                all_klines.extend(klines)

                if len(klines) < limit:
                    break

                last_close_time = klines[-1][6]
                start_time = last_close_time + 1

            except Exception as e:
                print(f"获取数据时出错: {e}")
                break

        if not all_klines:
            print("❌ 未能获取到数据，使用示例数据")
            # 创建示例数据
            dates = pd.date_range(start=start_str, periods=1000, freq='4H')
            # 使用当前时间作为随机种子，确保每次生成不同的数据
            np.random.seed(int(pd.Timestamp.now().timestamp()) % 10000)
            base_price = 2000
            prices = []
            for i in range(1000):
                change = np.random.normal(0, 0.02)
                base_price *= (1 + change)
                prices.append(base_price)

            df = pd.DataFrame({
                'open_time': dates,
                'open': prices,
                'high': [p * (1 + abs(np.random.normal(0, 0.01))) for p in prices],
                'low': [p * (1 - abs(np.random.normal(0, 0.01))) for p in prices],
                'close': [p * (1 + np.random.normal(0, 0.005)) for p in prices],
                'volume': [np.random.uniform(1000, 10000) for _ in range(1000)]
            })
        else:
            df = pd.DataFrame(all_klines, columns=[
                'open_time', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_asset_volume', 'number_of_trades',
                'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignored'
            ])

            df['open_time'] = pd.to_datetime(df['open_time'], unit='ms') + pd.Timedelta(hours=8)
            df['close_time'] = pd.to_datetime(df['close_time'], unit='ms') + pd.Timedelta(hours=8)

            numeric_cols = ['open', 'high', 'low', 'close', 'volume']
            df[numeric_cols] = df[numeric_cols].astype(float)

        print(f"✅ 成功获取 {len(df)} 条K线数据")
        return df[['open_time', 'open', 'high', 'low', 'close', 'volume']]

    def get_oil_data_by_crypto_timerange(self, crypto_df, crypto_interval='1h'):
        """根据数字货币数据的时间区间获取原油数据（使用Alpha Vantage API，固定使用1小时K线）"""
        print("正在根据数字货币时间区间获取原油期货价格数据（1小时K线）- 使用Alpha Vantage API...")

        if len(crypto_df) == 0:
            print("❌ 数字货币数据为空，无法获取原油数据")
            return pd.DataFrame()

        # 获取数字货币数据的时间范围
        crypto_start_time = crypto_df['open_time'].min()
        crypto_end_time = crypto_df['open_time'].max()

        print(f"数字货币时间范围: {crypto_start_time} 至 {crypto_end_time}")

        # 使用优化的批量获取方法
        return self.get_oil_data_batch_optimized(
            crypto_start_time.strftime('%Y-%m-%d %H:%M:%S'),
            crypto_end_time.strftime('%Y-%m-%d %H:%M:%S'),
            crypto_interval
        )

    def get_oil_data_batch_optimized(self, start_str, end_str, crypto_interval='1h', max_records_per_request=None):
        """优化的原油数据批量获取方法 - 使用Alpha Vantage API按月获取数据，支持大数据量分批获取"""
        # 使用配置参数
        config = self.oil_batch_config
        if max_records_per_request is None:
            max_records_per_request = config['max_records_per_request']

        print(f"🚀 开始优化批量获取原油数据: {start_str} 到 {end_str}")
        print(f"⚙️ 配置: 每次最多{max_records_per_request}条记录，请求间隔≥{config['min_request_interval']}秒")

        import time
        start_timestamp = int(pd.to_datetime(start_str).timestamp())
        end_timestamp = int(pd.to_datetime(end_str).timestamp())

        # 计算预估时间跨度（小时）
        total_hours = (end_timestamp - start_timestamp) // 3600
        print(f"📊 预估数据时间跨度: {total_hours} 小时")

        # Alpha Vantage API按月获取数据，需要生成月份列表
        from datetime import datetime, timedelta
        import calendar

        start_date = datetime.fromtimestamp(start_timestamp)
        end_date = datetime.fromtimestamp(end_timestamp)

        # 生成需要获取的月份列表
        month_list = []
        current_date = datetime(start_date.year, start_date.month, 1)

        while current_date <= end_date:
            month_list.append(current_date.strftime('%Y-%m'))
            # 跳转到下个月
            if current_date.month == 12:
                current_date = datetime(current_date.year + 1, 1, 1)
            else:
                current_date = datetime(current_date.year, current_date.month + 1, 1)

        print(f"📋 需要获取的月份: {month_list}")
        print(f"📋 预计需要 {len(month_list)} 个月的数据")

        all_oil_data = []
        batch_count = 0

        # 请求速率控制
        last_request_time = 0
        min_interval = config['min_request_interval']  # 从配置获取最小请求间隔

        # 进度跟踪
        successful_batches = 0
        failed_batches = 0

        for month in month_list:
            batch_count += 1

            # 计算当前月份的时间范围
            year, month_num = map(int, month.split('-'))
            month_start = datetime(year, month_num, 1)
            if month_num == 12:
                month_end = datetime(year + 1, 1, 1) - timedelta(seconds=1)
            else:
                month_end = datetime(year, month_num + 1, 1) - timedelta(seconds=1)

            month_start_ts = month_start.timestamp()
            month_end_ts = month_end.timestamp()

            # 调整时间范围到实际需要的时间段
            current_start = max(month_start_ts, start_timestamp)
            current_end = min(month_end_ts, end_timestamp)

            # 速率限制：确保请求间隔至少1秒
            current_time = time.time()
            time_since_last = current_time - last_request_time
            if time_since_last < min_interval:
                sleep_time = min_interval - time_since_last
                print(f"⏳ 速率控制: 等待 {sleep_time:.2f} 秒...")
                time.sleep(sleep_time)

            # 计算和显示进度
            progress_percent = (batch_count / len(month_list)) * 100
            print(f"\n📦 月份 {batch_count}/{len(month_list)} ({progress_percent:.1f}%) - {month}")
            print(f"📅 时间段: {pd.to_datetime(current_start, unit='s')} 到 {pd.to_datetime(current_end, unit='s')}")

            # 获取当前月份的数据
            batch_data = self._get_oil_data_single_batch(current_start, current_end)
            last_request_time = time.time()

            if batch_data is not None and len(batch_data) > 0:
                all_oil_data.append(batch_data)
                successful_batches += 1
                print(f"✅ 月份 {month} 成功！获取 {len(batch_data)} 条记录")
                print(f"📊 成功率: {successful_batches}/{batch_count} ({(successful_batches / batch_count) * 100:.1f}%)")
            else:
                failed_batches += 1
                print(f"⚠️ 月份 {month} 获取失败，跳过")
                print(f"📊 失败率: {failed_batches}/{batch_count} ({(failed_batches / batch_count) * 100:.1f}%)")

                # 如果失败率过高，提供建议
                if batch_count >= 3 and failed_batches / batch_count > 0.5:
                    print(
                        f"🚨 警告：失败率过高 ({(failed_batches / batch_count) * 100:.1f}%)，可能需要检查网络连接或API状态")

            # 避免无限循环
            if batch_count > 24:  # 最多获取24个月的数据
                print(f"⚠️ 已达到最大月份限制 (24个月)，停止获取")
                break

        # 合并所有批次的数据
        if all_oil_data:
            print(f"\n🔄 合并 {len(all_oil_data)} 个批次的数据...")
            final_oil_df = pd.concat(all_oil_data, ignore_index=True)

            # 去重和排序
            original_count = len(final_oil_df)
            final_oil_df = final_oil_df.drop_duplicates(subset=['oil_timestamp']).reset_index(drop=True)
            final_oil_df = final_oil_df.sort_values('oil_timestamp').reset_index(drop=True)
            duplicates_removed = original_count - len(final_oil_df)

            print(f"🎉 批量获取完成！")
            print(f"📊 处理汇总:")
            print(f"   - 成功批次: {successful_batches}/{batch_count}")
            print(f"   - 原始记录: {original_count} 条")
            print(f"   - 去重后: {len(final_oil_df)} 条 (删除重复: {duplicates_removed})")
            print(f"   - 数据完整性: {(len(final_oil_df) / max(1, total_hours)) * 100:.1f}% (基于时间跨度)")
            print(f"📅 时间范围: {final_oil_df['oil_timestamp'].min()} 至 {final_oil_df['oil_timestamp'].max()}")
            print(f"💰 价格范围: ${final_oil_df['oil_close'].min():.2f} - ${final_oil_df['oil_close'].max():.2f}")
            print(f"⏱️ 请求间隔合规: 所有请求间隔 ≥ {min_interval} 秒")

            return final_oil_df
        else:
            print("❌ 所有批次都获取失败，使用模拟数据")
            print(f"💡 建议: 检查网络连接、API密钥或稍后重试")
            return self._create_dummy_oil_data(start_str, end_str)

    def fetch_oil_price_data(self, symbol="CL=F", period1=None, period2=None, interval="1d"):
        """
        获取Yahoo Finance期货价格数据，支持原油和黄金

        参数:
        - symbol: 期货代码，默认为CL=F (原油期货)，可设置为GC=F (黄金期货)
        - period1: 开始时间戳（整数）
        - period2: 结束时间戳（整数）
        - interval: 数据间隔，默认为1d（每日）

        返回:
        - 包含价格数据的字典，或在失败时返回None
        """

        # 如果没有提供时间戳，使用当前时间
        if period2 is None:
            period2 = int(time.time())
        if period1 is None:
            period1 = period2 - 86400  # 24小时前

        # 构建API URL
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"

        params = {
            "period1": period1,
            "period2": period2,
            "interval": interval,
            "includePrePost": "true",
            "events": "div|split|earn",
            "lang": "en-US",
            "region": "US",
            "source": "cosaic"
        }

        try:
            # 发送请求
            proxies = {
                'http': 'socks5h://127.0.0.1:7890',
                'https': 'socks5h://127.0.0.1:7890'
            }
            headers = {
                'User-Agent': 'PostmanRuntime/7.44.1',
                'Accept': '*/*',
                'Accept-Language': 'zh-CN,zh;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Referer': 'https://finance.yahoo.com/quote/CL=F/history?p=CL%3DF',
                'Cookie': 'B=1f3q7g2b0g8c5&b=3&s=4k; GUC=0; A1=20231030.01; A3=20231030.01; A2=20231030.01; A4=20231030.01; A5=20231030.01; A6=20231030.01; A7=20231030.01; A8=20231030.01; A9=20231030.01; A10=20231030.01; A11=20231030.01; A12=20231030.01'
            }
            response = requests.get(url, params=params, timeout=60, headers=headers, proxies=proxies)
            response.raise_for_status()

            # 解析JSON数据
            data = response.json()

            return data

        except requests.exceptions.RequestException as e:
            print(f"请求错误: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"JSON解析错误: {e}")
            return None

    def parse_oil_data(self, data):
        """
        解析原油期货数据，提取关键信息
        """
        if not data or "chart" not in data:
            return None

        result = data["chart"]["result"]
        if not result:
            return None

        chart_data = result[0]
        meta = chart_data.get("meta", {})

        # 提取关键价格信息
        price_info = {
            "symbol": meta.get("symbol"),
            "shortName": meta.get("shortName"),
            "currency": meta.get("currency"),
            "exchangeName": meta.get("exchangeName"),
            "regularMarketPrice": meta.get("regularMarketPrice"),
            "regularMarketTime": meta.get("regularMarketTime"),
            "regularMarketDayHigh": meta.get("regularMarketDayHigh"),
            "regularMarketDayLow": meta.get("regularMarketDayLow"),
            "regularMarketVolume": meta.get("regularMarketVolume"),
            "chartPreviousClose": meta.get("chartPreviousClose"),
            "fiftyTwoWeekHigh": meta.get("fiftyTwoWeekHigh"),
            "fiftyTwoWeekLow": meta.get("fiftyTwoWeekLow"),
            "timezone": meta.get("timezone")
        }

        # 提取历史数据
        timestamps = chart_data.get("timestamp", [])
        indicators = chart_data.get("indicators", {})
        quotes = indicators.get("quote", [])

        historical_data = []
        if quotes and timestamps:
            quote = quotes[0]
            for i, timestamp in enumerate(timestamps):
                historical_data.append({
                    "timestamp": timestamp,
                    "datetime": datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
                    "open": quote.get("open", [])[i] if i < len(quote.get("open", [])) else None,
                    "high": quote.get("high", [])[i] if i < len(quote.get("high", [])) else None,
                    "low": quote.get("low", [])[i] if i < len(quote.get("low", [])) else None,
                    "close": quote.get("close", [])[i] if i < len(quote.get("close", [])) else None,
                    "volume": quote.get("volume", [])[i] if i < len(quote.get("volume", [])) else None
                })

        return {
            "price_info": price_info,
            "historical_data": historical_data
        }

    def _get_oil_data_single_batch(self, start_timestamp, end_timestamp):
        """获取单个批次的原油数据（使用Yahoo Finance API）"""

        config = self.oil_batch_config
        max_retries = config['max_retries']
        interval = "1d"

        # 获取合适的interval
        time_diff = end_timestamp - start_timestamp
        # if time_diff <= 86400 * 7:  # 小于等于7天，使用1小时数据
        #     interval = "1h"
        # elif time_diff <= 86400 * 30:  # 小于等于30天，使用4小时数据
        #     interval = "4h"
        # else:  # 大于30天，使用1天数据
        #     interval = "1d"

        print(
            f"🔄 使用Yahoo Finance API获取原油数据，时间范围: {datetime.fromtimestamp(start_timestamp)} 到 {datetime.fromtimestamp(end_timestamp)}, 间隔: {interval}")

        for retry in range(max_retries):
            try:
                if retry > 0:
                    wait_time = 1.0 * (2 ** retry)
                    print(f"🔄 重试 {retry + 1}/{max_retries}，等待 {wait_time:.1f} 秒...")
                    time.sleep(wait_time)

                # 使用新的Yahoo Finance API获取数据
                raw_data = self.fetch_oil_price_data(
                    symbol="CL=F",
                    period1=int(start_timestamp),
                    period2=int(end_timestamp),
                    interval=interval
                )
                
                # 同时获取黄金期货数据
                gold_data = self.fetch_oil_price_data(
                    symbol="GC=F",
                    period1=int(start_timestamp),
                    period2=int(end_timestamp),
                    interval=interval
                )

                if not raw_data:
                    print(f"❌ 获取原始数据失败")
                    if retry == max_retries - 1:
                        return None
                    continue

                # 解析数据
                parsed_data = self.parse_oil_data(raw_data)

                if not parsed_data or not parsed_data['historical_data']:
                    print(f"❌ 解析数据失败或历史数据为空")
                    if retry == max_retries - 1:
                        return None
                    continue

                # 转换为DataFrame格式
                oil_records = []
                for data_point in parsed_data['historical_data']:
                    if all(data_point[key] is not None for key in ['open', 'high', 'low', 'close', 'volume']):
                        try:
                            # 转换时间戳为datetime
                            timestamp = pd.to_datetime(data_point['timestamp'], unit='s')

                            # 过滤时间范围
                            timestamp_unix = data_point['timestamp']
                            if timestamp_unix < start_timestamp or timestamp_unix > end_timestamp:
                                continue

                            oil_records.append({
                                'oil_timestamp': timestamp,
                                'oil_open': float(data_point['open']),
                                'oil_high': float(data_point['high']),
                                'oil_low': float(data_point['low']),
                                'oil_close': float(data_point['close']),
                                'oil_volume': float(data_point['volume'])
                            })
                        except (ValueError, KeyError) as e:
                            print(f"⚠️ 数据解析错误: {e} for timestamp {data_point['timestamp']}")
                            continue

                if oil_records:
                    print(f"✅ 成功获取 {len(oil_records)} 条原油数据")
                    return pd.DataFrame(oil_records).sort_values('oil_timestamp').reset_index(drop=True)
                else:
                    print(f"❌ 时间范围内无有效数据")
                    if retry == max_retries - 1:
                        return None
                    continue

            except Exception as e:
                print(f"❌ 请求处理错误: {e}")
                if retry == max_retries - 1:
                    return None

        return None

    def get_oil_data(self, start_str, end_str=None, crypto_interval='1h'):
        """获取原油期货价格数据（使用Yahoo Finance API，自动选择合适的时间间隔）"""
        print("正在获取原油期货价格数据 - 使用Yahoo Finance API...")

        # 直接使用优化的批量获取方法
        if end_str is None:
            import time
            end_str = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')

        return self.get_oil_data_batch_optimized(start_str, end_str, crypto_interval)

    def _create_dummy_oil_data(self, start_str, end_str=None):
        """创建示例原油数据"""
        start_date = pd.Timestamp(start_str)
        end_date = pd.Timestamp(end_str) if end_str else pd.Timestamp.now()

        # 创建时间序列（1分钟间隔）
        date_range = pd.date_range(start=start_date, end=end_date, freq='1min')

        # 生成模拟原油价格数据
        # 使用当前时间作为随机种子，确保每次生成不同的数据
        np.random.seed(int(pd.Timestamp.now().timestamp()) % 10000)
        base_price = 70.0  # 基础油价70美元

        oil_data = []
        for i, timestamp in enumerate(date_range):
            # 随机游走生成价格
            if i == 0:
                open_price = base_price
            else:
                # 使用上一个收盘价作为开盘价
                open_price = oil_data[-1]['oil_close']

            # 生成日内波动
            daily_change = np.random.normal(0, 0.02)  # 2%日波动
            intraday_volatility = 0.005  # 0.5%日内波动

            high_price = open_price * (1 + abs(np.random.normal(0, intraday_volatility)))
            low_price = open_price * (1 - abs(np.random.normal(0, intraday_volatility)))
            close_price = open_price * (1 + daily_change + np.random.normal(0, intraday_volatility / 2))

            # 确保high >= max(open, close) 和 low <= min(open, close)
            high_price = max(high_price, open_price, close_price)
            low_price = min(low_price, open_price, close_price)

            volume = np.random.uniform(50000, 200000)

            oil_data.append({
                'oil_timestamp': timestamp,
                'oil_open': round(open_price, 2),
                'oil_high': round(high_price, 2),
                'oil_low': round(low_price, 2),
                'oil_close': round(close_price, 2),
                'oil_volume': round(volume, 0)
            })

        oil_df = pd.DataFrame(oil_data)
        print(
            f"✅ 生成了 {len(oil_df)} 条模拟原油数据，价格范围: ${oil_df['oil_close'].min():.2f} - ${oil_df['oil_close'].max():.2f}")

        return oil_df

    def _create_dummy_oil_data_for_crypto(self, crypto_df, crypto_interval='1h'):
        """根据数字货币数据创建匹配的模拟原油数据"""
        print("正在生成与数字货币数据匹配的模拟原油数据...")

        if len(crypto_df) == 0:
            return pd.DataFrame()

        # 获取数字货币的时间戳
        crypto_timestamps = crypto_df['open_time'].tolist()

        # 生成模拟原油价格数据
        # 使用当前时间作为随机种子，确保每次生成不同的数据
        np.random.seed(int(pd.Timestamp.now().timestamp()) % 10000)
        base_price = 70.0  # 基础油价70美元

        oil_data = []
        for i, timestamp in enumerate(crypto_timestamps):
            # 随机游走生成价格
            if i == 0:
                open_price = base_price
            else:
                # 使用上一个收盘价作为开盘价
                open_price = oil_data[-1]['oil_close']

            # 根据时间间隔调整波动率
            if crypto_interval in ['1m', '5m']:
                daily_change = np.random.normal(0, 0.005)  # 0.5%波动
                intraday_volatility = 0.002  # 0.2%日内波动
            elif crypto_interval in ['15m', '30m']:
                daily_change = np.random.normal(0, 0.01)  # 1%波动
                intraday_volatility = 0.003  # 0.3%日内波动
            elif crypto_interval in ['1h', '2h']:
                daily_change = np.random.normal(0, 0.015)  # 1.5%波动
                intraday_volatility = 0.005  # 0.5%日内波动
            elif crypto_interval == '4h':
                daily_change = np.random.normal(0, 0.025)  # 2.5%波动
                intraday_volatility = 0.008  # 0.8%日内波动
            else:  # 日线或更长
                daily_change = np.random.normal(0, 0.03)  # 3%波动
                intraday_volatility = 0.01  # 1%日内波动

            high_price = open_price * (1 + abs(np.random.normal(0, intraday_volatility)))
            low_price = open_price * (1 - abs(np.random.normal(0, intraday_volatility)))
            close_price = open_price * (1 + daily_change + np.random.normal(0, intraday_volatility / 2))

            # 确保high >= max(open, close) 和 low <= min(open, close)
            high_price = max(high_price, open_price, close_price)
            low_price = min(low_price, open_price, close_price)

            # 根据时间间隔调整成交量
            if crypto_interval in ['1m', '5m']:
                volume = np.random.uniform(10000, 50000)
            elif crypto_interval in ['15m', '30m']:
                volume = np.random.uniform(30000, 100000)
            elif crypto_interval in ['1h', '2h']:
                volume = np.random.uniform(50000, 200000)
            elif crypto_interval == '4h':
                volume = np.random.uniform(100000, 400000)
            else:  # 日线或更长
                volume = np.random.uniform(200000, 800000)

            oil_data.append({
                'oil_timestamp': timestamp,
                'oil_open': round(open_price, 2),
                'oil_high': round(high_price, 2),
                'oil_low': round(low_price, 2),
                'oil_close': round(close_price, 2),
                'oil_volume': round(volume, 0)
            })

        oil_df = pd.DataFrame(oil_data)
        print(f"✅ 生成了 {len(oil_df)} 条与数字货币匹配的模拟原油数据")
        print(f"时间范围: {oil_df['oil_timestamp'].min()} 至 {oil_df['oil_timestamp'].max()}")
        print(f"价格范围: ${oil_df['oil_close'].min():.2f} - ${oil_df['oil_close'].max():.2f}")

        return oil_df

    def prepare_oil_data_for_merge(self, oil_df, crypto_df):
        """准备原油数据以便与数字货币数据合并"""
        print("正在准备原油数据以便合并...")

        # 重命名时间列以便合并
        oil_prepared = oil_df.copy()
        oil_prepared = oil_prepared.rename(columns={'oil_timestamp': 'open_time'})

        # 确保时间列为datetime类型
        oil_prepared['open_time'] = pd.to_datetime(oil_prepared['open_time'])

        # 移除缺失值
        oil_prepared = oil_prepared.dropna()

        print(f"✅ 原油数据准备完成，共 {len(oil_prepared)} 条记录")
        print(f"时间范围: {oil_prepared['open_time'].min()} 到 {oil_prepared['open_time'].max()}")

        return oil_prepared

    def resample_oil_data_to_match_crypto(self, oil_df, crypto_df, crypto_interval):
        """将原油数据重采样以匹配数字货币数据的时间间隔（备用函数）"""
        print(f"正在将原油数据重采样到 {crypto_interval} 间隔...")

        # 设置时间为索引
        oil_df = oil_df.set_index('oil_timestamp')

        # 根据crypto_interval确定重采样规则
        if crypto_interval == '1h':
            resample_rule = '1H'
        elif crypto_interval == '4h':
            resample_rule = '4H'
        elif crypto_interval == '1d':
            resample_rule = '1D'
        elif crypto_interval == '1m':
            resample_rule = '1min'
        else:
            resample_rule = '1H'  # 默认1小时

        # 重采样原油数据
        oil_resampled = oil_df.resample(resample_rule).agg({
            'oil_open': 'first',
            'oil_high': 'max',
            'oil_low': 'min',
            'oil_close': 'last',
            'oil_volume': 'sum'
        })

        # 重置索引
        oil_resampled = oil_resampled.reset_index()
        oil_resampled = oil_resampled.rename(columns={'oil_timestamp': 'open_time'})

        # 移除缺失值
        oil_resampled = oil_resampled.dropna()

        print(f"✅ 原油数据重采样完成，共 {len(oil_resampled)} 条记录")
        return oil_resampled

    def merge_crypto_oil_data(self, crypto_df, oil_df):
        """优化的数字货币数据和原油数据合并"""
        print("正在合并数字货币和原油数据...")

        if len(oil_df) == 0:
            print("⚠️ 原油数据为空，无法合并")
            return crypto_df

        # 确保时间列为datetime类型
        crypto_df['open_time'] = pd.to_datetime(crypto_df['open_time'])
        oil_df['open_time'] = pd.to_datetime(oil_df['open_time'])

        print(
            f"数字货币数据: {len(crypto_df)} 条，时间范围: {crypto_df['open_time'].min()} 至 {crypto_df['open_time'].max()}")
        print(f"原油数据: {len(oil_df)} 条，时间范围: {oil_df['open_time'].min()} 至 {oil_df['open_time'].max()}")

        # 使用asof合并，找到最接近的原油数据
        crypto_df = crypto_df.sort_values('open_time').reset_index(drop=True)
        oil_df = oil_df.sort_values('open_time').reset_index(drop=True)

        # 先尝试精确匹配
        merged_df = pd.merge(crypto_df, oil_df, on='open_time', how='left')

        # 统计匹配情况
        exact_matches = merged_df['oil_close'].notna().sum()
        total_records = len(merged_df)
        match_rate = exact_matches / total_records * 100

        print(f"精确时间匹配: {exact_matches}/{total_records} ({match_rate:.1f}%)")

        # 如果匹配率低于50%，使用近似匹配
        if match_rate < 50:
            print("精确匹配率较低，使用近似时间匹配...")
            merged_df = pd.merge_asof(
                crypto_df, oil_df,
                on='open_time',
                direction='nearest',
                tolerance=pd.Timedelta(hours=2)  # 允许2小时的时间差
            )

            # 重新统计匹配情况
            approx_matches = merged_df['oil_close'].notna().sum()
            approx_match_rate = approx_matches / total_records * 100
            print(f"近似时间匹配: {approx_matches}/{total_records} ({approx_match_rate:.1f}%)")

        # 处理剩余的缺失值
        oil_columns = ['oil_open', 'oil_high', 'oil_low', 'oil_close', 'oil_volume']

        missing_before = merged_df[oil_columns].isna().sum().sum()

        # 前向填充
        merged_df[oil_columns] = merged_df[oil_columns].ffill()

        # 后向填充（处理开头的缺失值）
        merged_df[oil_columns] = merged_df[oil_columns].bfill()

        # 如果还有缺失值，用均值填充
        for col in oil_columns:
            if merged_df[col].isna().any():
                mean_value = merged_df[col].mean()
                if pd.isna(mean_value):
                    # 如果均值也是NaN，使用默认值
                    default_values = {
                        'oil_open': 70.0, 'oil_high': 71.0, 'oil_low': 69.0,
                        'oil_close': 70.0, 'oil_volume': 100000.0
                    }
                    mean_value = default_values.get(col, 70.0)
                merged_df[col] = merged_df[col].fillna(mean_value)

        missing_after = merged_df[oil_columns].isna().sum().sum()

        print(f"缺失值处理: {missing_before} → {missing_after}")
        print(f"✅ 数据合并完成，共 {len(merged_df)} 条记录")

        # 验证合并结果
        if len(merged_df) != len(crypto_df):
            print("⚠️ 警告：合并后数据量发生变化")

        # 显示原油数据范围
        if merged_df['oil_close'].notna().any():
            oil_min = merged_df['oil_close'].min()
            oil_max = merged_df['oil_close'].max()
            oil_mean = merged_df['oil_close'].mean()
            print(f"合并后原油价格范围: ${oil_min:.2f} - ${oil_max:.2f}，均值: ${oil_mean:.2f}")

        return merged_df

    def calculate_indicators(self, df, prefix='oil_'):
        """计算技术指标，支持原油和黄金

        参数:
        - df: 包含价格数据的DataFrame
        - prefix: 指标前缀，默认为'oil_'（原油），可设置为'gld_'（黄金）

        返回:
        - 包含计算指标的DataFrame
        """
        print(f"计算{prefix}技术指标...")

        # 检查数据是否存在
        columns = [f"{prefix}open", f"{prefix}high", f"{prefix}low", f"{prefix}close", f"{prefix}volume"]
        missing_columns = [col for col in columns if col not in df.columns]

        if missing_columns:
            print(f"⚠️ 缺少{prefix}数据列: {missing_columns}")
            print(f"将使用默认值填充{prefix}指标...")
            # 创建默认的指标列
            for col in columns:
                if col not in df.columns:
                    if 'close' in col:
                        df[col] = 70.0  # 默认油价70美元
                    elif 'volume' in col:
                        df[col] = 100000.0  # 默认成交量
                    else:
                        df[col] = 70.0  # 其他价格列默认值

        # 确保指标数据为数值类型
        for col in columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 基础指标 - 添加安全检查
        try:
            # 实体比例
            high_low_diff = df[f"{prefix}high"] - df[f"{prefix}low"]
            # 避免除零错误
            high_low_diff = high_low_diff.replace(0, 1e-10)
            df[f"{prefix}body_ratio"] = abs(df[f"{prefix}close"] - df[f"{prefix}open"]) / high_low_diff
            df[f"{prefix}body_ratio"] = df[f"{prefix}body_ratio"].fillna(0).clip(0, 1)  # 限制在0-1之间
        except Exception as e:
            print(f"⚠️ 计算{prefix}实体比例时出错: {e}")
            df[f"{prefix}body_ratio"] = 0.5

        # RSI指标 - 添加错误处理
        try:
            df[f"{prefix}RSI6"] = self._calculate_rsi(df[f"{prefix}close"], 6)
            df[f"{prefix}RSI12"] = self._calculate_rsi(df[f"{prefix}close"], 12)
            df[f"{prefix}RSI24"] = self._calculate_rsi(df[f"{prefix}close"], 24)

            # 确保RSI值在合理范围内
            for col in [f"{prefix}RSI6", f"{prefix}RSI12", f"{prefix}RSI24"]:
                df[col] = df[col].fillna(50).clip(0, 100)
        except Exception as e:
            print(f"⚠️ 计算{prefix}RSI时出错: {e}")
            df[f"{prefix}RSI6"] = 50
            df[f"{prefix}RSI12"] = 50
            df[f"{prefix}RSI24"] = 50

        # 移动平均线 - 添加错误处理
        try:
            df[f"{prefix}MA_5"] = df[f"{prefix}close"].rolling(window=5, min_periods=1).mean()
            df[f"{prefix}MA_10"] = df[f"{prefix}close"].rolling(window=10, min_periods=1).mean()
            df[f"{prefix}MA_20"] = df[f"{prefix}close"].rolling(window=20, min_periods=1).mean()

            # 填充缺失值
            df[f"{prefix}MA_5"] = df[f"{prefix}MA_5"].fillna(df[f"{prefix}close"])
            df[f"{prefix}MA_10"] = df[f"{prefix}MA_10"].fillna(df[f"{prefix}close"])
            df[f"{prefix}MA_20"] = df[f"{prefix}MA_20"].fillna(df[f"{prefix}close"])
        except Exception as e:
            print(f"⚠️ 计算{prefix}移动平均线时出错: {e}")
            df[f"{prefix}MA_5"] = df[f"{prefix}close"]
            df[f"{prefix}MA_10"] = df[f"{prefix}close"]
            df[f"{prefix}MA_20"] = df[f"{prefix}close"]

        # 布林带 - 添加错误处理
        try:
            std = df[f"{prefix}close"].rolling(window=20, min_periods=1).std()
            std = std.fillna(df[f"{prefix}close"].std() if df[f"{prefix}close"].std() > 0 else 1.0)

            df[f"{prefix}Bollinger_Upper"] = df[f"{prefix}MA_20"] + 2 * std
            df[f"{prefix}Bollinger_Lower"] = df[f"{prefix}MA_20"] - 2 * std

            # 确保布林带下轨不为负值
            df[f"{prefix}Bollinger_Lower"] = df[f"{prefix}Bollinger_Lower"].clip(lower=0.1)
        except Exception as e:
            print(f"⚠️ 计算{prefix}布林带时出错: {e}")
            df[f"{prefix}Bollinger_Upper"] = df[f"{prefix}close"] * 1.1
            df[f"{prefix}Bollinger_Lower"] = df[f"{prefix}close"] * 0.9

        # ROC和动量 - 添加错误处理
        try:
            # 避免除零错误
            close_shift5 = df[f"{prefix}close"].shift(5).replace(0, 1e-10)
            df[f"{prefix}ROC_5"] = (df[f"{prefix}close"] - df[f"{prefix}close"].shift(5)) / close_shift5
            df[f"{prefix}ROC_5"] = df[f"{prefix}ROC_5"].fillna(0).clip(-1, 1)  # 限制在-100%到100%之间

            df[f"{prefix}Momentum_10"] = df[f"{prefix}close"] - df[f"{prefix}close"].shift(10)
            df[f"{prefix}Momentum_10"] = df[f"{prefix}Momentum_10"].fillna(0)
        except Exception as e:
            print(f"⚠️ 计算{prefix}ROC和动量时出错: {e}")
            df[f"{prefix}ROC_5"] = 0
            df[f"{prefix}Momentum_10"] = 0

        # ATR指标 - 添加错误处理
        try:
            df[f"{prefix}H-L"] = df[f"{prefix}high"] - df[f"{prefix}low"]
            df[f"{prefix}H-PC"] = abs(df[f"{prefix}high"] - df[f"{prefix}close"].shift(1))
            df[f"{prefix}L-PC"] = abs(df[f"{prefix}low"] - df[f"{prefix}close"].shift(1))

            # 填充缺失值
            df[f"{prefix}H-PC"] = df[f"{prefix}H-PC"].fillna(df[f"{prefix}H-L"])
            df[f"{prefix}L-PC"] = df[f"{prefix}L-PC"].fillna(df[f"{prefix}H-L"])

            tr = df[[f"{prefix}H-L", f"{prefix}H-PC", f"{prefix}L-PC"]].max(axis=1)
            df[f"{prefix}ATR"] = tr.rolling(window=14, min_periods=1).mean()
            df[f"{prefix}ATR"] = df[f"{prefix}ATR"].fillna(tr.mean() if tr.mean() > 0 else 1.0)

            # 删除临时列
            df.drop(columns=[f"{prefix}H-L", f"{prefix}H-PC", f"{prefix}L-PC"], inplace=True)
        except Exception as e:
            print(f"⚠️ 计算{prefix}ATR时出错: {e}")
            df[f"{prefix}ATR"] = 1.0

        # 成交量指标 - 添加错误处理
        try:
            df[f"{prefix}Volume_MA_5"] = df[f"{prefix}volume"].rolling(window=5, min_periods=1).mean()
            df[f"{prefix}Volume_MA_5"] = df[f"{prefix}Volume_MA_5"].fillna(
                df[f"{prefix}volume"].mean() if df[f"{prefix}volume"].mean() > 0 else 100000.0)

            volume_ma10 = df[f"{prefix}volume"].rolling(window=10, min_periods=1).mean()
            volume_ma10 = volume_ma10.replace(0, 1e-10)  # 避免除零
            df[f"{prefix}volume_spike"] = df[f"{prefix}volume"] > volume_ma10 * 1.5
            df[f"{prefix}volume_spike"] = df[f"{prefix}volume_spike"].fillna(False)
        except Exception as e:
            print(f"⚠️ 计算{prefix}成交量指标时出错: {e}")
            df[f"{prefix}Volume_MA_5"] = 100000.0
            df[f"{prefix}volume_spike"] = False

        # 价格位置指标 - 添加错误处理
        try:
            high_low_diff = df[f"{prefix}high"] - df[f"{prefix}low"]
            high_low_diff = high_low_diff.replace(0, 1e-10)
            df[f"{prefix}price_position"] = (df[f"{prefix}close"] - df[f"{prefix}low"]) / high_low_diff
            df[f"{prefix}price_position"] = df[f"{prefix}price_position"].fillna(0.5).clip(0, 1)
        except Exception as e:
            print(f"⚠️ 计算{prefix}价格位置时出错: {e}")
            df[f"{prefix}price_position"] = 0.5

        # 相对位置 - 添加错误处理
        try:
            ma20 = df[f"{prefix}MA_20"].replace(0, 1e-10)
            df[f"{prefix}relative_position"] = (df[f"{prefix}close"] - df[f"{prefix}MA_20"]) / ma20
            df[f"{prefix}relative_position"] = df[f"{prefix}relative_position"].fillna(0).clip(-1, 1)
        except Exception as e:
            print(f"⚠️ 计算{prefix}相对位置时出错: {e}")
            df[f"{prefix}relative_position"] = 0

        # 波动率 - 添加错误处理
        try:
            close = df[f"{prefix}close"].replace(0, 1e-10)
            df[f"{prefix}volatility_ratio"] = df[f"{prefix}ATR"] / close
            df[f"{prefix}volatility_ratio"] = df[f"{prefix}volatility_ratio"].fillna(0.01).clip(0, 0.5)

            close_rolling20 = df[f"{prefix}close"].rolling(20, min_periods=1)
            std_20 = close_rolling20.std()
            mean_20 = close_rolling20.mean()
            mean_20 = mean_20.replace(0, 1e-10)
            df[f"{prefix}price_volatility"] = std_20 / mean_20
            df[f"{prefix}price_volatility"] = df[f"{prefix}price_volatility"].fillna(0.01).clip(0, 0.5)
        except Exception as e:
            print(f"⚠️ 计算{prefix}波动率时出错: {e}")
            df[f"{prefix}volatility_ratio"] = 0.01
            df[f"{prefix}price_volatility"] = 0.01

        # 相关性指标 - 添加错误处理
        if 'close' in df.columns:
            try:
                # 价格相关性（滚动窗口）
                df[f"{prefix}crypto_price_corr"] = df['close'].rolling(window=20, min_periods=5).corr(df[f"{prefix}close"])
                df[f"{prefix}crypto_price_corr"] = df[f"{prefix}crypto_price_corr"].fillna(0).clip(-1, 1)

                # 价格比率
                close = df[f"{prefix}close"].replace(0, 1e-10)
                df[f"{prefix}crypto_price_ratio"] = df['close'] / close
                df[f"{prefix}crypto_price_ratio"] = df[f"{prefix}crypto_price_ratio"].fillna(1.0).clip(0.1, 10.0)

                # 收益率相关性
                crypto_returns = df['close'].pct_change().fillna(0)
                close_returns = df[f"{prefix}close"].pct_change().fillna(0)
                df[f"{prefix}crypto_returns_corr"] = crypto_returns.rolling(window=20, min_periods=5).corr(close_returns)
                df[f"{prefix}crypto_returns_corr"] = df[f"{prefix}crypto_returns_corr"].fillna(0).clip(-1, 1)

                # 波动率比率
                crypto_vol = df['close'].rolling(20, min_periods=1).std()
                close_vol = df[f"{prefix}close"].rolling(20, min_periods=1).std()
                close_vol = close_vol.replace(0, 1e-10)
                df[f"{prefix}crypto_vol_ratio"] = crypto_vol / close_vol
                df[f"{prefix}crypto_vol_ratio"] = df[f"{prefix}crypto_vol_ratio"].fillna(1.0).clip(0.1, 10.0)

                # 相对强度比较
                df[f"{prefix}crypto_rsi_diff"] = df.get('RSI6', 50) - df[f"{prefix}RSI6"]
                df[f"{prefix}crypto_rsi_diff"] = df[f"{prefix}crypto_rsi_diff"].fillna(0).clip(-100, 100)

                # 趋势一致性
                crypto_trend = df['close'] > df.get('MA_20', df['close'])
                close_trend = df[f"{prefix}close"] > df[f"{prefix}MA_20"]
                df[f"{prefix}crypto_trend_consistency"] = (crypto_trend == close_trend).astype(int)
                df[f"{prefix}crypto_trend_consistency"] = df[f"{prefix}crypto_trend_consistency"].fillna(0)
            except Exception as e:
                print(f"⚠️ 计算{prefix}相关性指标时出错: {e}")
                # 设置默认值
                df[f"{prefix}crypto_price_corr"] = 0
                df[f"{prefix}crypto_price_ratio"] = 1.0
                df[f"{prefix}crypto_returns_corr"] = 0
                df[f"{prefix}crypto_vol_ratio"] = 1.0
                df[f"{prefix}crypto_rsi_diff"] = 0
                df[f"{prefix}crypto_trend_consistency"] = 0
        else:
            print(f"⚠️ 数字货币数据不存在，跳过{prefix}相关性计算")
            # 设置默认值
            df[f"{prefix}crypto_price_corr"] = 0
            df[f"{prefix}crypto_price_ratio"] = 1.0
            df[f"{prefix}crypto_returns_corr"] = 0
            df[f"{prefix}crypto_vol_ratio"] = 1.0
            df[f"{prefix}crypto_rsi_diff"] = 0
            df[f"{prefix}crypto_trend_consistency"] = 0

        # 市场状态 - 添加错误处理
        try:
            df[f"{prefix}market_state"] = np.where(
                (df[f"{prefix}close"] > df[f"{prefix}MA_20"]) & (df[f"{prefix}RSI6"] > 50),
                '强势',
                np.where(
                    (df[f"{prefix}close"] < df[f"{prefix}MA_20"]) & (df[f"{prefix}RSI6"] < 50),
                    '弱势',
                    '震荡'
                )
            )
            df[f"{prefix}market_state"] = df[f"{prefix}market_state"].fillna('震荡')
        except Exception as e:
            print(f"⚠️ 计算{prefix}市场状态时出错: {e}")
            df[f"{prefix}market_state"] = '震荡'

        # 最终检查：确保所有指标列都不包含无穷大或NaN值
        indicator_columns = [col for col in df.columns if col.startswith(prefix)]
        for col in indicator_columns:
            if col in df.columns:
                # 替换无穷大值
                df[col] = df[col].replace([np.inf, -np.inf], np.nan)
                # 根据列类型填充NaN
                if 'ratio' in col or 'corr' in col or 'position' in col:
                    df[col] = df[col].fillna(0.5)  # 比率类指标用0.5填充
                elif 'RSI' in col:
                    df[col] = df[col].fillna(50)  # RSI用50填充
                elif 'MA' in col:
                    df[col] = df[col].fillna(df[f"{prefix}close"])  # 均线用收盘价填充
                elif 'volume' in col:
                    df[col] = df[col].fillna(100000.0)  # 成交量用默认值填充
                else:
                    df[col] = df[col].fillna(0)  # 其他指标用0填充

        print(f"✅ {prefix}技术指标计算完成")
        print(f"📊 {prefix}指标列数: {len([col for col in df.columns if col.startswith(prefix)])}")

        return df

    def calculate_basic_indicators(self, df):
        """计算基础技术指标"""
        print("计算基础技术指标...")

        # 回撤指标
        df['drawdown_ratio'] = (df['high'] - df['low']) / df['high']

        # K线实体比例
        df['body_ratio'] = abs(df['close'] - df['open']) / (df['high'] - df['low'])
        df['body_ratio'] = df['body_ratio'].fillna(0)

        # RSI指标
        df['RSI6'] = self._calculate_rsi(df['close'], 6)
        df['RSI12'] = self._calculate_rsi(df['close'], 12)
        df['RSI24'] = self._calculate_rsi(df['close'], 24)

        # KDJ指标
        kdj = self._calculate_kdj(df)
        df = pd.concat([df, kdj], axis=1)

        # 传统MACD指标计算
        df = self._calculate_traditional_macd(df)

        # 移动平均线
        df['MA_5'] = df['close'].rolling(window=5).mean()
        df['MA_10'] = df['close'].rolling(window=10).mean()
        df['MA_20'] = df['close'].rolling(window=20).mean()
        df['MA_42'] = df['close'].rolling(window=42).mean()

        # 布林带
        std = df['close'].rolling(window=20).std()
        df['Bollinger_Upper'] = df['MA_20'] + 2 * std
        df['Bollinger_Lower'] = df['MA_20'] - 2 * std

        # ROC和动量
        df['ROC_5'] = (df['close'] - df['close'].shift(5)) / df['close'].shift(5)
        df['Momentum_10'] = df['close'] - df['close'].shift(10)

        # 成交量特征
        df['Volume_MA_5'] = df['volume'].rolling(window=5).mean()
        df['volume_spike'] = df['volume'] > df['volume'].rolling(window=10).mean() * 1.5

        # ATR指标
        df['H-L'] = df['high'] - df['low']
        df['H-PC'] = abs(df['high'] - df['close'].shift(1))
        df['L-PC'] = abs(df['low'] - df['close'].shift(1))
        tr = df[['H-L', 'H-PC', 'L-PC']].max(axis=1)
        df['ATR'] = tr.rolling(window=14).mean()
        df.drop(columns=['H-L', 'H-PC', 'L-PC'], inplace=True)

        return df

    def _calculate_rsi(self, series, period=14):
        """计算RSI指标"""
        delta = series.diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        avg_gain = gain.ewm(alpha=1 / period, min_periods=period).mean()
        avg_loss = loss.ewm(alpha=1 / period, min_periods=period).mean()
        rs = avg_gain / (avg_loss + 1e-10)
        return 100 - (100 / (1 + rs))

    def _calculate_kdj(self, df, n=9, k_period=3, d_period=3):
        """计算KDJ指标"""
        low_min = df['low'].rolling(window=n).min()
        high_max = df['high'].rolling(window=n).max()
        rsv = 100 * (df['close'] - low_min) / (high_max - low_min + 1e-10)
        k = rsv.ewm(span=k_period).mean()
        d = k.ewm(span=d_period).mean()
        j = 3 * k - 2 * d
        return pd.DataFrame({'K': k, 'D': d, 'J': j})

    def _calculate_traditional_macd(self, df, fast=12, slow=26, signal=9):
        """计算传统MACD指标"""
        print("计算传统MACD指标...")

        # 计算EMA
        df['MACD_fast_ema'] = df['close'].ewm(span=fast).mean()
        df['MACD_slow_ema'] = df['close'].ewm(span=slow).mean()

        # 计算MACD线 (DIF)
        df['MACD_DIF'] = df['MACD_fast_ema'] - df['MACD_slow_ema']

        # 计算信号线 (DEA)
        df['MACD_DEA'] = df['MACD_DIF'].ewm(span=signal).mean()

        # 计算MACD柱状图
        df['MACD_histogram'] = df['MACD_DIF'] - df['MACD_DEA']

        # MACD位置关系
        df['MACD_DIF_above_DEA'] = df['MACD_DIF'] > df['MACD_DEA']
        df['MACD_DIF_below_DEA'] = df['MACD_DIF'] < df['MACD_DEA']

        # MACD交叉信号
        df['MACD_golden_cross'] = (df['MACD_DIF'] > df['MACD_DEA']) & (
                df['MACD_DIF'].shift(1) <= df['MACD_DEA'].shift(1))
        df['MACD_death_cross'] = (df['MACD_DIF'] < df['MACD_DEA']) & (
                df['MACD_DIF'].shift(1) >= df['MACD_DEA'].shift(1))

        # MACD零线位置
        df['MACD_DIF_above_zero'] = df['MACD_DIF'] > 0
        df['MACD_DIF_below_zero'] = df['MACD_DIF'] < 0
        df['MACD_cross_zero_up'] = (df['MACD_DIF'] > 0) & (df['MACD_DIF'].shift(1) <= 0)
        df['MACD_cross_zero_down'] = (df['MACD_DIF'] < 0) & (df['MACD_DIF'].shift(1) >= 0)

        # MACD动量
        df['MACD_DIF_momentum'] = df['MACD_DIF'] - df['MACD_DIF'].shift(1)
        df['MACD_DEA_momentum'] = df['MACD_DEA'] - df['MACD_DEA'].shift(1)
        df['MACD_hist_momentum'] = df['MACD_histogram'] - df['MACD_histogram'].shift(1)

        # MACD强度指标
        df['MACD_DIF_strength'] = abs(df['MACD_DIF']) / df['close']
        df['MACD_DEA_strength'] = abs(df['MACD_DEA']) / df['close']
        df['MACD_hist_strength'] = abs(df['MACD_histogram']) / df['close']

        # MACD背离检测
        df['MACD_bullish_divergence'] = (df['close'] < df['close'].shift(5)) & (
                df['MACD_DIF'] > df['MACD_DIF'].shift(5))
        df['MACD_bearish_divergence'] = (df['close'] > df['close'].shift(5)) & (
                df['MACD_DIF'] < df['MACD_DIF'].shift(5))

        # MACD趋势一致性
        df['MACD_trend_consistency'] = ((df['MACD_DIF'] > 0) & (df['MACD_DIF'] > df['MACD_DIF'].shift(1)) &
                                        (df['MACD_histogram'] > 0)).astype(int)

        # MACD超买超卖
        df['MACD_overbought'] = (df['MACD_DIF'] > df['MACD_DIF'].rolling(20).quantile(0.8)).astype(int)
        df['MACD_oversold'] = (df['MACD_DIF'] < df['MACD_DIF'].rolling(20).quantile(0.2)).astype(int)

        # MACD动量加速
        df['MACD_DIF_acceleration'] = df['MACD_DIF_momentum'] - df['MACD_DIF_momentum'].shift(1)
        df['MACD_DEA_acceleration'] = df['MACD_DEA_momentum'] - df['MACD_DEA_momentum'].shift(1)
        df['MACD_hist_acceleration'] = df['MACD_hist_momentum'] - df['MACD_hist_momentum'].shift(1)

        # MACD波动率
        df['MACD_DIF_volatility'] = df['MACD_DIF'].rolling(20).std()
        df['MACD_DEA_volatility'] = df['MACD_DEA'].rolling(20).std()
        df['MACD_hist_volatility'] = df['MACD_histogram'].rolling(20).std()

        # MACD相对强度
        df['MACD_DIF_relative_strength'] = df['MACD_DIF'] / (df['MACD_DIF_volatility'] + 1e-10)
        df['MACD_DEA_relative_strength'] = df['MACD_DEA'] / (df['MACD_DEA_volatility'] + 1e-10)
        df['MACD_hist_relative_strength'] = df['MACD_histogram'] / (df['MACD_hist_volatility'] + 1e-10)

        # MACD综合信号强度
        df['MACD_signal_strength'] = (
                df['MACD_golden_cross'].astype(int) * 3 +
                df['MACD_death_cross'].astype(int) * (-3) +
                df['MACD_cross_zero_up'].astype(int) * 2 +
                df['MACD_cross_zero_down'].astype(int) * (-2) +
                df['MACD_bullish_divergence'].astype(int) * 2 +
                df['MACD_bearish_divergence'].astype(int) * (-2) +
                df['MACD_trend_consistency'].astype(int) * 1 +
                (df['MACD_DIF_above_DEA']).astype(int) * 1 +
                (df['MACD_DIF_below_DEA']).astype(int) * (-1)
        )

        # MACD颜色编码 (用于绘图)
        df['MACD_DIF_color'] = np.where(df['MACD_DIF_above_DEA'], 1, 2)  # 1: 绿色, 2: 红色
        df['MACD_DEA_color'] = 3  # 3: 黄色
        df['MACD_hist_color'] = np.where(df['MACD_histogram'] > 0,
                                         np.where(df['MACD_histogram'] > df['MACD_histogram'].shift(1), 1, 2),
                                         # 1: 青色, 2: 蓝色
                                         np.where(df['MACD_histogram'] < df['MACD_histogram'].shift(1), 3,
                                                  4))  # 3: 红色, 4: 深红色

        return df

    def identify_smc_structure(self, df):
        """识别SMC市场结构（改进版）"""
        print("识别市场结构（改进版）...")

        # 初始化字段
        df['SMC_is_BOS_High'] = False
        df['SMC_is_BOS_Low'] = False
        df['SMC_is_CHoCH_High'] = False
        df['SMC_is_CHoCH_Low'] = False

        df['SMC_BOS_High_Value'] = None
        df['SMC_BOS_Low_Value'] = None
        df['SMC_CHoCH_High_Value'] = None
        df['SMC_CHoCH_Low_Value'] = None

        df['SMC_Weak_High'] = None
        df['SMC_Strong_Low'] = None

        # 枢轴点
        window = 10
        df['SMC_pivot_high'] = df['high'][
            (df['high'].shift(window) < df['high']) & (df['high'].shift(-window) < df['high'])]
        df['SMC_pivot_low'] = df['low'][(df['low'].shift(window) > df['low']) & (df['low'].shift(-window) > df['low'])]

        # 状态变量
        last_high = None
        last_low = None
        current_trend = None  # 'bullish', 'bearish', None
        bos_high_value = None
        bos_low_value = None
        choch_high_value = None
        choch_low_value = None

        in_bos_high_trend = False
        in_bos_low_trend = False

        for i in range(len(df)):
            row = df.iloc[i]

            # === 处理高点结构 ===
            if not pd.isna(row['SMC_pivot_high']):
                current_high = row['SMC_pivot_high']

                if last_high is not None and current_high > last_high:
                    # BOS 高点
                    df.at[i, 'SMC_is_BOS_High'] = True
                    bos_high_value = current_high
                    in_bos_high_trend = True
                    current_trend = 'bullish'

                elif last_low is not None and current_trend == 'bullish' and current_high > last_low:
                    # CHoCH 高点
                    df.at[i, 'SMC_is_CHoCH_High'] = True
                    choch_high_value = current_high
                    in_bos_high_trend = False  # 停止BOS高点延续
                    current_trend = 'bearish'

                last_high = current_high

            # === 处理低点结构 ===
            elif not pd.isna(row['SMC_pivot_low']):
                current_low = row['SMC_pivot_low']

                if last_low is not None and current_low < last_low:
                    # BOS 低点
                    df.at[i, 'SMC_is_BOS_Low'] = True
                    bos_low_value = current_low
                    in_bos_low_trend = True
                    current_trend = 'bearish'

                elif last_high is not None and current_trend == 'bearish' and current_low < last_high:
                    # CHoCH 低点
                    df.at[i, 'SMC_is_CHoCH_Low'] = True
                    choch_low_value = current_low
                    in_bos_low_trend = False  # 停止BOS低点延续
                    current_trend = 'bullish'

                last_low = current_low

            # === 持续表达区间值 ===
            # BOS 高点区间持续标记
            if in_bos_high_trend:
                df.at[i, 'SMC_is_BOS_High'] = True
                df.at[i, 'SMC_BOS_High_Value'] = bos_high_value

            # BOS 低点区间持续标记
            if in_bos_low_trend:
                df.at[i, 'SMC_is_BOS_Low'] = True
                df.at[i, 'SMC_BOS_Low_Value'] = bos_low_value

            # CHoCH 高点延续表达
            if choch_high_value is not None and not df.at[i, 'SMC_is_BOS_High']:
                df.at[i, 'SMC_is_CHoCH_High'] = True
                df.at[i, 'SMC_CHoCH_High_Value'] = choch_high_value

            # CHoCH 低点延续表达
            if choch_low_value is not None and not df.at[i, 'SMC_is_BOS_Low']:
                df.at[i, 'SMC_is_CHoCH_Low'] = True
                df.at[i, 'SMC_CHoCH_Low_Value'] = choch_low_value

        # ✅ 弱高点和强低点计算（持续表达）
        df = self._calculate_weak_high_strong_low(df)

        # ✅ 添加扫单字段
        df['SMC_swept_prev_high'] = df['high'] > df['high'].shift(1)
        df['SMC_swept_prev_low'] = df['low'] < df['low'].shift(1)

        # ✅ 添加订单块标记
        df['SMC_bullish_ob'] = (df['close'] > df['open']) & (df['volume'] > df['volume'].rolling(5).mean())
        df['SMC_bearish_ob'] = (df['close'] < df['open']) & (df['volume'] > df['volume'].rolling(5).mean())

        return df

    def _calculate_weak_high_strong_low(self, df):
        """改进版：持续表达 Weak High / Strong Low"""
        print("计算 Weak High 和 Strong Low（持续表达中）...")

        # 参数设置
        lookback_period = 20
        volume_threshold = 1.5
        body_ratio_threshold = 0.6

        # 初始化变量
        last_weak_high = None
        last_strong_low = None

        # 创建新列并初始化为 None（float 类型）
        df['SMC_Weak_High'] = np.nan
        df['SMC_Strong_Low'] = np.nan

        for i in range(len(df)):
            if i < lookback_period:
                # 前期数据不够，延续上一个值
                df.at[i, 'SMC_Weak_High'] = last_weak_high
                df.at[i, 'SMC_Strong_Low'] = last_strong_low
                continue

            # 提取当前行
            current_row = df.iloc[i]

            # 提取回看区间
            recent = df.iloc[i - lookback_period:i]
            recent_highs = recent['high'].values
            recent_lows = recent['low'].values
            recent_volumes = recent['volume'].values

            recent_body_ratios = abs(recent['close'] - recent['open']) / (recent['high'] - recent['low'])
            recent_body_ratios = recent_body_ratios.replace([np.inf, -np.inf], 0).fillna(0)

            avg_volume = np.mean(recent_volumes)
            avg_body_ratio = np.mean(recent_body_ratios)

            # ✅ 检测 Weak High
            if (
                    current_row['high'] >= np.percentile(recent_highs, 80) and
                    current_row['volume'] < avg_volume * volume_threshold and
                    current_row['body_ratio'] < body_ratio_threshold
            ):
                last_weak_high = current_row['high']

            # ✅ 检测 Strong Low
            if (
                    current_row['low'] <= np.percentile(recent_lows, 20) and
                    current_row['volume'] > avg_volume * volume_threshold and
                    current_row['body_ratio'] > body_ratio_threshold
            ):
                last_strong_low = current_row['low']

            # 持续表达
            df.at[i, 'SMC_Weak_High'] = last_weak_high
            df.at[i, 'SMC_Strong_Low'] = last_strong_low

        return df

    def calculate_luxalgo_smc_features(self, df):
        """计算LuxAlgo SMC核心特征"""
        print("计算LuxAlgo SMC特征...")

        # 1. 内部结构识别 (Internal Structure)
        df = self._calculate_internal_structure(df)

        # 2. 摆动结构识别 (Swing Structure)
        df = self._calculate_swing_structure(df)

        # 3. 订单块识别 (Order Blocks)
        df = self._calculate_order_blocks(df)

        # 4. 公平价值缺口 (Fair Value Gaps)
        df = self._calculate_fair_value_gaps(df)

        # 5. 等高/等低 (Equal Highs/Lows)
        df = self._calculate_equal_highs_lows(df)

        # 6. 溢价/折价区域 (Premium/Discount Zones)
        df = self._calculate_premium_discount_zones(df)

        # 7. 多时间框架水平 (Multi-Timeframe Levels)
        df = self._calculate_mtf_levels(df)

        return df

    def _calculate_internal_structure(self, df):
        """计算内部结构特征"""
        # 内部结构大小参数
        internal_size = 5

        # 识别内部关键点
        df['SMC_internal_pivot_high'] = df['high'][(df['high'].shift(internal_size) < df['high']) &
                                                   (df['high'].shift(-internal_size) < df['high'])]
        df['SMC_internal_pivot_low'] = df['low'][(df['low'].shift(internal_size) > df['low']) &
                                                 (df['low'].shift(-internal_size) > df['low'])]

        # 内部BOS和CHoCH
        df['SMC_internal_bullish_bos'] = False
        df['SMC_internal_bearish_bos'] = False
        df['SMC_internal_bullish_choch'] = False
        df['SMC_internal_bearish_choch'] = False

        last_internal_high = None
        last_internal_low = None
        internal_trend = None

        for i in range(len(df)):
            row = df.iloc[i]

            if not pd.isna(row['SMC_internal_pivot_high']):
                if last_internal_high is not None and row['SMC_internal_pivot_high'] > last_internal_high:
                    df.at[i, 'SMC_internal_bullish_bos'] = True
                    internal_trend = 'bullish'
                elif last_internal_high is not None and internal_trend == 'bullish':
                    df.at[i, 'SMC_internal_bearish_choch'] = True
                    internal_trend = 'bearish'
                last_internal_high = row['SMC_internal_pivot_high']

            elif not pd.isna(row['SMC_internal_pivot_low']):
                if last_internal_low is not None and row['SMC_internal_pivot_low'] < last_internal_low:
                    df.at[i, 'SMC_internal_bearish_bos'] = True
                    internal_trend = 'bearish'
                elif last_internal_low is not None and internal_trend == 'bearish':
                    df.at[i, 'SMC_internal_bullish_choch'] = True
                    internal_trend = 'bullish'
                last_internal_low = row['SMC_internal_pivot_low']

        # 内部结构强度
        df['SMC_internal_structure_strength'] = (
                df['SMC_internal_bullish_bos'].astype(int) +
                df['SMC_internal_bearish_bos'].astype(int) +
                df['SMC_internal_bullish_choch'].astype(int) +
                df['SMC_internal_bearish_choch'].astype(int)
        )

        return df

    def _calculate_swing_structure(self, df):
        """计算摆动结构特征"""
        # 摆动结构大小参数
        swing_size = 50

        # 识别摆动关键点
        df['SMC_swing_pivot_high'] = df['high'][(df['high'].shift(swing_size) < df['high']) &
                                                (df['high'].shift(-swing_size) < df['high'])]
        df['SMC_swing_pivot_low'] = df['low'][(df['low'].shift(swing_size) > df['low']) &
                                              (df['low'].shift(-swing_size) > df['low'])]

        # 摆动BOS和CHoCH
        df['SMC_swing_bullish_bos'] = False
        df['SMC_swing_bearish_bos'] = False
        df['SMC_swing_bullish_choch'] = False
        df['SMC_swing_bearish_choch'] = False

        last_swing_high = None
        last_swing_low = None
        swing_trend = None

        for i in range(len(df)):
            row = df.iloc[i]

            if not pd.isna(row['SMC_swing_pivot_high']):
                if last_swing_high is not None and row['SMC_swing_pivot_high'] > last_swing_high:
                    df.at[i, 'SMC_swing_bullish_bos'] = True
                    swing_trend = 'bullish'
                elif last_swing_high is not None and swing_trend == 'bullish':
                    df.at[i, 'SMC_swing_bearish_choch'] = True
                    swing_trend = 'bearish'
                last_swing_high = row['SMC_swing_pivot_high']

            elif not pd.isna(row['SMC_swing_pivot_low']):
                if last_swing_low is not None and row['SMC_swing_pivot_low'] < last_swing_low:
                    df.at[i, 'SMC_swing_bearish_bos'] = True
                    swing_trend = 'bearish'
                elif last_swing_low is not None and swing_trend == 'bearish':
                    df.at[i, 'SMC_swing_bullish_choch'] = True
                    swing_trend = 'bullish'
                last_swing_low = row['SMC_swing_pivot_low']

        # 摆动结构强度
        df['SMC_swing_structure_strength'] = (
                df['SMC_swing_bullish_bos'].astype(int) +
                df['SMC_swing_bearish_bos'].astype(int) +
                df['SMC_swing_bullish_choch'].astype(int) +
                df['SMC_swing_bearish_choch'].astype(int)
        )

        return df

    def _calculate_order_blocks(self, df):
        """计算订单块特征"""
        # 内部订单块
        df['SMC_internal_bullish_ob'] = False
        df['SMC_internal_bearish_ob'] = False

        # 摆动订单块
        df['SMC_swing_bullish_ob'] = False
        df['SMC_swing_bearish_ob'] = False

        # 计算body_ratio (如果不存在)
        if 'body_ratio' not in df.columns:
            df['body_ratio'] = abs(df['close'] - df['open']) / (df['high'] - df['low'])

        # 订单块识别逻辑
        for i in range(1, len(df)):
            current_row = df.iloc[i]
            prev_row = df.iloc[i - 1]

            # 内部订单块 (基于5根K线)
            if i >= 5:
                # 看涨内部订单块
                if (current_row['close'] > current_row['open'] and
                        current_row['volume'] > df['volume'].rolling(5).mean().iloc[i] and
                        current_row['body_ratio'] > 0.6):
                    df.at[i, 'SMC_internal_bullish_ob'] = True

                # 看跌内部订单块
                if (current_row['close'] < current_row['open'] and
                        current_row['volume'] > df['volume'].rolling(5).mean().iloc[i] and
                        current_row['body_ratio'] > 0.6):
                    df.at[i, 'SMC_internal_bearish_ob'] = True

            # 摆动订单块 (基于20根K线)
            if i >= 20:
                # 看涨摆动订单块
                if (current_row['close'] > current_row['open'] and
                        current_row['volume'] > df['volume'].rolling(20).mean().iloc[i] * 1.5 and
                        current_row['body_ratio'] > 0.7):
                    df.at[i, 'SMC_swing_bullish_ob'] = True

                # 看跌摆动订单块
                if (current_row['close'] < current_row['open'] and
                        current_row['volume'] > df['volume'].rolling(20).mean().iloc[i] * 1.5 and
                        current_row['body_ratio'] > 0.7):
                    df.at[i, 'SMC_swing_bearish_ob'] = True

        # 订单块强度
        df['SMC_order_block_strength'] = (
                df['SMC_internal_bullish_ob'].astype(int) +
                df['SMC_internal_bearish_ob'].astype(int) +
                df['SMC_swing_bullish_ob'].astype(int) +
                df['SMC_swing_bearish_ob'].astype(int)
        )

        return df

    def _calculate_fair_value_gaps(self, df):
        """计算公平价值缺口特征"""
        df['SMC_bullish_fvg'] = False
        df['SMC_bearish_fvg'] = False
        df['SMC_fvg_size'] = 0.0
        df['SMC_fvg_filled'] = False

        for i in range(2, len(df)):
            current_row = df.iloc[i]
            prev_row = df.iloc[i - 1]
            prev2_row = df.iloc[i - 2]

            # 看涨公平价值缺口
            if (current_row['low'] > prev2_row['high'] and
                    prev_row['close'] > prev2_row['high']):
                df.at[i, 'SMC_bullish_fvg'] = True
                df.at[i, 'SMC_fvg_size'] = current_row['low'] - prev2_row['high']

            # 看跌公平价值缺口
            elif (current_row['high'] < prev2_row['low'] and
                  prev_row['close'] < prev2_row['low']):
                df.at[i, 'SMC_bearish_fvg'] = True
                df.at[i, 'SMC_fvg_size'] = prev2_row['low'] - current_row['high']

            # 检查缺口是否被填补
            if i > 2:
                if df.iloc[i - 1]['SMC_bullish_fvg']:
                    if current_row['low'] <= df.iloc[i - 1]['low']:
                        df.at[i - 1, 'SMC_fvg_filled'] = True
                elif df.iloc[i - 1]['SMC_bearish_fvg']:
                    if current_row['high'] >= df.iloc[i - 1]['high']:
                        df.at[i - 1, 'SMC_fvg_filled'] = True

        # 公平价值缺口强度
        df['SMC_fvg_strength'] = df['SMC_fvg_size'] / df['ATR']

        return df

    def _calculate_equal_highs_lows(self, df):
        """计算等高/等低特征"""
        df['equal_highs'] = False
        df['equal_lows'] = False
        df['equal_highs_count'] = 0
        df['equal_lows_count'] = 0

        # 等高/等低检测参数
        threshold = 0.1  # ATR的10%作为阈值
        confirmation_bars = 3

        for i in range(confirmation_bars, len(df)):
            current_row = df.iloc[i]
            atr_threshold = current_row['ATR'] * threshold

            # 检测等高
            high_level = current_row['high']
            equal_high_count = 0

            for j in range(1, confirmation_bars + 1):
                if abs(df.iloc[i - j]['high'] - high_level) <= atr_threshold:
                    equal_high_count += 1

            if equal_high_count >= 2:  # 至少2个高点
                df.at[i, 'equal_highs'] = True
                df.at[i, 'equal_highs_count'] = equal_high_count

            # 检测等低
            low_level = current_row['low']
            equal_low_count = 0

            for j in range(1, confirmation_bars + 1):
                if abs(df.iloc[i - j]['low'] - low_level) <= atr_threshold:
                    equal_low_count += 1

            if equal_low_count >= 2:  # 至少2个低点
                df.at[i, 'equal_lows'] = True
                df.at[i, 'equal_lows_count'] = equal_low_count

        return df

    def _calculate_premium_discount_zones(self, df):
        """计算溢价/折价区域特征"""
        # 计算最近的高点和低点
        df['recent_high'] = df['high'].rolling(window=50).max()
        df['recent_low'] = df['low'].rolling(window=50).min()

        # 计算区域边界
        df['premium_zone_top'] = df['recent_high']
        df['premium_zone_bottom'] = 0.95 * df['recent_high'] + 0.05 * df['recent_low']

        df['discount_zone_top'] = 0.95 * df['recent_low'] + 0.05 * df['recent_high']
        df['discount_zone_bottom'] = df['recent_low']

        df['equilibrium_zone_top'] = 0.525 * df['recent_high'] + 0.475 * df['recent_low']
        df['equilibrium_zone_bottom'] = 0.525 * df['recent_low'] + 0.475 * df['recent_high']

        # 判断价格位置
        df['in_premium_zone'] = (df['close'] >= df['premium_zone_bottom']) & (df['close'] <= df['premium_zone_top'])
        df['in_discount_zone'] = (df['close'] >= df['discount_zone_bottom']) & (df['close'] <= df['discount_zone_top'])
        df['in_equilibrium_zone'] = (df['close'] >= df['equilibrium_zone_bottom']) & (
                df['close'] <= df['equilibrium_zone_top'])

        # 区域强度
        df['zone_strength'] = (
                df['in_premium_zone'].astype(int) * 1 +
                df['in_discount_zone'].astype(int) * 2 +
                df['in_equilibrium_zone'].astype(int) * 0.5
        )

        return df

    def _calculate_mtf_levels(self, df):
        """计算多时间框架水平特征"""
        # 日线水平
        df['daily_high'] = df['high'].rolling(window=24).max()  # 假设4小时图，24根K线为1天
        df['daily_low'] = df['low'].rolling(window=24).min()

        # 周线水平
        df['weekly_high'] = df['high'].rolling(window=168).max()  # 24*7=168根K线为1周
        df['weekly_low'] = df['low'].rolling(window=168).min()

        # 月线水平
        df['monthly_high'] = df['high'].rolling(window=720).max()  # 24*30=720根K线为1月
        df['monthly_low'] = df['low'].rolling(window=720).min()

        # 价格相对于各时间框架水平的位置
        df['price_vs_daily'] = (df['close'] - df['daily_low']) / (df['daily_high'] - df['daily_low'])
        df['price_vs_weekly'] = (df['close'] - df['weekly_low']) / (df['weekly_high'] - df['weekly_low'])
        df['price_vs_monthly'] = (df['close'] - df['monthly_low']) / (df['monthly_high'] - df['monthly_low'])

        # 多时间框架支撑阻力
        df['near_daily_high'] = (df['close'] >= df['daily_high'] * 0.98) & (df['close'] <= df['daily_high'])
        df['near_daily_low'] = (df['close'] >= df['daily_low']) & (df['close'] <= df['daily_low'] * 1.02)
        df['near_weekly_high'] = (df['close'] >= df['weekly_high'] * 0.98) & (df['close'] <= df['weekly_high'])
        df['near_weekly_low'] = (df['close'] >= df['weekly_low']) & (df['close'] <= df['weekly_low'] * 1.02)
        df['near_monthly_high'] = (df['close'] >= df['monthly_high'] * 0.98) & (df['close'] <= df['monthly_high'])
        df['near_monthly_low'] = (df['close'] >= df['monthly_low']) & (df['close'] <= df['monthly_low'] * 1.02)

        # MTF强度
        df['mtf_strength'] = (
                df['near_daily_high'].astype(int) + df['near_daily_low'].astype(int) +
                df['near_weekly_high'].astype(int) + df['near_weekly_low'].astype(int) +
                df['near_monthly_high'].astype(int) + df['near_monthly_low'].astype(int)
        )

        return df

    def calculate_squeeze_momentum_features(self, df):
        """计算Squeeze Momentum指标特征 (LazyBear版本)"""
        print("计算Squeeze Momentum特征...")

        # 参数设置
        bb_length = 20
        bb_mult = 2.0
        kc_length = 20
        kc_mult = 1.5
        use_true_range = True

        # 计算布林带 (Bollinger Bands)
        source = df['close']
        basis = source.rolling(window=bb_length).mean()
        dev = bb_mult * source.rolling(window=bb_length).std()
        upper_bb = basis + dev
        lower_bb = basis - dev

        # 计算肯特纳通道 (Keltner Channel)
        ma = source.rolling(window=kc_length).mean()

        if use_true_range:
            # 使用真实波幅 (True Range)
            tr1 = df['high'] - df['low']
            tr2 = abs(df['high'] - df['close'].shift(1))
            tr3 = abs(df['low'] - df['close'].shift(1))
            range_series = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        else:
            # 使用高低价差
            range_series = df['high'] - df['low']

        rangema = range_series.rolling(window=kc_length).mean()
        upper_kc = ma + rangema * kc_mult
        lower_kc = ma - rangema * kc_mult

        # 挤压状态判断
        df['SMI_squeeze_on'] = (lower_bb > lower_kc) & (upper_bb < upper_kc)
        df['SMI_squeeze_off'] = (lower_bb < lower_kc) & (upper_bb > upper_kc)
        df['SMI_no_squeeze'] = (~df['SMI_squeeze_on']) & (~df['SMI_squeeze_off'])

        # 计算动量值
        highest_high = df['high'].rolling(window=kc_length).max()
        lowest_low = df['low'].rolling(window=kc_length).min()
        avg_hl = (highest_high + lowest_low) / 2
        avg_avg_hl = avg_hl.rolling(window=kc_length).mean()
        sma_close = source.rolling(window=kc_length).mean()

        # 线性回归计算动量
        momentum_val = self._calculate_linear_regression(
            source - avg_avg_hl,
            sma_close,
            kc_length
        )

        df['SMI_squeeze_momentum'] = momentum_val

        # 动量颜色和状态
        df['SMI_momentum_color'] = 0  # 0: 灰色, 1: 绿色, 2: 红色, 3: 蓝色, 4: 黑色
        df['SMI_squeeze_color'] = 0  # 0: 蓝色, 1: 黑色, 2: 灰色

        for i in range(1, len(df)):
            current_val = df.iloc[i]['SMI_squeeze_momentum']
            prev_val = df.iloc[i - 1]['SMI_squeeze_momentum']

            # 动量颜色判断
            if current_val > 0:
                if current_val > prev_val:
                    df.at[i, 'SMI_momentum_color'] = 1  # 绿色 (lime)
                else:
                    df.at[i, 'SMI_momentum_color'] = 2  # 绿色 (green)
            else:
                if current_val < prev_val:
                    df.at[i, 'SMI_momentum_color'] = 3  # 红色 (red)
                else:
                    df.at[i, 'SMI_momentum_color'] = 4  # 红色 (maroon)

            # 挤压颜色判断
            if df.iloc[i]['SMI_no_squeeze']:
                df.at[i, 'SMI_squeeze_color'] = 0  # 蓝色
            elif df.iloc[i]['SMI_squeeze_on']:
                df.at[i, 'SMI_squeeze_color'] = 1  # 黑色
            else:
                df.at[i, 'SMI_squeeze_color'] = 2  # 灰色

        # 挤压状态强度
        df['SMI_squeeze_strength'] = (
                df['SMI_squeeze_on'].astype(int) * 2 +
                df['SMI_squeeze_off'].astype(int) * 1 +
                df['SMI_no_squeeze'].astype(int) * 0
        )

        # 动量强度
        df['SMI_momentum_strength'] = abs(df['SMI_squeeze_momentum'])
        df['SMI_momentum_acceleration'] = df['SMI_squeeze_momentum'].diff()

        # 挤压突破信号
        df['SMI_squeeze_breakout_bullish'] = (
                (df['SMI_squeeze_on'].shift(1) == True) &
                (df['SMI_squeeze_off'] == True) &
                (df['SMI_squeeze_momentum'] > 0)
        )

        df['SMI_squeeze_breakout_bearish'] = (
                (df['SMI_squeeze_on'].shift(1) == True) &
                (df['SMI_squeeze_off'] == True) &
                (df['SMI_squeeze_momentum'] < 0)
        )

        # 动量反转信号
        df['SMI_momentum_reversal_bullish'] = (
                (df['SMI_squeeze_momentum'].shift(1) < 0) &
                (df['SMI_squeeze_momentum'] > 0) &
                (df['SMI_momentum_acceleration'] > 0)
        )

        df['SMI_momentum_reversal_bearish'] = (
                (df['SMI_squeeze_momentum'].shift(1) > 0) &
                (df['SMI_squeeze_momentum'] < 0) &
                (df['SMI_momentum_acceleration'] < 0)
        )

        # 综合挤压动量信号
        df['SMI_squeeze_momentum_signal'] = (
                df['SMI_squeeze_breakout_bullish'].astype(int) * 3 +
                df['SMI_squeeze_breakout_bearish'].astype(int) * 3 +
                df['SMI_momentum_reversal_bullish'].astype(int) * 2 +
                df['SMI_momentum_reversal_bearish'].astype(int) * 2 +
                (df['SMI_squeeze_momentum'] > 0).astype(int) * 1 +
                (df['SMI_squeeze_momentum'] < 0).astype(int) * (-1)
        )

        return df

    def _calculate_linear_regression(self, x, y, length):
        """计算线性回归"""
        momentum_values = []

        for i in range(length, len(x)):
            x_window = x.iloc[i - length + 1:i + 1].values
            y_window = y.iloc[i - length + 1:i + 1].values

            if len(x_window) == length and len(y_window) == length:
                # 简单的线性回归计算
                x_mean = np.mean(x_window)
                y_mean = np.mean(y_window)

                numerator = np.sum((x_window - x_mean) * (y_window - y_mean))
                denominator = np.sum((x_window - x_mean) ** 2)

                if denominator != 0:
                    slope = numerator / denominator
                    intercept = y_mean - slope * x_mean
                    # 预测当前值
                    current_x = x.iloc[i]
                    predicted_y = slope * current_x + intercept
                    momentum_values.append(predicted_y)
                else:
                    momentum_values.append(0)
            else:
                momentum_values.append(0)

        # 填充前面的值
        full_momentum = [0] * length + momentum_values

        return pd.Series(full_momentum, index=x.index)

    def calculate_advanced_features(self, df):
        """计算高级技术指标"""
        print("计算高级技术指标...")

        # 基础技术指标
        df['range'] = df['high'] - df['low']
        df['body_range'] = abs(df['close'] - df['open'])
        df['wick_ratio'] = (df['range'] - df['body_range']) / df['range']
        df['body_ratio'] = df['body_range'] / df['range']

        # 价格位置指标
        df['price_position'] = (df['close'] - df['low']) / df['range']
        df['relative_position'] = (df['close'] - df['MA_20']) / df['MA_20']
        df['price_to_ma_ratio'] = df['close'] / df['MA_20']

        # 成交量指标
        df['volume_ratio'] = df['volume'] / df['Volume_MA_5']
        df['volume_price_trend'] = df['volume'] * (df['close'] - df['open']) / abs(df['close'] - df['open'])
        df['volume_ma_ratio'] = df['volume'] / df['volume'].rolling(20).mean()

        # 趋势强度指标
        df['trend_strength'] = abs(df['MA_5'] - df['MA_20']) / df['MA_20']
        df['momentum_ratio'] = df['Momentum_10'] / df['close']
        df['roc_ratio'] = df['ROC_5'] / 100
        df['trend_consistency'] = (
                (df['close'] > df['MA_5']) & (df['MA_5'] > df['MA_10']) & (df['MA_10'] > df['MA_20'])).astype(int)

        # 波动率指标
        df['volatility_ratio'] = df['ATR'] / df['close']
        df['price_volatility'] = df['close'].rolling(20).std() / df['close'].rolling(20).mean()
        df['volume_volatility'] = df['volume'].rolling(20).std() / df['volume'].rolling(20).mean()

        # 布林带指标
        df['bb_position'] = (df['close'] - df['Bollinger_Lower']) / (df['Bollinger_Upper'] - df['Bollinger_Lower'])

        # RSI相关指标
        df['rsi_divergence'] = df['RSI6'] - df['RSI12']
        df['rsi_momentum'] = df['RSI6'] - df['RSI6'].shift(1)
        df['rsi_oversold'] = (df['RSI6'] < 30).astype(int)
        df['rsi_overbought'] = (df['RSI6'] > 70).astype(int)

        # 支撑阻力指标
        df['support_distance'] = (df['close'] - df['low'].rolling(20).min()) / df['close']
        df['resistance_distance'] = (df['high'].rolling(20).max() - df['close']) / df['close']

        # 资金流向指标
        df['money_flow'] = ((df['close'] - df['low']) - (df['high'] - df['close'])) / (df['high'] - df['low'])
        df['money_flow_volume'] = df['money_flow'] * df['volume']

        # 市场结构指标
        df['structure_break'] = ((df['SMC_is_BOS_High'] == True) | (df['SMC_is_CHoCH_High'] == True)).astype(int)
        df['sweep_signal'] = ((df['SMC_swept_prev_high'] == True) | (df['SMC_swept_prev_low'] == True)).astype(int)
        df['structure_strength'] = df['structure_break'] * df['volume_ratio']

        # 机构行为指标
        df['institutional_volume'] = (df['volume'] > df['Volume_MA_5'] * 1.5) & (df['body_ratio'] > 0.6)
        df['large_order_flow'] = (df['volume'] > df['volume'].rolling(50).quantile(0.9)) & (df['body_ratio'] > 0.7)

        # 流动性指标
        df['liquidity_ratio'] = df['volume'] / df['ATR']
        df['liquidity_ma'] = df['liquidity_ratio'].rolling(20).mean()
        df['liquidity_signal'] = df['liquidity_ratio'] > df['liquidity_ma'] * 1.2

        # 市场情绪指标
        df['fear_greed'] = (df['RSI6'] - 50) / 50
        df['market_sentiment'] = df['fear_greed'] * df['volume_ratio']

        # 价格动量指标
        df['price_momentum'] = (df['close'] - df['close'].shift(5)) / df['close'].shift(5)
        df['volume_momentum'] = (df['volume'] - df['volume'].shift(5)) / df['volume'].shift(5)
        df['momentum_acceleration'] = df['price_momentum'] - df['price_momentum'].shift(1)

        # 市场效率指标
        df['market_efficiency'] = abs(df['close'] - df['close'].shift(1)).rolling(20).sum() / (
                df['high'].rolling(20).max() - df['low'].rolling(20).min())
        df['efficiency_ratio'] = df['market_efficiency'] / df['market_efficiency'].rolling(50).mean()

        # 波动收缩指标
        range_ma = df['range'].rolling(20).mean()
        range_ma = range_ma.fillna(df['range'].mean())
        df['volatility_contraction'] = df['range'] < range_ma * 0.8

        # 移动平均斜率
        df['ma5_slope'] = (df['MA_5'] - df['MA_5'].shift(1)) / df['MA_5'].shift(1)
        df['ma10_slope'] = (df['MA_10'] - df['MA_10'].shift(1)) / df['MA_10'].shift(1)
        df['ma20_slope'] = (df['MA_20'] - df['MA_20'].shift(1)) / df['MA_20'].shift(1)

        # RSI变化
        df['rsi_change'] = df['RSI6'] - df['RSI6'].shift(1)
        df['rsi_acceleration'] = df['rsi_change'] - df['rsi_change'].shift(1)

        # ==================== CMACD指标计算 ====================
        print("计算CMACD多时间框架MACD指标...")

        # MACD参数
        fast_length = 12
        slow_length = 26
        signal_length = 9

        # 计算EMA
        df['CMACD_fast_ema'] = df['close'].ewm(span=fast_length).mean()
        df['CMACD_slow_ema'] = df['close'].ewm(span=slow_length).mean()

        # 计算MACD线
        df['CMACD_macd'] = df['CMACD_fast_ema'] - df['CMACD_slow_ema']

        # 使用SMA作为Signal线，确保与Pine一致
        df['CMACD_signal'] = df['CMACD_macd'].rolling(window=signal_length, min_periods=signal_length).mean()

        # 计算直方图
        df['CMACD_histogram'] = df['CMACD_macd'] - df['CMACD_signal']

        # 多时间框架
        df['CMACD_mtf_4h_macd'] = df['CMACD_macd']
        df['CMACD_mtf_4h_signal'] = df['CMACD_signal']
        df['CMACD_mtf_4h_hist'] = df['CMACD_histogram']

        df['CMACD_mtf_1h_macd'] = df['CMACD_macd'].rolling(window=4).mean()
        df['CMACD_mtf_1h_signal'] = df['CMACD_signal'].rolling(window=4).mean()
        df['CMACD_mtf_1h_hist'] = df['CMACD_histogram'].rolling(window=4).mean()

        df['CMACD_mtf_1d_macd'] = df['CMACD_macd'].rolling(window=6).mean()
        df['CMACD_mtf_1d_signal'] = df['CMACD_signal'].rolling(window=6).mean()
        df['CMACD_mtf_1d_hist'] = df['CMACD_histogram'].rolling(window=6).mean()

        # 信号状态
        df['CMACD_macd_above_signal'] = df['CMACD_macd'] >= df['CMACD_signal']
        df['CMACD_macd_below_signal'] = df['CMACD_macd'] < df['CMACD_signal']

        df['CMACD_hist_A_up'] = (df['CMACD_histogram'] > df['CMACD_histogram'].shift(1)) & (df['CMACD_histogram'] > 0)
        df['CMACD_hist_A_down'] = (df['CMACD_histogram'] < df['CMACD_histogram'].shift(1)) & (df['CMACD_histogram'] > 0)
        df['CMACD_hist_B_down'] = (df['CMACD_histogram'] < df['CMACD_histogram'].shift(1)) & (
                df['CMACD_histogram'] <= 0)
        df['CMACD_hist_B_up'] = (df['CMACD_histogram'] > df['CMACD_histogram'].shift(1)) & (df['CMACD_histogram'] <= 0)

        df['CMACD_macd_color'] = np.where(df['CMACD_macd_above_signal'], 1, 2)
        df['CMACD_signal_color'] = 3

        df['CMACD_hist_color'] = np.where(df['CMACD_hist_A_up'], 1,
                                          np.where(df['CMACD_hist_A_down'], 2,
                                                   np.where(df['CMACD_hist_B_down'], 3,
                                                            np.where(df['CMACD_hist_B_up'], 4, 5))))

        df['CMACD_cross_up'] = (df['CMACD_macd'] > df['CMACD_signal']) & (
                df['CMACD_macd'].shift(1) <= df['CMACD_signal'].shift(1))
        df['CMACD_cross_down'] = (df['CMACD_macd'] < df['CMACD_signal']) & (
                df['CMACD_macd'].shift(1) >= df['CMACD_signal'].shift(1))

        df['CMACD_macd_momentum'] = df['CMACD_macd'] - df['CMACD_macd'].shift(1)
        df['CMACD_signal_momentum'] = df['CMACD_signal'] - df['CMACD_signal'].shift(1)
        df['CMACD_hist_momentum'] = df['CMACD_histogram'] - df['CMACD_histogram'].shift(1)

        df['CMACD_strength'] = abs(df['CMACD_macd']) / df['close']
        df['CMACD_signal_strength'] = abs(df['CMACD_signal']) / df['close']
        df['CMACD_hist_strength'] = abs(df['CMACD_histogram']) / df['close']

        df['CMACD_bullish_divergence'] = (df['close'] < df['close'].shift(5)) & (
                df['CMACD_macd'] > df['CMACD_macd'].shift(5))
        df['CMACD_bearish_divergence'] = (df['close'] > df['close'].shift(5)) & (
                df['CMACD_macd'] < df['CMACD_macd'].shift(5))

        df['CMACD_trend_consistency'] = ((df['CMACD_macd'] > 0) & (df['CMACD_macd'] > df['CMACD_macd'].shift(1)) &
                                         (df['CMACD_histogram'] > 0)).astype(int)

        df['CMACD_overbought'] = (df['CMACD_macd'] > df['CMACD_macd'].rolling(20).quantile(0.8)).astype(int)
        df['CMACD_oversold'] = (df['CMACD_macd'] < df['CMACD_macd'].rolling(20).quantile(0.2)).astype(int)

        df['CMACD_above_zero'] = df['CMACD_macd'] > 0
        df['CMACD_below_zero'] = df['CMACD_macd'] < 0
        df['CMACD_cross_zero_up'] = (df['CMACD_macd'] > 0) & (df['CMACD_macd'].shift(1) <= 0)
        df['CMACD_cross_zero_down'] = (df['CMACD_macd'] < 0) & (df['CMACD_macd'].shift(1) >= 0)

        df['CMACD_mtf_consistency'] = ((df['CMACD_mtf_4h_macd'] > df['CMACD_mtf_4h_signal']) &
                                       (df['CMACD_mtf_1h_macd'] > df['CMACD_mtf_1h_signal']) &
                                       (df['CMACD_mtf_1d_macd'] > df['CMACD_mtf_1d_signal'])).astype(int)

        df['CMACD_signal_strength'] = (
                df['CMACD_cross_up'].astype(int) * 3 +
                df['CMACD_cross_down'].astype(int) * 3 +
                df['CMACD_cross_zero_up'].astype(int) * 2 +
                df['CMACD_cross_zero_down'].astype(int) * 2 +
                df['CMACD_bullish_divergence'].astype(int) * 2 +
                df['CMACD_bearish_divergence'].astype(int) * 2 +
                df['CMACD_trend_consistency'].astype(int) * 1 +
                df['CMACD_macd_above_signal'].astype(int) * 1 +
                df['CMACD_macd_below_signal'].astype(int) * (-1)
        )

        df['CMACD_momentum_acceleration'] = df['CMACD_macd_momentum'] - df['CMACD_macd_momentum'].shift(1)
        df['CMACD_signal_acceleration'] = df['CMACD_signal_momentum'] - df['CMACD_signal_momentum'].shift(1)
        df['CMACD_hist_acceleration'] = df['CMACD_hist_momentum'] - df['CMACD_hist_momentum'].shift(1)

        df['CMACD_volatility'] = df['CMACD_macd'].rolling(20).std()
        df['CMACD_signal_volatility'] = df['CMACD_signal'].rolling(20).std()
        df['CMACD_hist_volatility'] = df['CMACD_histogram'].rolling(20).std()

        df['CMACD_relative_strength'] = df['CMACD_macd'] / (df['CMACD_volatility'] + 1e-10)
        df['CMACD_signal_relative_strength'] = df['CMACD_signal'] / (df['CMACD_signal_volatility'] + 1e-10)
        df['CMACD_hist_relative_strength'] = df['CMACD_histogram'] / (df['CMACD_hist_volatility'] + 1e-10)

        return df

    def _create_normalized_label_data(self, df):
        """创建用于标签计算的独立归一化数据框"""
        # 创建独立的副本，避免影响原始数据
        label_df = df.copy()

        # 需要归一化的数值列
        numeric_columns = [
            'RSI6', 'RSI12', 'RSI24', 'K', 'D', 'J',
            'CMACD_macd', 'CMACD_signal', 'CMACD_histogram',
            'volume_ratio', 'body_ratio', 'trend_strength', 'momentum_ratio',
            'drawdown_ratio', 'volatility_ratio', 'price_volatility',
            'volume_volatility', 'support_distance', 'resistance_distance',
            'money_flow_volume', 'CMACD_momentum_acceleration',
            'CMACD_signal_strength', 'trend_consistency'
        ]

        # 只对存在的列进行归一化
        existing_columns = [col for col in numeric_columns if col in label_df.columns]

        # 创建完整的数据框副本，包含所有列
        normalized_df = label_df.copy()

        # 只对数值列进行归一化
        if existing_columns:
            scaler = MinMaxScaler()
            normalized_df[existing_columns] = scaler.fit_transform(label_df[existing_columns])

        return normalized_df

    def generate_smc_labels(self, df):
        """生成优化后的SMC策略标签，包括 market_state """
        print("生成优化后的SMC策略标签...")

        # 创建独立的归一化数据框
        label_df = self._create_normalized_label_data(df)

        def determine_label(row):
            try:

                # 动态阈值参数（基于归一化数据调整）
                RSI_LOWER_BOUND = 0.3  # 归一化后的30%
                RSI_UPPER_BOUND = 0.65  # 归一化后的65%
                VOLUME_MULTIPLIER = 0.7  # 归一化后的阈值

                # ✅ 优化后的买入信号条件
                buy_conditions = [

                    # 规则线
                    (
                        (row.get('SMC_is_BOS_Low') == True) and
                        (row.get('SMC_BOS_Low_Value', 0) is not None) and
                        (abs(row.get('open', 0) - row.get('SMC_BOS_Low_Value', 0)) <= row.get('SMC_BOS_Low_Value',
                                                                                              0) * 0.04) and  # 允许2%的容差

                        (row.get('CMACD_macd', 0) < row.get('CMACD_signal', 0)) and
                        (row.get('RSI6', 0) < row.get('RSI24', 0)) and
                        (row.get('J', 0) < row.get('K', 0)),

                        '底部动能确认',
                        13
                    ),

                    # 规则线
                    (
                        (row.get('SMC_is_BOS_Low') == True) and
                        (row.get('SMC_BOS_Low_Value', 0) is not None) and
                        (abs(row.get('open', 0) - row.get('SMC_BOS_Low_Value', 0)) <= row.get('SMC_BOS_Low_Value',
                                                                                              0) * 0.02),  # 允许2%的容差
                        '底部动能确认',
                        10
                    ),

                    # 规则线
                    (
                        (row.get('SMC_is_BOS_Low') == True) and
                        (row.get('CMACD_histogram', 0) < 0.5) and
                        (row.get('RSI6', 0) < row.get('RSI24', 0)) and
                        (row.get('CMACD_macd', 0) < row.get('CMACD_signal', 0)),
                        '底部bos确认',
                        8
                    ),

                    # 结构突破 + 动能共振（低位）
                    (
                        (row.get('SMC_is_BOS_Low') == True) and
                        (row.get('CMACD_histogram', 0) < 0.5) and
                        (row.get('RSI6', 0) < 0.5),
                        '结构突破+动能确认',
                        5
                    ),

                    # CMACD 金叉 + 动能增强 + 非顶部
                    (
                        (row.get('CMACD_cross_up') == True) and
                        (row.get('CMACD_histogram', -1) > row.get('CMACD_histogram_prev', -2)) and
                        (row.get('CMACD_histogram', -1) > 0) and
                        (row.get('RSI6', 0) < 0.55),
                        'CMACD金叉+动能增强',
                        6
                    ),

                    # RSI 超跌反弹 + 成交量放大 + 大阳线确认
                    (
                        (row.get('RSI6', 0) < 0.4) and
                        (row.get('volume_ratio', 0) > 1.2) and
                        (row.get('body_ratio', 0) > 0.6) and
                        (row.get('close', 0) > row.get('open', 0)),
                        '超跌反弹确认',
                        4
                    ),

                    # 支撑确认 + 非顶部 RSI
                    (
                        (row.get('close', 0) > row.get('MA_10', 0)) and
                        (row.get('support_distance', 1) < 0.25) and
                        (row.get('RSI6', 0) < 0.6),
                        '支撑确认',
                        3
                    ),

                    # 资金流入+低位条件
                    (
                        (row.get('money_flow_volume', 0) > 0.5) and
                        (row.get('volume_ratio', 0) > 1.2) and
                        (row.get('RSI6', 0) < 0.5),
                        '低位资金流入',
                        4
                    ),

                    # 大阳线反转 + RSI 不过热
                    (
                        (row.get('body_ratio', 0) > 0.8) and
                        (row.get('close', 0) > row.get('open', 0)) and
                        (row.get('RSI6', 0) < 0.6),
                        '大阳线反转',
                        4
                    )
                ]

                # ✅ 优化后的卖出信号条件 - 重点改进
                sell_conditions = [

                    # 规则线
                    (
                        (row.get('SMC_is_BOS_High') == True) and
                        (row.get('SMC_BOS_High_Value', 0) is not None) and
                        (abs(row.get('open', 0) - row.get('SMC_BOS_High_Value', 0)) <= row.get('SMC_BOS_High_Value',
                                                                                               0) * 0.02),  # 允许2%的容差
                        '顶部动能确认',
                        10
                    ),

                    # 1. 结构破坏信号（保持）
                    ((row['SMC_is_CHoCH_High'] == True) or (row['SMC_swept_prev_low'] == True), '结构破坏', 4),

                    # 2. 新增：价格动量衰竭信号
                    ((row['RSI6'] > 0.8) and (row['momentum_ratio'] < -0.3) and (row['volume_ratio'] < 0.6), '动量衰竭',
                     5),

                    # 3. 新增：背离信号检测
                    ((row['RSI6'] > 0.75) and (row['CMACD_macd'] < row['CMACD_signal']) and (row['volume_ratio'] < 0.7),
                     'RSI-CMACD背离', 6),

                    # 4. 新增：价格结构顶部信号
                    ((row['high'] > row['MA_20'] * 1.05) and (row['body_ratio'] < 0.4) and (row['volume_ratio'] < 0.8),
                     '价格顶部', 5),

                    # 5. 新增：成交量萎缩信号
                    ((row['volume_ratio'] < 0.5) and (row['close'] < row['open']) and (row['RSI6'] > 0.7), '成交量萎缩',
                     4),

                    # 6. 新增：趋势反转确认
                    ((row['MA_5'] < row['MA_10']) and (row['momentum_ratio'] < -0.2) and (row['trend_strength'] < 0.3),
                     '趋势反转', 4),

                    # 7. 新增：支撑破位信号
                    ((row['close'] < row['MA_20']) and (row['support_distance'] > 0.5), '支撑破位', 3),

                    # 8. 新增：超买区域信号
                    ((row['RSI6'] > 0.85) and (row['K'] > 0.8) and (row['volume_ratio'] < 0.6), '超买区域', 5)
                ]

                # 计算得分
                buy_score = sum(weight for condition, _, weight in buy_conditions if condition)
                sell_score = sum(weight for condition, _, weight in sell_conditions if condition)

                # 风险因子（基于归一化数据）
                risk_factors = [
                    row['drawdown_ratio'] > 0.5,  # 归一化后阈值调整
                    row['volatility_ratio'] > 0.6,  # 归一化后阈值调整
                    row['volume_volatility'] > 0.7  # 归一化后阈值调整
                ]
                risk_score = sum(risk_factors)

                # ✅ 优化后的阈值（基于归一化数据调整）
                min_buy_score = 15  # 提高买入门槛
                min_sell_score = 12  # 提高卖出门槛，减少误判

                if buy_score >= min_buy_score and buy_score > sell_score:
                    confidence = min(buy_score / 30, 1.0)  # 调整分母
                    risk_level = '低' if risk_score <= 1 else '中' if risk_score <= 2 else '高'
                    return ('1', confidence, '买入信号', risk_level)
                elif sell_score >= min_sell_score and sell_score > buy_score:
                    confidence = min(sell_score / 30, 1.0)  # 调整分母
                    risk_level = '低' if risk_score <= 1 else '中' if risk_score <= 2 else '高'
                    return ('2', confidence, '卖出信号', risk_level)
                else:
                    return ('0', 0.5, '无明显信号', '低')
            except Exception as e:
                return ('0', 0.5, '错误', '低')

        # 应用标签生成
        print(label_df.columns)
        results = label_df.apply(determine_label, axis=1)
        results_list = results.tolist()
        results_df = pd.DataFrame(results_list)
        results_df.columns = ['label', 'confidence', 'signal_reason', 'risk_level']

        # 添加结果列到原始数据框
        df['label'] = results_df['label']
        df['confidence'] = results_df['confidence']
        df['signal_reason'] = results_df['signal_reason']
        df['risk_level'] = results_df['risk_level']

        # 信号强度和质量
        df['signal_strength'] = df['confidence'] * df['volume_ratio']
        df['signal_quality'] = df['confidence'] * (1 - df['drawdown_ratio'])

        # ✅ 添加 market_state 列，避免 KeyError
        df['market_state'] = np.where(
            (df['trend_strength'] > 0.02) & (df['volume_ratio'] > 1.2) & (df['trend_consistency'] == 1),
            '强势',
            np.where(
                (df['trend_strength'] < -0.02) & (df['volume_ratio'] > 1.2),
                '弱势',
                '震荡'
            )
        )

        return df

    def process_complete_system(self, symbol, interval, start_date, end_date=None):
        """完整的交易系统处理流程（包含原油数据）"""
        print(f"🚀 开始处理 {symbol} {interval} 完整交易系统（包含原油数据）...")

        # 1. 获取数字货币历史数据
        df = self.get_historical_data(symbol, interval, start_date, end_date)

        if len(df) == 0:
            print("❌ 没有获取到数字货币数据，无法继续处理")
            return None

        print(f"✅ 数字货币数据获取完成，共 {len(df)} 条记录")
        print(f"数字货币时间范围: {df['open_time'].min()} 至 {df['open_time'].max()}")

        # 2. 根据数字货币数据的时间区间获取原油数据
        print("\n开始根据数字货币时间区间获取原油数据...")
        oil_df = self.get_oil_data_by_crypto_timerange(df, interval)

        # 3. 准备原油数据以便合并
        if len(oil_df) > 0:
            oil_resampled = self.prepare_oil_data_for_merge(oil_df, df)
        else:
            print("⚠️ 使用模拟原油数据...")
            oil_resampled = self._create_dummy_oil_data_for_crypto(df, interval)
            oil_resampled = self.prepare_oil_data_for_merge(oil_resampled, df)

        # 4. 合并数字货币和原油数据
        print("\n开始合并数字货币和原油数据...")
        df = self.merge_crypto_oil_data(df, oil_resampled)

        # 5. 计算基础技术指标（数字货币）
        df = self.calculate_basic_indicators(df)

        # 6. 计算原油相关指标
        df = self.calculate_indicators(df, prefix='oil_')

        # 7. 计算黄金相关指标
        df = self.calculate_indicators(df, prefix='gld_')

        # 8. 识别SMC结构
        df = self.identify_smc_structure(df)

        # 9. 计算LuxAlgo SMC特征
        df = self.calculate_luxalgo_smc_features(df)

        # 10. 计算Squeeze Momentum特征
        df = self.calculate_squeeze_momentum_features(df)

        # 11. 填充缺失值
        df.fillna({
            'RSI6': 0, 'RSI12': 0, 'RSI24': 0,
            'K': 50, 'D': 50, 'J': 50,
            'MA_5': df['close'], 'MA_10': df['close'],
            'MA_20': df['close'], 'MA_42': df['close'],
            # 原油指标填充
            'oil_RSI6': 50, 'oil_RSI12': 50, 'oil_RSI24': 50,
            'oil_MA_5': df['oil_close'], 'oil_MA_10': df['oil_close'],
            'oil_MA_20': df['oil_close']
        }, inplace=True)

        # 12. 计算高级特征（包含原油特征）
        df = self.calculate_advanced_features(df)

        # 13. 删除前50行（确保所有指标计算完整）
        if len(df) > 50:
            df = df.iloc[50:].reset_index(drop=True)

        # 14. 生成标签
        df = self.generate_smc_labels(df)

        # 15. 保存结果
        output_file = f"complete_dataset_{symbol}_{interval}_with_oil_data.csv"
        df.to_csv(output_file, index=False)

        # 16. 输出统计信息
        self._print_statistics(df, output_file)

        return df

    def _print_statistics(self, df, output_file):
        """打印统计信息"""
        print(f"\n=== 完整交易系统处理完成 (集成原油数据) ===")
        print(f"输出文件: {output_file}")
        print(f"数据条数: {len(df)}")
        print(f"\n标签分布:")
        print(df['label'].value_counts())
        print(f"\n置信度统计:")
        print(df['confidence'].describe())
        print(f"\n风险等级分布:")
        print(df['risk_level'].value_counts())
        print(f"\n市场状态分布:")
        print(df['market_state'].value_counts())

        # 原油数据统计
        if 'oil_close' in df.columns:
            print(f"\n=== 原油数据统计 ===")
            print(f"原油价格范围: ${df['oil_close'].min():.2f} - ${df['oil_close'].max():.2f}")
            print(f"原油平均价格: ${df['oil_close'].mean():.2f}")
            print(f"原油价格标准差: ${df['oil_close'].std():.2f}")

            if 'oil_market_state' in df.columns:
                print(f"\n原油市场状态分布:")
                print(df['oil_market_state'].value_counts())

            # 相关性统计
            if 'crypto_oil_price_corr' in df.columns:
                print(f"\n数字货币-原油相关性统计:")
                print(f"价格相关性均值: {df['crypto_oil_price_corr'].mean():.3f}")
                print(f"收益率相关性均值: {df['crypto_oil_returns_corr'].mean():.3f}")
                print(f"趋势一致性比例: {df['crypto_oil_trend_consistency'].mean():.3f}")

                # 强相关性时段统计
                strong_corr = df[abs(df['crypto_oil_price_corr']) > 0.5]
                print(f"强相关时段占比: {len(strong_corr) / len(df) * 100:.1f}%")

            # 原油RSI统计
            if 'oil_RSI6' in df.columns:
                print(f"\n原油技术指标统计:")
                print(f"原油RSI6均值: {df['oil_RSI6'].mean():.1f}")
                print(f"原油超买次数(RSI>70): {(df['oil_RSI6'] > 70).sum()}")
                print(f"原油超卖次数(RSI<30): {(df['oil_RSI6'] < 30).sum()}")

            # 原油成交量统计
            if 'oil_volume' in df.columns:
                print(f"原油成交量均值: {df['oil_volume'].mean():.0f}")
                if 'oil_volume_spike' in df.columns:
                    print(f"原油成交量异常次数: {df['oil_volume_spike'].sum()}")

        # LuxAlgo SMC特征统计
        print(f"\n=== LuxAlgo SMC特征统计 ===")

        # 内部结构统计
        internal_features = ['SMC_internal_bullish_bos', 'SMC_internal_bearish_bos',
                             'SMC_internal_bullish_choch', 'SMC_internal_bearish_choch']
        print(f"内部结构信号总数: {sum(df[feature].sum() for feature in internal_features)}")

        # 摆动结构统计
        swing_features = ['SMC_swing_bullish_bos', 'SMC_swing_bearish_bos',
                          'SMC_swing_bullish_choch', 'SMC_swing_bearish_choch']
        print(f"摆动结构信号总数: {sum(df[feature].sum() for feature in swing_features)}")

        # 订单块统计
        ob_features = ['SMC_internal_bullish_ob', 'SMC_internal_bearish_ob',
                       'SMC_swing_bullish_ob', 'SMC_swing_bearish_ob']
        print(f"订单块信号总数: {sum(df[feature].sum() for feature in ob_features)}")

        # 公平价值缺口统计
        fvg_count = df['SMC_bullish_fvg'].sum() + df['SMC_bearish_fvg'].sum()
        fvg_filled = df['SMC_fvg_filled'].sum()
        print(f"公平价值缺口总数: {fvg_count}")
        print(f"已填补缺口数: {fvg_filled}")
        print(f"未填补缺口数: {fvg_count - fvg_filled}")

        # 等高/等低统计
        print(f"等高信号数: {df['equal_highs'].sum()}")
        print(f"等低信号数: {df['equal_lows'].sum()}")

        # 区域统计
        zone_features = ['in_premium_zone', 'in_discount_zone', 'in_equilibrium_zone']
        zone_names = ['溢价区域', '折价区域', '均衡区域']
        for feature, name in zip(zone_features, zone_names):
            count = df[feature].sum()
            print(f"{name}信号数: {count}")

        # 多时间框架统计
        mtf_features = ['near_daily_high', 'near_daily_low', 'near_weekly_high',
                        'near_weekly_low', 'near_monthly_high', 'near_monthly_low']
        mtf_names = ['日线高点', '日线低点', '周线高点', '周线低点', '月线高点', '月线低点']
        for feature, name in zip(mtf_features, mtf_names):
            count = df[feature].sum()
            print(f"{name}信号数: {count}")

        # Squeeze Momentum统计
        print(f"\n=== Squeeze Momentum特征统计 ===")
        squeeze_features = ['SMI_squeeze_on', 'SMI_squeeze_off', 'SMI_no_squeeze']
        squeeze_names = ['挤压状态', '释放状态', '无挤压']
        for feature, name in zip(squeeze_features, squeeze_names):
            count = df[feature].sum()
            print(f"{name}信号数: {count}")

        # 动量信号统计
        momentum_features = ['SMI_squeeze_breakout_bullish', 'SMI_squeeze_breakout_bearish',
                             'SMI_momentum_reversal_bullish', 'SMI_momentum_reversal_bearish']
        momentum_names = ['看涨突破', '看跌突破', '看涨反转', '看跌反转']
        for feature, name in zip(momentum_features, momentum_names):
            count = df[feature].sum()
            print(f"{name}信号数: {count}")

        # 动量颜色统计
        color_counts = df['SMI_momentum_color'].value_counts()
        color_names = {0: '灰色', 1: '绿色(lime)', 2: '绿色(green)', 3: '红色(red)', 4: '红色(maroon)'}
        print(f"动量颜色分布:")
        for color_id, count in color_counts.items():
            color_name = color_names.get(color_id, f'颜色{color_id}')
            print(f"{color_name}: {count}")

        # 挤压颜色统计
        squeeze_color_counts = df['SMI_squeeze_color'].value_counts()
        squeeze_color_names = {0: '蓝色', 1: '黑色', 2: '灰色'}
        print(f"挤压颜色分布:")
        for color_id, count in squeeze_color_counts.items():
            color_name = squeeze_color_names.get(color_id, f'颜色{color_id}')
            print(f"{color_name}: {count}")

        # CMACD统计
        print(f"\n=== CMACD指标统计 ===")
        print(f"MACD金叉信号数: {df['CMACD_cross_up'].sum()}")
        print(f"MACD死叉信号数: {df['CMACD_cross_down'].sum()}")
        print(f"MACD零线上穿数: {df['CMACD_cross_zero_up'].sum()}")
        print(f"MACD零线下穿数: {df['CMACD_cross_zero_down'].sum()}")
        print(f"MACD看涨背离数: {df['CMACD_bullish_divergence'].sum()}")
        print(f"MACD看跌背离数: {df['CMACD_bearish_divergence'].sum()}")
        print(f"MACD趋势一致数: {df['CMACD_trend_consistency'].sum()}")
        print(f"MACD多时间框架一致数: {df['CMACD_mtf_consistency'].sum()}")

        # MACD直方图状态统计
        hist_features = ['CMACD_hist_A_up', 'CMACD_hist_A_down', 'CMACD_hist_B_down', 'CMACD_hist_B_up']
        hist_names = ['直方图A上涨', '直方图A下跌', '直方图B下跌', '直方图B上涨']
        for feature, name in zip(hist_features, hist_names):
            count = df[feature].sum()
            print(f"{name}信号数: {count}")

        # MACD颜色统计
        macd_color_counts = df['CMACD_macd_color'].value_counts()
        macd_color_names = {1: '绿色(lime)', 2: '红色(red)'}
        print(f"MACD颜色分布:")
        for color_id, count in macd_color_counts.items():
            color_name = macd_color_names.get(color_id, f'颜色{color_id}')
            print(f"{color_name}: {count}")

        # 直方图颜色统计
        hist_color_counts = df['CMACD_hist_color'].value_counts()
        hist_color_names = {1: '青色(aqua)', 2: '蓝色(blue)', 3: '红色(red)', 4: '深红色(maroon)', 5: '黄色(yellow)'}
        print(f"直方图颜色分布:")
        for color_id, count in hist_color_counts.items():
            color_name = hist_color_names.get(color_id, f'颜色{color_id}')
            print(f"{color_name}: {count}")

        # 信号质量分析
        signal_quality = df[df['label'] != '0'].copy()
        if len(signal_quality) > 0:
            print(f"\n=== 信号质量分析 (共{len(signal_quality)}个信号) ===")
            print(f"平均置信度: {signal_quality['confidence'].mean():.3f}")
            print(f"平均信号强度: {signal_quality['signal_strength'].mean():.3f}")
            print(f"平均信号质量: {signal_quality['signal_quality'].mean():.3f}")
            # 计算风险分数（基于risk_level）
            risk_score_mapping = {'低': 1, '中': 2, '高': 3}
            signal_quality['risk_score'] = signal_quality['risk_level'].map(risk_score_mapping)
            print(f"平均风险分数: {signal_quality['risk_score'].mean():.3f}")

            # LuxAlgo特征在信号中的分布
            luxalgo_features = {
                'LuxAlgo BOS': signal_quality['SMC_internal_bullish_bos'].sum() + signal_quality[
                    'SMC_swing_bullish_bos'].sum(),
                'LuxAlgo CHoCH': signal_quality['SMC_internal_bullish_choch'].sum() + signal_quality[
                    'SMC_swing_bullish_choch'].sum(),
                'LuxAlgo订单块': signal_quality['SMC_internal_bullish_ob'].sum() + signal_quality[
                    'SMC_swing_bullish_ob'].sum(),
                'LuxAlgo缺口': signal_quality['SMC_bullish_fvg'].sum() + signal_quality['SMC_bearish_fvg'].sum(),
                'LuxAlgo等高/等低': signal_quality['equal_highs'].sum() + signal_quality['equal_lows'].sum()
            }

            print(f"\nLuxAlgo特征在信号中的分布:")
            for feature, count in luxalgo_features.items():
                if count > 0:
                    print(f"{feature}: {count}")

                    # Squeeze Momentum特征在信号中的分布
        squeeze_features = {
            'Squeeze看涨突破': signal_quality['SMI_squeeze_breakout_bullish'].sum(),
            'Squeeze看跌突破': signal_quality['SMI_squeeze_breakout_bearish'].sum(),
            'Squeeze看涨反转': signal_quality['SMI_momentum_reversal_bullish'].sum(),
            'Squeeze看跌反转': signal_quality['SMI_momentum_reversal_bearish'].sum(),
            'Squeeze挤压状态': signal_quality['SMI_squeeze_on'].sum(),
            'Squeeze释放状态': signal_quality['SMI_squeeze_off'].sum()
        }

        print(f"\nSqueeze Momentum特征在信号中的分布:")
        for feature, count in squeeze_features.items():
            if count > 0:
                print(f"{feature}: {count}")

        # CMACD特征在信号中的分布
        cmacd_features = {
            'CMACD金叉': signal_quality['CMACD_cross_up'].sum(),
            'CMACD死叉': signal_quality['CMACD_cross_down'].sum(),
            'CMACD零线上穿': signal_quality['CMACD_cross_zero_up'].sum(),
            'CMACD零线下穿': signal_quality['CMACD_cross_zero_down'].sum(),
            'CMACD看涨背离': signal_quality['CMACD_bullish_divergence'].sum(),
            'CMACD看跌背离': signal_quality['CMACD_bearish_divergence'].sum(),
            'CMACD趋势一致': signal_quality['CMACD_trend_consistency'].sum(),
            'CMACD多时间框架一致': signal_quality['CMACD_mtf_consistency'].sum(),
            'CMACD强势上涨': signal_quality['CMACD_hist_A_up'].sum(),
            'CMACD强势下跌': signal_quality['CMACD_hist_B_down'].sum()
        }

        print(f"\nCMACD特征在信号中的分布:")
        for feature, count in cmacd_features.items():
            if count > 0:
                print(f"{feature}: {count}")

    def plot_macd_chart(self, df, symbol="BTCUSDT", save_path=None):
        """绘制MACD图表"""
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        from matplotlib.patches import Rectangle

        # 设置中文字体
        plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS', 'DejaVu Sans']
        plt.rcParams['axes.unicode_minus'] = False

        # 创建图表
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 10), height_ratios=[2, 1])
        fig.suptitle(f'{symbol} MACD技术分析图表', fontsize=16, fontweight='bold')

        # 绘制价格和移动平均线
        ax1.plot(df['open_time'], df['close'], label='收盘价', color='black', linewidth=1)
        ax1.plot(df['open_time'], df['MA_5'], label='MA5', color='blue', linewidth=1, alpha=0.7)
        ax1.plot(df['open_time'], df['MA_10'], label='MA10', color='orange', linewidth=1, alpha=0.7)
        ax1.plot(df['open_time'], df['MA_20'], label='MA20', color='red', linewidth=1, alpha=0.7)

        # 标记MACD信号点
        golden_cross_points = df[df['MACD_golden_cross'] == True]
        death_cross_points = df[df['MACD_death_cross'] == True]

        if len(golden_cross_points) > 0:
            ax1.scatter(golden_cross_points['open_time'], golden_cross_points['close'],
                        color='green', marker='^', s=100, label='MACD金叉', zorder=5)

        if len(death_cross_points) > 0:
            ax1.scatter(death_cross_points['open_time'], death_cross_points['close'],
                        color='red', marker='v', s=100, label='MACD死叉', zorder=5)

        ax1.set_ylabel('价格', fontsize=12)
        ax1.legend(loc='upper left')
        ax1.grid(True, alpha=0.3)

        # 绘制MACD
        # MACD线
        ax2.plot(df['open_time'], df['MACD_DIF'], label='MACD(DIF)', color='blue', linewidth=1.5)
        ax2.plot(df['open_time'], df['MACD_DEA'], label='信号线(DEA)', color='red', linewidth=1.5)

        # 绘制MACD柱状图
        colors = ['green' if x > 0 else 'red' for x in df['MACD_histogram']]
        ax2.bar(df['open_time'], df['MACD_histogram'], color=colors, alpha=0.6, width=0.8, label='MACD柱状图')

        # 绘制零线
        ax2.axhline(y=0, color='black', linestyle='--', alpha=0.5)

        # 标记交叉点
        if len(golden_cross_points) > 0:
            ax2.scatter(golden_cross_points['open_time'], golden_cross_points['MACD_DIF'],
                        color='green', marker='^', s=100, zorder=5)

        if len(death_cross_points) > 0:
            ax2.scatter(death_cross_points['open_time'], death_cross_points['MACD_DIF'],
                        color='red', marker='v', s=100, zorder=5)

        ax2.set_ylabel('MACD', fontsize=12)
        ax2.set_xlabel('时间', fontsize=12)
        ax2.legend(loc='upper left')
        ax2.grid(True, alpha=0.3)

        # 格式化x轴日期
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))

        # 自动旋转日期标签
        plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
        plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)

        # 调整布局
        plt.tight_layout()

        # 保存图表
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f"MACD图表已保存到: {save_path}")

        plt.show()

        return fig

    def generate_macd_report(self, df, symbol="BTCUSDT"):
        """生成MACD分析报告"""
        print(f"\n{'=' * 60}")
        print(f"📊 {symbol} MACD技术分析报告")
        print(f"{'=' * 60}")

        # 基本统计
        print(f"\n📈 MACD基本统计:")
        print(f"MACD DIF 平均值: {df['MACD_DIF'].mean():.6f}")
        print(f"MACD DIF 标准差: {df['MACD_DIF'].std():.6f}")
        print(f"MACD DIF 最大值: {df['MACD_DIF'].max():.6f}")
        print(f"MACD DIF 最小值: {df['MACD_DIF'].min():.6f}")

        # 信号统计
        golden_cross_count = df['MACD_golden_cross'].sum()
        death_cross_count = df['MACD_death_cross'].sum()
        cross_zero_up_count = df['MACD_cross_zero_up'].sum()
        cross_zero_down_count = df['MACD_cross_zero_down'].sum()

        print(f"\n🎯 MACD信号统计:")
        print(f"金叉次数: {golden_cross_count}")
        print(f"死叉次数: {death_cross_count}")
        print(f"零线上穿次数: {cross_zero_up_count}")
        print(f"零线下穿次数: {cross_zero_down_count}")

        # 背离统计
        bullish_div_count = df['MACD_bullish_divergence'].sum()
        bearish_div_count = df['MACD_bearish_divergence'].sum()

        print(f"\n🔄 MACD背离统计:")
        print(f"看涨背离次数: {bullish_div_count}")
        print(f"看跌背离次数: {bearish_div_count}")

        # 趋势分析
        current_dif = df['MACD_DIF'].iloc[-1]
        current_dea = df['MACD_DEA'].iloc[-1]
        current_hist = df['MACD_histogram'].iloc[-1]

        print(f"\n📊 当前MACD状态:")
        print(f"当前DIF: {current_dif:.6f}")
        print(f"当前DEA: {current_dea:.6f}")
        print(f"当前柱状图: {current_hist:.6f}")

        # 趋势判断
        if current_dif > current_dea:
            trend = "看涨趋势"
            trend_color = "🟢"
        else:
            trend = "看跌趋势"
            trend_color = "🔴"

        if current_dif > 0:
            zero_position = "零线之上"
            zero_color = "🟢"
        else:
            zero_position = "零线之下"
            zero_color = "🔴"

        print(f"趋势方向: {trend_color} {trend}")
        print(f"零线位置: {zero_color} {zero_position}")

        # 信号强度分析
        signal_strength = df['MACD_signal_strength'].iloc[-1]
        if signal_strength > 0:
            signal_quality = "强买入信号"
            signal_emoji = "🟢"
        elif signal_strength < 0:
            signal_quality = "强卖出信号"
            signal_emoji = "🔴"
        else:
            signal_quality = "中性信号"
            signal_emoji = "🟡"

        print(f"信号强度: {signal_emoji} {signal_quality} (强度值: {signal_strength})")

        # 动量分析
        dif_momentum = df['MACD_DIF_momentum'].iloc[-1]
        hist_momentum = df['MACD_hist_momentum'].iloc[-1]

        print(f"\n⚡ MACD动量分析:")
        print(f"DIF动量: {dif_momentum:.6f}")
        print(f"柱状图动量: {hist_momentum:.6f}")

        if dif_momentum > 0 and hist_momentum > 0:
            momentum_status = "双重看涨动量"
            momentum_emoji = "🟢"
        elif dif_momentum < 0 and hist_momentum < 0:
            momentum_status = "双重看跌动量"
            momentum_emoji = "🔴"
        else:
            momentum_status = "动量分歧"
            momentum_emoji = "🟡"

        print(f"动量状态: {momentum_emoji} {momentum_status}")

        # 波动率分析
        dif_volatility = df['MACD_DIF_volatility'].iloc[-1]
        hist_volatility = df['MACD_hist_volatility'].iloc[-1]

        print(f"\n📊 MACD波动率分析:")
        print(f"DIF波动率: {dif_volatility:.6f}")
        print(f"柱状图波动率: {hist_volatility:.6f}")

        # 相对强度分析
        dif_relative_strength = df['MACD_DIF_relative_strength'].iloc[-1]
        hist_relative_strength = df['MACD_hist_relative_strength'].iloc[-1]

        print(f"\n💪 MACD相对强度:")
        print(f"DIF相对强度: {dif_relative_strength:.3f}")
        print(f"柱状图相对强度: {hist_relative_strength:.3f}")

        # 交易建议
        print(f"\n💡 交易建议:")

        if golden_cross_count > death_cross_count:
            print("✅ 金叉信号多于死叉，整体偏多")
        elif death_cross_count > golden_cross_count:
            print("❌ 死叉信号多于金叉，整体偏空")
        else:
            print("⚖️ 金叉死叉信号平衡，需要其他指标确认")

        if bullish_div_count > bearish_div_count:
            print("✅ 看涨背离多于看跌背离，可能即将反转向上")
        elif bearish_div_count > bullish_div_count:
            print("❌ 看跌背离多于看涨背离，可能即将反转向下")

        if current_dif > 0 and current_dif > current_dea:
            print("✅ 当前处于强势上涨状态")
        elif current_dif < 0 and current_dif < current_dea:
            print("❌ 当前处于弱势下跌状态")
        else:
            print("⚠️ 当前处于盘整状态，需要等待明确信号")

        print(f"\n{'=' * 60}")

        return {
            'trend': trend,
            'signal_strength': signal_strength,
            'golden_cross_count': golden_cross_count,
            'death_cross_count': death_cross_count,
            'bullish_divergence_count': bullish_div_count,
            'bearish_divergence_count': bearish_div_count,
            'current_dif': current_dif,
            'current_dea': current_dea,
            'current_histogram': current_hist
        }

    def get_commodity_data_by_crypto_timerange(self, crypto_df, crypto_interval='1h', symbol='CL=F', name='原油'):
        """根据数字货币数据的时间区间获取商品数据（使用Alpha Vantage API，固定使用1小时K线）"""
        print(f"正在根据数字货币时间区间获取{name}期货价格数据（1小时K线）- 使用Alpha Vantage API...")

        if len(crypto_df) == 0:
            print(f"❌ 数字货币数据为空，无法获取{name}数据")
            return pd.DataFrame()

        # 获取时间范围
        start_str = crypto_df['open_time'].min().strftime('%Y-%m-%d %H:%M:%S')
        end_str = crypto_df['open_time'].max().strftime('%Y-%m-%d %H:%M:%S')
        print(f"时间范围: {start_str} 至 {end_str}")

        # 计算时间跨度（小时）
        start_dt = pd.to_datetime(start_str)
        end_dt = pd.to_datetime(end_str)
        total_hours = int((end_dt - start_dt).total_seconds() / 3600) + 1
        print(f"总时间跨度: {total_hours} 小时")

        # 确定批次大小（每批约5000小时，约7个月）
        batch_hours = 5000
        batch_count = max(1, (total_hours + batch_hours - 1) // batch_hours)
        print(f"将数据分成 {batch_count} 个批次获取，每批约 {batch_hours} 小时")

        all_data = []
        successful_batches = 0
        min_interval = 12  # API限制：每分钟5次请求，最小间隔12秒

        # 记录每个批次的开始时间
        batch_start_times = []

        for i in range(batch_count):
            batch_start = start_dt + pd.Timedelta(hours=batch_hours * i)
            batch_end = min(batch_start + pd.Timedelta(hours=batch_hours - 1, minutes=59), end_dt)
            batch_start_str = batch_start.strftime('%Y-%m-%d %H:%M:%S')
            batch_end_str = batch_end.strftime('%Y-%m-%d %H:%M:%S')
            print(f"正在获取第 {i+1}/{batch_count} 批 {name}数据: {batch_start_str} 至 {batch_end_str}")

            # 记录批次请求开始时间
            batch_start_times.append(pd.Timestamp.now())

            # 检查与前一批的间隔时间，确保符合API限制
            if i > 0:
                time_since_last_batch = (batch_start_times[i] - batch_start_times[i-1]).total_seconds()
                if time_since_last_batch < min_interval:
                    wait_time = min_interval - time_since_last_batch
                    print(f"⏳ API请求间隔保护: 距离上次请求仅 {time_since_last_batch:.1f} 秒，需等待 {wait_time:.1f} 秒")
                    time.sleep(wait_time)
                    batch_start_times[i] = pd.Timestamp.now()

            # 获取该批次的数据
            batch_data = self.get_historical_data(batch_start_str, batch_end_str, interval='1h', symbol=symbol, name=name)

            if batch_data is not None and len(batch_data) > 0:
                all_data.append(batch_data)
                successful_batches += 1
                print(f"✅ 第 {i+1} 批获取成功，获取 {len(batch_data)} 条数据")
            else:
                print(f"❌ 第 {i+1} 批获取失败")

            # 如果不是最后一批，添加额外延迟以避免触发API限制
            if i < batch_count - 1:
                wait_time = min_interval
                print(f"⏳ 批次间等待 {wait_time} 秒以避免触发API限制...")
                time.sleep(wait_time)

        # 合并所有批次的数据
        if all_data:
            print(f"🔄 合并 {len(all_data)} 个批次的数据...")
            final_df = pd.concat(all_data, ignore_index=True)

            # 去重和排序
            original_count = len(final_df)
            timestamp_col = f"{name.lower()}_timestamp"
            final_df = final_df.drop_duplicates(subset=[timestamp_col]).reset_index(drop=True)
            final_df = final_df.sort_values(timestamp_col).reset_index(drop=True)
            duplicates_removed = original_count - len(final_df)

            print(f"🎉 批量获取完成！")
            print(f"📊 处理汇总:")
            print(f"   - 成功批次: {successful_batches}/{batch_count}")
            print(f"   - 原始记录: {original_count} 条")
            print(f"   - 去重后: {len(final_df)} 条 (删除重复: {duplicates_removed})")
            print(f"   - 数据完整性: {(len(final_df) / max(1, total_hours)) * 100:.1f}% (基于时间跨度)")
            print(f"📅 时间范围: {final_df[timestamp_col].min()} 至 {final_df[timestamp_col].max()}")
            close_col = f"{name.lower()}_close"
            print(f"💰 价格范围: ${final_df[close_col].min():.2f} - ${final_df[close_col].max():.2f}")
            print(f"⏱️ 请求间隔合规: 所有请求间隔 ≥ {min_interval} 秒")

            return final_df
        else:
            print(f"❌ 所有批次都获取失败，使用模拟数据")
            print(f"💡 建议: 检查网络连接、API密钥或稍后重试")
            return self._create_dummy_commodity_data(start_str, end_str, name=name.lower())

    def _create_dummy_commodity_data(self, start_str, end_str, name='oil'):
        """创建模拟商品数据"""
        print(f"正在生成模拟{name}数据...")

        start_dt = pd.to_datetime(start_str)
        end_dt = pd.to_datetime(end_str)
        total_hours = int((end_dt - start_dt).total_seconds() / 3600) + 1

        # 随机生成价格数据，原油价格范围在40-90之间，黄金价格范围在1600-2000之间
        base_price = 65.0 if name == 'oil' else 1800.0
        data = []

        current_time = start_dt
        for _ in range(total_hours):
            if current_time > end_dt:
                break

            # 价格随机波动
            price_change = np.random.normal(0, 0.5 if name == 'oil' else 5.0)
            base_price += price_change
            if name == 'oil':
                base_price = max(40.0, min(90.0, base_price))  # 限制在40-90之间
            else:
                base_price = max(1600.0, min(2000.0, base_price))  # 限制在1600-2000之间

            open_price = base_price + np.random.uniform(-0.5, 0.5 if name == 'oil' else 5.0)
            high_price = max(open_price, base_price + np.random.uniform(0, 1.0 if name == 'oil' else 10.0))
            low_price = min(open_price, base_price - np.random.uniform(0, 1.0 if name == 'oil' else 10.0))
            close_price = base_price + np.random.uniform(-0.5, 0.5 if name == 'oil' else 5.0)
            volume = np.random.randint(50000, 200000 if name == 'oil' else 10000, 50000)

            data.append({
                f"{name}_timestamp": current_time,
                f"{name}_open": round(open_price, 2),
                f"{name}_high": round(high_price, 2),
                f"{name}_low": round(low_price, 2),
                f"{name}_close": round(close_price, 2),
                f"{name}_volume": round(volume, 0)
            })

            current_time += pd.Timedelta(hours=1)

        df = pd.DataFrame(data)
        print(f"✅ 生成了 {len(df)} 条模拟{name}数据，价格范围: ${df[f'{name}_close'].min():.2f} - ${df[f'{name}_close'].max():.2f}")

        return df

    def _create_dummy_commodity_data_for_crypto(self, crypto_df, crypto_interval='1h', name='oil'):
        """根据数字货币数据创建匹配的模拟商品数据"""
        print(f"正在生成与数字货币数据匹配的模拟{name}数据...")

        if len(crypto_df) == 0:
            return pd.DataFrame()

        start_dt = crypto_df['open_time'].min()
        end_dt = crypto_df['open_time'].max()
        total_hours = int((end_dt - start_dt).total_seconds() / 3600) + 1

        # 随机生成价格数据，原油价格范围在40-90之间，黄金价格范围在1600-2000之间
        base_price = 65.0 if name == 'oil' else 1800.0
        data = []

        current_time = start_dt
        for _ in range(total_hours):
            if current_time > end_dt:
                break

            # 价格随机波动
            price_change = np.random.normal(0, 0.5 if name == 'oil' else 5.0)
            base_price += price_change
            if name == 'oil':
                base_price = max(40.0, min(90.0, base_price))  # 限制在40-90之间
            else:
                base_price = max(1600.0, min(2000.0, base_price))  # 限制在1600-2000之间

            open_price = base_price + np.random.uniform(-0.5, 0.5 if name == 'oil' else 5.0)
            high_price = max(open_price, base_price + np.random.uniform(0, 1.0 if name == 'oil' else 10.0))
            low_price = min(open_price, base_price - np.random.uniform(0, 1.0 if name == 'oil' else 10.0))
            close_price = base_price + np.random.uniform(-0.5, 0.5 if name == 'oil' else 5.0)
            volume = np.random.randint(50000, 200000 if name == 'oil' else 10000, 50000)

            data.append({
                f"{name}_timestamp": current_time,
                f"{name}_open": round(open_price, 2),
                f"{name}_high": round(high_price, 2),
                f"{name}_low": round(low_price, 2),
                f"{name}_close": round(close_price, 2),
                f"{name}_volume": round(volume, 0)
            })

            current_time += pd.Timedelta(hours=1)

        df = pd.DataFrame(data)
        print(f"✅ 生成了 {len(df)} 条与数字货币匹配的模拟{name}数据")
        print(f"时间范围: {df[f'{name}_timestamp'].min()} 至 {df[f'{name}_timestamp'].max()}")
        print(f"价格范围: ${df[f'{name}_close'].min():.2f} - ${df[f'{name}_close'].max():.2f}")

        return df

    def prepare_commodity_data_for_merge(self, commodity_df, crypto_df, name='oil'):
        """准备商品数据以便与数字货币数据合并"""
        print(f"正在准备{name}数据以便合并...")

        # 重命名时间列以便合并
        commodity_prepared = commodity_df.copy()
        commodity_prepared = commodity_prepared.rename(columns={f"{name}_timestamp": 'open_time'})

        # 确保时间列为datetime类型
        commodity_prepared['open_time'] = pd.to_datetime(commodity_prepared['open_time'])

        # 确保价格列为数值类型
        for col in [f"{name}_open", f"{name}_high", f"{name}_low", f"{name}_close", f"{name}_volume"]:
            if col in commodity_prepared.columns:
                commodity_prepared[col] = pd.to_numeric(commodity_prepared[col], errors='coerce')

        print(f"✅ {name}数据准备完成，共 {len(commodity_prepared)} 条记录")
        print(f"时间范围: {commodity_prepared['open_time'].min()} 到 {commodity_prepared['open_time'].max()}")

        return commodity_prepared

    def resample_commodity_data_to_match_crypto(self, commodity_df, crypto_df, crypto_interval, name='oil'):
        """将商品数据重采样以匹配数字货币数据的时间间隔（备用函数）"""
        print(f"正在将{name}数据重采样到 {crypto_interval} 间隔...")

        # 设置时间为索引
        commodity_df = commodity_df.set_index(f"{name}_timestamp")

        # 根据crypto_interval确定重采样规则
        if crypto_interval == '1h':
            resample_rule = '1H'
        elif crypto_interval == '4h':
            resample_rule = '4H'
        elif crypto_interval == '1d':
            resample_rule = '1D'
        elif crypto_interval == '1m':
            resample_rule = '1min'
        else:
            resample_rule = '1H'  # 默认1小时

        # 重采样商品数据
        commodity_resampled = commodity_df.resample(resample_rule).agg({
            f"{name}_open": 'first',
            f"{name}_high": 'max',
            f"{name}_low": 'min',
            f"{name}_close": 'last',
            f"{name}_volume": 'sum'
        })

        # 重置索引
        commodity_resampled = commodity_resampled.reset_index()
        commodity_resampled = commodity_resampled.dropna()

        print(f"✅ {name}数据重采样完成，共 {len(commodity_resampled)} 条记录")
        return commodity_resampled

    def merge_crypto_commodity_data(self, crypto_df, commodity_df, name='oil'):
        """优化的数字货币数据和商品数据合并"""
        print(f"正在合并数字货币和{name}数据...")

        if len(commodity_df) == 0:
            print(f"⚠️ {name}数据为空，无法合并")
            return crypto_df

        # 确保时间列为datetime类型
        crypto_df['open_time'] = pd.to_datetime(crypto_df['open_time'])
        commodity_df['open_time'] = pd.to_datetime(commodity_df['open_time'])

        print(f"数字货币数据: {len(crypto_df)} 条，时间范围: {crypto_df['open_time'].min()} 至 {crypto_df['open_time'].max()}")
        print(f"{name}数据: {len(commodity_df)} 条，时间范围: {commodity_df['open_time'].min()} 至 {commodity_df['open_time'].max()}")

        # 使用asof合并，找到最接近的商品数据
        crypto_df = crypto_df.sort_values('open_time').reset_index(drop=True)
        commodity_df = commodity_df.sort_values('open_time').reset_index(drop=True)

        # 先尝试精确匹配
        merged_df = pd.merge(crypto_df, commodity_df, on='open_time', how='left')

        # 统计匹配情况
        close_col = f"{name}_close"
        exact_matches = merged_df[close_col].notna().sum()
        total_records = len(merged_df)
        match_rate = exact_matches / total_records * 100
        print(f"精确匹配率: {match_rate:.1f}% ({exact_matches}/{total_records} 条记录)")

        # 如果匹配率低于50%，使用近似匹配
        if match_rate < 50:
            print("精确匹配率较低，使用近似时间匹配...")
            merged_df = pd.merge_asof(
                crypto_df, commodity_df,
                on='open_time',
                direction='nearest',
                tolerance=pd.Timedelta(hours=2)  # 允许2小时的时间差
            )

            # 重新计算匹配率
            exact_matches = merged_df[close_col].notna().sum()
            match_rate = exact_matches / total_records * 100
            print(f"近似匹配后匹配率: {match_rate:.1f}% ({exact_matches}/{total_records} 条记录)")

        # 填充缺失值
        commodity_columns = [f"{name}_open", f"{name}_high", f"{name}_low", f"{name}_close", f"{name}_volume"]

        missing_before = merged_df[commodity_columns].isna().sum().sum()

        # 前向填充
        merged_df[commodity_columns] = merged_df[commodity_columns].ffill()

        # 后向填充（处理开头的缺失值）
        merged_df[commodity_columns] = merged_df[commodity_columns].bfill()

        # 如果还有缺失值，用均值填充
        for col in commodity_columns:
            if merged_df[col].isna().any():
                mean_value = merged_df[col].mean()
                if not np.isnan(mean_value):
                    merged_df[col] = merged_df[col].fillna(mean_value)
                else:
                    # 如果均值不可用，使用默认值
                    if 'volume' in col:
                        merged_df[col] = merged_df[col].fillna(100000.0)
                    elif 'close' in col:
                        merged_df[col] = merged_df[col].fillna(70.0 if name == 'oil' else 1800.0)
                    else:
                        merged_df[col] = merged_df[col].fillna(0.0)

        missing_after = merged_df[commodity_columns].isna().sum().sum()
        print(f"缺失值处理: 处理前 {missing_before} 个缺失值，处理后 {missing_after} 个缺失值")

        if len(merged_df) != len(crypto_df):
            print("⚠️ 警告：合并后数据量发生变化")

        # 显示商品数据范围
        if merged_df[close_col].notna().any():
            commodity_min = merged_df[close_col].min()
            commodity_max = merged_df[close_col].max()
            commodity_mean = merged_df[close_col].mean()
            print(f"合并后{name}价格范围: ${commodity_min:.2f} - ${commodity_max:.2f}，均值: ${commodity_mean:.2f}")

        return merged_df

    def prepare_data(self, symbol, start_date, end_date, interval='1h'):
        """
        准备完整数据集，包括数字货币数据、原油数据和黄金数据

        参数:
        - symbol: 数字货币交易对，如 'BTCUSDT'
        - start_date: 开始日期，如 '2023-01-01 00:00:00'
        - end_date: 结束日期，如 '2023-12-31 23:59:59'
        - interval: K线间隔，默认为 '1h'

        返回:
        - 准备好的完整数据集 DataFrame
        """
        print(f"正在准备 {symbol} 的完整数据集，时间范围: {start_date} 至 {end_date}，K线间隔: {interval}")

        # 1. 获取数字货币数据
        print("开始获取数字货币数据...")
        df = self.get_crypto_data(symbol, start_date, end_date, interval)
        print(f"✅ 数字货币数据获取完成，共 {len(df)} 条记录")
        print(f"数字货币时间范围: {df['open_time'].min()} 至 {df['open_time'].max()}")

        # 2. 根据数字货币数据的时间区间获取原油数据
        print("\n开始根据数字货币时间区间获取原油数据...")
        oil_df = self.get_commodity_data_by_crypto_timerange(df, interval, symbol='CL=F', name='原油')

        # 3. 根据数字货币数据的时间区间获取黄金数据
        print("\n开始根据数字货币时间区间获取黄金数据...")
        gold_df = self.get_commodity_data_by_crypto_timerange(df, interval, symbol='GC=F', name='黄金')

        # 4. 准备原油数据以便合并
        if len(oil_df) > 0:
            oil_resampled = self.prepare_commodity_data_for_merge(oil_df, df, name='oil')
        else:
            print("⚠️ 使用模拟原油数据...")
            oil_resampled = self._create_dummy_commodity_data_for_crypto(df, interval, name='oil')
            oil_resampled = self.prepare_commodity_data_for_merge(oil_resampled, df, name='oil')

        # 5. 准备黄金数据以便合并
        if len(gold_df) > 0:
            gold_resampled = self.prepare_commodity_data_for_merge(gold_df, df, name='gld')
        else:
            print("⚠️ 使用模拟黄金数据...")
            gold_resampled = self._create_dummy_commodity_data_for_crypto(df, interval, name='gld')
            gold_resampled = self.prepare_commodity_data_for_merge(gold_resampled, df, name='gld')

        # 6. 合并数字货币和原油数据
        print("\n开始合并数字货币和原油数据...")
        df = self.merge_crypto_commodity_data(df, oil_resampled, name='oil')

        # 7. 合并数字货币和黄金数据
        print("\n开始合并数字货币和黄金数据...")
        df = self.merge_crypto_commodity_data(df, gold_resampled, name='gld')

        # 8. 计算基础技术指标（数字货币）
        df = self.calculate_basic_indicators(df)

        # 9. 计算原油相关指标
        df = self.calculate_indicators(df, prefix='oil_')

        # 10. 计算黄金相关指标
        df = self.calculate_indicators(df, prefix='gld_')

        # 11. 识别SMC结构
        df = self.identify_smc_structure(df)

        # 12. 计算LuxAlgo SMC特征
        df = self.calculate_luxalgo_smc_features(df)

        # 13. 计算Squeeze Momentum特征
        df = self.calculate_squeeze_momentum_features(df)

        # 14. 填充缺失值
        df.fillna({
            'RSI6': 0, 'RSI12': 0, 'RSI24': 0,
            'MA_5': df['close'], 'MA_10': df['close'],
            'MA_20': df['close'], 'MA_42': df['close'],
            'K': 50, 'D': 50, 'J': 50,
            'MA_5': df['close'], 'MA_10': df['close'],
            'MA_20': df['close'], 'MA_42': df['close'],
            # 原油指标填充
            'oil_RSI6': 50, 'oil_RSI12': 50, 'oil_RSI24': 50,
            'oil_MA_5': df['oil_close'], 'oil_MA_10': df['oil_close'],
            'oil_MA_20': df['oil_close'],
            # 黄金指标填充
            'gld_RSI6': 50, 'gld_RSI12': 50, 'gld_RSI24': 50,
            'gld_MA_5': df['gld_close'], 'gld_MA_10': df['gld_close'],
            'gld_MA_20': df['gld_close']
        }, inplace=True)

        # 15. 计算高级特征（包含原油和黄金特征）
        df = self.calculate_advanced_features(df)

        # 16. 删除前50行（确保所有指标计算完整）
        if len(df) > 50:
            df = df.iloc[50:].reset_index(drop=True)

        # 17. 生成标签
        df = self.generate_smc_labels(df)

        # 18. 保存结果
        output_file = f"complete_dataset_{symbol}_{interval}_with_commodity_data.csv"
        df.to_csv(output_file, index=False)

        # 19. 输出统计信息
        self._print_statistics(df, output_file)

        return df

    def _print_statistics(self, df, output_file):
        """
        打印数据集统计信息

        参数:
        - df: 数据集 DataFrame
        - output_file: 输出文件名
        """
        print(f"\n=== 数据集统计信息 ===")
        print(f"数据集保存至: {output_file}")
        print(f"总记录数: {len(df)}")
        print(f"总列数: {len(df.columns)}")
        print(f"时间范围: {df['open_time'].min()} 至 {df['open_time'].max()}")
        print(f"价格范围: ${df['close'].min():.2f} - ${df['close'].max():.2f}")

        print(f"\n风险等级分布:")
        print(df['risk_level'].value_counts())
        print(f"\n市场状态分布:")
        print(df['market_state'].value_counts())

        # 原油数据统计
        if 'oil_close' in df.columns:
            print(f"\n=== 原油数据统计 ===")
            print(f"原油价格范围: ${df['oil_close'].min():.2f} - ${df['oil_close'].max():.2f}")
            print(f"原油平均价格: ${df['oil_close'].mean():.2f}")
            print(f"原油价格标准差: ${df['oil_close'].std():.2f}")

            if 'oil_market_state' in df.columns:
                print(f"\n原油市场状态分布:")
                print(df['oil_market_state'].value_counts())

            # 相关性统计
            if 'oil_crypto_price_corr' in df.columns:
                print(f"\n数字货币-原油相关性统计:")
                print(f"价格相关性均值: {df['oil_crypto_price_corr'].mean():.3f}")
                print(f"收益率相关性均值: {df['oil_crypto_returns_corr'].mean():.3f}")
                print(f"趋势一致性比例: {df['oil_crypto_trend_consistency'].mean():.3f}")

                # 强相关性时段统计
                strong_corr_periods = len(df[df['oil_crypto_price_corr'].abs() > 0.7])
                total_periods = len(df)
                strong_corr_ratio = strong_corr_periods / total_periods * 100
                print(f"强相关性时段 (|r| > 0.7): {strong_corr_ratio:.1f}% ({strong_corr_periods}/{total_periods} 条记录)")

        # 黄金数据统计
        if 'gld_close' in df.columns:
            print(f"\n=== 黄金数据统计 ===")
            print(f"黄金价格范围: ${df['gld_close'].min():.2f} - ${df['gld_close'].max():.2f}")
            print(f"黄金平均价格: ${df['gld_close'].mean():.2f}")
            print(f"黄金价格标准差: ${df['gld_close'].std():.2f}")

            if 'gld_market_state' in df.columns:
                print(f"\n黄金市场状态分布:")
                print(df['gld_market_state'].value_counts())

            # 相关性统计
            if 'gld_crypto_price_corr' in df.columns:
                print(f"\n数字货币-黄金相关性统计:")
                print(f"价格相关性均值: {df['gld_crypto_price_corr'].mean():.3f}")
                print(f"收益率相关性均值: {df['gld_crypto_returns_corr'].mean():.3f}")
                print(f"趋势一致性比例: {df['gld_crypto_trend_consistency'].mean():.3f}")

                # 强相关性时段统计
                strong_corr_periods = len(df[df['gld_crypto_price_corr'].abs() > 0.7])
                total_periods = len(df)
                strong_corr_ratio = strong_corr_periods / total_periods * 100
                print(f"强相关性时段 (|r| > 0.7): {strong_corr_ratio:.1f}% ({strong_corr_periods}/{total_periods} 条记录)")


# ==================== 程序入口 ====================
if __name__ == '__main__':
    try:
        # 创建交易系统实例
        trading_system = CompleteTradingSystem()

        # 展示新的原油API优化功能
        print("🚀 完整的数字货币量化交易系统 v2.4")
        print("🔧 已集成优化的原油数据批量获取功能")
        print("=" * 60)

        print("\n📋 当前原油API配置:")
        for key, value in trading_system.oil_batch_config.items():
            print(f"   - {key}: {value}")

        print("\n💡 新增优化特性:")
        print("   ✅ 每次请求最多5000条记录（可配置）")
        print("   ✅ 请求间隔≥1秒，符合API频率限制")
        print("   ✅ 自动分批处理大数据量")
        print("   ✅ 智能重试和错误恢复")
        print("   ✅ 实时进度显示和统计")
        print("   ✅ 数据去重和完整性检查")

        # 可选：演示配置自定义
        # print("\n🔧 演示配置自定义:")
        # trading_system.update_oil_batch_config(
        #     max_records_per_request=3000,  # 降低每次请求数量
        #     min_request_interval=1.5,      # 增加请求间隔
        #     max_retries=5                  # 增加重试次数
        # )

        # 设置原油API密钥（请替换为实际的API密钥）
        # trading_system.oil_api_key = 'your_actual_api_key_here'

        print("\n⚠️  注意：请在代码中设置您的原油数据API密钥")
        print("您可以通过 trading_system.oil_api_key = 'your_key' 来设置")
        print("如果没有API密钥，系统将使用模拟原油数据\n")

        # 配置参数
        symbol = 'SUIUSDT'  # 交易对
        interval = '1h'  # 时间间隔
        # start_date = '2025-04-07 16:00:00'  # 开始日期
        start_date = '2000-01-01'  # 开始日期
        end_date = None  # 结束日期（None表示到现在）

        # 执行完整处理流程（包含原油数据）
        df = trading_system.process_complete_system(symbol, interval, start_date, end_date)

        if df is not None:
            print(f"\n✅ 完整交易系统处理成功完成！")
            print(f"📊 数据集包含 {len(df)} 条记录")
            print(f"📈 信号分布: {dict(df['label'].value_counts())}")
        else:
            print("❌ 处理失败")

    except Exception as e:
        print(f"❌ 处理过程中出错: {e}")
        import traceback

        traceback.print_exc()