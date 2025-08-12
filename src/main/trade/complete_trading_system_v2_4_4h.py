import os
import requests
import pandas as pd
import numpy as np
import decimal
import time
import logging
import warnings
from sklearn.preprocessing import MinMaxScaler
from src.main.utils.sql_util import MySQLUtil

warnings.filterwarnings('ignore')

# 设置工作目录为脚本所在路径
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# 配置日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CompleteTradingSystem:
    """完整的数字货币量化交易系统"""

    def __init__(self):
        self.base_url = 'https://api.binance.com/api/v3/klines'

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
                response = requests.get(self.base_url, params=params, timeout=60, proxies=proxies)
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
            np.random.seed(42)
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

    def calculate_basic_indicators(self, df):
        """计算基础技术指标"""
        logger.info("计算基础技术指标...")

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
        logger.info("计算传统MACD指标...")

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
        logger.info("识别市场结构（改进版）...")

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
        logger.info("计算 Weak High 和 Strong Low（持续表达中）...")

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
        logger.info("计算LuxAlgo SMC特征...")

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
        result = {

        }
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
        logger.info("计算Squeeze Momentum特征...")

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
        logger.info("计算高级技术指标...")

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
        logger.info("计算CMACD多时间框架MACD指标...")

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

    """
    #generate_smc_label函数涉及的指标列清单
    #列名	类别	数据类型	用途描述	示例条件
    #SMC_is_BOS_Low	价格结构分析	bool	标识是否出现底部突破结构(Break of Structure Low)	(row.get('SMC_is_BOS_Low') == True
    #SMC_BOS_Low_Value	价格结构分析	float	底部突破结构对应的关键支撑价格	(abs(row['open'] - row['SMC_BOS_Low_Value']) <= row['SMC_BOS_Low_Value']*0.04
    #SMC_is_BOS_High	价格结构分析	bool	标识是否出现顶部突破结构(Break of Structure High)	(row.get('SMC_is_BOS_High') == True
    #SMC_BOS_High_Value	价格结构分析	float	顶部突破结构对应的关键阻力价格	(abs(row['open'] - row['SMC_BOS_High_Value']) <= row['SMC_BOS_High_Value']*0.02
    #SMC_is_CHoCH_High	价格结构分析	bool	标识是否出现高点被突破(Change of Character High)	(row['SMC_is_CHoCH_High'] == True)
    #SMC_swept_prev_low	价格结构分析	bool	标识价格是否扫除前低（显示空头力量）	(row['SMC_swept_prev_low'] == True)
    #CMACD_macd	技术指标	float	CMACD主线（类似MACD的DIF线）	(row['CMACD_macd'] < row['CMACD_signal'])
    #CMACD_signal	技术指标	float	CMACD信号线（类似MACD的DEA线）	(row['CMACD_macd'] > row['CMACD_signal'])
    #CMACD_histogram	技术指标	float	CMACD柱状图（MACD与信号线的差值）	(row['CMACD_histogram'] < 0.5)
    #CMACD_histogram_prev	技术指标	float	前一期CMACD柱状图值（用于动量比较）	(row['CMACD_histogram'] > row['CMACD_histogram_prev'])
    #CMACD_cross_up	技术指标	bool	标识CMACD是否出现金叉	(row.get('CMACD_cross_up') == True)
    #RSI6	技术指标	float	6周期相对强弱指标（短周期敏感度）	(row['RSI6'] < 0.4)
    #RSI24	技术指标	float	24周期相对强弱指标（长周期趋势确认）	(row['RSI6'] < row['RSI24'])
    #J	技术指标	float	随机震荡指标J值（反映超买超卖状态）	(row['J'] < row['K'])
    #K	技术指标	float	随机震荡指标K值（主震荡线）	(row['K'] > 0.8)
    #open	量价特征	float	当前K线开盘价	(row['open'] > row['MA_10'])
    #close	量价特征	float	当前K线收盘价	(row['close'] > row['open'])
    #high	量价特征	float	当前K线最高价	(row['high'] > row['MA_20'] * 1.05)
    #volume_ratio	量价特征	float	成交量比率（当前成交量/平均成交量）	(row['volume_ratio'] > 1.2)
    #body_ratio	量价特征	float	K线实体比率（实体长度/总波动范围）	(row['body_ratio'] > 0.8)
    #money_flow_volume	量价特征	float	资金流量指标（结合价格和成交量的资金流向）	(row['money_flow_volume'] > 0.5)
    #MA_5	趋势与波动	float	5周期移动平均线（短期趋势）	(row['MA_5'] < row['MA_10'])
    #MA_10	趋势与波动	float	10周期移动平均线（动态支撑/阻力）	(row['close'] > row['MA_10'])
    #MA_20	趋势与波动	float	20周期移动平均线（中期趋势基准）	(row['close'] < row['MA_20'])
    #momentum_ratio	趋势与波动	float	价格动量比率（当前价格变化率）	(row['momentum_ratio'] < -0.3)
    #trend_strength	趋势与波动	float	趋势强度指标（如ADX值）	(row['trend_strength'] < 0.3)
    #support_distance	趋势与波动	float	距离最近支撑位的百分比距离	(row['support_distance'] < 0.25)
    #drawdown_ratio	风险控制	float	回撤比率（当前价格从高点回落的幅度）	(row['drawdown_ratio'] > 0.5)
    #volatility_ratio	风险控制	float	波动率比率（当前波动率/历史平均波动率）	(row['volatility_ratio'] > 0.6)
    #volume_volatility	风险控制	float	成交量波动率（成交量变化的波动程度）
    """
    def generate_smc_labels(self, df):
        """生成优化后的SMC策略标签，包括 market_state """
        logger.info("生成优化后的SMC策略标签...")

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

                        (row.get('CMACD_macd', 0) < row.get('CMACD_signal', 0))and
                        (row.get('RSI6', 0) < row.get('RSI24', 0)) and
                        (row.get('J', 0) < row.get('K', 0)) ,

                        '底部动能确认',
                        13
                    ),


                    # 规则线
                    (
                        (row.get('SMC_is_BOS_Low') == True) and
                        (row.get('SMC_BOS_Low_Value', 0) is not None) and
                        (abs(row.get('open', 0) - row.get('SMC_BOS_Low_Value', 0)) <= row.get('SMC_BOS_Low_Value', 0) * 0.02),  # 允许2%的容差
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

                #买入 卖出说明
                buy_remark  = ' | '.join(desc for condition, desc, _ in buy_conditions if condition)
                sell_remark = ' | '.join(desc for condition, desc, _ in sell_conditions if condition)
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
                    return ('1', confidence, '买入信号: ' + buy_remark, risk_level)
                elif sell_score >= min_sell_score and sell_score > buy_score:
                    confidence = min(sell_score / 30, 1.0)  # 调整分母
                    risk_level = '低' if risk_score <= 1 else '中' if risk_score <= 2 else '高'
                    return ('2', confidence, '卖出信号: ' + sell_remark, risk_level)
                else:
                    return ('0', 0.5, '无明显信号', '低')
            except Exception as e:
                return ('0', 0.5, '错误', '低')

        # 应用标签生成
        #print(label_df.columns)
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

    def process_complete_system(self, symbol, interval, kline_info):
        """完整的交易系统处理流程"""
        logger.info(f"🚀 开始处理 {symbol} {interval} 完整交易系统...")
        start_time = time.time()
        last_row = MySQLUtil.fetch_dataframe('kline_data',
                                       conditions={'symbol': ('=', symbol), '`interval`': ('=', interval)},
                                       order_by='open_time desc', limit=1)
        # 把所有 Decimal 类型的列转换为 float
        last_row = last_row.applymap(lambda x: float(x) if isinstance(x, decimal.Decimal) else x)

        # 获取最后一条
        if not last_row.empty:
            new_id = last_row['id'].iloc[-1] + 1
        else:
            new_id = 1  # 如果df为空，默认从1开始

        new_row = {
            'symbol': {symbol},
            'interval': {interval},
            'id': {new_id},
            'open_time': kline_info['open_time'],
            'open': kline_info['open'],
            'close': kline_info['close'],
            'high': kline_info['high'],
            'low': kline_info['low'],
            'volume': kline_info['volume'],
            'create_datetime': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        }


        insert_status=MySQLUtil.insert('kline_data', {k: new_row[k] for k in
                                        ['id', 'symbol', 'interval', 'open_time', 'open', 'high', 'low', 'close',
                                         'volume', 'create_datetime']
                                        })

        logger.info(f"插入最新一条K线数据成功: {insert_status}, detl：{new_row}")
        #1.从新拉取写入后的所有数据，原有数据+1条新增
        df = MySQLUtil.fetch_dataframe('kline_data',
                                             conditions={'symbol': ('=', symbol), '`interval`': ('=', interval)},
                                             order_by='open_time desc', limit=2000)
        # 把所有 Decimal 类型的列转换为 float
        df = df.applymap(lambda x: float(x) if isinstance(x, decimal.Decimal) else x)
        # 按 datetime 正序排序,防止时序错误
        #df = df.sort_values(by='open_time', ascending=True)
        df = df[::-1].reset_index(drop=True)  # 反转为正序

        if len(df) == 0:
            logger.error("❌ 没有获取到数据，无法继续处理")
            return None
        else:
            logger.info(f"从数据库获取数据总条数：{len(df)}, start:{df.iloc[0]['open_time']},  end: {df.iloc[-1]['open_time']}")
        # 2. 计算基础技术指标
        df = self.calculate_basic_indicators(df)

        # 3. 识别SMC结构
        df = self.identify_smc_structure(df)

        # 4. 计算LuxAlgo SMC特征
        df = self.calculate_luxalgo_smc_features(df)

        # 5. 计算Squeeze Momentum特征
        df = self.calculate_squeeze_momentum_features(df)

        # 6. 填充缺失值
        df.fillna({
            'RSI6': 0, 'RSI12': 0, 'RSI24': 0,
            'K': 50, 'D': 50, 'J': 50,
            'MA_5': df['close'], 'MA_10': df['close'],
            'MA_20': df['close'], 'MA_42': df['close']
        }, inplace=True)

        # 7. 计算高级特征
        df = self.calculate_advanced_features(df)

        # 8. 删除前50行（确保所有指标计算完整）
        if len(df) > 50:
            df = df.iloc[50:].reset_index(drop=True)

        # 9. 生成标签
        df = self.generate_smc_labels(df)

        # 显示所有列
        pd.set_option('display.max_columns', None)
        # 还可以设置显示的宽度更大，防止换行
        pd.set_option('display.width', 1000)
        # 如果行太多，也可以设置显示所有行
        pd.set_option('display.max_rows', None)

        #print(df[['symbol', 'interval', 'id', 'open_time', 'open', 'close', 'low', 'volume', 'label']].head(1))
        #print(df[['symbol', 'interval', 'id', 'open_time', 'open', 'close', 'low', 'volume', 'label']].iloc[-1])
        result = df.tail(1).replace([np.inf, -np.inf], np.nan)
        insert_status=MySQLUtil.insert_dataframe('complete_tech_indicators',result)

        columns = [
            'symbol', 'interval', 'id', 'open_time', 'open', 'close',
            'low', 'volume', 'label', 'confidence', 'signal_reason',
            'risk_level', 'market_state'
        ]
        logger.info(
            f"插入最新一条指标计算结果: {insert_status}, detl：\n"
            f"{result[columns].to_string(index=False)}"
        )

        # 10. 保存结果
        #output_file = f"complete_dataset_{symbol}_{interval}_squeeze_luxalgo_advanced1_chk.csv"
        # df.dropna(inplace=True)
        #df.to_csv(output_file, index=False)

        # 11. 输出统计信息
        #self._print_statistics(df, output_file)

        end_time = time.time()
        elapsed = end_time - start_time  # 不取整
        hours, remainder = divmod(int(elapsed), 3600)
        minutes, seconds = divmod(remainder, 60)
        milliseconds = int((elapsed - int(elapsed)) * 1000)
        logger.info(f"start： {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}， end：{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}"
              f"：耗时: {hours:02}:{minutes:02}:{seconds:02}")
        return df

    def _print_statistics(self, df, output_file):
        """打印统计信息"""
        print(f"\n=== 完整交易系统处理完成 (集成LuxAlgo SMC) ===")
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



def init_complete_trading_system(trading_system, symbol, interval, start_date, end_date=None):
    """完整的交易系统处理流程"""
    print(f"🚀 开始处理 {symbol} {interval} 完整交易系统...")

    # 1. 获取历史数据
    df = trading_system.get_historical_data(symbol, interval, start_date, end_date)
    df["symbol"] = symbol  # 固定交易对
    df["interval"] = interval  # 固定周期
    # 添加从1开始的自增序号
    df['id'] = range(1, len(df) + 1)
    # 添加当前时间列
    df["create_datetime"] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    MySQLUtil.insert_dataframe('kline_data', df)

    if len(df) == 0:
        print("❌ 没有获取到数据，无法继续处理")
        return None

    # 2. 计算基础技术指标
    df = trading_system.calculate_basic_indicators(df)

    # 3. 识别SMC结构
    df = trading_system.identify_smc_structure(df)

    # 4. 计算LuxAlgo SMC特征
    df = trading_system.calculate_luxalgo_smc_features(df)

    # 5. 计算Squeeze Momentum特征
    df = trading_system.calculate_squeeze_momentum_features(df)

    # 6. 填充缺失值
    df.fillna({
        'RSI6': 0, 'RSI12': 0, 'RSI24': 0,
        'K': 50, 'D': 50, 'J': 50,
        'MA_5': df['close'], 'MA_10': df['close'],
        'MA_20': df['close'], 'MA_42': df['close']
    }, inplace=True)

    # 7. 计算高级特征
    df = trading_system.calculate_advanced_features(df)

    # 8. 删除前50行（确保所有指标计算完整）
    if len(df) > 50:
        df = df.iloc[50:].reset_index(drop=True)

    # 9. 生成标签
    df = trading_system.generate_smc_labels(df)

    df["symbol"] = symbol  # 固定交易对
    df["interval"] = interval  # 固定周期

    # 添加当前时间列
    df["create_datetime"] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')

    # 10. 保存结果
    output_file = f"complete_dataset_{symbol}_{interval}_squeeze_luxalgo_advanced1.csv"
    # df.dropna(inplace=True)
    df.to_csv(output_file, index=False)


    MySQLUtil.insert_dataframe('complete_tech_indicators', df)

    # 11. 输出统计信息
    #trading_system._print_statistics(df, output_file=None)

    return df



# ==================== 程序入口 ====================
if __name__ == '__main__':
    try:
        # 创建交易系统实例
        trading_system = CompleteTradingSystem()

        # 配置参数
        symbol = 'SUIUSDT'  # 交易对
        interval = '1m'  # 时间间隔
        # start_date = '2025-04-07 16:00:00'  # 开始日期
        start_date = '2025-08-03 00:00:00'  # 开始日期
        end_date = '2025-08-04 00:00:00'  # 结束日期（None表示到现在）

        MySQLUtil.init_pool()
        # 执行存量初始化逻辑处理流程
        #df = init_complete_trading_system(trading_system, symbol, interval, start_date, end_date)

        # 执行完整处理流程
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