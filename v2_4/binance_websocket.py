import websocket
import json
import threading
import time
import logging
import os
from datetime import datetime
import pandas as pd
from sql_util import  MySQLUtil

from complete_trading_system_v2_4_4h  import CompleteTradingSystem

# 设置工作目录为脚本所在路径
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# 配置日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SimpleBinanceWebSocket:
    """简化的币安WebSocket客户端 - 专门用于接收K线数据"""
    
    def __init__(self, symbols=None, interval=None):
        """
        初始化WebSocket客户端
        
        Args:
            symbols (list): 要订阅的交易对列表，默认为['BTCUSDT', 'ETHUSDT']
        """
        self.symbols = symbols or 'BTCUSDT'
        #self.symbols = symbols or ['BTCUSDT', 'ETHUSDT']
        self.interval = interval
        self.ws = None
        self.is_connected = False
        self.kline_data = {}
        
        # 初始化K线数据存储
        self.kline_data[symbols] = {
            'current_kline': None,
            'completed_klines': []
        }

        # 创建交易系统实例
        self.trading_system = CompleteTradingSystem()

        MySQLUtil.init_pool()

    def on_message(self, ws, message):
        """处理接收到的消息"""
        try:
            data = json.loads(message)
            
            # 只处理K线数据
            if 'k' in data:
                self._handle_kline_data(data)
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON解析错误: {e}")
        except Exception as e:
            logger.error(f"处理消息时出错: {e}")
    
    def on_error(self, ws, error):
        """处理WebSocket错误"""
        logger.error(f"WebSocket错误: {error}")
        self.is_connected = False
    
    def on_close(self, ws, close_status_code, close_msg):
        """处理WebSocket连接关闭"""
        logger.info(f"WebSocket连接关闭: {close_status_code} - {close_msg}")
        self.is_connected = False
        self._schedule_reconnect()

    def _schedule_reconnect(self, delay=2):
        """延迟重新连接"""
        logger.info(f"🕒 {delay} 秒后尝试重连...")
        time.sleep(delay)
        self.connect()

    def on_open(self, ws):
        """处理WebSocket连接打开"""
        logger.info("WebSocket连接已建立")
        self.is_connected = True
        
        # 订阅K线数据流
        self._subscribe_kline_streams()
    
    def _handle_kline_data(self, data):
        """处理K线数据"""
        try:
            kline = data['k']
            symbol = kline['s']
            
            if symbol not in self.kline_data:
                return
            
            # 构建K线数据
            kline_info = {
                'symbol': symbol,
                'timestamp': kline['t'],
                'open_time': datetime.fromtimestamp(kline['t']/1000),
                'open': float(kline['o']),
                'high': float(kline['h']),
                'low': float(kline['l']),
                'close': float(kline['c']),
                'volume': float(kline['v']),
                'close_time': kline['T'],
                'close_time_formatted': datetime.fromtimestamp(kline['T']/1000),
                'quote_volume': float(kline['q']),
                'trades': kline['n'],
                'is_final': kline['x']
            }
            
            # 更新当前K线数据
            self.kline_data[symbol]['current_kline'] = kline_info
            
            # 如果是完成的K线，添加到已完成列表并输出
            if kline['x']:
                self.kline_data[symbol]['completed_klines'].append(kline_info)
                
                # 输出完成的K线信息
                logger.info(f"=== {symbol} 1分钟K线完成 ===")
                logger.info(f"时间: {kline_info['open_time']}")
                logger.info(f"开盘: {kline_info['open']}")
                logger.info(f"最高: {kline_info['high']}")
                logger.info(f"最低: {kline_info['low']}")
                logger.info(f"收盘: {kline_info['close']}")
                logger.info(f"成交量: {kline_info['volume']}")
                # 执行完整处理流程
                interval=self.interval
                first_symbol = self.symbols
                df = self.trading_system.process_complete_system(first_symbol, interval, kline_info)

                # 计算价格变化
                """
                price_change = kline_info['close'] - kline_info['open']
                price_change_percent = (price_change / kline_info['open']) * 100
                logger.info(f"价格变化: {price_change:.4f} ({price_change_percent:.2f}%)")
                logger.info("=" * 40)
                """
        except Exception as e:
            logger.error(f"处理K线数据时出错: {e}")
    
    def _subscribe_kline_streams(self):
        """订阅K线数据流"""
       # for symbol in self.symbols:
            #支持1s,1m,3m,5m,15m,30m,1h,2h,4h,6h,8h,12h,1d,3d,1w,1M
        # 订阅1分钟K线数据
        stream_name = f"{self.symbols.lower()}@kline_{self.interval.lower()}"
        subscribe_msg = {
            "method": "SUBSCRIBE",
            "params": [stream_name],
            "id": 1
        }
        self.ws.send(json.dumps(subscribe_msg))
        logger.info(f"已订阅K线数据流: {stream_name}")
    
    def connect(self):
        """建立WebSocket连接"""
        try:
            # 币安WebSocket URL
            #websocket_url = "wss://stream.binance.com:9443/ws"
            ## 测试网络地址
            websocket_url = "wss://stream.testnet.binance.vision:9443/ws"
            # 创建WebSocket连接
            self.ws = websocket.WebSocketApp(
                websocket_url,
                on_open=self.on_open,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close
            )
            
            logger.info("正在连接到币安WebSocket...")
            self.ws.run_forever(
                http_proxy_host="127.0.0.1",
                http_proxy_port=7890,  # 根据你代理工具实际端口修改
                proxy_type="http",
                ping_interval=30,  # 每 30 秒发送一次 ping
                ping_timeout=10,  # 等待 Pong 的最大时间
                ping_payload="keepalive"  # 可自定义 ping 内容
            )
            
        except Exception as e:
            logger.error(f"连接WebSocket时出错: {e}")
    
    def start(self):
        """启动WebSocket客户端"""
        logger.info("启动简化的币安WebSocket客户端...")
        #logger.info(f"订阅的交易对: {', '.join(self.symbols)}")
        logger.info(f"订阅的交易对: {self.symbols}")
        # 在单独的线程中运行WebSocket连接
        ws_thread = threading.Thread(target=self.connect)
        ws_thread.daemon = True
        ws_thread.start()
        
        # 主线程每分钟输出一次状态
        try:
            while True:
                time.sleep(10)  # 等待60秒
                self._print_status()
        except KeyboardInterrupt:
            logger.info("正在关闭WebSocket连接...")
            if self.ws:
                self.ws.close()
    
    def _print_status(self):
        """打印当前状态"""
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"\n=== {current_time} 状态报告 ===")
        
        #for symbol in self.symbols:
        symbol=self.symbols
        if  symbol in self.kline_data:
            current_kline = self.kline_data[symbol]['current_kline']
            completed_count = len(self.kline_data[symbol]['completed_klines'])

            logger.info(f"{symbol}:")
            logger.info(f"  已完成的K线数量: {completed_count}")

            if current_kline:
                logger.info(f"  当前K线时间: {current_kline['open_time']}")
                logger.info(f"  当前价格: {current_kline['close']}")
                logger.info(f"  是否完成: {current_kline['is_final']}")
        
        logger.info("=" * 50)
    
    def get_latest_kline(self, symbol):
        """获取指定交易对的最新K线数据"""
        if symbol in self.kline_data and self.kline_data[symbol]['completed_klines']:
            return self.kline_data[symbol]['completed_klines'][-1]
        return None
    
    def get_all_klines(self, symbol):
        """获取指定交易对的所有K线数据"""
        if symbol in self.kline_data:
            return self.kline_data[symbol]['completed_klines']
        return []
    
    def save_klines_to_csv(self, symbol, filename=None):
        """将K线数据保存到CSV文件"""
        if symbol not in self.kline_data:
            logger.error(f"未找到交易对 {symbol} 的数据")
            return
        
        if filename is None:
            filename = f"{symbol}_klines_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        try:
            klines = self.kline_data[symbol]['completed_klines']
            if not klines:
                logger.warning(f"没有 {symbol} 的K线数据可保存")
                return
            
            # 准备数据
            data_list = []
            for kline in klines:
                data_list.append({
                    'timestamp': kline['open_time'],
                    'open': kline['open'],
                    'high': kline['high'],
                    'low': kline['low'],
                    'close': kline['close'],
                    'volume': kline['volume'],
                    'quote_volume': kline['quote_volume'],
                    'trades': kline['trades']
                })
            
            # 创建DataFrame并保存
            df = pd.DataFrame(data_list)
            df.to_csv(filename, index=False)
            logger.info(f"{symbol} K线数据已保存到 {filename}")
            
        except Exception as e:
            logger.error(f"保存K线数据时出错: {e}")


def main():
    """主函数"""
    # 创建WebSocket客户端
    symbols = ['BTCUSDT', 'ETHUSDT']  # 可以修改为其他交易对
    client = SimpleBinanceWebSocket(symbols)
    
    try:
        # 启动客户端
        client.start()
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
    except Exception as e:
        logger.error(f"程序运行出错: {e}")


if __name__ == "__main__":
    main() 