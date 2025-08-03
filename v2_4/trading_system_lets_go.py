#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
币安WebSocket客户端快速启动脚本
"""

import sys
import argparse
from binance_websocket import SimpleBinanceWebSocket

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='币安WebSocket客户端')
    parser.add_argument('--symbols', nargs='+', 
                       default='SUIUSDT',
                       help='要订阅的交易对列表 (默认: SUIUSDT)')
    parser.add_argument('--interval', default='1m',
                       choices=['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M'],
                       help='K线时间间隔 (默认: 1m)')
    parser.add_argument('--save', action='store_true',
                       help='是否保存数据到CSV文件')
    parser.add_argument('--test', action='store_true',
                       help='运行测试模式')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("币安WebSocket客户端")
    print("=" * 60)
    #print(f"订阅的交易对: {', '.join(args.symbols)}")
    print(f"订阅的交易对: {args.symbols}")
    print(f"K线时间间隔: {args.interval}")
    print(f"保存数据: {'是' if args.save else '否'}")
    print(f"测试模式: {'是' if args.test else '否'}")
    print("=" * 60)
    
    try:
        # 创建WebSocket客户端
        client = SimpleBinanceWebSocket(args.symbols, args.interval)
        
        if args.test:
            # 测试模式：运行5分钟
            print("运行测试模式（5分钟）...")
            import threading
            import time
            
            ws_thread = threading.Thread(target=client.connect)
            ws_thread.daemon = True
            ws_thread.start()
            
            # 等待连接建立
            time.sleep(5)
            
            if not client.is_connected:
                print("❌ WebSocket连接失败")
                return
            
            print("✅ WebSocket连接成功！")
            
            # 运行5分钟
            start_time = time.time()
            while time.time() - start_time < 300:  # 5分钟
                time.sleep(10)
                
                # 显示状态
                for symbol in args.symbols:
                    klines = client.get_all_klines(symbol)
                    current_kline = client.kline_data[symbol]['current_kline']
                    if current_kline:
                        print(f"{symbol}: 价格 {current_kline['close']}, 已接收 {len(klines)} 条K线")
            
            print("测试完成！")
            
            if args.save:
                for symbol in args.symbols:
                    client.save_klines_to_csv(symbol)
        
        else:
            # 正常模式
            print("启动WebSocket客户端...")
            print("按 Ctrl+C 停止程序")
            print("-" * 60)
            
            client.start()
    
    except KeyboardInterrupt:
        print("\n程序被用户中断")
    except Exception as e:
        print(f"程序运行出错: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 