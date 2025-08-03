
from websocket import WebSocketApp

def on_message(ws, message):
    print("📩 收到消息:", message)

def on_error(ws, error):
    print("❌ 错误:", error)

def on_close(ws, close_status_code, close_msg):
    print("🔌 连接关闭", close_status_code, close_msg)

def on_open(ws):
    print("✅ 连接成功")

ws = WebSocketApp(
    "wss://stream.binance.com:9443/ws/btcusdt@trade",
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)

ws.run_forever(
    http_proxy_host="127.0.0.1",
    http_proxy_port=7890,  # 根据你代理工具实际端口修改
    proxy_type="http"
)
