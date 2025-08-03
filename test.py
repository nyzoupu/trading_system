import  pandas as pd
from datetime import datetime, timedelta, timezone
import  time
start_str = '2025-08-01 00:00:00'  # 开始日期
end_str = '2025-08-01 23:13:00'  # 结束日期（None表示到现在）

start_time = int(
    pd.Timestamp(datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S").astimezone(timezone.utc)).timestamp() * 1000)
end_time = int(pd.Timestamp(
    datetime.strptime(end_str, "%Y-%m-%d %H:%M:%S").astimezone(timezone.utc)).timestamp() * 1000) if end_str else None

zone_start_time = int(pd.Timestamp(start_str).timestamp() * 1000)
zone_end_time = int(pd.Timestamp(end_str).timestamp() * 1000) if end_str else None

print("当前系统时区偏移:", time.timezone / 3600, "小时")  # UTC+8 会输出 -8

print(start_str)
print('-----utc datetime-------')
print(start_time)
print(datetime.utcfromtimestamp(start_time/1000))
print(datetime.fromtimestamp(start_time/1000))

zone_start_time = int(pd.Timestamp(start_str).timestamp() * 1000)
print("当前系统时区偏移:", time.timezone / 3600, "小时")  # UTC+8 会输出 -8
print('--------zone时区时间-------')
print(zone_start_time)
print(datetime.utcfromtimestamp(zone_start_time/1000))
print(datetime.fromtimestamp(zone_start_time/1000))

