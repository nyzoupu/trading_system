import pymysql
from DBUtils.PooledDB import PooledDB
import pymysql.cursors
import json
import os
import logging
import pandas as pd
import numpy as np

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
class MySQLUtil:
    """
    PyMySQL 数据库操作工具类。
    集成了连接池和上下文管理器，提供方便的查询和更新方法，
    并增加了一些日常CRUD操作的简化封装。
    """
    _pool = None # 类变量，用于存储连接池实例

    @classmethod
    def init_pool(cls, config_path='../resource/mysql_config.json', section='database'):
        """
        初始化数据库连接池。
        这个方法应该在应用程序启动时只调用一次。
        从配置文件加载数据库连接参数。
        """
        if cls._pool is None:

            try:
                base_dir = os.path.dirname(os.path.abspath(__file__))
                full_config_path = os.path.join(base_dir, config_path)

                file_config={}
                if os.path.exists(full_config_path):
                    with open(full_config_path, 'r', encoding='utf-8') as f:
                        config_data = json.load(f)
                    file_config = config_data.get(section, {})

                override_config = {
                        "database": {
                             "host": "localhost",
                             "user": "root",
                             "password": "root@123",
                             "db": "trading_system",
                             "port": 3306,
                             "mincached": 2,
                             "maxcached": 5,
                             "maxconnections": 10
                        }
                }


                # 合并配置：配置变量优先，文件为默认
                db_config = {**file_config, **((override_config or {}).get(section, {}))}
                if not db_config:
                    raise ValueError(f"配置文件 '{config_path}' 中未找到 '{section}' 节。")

                cls._pool = PooledDB(
                    creator=pymysql,
                    host=db_config.get('host'),
                    user=db_config.get('user'),
                    password=db_config.get('password'),
                    database=db_config.get('db'),
                    port=db_config.get('port', 3306),
                    charset=db_config.get('charset', 'utf8mb4'),
                    cursorclass=pymysql.cursors.DictCursor,
                    mincached=db_config.get('mincached', 5),
                    maxcached=db_config.get('maxcached', 10),
                    maxconnections=db_config.get('maxconnections', 20),
                    blocking=True
                )
                print("数据库连接池初始化成功。")
            except FileNotFoundError:
                print(f"错误: 配置文件 '{full_config_path}' 未找到。")
                raise
            except json.JSONDecodeError as e:
                print(f"错误: 解析配置文件 '{full_config_path}' 失败: {e}")
                raise
            except pymysql.Error as e:
                print(f"数据库连接池初始化失败: {e}")
                raise
            except ValueError as e:
                print(f"配置错误: {e}")
                raise
        else:
            print("连接池已初始化，无需重复操作。")

     # --- 核心连接和执行逻辑 (内部方法) ---
    @classmethod
    def _get_connection(cls):
        """内部方法：从连接池获取连接"""
        if cls._pool is None:
            raise Exception("数据库连接池未初始化，请先调用 MySQLUtil.init_pool()。")
        return cls._pool.connection()

    @classmethod
    def _execute_sql(cls, sql, params=None, is_write_op=False):
        """内部方法：执行单条SQL并处理连接归还"""
        conn = None
        try:
            conn = cls._get_connection()
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                if is_write_op:
                    conn.commit()
                    return cursor.lastrowid if sql.strip().upper().startswith("INSERT") else cursor.rowcount
                else:
                    return cursor.fetchall()
        except pymysql.Error as e:
            if is_write_op and conn: # 写操作才需要回滚
                conn.rollback()
            print(f"数据库操作错误: {e}")
            raise
        finally:
            if conn:
                conn.close() # 归还连接到池

    @classmethod
    def _execute_many_sql(cls, sql, params_list):
        """内部方法：执行多条SQL并处理连接归还 (用于executemany)"""
        conn = None
        try:
            conn = cls._get_connection()
            with conn.cursor() as cursor:
                cursor.executemany(sql, params_list)
                conn.commit()
                return cursor.rowcount # executemany 返回受影响的总行数
        except pymysql.Error as e:
            if conn:
                conn.rollback()
            print(f"数据库批量操作错误: {e}")
            raise
        finally:
            if conn:
                conn.close() # 归还连接到池

    @classmethod
    def _build_where_clauses(cls, conditions: dict):
        """
        内部方法：根据条件字典构建 WHERE 子句和参数列表。
        支持:
        - 精确匹配: {"column": "value"}
        - IN Clause: {"column": ["val1", "val2"]}
        - LIKE Clause: {"column": ("LIKE", "%value%")}
        - BETWEEN Clause: {"column": ("BETWEEN", val1, val2)}
        - 其他操作符: {"column": (">", value)}, {"column": ("<=", value)}, etc.
        """
        where_clauses = []
        params = []
        for col, val in conditions.items():
            if isinstance(val, tuple) and len(val) > 1:
                operator = val[0].upper()
                if operator == "BETWEEN" and len(val) == 3:
                    where_clauses.append(f"{col} {operator} %s AND %s")
                    params.append(val[1])
                    params.append(val[2])
                elif operator in ["=", "<", ">", "<=", ">=", "<>", "!=", "LIKE", "NOT LIKE"]:
                    where_clauses.append(f"{col} {operator} %s")
                    params.append(val[1])
                else:
                    raise ValueError(f"Unsupported operator or incorrect number of arguments for {operator} in condition: {col}: {val}")
            elif isinstance(val, (list, tuple)): # Handles IN clause for lists/tuples (not nested tuples for operators)
                placeholders = ', '.join(['%s'] * len(val))
                where_clauses.append(f"{col} IN ({placeholders})")
                params.extend(val)
            else: # Default to exact equality
                where_clauses.append(f"{col} = %s")
                params.append(val)
        return ' AND '.join(where_clauses), params

    # --- 优化后的日常操作方法 (类方法) ---

    @classmethod
    def find_all(cls, table_name, conditions: dict = None, columns='*', order_by: str = None, limit: int = None, offset: int = None):
        """
        查询指定表的所有数据，可带条件、指定列、排序、限制数量和偏移。
        支持更丰富的条件字典 (LIKE, BETWEEN, >, <等)。
        """
        sql_parts = [f"SELECT {', '.join(columns) if isinstance(columns, list) else columns} FROM {table_name}"]
        params = []

        if conditions:
            where_clause, where_params = cls._build_where_clauses(conditions)
            if where_clause: # Only add WHERE if there are conditions
                sql_parts.append(f"WHERE {where_clause}")
                params.extend(where_params)

        if order_by:
            sql_parts.append(f"ORDER BY {order_by}")
        if limit is not None:
            sql_parts.append(f"LIMIT %s")
            params.append(limit)
        if offset is not None:
            sql_parts.append(f"OFFSET %s")
            params.append(offset)

        sql = " ".join(sql_parts)
        return cls._execute_sql(sql, params, is_write_op=False)

    @classmethod
    def find_one(cls, table_name, conditions: dict = None, columns='*'):
        """
        查询指定表的单条数据，可带条件、指定列。
        支持更丰富的条件字典 (LIKE, BETWEEN, >, <等)。
        """
        results = cls.find_all(table_name, conditions, columns, limit=1)
        return results[0] if results else None

    @classmethod
    def insert(cls, table_name, data: dict):
        """
        插入单条数据到指定表。
        :param table_name: 表名
        :param data: 要插入的数据字典
        :return: 插入行的自增ID（如果有）
        """
        if not data:
            return 0

        # 对列名进行反引号转义
        columns = ', '.join([f"`{col}`" for col in data.keys()])
        placeholders = ', '.join(['%s'] * len(data))
        sql = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
        values = list(data.values())

        return cls._execute_sql(sql, values, is_write_op=True)

    @classmethod
    def insert_many(cls, table_name, data_list: list[dict]):
        """
        批量插入多条数据到指定表。
        要求 data_list 中的每个字典结构相同（即拥有相同的键）。
        自动处理数据中的 nan 值（转换为 None/NULL）
        自动转义列名和表名防止 SQL 语法错误
        :param table_name: 表名
        :param data_list: 包含要插入的字典的列表。
        :return: 受影响的总行数。
        """
        if not data_list:
            return 0

        # 处理 nan 值（转换为 None）
        def convert_nan_to_none(item):
            """递归处理字典中的 nan 值"""
            if isinstance(item, dict):
                return {k: convert_nan_to_none(v) for k, v in item.items()}
            elif isinstance(item, list):
                return [convert_nan_to_none(v) for v in item]
            else:
                # 处理 float('nan') 和 np.nan
                try:
                    if item is not None and item != item:  # nan 不等于自身
                        return None
                except TypeError:
                    pass
                return item

        # 处理数据中的 nan
        processed_data = [convert_nan_to_none(data) for data in data_list]

        try:
            # 转义所有列名（使用反引号）
            columns = ', '.join([f'`{key}`' for key in processed_data[0].keys()])

            # 创建占位符字符串
            placeholders = ', '.join(['%s'] * len(processed_data[0]))

            # 转义表名
            sql = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"

            # 提取值列表
            values_list = [list(data.values()) for data in processed_data]

            # 执行批量插入
            return cls._execute_many_sql(sql, values_list)

        except Exception as e:
            # 记录错误信息以便调试
            error_message = f"批量插入失败: {str(e)}"

            # 添加额外调试信息
            debug_info = {
                "table_name": table_name,
                "sql": sql,
                "first_row_keys": list(processed_data[0].keys()) if processed_data else [],
                "first_row_values": values_list[0] if values_list else [],
                "data_count": len(data_list)
            }

            # 记录错误（实际项目中应使用日志系统）
            print(f"SQL 错误: {error_message}")
            print(f"调试信息: {debug_info}")

            # 重新抛出异常以便上层处理
            raise RuntimeError(f"数据库批量操作错误: {str(e)}") from e

    @classmethod
    def update(cls, table_name, data: dict, conditions: dict):
        """
        更新指定表的数据。
        支持更丰富的条件字典 (LIKE, BETWEEN, >, <等)。
        :return: 受影响的行数
        """
        if not data or not conditions:
            return 0

        set_clauses = []
        update_values = []
        for col, val in data.items():
            set_clauses.append(f"{col} = %s")
            update_values.append(val)

        where_clause, where_params = cls._build_where_clauses(conditions)
        if not where_clause:
             raise ValueError("Update operation requires conditions.")

        sql = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE {where_clause}"
        params = update_values + where_params
        return cls._execute_sql(sql, params, is_write_op=True)

    @classmethod
    def delete(cls, table_name, conditions: dict):
        """
        从指定表删除数据。
        支持更丰富的条件字典 (LIKE, BETWEEN, >, <等)。
        :return: 受影响的行数
        """
        if not conditions:
            raise ValueError("删除操作必须提供条件，否则可能删除所有数据。")

        where_clause, params = cls._build_where_clauses(conditions)
        if not where_clause:
            # This should ideally not happen if conditions is not empty and _build_where_clauses is correct
            # but acts as a safeguard.
            raise ValueError("No valid WHERE clause could be built from conditions.")


        sql = f"DELETE FROM {table_name} WHERE {where_clause}"
        return cls._execute_sql(sql, params, is_write_op=True)

    # --- fetch_all, fetch_one, execute 现在可以直接调用 _execute_sql ---
    @classmethod
    def fetch_all(cls, sql, params=None):
        return cls._execute_sql(sql, params, is_write_op=False)

    @classmethod
    def fetch_one(cls, sql, params=None):
        results = cls._execute_sql(sql, params, is_write_op=False)
        return results[0] if results else None

    @classmethod
    def execute(cls, sql, params=None):
        # execute 通常用于 INSERT/UPDATE/DELETE，所以默认视为写操作
        return cls._execute_sql(sql, params, is_write_op=True)

# datafram
    @staticmethod
    def _sanitize_nan(obj):
        if isinstance(obj, pd.DataFrame):
            # 强制每列类型为 object（混合类型），避免类型阻挡 NaN 显示
            obj = obj.astype(object)
            return obj.applymap(lambda x: None if pd.isna(x) else x)

        if isinstance(obj, list):
            return [MySQLUtil._sanitize_nan(row) for row in obj]

        if isinstance(obj, dict):
            return {k: None if pd.isna(v) else v for k, v in obj.items()}

        # 对单个值的处理（包括 np.nan、pd.NaT、float('nan') 等）
        if pd.isna(obj):
            return None

        return obj

    @classmethod
    def insert_dataframe(cls, table_name: str, df: pd.DataFrame) -> int:
        if df.empty:
            logging.warning("🟡 insert_dataframe: DataFrame 是空的，未执行插入。")
            return 0
        df = cls._sanitize_nan(df)
        data_list = df.to_dict(orient='records')
        return cls.insert_many(table_name, data_list)

    @classmethod
    def fetch_dataframe(cls, table_name: str, conditions: dict = None, columns='*', order_by=None, limit=None, offset=None) -> pd.DataFrame:
        rows = cls.find_all(table_name, conditions, columns, order_by, limit, offset)
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    @classmethod
    def update_from_dataframe(cls, table_name: str, df: pd.DataFrame, key_columns: list) -> int:
        if df.empty or not key_columns:
            logging.warning("🟡 update_from_dataframe: DataFrame 为空或缺少主键列")
            return 0
        df = cls._sanitize_nan(df)
        total_updated = 0
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            condition = {k: row_dict[k] for k in key_columns}
            data = {k: v for k, v in row_dict.items() if k not in key_columns}
            if data:
                total_updated += cls.update(table_name, data, condition)
        return total_updated

    @classmethod
    def upsert_from_dataframe(cls, table_name: str, df: pd.DataFrame, key_columns: list) -> int:
        if df.empty:
            logging.warning("🟡 upsert_from_dataframe: DataFrame 是空的，未执行操作。")
            return 0
        if not key_columns:
            raise ValueError("主键列不能为空")
        df = cls._sanitize_nan(df)
        columns = list(df.columns)
        placeholders = ', '.join(['%s'] * len(columns))
        update_clause = ', '.join([f"{col} = VALUES({col})" for col in columns if col not in key_columns])
        if not update_clause:
            logging.warning("🟡 没有需要更新的字段（所有字段都是主键）")
            return 0
        sql = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_clause}
        """
        values_list = df[columns].values.tolist()
        return cls._execute_many_sql(sql, values_list)