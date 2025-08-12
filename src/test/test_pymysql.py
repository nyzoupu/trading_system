from src.main.utils.sql_util import MySQLUtil

MySQLUtil.init_pool()
print(MySQLUtil.find_all('users'))

#插入一条
#MySQLUtil.insert('users',{'email':'paulsenzou@webank.com','password':'123','id':'58'})
# MySQLUtil.insert_many('users',[{'email':'paulsenzou@webank.com','password':'123','id':'58'},{'email':'paulsenzou@webank.com','password':'123','id':'36'}])
# print(MySQLUtil.find_all('users'))
#
# deleted_rows = MySQLUtil.delete("users", conditions={"email": ("LIKE", "%paulsenzou%")})
# print(f'删除：{deleted_rows}')
# 改成只查已有字段
# df = MySQLUtil.fetch_dataframe('users', conditions={'email': 'paulsenzou@webank.com'})
# print(df)

import pandas as pd


def test_mysqlutil_with_dataframe():

    print("✅ 清空测试数据表...")
    MySQLUtil.execute("DELETE FROM users")

    print("✅ 插入 DataFrame ...")
    df_insert = pd.DataFrame([
        {'email': 'alice@example.com', 'password': '123456', 'age': 25},
        {'email': 'bob@example.com', 'password': 'abcdef', 'age': 30},
        {'email': 'carol@example.com', 'password': 'qwerty', 'age': 35},
    ])
    inserted_rows = MySQLUtil.insert_dataframe('users', df_insert)
    print(f"🔄 插入了 {inserted_rows} 行")

    print("✅ 查询为 DataFrame ...")
    df_query = MySQLUtil.fetch_dataframe('users', conditions={'age': ('>', 26)})
    print(df_query)

    print("✅ 更新 DataFrame 中的记录 ...")
    df_query['age'] += 1
    updated_rows = MySQLUtil.update_from_dataframe('users', df_query, key_columns=['id'])
    print(f"🔄 更新了 {updated_rows} 行")

    print("✅ 查询更新结果 ...")
    df_updated = MySQLUtil.fetch_dataframe('users')
    print(df_updated)

    print("✅ 测试 UPSERT 操作 ...")
    df_upsert = pd.DataFrame([
        {'id': 4, 'email': 'alice@update.com', 'password': '123456', 'age': 99},  # 更新
        {'id':3,'email': 'david@example.com', 'password': 'newpass', 'age': 40}  # 新增
    ])
    # 替换nan为None
    df_upsert = df_upsert.where(pd.notnull(df_upsert), None)


    upserted_rows = MySQLUtil.upsert_from_dataframe('users', df_upsert, key_columns=['id'])
    print(f"🔁 UPSERT 影响行数: {upserted_rows}")

    print("✅ 最终数据：")
    final_df = MySQLUtil.fetch_dataframe('users')
    print(final_df)

if __name__ == "__main__":
    test_mysqlutil_with_dataframe()
