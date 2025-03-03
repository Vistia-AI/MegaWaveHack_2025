import sqlite3
import pandas as pd

# Kết nối đến cơ sở dữ liệu SQLite
conn = sqlite3.connect("db.sqlite")

# Đọc dữ liệu từ bảng "products" vào DataFrame
df = pd.read_sql_query("SELECT * FROM coin_prices", conn)

# Hiển thị DataFrame
print(df)

# Đóng kết nối
conn.close()
