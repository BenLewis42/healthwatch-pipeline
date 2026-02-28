import duckdb
conn = duckdb.connect('data/warehouse.duckdb')
print(conn.execute('SHOW TABLES').df())
