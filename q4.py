import MySQLdb
import pandas as pd
import sys

conn = MySQLdb.connect(<params>) # build the MySQL connection
query = "<sql_query>" # the SQL query
df = pandas.read_sql(query, con=connection)
df.<method>.to_csv(sys.stdout, encoding='utf-8', <params>)