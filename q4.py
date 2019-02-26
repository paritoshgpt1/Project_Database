import MySQLdb
import pandas as pd
import sys

# build the MySQL connection
conn = MySQLdb.connect("localhost", "clouduser", "dbroot", "yelp_db")
query = "SELECT review_count, stars FROM businesses"  # the SQL query
df = pd.read_sql(query, con=conn)
df.describe().to_csv(sys.stdout, encoding='utf-8',
                     float_format='%.2f', index=False)
