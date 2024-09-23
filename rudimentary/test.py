import pandas as pd
from pprint import pprint as pp
from connections import Connections


conn = Connections() 


with conn.adb_connect() as connection:

    query = """select * from users_master limit 10;"""

    df = pd.read_sql_query(query, connection)
    
    pp(df)

    # df.to_csv('testing.csv', index=False)