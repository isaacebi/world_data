import sqlite3
import pandas as pd

class SQLite_Helper:
    def __init__(self, DB_path):
        self.DB_path = DB_path # incase needed
        self.cnx = sqlite3.connect(DB_path)

    def getDB(self, tableName):
        try:
            query = f"SELECT * FROM {tableName}"
            df = pd.read_sql_query(query, self.cnx)

        # if unable to connect or table is unavailable, just return empty dataframe
        except:
            df = pd.DataFrame()

        finally:
            return df

    def commitDB(self, tableName, values, df):
        # check if data to commit is empty - error handling
        if df.empty:
            return None

        # initialization
        columnWithDtype = ""

        # iterate through values datatype - to create sql query
        for key in values:
            for value in values[key]:
                columnWithDtype = ", ".join(filter(None, [columnWithDtype, f"{value} {key}"]))

        # another way to iterate is using list method
        placeHolders = ", ".join("?" for _ in range(len(df.columns)))
        col_insert_query = ", ".join(filter(None, [f"{value}" for key in values for value in values[key]]))

        # creating table if table not exist - error handling
        query = f"CREATE TABLE IF NOT EXISTS {tableName} ({columnWithDtype})"
        cursor = self.cnx.cursor()
        cursor.execute(query)

        # update db
        for row in df.itertuples(index=False):
            query = f"INSERT INTO {tableName} ({col_insert_query}) VALUES ({placeHolders})"
            cursor.execute(query, row)
        
        # commit to db
        self.cnx.commit()
