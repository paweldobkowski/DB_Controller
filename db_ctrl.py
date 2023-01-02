from sqlalchemy import create_engine
import datetime
import pandas as pd
import numpy as np
import re
import datetime
import os.path

from utilities.logs import logging


server = ""
database = ""

class Python_DB_Controller:
    def __init__(self, server=server, database=database):
        # Create engine to DB
        self.engine = create_engine(
            "mssql+pyodbc://"
            + server
            + "/"
            + database
            + "?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server", fast_executemany=True
        )
        self.cnxn = None

    def connection_open(self):
        # Open connection to database
        self.cnxn = self.engine.connect()

        logging.info("Polaczono z baza danych")

    def query_read_only(self, sql, print_sql_string=True):
        # Read data from database to DF
        df = pd.read_sql(sql, self.cnxn)

        query_info = "Wykonano zapytanie do bazy: " + str(sql)
        query_info = query_info.replace("\n", " ")
        query_info = re.sub(' +', ' ', query_info)   # delete unnecessary spaces

        if print_sql_string == True:
            logging.info(query_info)

        return df

    def query(self, sql, print_sql_string=True):
        # Make query to DB
        self.cnxn.execute(sql)

        query_info = "Wykonano zapytanie do bazy: " + str(sql)
        query_info = query_info.replace("\n", " ")
        query_info = re.sub(' +', ' ', query_info)   # delete unnecessary spaces

        if print_sql_string == True:
            logging.info(query_info)

    def query_insert_df(self, dataframe_to_insert,schema_for_table, name_table_in_database):
        # Insert
        dataframe_to_insert.to_sql(schema=schema_for_table,name=name_table_in_database, con=self.cnxn, if_exists='append', index=False, method=None, chunksize=100)

        # Count inserted rows
        amount_of_rows = len(dataframe_to_insert.index)

        # Printing out the amount of new rows
        if amount_of_rows == 0:
            logging.warning(f"Liczba wierszy wstawionych: {amount_of_rows}")
        else:
            logging.info(f"Liczba wierszy wstawionych: {amount_of_rows}")

        return amount_of_rows

    def query_upsert_df(self, dataframe_to_insert, dataframe_from_db, join_on_columns,schema_for_table, name_table_in_database, save_history_updates=True):
        if not dataframe_from_db.empty:
            ## UPDATE
            ## Get unique id in two dataframes (inner join)
            df_same_id = pd.merge(dataframe_to_insert, dataframe_from_db, how='inner',
                                      on=join_on_columns, indicator=True,
                                      suffixes=('', '_delme'))  # left join with data from excel and db
            df_same_id = df_same_id[join_on_columns]

            # filter both dataframes by unique id in both
            df1 = dataframe_to_insert
            df2 = dataframe_from_db

            df1 = pd.merge(df1, df_same_id, how='left',
                                  on=join_on_columns, indicator=True,
                                  suffixes=('', '_delme'))
            df2 = pd.merge(df2, df_same_id, how='left',
                                  on=join_on_columns, indicator=True,
                                  suffixes=('', '_delme'))
            # Take only new data
            df1 = df1[df1['_merge'] == 'both']
            df2 = df2[df2['_merge'] == 'both']

            # Drop merge column
            df1 = df1.drop(columns=['_merge'])
            df2 = df2.drop(columns=['_merge'])

            # Make index
            df1 = df1.set_index(join_on_columns)
            df2 = df2.set_index(join_on_columns)

            ## compare two dataframes and get diffs
            df_compare = df1.compare(df2, align_axis=0, keep_shape=False).rename({'self': 'new_value', 'other': 'old_value'})

            df_compare = df_compare.stack(dropna=False)

            df_compare = df_compare.reset_index()

            df_compare_col_list = df_compare.columns.values.tolist()

            # save history of updates if there are some updates
            if save_history_updates == True:
                if not df_compare.empty:
                    # if directory data_updates_history does not exists, create it:
                    if not os.path.exists("data_updates_history"):
                        os.makedirs("data_updates_history")

                    now = datetime.datetime.now()
                    date_time_str = now.strftime("%Y_%m_%d_%H_%M_%S")
                    save_path = "data_updates_history\\" + date_time_str + "_updated_rows_" + name_table_in_database +".xlsx"
                    df_compare.to_excel(save_path)

            # only new values
            df_compare = df_compare[df_compare[df_compare_col_list[-3]] == "new_value"]

            ## update only diffs - dynamic sql
            amount_of_rows_updated = 0
            for index, row in df_compare.iterrows():
                amount_of_rows_updated += 1
                # depends on key values
                where_string = ''
                i = 0
                for x in join_on_columns:
                    where_string += x + """='""" + str(row[df_compare_col_list[i]]) + """' AND """
                    i += 1
                sql = """UPDATE """ + str(schema_for_table) + """.""" + str(name_table_in_database) + """ SET """ + \
                      str(row[df_compare_col_list[-2]]) + """='""" + str(row[df_compare_col_list[-1]]) + """'"""\
                      """ WHERE (""" + where_string[:-5] + """);"""
                sql = sql.replace("""'nan'""",'NULL')   # change NaN to NULL for insert NULL values
                self.cnxn.execute(sql)
                logging.info("Wykonano zapytanie do bazy: " + sql)

            # Printing out the amount of updated rows
            if amount_of_rows_updated == 0:
                logging.info(f"Liczba wierszy na ktorych byl update: {amount_of_rows_updated}")
            else:
                logging.info(f"Liczba wierszy na ktorych byl update: {amount_of_rows_updated}")

            # INSERT
            # Check for new rows
            df_insert_only = pd.merge(dataframe_to_insert, dataframe_from_db, how='left',
                                                  on=join_on_columns, indicator=True,
                                                  suffixes=('', '_delme'))  # left join with data from excel and db

            # Discard the columns that acquired a suffix
            df_insert_only = df_insert_only[
                [c for c in df_insert_only.columns if not c.endswith('_delme')]]

            # Take only new data
            df_insert_only = df_insert_only[df_insert_only['_merge'] == 'left_only']

            # Drop merge column
            df_insert_only = df_insert_only.drop(columns=['_merge'])
        else:
            df_insert_only = dataframe_to_insert

        # Insert
        df_insert_only.to_sql(schema=schema_for_table,name=name_table_in_database, con=self.cnxn, if_exists='append', index=False, method=None, chunksize=100)

        # Count inserted rows
        amount_of_rows = len(df_insert_only.index)

        # Printing out the amount of new rows
        if amount_of_rows == 0:
            logging.warning(f"Liczba wierszy wstawionych: {amount_of_rows}")
        else:
            logging.info(f"Liczba wierszy wstawionych: {amount_of_rows}")

        return amount_of_rows, amount_of_rows_updated

    def connection_close(self):
        # Close connection to DB
        self.engine.dispose()
        self.cnxn.close()

        logging.info("Rozlaczono z baza danych")
