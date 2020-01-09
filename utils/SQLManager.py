from sqlalchemy import create_engine
import pandas as pd


class SQLManager:
    _instance = None

    @staticmethod
    def get_istance():
        """ Static access method. """
        if SQLManager._instance == None:
            SQLManager()
        return SQLManager._instance

    def __init__(self):
        """ Virtually private constructor. """
        if SQLManager._instance != None:
            raise Exception("This class is a singleton!")
        else:
            self._db_connection_str = 'mysql+pymysql://root:pillola96ct@127.0.0.1/tesi_siremar_smartTicketing'
            self._db_connection = create_engine(self._db_connection_str)
            SQLManager._instance = self


    def execute_query(self, string_sql):
        return pd.read_sql(string_sql, con=self._db_connection)
