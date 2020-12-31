######  GENERAL  ######
import pandas as pd

######  DATABASE  ######
import sqlite3

######  PARALLELIZATION  ######
from multiprocessing import Lock

######  LOGGING  ######
from ProcessLogger import ProcessLogger

# The DatabaseManager takes care of interfacing with the sqlite3 database while making sure the access is handled in a threadsafe manner
class DatabaseManager:
    def __init__(self, data_path, verbose=False):
        self.__verbose = verbose
        self.__db_lock = Lock()
        self.__data_path = data_path
        self.__logger = ProcessLogger("__DBManager__")

    def add(self, data, table, if_exists="append"):
        self.__db_lock.acquire()
        conn = sqlite3.connect(self.__data_path)

        e = conn.execute("select count(*) from sqlite_master where type='table' and name='" + table + "'")
        if e.fetchone()[0] == 0 and self.__verbose:
            self.__logger.info("Creating table " + table + " in database")

        if self.__verbose:
            self.__logger.info("Adding data to table " + table + " in database")

        data.to_sql(table,
                    conn,
                    if_exists=if_exists,
                    index=False)

        conn.close()
        self.__db_lock.release()

    def get(self, table, columns=None, predicate=None):
        self.__db_lock.acquire()
        _columns = "*"
        if columns:
            _columns = ""
            for col in columns[:-1]:
                _columns += col + ","
            _columns += columns[-1]
        _predicate = "" if not predicate else " where " + predicate
        try:
            data = pd.read_sql(
                "select " + _columns + " from " + table + _predicate,
                con=sqlite3.connect(self.__data_path)
            )
        except pd.io.sql.DatabaseError:
            data = pd.DataFrame({})

        self.__db_lock.release()
        return data

    def delete(self, table, predicate):
        self.__db_lock.acquire()
        conn = sqlite3.connect(self.__data_path)
        conn.execute(
            "delete from " + table + " " + predicate
        )
        conn.commit()
        self.__db_lock.release()
