######  GENERAL  ######
import pandas as pd
from ToolBox import decompress
from DBManager import DBManager

######  LOGGING  ######
from ProcessLogger import ProcessLogger

######  PARALLELIZATION  ######
from multiprocessing import Process, Queue

# The ResultManager takes care of preparing results for storage and then sending the results to the DBManager.
# Processing of results happens on a separate process to avoid blocking the server
class ResultManager:
    def __init__(self, result_data_path):
        self.__logger = ProcessLogger("__ResultManager__")
        self.__dbm = DBManager(result_data_path)

        self.__prepare_data()

        self.__queue = Queue()
        self.__result_handler = Process(target=self.__process,
                                        args=(self.__queue, self.__dbm))
        self.__result_handler.start()

    def __prepare_data(self):
        tables = self.__dbm.get("sqlite_master", columns=["tbl_name"], predicate="type='table'")["tbl_name"].to_list()
        if len(tables) > 0:
            entries = 0
            tbl_name = ""
            for table in tables:
                count = self.__dbm.get(table, columns=["count(id)"])["tbl_name"].to_list()[0]
                if entries >= count:
                    entries = count
                    tbl_name = table

            tables.remove(tbl_name)

            for table in tables:
                self.__dbm.delete(table, "where id not in (select id from " + tbl_name + ")")

    def __process(self, q, dbm):
        while True:
            data = decompress(
                q.get()
            )

            for key in list(data):
                df = pd.read_json(data[key], orient="records")

                dbm.store_result(
                    df=df,
                    table=key
                )

    def store_result(self, result):
        self.__queue.put(result)