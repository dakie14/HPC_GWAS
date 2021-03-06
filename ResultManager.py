######  GENERAL  ######
import pandas as pd
from ToolBox import decompress
from DatabaseManager import DatabaseManager

######  LOGGING  ######
from ProcessLogger import ProcessLogger

######  PARALLELIZATION  ######
from multiprocessing import Process, Queue

# The ResultManager takes care of preparing results for storage and then sending the results to the DBManager.
# Processing of results happens on a separate process to avoid blocking the server
class ResultManager:
    def __init__(self, output_path, verbose=False):
        self.__logger = ProcessLogger("__ResultManager__")
        self.__output_path = output_path

    def run(self, snps_to_run):
        q = Queue()
        self.__result_handler = Process(target=self.process_results,
                                        args=(q, self.__output_path, snps_to_run))
        self.__result_handler.start()
        self.__queue = q

    def get_stored_ids(self, chromosomes):
        dbm = DatabaseManager(self.__output_path)
        data = {}
        for c in chromosomes:
            df = dbm.get("Intercept")
            if len(df) > 0:
                data[c] = df["id"].to_list()
        return data

    def __prepare_data(self, dbm):
        self.__logger.info("Preparing data..")
        tables = dbm.get("sqlite_master", columns=["tbl_name"], predicate="type='table'")["tbl_name"].to_list()
        entries = 0
        if len(tables) > 0:
            tbl_name = ""
            for table in tables:
                count = dbm.get(table, columns=["count(id)"])["count(id)"].to_list()[0]
                if entries <= count:
                    entries = count
                    tbl_name = table
            tables.remove(tbl_name)
            for table in tables:
                dbm.delete(table, "where id not in (select id from " + tbl_name + ")")
        self.__logger.info("Data ready")
        return entries

    def process_results(self, q, output_path, total_snps):
        dbm = DatabaseManager(output_path)
        entries = self.__prepare_data(dbm)

        while True:
            data = decompress(
                q.get()
            )

            for key in list(data):
                df = pd.read_json(data[key], orient="records")
                self.__logger.info("Adding data for " + key + " to database (Status: " + str(entries + len(df)) + " of " + str(total_snps) + ")")
                dbm.add(df, key)

            entries += len(data["Intercept"])



    def store_result(self, result):
        self.__queue.put(result)