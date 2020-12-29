######  GENERAL  ######
from GWASManager import GWASManager
import os
import uuid
import pandas as pd
import time
from ToolBox import compress, decompress

######  HTTP  ######
import requests

######  LOGGER  ######
from ProcessLogger import ProcessLogger

class DataManager:
    def __init__(self, host="local", port=5000, data_path=None, result_path=None):
        self.__server_url = "http://" + host + ":" + str(port)
        self.__gwas_manager = None
        self.__local = False
        self.__logger = ProcessLogger("__DataManager__")

        if host == "local":
            if not data_path or not os.path.isfile(data_path):
                self.__logger.info("Error: could not find database for data")
                exit(1)
            self.__gwas_manager = GWASManager(
                data_path,
                result_path if result_path else os.getcwd() + "/" + str(uuid.uuid4()) + ".sqlite3"
            )
            self.__local = True

    def get_covariates(self):
        if self.__local:
            return self.__gwas_manager.get_covariates()

        return pd.read_json(
            requests.get(self.__server_url + "/covariates").text,
            orient="records"
        )

    def get_batch(self, size):
        if self.__local:
            data = self.__gwas_manager.get_batch(size)
            return decompress(data)

        r = requests.get(self.__server_url + "/batch")
        if r.status_code != 200:
            time.sleep(5)
            return self.get_batch(size)

        data = r.content
        return decompress(data)

    def store_result(self, results):
        data = {}
        for result in results:
            for key in list(result):
                if key not in list(data):
                    data[key] = result[key]
                else:
                    data[key] = data[key].append(result[key])

        for key in list(data):
            data[key] = data[key].to_json(orient="records")

        compressed = compress(data)

        if self.__local:
            self.__gwas_manager.store_result(compressed)
            return

        # Keep trying to post results until server accepts
        while requests.post(
                self.__server_url + "/store_result",
                data=compressed
        ).status_code != 200:
            time.sleep(5)
            continue