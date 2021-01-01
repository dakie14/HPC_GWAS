######  GENERAL  ######
import pandas as pd
import time
from ToolBox import compress, decompress

######  HTTP  ######
import requests

######  LOGGER  ######
from ProcessLogger import ProcessLogger

class DataManager:
    def __init__(self, host="0.0.0.0", port=5000):
        self.__server_url = "http://" + host + ":" + str(port)
        self.__logger = ProcessLogger("__DataManager__")
        requests.get(self.__server_url + "/status")

    def get_supplementary_data(self, name):
        iterations = 0
        while iterations < 30:
            iterations += 1
            try:
                r = requests.get(self.__server_url + "/data", params={"name": name})
                return pd.read_json(
                    r.text,
                    orient="records"
                )
            except requests.exceptions.ConnectionError:
                time.sleep(1)
                continue

        self.__logger.info("Error: Could not connect to host")
        exit(0)


    def get_batch(self, size):
        iterations = 0
        while iterations < 30:
            iterations += 1
            try:
                r = requests.get(self.__server_url + "/batch", params={"batch_size": size})
            except requests.exceptions.ConnectionError:
                time.sleep(1)
                continue
            else:
                if r.status_code != 200:
                    time.sleep(1)
                    continue

                data = r.content
                return decompress(data)

    def store_result(self, results):
        data = {}
        for key in list(results):
            data[key] = results[key].to_json(orient="records")

        compressed = compress(data)

        iterations = 0
        while iterations < 60:
            iterations += 1
            try:
                r = requests.post(self.__server_url + "/result", data=compressed)
            except requests.exceptions.ConnectionError:
                time.sleep(1)
                continue
            else:
                # Keep trying to post results until server accepts
                if r.status_code != 200:
                    time.sleep(1)
                    continue
