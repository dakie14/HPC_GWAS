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
        return pd.read_json(
            requests.get(self.__server_url + "/data", params={"name": name}).text,
            orient="records"
        )

    def get_batch(self, size):
        r = requests.get(self.__server_url + "/batch", params={"batch_size": size})
        if r.status_code != 200:
            time.sleep(5)
            return self.get_batch(size)

        data = r.content
        return decompress(data)

    def store_result(self, results):
        data = {}
        for key in list(results):
            data[key] = results[key].to_json(orient="records")

        compressed = compress(data)

        # Keep trying to post results until server accepts
        while requests.post(
                self.__server_url + "/result",
                data=compressed
        ).status_code != 200:
            time.sleep(5)
            continue
